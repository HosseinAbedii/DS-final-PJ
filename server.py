from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from kafka import KafkaProducer
import json
import logging
from datetime import datetime
import signal
import sys
from typing import Dict, Any
from queue import Queue
import threading
import time

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Kafka configuration
kafka_bootstrap_servers = ['kafka:9092']
kafka_producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ingest.log')
    ]
)

# Data queues for different types
data_queues = {
    'stock': Queue(),
    'order_book': Queue(),
    'news_sentiment': Queue(),
    'market_data': Queue(),
    'economic_indicator': Queue()
}

def broadcast_data():
    """Continuously broadcast data from queues"""
    while True:
        for data_type, queue in data_queues.items():
            if not queue.empty():
                data = queue.get()
                socketio.emit(f'data_{data_type}', data)
                logging.info(f"Broadcasting {data_type} data: {data}")
        time.sleep(0.1)  # Small delay to prevent CPU overload

def send_to_kafka(data_type, data):
    """Send data to appropriate Kafka topic"""
    try:
        topic = f"financial_data_{data_type}"
        kafka_producer.send(topic, value=data)
        logging.info(f"Sent to Kafka topic {topic}: {data}")
    except Exception as e:
        logging.error(f"Error sending to Kafka: {e}")

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route('/ingest', methods=['GET', 'POST'])
def ingest_data():
    if request.method == 'GET':
        return jsonify({
            "message": "Data ingestion API endpoint",
            "supported_data_types": list(data_queues.keys()),
            "status": "active"
        }), 200
        
    elif request.method == 'POST':
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "No data provided"}), 400

            # Determine data type
            data_type = data.get('data_type', 'stock')
            if data_type not in data_queues:
                return jsonify({"error": f"Unsupported data type: {data_type}"}), 400

            # Add data to appropriate queue
            data_queues[data_type].put(data)
            
            # Send to Kafka
            send_to_kafka(data_type, data)
            
            logging.info(f"Processed {data_type} data: {data}")

            return jsonify({
                "status": "success",
                "message": f"Successfully processed {data_type} data",
                "data": data
            }), 200
            
        except Exception as e:
            logging.error(f"Error processing request: {e}")
            return jsonify({"error": str(e)}), 500

@socketio.on('connect')
def handle_connect():
    logging.info(f"Client connected: {request.sid}")
    emit('connection_status', {'status': 'connected'})

@socketio.on('disconnect')
def handle_disconnect():
    logging.info(f"Client disconnected: {request.sid}")

def graceful_shutdown(signum, frame):
    logging.info("Server shutting down gracefully...")
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    
    # Start broadcasting thread
    broadcast_thread = threading.Thread(target=broadcast_data, daemon=True)
    broadcast_thread.start()
    
    # Start the server
    logging.info("Starting ingestion server with WebSocket support...")
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
