from flask import Flask
from flask_socketio import SocketIO, emit
import logging

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@socketio.on('connect')
def handle_connect():
    logger.info(f"Client connected")

@socketio.on('kafka_data')
def handle_kafka_data(data):
    logger.info("=" * 50)
    logger.info("Received Kafka Data:")
    logger.info(f"Symbol: {data.get('stock_symbol')}")
    logger.info(f"Price: {data.get('price')}")
    logger.info(f"Volume: {data.get('volume')}")
    logger.info(f"Timestamp: {data.get('timestamp')}")
    logger.info("=" * 50)

if __name__ == '__main__':
    logger.info("Starting WebSocket server on port 6000...")
    socketio.run(app, host='0.0.0.0', port=6000, debug=True)

