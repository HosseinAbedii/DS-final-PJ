from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
from datetime import datetime
import signal
import sys

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ingest.log')
    ]
)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route('/ingest', methods=['GET', 'POST'])
def ingest_data():
    if request.method == 'GET':
        return jsonify({
            "message": "This endpoint accepts POST requests for data ingestion",
            "usage": {
                "method": "POST",
                "content-type": "application/json",
                "example_payload": {
                    "stock_symbol": "AAPL",
                    "data_type": "stock",
                    "timestamp": "1234567890"
                }
            }
        }), 200
        
    elif request.method == 'POST':
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "No data provided"}), 400

            logging.info(f"Received data: {data}")
            return jsonify({"status": "success"}), 200
            
        except Exception as e:
            logging.error(f"Error processing request: {e}")
            return jsonify({"error": str(e)}), 500

@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({
        "error": "Method not allowed",
        "message": "This endpoint doesn't support this HTTP method"
    }), 405

@app.errorhandler(404)
def not_found(e):
    return jsonify({
        "error": "Not found",
        "message": "The requested resource was not found"
    }), 404

def graceful_shutdown(signum, frame):
    logging.info("Server shutting down gracefully...")
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    
    # Start the server
    logging.info("Starting ingestion server...")
    app.run(host='0.0.0.0', port=5000)
