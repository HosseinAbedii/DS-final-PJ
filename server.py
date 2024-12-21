from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
from datetime import datetime
import signal
import sys
from typing import Dict, Any

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

# Data schemas for validation
STOCK_SCHEMA = {
    'required': ['stock_symbol', 'opening_price', 'closing_price', 'high', 'low', 'volume', 'timestamp'],
    'types': {
        'stock_symbol': str,
        'opening_price': (int, float),
        'closing_price': (int, float),
        'high': (int, float),
        'low': (int, float),
        'volume': int,
        'timestamp': (int, float)
    }
}

DATA_SCHEMAS = {
    'stock': STOCK_SCHEMA,
    'order_book': {
        'required': ['data_type', 'timestamp', 'stock_symbol', 'order_type', 'price', 'quantity'],
        'types': {
            'data_type': str,
            'timestamp': (int, float),
            'stock_symbol': str,
            'order_type': str,
            'price': (int, float),
            'quantity': int
        }
    },
    'news_sentiment': {
        'required': ['data_type', 'timestamp', 'stock_symbol', 'sentiment_score', 'sentiment_magnitude'],
        'types': {
            'data_type': str,
            'timestamp': (int, float),
            'stock_symbol': str,
            'sentiment_score': (int, float),
            'sentiment_magnitude': (int, float)
        }
    }
}

def validate_data(data: Dict[str, Any]) -> tuple[bool, str]:
    """Validate incoming data against schemas"""
    if not isinstance(data, dict):
        return False, "Data must be a JSON object"
    
    data_type = data.get('data_type', 'stock')
    schema = DATA_SCHEMAS.get(data_type)
    
    if not schema:
        return False, f"Unknown data type: {data_type}"
    
    # Check required fields
    for field in schema['required']:
        if field not in data:
            return False, f"Missing required field: {field}"
    
    # Validate types
    for field, expected_type in schema['types'].items():
        if field in data and not isinstance(data[field], expected_type):
            return False, f"Invalid type for {field}: expected {expected_type}, got {type(data[field])}"
    
    return True, ""

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route('/ingest', methods=['GET', 'POST'])
def ingest_data():
    if request.method == 'GET':
        return jsonify({
            "message": "Data ingestion API endpoint",
            "supported_data_types": list(DATA_SCHEMAS.keys()),
            "usage": {
                "method": "POST",
                "content-type": "application/json",
                "schemas": DATA_SCHEMAS
            }
        }), 200
        
    elif request.method == 'POST':
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "No data provided"}), 400

            is_valid, error_message = validate_data(data)
            if not is_valid:
                return jsonify({"error": error_message}), 400

            data_type = data.get('data_type', 'stock')
            logging.info(f"Received valid {data_type} data: {data}")
            return jsonify({
                "status": "success",
                "message": f"Successfully ingested {data_type} data"
            }), 200
            
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
