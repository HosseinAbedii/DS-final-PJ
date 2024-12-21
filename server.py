from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import logging
from datetime import datetime
import signal
import sys
from typing import Dict, Any

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes
socketio = SocketIO(app, cors_allowed_origins="*")

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
    },
    'market_data': {
        'required': ['data_type', 'timestamp', 'stock_symbol', 'market_cap', 'pe_ratio'],
        'types': {
            'data_type': str,
            'timestamp': (int, float),
            'stock_symbol': str,
            'market_cap': (int, float),
            'pe_ratio': (int, float)
        }
    },
    'economic_indicator': {
        'required': ['data_type', 'timestamp', 'indicator_name', 'value'],
        'types': {
            'data_type': str,
            'timestamp': (int, float),
            'indicator_name': str,
            'value': (int, float)
        }
    }
}

def serialize_schema(schema):
    """Convert schema types to string representations"""
    serialized = {
        'required': schema['required'],
        'types': {}
    }
    
    for field, type_info in schema['types'].items():
        if isinstance(type_info, tuple):
            # Handle tuple of types (e.g., (int, float))
            serialized['types'][field] = [t.__name__ for t in type_info]
        else:
            # Handle single type
            serialized['types'][field] = type_info.__name__
    
    return serialized

def get_serialized_schemas():
    """Get JSON-serializable version of all schemas"""
    return {
        data_type: serialize_schema(schema)
        for data_type, schema in DATA_SCHEMAS.items()
    }

def validate_and_convert_data(data: Dict[str, Any]) -> tuple[bool, str, Dict[str, Any]]:
    """Validate and convert data types"""
    if not isinstance(data, dict):
        return False, "Data must be a JSON object", None
    
    data_type = data.get('data_type', 'stock')
    schema = DATA_SCHEMAS.get(data_type)
    
    if not schema:
        return False, f"Unknown data type: {data_type}", None
    
    converted_data = {}
    
    # Check required fields and convert types
    for field in schema['required']:
        if field not in data:
            return False, f"Missing required field: {field}", None
        
        value = data[field]
        expected_type = schema['types'][field]
        
        try:
            if isinstance(expected_type, tuple):
                # Handle numeric types (int, float)
                if isinstance(value, (int, float)):
                    converted_data[field] = value
                else:
                    return False, f"Invalid type for {field}: expected number", None
            else:
                # Handle string types
                if expected_type == str:
                    converted_data[field] = str(value)
                elif expected_type == int:
                    converted_data[field] = int(value)
                else:
                    converted_data[field] = value
        except (ValueError, TypeError):
            return False, f"Invalid value for {field}: {value}", None
    
    return True, "", converted_data

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
                "schemas": get_serialized_schemas()
            }
        }), 200
        
    elif request.method == 'POST':
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "No data provided"}), 400

            is_valid, error_message, converted_data = validate_and_convert_data(data)
            if not is_valid:
                return jsonify({"error": error_message}), 400

            data_type = data.get('data_type', 'stock')
            logging.info(f"Received valid {data_type} data: {converted_data}")
            
            # Broadcast the validated data to all connected clients
            socketio.emit(f'data_{data_type}', converted_data)
            
            return jsonify({
                "status": "success",
                "message": f"Successfully ingested and broadcast {data_type} data",
                "data": converted_data
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
    
    # Start the server with SocketIO
    logging.info("Starting ingestion server with WebSocket support...")
    socketio.run(app, host='0.0.0.0', port=5000)
