from flask import Flask, jsonify, render_template, send_from_directory, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import redis
import json
import socketio
import threading
import time
import os
from datetime import datetime, timedelta

# Kubernetes environment configuration
API_PORT = int(os.getenv('API_PORT', 5001))
API_HOST = os.getenv('API_HOST', '0.0.0.0')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis.default.svc.cluster.local')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
CONSUMER_SERVICE = os.getenv('CONSUMER_SERVICE', 'spark-consumer-service')
CONSUMER_PORT = int(os.getenv('CONSUMER_PORT', 6001))
CONSUMER_URL = os.getenv('CONSUMER_URL', 'http://spark-consumer-service.default.svc.cluster.local:6001')

app = Flask(__name__, 
    static_folder='/app/static',  # Update static folder path
    template_folder='/app/templates'  # Update template folder path
)
CORS(app)
socket_app = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

# Redis configuration with retry mechanism
def get_redis_client():
    while True:
        try:
            client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                decode_responses=True,
                socket_timeout=5,
                retry_on_timeout=True
            )
            client.ping()  # Test the connection
            print(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            return client
        except redis.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}. Retrying in 5 seconds...")
            time.sleep(5)

redis_client = get_redis_client()
REDIS_KEY = "stock_data"
MAX_STORED_RECORDS = 1000

# Consumer WebSocket client
sio = socketio.Client()

def store_in_redis(data):
    """Store data in Redis with timestamp as score"""
    try:
        timestamp = datetime.now().timestamp()
        data['timestamp'] = timestamp
        # Store in sorted set
        redis_client.zadd(REDIS_KEY, {json.dumps(data): timestamp})
        # Trim old data
        redis_client.zremrangebyrank(REDIS_KEY, 0, -MAX_STORED_RECORDS-1)
        print(f"Data successfully stored in Redis at timestamp {timestamp}")
        return True
    except Exception as e:
        print(f"Redis store error: {e}")
        return False

@app.route('/api/historical-data')
def get_historical_data():
    """Get historical data from Redis"""
    try:
        # Get all data from Redis sorted set
        data = redis_client.zrange(REDIS_KEY, 0, -1, withscores=True)
        result = []
        for item, score in data:
            record = json.loads(item)
            record['timestamp'] = score
            result.append(record)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/debug/redis-stats')
def get_redis_stats():
    """Debug endpoint to check Redis data statistics"""
    try:
        stats = {
            'total_records': redis_client.zcard(REDIS_KEY),
            'latest_timestamp': redis_client.zrange(REDIS_KEY, -1, -1, withscores=True),
            'first_record': redis_client.zrange(REDIS_KEY, 0, 0, withscores=True),
            'last_record': redis_client.zrange(REDIS_KEY, -1, -1, withscores=True)
        }
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/debug/last-records/<int:count>')
def get_last_records(count):
    """Debug endpoint to get last N records"""
    try:
        data = redis_client.zrange(REDIS_KEY, -count, -1, withscores=True)
        result = []
        for item, score in data:
            record = json.loads(item)
            record['timestamp'] = score
            result.append(record)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Add new route to serve index.html
@app.route('/')
def index():
    try:
        return render_template('index.html')
    except Exception as e:
        print(f"Error serving index.html: {e}")
        return f"Error loading template: {str(e)}", 500

# Add a route for debugging template paths
@app.route('/debug/paths')
def debug_paths():
    return {
        'template_folder': app.template_folder,
        'static_folder': app.static_folder,
        'root_path': app.root_path,
        'templates_list': os.listdir(app.template_folder) if os.path.exists(app.template_folder) else []
    }

# Add new endpoint for fetching live data
@app.route('/api/live-data')
def get_live_data():
    """Get most recent data from Redis"""
    """Get most recent data from Redis"""
    try:
        # Get last 50 records from Redis sorted set
        data = redis_client.zrange(REDIS_KEY, -50, -1, withscores=True)
        result = []
        for item, score in data:
            record = json.loads(item)
            record['timestamp'] = score
            result.append(record)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Add endpoint for fetching trading signals
@app.route('/api/trading-signals')
def get_trading_signals():
    """Get trading signals from Redis"""
    try:
        # Get signals from Redis (assuming they're stored with a different key)
        signals = redis_client.zrange(REDIS_KEY + "_signals", -50, -1, withscores=True)
        result = []
        for item, score in signals:
            record = json.loads(item)
            record['timestamp'] = score
            result.append(record)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@sio.on('connect')
def on_connect():
    print('Connected to consumer WebSocket')

@sio.on('stock_update')
def on_stock_update(data):
    # Store in Redis
    store_in_redis(data)
    # Forward to connected clients
    socket_app.emit('live_stock_update', data)

@sio.on('trading_signal')
def on_trading_signal(data):
    # Store signal in Redis
    store_in_redis(data)
    # Forward signal to connected clients
    socket_app.emit('live_trading_signal', data)

def connect_to_consumer():
    while True:
        try:
            consumer_url = f"http://{CONSUMER_SERVICE}:{CONSUMER_PORT}"
            print(f"Attempting to connect to consumer at: {consumer_url}")
            sio.connect(consumer_url)
            print(f"Successfully connected to consumer WebSocket")
            break
        except Exception as e:
            print(f"Connection error: {e}, retrying in 5 seconds...")
            time.sleep(5)

if __name__ == '__main__':
    print(f"Starting API server on {API_HOST}:{API_PORT}")
    print(f"Redis host: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Consumer service: {CONSUMER_SERVICE}:{CONSUMER_PORT}")
    
    consumer_thread = threading.Thread(target=connect_to_consumer)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    socket_app.run(app, 
        host=API_HOST, 
        port=API_PORT, 
        debug=False,  # Disable debug in production
        use_reloader=False  # Disable reloader in production
    )