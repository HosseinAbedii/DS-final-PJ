from flask import Flask, jsonify, render_template, send_from_directory
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import redis
import json
import socketio
import threading
import time
import os
from datetime import datetime, timedelta

# Change these constants
API_PORT = 5001
API_HOST = '0.0.0.0'

# Redis configuration from environment variables
REDIS_HOST = os.getenv('REDIS_HOST', 'redis.default.svc.cluster.local')  # Use full DNS name
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

app = Flask(__name__, 
    static_folder='static',  # Add this line
    template_folder='templates'  # And this line
)
CORS(app)
socket_app = SocketIO(app, cors_allowed_origins="*")

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
    return render_template('index.html')

# Add new endpoint for fetching live data
@app.route('/api/live-data')
def get_live_data():
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
            # Use the service's cluster DNS name
            consumer_url = os.getenv('CONSUMER_URL', 'http://spark-consumer-service.default.svc.cluster.local:6001')
            print(f"Attempting to connect to consumer at: {consumer_url}")
            sio.connect(consumer_url)
            print(f"Successfully connected to consumer WebSocket")
            break
        except Exception as e:
            print(f"Connection error: {e}, retrying in 5 seconds...")
            time.sleep(5)

if __name__ == '__main__':
    print(f"Starting API server on port {API_PORT}...")
    
    # Start consumer connection in background
    consumer_thread = threading.Thread(target=connect_to_consumer)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    # Start Flask-SocketIO on new port
    socket_app.run(app, 
        host=API_HOST, 
        port=API_PORT, 
        debug=True,
        allow_unsafe_werkzeug=True
    )

