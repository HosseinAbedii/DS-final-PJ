from flask import Flask, jsonify, request
import redis
import json
from datetime import datetime, timedelta

app = Flask(__name__)
redis_client = redis.Redis(host='redis', port=6379, db=0)

@app.route('/')
def home():
    return app.send_static_file('index.html')

@app.route('/api/historical-data')
def get_historical_data():
    hours = int(request.args.get('hours', 24))
    current_time = datetime.now()
    start_time = current_time - timedelta(hours=hours)
    
    # Get all keys from Redis
    all_keys = redis_client.keys('financial_data:*')
    data = []
    
    for key in all_keys:
        try:
            record = json.loads(redis_client.get(key))
            record_time = datetime.fromtimestamp(record['timestamp']/1000)
            if record_time >= start_time:
                data.append(record)
        except:
            continue
    
    # Sort by timestamp
    data.sort(key=lambda x: x['timestamp'], reverse=True)
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
