import time
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import psutil
import os
import numpy as np
import threading
import signal
import sys
from datetime import datetime

# Set process affinity to a single core
p = psutil.Process(os.getpid())
p.cpu_affinity([0])  # Pin to the first core.

# List of sample stock symbols
stocks = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]

# API endpoint to send the generated data
api_endpoint = "http://localhost:5000/ingest"

# Configure retry strategy
retry_strategy = Retry(
    total=3,  # number of retries
    backoff_factor=0.5,  # wait 0.5s * (2 ** retry) between retries
    status_forcelist=[500, 502, 503, 504]  # HTTP status codes to retry on
)

# Create session with retry strategy
session = requests.Session()
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)


def check_server():
    """Check if the server is available"""
    print(f"Checking server availability at {api_endpoint}...")
    try:
        response = session.get(api_endpoint.replace('/ingest', '/health'), timeout=2)
        return response.status_code == 200
    except:
        return False

def wait_for_server(max_attempts=5):
    """Wait for server to become available with exponential backoff"""
    attempt = 0
    while attempt < max_attempts:
        if check_server():
            print("Server is available!")
            return True
        wait_time = 2 ** attempt
        print(f"Server not available. Retrying in {wait_time} seconds... ({attempt + 1}/{max_attempts})")
        time.sleep(wait_time)
        attempt += 1
    return False

def graceful_shutdown(signum, frame):
    """Handle graceful shutdown on CTRL+C"""
    print("\nShutting down gracefully...")
    sys.exit(0)


def generate_data():
    stock_symbol = random.choice(stocks)
    prev_price = 1000  # Assume some initial price
    dt = 1  # Time step, e.g., 1 day
    mu = 0.0002  # Drift (average daily return)
    sigma = 0.01  # Volatility (standard deviation of daily return)

    price_change = np.exp((mu - 0.5 * sigma**2) * dt +
                          sigma * np.sqrt(dt) * np.random.normal())
    opening_price = max(0, prev_price * price_change)
    closing_price = max(0, opening_price +
                        round(random.normalvariate(0, 10), 2))
    high = max(opening_price, closing_price) + \
        round(abs(random.normalvariate(0, 5)), 2)
    low = min(opening_price, closing_price) - \
        round(abs(random.normalvariate(0, 5)), 2)
    volume = max(0, int(np.random.poisson(5000) *
                 (1 + 0.1 * np.random.normal())))

    data = {
        "stock_symbol": stock_symbol,
        "opening_price": opening_price,
        "closing_price": closing_price,
        "high": high,
        "low": low,
        "volume": volume,
        "timestamp": time.time()
    }
    return data


def generate_additional_data():
    stock_symbol = random.choice(stocks)
    timestamp = time.time()
    data_types = ['order_book', 'news_sentiment',
                  'market_data', 'economic_indicator']
    data_type = random.choice(data_types)

    if data_type == 'order_book':
        data = {
            "data_type": "order_book",
            "timestamp": timestamp,
            "stock_symbol": stock_symbol,
            "order_type": random.choice(['buy', 'sell']),
            "price": random.uniform(100, 1000),
            "quantity": random.randint(1, 100)
        }
    elif data_type == 'news_sentiment':
        data = {
            "data_type": "news_sentiment",
            "timestamp": timestamp,
            "stock_symbol": stock_symbol,
            "sentiment_score": random.uniform(-1, 1),
            "sentiment_magnitude": random.uniform(0, 1)
        }
    elif data_type == 'market_data':
        data = {
            "data_type": "market_data",
            "timestamp": timestamp,
            "stock_symbol": stock_symbol,
            "market_cap": random.uniform(1e9, 1e12),
            "pe_ratio": random.uniform(5, 30)
        }
    elif data_type == 'economic_indicator':
        data = {
            "data_type": "economic_indicator",
            "timestamp": timestamp,
            "indicator_name": "GDP Growth Rate",
            "value": random.uniform(-5, 5),
            "timestamp": timestamp
        }
    return data


def send_data(data):
    try:
        response = session.post(
            api_endpoint,
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=5  # 5 seconds timeout
        )
        response.raise_for_status()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Successfully sent {data.get('data_type', 'stock')} data")
    except requests.exceptions.RequestException as e:
        # Only print error once per minute to avoid spam
        current_time = time.time()
        if not hasattr(send_data, 'last_error_time') or current_time - send_data.last_error_time > 60:
            print(f"Connection error: {e}")
            send_data.last_error_time = current_time


def send_additional_data():
    while True:
        data = generate_additional_data()
        send_data(data)
        time.sleep(random.uniform(1, 5))


if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    
    if not wait_for_server():
        print("Could not connect to server. Exiting.")
        sys.exit(1)
        
    print("Starting data generation...")
    threading.Thread(target=send_additional_data, daemon=True).start()
    
    while True:
        try:
            data = generate_data()
            send_data(data)
            time.sleep(random.uniform(1, 5))
        except KeyboardInterrupt:
            break