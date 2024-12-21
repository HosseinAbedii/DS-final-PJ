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
            "value": random.uniform(-5, 5)
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
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error: {e}. Is the server running at {api_endpoint}?")
    except requests.exceptions.Timeout:
        print(f"Request timed out while sending data to {api_endpoint}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending data: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


def send_additional_data():
    while True:
        data = generate_additional_data()
        send_data(data)
        time.sleep(random.uniform(1, 5))


if __name__ == "__main__":
    threading.Thread(target=send_additional_data, daemon=True).start()
    while True:
        data = generate_data()
        send_data(data)
        time.sleep(random.uniform(1, 5))