import time
import random
import requests
import json
import psutil
import os
import numpy as np
import threading
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# Set process affinity to a single core
p = psutil.Process(os.getpid())
p.cpu_affinity([0])  # Pin to the first core.

# List of sample stock symbols
stocks = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]

# API endpoint to send the generated data
api_endpoint = "http://localhost:5000/ingest"

# Kafka configuration
kafka_topic = "financial_data"
kafka_bootstrap_servers = ["kafka:9092"]  # Changed to use K8s service name

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,  # Add retries for robustness
    retry_backoff_ms=1000
) 

# Create Kafka topic if it doesn't exist
admin_client = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers, client_id='test')
topic_list = [NewTopic(name=kafka_topic, num_partitions=1, replication_factor=1)]
try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
except Exception as e:
    print(f"Topic creation failed: {e}")

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
        producer.send(kafka_topic, value=data)
        producer.flush()
    except Exception as e:
        print(f"Error occurred: {e}")


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
