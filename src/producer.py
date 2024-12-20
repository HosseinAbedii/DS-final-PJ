import time
import random
import json
import numpy as np
import threading
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# Kafka configuration
KAFKA_TOPIC = 'financial_data'
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']

# Stock symbols
STOCKS = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    retry_backoff_ms=1000,
    acks='all'
)

def create_kafka_topic():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='financial-data-producer'
        )
        topic_list = [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Successfully created topic: {KAFKA_TOPIC}")
    except Exception as e:
        if "TopicAlreadyExistsError" in str(e):
            print(f"Topic {KAFKA_TOPIC} already exists")
        else:
            print(f"Error creating topic: {e}")
    finally:
        if admin_client:
            admin_client.close()

def generate_market_data():
    stock_symbol = random.choice(STOCKS)
    prev_price = 1000
    dt = 1
    mu = 0.0002
    sigma = 0.01

    price_change = np.exp((mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * np.random.normal())
    opening_price = max(0, prev_price * price_change)
    closing_price = max(0, opening_price + round(random.normalvariate(0, 10), 2))
    high = max(opening_price, closing_price) + round(abs(random.normalvariate(0, 5)), 2)
    low = min(opening_price, closing_price) - round(abs(random.normalvariate(0, 5)), 2)
    volume = max(0, int(np.random.poisson(5000) * (1 + 0.1 * np.random.normal())))

    return {
        "stock_symbol": stock_symbol,
        "opening_price": opening_price,
        "closing_price": closing_price,
        "high": high,
        "low": low,
        "volume": volume,
        "timestamp": time.time()
    }

def generate_additional_data():
    stock_symbol = random.choice(STOCKS)
    timestamp = time.time()
    data_types = ['order_book', 'news_sentiment', 'market_data', 'economic_indicator']
    data_type = random.choice(data_types)

    if data_type == 'order_book':
        return {
            "data_type": "order_book",
            "timestamp": timestamp,
            "stock_symbol": stock_symbol,
            "order_type": random.choice(['buy', 'sell']),
            "price": random.uniform(100, 1000),
            "quantity": random.randint(1, 100)
        }
    elif data_type == 'news_sentiment':
        return {
            "data_type": "news_sentiment",
            "timestamp": timestamp,
            "stock_symbol": stock_symbol,
            "sentiment_score": random.uniform(-1, 1),
            "sentiment_magnitude": random.uniform(0, 1)
        }

def send_data(data):
    try:
        future = producer.send(KAFKA_TOPIC, value=data)
        future.get(timeout=10)
        print(f"Sent {data.get('data_type', 'market')} data for {data['stock_symbol']}")
    except Exception as e:
        print(f"Error sending message: {e}")

def send_additional_data():
    while True:
        data = generate_additional_data()
        send_data(data)
        time.sleep(random.uniform(1, 5))

def main():
    print("Starting producer... creating topic:", KAFKA_TOPIC)
    create_kafka_topic()
    print("Starting to send messages...")
    
    # Start additional data thread
    threading.Thread(target=send_additional_data, daemon=True).start()
    
    # Main market data loop
    while True:
        try:
            data = generate_market_data()
            send_data(data)
            time.sleep(random.uniform(1, 5))
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
