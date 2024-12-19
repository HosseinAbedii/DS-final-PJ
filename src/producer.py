import time
import random
import json
import numpy as np
from kafka import KafkaProducer

# Kafka configuration
KAFKA_TOPIC = 'financial_data'
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']

# Stock symbols
STOCKS = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA", "META", "NFLX", "NVDA"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

def generate_market_data():
    stock = random.choice(STOCKS)
    base_price = random.uniform(100, 1000)
    
    data = {
        "stock_symbol": stock,
        "price": base_price,
        "volume": random.randint(1000, 100000),
        "trade_type": random.choice(["buy", "sell"]),
        "timestamp": time.time(),
        "market_cap": random.uniform(1e9, 1e12),
        "volatility": random.uniform(0.1, 0.5)
    }
    return data

def main():
    print("Starting producer... sending to topic:", KAFKA_TOPIC)
    while True:
        try:
            data = generate_market_data()
            producer.send(KAFKA_TOPIC, value=data)
            print(f"Sent data: {data['stock_symbol']} - ${data['price']:.2f}")
            time.sleep(random.uniform(0.5, 2))
        except Exception as e:
            print(f"Error sending message: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
