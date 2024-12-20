from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd
from collections import deque

# Configuration
KAFKA_TOPIC = "financial_data"
KAFKA_OUTPUT_TOPIC = "stock_indicators"
BROKER = "localhost:9092"
MA_PERIOD = 12
EMA_PERIOD = 12
RSI_PERIOD = 12

# Initialize Kafka Consumer and Producer
consumer = KafkaConsumer(
    KAFKA_TOPIC, 
    bootstrap_servers=BROKER, 
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers=BROKER, 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# State storage for calculations
price_data = {}

def calculate_indicators(symbol, price):
    if symbol not in price_data:
        price_data[symbol] = {
            "prices": deque(maxlen=RSI_PERIOD),
            "ema": None,
            "gains": deque(maxlen=RSI_PERIOD),
            "losses": deque(maxlen=RSI_PERIOD)
        }
    
    data = price_data[symbol]
    prices = data["prices"]
    prices.append(price)

    # Moving Average
    ma = sum(prices) / len(prices) if len(prices) > 0 else None

    # EMA
    if data["ema"] is None:
        data["ema"] = price  # Initialize EMA with the first price
    else:
        alpha = 2 / (EMA_PERIOD + 1)
        data["ema"] = (price * alpha) + (data["ema"] * (1 - alpha))
    ema = data["ema"]

    # RSI
    if len(prices) > 1:
        change = price - prices[-2]
        gain = max(change, 0)
        loss = -min(change, 0)
        data["gains"].append(gain)
        data["losses"].append(loss)

        avg_gain = sum(data["gains"]) / len(data["gains"])
        avg_loss = sum(data["losses"]) / len(data["losses"])
        rs = avg_gain / avg_loss if avg_loss != 0 else 0
        rsi = 100 - (100 / (1 + rs))
    else:
        rsi = None

    return {"ma": ma, "ema": ema, "rsi": rsi}

# Consume and process messages
for message in consumer:
    stock = message.value
    symbol = stock["stock_symbol"]
    closing_price = stock["closing_price"]

    # Calculate indicators
    indicators = calculate_indicators(symbol, closing_price)

    # Prepare the result
    result = {
        "stock_symbol": symbol,
        "timestamp": stock["timestamp"],
        "closing_price": closing_price,
        "ma": indicators["ma"],
        "ema": indicators["ema"],
        "rsi": indicators["rsi"]
    }

    # Publish to output topic
    producer.send(KAFKA_OUTPUT_TOPIC, result)
    print(f"Processed: {result}")
