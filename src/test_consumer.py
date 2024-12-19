from kafka import KafkaConsumer
import json

# Create a consumer instance
consumer = KafkaConsumer(
    'financial_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Consume messages
print("Starting consumer... Waiting for messages...")
try:
    for message in consumer:
        print(f"Received message: {json.dumps(message.value, indent=2)}")
except KeyboardInterrupt:
    print("Stopping consumer...")
    consumer.close()
