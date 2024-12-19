from kafka import KafkaConsumer
import json
import sys
import time

def create_consumer(retries=5):
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                'financial_data',
                bootstrap_servers=['localhost:9092'],  # Make sure you've port-forwarded kafka
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='test-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000  # Wait 1 second for messages
            )
            return consumer
        except Exception as e:
            print(f"Attempt {attempt + 1}/{retries}: Failed to connect to Kafka: {e}")
            if attempt < retries - 1:
                print("Retrying in 5 seconds...")
                time.sleep(5)
            else:
                print("Failed to connect to Kafka after multiple attempts")
                print("Make sure you've port-forwarded the Kafka service using:")
                print("kubectl port-forward service/kafka 9092:9092")
                sys.exit(1)

def main():
    print("Starting consumer... Waiting for messages...")
    print("Make sure Kafka is port-forwarded: kubectl port-forward service/kafka 9092:9092")
    
    consumer = create_consumer()
    
    try:
        while True:
            try:
                messages = consumer.poll(timeout_ms=1000)
                for tp, msgs in messages.items():
                    for msg in msgs:
                        print(f"Received message: {json.dumps(msg.value, indent=2)}")
            except Exception as e:
                print(f"Error reading message: {e}")
                time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
