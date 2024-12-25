from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import logging
import time
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Kafka configuration
kafka_bootstrap_servers = ['kafka:9092']
kafka_topic = 'financial_data'
 # Initialize Kafka producer
def create_producer():
    retries = 0
    max_retries = 30
    while retries < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Successfully connected to Kafka")
            return producer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            retries += 1
            time.sleep(10)
    return None

def create_kafka_topic():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_bootstrap_servers,
            client_id='financial-data-ingestion'
        )
        topic_list = [NewTopic(name=kafka_topic, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(f"Created Kafka topic: {kafka_topic}")
    except Exception as e:
        if "TopicAlreadyExistsError" in str(e):
            logger.info(f"Topic {kafka_topic} already exists")
        else:
            logger.error(f"Error creating topic: {e}")

@app.route('/ingest', methods=['POST'])
def ingest_data():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # Send to Kafka
        producer.send(kafka_topic, value=data)
        producer.flush()
        logger.info(f"Forwarded data to Kafka: {data}")
        return jsonify({"status": "success"}), 200

    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/health')
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    # Create topic and initialize producer
    create_kafka_topic()
    producer = create_producer()
    
    if not producer:
        logger.error("Failed to connect to Kafka. Exiting.")
        exit(1)

    # Start Flask app
    app.run(host='0.0.0.0', port=5000)
