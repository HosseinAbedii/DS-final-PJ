from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaAdminClient, NewTopic
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Kafka configuration
kafka_topic = "financial_data"
kafka_bootstrap_servers = ['kafka:9092']

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    acks='all'
)

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
        # Send to Kafka
        future = producer.send(kafka_topic, value=data)
        future.get(timeout=10)  # Wait for confirmation
        logger.info(f"Successfully ingested data for {data.get('stock_symbol', 'unknown')}")
        return jsonify({"status": "success"}), 200
    except Exception as e:
        logger.error(f"Error ingesting data: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health')
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    # Create Kafka topic on startup
    create_kafka_topic()
    # Start Flask app
    app.run(host='0.0.0.0', port=5000)
