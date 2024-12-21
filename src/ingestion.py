from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaAdminClient, NewTopic
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Kafka configuration
kafka_bootstrap_servers = ['kafka:9092']

# Update Kafka configuration with stock data going to financial_data
TOPICS = {
    'stock': 'financial_data',  # Stock data goes to main financial_data topic
    'order_book': 'financial_data_order_book',
    'news_sentiment': 'financial_data_sentiment',
    'market_data': 'financial_data_market',
    'economic_indicator': 'financial_data_economic'
}

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    acks='all'
)

def create_kafka_topics():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_bootstrap_servers,
            client_id='financial-data-ingestion'
        )
        topic_list = [
            NewTopic(name=topic, num_partitions=1, replication_factor=1)
            for topic in TOPICS.values()
        ]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(f"Created Kafka topics: {list(TOPICS.values())}")
    except Exception as e:
        if "TopicAlreadyExistsError" in str(e):
            logger.info("Topics already exist")
        else:
            logger.error(f"Error creating topics: {e}")

@app.route('/ingest', methods=['POST'])
def ingest_data():
    try:
        data = request.json
        data_type = data.get('data_type', 'stock')
        
        if data_type not in TOPICS:
            return jsonify({"error": f"Unsupported data type: {data_type}"}), 400
            
        # Send to appropriate Kafka topic
        topic = TOPICS[data_type]
        future = producer.send(topic, value=data)
        future.get(timeout=10)
        
        logger.info(f"Ingested {data_type} data to Kafka topic {topic}")
        return jsonify({"status": "success"}), 200
        
    except Exception as e:
        logger.error(f"Error ingesting data: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health')
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    # Create Kafka topics on startup
    create_kafka_topics()
    # Start Flask app
    app.run(host='0.0.0.0', port=5000)
