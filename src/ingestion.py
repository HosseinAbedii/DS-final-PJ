from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic  
import socketio
import json
import logging
import sys
import time
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
kafka_bootstrap_servers = ['kafka:9092']
kafka_topic = 'financial_data'

# Socket.IO configuration with all transports enabled
sio = socketio.Client(
    reconnection=True,
    reconnection_attempts=10,
    reconnection_delay=1,
    reconnection_delay_max=5,
    logger=logger,
    engineio_logger=True
)

# Get server URL from environment or use default
SERVER_URL = os.getenv('SERVER_URL', 'http://192.168.220.128:5000')

def create_kafka_topic():
    """Create Kafka topic if it doesn't exist"""
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

def forward_to_kafka(data):
    """Forward stock data to Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.send(kafka_topic, value=data)
        producer.flush()
        logger.info(f"Forwarded stock data to Kafka: {data}")
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")

@sio.event
def connect():
    logger.info(f"Connected to server at {SERVER_URL}")

@sio.event
def connect_error(error):
    logger.error(f"Connection error: {error}")

@sio.event
def disconnect():
    logger.info("Disconnected from server")

@sio.on('data_stock')
def on_stock_data(data):
    """Handle incoming stock data from WebSocket"""
    logger.info(f"Received stock data: {data}")
    forward_to_kafka(data)

def start_listening():
    """Start listening to WebSocket events with retry"""
    retry_count = 0
    max_retries = 30
    
    while retry_count < max_retries:
        try:
            logger.info(f"Attempting to connect to {SERVER_URL} (attempt {retry_count + 1}/{max_retries})")
            create_kafka_topic()
            sio.connect(SERVER_URL, transports=['websocket', 'polling'])
            sio.wait()
            break
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            retry_count += 1
            if retry_count == max_retries:
                logger.error("Max retries reached. Exiting.")
                sys.exit(1)
            time.sleep(10)  # Wait 10 seconds before retry

if __name__ == '__main__':
    logger.info("Starting stock data ingestion service...")
    try:
        start_listening()
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        sio.disconnect()
