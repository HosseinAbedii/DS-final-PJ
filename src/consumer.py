from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
from kafka import KafkaConsumer
import json
import logging
import sys
from datetime import datetime
from flask import Flask
from flask_socketio import SocketIO
from flask_cors import CORS
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
KAFKA_TOPIC = 'financial_data'

def create_kafka_consumer():
    """Create and return a Kafka consumer instance"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='financial_data_consumer_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info("Kafka consumer created successfully")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        sys.exit(1)

def process_message(message):
    """Process each message from Kafka"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"[{timestamp}] Received message: {message.value}")
        # Add your data processing logic here
        return message.value
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

def start_consuming():
    """Start consuming messages from Kafka"""
    consumer = create_kafka_consumer()
    logger.info(f"Starting to consume messages from topic: {KAFKA_TOPIC}")
    
    try:
        for message in consumer:
            processed_data = process_message(message)
            if processed_data:
                # Add your storage or further processing logic here
                pass
    except KeyboardInterrupt:
        logger.info("Gracefully shutting down consumer...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        consumer.close()
        logger.info("Consumer closed")

# Define the schema for the incoming data : this change for git bug 
schema = StructType([
    StructField("stock_symbol", StringType(), True),
    StructField("opening_price", FloatType(), True),
    StructField("closing_price", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", LongType(), True),
    StructField("price", FloatType(), True)
])

# Initialize Spark session with cluster configuration
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "512m") \
    .config("spark.cores.max", "2") \
    .config("spark.driver.host", "spark-consumer") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.kubernetes.namespace", "default") \
    .config("spark.kafka.consumer.cache.enabled", "false") \
    .getOrCreate()

# Initialize Flask and SocketIO
app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

def foreach_batch_function(df, epoch_id):
    print(f"\n=== Batch {epoch_id} ===")
    records = df.collect()
    for record in records:
        # Convert record to dictionary
        data = record.asDict()
        # Broadcast to WebSocket clients
        socketio.emit('stock_update', data)
    print("Number of records:", len(records))
    df.show(truncate=False)

if __name__ == "__main__":
    print("Starting Spark Streaming application...")
    print("Connecting to Spark Master:", spark.sparkContext.master)
    
    try:
        # Start Flask-SocketIO server in a separate thread
        socketio_thread = threading.Thread(target=lambda: socketio.run(app, host='0.0.0.0', port=6001))
        socketio_thread.daemon = True
        socketio_thread.start()

        # Start Spark streaming with explicit configuration
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka.default.svc.cluster.local:9092") \
            .option("subscribe", "financial_data") \
            .option("startingOffsets", "latest") \
            .option("kafka.security.protocol", "PLAINTEXT") \
            .option("failOnDataLoss", "false") \
            .load()

        # Parse JSON data
        df = df.selectExpr("CAST(value AS STRING)")
        parsed_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

        # Write stream to console with more detailed configuration
        query = parsed_df.writeStream \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .foreachBatch(foreach_batch_function) \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()

        print("Streaming query started successfully")
        query.awaitTermination()
    except Exception as e:
        print("Error in streaming application:", str(e))
        raise
