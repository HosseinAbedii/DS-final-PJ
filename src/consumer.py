from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
from flask import Flask
from flask_socketio import SocketIO
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask and SocketIO
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Define the schema for the incoming data
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

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", "spark-consumer") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

def process_batch(df, epoch_id):
    """Process each batch and broadcast via WebSocket"""
    try:
        print(f"\n=== Batch {epoch_id} ===")
        
        # Convert DataFrame to list of dictionaries
        rows = df.toJSON().collect()
        for row in rows:
            # Parse JSON string to dict
            data = json.loads(row)
            # Emit to WebSocket clients
            socketio.emit('stock_update', data)
            print(f"Broadcasted: {data}")
            
    except Exception as e:
        logger.error(f"Error processing batch: {e}")

if __name__ == "__main__":
    logger.info("Starting Spark Streaming application...")
    
    try:
        # Start Flask-SocketIO server in a separate thread
        import threading
        server_thread = threading.Thread(
            target=lambda: socketio.run(
                app, 
                host='0.0.0.0', 
                port=6001, 
                debug=True,
                use_reloader=False
            )
        )
        server_thread.daemon = True
        server_thread.start()

        # Create streaming DataFrame from Kafka
        streaming_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka.default.svc.cluster.local:9092") \
            .option("subscribe", "financial_data") \
            .option("startingOffsets", "latest") \
            .load()

        # Parse JSON data
        parsed_df = streaming_df.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .select("data.*")

        # Process the stream
        query = parsed_df.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("update") \
            .start()

        logger.info("Streaming query started. Broadcasting on port 6001...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in streaming application: {e}")
        raise
