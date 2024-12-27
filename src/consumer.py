from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
from flask import Flask
from flask_socketio import SocketIO
import threading
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

# Initialize Flask and SocketIO with logging
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", logger=True)

@socketio.on('connect')
def handle_connect():
    logger.info('Client connected to WebSocket')

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected from WebSocket')

def foreach_batch_function(df, epoch_id):
    try:
        logger.info(f"\n=== Processing batch {epoch_id} at {datetime.now()} ===")
        print(f"\n=== Batch {epoch_id} ===")
        df.show(truncate=False)
        
        # Add WebSocket broadcast with logging
        rows = df.toJSON().collect()
        for row in rows:
            data = json.loads(row)
            try:
                socketio.emit('stock_update', data)
                logger.info(f"Successfully broadcasted data: {data['stock_symbol']} - ${data['price']}")
            except Exception as e:
                logger.error(f"Failed to broadcast data: {e}")
        
        logger.info(f"Processed {len(rows)} records in batch {epoch_id}")
    except Exception as e:
        logger.error(f"Error processing batch {epoch_id}: {e}")

if __name__ == "__main__":
    logger.info("Starting Spark Streaming and WebSocket server...")
    
    try:
        # Start WebSocket server thread with logging
        websocket_thread = threading.Thread(
            target=lambda: socketio.run(
                app, 
                host='0.0.0.0', 
                port=6001, 
                debug=False, 
                use_reloader=False
            )
        )
        websocket_thread.daemon = True
        websocket_thread.start()
        logger.info("WebSocket server started on port 6001")
        
        # Start Spark streaming
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "financial_data") \
            .load()

        # Parse JSON data
        df = df.selectExpr("CAST(value AS STRING)")
        df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

        # Write stream to console
        query = df.writeStream \
            .outputMode("append") \
            .foreachBatch(foreach_batch_function) \
            .start()

        logger.info("Spark Streaming started. Waiting for data...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in main application: {e}")
        raise
