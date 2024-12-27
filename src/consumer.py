from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add these constants at the top
KAFKA_TOPIC = 'financial_data'
KAFKA_BOOTSTRAP_SERVERS = 'kafka.default.svc.cluster.local:9092'

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

# Initialize Spark session with cluster configuration
spark = SparkSession.builder \
    .appName("StockDataConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "512m") \
    .config("spark.cores.max", "2") \
    .config("spark.driver.host", "spark-consumer") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

def process_batch(df, epoch_id):
    """Process each batch of data"""
    try:
        print("\n" + "="*80)
        print(f"Batch Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Batch ID: {epoch_id}")
        print(f"Processing data from topic: {KAFKA_TOPIC}")
        print("="*80)

        if df.isEmpty():
            print("No data received in this batch")
            return

        if not df.isEmpty():
            # Show detailed stock information
            print("\nDetailed Stock Information:")
            df.select(
                to_timestamp(col("timestamp")).alias("time"),
                "stock_symbol",
                "price",
                "opening_price",
                "closing_price",
                "high",
                "low",
                "volume"
            ).show(truncate=False)

            # Calculate statistics
            stats = df.groupBy("stock_symbol").agg({
                "price": "avg",
                "volume": "sum",
                "high": "max",
                "low": "min"
            })
            
            print("\nStock Statistics in this Batch:")
            stats.show(truncate=False)
            
            # Print total number of records
            count = df.count()
            print(f"\nTotal records in batch: {count}")
    except Exception as e:
        logger.error(f"Error processing batch: {e}")

if __name__ == "__main__":
    logger.info("Starting Spark Streaming application...")
    logger.info(f"Connecting to Kafka topic: {KAFKA_TOPIC}")
    logger.info(f"Using Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    try:
        # Create streaming DataFrame from Kafka with explicit topic configuration
        streaming_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 100) \
            .load()

        # Parse JSON data with more detailed error handling
        parsed_df = streaming_df.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .select("data.*")

        # Process the stream with checkpoint
        query = parsed_df.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("update") \
            .trigger(processingTime="5 seconds") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()

        logger.info("Streaming query started. Waiting for data...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in streaming application: {e}")
        raise
