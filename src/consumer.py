from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType

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
    .appName("KafkaConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "512m") \
    .config("spark.cores.max", "2") \
    .config("spark.driver.host", "spark-consumer") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.kubernetes.namespace", "default") \
    .config("spark.kafka.consumer.cache.enabled", "false") \
    .getOrCreate()

def foreach_batch_function(df, epoch_id):
    print(f"\n=== Batch {epoch_id} ===")
    print("Number of records:", df.count())
    df.show(truncate=False)

if __name__ == "__main__":
    print("Starting Spark Streaming application...")
    print("Connecting to Spark Master:", spark.sparkContext.master)
    
    try:
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
