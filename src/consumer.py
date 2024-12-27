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

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

def foreach_batch_function(df, epoch_id):
    # Print the batch to terminal
    print(f"\n=== Batch {epoch_id} ===")
    df.show(truncate=False)

if __name__ == "__main__":
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

    query.awaitTermination()
