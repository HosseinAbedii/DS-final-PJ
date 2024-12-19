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
    StructField("data_type", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sentiment_score", FloatType(), True),
    StructField("sentiment_magnitude", FloatType(), True),
    StructField("market_cap", FloatType(), True),
    StructField("pe_ratio", FloatType(), True),
    StructField("indicator_name", StringType(), True),
    StructField("value", FloatType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

# Read data from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "financial_data") \
    .load()

# Convert the value column from binary to string
df = df.selectExpr("CAST(value AS STRING)")

# Parse the JSON data and apply the schema
df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Print the data to the terminal
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
