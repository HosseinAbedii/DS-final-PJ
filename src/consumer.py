from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
import websockets
import asyncio
import json
import redis
from datetime import datetime, timedelta

# Redis connection
redis_client = redis.Redis(host='redis', port=6379, db=0)

# WebSocket server
WEBSOCKET_PORT = 8765
connected_clients = set()

async def register(websocket):
    connected_clients.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)

async def broadcast_message(message):
    if connected_clients:
        await asyncio.gather(
            *[client.send(json.dumps(message)) for client in connected_clients]
        )

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

# Function to write each batch of data to the log file
def foreach_batch_function(df, epoch_id):
    # Convert batch to list of dictionaries
    data_list = df.select("*").toPandas().to_dict('records')
    
    # Counter for notifications
    batch_count = 0
    
    for record in data_list:
        # Add timestamp
        record['processed_time'] = datetime.now().isoformat()
        
        # Store in Redis with 24h expiration
        key = f"financial_data:{record['timestamp']}"
        redis_client.setex(key, timedelta(days=1), json.dumps(record))
        
        batch_count += 1
        if batch_count % 10 == 0:
            notification = {
                'type': 'notification',
                'message': f'Processed {batch_count} records',
                'timestamp': datetime.now().isoformat()
            }
            redis_client.setex(
                f"notification:{datetime.now().timestamp()}", 
                timedelta(days=1), 
                json.dumps(notification)
            )
            
        # Broadcast via WebSocket
        asyncio.create_task(broadcast_message(record))

# Start WebSocket server
async def start_websocket_server():
    async with websockets.serve(register, "0.0.0.0", WEBSOCKET_PORT):
        await asyncio.Future()  # run forever

# Main function
if __name__ == "__main__":
    # Start WebSocket server in the background
    asyncio.get_event_loop().create_task(start_websocket_server())
    
    # Start Spark streaming
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "financial_data") \
        .load()

    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    query = df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .start()

    query.awaitTermination()
