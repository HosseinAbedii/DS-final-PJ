from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import from_json, col, avg, col, when, lag, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
from flask import Flask, jsonify
from flask_socketio import SocketIO
import threading
import json
import logging
import random
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Update schema without price and timestamp
schema = StructType([
    StructField("stock_symbol", StringType(), True),
    StructField("opening_price", FloatType(), True),
    StructField("closing_price", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", FloatType(), True),
])

# Add API endpoints
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
try:
    socketio = SocketIO(app, 
        cors_allowed_origins="*", 
        logger=True,
        async_mode='eventlet',
        ping_timeout=60,
        ping_interval=25
    )
except Exception as e:
    logger.warning(f"Falling back to default async mode: {e}")
    socketio = SocketIO(app, 
        cors_allowed_origins="*", 
        logger=True,
        ping_timeout=60,
        ping_interval=25
    )

@app.route('/')
def home():
    return jsonify({
        "status": "running",
        "message": "Stock Data WebSocket Server"
    })

@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "websocket_port": 6001,
        "connections": len(socketio.server.eio.clients)
    })

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

@socketio.on('connect')
def handle_connect():
    logger.info('Client connected to WebSocket')

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected from WebSocket')



def calculate_indicators(df, windows = 12):
    window_spec = Window.partitionBy("stock_symbol").orderBy("timestamp")

    # Calculate Moving Average (MA) over a 12-period window
    df = df.withColumn("MA", avg("closing_price").over(window_spec.rowsBetween(-(windows - 1), 0)))


    alpha = 2 / (windows + 1)

    # Initialize EMA with the first closing price (or set a default value)
    df = df.withColumn("EMA_init", lag("closing_price", 1).over(window_spec))

    # Fill null values in the EMA initialization for the first row
    df = df.withColumn("EMA", when(col("EMA_init").isNull(), col("closing_price"))
                       .otherwise(alpha) * col("closing_price") + (1 - alpha) * col("EMA_init")))

    # Drop the intermediate EMA initialization column
    df = df.drop("EMA_init")

    # Calculate Relative Strength Index (RSI)
    df = df.withColumn("change", col("closing_price") - lag("closing_price", 1).over(window_spec))
    df = df.withColumn("gain", when(col("change") > 0, col("change")).otherwise(0))
    df = df.withColumn("loss", when(col("change") < 0, -col("change")).otherwise(0))
    
    avg_gain = avg("gain").over(window_spec.rowsBetween(-(windows - 1), 0))
    avg_loss = avg("loss").over(window_spec.rowsBetween(-(windows - 1), 0))
    df = df.withColumn("RS", avg_gain / avg_loss)
    df = df.withColumn("RSI", 100 - (100 / (1 + col("RS"))))


    # Generate trading signals based on EMA and RSI
    df = df.withColumn("signal", when((col("EMA") > col("MA")) & (col("RSI") < 30), "BUY")
                              .when((col("EMA") < col("MA")) & (col("RSI") > 70), "SELL")
                              .otherwise("HOLD"))

    return df


def foreach_batch_function(df, epoch_id):
    try:
        logger.info(f"\n=== Processing batch {epoch_id} at {datetime.now()} ===")
        print(f"\n=== Batch {epoch_id} ===")
        df = calculate_indicators(df)
        df.show(truncate=False)
        
        rows = df.toJSON().collect()
        for row in rows:
            data = json.loads(row)
            try:
                # Create a formatted data object with current price
                formatted_data = {
                    'stock_symbol': data['stock_symbol'],
                    'current_price': data['closing_price'],  # Use closing price as current price
                    'opening_price': data['opening_price'],
                    'closing_price': data['closing_price'],
                    'MA': data.get('MA'),
                    'EMA': data.get('EMA'),
                    'RSI': data.get('RSI'),
                    'high': data['high'],
                    'low': data['low'],
                    'volume': data['volume'],
                    'signal': data.get('signal')
                }
                
                # Generate and emit trading signal
                # trading_signal = generate_trading_signal(formatted_data)
                # if trading_signal:
                socketio.emit('signal', formatted_data["signal"])
                    # logger.info(f"Trading Signal: {trading_signal['signal']} for {trading_signal['stock']}")
                
                # Emit regular stock update
                socketio.emit('stock_update', formatted_data)
                logger.info(f"Stock Update: {formatted_data['stock_symbol']} - ${formatted_data['current_price']}")
                logger.info(f"Stock Update: {formatted_data['stock_symbol']} - MA: {formatted_data['MA']}, EMA: {formatted_data['EMA']}, RSI: {formatted_data['RSI']}, Signal: {formatted_data['signal']}")
                
            except Exception as e:
                logger.error(f"Failed to broadcast data: {e}")
                logger.error(f"Raw data: {data}")
        
        logger.info(f"Processed {len(rows)} records in batch {epoch_id}")
    except Exception as e:
        logger.error(f"Error processing batch {epoch_id}: {e}")

if __name__ == "__main__":
    logger.info("Starting Spark Streaming and WebSocket server...")
    
    try:
        # Modified WebSocket server configuration
        websocket_thread = threading.Thread(
            target=lambda: socketio.run(
                app, 
                host='0.0.0.0', 
                port=6001,
                debug=True,  # Enable debug mode
                use_reloader=False,
                log_output=True,
                allow_unsafe_werkzeug=True
            )
        )
        websocket_thread.daemon = True
        websocket_thread.start()
        logger.info("WebSocket server started on port 6001 - Test at http://localhost:6001")
        
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
