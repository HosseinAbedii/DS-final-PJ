from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
from flask import Flask, jsonify
from flask_socketio import SocketIO
import threading
import json
import logging
import random
from datetime import datetime, timedelta
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Update schema to match the data structure
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

TRADE_SIGNALS = ['BUY', 'SELL', 'HOLD']
SIGNAL_PROBABILITY = 0.7  # 70% chance to generate a signal

def generate_trading_signal(price_data):
    """Generate random trading signal with reasoning"""
    if random.random() > SIGNAL_PROBABILITY:
        return None
        
    signal = random.choice(TRADE_SIGNALS)
    confidence = random.uniform(0.6, 0.95)
    
    return {
        'signal': signal,
        'confidence': confidence,
        'timestamp': datetime.now().isoformat(),
        'stock': price_data['stock_symbol'],
        'price': price_data['current_price'],
        'reason': f"Algorithm detected potential {signal.lower()} opportunity at ${price_data['current_price']}"
    }

def foreach_batch_function(df, epoch_id):
    try:
        logger.info(f"\n=== Processing batch {epoch_id} at {datetime.now()} ===")
        
        rows = df.toJSON().collect()
        for row in rows:
            data = json.loads(row)
            try:
                # Format data for WebSocket emission
                stock_data = {
                    'stock_symbol': data['stock_symbol'],
                    'current_price': data['closing_price'],
                    'opening_price': data['opening_price'],
                    'closing_price': data['closing_price'],
                    'high': data['high'],
                    'low': data['low'],
                    'volume': data['volume'],
                    'timestamp': int(time.time() * 1000)  # Add timestamp
                }
                
                # Log the data being emitted
                logger.info(f"Emitting stock data: {stock_data}")
                
                # Emit to WebSocket
                socketio.emit('stock_update', stock_data)
                
                # Generate and emit trading signal
                if random.random() < 0.3:  # 30% chance to generate signal
                    signal = generate_trading_signal(stock_data)
                    socketio.emit('trading_signal', signal)
                    logger.info(f"Emitted trading signal: {signal}")
                
            except Exception as e:
                logger.error(f"Failed to process data: {e}")
                logger.error(f"Problematic data: {data}")
        
        logger.info(f"Processed {len(rows)} records in batch {epoch_id}")
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")

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
            .option("kafka.bootstrap.servers", "localhost:9092") \
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
