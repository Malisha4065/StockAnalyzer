# spark_streaming.py - HFT Real-time Signal Generation Engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    avg,
    window,
    when,
    lit,
    max as spark_max,
    min as spark_min,
    coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os, redis, json

def create_spark_session():
    """Create optimized Spark session for HFT"""
    spark = SparkSession.builder \
        .appName("HFT_Signal_Generator") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def send_signal_to_redis(batch_df, batch_id):
    """Send trading signals to Redis for immediate access by trading system"""
    try:
        redis_client = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/1'), decode_responses=True)
        
        # Collect signals and send to Redis
        signals = batch_df.collect()
        
        for signal in signals:
            signal_data = {
                "symbol": signal.symbol,
                "action": signal.signal,
                "confidence": float(signal.confidence) if signal.confidence else 0.0,
                "price": float(signal.current_price) if signal.current_price else 0.0,
                "ma_short": float(signal.ma_short) if signal.ma_short else 0.0,
                "ma_long": float(signal.ma_long) if signal.ma_long else 0.0,
                "timestamp": str(signal.window),
                "volume": int(signal.avg_volume) if signal.avg_volume else 0
            }
            
            # Store signal in Redis with 5-minute expiry
            redis_client.setex(f"signals:{signal.symbol}", 300, json.dumps(signal_data))
            
            # Also store in a signals stream for historical analysis
            redis_client.lpush("signal_history", json.dumps(signal_data))
            redis_client.ltrim("signal_history", 0, 1000)  # Keep last 1000 signals
            
            print(f"Signal sent to Redis - {signal.symbol}: {signal.signal} @ ${signal.current_price}")
    except Exception as e:
        print(f"Error sending signals to Redis: {e}")

def main():
    print("Starting HFT Real-time Signal Generation Engine...")
    
    # Configuration
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    hdfs_namenode = os.getenv('HDFS_NAMENODE', 'hdfs://localhost:9000')
    
    spark = create_spark_session()
    
    # Enhanced schema for HFT market data
    schema = StructType([
        StructField("symbol", StringType()),
        StructField("timestamp", StringType()),
        StructField("price", DoubleType()),
        StructField("bid", DoubleType()),
        StructField("ask", DoubleType()),
        StructField("volume", IntegerType()),
        StructField("spread", DoubleType()),
        StructField("spread_pct", DoubleType())
    ])
    
    try:
        print("Reading from Kafka market data stream...")
        
        # Read from Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", "market_data") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON and add timestamp processing
        parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", col("timestamp").cast("timestamp")) \
            .filter(col("symbol").isNotNull() & col("price").isNotNull())
        
        print("Creating HFT trading signals...")
        
        # Prepare streaming aggregates for short- and long-term views
        prices_with_watermark = parsed_df.withWatermark("timestamp", "2 minutes")

        short_window_metrics = prices_with_watermark.groupBy(
            window(col("timestamp"), "30 seconds", "5 seconds").alias("window"),
            col("symbol")
        ).agg(
            avg("price").alias("ma_short"),
            avg("volume").alias("avg_volume"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low")
        ).withColumn("current_price", col("ma_short")) \
         .withColumn("window_end", col("window").getField("end")) \
         .withWatermark("window_end", "2 minutes")

        long_window_metrics = prices_with_watermark.groupBy(
            window(col("timestamp"), "2 minutes", "5 seconds").alias("window"),
            col("symbol")
        ).agg(
            avg("price").alias("ma_long")
        ).withColumn("window_end", col("window").getField("end")) \
         .withWatermark("window_end", "2 minutes") \
         .select("symbol", "window_end", "ma_long")

        signals_df = short_window_metrics.join(long_window_metrics, ["symbol", "window_end"], "left") \
            .withColumn("ma_long", coalesce(col("ma_long"), col("ma_short"))) \
            .withColumn("price_momentum",
                when((col("ma_long") > 0) & col("ma_short").isNotNull(),
                     (col("ma_short") - col("ma_long")) / col("ma_long") * 100)
                .otherwise(lit(0.0))
            ) \
            .withColumn("signal",
                # If momentum is even slightly positive (>0.1%), signal a BUY
                when(col("price_momentum") > 0.1, "BUY")
                # If momentum is even slightly negative (<-0.1%), signal a SELL
                .when(col("price_momentum") < -0.1, "SELL")
                .otherwise("HOLD")
            ) \
            .withColumn("confidence",
                when(col("signal") == "BUY",
                     # A momentum of 1.1% or higher is now considered max confidence (1.0)
                     when(col("price_momentum") > 1.1, lit(1.0))
                     # Scale confidence between 0.1% and 1.1% momentum
                     .otherwise((col("price_momentum") - 0.1) / 1.0))
                .when(col("signal") == "SELL",
                     # A momentum of -1.1% or lower is now considered max confidence (1.0)
                     when(col("price_momentum") < -1.1, lit(1.0))
                     # Scale confidence between -0.1% and -1.1% momentum
                     .otherwise((-col("price_momentum") - 0.1) / 1.0))
                .otherwise(lit(0.0))
            ) \
            .drop("window_end") #\
            #.filter(col("signal") != "HOLD")
        
        print("Starting signal generation stream...")
        
        # Send signals to Redis for real-time trading
        redis_query = signals_df.writeStream \
            .outputMode("append") \
            .foreachBatch(send_signal_to_redis) \
            .option("checkpointLocation", "/tmp/redis_checkpoint") \
            .trigger(processingTime='1 second') \
            .start()
        
        # Also write signals to HDFS for historical analysis
        try:
            hdfs_query = signals_df.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", f"{hdfs_namenode}/stock_data/signals") \
                .option("checkpointLocation", f"/tmp/hdfs_checkpoint_signals") \
                .trigger(processingTime='5 seconds') \
                .start()
        except Exception as hdfs_error:
            print(f"HDFS write failed, continuing with Redis-only: {hdfs_error}")
            hdfs_query = None
        
        print("HFT Signal Generation System Online!")
        print(f"- Real-time signals in Redis: signals:<symbol>")
        print("- Signal history in Redis: signal_history")
        if hdfs_query:
            print(f"- Historical signals in HDFS: {hdfs_namenode}/stock_data/signals")
        print("Press Ctrl+C to stop...")
        
        # Wait for termination
        redis_query.awaitTermination()
        
    except Exception as e:
        print(f"Error in HFT signal generation: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Stopping HFT Signal Generation System...")
        spark.stop()

if __name__ == "__main__":
    main()