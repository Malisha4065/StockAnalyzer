# spark_streaming.py - HFT Real-time Signal Generation Engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window, lag, when, lit, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os, time, subprocess, redis
from pyspark.sql import Row
import json

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
    redis_client = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/1'), decode_responses=True)
    
    # Collect signals and send to Redis
    signals = batch_df.collect()
    
    for signal in signals:
        signal_data = {
            "symbol": signal.symbol,
            "action": signal.signal,
            "confidence": float(signal.confidence) if signal.confidence else 0.0,
            "price": float(signal.current_price),
            "ma_short": float(signal.ma_short) if signal.ma_short else 0.0,
            "ma_long": float(signal.ma_long) if signal.ma_long else 0.0,
            "timestamp": signal.window.start.isoformat(),
            "volume": int(signal.avg_volume) if signal.avg_volume else 0
        }
        
        # Store signal in Redis with 5-minute expiry
        redis_client.setex(f"signals:{signal.symbol}", 300, json.dumps(signal_data))
        
        # Also store in a signals stream for historical analysis
        redis_client.lpush("signal_history", json.dumps(signal_data))
        redis_client.ltrim("signal_history", 0, 1000)  # Keep last 1000 signals
        
        print(f"Signal sent to Redis - {signal.symbol}: {signal.signal} @ ${signal.current_price}")

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
            StructField("price", DoubleType()),
            StructField("volume", IntegerType())
        ])
        
        print("Reading from Kafka...")
        
        # Read from Kafka with error handling
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", "stock_ticks") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("Processing JSON data...")
        
        # Deserialize JSON with error handling
        json_df = df.selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), schema).alias("data")) \
            .select("data.*") \
            .filter(col("ticker").isNotNull() & col("price").isNotNull())
        
        # Moving average (5-minute window) with watermark
        print("Creating aggregation...")
        agg_df = json_df \
            .withColumn("timestamp", col("timestamp").cast("timestamp")) \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("ticker")
            ).agg(
                avg("price").alias("avg_price"),
                avg("volume").alias("avg_volume")
            )
        
        # Ensure target directories exist using a simple filesystem touch via DataFrame write
        print("Ensuring HDFS directories exist (parquet path)...")
        try:
            spark.range(0,1).write.mode("ignore").parquet(f"{hdfs_namenode}/stock_data/_init_tmp")
        except Exception as e:
            print(f"Warning: could not pre-create directory: {e}")
        
        # Determine primary output + checkpoint
        checkpoint_base = f"{hdfs_namenode}/stock_data/checkpoint"
        output_base = f"{hdfs_namenode}/stock_data/stream"
        fresh_run = os.getenv("FRESH_RUN", "0") == "1"

        print(f"FRESH_RUN={fresh_run}")

        # If fresh run requested, rename old checkpoint path to avoid corruption reuse
        if fresh_run:
            try:
                print("Attempting to move old checkpoint directory (if exists) to backup...")
                # Use 'hdfs dfs -mv' (ignore errors if not exists)
                subprocess.call(["hdfs", "dfs", "-mv", checkpoint_base, f"{checkpoint_base}_bak_{int(time.time())}"])
            except Exception as e:
                print(f"Could not move old checkpoint: {e}")

        print("Starting streaming query (with stateful aggregation)...")

        def start_query(chkpt, path):
            return (agg_df.writeStream
                    .outputMode("append")
                    .format("parquet")
                    .option("path", path)
                    .option("checkpointLocation", chkpt)
                    .trigger(processingTime='30 seconds')
                    .start())

        try:
            query = start_query(checkpoint_base, output_base)
        except Exception as primary_err:
            print(f"Primary checkpoint failed: {primary_err}")
            alt_checkpoint = f"{checkpoint_base}_alt_{uuid.uuid4().hex}"
            print(f"Retrying with fresh checkpoint: {alt_checkpoint}")
            try:
                query = start_query(alt_checkpoint, output_base)
            except Exception as second_err:
                print(f"Second attempt failed: {second_err}")
                print("Falling back to LOCAL filesystem (non-HDFS) output.")
                local_cp = "/app/data/checkpoint"
                local_out = "/app/data/stream"
                os.makedirs(local_cp, exist_ok=True)
                os.makedirs(local_out, exist_ok=True)
                query = start_query(local_cp, local_out)
        
        print("Streaming query started successfully!")
        print(f"Writing data to: {hdfs_namenode}/stock_data/stream")
        print(f"Checkpoint location: {hdfs_namenode}/stock_data/checkpoint")
        print("Press Ctrl+C to stop...")
        
        # Keep the application running
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error in streaming application: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Stopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    main()