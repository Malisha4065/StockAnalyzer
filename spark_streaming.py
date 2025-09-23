# spark_streaming.py - Improved with error handling and retries
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os
import time
import subprocess
import shutil
import uuid

def wait_for_hdfs_ready(hdfs_namenode, max_retries=15):
    """Poll HDFS safemode status and force leave if stuck."""
    namenode_host = hdfs_namenode.replace("hdfs://", "").split(":")[0]
    for attempt in range(1, max_retries + 1):
        print(f"Attempt {attempt}: Checking if HDFS is out of safemode...")
        try:
            # Query safemode status
            out = subprocess.check_output(["hdfs", "dfsadmin", "-safemode", "get"], stderr=subprocess.STDOUT, text=True)
            if "OFF" in out:
                print("HDFS safemode is OFF")
                return True
            else:
                print(f"HDFS still in safemode: {out.strip()}")
                
                # After several attempts, try to force leave safemode
                if attempt >= 8:
                    print("Attempting to force leave safemode...")
                    try:
                        force_out = subprocess.check_output(["hdfs", "dfsadmin", "-safemode", "leave"], 
                                                          stderr=subprocess.STDOUT, text=True)
                        print(f"Force leave result: {force_out.strip()}")
                    except Exception as fe:
                        print(f"Could not force leave safemode: {fe}")
                        
        except Exception as e:
            print(f"Could not query safemode yet: {e}")
        
        sleep_time = 15 if attempt < 5 else 30  # Longer waits after initial attempts
        if attempt < max_retries:
            time.sleep(sleep_time)
    
    print("HDFS did not exit safemode reliably; will attempt streaming anyway.")
    return False

def create_spark_session_with_retry(max_retries=5):
    """Create Spark session with retry logic"""
    for attempt in range(max_retries):
        try:
            print(f"Creating Spark session (attempt {attempt + 1})...")
            
            spark = SparkSession.builder \
                .appName("StockStreamProcessor") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            # Set log level to reduce noise
            spark.sparkContext.setLogLevel("WARN")
            
            print("Spark session created successfully!")
            return spark
            
        except Exception as e:
            print(f"Failed to create Spark session: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in 10 seconds...")
                time.sleep(10)
            else:
                raise
    
    return None

def main():
    print("Starting Stock Stream Processor...")
    
    # Configuration from environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    hdfs_namenode = os.getenv('HDFS_NAMENODE', 'hdfs://localhost:9000')
    
    print(f"Kafka servers: {kafka_servers}")
    print(f"HDFS namenode: {hdfs_namenode}")
    
    # Create Spark session with retry
    spark = create_spark_session_with_retry()
    if not spark:
        print("Failed to create Spark session. Exiting.")
        return
    
    # Wait for HDFS to be ready
    if not wait_for_hdfs_ready(hdfs_namenode):
        print("HDFS readiness check incomplete but proceeding with streaming setup...")
        print("If errors persist, manually run: docker compose exec namenode hdfs dfsadmin -safemode leave")
    
    try:
        # Define schema for incoming JSON data
        schema = StructType([
            StructField("ticker", StringType()),
            StructField("timestamp", StringType()),
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