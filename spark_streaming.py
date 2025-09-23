# spark_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os

spark = SparkSession.builder \
    .appName("StockStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

schema = StructType([
    StructField("ticker", StringType()),
    StructField("timestamp", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", IntegerType())
])

# Use environment variables for configuration
kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
hdfs_namenode = os.getenv('HDFS_NAMENODE', 'hdfs://localhost:9000')

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "stock_ticks") \
    .load()

# Deserialize JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Moving average (5-minute window) with watermark so aggregation can use append mode
agg_df = json_df \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("ticker")
    ).agg(avg("price").alias("avg_price"))

# Write to HDFS in Parquet
query = agg_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"{hdfs_namenode}/stock_data/stream") \
    .option("checkpointLocation", f"{hdfs_namenode}/stock_data/checkpoint") \
    .start()

query.awaitTermination()
