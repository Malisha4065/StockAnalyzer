# spark_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("StockStreamProcessor") \
    .getOrCreate()

schema = StructType([
    StructField("ticker", StringType()),
    StructField("timestamp", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", IntegerType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_ticks") \
    .load()

# Deserialize JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Moving average (5-minute window)
agg_df = json_df \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("ticker")
    ).agg(avg("price").alias("avg_price"))

# Write to HDFS in Parquet
query = agg_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/stock_data/stream") \
    .option("checkpointLocation", "hdfs://localhost:9000/stock_data/checkpoint") \
    .start()

query.awaitTermination()
