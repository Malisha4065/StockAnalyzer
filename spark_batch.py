# spark_batch.py
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("StockBatchAnalysis") \
    .getOrCreate()

# Use environment variable for HDFS namenode
hdfs_namenode = os.getenv('HDFS_NAMENODE', 'hdfs://localhost:9000')
df = spark.read.parquet(f"{hdfs_namenode}/stock_data/batch")

df.createOrReplaceTempView("stocks")

# Example: Average closing price in last 30 days
result = spark.sql("""
    SELECT AVG(Close) AS avg_close
    FROM stocks
    WHERE Date >= date_sub(current_date(), 30)
""")

result.show()
