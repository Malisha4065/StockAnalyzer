# spark_batch.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StockBatchAnalysis") \
    .getOrCreate()

df = spark.read.parquet("hdfs://localhost:9000/stock_data/batch")

df.createOrReplaceTempView("stocks")

# Example: Average closing price in last 30 days
result = spark.sql("""
    SELECT AVG(Close) AS avg_close
    FROM stocks
    WHERE Date >= date_sub(current_date(), 30)
""")

result.show()
