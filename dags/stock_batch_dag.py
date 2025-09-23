# dags/stock_batch_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from pyarrow import parquet as pq
import pyarrow as pa
import os

HDFS_PATH = "/usr/local/hadoop/hdfs/data/stock_data/batch"

def fetch_and_store():
    ticker = yf.Ticker("AAPL")
    df = ticker.history(period="1mo", interval="1d")  # last 1 month daily
    table = pa.Table.from_pandas(df.reset_index())
    os.makedirs(HDFS_PATH, exist_ok=True)
    pq.write_table(table, f"{HDFS_PATH}/AAPL_{datetime.now().date()}.parquet")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="stock_batch_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 9, 23),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="fetch_and_store_data",
        python_callable=fetch_and_store
    )
