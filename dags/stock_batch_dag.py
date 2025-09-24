# dags/stock_batch_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import requests
import io
import logging

log = logging.getLogger(__name__)

# Use HDFS WebHDFS API instead of local filesystem
HDFS_NAMENODE_URL = "http://namenode:9870/webhdfs/v1"
HDFS_PATH = "/stock_data/batch"

def fetch_and_store():
    """
    Fetches stock data using yfinance with a temporary cache,
    and stores it as a Parquet file in HDFS.
    """
    try:
        # Create a temporary directory that is unique to this task run
        with tempfile.TemporaryDirectory() as temp_cache_dir:
            # Tell yfinance to use this temporary directory for its cache
            yf.set_tz_cache_location(temp_cache_dir)

            log.info("Fetching AAPL stock data...")
            ticker = yf.Ticker("AAPL")
            df = ticker.history(period="1mo", interval="1d")

            if df.empty:
                raise ValueError("No data retrieved from yfinance for AAPL")

            log.info(f"Retrieved {len(df)} records")

            # Convert to parquet in memory
            table = pa.Table.from_pandas(df.reset_index())
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            # Create directory in HDFS if it doesn't exist
            dir_url = f"{HDFS_NAMENODE_URL}{HDFS_PATH}?op=MKDIRS"
            response = requests.put(dir_url)
            log.info(f"HDFS directory creation response: {response.status_code}")

            # Upload file to HDFS using WebHDFS
            filename = f"AAPL_{datetime.now().strftime('%Y-%m-%d')}.parquet"
            upload_url = f"{HDFS_NAMENODE_URL}{HDFS_PATH}/{filename}?op=CREATE&overwrite=true"

            # First request to get the redirect URL from the NameNode
            response = requests.put(upload_url, allow_redirects=False)
            if response.status_code == 307: # 307 is Temporary Redirect
                redirect_url = response.headers['Location']
                
                # The second request sends the actual data to the DataNode
                upload_response = requests.put(redirect_url, data=buffer.getvalue())
                log.info(f"File upload response: {upload_response.status_code}")

                if upload_response.status_code == 201: # 201 means "Created"
                    log.info(f"Successfully uploaded {filename} to HDFS")
                else:
                    raise Exception(f"Failed to upload file to DataNode: {upload_response.status_code} - {upload_response.text}")
            else:
                raise Exception(f"Failed to get upload URL from NameNode: {response.status_code} - {response.text}")

    except Exception as e:
        log.error(f"Error in fetch_and_store: {e}")
        raise

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 9, 23),
}

with DAG(
    dag_id="stock_batch_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Fetch AAPL stock data and store in HDFS"
) as dag:
    wait_for_hdfs = BashOperator(
        task_id='wait_for_hdfs',
        bash_command="""
        echo "Waiting for HDFS to exit safemode..."
        # Use curl to check the NameNode's JMX endpoint for the Safemode status.
        # When safemode is OFF, the JSON value for "Safemode" is an empty string "".
        until curl -s http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo | grep -q '"Safemode" : ""'; do
        echo "HDFS is still in safemode, waiting 10 seconds..."
        sleep 10
        done
        echo "HDFS is ready."
        """,
    )

    fetch_and_store = PythonOperator(
        task_id="fetch_and_store_data",
        python_callable=fetch_and_store
    )

    wait_for_hdfs >> fetch_and_store
    