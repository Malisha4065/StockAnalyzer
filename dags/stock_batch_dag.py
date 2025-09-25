# dags/stock_batch_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from alpha_vantage.timeseries import TimeSeries
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
    Fetches stock data using Alpha Vantage API,
    and stores it as a Parquet file in HDFS.
    """
    try:
        # Get Alpha Vantage API key from environment variable
        api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        if not api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required")

        # Initialize Alpha Vantage TimeSeries
        ts = TimeSeries(key=api_key, output_format='pandas')

        log.info("Fetching AAPL stock data from Alpha Vantage...")
        # Fetch daily time series data
        data, meta_data = ts.get_daily(symbol='AAPL', outputsize='compact')

        if data.empty:
            raise ValueError("No data retrieved from Alpha Vantage for AAPL")

        log.info(f"Retrieved {len(data)} records")

        # Rename columns to match expected format
        data = data.rename(columns={
            '1. open': 'Open',
            '2. high': 'High',
            '3. low': 'Low',
            '4. close': 'Close',
            '5. volume': 'Volume'
        })

        # Convert volume to int
        data['Volume'] = data['Volume'].astype(int)

        # Convert to parquet in memory
        table = pa.Table.from_pandas(data.reset_index())
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
    