# producer.py
from kafka import KafkaProducer
from alpha_vantage.timeseries import TimeSeries
import json, time
import os

# Use environment variable for Kafka bootstrap servers, fallback to localhost
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Get Alpha Vantage API key from environment variable
api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
if not api_key:
    raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required")

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Initialize Alpha Vantage TimeSeries
ts = TimeSeries(key=api_key, output_format='pandas')

while True:
    try:
        # Fetch intraday data for AAPL (1min interval)
        data, meta_data = ts.get_intraday(symbol='AAPL', interval='1min', outputsize='compact')

        if not data.empty:
            # Get the most recent data point
            latest_data = data.iloc[0]  # Data is sorted with most recent first
            record = {
                "ticker": "AAPL",
                "timestamp": str(latest_data.name),  # timestamp is the index
                "price": float(latest_data["4. close"]),
                "volume": int(latest_data["5. volume"])
            }
            producer.send("stock_ticks", value=record)
            print("Sent:", record)
        else:
            print("No data received from Alpha Vantage")
    except Exception as e:
        print(f"Error fetching data: {e}")

    time.sleep(60)  # fetch every 1 minute
