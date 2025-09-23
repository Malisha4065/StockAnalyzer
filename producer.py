# producer.py
from kafka import KafkaProducer
import yfinance as yf
import json, time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

ticker = yf.Ticker("AAPL")

while True:
    data = ticker.history(period="1d", interval="1m").tail(1)
    record = {
        "ticker": "AAPL",
        "timestamp": str(data.index[-1]),
        "price": float(data["Close"].iloc[-1]),
        "volume": int(data["Volume"].iloc[-1])
    }
    producer.send("stock_ticks", value=record)
    print("Sent:", record)
    time.sleep(60)  # fetch every 1 minute
