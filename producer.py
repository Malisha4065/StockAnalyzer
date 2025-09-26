# producer.py - HFT Market Data Feed Simulator
from kafka import KafkaProducer
import yfinance as yf
import json, time, random
import os
import redis
from datetime import datetime

# Configuration
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Initialize Redis for caching latest prices
redis_client = redis.from_url(redis_url, decode_responses=True)

# Symbols to trade
SYMBOLS = ['AAPL', 'GOOGL', 'TSLA', 'MSFT', 'AMZN']

# Initialize price cache with realistic starting prices
PRICE_CACHE = {
    'AAPL': 150.00,
    'GOOGL': 2500.00, 
    'TSLA': 250.00,
    'MSFT': 300.00,
    'AMZN': 3200.00
}

def wait_for_kafka():
    from kafka import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable
    max_retries = 30
    retry_count = 0
    while retry_count < max_retries:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            admin_client.close()
            print("Kafka is available!")
            return True
        except NoBrokersAvailable:
            print(f"Waiting for Kafka... (attempt {retry_count + 1}/{max_retries})")
            time.sleep(2)
            retry_count += 1
    raise Exception("Kafka not available after maximum retries")

def simulate_market_tick(symbol, base_price):
    """Generate realistic market tick data with bid/ask spread"""
    # Simulate small price movements (0.1% to 2%)
    price_change_pct = random.uniform(-0.02, 0.02)
    new_price = base_price * (1 + price_change_pct)
    
    # Simulate bid/ask spread (0.01% to 0.1%)
    spread_pct = random.uniform(0.0001, 0.001)
    spread = new_price * spread_pct
    
    bid = round(new_price - spread/2, 2)
    ask = round(new_price + spread/2, 2)
    mid_price = round((bid + ask) / 2, 2)
    
    # Simulate volume (realistic ranges)
    volume = random.randint(100, 5000)
    
    return {
        "symbol": symbol,
        "timestamp": datetime.now().isoformat(),
        "price": mid_price,
        "bid": bid,
        "ask": ask,
        "volume": volume,
        "spread": round(spread, 4),
        "spread_pct": round(spread_pct * 100, 4)
    }

def get_real_price_update(symbol):
    """Get real price from yfinance"""
    try:
        ticker = yf.Ticker(symbol)
        # Get latest data (1 day with 1-minute intervals)
        hist = ticker.history(period="1d", interval="1m")
        
        if not hist.empty:
            # Get the most recent price data
            latest_data = hist.iloc[-1]
            real_price = float(latest_data["Close"])
            volume = int(latest_data["Volume"])
            
            # Cache the real price
            PRICE_CACHE[symbol] = real_price
            redis_client.hset(f"real_prices", symbol, real_price)
            
            return real_price, volume
    except Exception as e:
        print(f"Error fetching real data for {symbol}: {e}")
        
    return None, None

# Wait for services and start producing
wait_for_kafka()
print("Initializing Redis connection...")
redis_client.ping()
print("Redis connected!")

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    batch_size=16384,
    linger_ms=10,  # Low latency for HFT
    compression_type='snappy'
)

print("Starting HFT Market Data Feed...")
print(f"Producing data for symbols: {SYMBOLS}")

# Real data fetch counter (every 60 seconds)
real_data_counter = 0
REAL_DATA_INTERVAL = 60  # seconds

while True:
    try:
        # Every 60 seconds, get real price from yfinance for one symbol
        if real_data_counter % REAL_DATA_INTERVAL == 0:
            symbol_to_update = SYMBOLS[random.randint(0, len(SYMBOLS)-1)]
            real_price, real_volume = get_real_price_update(symbol_to_update)
            if real_price:
                print(f"Updated real price for {symbol_to_update}: ${real_price}")

        # Generate high-frequency simulated ticks for all symbols
        for symbol in SYMBOLS:
            base_price = PRICE_CACHE[symbol]
            tick_data = simulate_market_tick(symbol, base_price)
            
            # Update price cache with simulated movement
            PRICE_CACHE[symbol] = tick_data['price']
            
            # Send to different Kafka topics for organization
            producer.send("market_data", value=tick_data)
            
            # Cache in Redis for trading system
            redis_client.hset(f"live_prices:{symbol}", mapping={
                "price": tick_data['price'],
                "bid": tick_data['bid'], 
                "ask": tick_data['ask'],
                "volume": tick_data['volume'],
                "timestamp": tick_data['timestamp']
            })
            redis_client.expire(f"live_prices:{symbol}", 30)  # 30 second TTL
            
        print(f"Sent market data batch for {len(SYMBOLS)} symbols")
        real_data_counter += 1
        
        # High frequency: every 100ms for real HFT simulation
        time.sleep(0.1)  
        
    except Exception as e:
        print(f"Error in market data feed: {e}")
        time.sleep(1)
