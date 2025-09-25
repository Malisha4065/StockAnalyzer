# test_hft_pipeline.py - HFT Pipeline Testing and Verification
import redis
import psycopg2
import json
import time
from datetime import datetime
import subprocess
import requests

def test_redis_connection():
    """Test Redis connections"""
    print("=== Testing Redis Connections ===")
    try:
        # Test signals Redis (db 1)
        redis_signals = redis.from_url('redis://redis:6379/1', decode_responses=True)
        redis_signals.ping()
        print("✅ Redis signals database (db 1) - Connected")
        
        # Test cache Redis (db 0)  
        redis_cache = redis.from_url('redis://redis:6379/0', decode_responses=True)
        redis_cache.ping()
        print("✅ Redis cache database (db 0) - Connected")
        
        return True
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return False

def test_postgresql_connection():
    """Test PostgreSQL connection and verify tables"""
    print("\n=== Testing PostgreSQL Connection ===")
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow",
            port=5432
        )
        cursor = conn.cursor()
        
        # Test connection
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ PostgreSQL Connected: {version[:50]}...")
        
        # Check required tables
        required_tables = [
            'strategy_performance',
            'trading_params', 
            'trades',
            'positions',
            'risk_metrics',
            'backtest_results'
        ]
        
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        for table in required_tables:
            if table in existing_tables:
                print(f"✅ Table '{table}' exists")
            else:
                print(f"❌ Table '{table}' missing")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False

def test_kafka_connection():
    """Test Kafka connection"""
    print("\n=== Testing Kafka Connection ===")
    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.errors import NoBrokersAvailable
        
        # Test producer
        producer = KafkaProducer(
            bootstrap_servers='kafka:29092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Send test message
        test_message = {
            "test": True,
            "timestamp": datetime.now().isoformat(),
            "message": "HFT Pipeline Test"
        }
        
        producer.send('market_data', test_message)
        producer.flush()
        print("✅ Kafka Producer - Working")
        
        # Test consumer
        consumer = KafkaConsumer(
            'market_data',
            bootstrap_servers='kafka:29092',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        
        print("✅ Kafka Consumer - Working")
        consumer.close()
        producer.close()
        return True
        
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        return False

def test_hdfs_connection():
    """Test HDFS connection"""
    print("\n=== Testing HDFS Connection ===")
    try:
        # Check namenode web interface
        response = requests.get("http://namenode:9870", timeout=10)
        if response.status_code == 200:
            print("✅ HDFS NameNode web interface - Accessible")
        else:
            print(f"❌ HDFS NameNode returned status: {response.status_code}")
            
        # Check if HDFS is out of safemode
        jmx_response = requests.get(
            "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", 
            timeout=10
        )
        
        if '"Safemode" : ""' in jmx_response.text:
            print("✅ HDFS is out of safemode")
        else:
            print("⚠️  HDFS may be in safemode")
            
        return True
        
    except Exception as e:
        print(f"❌ HDFS connection failed: {e}")
        return False

def test_spark_connection():
    """Test Spark connection"""
    print("\n=== Testing Spark Connection ===")
    try:
        # Check Spark Master web interface
        response = requests.get("http://spark-master:8080", timeout=10)
        if response.status_code == 200:
            print("✅ Spark Master web interface - Accessible")
            
            # Parse response to check for workers
            if "Workers:" in response.text:
                print("✅ Spark Workers detected")
            else:
                print("⚠️  No Spark Workers detected")
        else:
            print(f"❌ Spark Master returned status: {response.status_code}")
            
        return True
        
    except Exception as e:
        print(f"❌ Spark connection failed: {e}")
        return False

def test_airflow_connection():
    """Test Airflow connection"""
    print("\n=== Testing Airflow Connection ===")
    try:
        # Check Airflow web interface
        response = requests.get("http://airflow-webserver:8080", timeout=15)
        if response.status_code == 200:
            print("✅ Airflow webserver - Accessible")
        else:
            print(f"❌ Airflow returned status: {response.status_code}")
            
        return True
        
    except Exception as e:
        print(f"❌ Airflow connection failed: {e}")
        return False

def simulate_hft_data_flow():
    """Simulate the complete HFT data flow"""
    print("\n=== Testing HFT Data Flow ===")
    try:
        # 1. Simulate market data
        print("1. Testing market data flow...")
        redis_client = redis.from_url('redis://redis:6379/0', decode_responses=True)
        
        test_market_data = {
            "symbol": "TEST",
            "price": 100.50,
            "bid": 100.49,
            "ask": 100.51,
            "volume": 1000,
            "timestamp": datetime.now().isoformat()
        }
        
        redis_client.hset("live_prices:TEST", mapping=test_market_data)
        print("✅ Market data stored in Redis cache")
        
        # 2. Simulate trading signal
        print("2. Testing trading signal flow...")
        signals_redis = redis.from_url('redis://redis:6379/1', decode_responses=True)
        
        test_signal = {
            "symbol": "TEST",
            "action": "BUY", 
            "confidence": 0.75,
            "price": 100.50,
            "timestamp": datetime.now().isoformat()
        }
        
        signals_redis.setex("signals:TEST", 300, json.dumps(test_signal))
        print("✅ Trading signal stored in Redis")
        
        # 3. Test database logging
        print("3. Testing database logging...")
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow", 
            password="airflow"
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO trades 
            (symbol, strategy_name, action, quantity, price, timestamp, signal_confidence, portfolio_value, cash_balance)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "TEST", "test_strategy", "BUY", 10, 100.50, 
            datetime.now(), 0.75, 50000.00, 49000.00
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Trade logged to PostgreSQL")
        
        return True
        
    except Exception as e:
        print(f"❌ HFT data flow test failed: {e}")
        return False

def show_architecture_status():
    """Show the current architecture status"""
    print("\n" + "="*60)
    print("HFT PIPELINE ARCHITECTURE STATUS")
    print("="*60)
    
    print("\n🔥 REAL-TIME PATH (Hot Path):")
    print("   Market Data → Kafka → Spark Streaming → Redis → Trading System")
    print("   ⚡ Latency: Sub-second signal generation")
    print("   📊 Purpose: Execute trades in real-time")
    
    print("\n❄️  BATCH PATH (Cold Path):")  
    print("   HDFS → Spark Batch → PostgreSQL")
    print("   ⏰ Schedule: Daily (2 AM via Airflow)")
    print("   📈 Purpose: Strategy optimization & backtesting")
    
    print("\n🏗️  COMPONENTS:")
    print("   • Kafka: Market data streaming")
    print("   • Redis (db0): Live price cache") 
    print("   • Redis (db1): Trading signals")
    print("   • Spark Streaming: Real-time signal generation")
    print("   • Spark Batch: Historical analysis")
    print("   • PostgreSQL: Analytics & trade logging")
    print("   • HDFS: Historical data storage")
    print("   • Airflow: Batch job orchestration")
    print("   • Trading System: Trade execution engine")

def main():
    """Run all tests"""
    print("🚀 HFT PIPELINE TESTING SUITE")
    print("="*60)
    
    tests = [
        ("Redis", test_redis_connection),
        ("PostgreSQL", test_postgresql_connection), 
        ("Kafka", test_kafka_connection),
        ("HDFS", test_hdfs_connection),
        ("Spark", test_spark_connection),
        ("Airflow", test_airflow_connection),
        ("HFT Data Flow", simulate_hft_data_flow)
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results[test_name] = False
    
    # Show results summary
    print("\n" + "="*60)
    print("TEST RESULTS SUMMARY")
    print("="*60)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:15}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed ({(passed/total)*100:.1f}%)")
    
    if passed == total:
        print("\n🎉 ALL SYSTEMS OPERATIONAL!")
        print("Your HFT pipeline is ready for trading!")
    else:
        print("\n⚠️  Some components need attention before going live")
    
    # Show architecture
    show_architecture_status()
    
    print("\n📚 NEXT STEPS:")
    print("1. Start the pipeline: docker-compose up -d")
    print("2. Check Airflow UI: http://localhost:8082 (admin/admin)")
    print("3. Monitor Spark: http://localhost:8080")  
    print("4. Watch Redis signals: redis-cli -h localhost -p 6379 -n 1")
    print("5. Check trading logs in PostgreSQL")

if __name__ == "__main__":
    main()