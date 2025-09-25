#!/bin/bash
set -euo pipefail

# Modes / Flags
FAST_MODE=${FAST_MODE:-0}                       # Skip waits when 1
FULL_REBUILD=${FULL_REBUILD:-0}                 # --no-cache & --pull rebuild when 1
FRESH_RUN=${FRESH_RUN:-0}                       # Rotate Spark streaming checkpoint when 1
FORCE_LEAVE_SAFE_MODE=${FORCE_LEAVE_SAFE_MODE:-0} # Force HDFS safemode leave when 1

echo "[+] Preparing directories"
mkdir -p logs plugins data
chmod -R 777 logs plugins || true

echo "[+] Build phase"
if [ "$FULL_REBUILD" -eq 1 ]; then
	echo "[+] Performing full rebuild (no cache, pull latest)"
	docker compose build --no-cache --pull airflow-webserver airflow-scheduler airflow-worker stock-producer spark-streaming trading-system
else
	docker compose build airflow-webserver airflow-scheduler airflow-worker stock-producer spark-streaming trading-system
fi

echo "[+] Starting core infrastructure (ZK, Kafka, DB, Hadoop, Redis)"
docker compose up -d zookeeper kafka postgres redis namenode datanode

if [ "$FAST_MODE" -eq 0 ]; then
	echo "[+] Waiting (initial) for core services..."
	sleep 20
fi

# Kafka readiness
echo "[+] Checking Kafka readiness"
for i in {1..15}; do
	if docker compose exec -T kafka bash -c "kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1"; then
		echo "[+] Kafka is ready."
		break
	fi
	sleep 2
	[ $i -eq 15 ] && echo "[!] Kafka readiness timeout (continuing)"
done

# Redis readiness check
echo "[+] Checking Redis readiness"
for i in {1..10}; do
	if docker compose exec -T redis redis-cli ping | grep -q "PONG"; then
		echo "[+] Redis is ready."
		break
	fi
	sleep 2
	[ $i -eq 10 ] && echo "[!] Redis readiness timeout (continuing)"
done

# PostgreSQL readiness (for HFT analytics)
echo "[+] Checking PostgreSQL readiness"
for i in {1..15}; do
	if docker compose exec -T postgres pg_isready -U airflow >/dev/null 2>&1; then
		echo "[+] PostgreSQL is ready."
		break
	fi
	sleep 2
	[ $i -eq 15 ] && echo "[!] PostgreSQL readiness timeout (continuing)"
done

# HDFS preparation
if [ "$FORCE_LEAVE_SAFE_MODE" -eq 1 ]; then
	echo "[+] Forcing HDFS out of safemode (best-effort)"
	docker compose exec -T namenode hdfs dfsadmin -safemode leave || true
fi

if [ "$FRESH_RUN" -eq 1 ]; then
	echo "[+] Rotating previous Spark streaming checkpoint (if exists)"
	docker compose exec -T namenode hdfs dfs -test -d /stock_data/checkpoint && \
		docker compose exec -T namenode hdfs dfs -mv /stock_data/checkpoint /stock_data/checkpoint_$(date +%s) || true
fi

echo "[+] Starting Spark cluster"
docker compose up -d spark-master spark-worker

if [ "$FAST_MODE" -eq 0 ]; then
	echo "[+] Waiting for Spark..."
	sleep 12
fi

echo "[+] Starting Airflow services"
docker compose up -d airflow-webserver airflow-scheduler airflow-worker

if [ "$FAST_MODE" -eq 0 ]; then
	echo "[+] Waiting for Airflow webserver to become healthy (simple log probe)"
	ATTEMPTS=20
	while (( ATTEMPTS-- )); do
		if docker compose logs --tail=60 airflow-webserver | grep -q "Listening at"; then
			echo "[+] Airflow webserver responded."
			break
		fi
		sleep 2
	done
fi

# Conditional Airflow admin user creation (idempotent)
echo "[+] Ensuring Airflow admin user exists"
if ! docker compose exec -T airflow-webserver airflow users list 2>/dev/null | grep -q " admin "; then
	docker compose exec -T airflow-webserver airflow users create \
		--username admin --firstname Peter --lastname Parker --role Admin \
		--email spiderman@superhero.org --password admin || true
else
	echo "[+] Admin user already present."
fi

echo "[+] Starting data processing services (producer, streaming, trading system)"
docker compose up -d stock-producer spark-streaming trading-system

echo "[+] Verifying HFT pipeline components..."
if [ "$FAST_MODE" -eq 0 ]; then
	sleep 5
	echo "[+] Checking HFT pipeline health:"
	
	# Check if trading system started
	if docker compose ps trading-system | grep -q "Up"; then
		echo "    ✅ Trading System: Running"
	else
		echo "    ❌ Trading System: Not running"
	fi
	
	# Check Redis databases
	if docker compose exec -T redis redis-cli -n 0 ping | grep -q "PONG"; then
		echo "    ✅ Redis DB 0 (Price Cache): Ready"
	fi
	
	if docker compose exec -T redis redis-cli -n 1 ping | grep -q "PONG"; then
		echo "    ✅ Redis DB 1 (Signals): Ready"
	fi
	
	# Check PostgreSQL HFT tables
	if docker compose exec -T postgres psql -U airflow -d airflow -c "\dt" 2>/dev/null | grep -q "trading_params"; then
		echo "    ✅ PostgreSQL HFT Schema: Initialized"
	else
		echo "    ⚠️  PostgreSQL HFT Schema: May be initializing..."
	fi
fi

echo "[+] Summarizing container status"
docker compose ps

echo ""; echo "🚀 HFT PIPELINE STARTED! 🚀"; echo ""
echo "📊 Access URLs:"
echo "- Spark Master UI: http://localhost:8080"
echo "- Spark Worker UI: http://localhost:8081"
echo "- Airflow UI: http://localhost:8082 (admin/admin)"
echo "- Hadoop NameNode UI: http://localhost:9870" 
echo "" 
echo "🔧 HFT Monitoring Commands:"
echo "  # Watch trading system activity"
echo "  docker logs -f trading-system"
echo ""
echo "  # Check live prices in Redis"
echo "  docker exec -it redis redis-cli -n 0"
echo "  > HGETALL live_prices:AAPL"
echo ""
echo "  # Check trading signals"
echo "  docker exec -it redis redis-cli -n 1"
echo "  > GET signals:AAPL"
echo "  > LRANGE signal_history 0 10"
echo ""
echo "  # View trades in PostgreSQL"
echo "  docker exec -it postgres psql -U airflow -d airflow"
echo "  postgres=# SELECT * FROM trades ORDER BY timestamp DESC LIMIT 10;"
echo ""
echo "  # Test HFT pipeline"
echo "  python test_hft_pipeline.py"
echo "" 
echo "⚙️  Feature flags (export before running):" 
echo "  FAST_MODE=1                # Skip waits" 
echo "  FULL_REBUILD=1             # Force no-cache rebuild of custom images" 
echo "  FRESH_RUN=1                # Rotate streaming checkpoint state" 
echo "  FORCE_LEAVE_SAFE_MODE=1    # Force HDFS out of safemode early" 
echo "" 
echo "📈 Data Flow:"
echo "  Real-time: Market Data → Kafka → Spark → Redis → Trading System"
echo "  Batch:     HDFS → Spark Batch → PostgreSQL (Daily at 2 AM)"
echo ""
echo "🧪 Quick Test:"
echo "  python test_hft_pipeline.py"
echo "" 
echo "Helper commands:" 
echo "  docker compose logs -f spark-streaming" 
echo "  docker compose exec airflow-webserver airflow dags list" 
echo "  docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list" 
echo "  docker compose exec namenode hdfs dfs -ls /stock_data/stream | head" 
echo "" 
echo "Example (fresh clean run):" 
echo "  FRESH_RUN=1 FULL_REBUILD=1 bash start-services.sh" 
echo "" 
echo "🎯 Your HFT pipeline is ready for algorithmic trading simulation!"
