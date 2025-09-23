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
	docker compose build --no-cache --pull airflow-webserver airflow-scheduler airflow-worker stock-producer spark-streaming
else
	docker compose build airflow-webserver airflow-scheduler airflow-worker stock-producer spark-streaming
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

echo "[+] Starting data processing services (producer, streaming)"
docker compose up -d stock-producer spark-streaming

echo "[+] Summarizing container status"
docker compose ps

echo ""; echo "All services started!"; echo ""
echo "Access URLs:"
echo "- Spark Master UI: http://localhost:8080"
echo "- Spark Worker UI: http://localhost:8081"
echo "- Airflow UI: http://localhost:8082 (admin/admin)"
echo "- Hadoop NameNode UI: http://localhost:9870" 
echo "" 
echo "Feature flags (export before running):" 
echo "  FAST_MODE=1                # Skip waits" 
echo "  FULL_REBUILD=1             # Force no-cache rebuild of custom images" 
echo "  FRESH_RUN=1                # Rotate streaming checkpoint state" 
echo "  FORCE_LEAVE_SAFE_MODE=1    # Force HDFS out of safemode early" 
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
echo "Done."
