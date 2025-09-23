#!/bin/bash
set -euo pipefail

FAST_MODE=${FAST_MODE:-0}

echo "[+] Preparing directories"
mkdir -p logs plugins data
chmod -R 777 logs plugins || true

echo "[+] Building custom images (Airflow, producer, spark)"
docker compose build airflow-webserver airflow-scheduler airflow-worker stock-producer spark-streaming

echo "[+] Starting core infrastructure (ZK, Kafka, DB, Hadoop, Redis)"
docker compose up -d zookeeper kafka postgres redis namenode datanode

if [ "$FAST_MODE" -eq 0 ]; then
	echo "[+] Waiting (initial) for core services..."
	sleep 25
fi

echo "[+] Starting Spark cluster"
docker compose up -d spark-master spark-worker

if [ "$FAST_MODE" -eq 0 ]; then
	echo "[+] Waiting for Spark..."
	sleep 15
fi

echo "[+] Starting Airflow services"
docker compose up -d airflow-webserver airflow-scheduler airflow-worker

if [ "$FAST_MODE" -eq 0 ]; then
	echo "[+] Waiting for Airflow webserver to become healthy (simple log probe)"
	ATTEMPTS=15
	while (( ATTEMPTS-- )); do
		if docker compose logs --tail=50 airflow-webserver | grep -q "Listening at"; then
			break
		fi
		sleep 2
	done
fi

echo "[+] Starting data processing services (producer, streaming)"
docker compose up -d stock-producer spark-streaming

echo ""; echo "All services started!"; echo ""
echo "Access URLs:"
echo "- Spark Master UI: http://localhost:8080"
echo "- Spark Worker UI: http://localhost:8081"
echo "- Airflow UI: http://localhost:8082 (admin/admin)"
echo "- Hadoop NameNode UI: http://localhost:9870"
echo ""
echo "Helper commands:" 
echo "  Tail logs: docker compose logs -f spark-streaming" 
echo "  List DAGs: docker compose exec airflow-webserver airflow dags list" 
echo "  Rebuild Airflow only: docker compose build airflow-webserver airflow-scheduler airflow-worker && docker compose up -d airflow-webserver airflow-scheduler airflow-worker" 
echo "" 
echo "Set FAST_MODE=1 to skip most waits."
