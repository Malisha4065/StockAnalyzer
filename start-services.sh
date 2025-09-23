#!/bin/bash

# Create necessary directories
mkdir -p logs plugins data

# Set proper permissions
chmod -R 777 logs plugins

echo "Starting Stock Analyzer infrastructure..."

# Start core infrastructure first
docker-compose up -d zookeeper kafka postgres redis namenode datanode

echo "Waiting for core services to be ready..."
sleep 30

# Start Spark cluster
docker-compose up -d spark-master spark-worker

echo "Waiting for Spark cluster to be ready..."
sleep 20

# Start Airflow services
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker

echo "Waiting for Airflow to be ready..."
sleep 30

# Start data processing services
docker-compose up -d stock-producer spark-streaming

echo "All services started!"
echo ""
echo "Access URLs:"
echo "- Kafka UI: Not included (add if needed)"
echo "- Spark Master UI: http://localhost:8080"
echo "- Spark Worker UI: http://localhost:8081"
echo "- Airflow UI: http://localhost:8082 (admin/admin)"
echo "- Hadoop NameNode UI: http://localhost:9870"
echo ""
echo "To view logs: docker-compose logs -f [service-name]"
echo "To stop all services: docker-compose down"
