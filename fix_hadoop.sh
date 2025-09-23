#!/bin/bash
# Fix Hadoop Safe Mode and Restart Services

echo "=========================================="
echo "FIXING HADOOP SAFE MODE ISSUE"
echo "=========================================="

# Check HDFS status
echo "1. Checking HDFS status..."
docker compose exec namenode hdfs dfsadmin -report

echo ""
echo "2. Checking if HDFS is in safe mode..."
docker compose exec namenode hdfs dfsadmin -safemode get

echo ""
echo "3. Force HDFS out of safe mode..."
docker compose exec namenode hdfs dfsadmin -safemode leave

echo ""
echo "4. Verify safe mode is off..."
docker compose exec namenode hdfs dfsadmin -safemode get

echo ""
echo "5. Create necessary directories in HDFS..."
docker compose exec namenode hdfs dfs -mkdir -p /stock_data/stream
docker compose exec namenode hdfs dfs -mkdir -p /stock_data/batch
docker compose exec namenode hdfs dfs -mkdir -p /stock_data/checkpoint

echo ""
echo "6. Set permissions (making them more permissive for testing)..."
docker compose exec namenode hdfs dfs -chmod -R 777 /stock_data

echo ""
echo "7. Verify directories were created..."
docker compose exec namenode hdfs dfs -ls /
docker compose exec namenode hdfs dfs -ls /stock_data/

echo ""
echo "8. Restart Spark streaming job..."
docker compose stop spark-streaming
sleep 5
docker compose up -d spark-streaming

echo ""
echo "9. Check Airflow webserver logs..."
docker compose logs --tail=20 airflow-webserver

echo ""
echo "10. If Airflow is still not responding, restart it..."
read -p "Restart Airflow webserver? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker compose restart airflow-webserver
    echo "Waiting for Airflow to start..."
    sleep 15
    echo "Airflow should be available at http://localhost:8082"
fi

echo ""
echo "=========================================="
echo "VERIFICATION"
echo "=========================================="

echo "Checking services status:"
echo "- HDFS Safe Mode:"
docker compose exec namenode hdfs dfsadmin -safemode get

echo "- Spark Streaming Status:"
docker compose ps spark-streaming

echo "- Recent Spark Streaming logs:"
docker compose logs --tail=10 spark-streaming

echo ""
echo "Next steps:"
echo "1. Check http://localhost:8082 for Airflow UI"
echo "2. Check http://localhost:8080 for Spark Master UI"
echo "3. Monitor: docker compose logs -f spark-streaming"