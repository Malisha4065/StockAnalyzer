#!/bin/bash
# System Verification Script
set -e

echo "=========================================="
echo "BIG DATA PIPELINE VERIFICATION"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_service() {
    local service_name=$1
    local url=$2
    local expected_text=$3
    
    echo -n "Checking $service_name... "
    
    if curl -s --connect-timeout 5 "$url" | grep -q "$expected_text"; then
        echo -e "${GREEN}✓ Running${NC}"
        return 0
    else
        echo -e "${RED}✗ Not responding${NC}"
        return 1
    fi
}

echo ""
echo "1. CHECKING DOCKER CONTAINERS"
echo "----------------------------------------"
docker compose ps

echo ""
echo "2. CHECKING WEB INTERFACES"
echo "----------------------------------------"

# Check Spark Master
check_service "Spark Master" "http://localhost:8080" "Spark Master"

# Check Spark Worker
check_service "Spark Worker" "http://localhost:8081" "Spark Worker"

# Check Airflow
check_service "Airflow" "http://localhost:8082" "Airflow"

# Check Hadoop NameNode
check_service "Hadoop NameNode" "http://localhost:9870" "NameNode"

echo ""
echo "3. CHECKING KAFKA TOPICS"
echo "----------------------------------------"
echo "Available Kafka topics:"
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo "Checking if stock_ticks topic exists..."
if docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -q "stock_ticks"; then
    echo -e "${GREEN}✓ stock_ticks topic exists${NC}"
else
    echo -e "${YELLOW}! Creating stock_ticks topic${NC}"
    docker compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic stock_ticks --partitions 1 --replication-factor 1
fi

echo ""
echo "4. CHECKING KAFKA MESSAGES (Last 5 messages)"
echo "----------------------------------------"
timeout 10s docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock_ticks --from-beginning --max-messages 5 2>/dev/null || echo "No messages yet or timeout reached"

echo ""
echo "5. CHECKING HDFS STATUS"
echo "----------------------------------------"
echo "HDFS filesystem status:"
docker compose exec namenode hdfs dfsadmin -report

echo ""
echo "HDFS directory listing:"
docker compose exec namenode hdfs dfs -ls / 2>/dev/null || echo "Root directory empty or not accessible"

echo ""
echo "6. CHECKING AIRFLOW DAGS"
echo "----------------------------------------"
echo "DAG status:"
docker compose exec airflow-webserver airflow dags list

echo ""
echo "DAG state:"
docker compose exec airflow-webserver airflow dags state stock_batch_pipeline 2>/dev/null || echo "DAG not found or not running"

echo ""
echo "7. SPARK STREAMING JOB STATUS"
echo "----------------------------------------"
echo "Recent Spark Streaming logs (last 20 lines):"
docker compose logs --tail=20 spark-streaming

echo ""
echo "8. PRODUCER STATUS"
echo "----------------------------------------"
echo "Recent Producer logs (last 10 lines):"
docker compose logs --tail=10 stock-producer

echo ""
echo "=========================================="
echo "MANUAL VERIFICATION STEPS"
echo "=========================================="
echo ""
echo "🌐 Web Interfaces to check:"
echo "   • Spark Master:    http://localhost:8080"
echo "   • Spark Worker:    http://localhost:8081"  
echo "   • Airflow:         http://localhost:8082 (admin/admin)"
echo "   • Hadoop NameNode: http://localhost:9870"
echo ""
echo "🔧 Useful commands:"
echo "   • Watch Kafka messages:    docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock_ticks"
echo "   • Trigger Airflow DAG:     docker compose exec airflow-webserver airflow dags trigger stock_batch_pipeline"
echo "   • Check HDFS files:        docker compose exec namenode hdfs dfs -ls /stock_data/"
echo "   • Spark job status:        docker compose logs -f spark-streaming"
echo "   • Producer logs:           docker compose logs -f stock-producer"
echo ""
echo "🐛 Troubleshooting:"
echo "   • Restart producer:        docker compose restart stock-producer"
echo "   • Restart Spark streaming: docker compose restart spark-streaming"
echo "   • Rebuild Airflow:         docker compose build airflow-webserver && docker compose up -d airflow-webserver"