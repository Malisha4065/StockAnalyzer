# dags/stock_batch_dag.py - HFT Batch Analytics Orchestration
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import redis
import json
import logging
import os
import sys
from importlib import import_module

log = logging.getLogger(__name__)

def run_batch_analytics():
    """Trigger Spark batch analytics job"""
    try:
        log.info("Starting HFT batch analytics...")
        # Allow forcing local mode for batch independently without changing streaming job
        if os.environ.get("BATCH_LOCAL_MODE", "0") == "1":
            os.environ["SPARK_MASTER_URL"] = "local[*]"
            log.info("BATCH_LOCAL_MODE=1 -> Using local[*] for batch Spark session")
        else:
            os.environ.setdefault("SPARK_MASTER_URL", "spark://spark-master:7077")
        os.environ.setdefault("HDFS_NAMENODE", "hdfs://namenode:9000")

        # Ensure project root (mounted at /app) is importable inside Airflow containers
        project_root = os.environ.get("SPARK_APP_DIR", "/app")
        if project_root not in sys.path:
            sys.path.insert(0, project_root)

        spark_batch_module = os.environ.get("SPARK_BATCH_MODULE", "spark_batch")
        spark_batch_attr = os.environ.get("SPARK_BATCH_ENTRYPOINT", "main")

        module = import_module(spark_batch_module)
        # Force a reload so iterative debugging changes in mounted volume are reflected
        try:
            import importlib
            module = importlib.reload(module)
            log.info("Reloaded module %s", spark_batch_module)
        except Exception as re:
            log.warning("Could not reload module %s: %s (continuing with initial import)", spark_batch_module, re)
        entrypoint = getattr(module, spark_batch_attr, None)

        if entrypoint is None:
            raise AttributeError(
                f"Entrypoint '{spark_batch_attr}' not found in module '{spark_batch_module}'"
            )

        log.info("Invoking %s.%s", spark_batch_module, spark_batch_attr)
        entrypoint()
        log.info("Batch analytics completed successfully")
            
    except Exception as e:
        log.error(f"Error in batch analytics: {e}")
        raise

def update_trading_parameters():
    """Update trading parameters from analytics results"""
    try:
        log.info("Updating trading parameters...")
        
        # Connect to PostgreSQL to get optimized parameters
        conn = psycopg2.connect(
            host="postgres",
            database="airflow", 
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        
        # Get latest optimized parameters
        cursor.execute("""
            SELECT symbol, buy_threshold, sell_threshold, max_position
            FROM trading_params 
            WHERE active = true
        """)
        
        parameters = cursor.fetchall()
        
        # Update Redis with new parameters
        redis_client = redis.from_url('redis://redis:6379/1', decode_responses=True)
        
        for symbol, buy_thresh, sell_thresh, max_pos in parameters:
            param_data = {
                "buy_threshold": float(buy_thresh),
                "sell_threshold": float(sell_thresh),
                "max_position": int(max_pos),
                "updated": datetime.now().isoformat()
            }
            redis_client.setex(f"params:{symbol}", 3600*24, json.dumps(param_data))
            log.info(f"Updated parameters for {symbol}: {param_data}")
        
        cursor.close()
        conn.close()
        
        log.info(f"Updated parameters for {len(parameters)} symbols")
        
    except Exception as e:
        log.error(f"Error updating parameters: {e}")
        raise

def generate_performance_report():
    """Generate daily performance report"""
    try:
        log.info("Generating performance report...")
        
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow", 
            password="airflow"
        )
        cursor = conn.cursor()
        
        # Get today's performance
        cursor.execute("""
            SELECT symbol, total_return, sharpe_ratio, total_trades, win_rate
            FROM strategy_performance 
            WHERE date = CURRENT_DATE
        """)
        
        results = cursor.fetchall()
        
        # Calculate summary statistics
        if results:
            total_return = sum([r[1] for r in results])
            avg_sharpe = sum([r[2] for r in results]) / len(results)
            total_trades = sum([r[3] for r in results])
            avg_win_rate = sum([r[4] for r in results]) / len(results)
            
            report = f"""
            HFT Performance Report - {datetime.now().strftime('%Y-%m-%d')}
            ================================================
            
            Portfolio Performance:
            - Total Return: {total_return:.4f} ({total_return*100:.2f}%)
            - Average Sharpe Ratio: {avg_sharpe:.2f}
            - Total Trades: {total_trades}
            - Average Win Rate: {avg_win_rate:.2f}%
            
            Individual Symbol Performance:
            """
            
            for symbol, ret, sharpe, trades, win_rate in results:
                report += f"\n{symbol}: Return={ret:.4f}, Sharpe={sharpe:.2f}, Trades={trades}, Win Rate={win_rate:.1f}%"
            
            log.info(report)
            
            # Store report in Redis for dashboard access
            redis_client = redis.from_url('redis://redis:6379/0', decode_responses=True)
            redis_client.setex("daily_report", 3600*24, report)
        else:
            log.info("No performance data available for today")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        log.error(f"Error generating report: {e}")
        raise

def cleanup_old_data():
    """Clean up old data to maintain performance"""
    try:
        log.info("Cleaning up old data...")
        
        # Clean up old Redis signals
        redis_client = redis.from_url('redis://redis:6379/1', decode_responses=True)
        redis_client.ltrim("signal_history", 0, 500)  # Keep last 500 signals
        
        # Clean up old PostgreSQL data (keep last 90 days)
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM trades 
            WHERE timestamp < NOW() - INTERVAL '90 days'
        """)
        
        cursor.execute("""
            DELETE FROM strategy_performance 
            WHERE date < CURRENT_DATE - INTERVAL '90 days'
        """)
        
        deleted_trades = cursor.rowcount
        conn.commit()
        
        cursor.close()
        conn.close()
        
        log.info(f"Cleaned up {deleted_trades} old records")
        
    except Exception as e:
        log.error(f"Error in cleanup: {e}")
        raise

# DAG Definition
default_args = {
    "owner": "hft_system",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 9, 26),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="hft_daily_analytics",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # Run at 2 AM daily (after market close)
    catchup=False,
    description="HFT Daily Analytics and Optimization Pipeline",
    tags=["hft", "analytics", "batch"]
) as dag:
    
    # Task 1: Wait for HDFS to be ready (inherited from original)
    wait_for_hdfs = BashOperator(
        task_id='wait_for_hdfs',
        bash_command="""
        echo "Waiting for HDFS to exit safemode..."
        until curl -s http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo | grep -q '"Safemode" : ""'; do
            echo "HDFS is still in safemode, waiting 10 seconds..."
            sleep 10
        done
        echo "HDFS is ready."
        """,
    )
    
    # Task 2: Run batch analytics and optimization
    run_analytics = PythonOperator(
        task_id="run_batch_analytics",
        python_callable=run_batch_analytics
    )
    
    # Task 3: Update trading parameters from analytics results
    update_params = PythonOperator(
        task_id="update_trading_parameters",
        python_callable=update_trading_parameters
    )
    
    # Task 4: Generate performance report
    generate_report = PythonOperator(
        task_id="generate_performance_report",
        python_callable=generate_performance_report
    )
    
    # Task 5: Cleanup old data
    cleanup = PythonOperator(
        task_id="cleanup_old_data",
        python_callable=cleanup_old_data
    )
    
    # Define task dependencies
    wait_for_hdfs >> run_analytics >> [update_params, generate_report] >> cleanup