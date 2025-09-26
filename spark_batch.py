# spark_batch.py - HFT Backtesting and Analytics Engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, avg, stddev, max as spark_max, min as spark_min, count, sum as spark_sum, lit
from pyspark.sql.window import Window
import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta

def create_spark_session():
    """Create Spark session optimized for batch analytics"""
    master_url = os.environ.get("SPARK_MASTER_URL", "local[*]")
    hdfs_url = os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000")
    
    builder = (SparkSession.builder
        .appName("HFT_Batch_Analytics")
        .master(master_url)
        .config("spark.hadoop.fs.defaultFS", hdfs_url)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))

    # In containerized cluster mode, ensure proper driver host / bind if provided
    driver_host = os.environ.get("SPARK_DRIVER_HOST")
    if driver_host:
        builder = builder.config("spark.driver.host", driver_host)
    driver_bind = os.environ.get("SPARK_DRIVER_BIND_ADDRESS")
    if driver_bind:
        builder = builder.config("spark.driver.bindAddress", driver_bind)

    return builder.getOrCreate()

def get_postgres_connection():
    """Get PostgreSQL connection for storing analytics results"""
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow", 
        password="airflow",
        port=5432
    )

def backtest_momentum_strategy(spark, signals_df):
    """Backtest the momentum crossover strategy"""
    print("Running momentum strategy backtest...")
    
    # Define window for calculations
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")
    
    # Calculate strategy returns
    backtest_df = signals_df \
        .withColumn("prev_price", lag("price", 1).over(window_spec)) \
        .withColumn("price_return", 
            when(col("prev_price").isNotNull(), 
                (col("price") - col("prev_price")) / col("prev_price"))
            .otherwise(0.0)) \
        .withColumn("strategy_return",
            when(col("action") == "BUY", col("price_return"))
            .when(col("action") == "SELL", -col("price_return"))
            .otherwise(0.0)) \
        .withColumn("cumulative_return", 
            spark_sum("strategy_return").over(
                window_spec.rowsBetween(Window.unboundedPreceding, 0)
            ))
    
    # Calculate performance metrics by symbol
    performance_df = backtest_df.groupBy("symbol").agg(
        spark_sum("strategy_return").alias("total_return"),
        avg("strategy_return").alias("avg_return"),
        stddev("strategy_return").alias("volatility"),
        count("*").alias("total_trades"),
        spark_max("cumulative_return").alias("peak_return"),
        spark_min("cumulative_return").alias("trough_return")
    ).withColumn("sharpe_ratio",
        when(col("volatility") > 0, col("avg_return") / col("volatility"))
        .otherwise(0.0)
    ).withColumn("max_drawdown",
        col("peak_return") - col("trough_return")
    ).withColumn("win_rate",
        # This is simplified - in reality would count profitable trades
        when(col("avg_return") > 0, lit(0.6)).otherwise(lit(0.4))
    )
    
    return performance_df

def calculate_portfolio_risk(spark, positions_df, prices_df):
    """Calculate portfolio risk metrics"""
    print("Calculating portfolio risk metrics...")
    
    # Join positions with current prices
    portfolio_df = positions_df.join(prices_df, "symbol", "left") \
        .withColumn("market_value", col("quantity") * col("price")) \
        .withColumn("weight", col("market_value") / spark_sum("market_value").over(Window.partitionBy()))
    
    # Calculate portfolio volatility (simplified)
    risk_metrics = portfolio_df.agg(
        spark_sum("market_value").alias("total_value"),
        avg("price").alias("avg_price"),
        stddev("price").alias("portfolio_volatility")
    ).withColumn("var_95", col("portfolio_volatility") * 1.65) \
     .withColumn("date", lit(datetime.now().date()))
    
    return risk_metrics

def optimize_trading_parameters(spark, historical_signals):
    """Optimize trading parameters based on historical performance"""
    print("Optimizing trading parameters...")
    
    # Test different momentum thresholds
    optimization_results = []
    
    for buy_threshold in [1.0, 1.5, 2.0, 2.5, 3.0]:
        for sell_threshold in [-1.0, -1.5, -2.0, -2.5, -3.0]:
            # Recalculate signals with new thresholds
            optimized_signals = historical_signals \
                .withColumn("new_signal",
                    when(col("price_momentum") > buy_threshold, "BUY")
                    .when(col("price_momentum") < sell_threshold, "SELL")
                    .otherwise("HOLD")
                )
            
            # Calculate performance with new parameters
            performance = backtest_momentum_strategy(spark, optimized_signals)
            
            # Store results
            optimization_results.append({
                "buy_threshold": buy_threshold,
                "sell_threshold": sell_threshold,
                "performance": performance.collect()
            })
    
    return optimization_results

def store_results_in_postgres(performance_results, risk_metrics, optimization_results):
    """Store analytics results in PostgreSQL"""
    print("Storing results in PostgreSQL...")
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        # Store performance results
        for result in performance_results:
            cursor.execute("""
                INSERT INTO strategy_performance 
                (date, strategy_name, symbol, total_return, sharpe_ratio, max_drawdown, win_rate, total_trades)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, strategy_name, symbol) DO UPDATE SET
                total_return = EXCLUDED.total_return,
                sharpe_ratio = EXCLUDED.sharpe_ratio,
                max_drawdown = EXCLUDED.max_drawdown,
                win_rate = EXCLUDED.win_rate,
                total_trades = EXCLUDED.total_trades
            """, (
                datetime.now().date(),
                "momentum_crossover",
                result.symbol,
                float(result.total_return),
                float(result.sharpe_ratio),
                float(result.max_drawdown),
                float(result.win_rate),
                int(result.total_trades)
            ))
        
        # Store risk metrics
        for risk in risk_metrics:
            cursor.execute("""
                INSERT INTO risk_metrics 
                (date, portfolio_value, var_95, max_drawdown)
                VALUES (%s, %s, %s, %s)
            """, (
                risk.date,
                float(risk.total_value),
                float(risk.var_95),
                0.0  # Simplified
            ))
        
        # Store optimized parameters
        best_params = min(optimization_results, 
                         key=lambda x: -max([p.sharpe_ratio for p in x['performance']]))
        
        cursor.execute("""
            UPDATE trading_params SET
            buy_threshold = %s,
            sell_threshold = %s,
            updated_at = CURRENT_TIMESTAMP
            WHERE strategy_name = 'momentum_crossover'
        """, (
            best_params['buy_threshold'],
            abs(best_params['sell_threshold'])
        ))
        
        conn.commit()
        print("Results stored successfully in PostgreSQL!")
        
    except Exception as e:
        print(f"Error storing results: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def main():
    """Main batch analytics pipeline"""
    print("Starting HFT Batch Analytics Pipeline...")
    
    spark = create_spark_session()
    hdfs_namenode = os.getenv('HDFS_NAMENODE', 'hdfs://localhost:9000')
    
    try:
        # Check if HDFS data exists before trying to read using Hadoop FS API for fast existence check
        print("Checking for historical signals in HDFS...")
        signals_path = f"{hdfs_namenode}/stock_data/signals"
        
        # Use Spark's underlying Hadoop FileSystem API to check path
        # This avoids the "Wrong FS" error by using the session's configured filesystem
        sc = spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
        path_obj = sc._jvm.org.apache.hadoop.fs.Path(signals_path)
        
        if not fs.exists(path_obj):
            print(f"Signals path does not exist yet: {signals_path}")
            print("Run streaming job longer to accumulate data before analytics.")
            return
        
        # Read signals
        signals_df = spark.read.parquet(signals_path)
        # Normalize/align schema if needed: streaming writes columns: symbol, signal, confidence, current_price, ma_short, ma_long, price_momentum, window
        # Map to expected columns: symbol, action, price, timestamp
        if 'current_price' in signals_df.columns and 'signal' in signals_df.columns:
            from pyspark.sql.functions import col as _col
            if 'timestamp' not in signals_df.columns and 'window' in signals_df.columns:
                # Window is a struct with start/end; use end as representative timestamp
                from pyspark.sql.functions import col as _col, to_timestamp
                signals_df = signals_df.withColumn('timestamp', _col('window').getField('end'))
            signals_df = signals_df.withColumnRenamed('signal', 'action') \
                                   .withColumnRenamed('current_price', 'price')
        row_count = signals_df.count()
        print(f"Found {row_count} historical signals in HDFS after normalization")
        MIN_ROWS = int(os.getenv('MIN_BATCH_ROWS', '50'))
        if row_count < MIN_ROWS:
            print(f"Insufficient data (<{MIN_ROWS} rows). Current: {row_count}. Exiting early.")
            return
        
        # Read historical price data  
        try:
            prices_df = spark.read.parquet(f"{hdfs_namenode}/stock_data/batch")
            print("Loaded batch price data from HDFS")
        except:
            print("No batch price data found, using signals data for prices")
            prices_df = signals_df.select("symbol", "price", "timestamp").distinct()
        
        # Run backtesting
        performance_results = backtest_momentum_strategy(spark, signals_df).collect()
        
        # Calculate risk metrics
        risk_metrics = calculate_portfolio_risk(spark, 
                                              spark.createDataFrame([("AAPL", 100), ("GOOGL", 50)], ["symbol", "quantity"]),
                                              prices_df.groupBy("symbol").agg(avg("price").alias("price"))).collect()
        
        # Optimize parameters
        optimization_results = optimize_trading_parameters(spark, signals_df)
        
        # Store results in PostgreSQL
        store_results_in_postgres(performance_results, risk_metrics, optimization_results)
        
        print("\n=== BATCH ANALYTICS COMPLETE ===")
        print(f"Analyzed {len(performance_results)} symbols")
        print(f"Tested {len(optimization_results)} parameter combinations")
        print("Results stored in PostgreSQL for trading system consumption")
        
    except Exception as e:
        print(f"Error in batch analytics: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
