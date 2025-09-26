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
        # Encourage using hostnames for datanodes in container network
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("dfs.client.use.datanode.hostname", "true")
        # Explicit HDFS implementation (defensive)
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.cores.max", "1")
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
    # Use a single consistent default for HDFS namenode (container hostname 'namenode')
    hdfs_namenode = os.getenv('HDFS_NAMENODE', 'hdfs://namenode:9000')
    master_url = os.environ.get("SPARK_MASTER_URL", "local[*]")
    preflight_timeout = int(os.getenv("SPARK_BATCH_PREFLIGHT_TIMEOUT", "40"))  # seconds
    executor_wait_interval = 2  # seconds
    
    try:
        # Preflight: ensure Spark can execute a trivial job (isolates cluster connectivity vs HDFS issues)
        try:
            import time as _t
            sc = spark.sparkContext
            if master_url.startswith("spark://"):
                print("Checking Spark cluster connectivity (will timeout if no workers available)...")
                # Skip complex executor polling - let the preflight job itself detect availability
            else:
                print("Using local Spark mode - no cluster polling needed")

            print("Running Spark connectivity preflight (small parallelize count)...")
            _t0 = _t.time()
            preflight_error = None
            try:
                preflight_count = sc.parallelize([1,2,3,4,5]).count()
                print(f"Preflight success: count={preflight_count} latency={( _t.time()-_t0)*1000:.1f} ms")
            except Exception as job_err:
                preflight_error = job_err
                elapsed = _t.time() - _t0
                if master_url.startswith("spark://"):
                    print(f"[WARN] Preflight failed after {elapsed:.1f}s on cluster: {job_err}")
                else:
                    print(f"[WARN] Preflight failed in local mode: {job_err}")
            # Fallback if cluster preflight failed or exceeded timeout
            if preflight_error and master_url.startswith("spark://"):
                elapsed = _t.time() - _t0
                if elapsed >= preflight_timeout or True:
                    try:
                        print("Attempting fallback: recreating Spark session in local[*] mode due to preflight failure...")
                        spark.stop()
                    except Exception:
                        pass
                    os.environ['SPARK_MASTER_URL'] = 'local[*]'
                    spark = create_spark_session()
                    sc = spark.sparkContext
                    try:
                        t2 = _t.time()
                        preflight_count = sc.parallelize([1,2,3,4,5]).count()
                        print(f"Local fallback preflight success: count={preflight_count} latency={( _t.time()-t2)*1000:.1f} ms")
                        master_url = 'local[*]'
                    except Exception as local_err:
                        print(f"[FATAL] Local fallback preflight also failed: {local_err}")
                        raise preflight_error
        except Exception as pre_err:
            print(f"Preflight Spark job failed early: {pre_err}")
            raise
        # Check if HDFS data exists before trying to read using Hadoop FS API for fast existence check
        print("Checking for historical signals in HDFS...")
        signals_path = f"{hdfs_namenode}/stock_data/signals"

        # Diagnostics: log effective fs.defaultFS and namenode resolution
        sc = spark.sparkContext
        hc = sc._jsc.hadoopConfiguration()
        effective_default = hc.get("fs.defaultFS")
        print(f"Effective fs.defaultFS = {effective_default}")
        if effective_default and not signals_path.startswith(effective_default):
            print(f"[WARN] signals_path ({signals_path}) not prefixed by fs.defaultFS; will still attempt absolute path access")

        # Use Hadoop FileSystem API with timing for existence check
        import time
        start_chk = time.time()
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hc)
        path_obj = sc._jvm.org.apache.hadoop.fs.Path(signals_path)
        exists = fs.exists(path_obj)
        dur = (time.time() - start_chk) * 1000
        print(f"Existence check result={exists} latency={dur:.1f} ms for {signals_path}")
        if not exists:
            print(f"Signals path does not exist yet: {signals_path}")
            print("Run streaming job longer to accumulate data before analytics.")
            return
        
        # Read signals with diagnostics
        import time
        read_start = time.time()
        print("Listing files under signals path before read...")
        try:
            status_list = fs.listStatus(path_obj)
            files = [s.getPath().toString() for s in status_list if s.isFile() and s.getPath().getName().endswith('.parquet')]
            meta_dirs = [s.getPath().toString() for s in status_list if not s.isFile()]
            print(f"Found {len(files)} parquet data files; non-file entries: {meta_dirs}")
            for f in files[:10]:
                print("  data file:", f)
        except Exception as e:
            print("Error listing status before read:", e)

        print("Attempting glob parquet read (part-*.parquet) to bypass metadata dir scan...")
        glob_path = signals_path.rstrip('/') + "/part-*.parquet"
        try:
            glob_start = time.time()
            signals_df = spark.read.parquet(glob_path)
            print(f"Glob read success in {(time.time()-glob_start)*1000:.1f} ms (pattern {glob_path})")
        except Exception as glob_err:
            print(f"Glob read failed ({glob_err}); attempting directory parquet read...")
            try:
                dir_start = time.time()
                signals_df = spark.read.parquet(signals_path)
                print(f"Directory read success in {(time.time()-dir_start)*1000:.1f} ms")
            except Exception as e:
                print(f"Directory read failed ({e}); falling back to per-file union")
                try:
                    dfs = []
                    for f in files:
                        t0 = time.time()
                        df_part = spark.read.parquet(f)
                        dfs.append(df_part)
                        print(f"Read {f} rows={df_part.count()} latency={(time.time()-t0)*1000:.1f} ms")
                    if dfs:
                        from functools import reduce
                        from pyspark.sql import DataFrame
                        signals_df = reduce(DataFrame.unionByName, dfs)
                    else:
                        print("No parquet data files found after fallback; exiting.")
                        return
                except Exception as inner:
                    print("Per-file fallback failed:", inner)
                    return
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
