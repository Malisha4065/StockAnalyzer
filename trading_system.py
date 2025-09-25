# trading_system.py - HFT Trading Execution Engine
import redis
import psycopg2
import json
import time
import logging
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HFTTradingSystem:
    def __init__(self):
        # Redis connection for signals and parameters
        self.redis_signals = redis.from_url(
            os.getenv('REDIS_URL', 'redis://localhost:6379/1'), 
            decode_responses=True
        )
        
        # PostgreSQL connection for trade logging and positions
        self.postgres_url = os.getenv('POSTGRES_URL', 'postgresql://airflow:airflow@localhost:5432/airflow')
        
        # Trading parameters
        self.initial_capital = 50000.00
        self.max_position_size = 1000
        self.risk_per_trade = 0.02  # 2% risk per trade
        
        # Track positions and cash
        self.positions = {}  # symbol -> quantity
        self.cash_balance = self.initial_capital
        self.portfolio_value = self.initial_capital
        
        logger.info("HFT Trading System initialized")
        logger.info(f"Initial capital: ${self.initial_capital:,.2f}")

    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        return psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow",
            port=5432
        )

    def load_trading_parameters(self, symbol):
        """Load trading parameters from Redis"""
        try:
            params_data = self.redis_signals.get(f"params:{symbol}")
            if params_data:
                return json.loads(params_data)
            else:
                # Default parameters
                return {
                    "buy_threshold": 0.02,
                    "sell_threshold": 0.02,
                    "max_position": 100
                }
        except Exception as e:
            logger.error(f"Error loading parameters for {symbol}: {e}")
            return {
                "buy_threshold": 0.02,
                "sell_threshold": 0.02,
                "max_position": 100
            }

    def calculate_position_size(self, price, confidence, max_position):
        """Calculate position size based on price, confidence, and risk management"""
        # Base position size on available cash and risk tolerance
        max_dollar_amount = self.cash_balance * self.risk_per_trade
        base_position = int(max_dollar_amount / price)
        
        # Adjust by confidence (0.0 to 1.0)
        adjusted_position = int(base_position * confidence)
        
        # Respect maximum position limits
        final_position = min(adjusted_position, max_position)
        
        return max(final_position, 1) if final_position > 0 else 0

    def execute_trade(self, symbol, action, quantity, price, signal_data):
        """Execute a trade and log it"""
        try:
            current_position = self.positions.get(symbol, 0)
            trade_value = quantity * price
            
            # Risk checks
            if action == "BUY":
                if trade_value > self.cash_balance:
                    logger.warning(f"Insufficient cash for {symbol} BUY: need ${trade_value:.2f}, have ${self.cash_balance:.2f}")
                    return False
                
                # Update positions and cash
                self.positions[symbol] = current_position + quantity
                self.cash_balance -= trade_value
                
            elif action == "SELL":
                if current_position <= 0:
                    logger.warning(f"Cannot SELL {symbol}: no position held")
                    return False
                
                # Determine how much to sell
                sell_quantity = min(quantity, current_position)
                sell_value = sell_quantity * price
                
                # Update positions and cash
                self.positions[symbol] = current_position - sell_quantity
                self.cash_balance += sell_value
                
                quantity = sell_quantity  # Update for logging
                trade_value = sell_value
            
            # Log trade to PostgreSQL
            self.log_trade(symbol, action, quantity, price, signal_data)
            
            # Update portfolio value
            self.update_portfolio_value()
            
            logger.info(f"EXECUTED: {action} {quantity} {symbol} @ ${price:.2f} = ${trade_value:.2f}")
            logger.info(f"Position: {self.positions.get(symbol, 0)}, Cash: ${self.cash_balance:.2f}, Portfolio: ${self.portfolio_value:.2f}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error executing trade: {e}")
            return False

    def log_trade(self, symbol, action, quantity, price, signal_data):
        """Log trade to PostgreSQL"""
        try:
            conn = self.get_postgres_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO trades 
                (symbol, strategy_name, action, quantity, price, timestamp, signal_confidence, portfolio_value, cash_balance)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                symbol,
                "momentum_crossover",
                action,
                quantity,
                float(price),
                datetime.now(),
                float(signal_data.get('confidence', 0.0)),
                float(self.portfolio_value),
                float(self.cash_balance)
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error logging trade: {e}")

    def update_portfolio_value(self):
        """Update total portfolio value"""
        try:
            position_value = 0.0
            
            for symbol, quantity in self.positions.items():
                if quantity > 0:
                    # Get current price from Redis
                    live_price_data = self.redis_signals.hgetall(f"live_prices:{symbol}")
                    if live_price_data and 'price' in live_price_data:
                        current_price = float(live_price_data['price'])
                        position_value += quantity * current_price
            
            self.portfolio_value = self.cash_balance + position_value
            
        except Exception as e:
            logger.error(f"Error updating portfolio value: {e}")

    def update_positions_in_database(self):
        """Update positions table in PostgreSQL"""
        try:
            conn = self.get_postgres_connection()
            cursor = conn.cursor()
            
            for symbol, quantity in self.positions.items():
                # Get current price
                live_price_data = self.redis_signals.hgetall(f"live_prices:{symbol}")
                current_price = float(live_price_data.get('price', 0.0)) if live_price_data else 0.0
                market_value = quantity * current_price
                
                cursor.execute("""
                    INSERT INTO positions (symbol, quantity, current_price, market_value, last_updated)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    current_price = EXCLUDED.current_price,
                    market_value = EXCLUDED.market_value,
                    last_updated = EXCLUDED.last_updated
                """, (
                    symbol, quantity, current_price, market_value, datetime.now()
                ))
            
            # Update cash position
            cursor.execute("""
                INSERT INTO positions (symbol, quantity, current_price, market_value, last_updated)
                VALUES ('CASH', 1, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                current_price = EXCLUDED.current_price,
                market_value = EXCLUDED.market_value,
                last_updated = EXCLUDED.last_updated
            """, (self.cash_balance, self.cash_balance, datetime.now()))
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error updating positions in database: {e}")

    def process_signal(self, symbol, signal_data):
        """Process a trading signal"""
        try:
            action = signal_data.get('action')
            confidence = float(signal_data.get('confidence', 0.0))
            price = float(signal_data.get('price', 0.0))
            
            if action in ['BUY', 'SELL'] and confidence > 0.3:  # Minimum confidence threshold
                # Load trading parameters
                params = self.load_trading_parameters(symbol)
                
                # Calculate position size
                position_size = self.calculate_position_size(
                    price, confidence, params['max_position']
                )
                
                if position_size > 0:
                    # Execute the trade
                    success = self.execute_trade(symbol, action, position_size, price, signal_data)
                    return success
                else:
                    logger.info(f"Position size too small for {symbol} {action}")
            else:
                logger.debug(f"Signal ignored for {symbol}: action={action}, confidence={confidence}")
                
        except Exception as e:
            logger.error(f"Error processing signal for {symbol}: {e}")
        
        return False

    def run(self):
        """Main trading loop"""
        logger.info("Starting HFT Trading System...")
        logger.info("Monitoring Redis for trading signals...")
        
        symbols = ['AAPL', 'GOOGL', 'TSLA', 'MSFT', 'AMZN']
        last_update = time.time()
        
        while True:
            try:
                # Check for signals for each symbol
                signals_processed = 0
                
                for symbol in symbols:
                    signal_key = f"signals:{symbol}"
                    signal_data_raw = self.redis_signals.get(signal_key)
                    
                    if signal_data_raw:
                        signal_data = json.loads(signal_data_raw)
                        
                        # Process the signal
                        if self.process_signal(symbol, signal_data):
                            signals_processed += 1
                
                # Update positions in database every 30 seconds
                if time.time() - last_update > 30:
                    self.update_positions_in_database()
                    last_update = time.time()
                    
                    if signals_processed > 0:
                        logger.info(f"Processed {signals_processed} signals in last cycle")
                
                # Sleep for short interval (high frequency)
                time.sleep(0.5)  # 500ms cycle time
                
            except KeyboardInterrupt:
                logger.info("Trading system stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in main trading loop: {e}")
                time.sleep(5)  # Wait before retrying
        
        # Final portfolio update
        self.update_positions_in_database()
        logger.info("HFT Trading System stopped")
        logger.info(f"Final portfolio value: ${self.portfolio_value:.2f}")

if __name__ == "__main__":
    trading_system = HFTTradingSystem()
    trading_system.run()