-- init.sql: Initialize PostgreSQL for HFT Analytics

-- Strategy performance tracking
CREATE TABLE IF NOT EXISTS strategy_performance (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    strategy_name VARCHAR(50) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    total_return DECIMAL(10,4),
    sharpe_ratio DECIMAL(10,4),
    max_drawdown DECIMAL(10,4),
    win_rate DECIMAL(5,2),
    total_trades INTEGER,
    avg_trade_duration_minutes INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trading parameters for each symbol
CREATE TABLE IF NOT EXISTS trading_params (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    strategy_name VARCHAR(50) NOT NULL,
    ma_short_period INTEGER DEFAULT 5,
    ma_long_period INTEGER DEFAULT 20,
    buy_threshold DECIMAL(5,4) DEFAULT 0.01,
    sell_threshold DECIMAL(5,4) DEFAULT -0.01,
    max_position INTEGER DEFAULT 1000,
    risk_limit DECIMAL(10,2) DEFAULT 10000.00,
    active BOOLEAN DEFAULT true,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, strategy_name)
);

-- Trade execution log
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    strategy_name VARCHAR(50) NOT NULL,
    action VARCHAR(10) NOT NULL, -- BUY, SELL, HOLD
    quantity INTEGER NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    signal_confidence DECIMAL(3,2), -- 0.00 to 1.00
    portfolio_value DECIMAL(12,2),
    cash_balance DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Portfolio positions
CREATE TABLE IF NOT EXISTS positions (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL UNIQUE,
    quantity INTEGER DEFAULT 0,
    avg_cost DECIMAL(10,2) DEFAULT 0.00,
    current_price DECIMAL(10,2),
    market_value DECIMAL(12,2),
    unrealized_pnl DECIMAL(12,2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Risk metrics
CREATE TABLE IF NOT EXISTS risk_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    portfolio_value DECIMAL(12,2),
    cash_balance DECIMAL(12,2),
    total_exposure DECIMAL(12,2),
    var_95 DECIMAL(12,2), -- Value at Risk 95%
    max_drawdown DECIMAL(12,2),
    beta DECIMAL(5,3),
    correlation_spy DECIMAL(5,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Backtesting results
CREATE TABLE IF NOT EXISTS backtest_results (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(50) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    initial_capital DECIMAL(12,2),
    final_value DECIMAL(12,2),
    total_return DECIMAL(8,4),
    annual_return DECIMAL(8,4),
    volatility DECIMAL(8,4),
    sharpe_ratio DECIMAL(8,4),
    max_drawdown DECIMAL(8,4),
    calmar_ratio DECIMAL(8,4),
    win_rate DECIMAL(5,2),
    total_trades INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default trading parameters
INSERT INTO trading_params (symbol, strategy_name, ma_short_period, ma_long_period, buy_threshold, sell_threshold, max_position, risk_limit)
VALUES 
    ('AAPL', 'momentum_crossover', 5, 20, 0.02, -0.02, 100, 5000.00),
    ('GOOGL', 'momentum_crossover', 5, 20, 0.02, -0.02, 50, 5000.00),
    ('TSLA', 'momentum_crossover', 3, 15, 0.03, -0.03, 25, 3000.00)
ON CONFLICT (symbol, strategy_name) DO NOTHING;

-- Insert initial portfolio state
INSERT INTO positions (symbol, quantity, avg_cost, current_price, market_value, unrealized_pnl)
VALUES 
    ('CASH', 1, 50000.00, 1.00, 50000.00, 0.00),
    ('AAPL', 0, 0.00, 150.00, 0.00, 0.00),
    ('GOOGL', 0, 0.00, 2500.00, 0.00, 0.00)
ON CONFLICT (symbol) DO NOTHING;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_strategy_performance_date ON strategy_performance(date);
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);