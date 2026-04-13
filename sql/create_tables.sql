CREATE TABLE IF NOT EXISTS stock_prices_raw (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    symbol VARCHAR(16) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(18, 6) NOT NULL,
    high DECIMAL(18, 6) NOT NULL,
    low DECIMAL(18, 6) NOT NULL,
    close DECIMAL(18, 6) NOT NULL,
    adjusted_close DECIMAL(18, 6) NOT NULL,
    volume BIGINT UNSIGNED NOT NULL,
    dividend_amount DECIMAL(18, 6) NOT NULL DEFAULT 0,
    split_coefficient DECIMAL(18, 6) NOT NULL DEFAULT 1,
    source_loaded_at DATETIME NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_stock_prices_raw_symbol_trade_date (symbol, trade_date),
    KEY idx_stock_prices_raw_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    symbol VARCHAR(16) NOT NULL,
    status VARCHAR(32) NOT NULL,
    rows_loaded INT UNSIGNED NOT NULL DEFAULT 0,
    error_message TEXT NULL,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_pipeline_run_log_symbol_started_at (symbol, started_at),
    KEY idx_pipeline_run_log_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
