from __future__ import annotations


def build_schema_sql() -> str:
    return """
CREATE TABLE IF NOT EXISTS stock_symbol (
    symbol_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    symbol CHAR(9) NOT NULL,
    code CHAR(6) NOT NULL,
    market TINYINT UNSIGNED NOT NULL COMMENT '1=SH,2=SZ,3=BJ',
    name VARCHAR(32) NULL,
    dat_path VARCHAR(255) NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol_id),
    UNIQUE KEY uk_symbol (symbol),
    KEY idx_market_code (market, code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_bar_1m (
    trade_date DATE NOT NULL,
    minute_slot SMALLINT UNSIGNED NOT NULL COMMENT '571=09:31,900=15:00',
    symbol_id INT UNSIGNED NOT NULL,
    open FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    close FLOAT NOT NULL,
    volume INT UNSIGNED NOT NULL,
    amount_k INT UNSIGNED NOT NULL COMMENT '成交额按1000缩放',
    PRIMARY KEY (trade_date, minute_slot, symbol_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_bar_1m_import_state (
    symbol_id INT UNSIGNED NOT NULL,
    dat_path VARCHAR(255) NOT NULL,
    last_file_size BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_file_mtime BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_bar_trade_date DATE NULL,
    last_bar_minute_slot SMALLINT UNSIGNED NULL,
    last_import_mode TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '1=full,2=incremental',
    last_status TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '0=idle,1=running,2=success,3=failed',
    last_rows_affected INT UNSIGNED NOT NULL DEFAULT 0,
    last_error VARCHAR(500) NULL,
    last_started_at DATETIME NULL,
    last_finished_at DATETIME NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_bar_import_job (
    job_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_type TINYINT UNSIGNED NOT NULL COMMENT '1=full,2=incremental',
    status TINYINT UNSIGNED NOT NULL COMMENT '1=running,2=success,3=partial_success,4=failed',
    symbol_count INT UNSIGNED NOT NULL DEFAULT 0,
    success_symbol_count INT UNSIGNED NOT NULL DEFAULT 0,
    failed_symbol_count INT UNSIGNED NOT NULL DEFAULT 0,
    inserted_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME NULL,
    note VARCHAR(500) NULL,
    PRIMARY KEY (job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_pool (
    pool_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    owner_key VARCHAR(64) NOT NULL,
    pool_name VARCHAR(128) NOT NULL,
    description VARCHAR(500) NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (pool_id),
    UNIQUE KEY uk_owner_pool_name (owner_key, pool_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_pool_symbol (
    pool_id BIGINT UNSIGNED NOT NULL,
    symbol_id INT UNSIGNED NOT NULL,
    sort_order INT UNSIGNED NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pool_id, symbol_id),
    KEY idx_symbol_id (symbol_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_qfq_factor (
    symbol_id INT UNSIGNED NOT NULL,
    trade_date DATE NOT NULL,
    factor FLOAT NOT NULL COMMENT '当次前复权变动因子，非累计值',
    source_file_mtime BIGINT UNSIGNED NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol_id, trade_date),
    KEY idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()
