-- Binance 公開資料庫架構設計（最終修正版）
-- 支援所有 Binance 公開資料類型的完整 PostgreSQL 架構

-- 創建專用的 schema
CREATE SCHEMA IF NOT EXISTS binance_data;

-- 設置默認 schema
SET search_path TO binance_data, public;

-- ==============================================
-- 1. 元數據表 (Metadata Tables)
-- ==============================================

-- 交易對資訊表
CREATE TABLE symbols (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL UNIQUE,
    base_asset VARCHAR(20) NOT NULL,
    quote_asset VARCHAR(20) NOT NULL,
    trading_type VARCHAR(10) NOT NULL CHECK (trading_type IN ('spot', 'um', 'cm')),
    status VARCHAR(20) DEFAULT 'TRADING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 創建索引
CREATE INDEX idx_symbols_trading_type ON symbols(trading_type);
CREATE INDEX idx_symbols_status ON symbols(status);

-- 資料同步狀態表
CREATE TABLE sync_status (
    id SERIAL PRIMARY KEY,
    symbol_id INTEGER REFERENCES symbols(id),
    data_type VARCHAR(50) NOT NULL,
    time_period VARCHAR(10) NOT NULL CHECK (time_period IN ('daily', 'monthly')),
    interval_type VARCHAR(10), -- K線資料的時間間隔
    last_sync_date DATE NOT NULL,
    last_sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_count BIGINT DEFAULT 0,
    file_format VARCHAR(10) DEFAULT '.csv',
    status VARCHAR(20) DEFAULT 'active',
    UNIQUE(symbol_id, data_type, time_period, interval_type)
);

-- 創建索引
CREATE INDEX idx_sync_status_symbol_data ON sync_status(symbol_id, data_type);
CREATE INDEX idx_sync_status_last_sync ON sync_status(last_sync_date);

-- ==============================================
-- 2. K線資料表 (Klines Data Tables)
-- ==============================================

-- 基本 K線資料表 (支援所有交易類型) - 修正分區鍵問題
CREATE TABLE klines (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    trading_type VARCHAR(10) NOT NULL,
    interval_type VARCHAR(10) NOT NULL,
    open_time BIGINT NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    volume DECIMAL(20,8) NOT NULL,
    close_time BIGINT NOT NULL,
    quote_asset_volume DECIMAL(20,8) NOT NULL,
    number_of_trades INTEGER NOT NULL,
    taker_buy_base_asset_volume DECIMAL(20,8) NOT NULL,
    taker_buy_quote_asset_volume DECIMAL(20,8) NOT NULL,
    data_source VARCHAR(20) DEFAULT 'binance',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, open_time)  -- 包含分區鍵的複合主鍵
) PARTITION BY RANGE (open_time);

-- 創建 2024 年的分區（使用毫秒時間戳）
-- 2024-01-01 00:00:00 UTC = 1704067200000 ms
-- 2024-02-01 00:00:00 UTC = 1706745600000 ms
-- 2024-03-01 00:00:00 UTC = 1709251200000 ms
-- 2024-04-01 00:00:00 UTC = 1711929600000 ms
-- 2024-05-01 00:00:00 UTC = 1714521600000 ms
-- 2024-06-01 00:00:00 UTC = 1717200000000 ms
-- 2024-07-01 00:00:00 UTC = 1719792000000 ms
-- 2024-08-01 00:00:00 UTC = 1722470400000 ms
-- 2024-09-01 00:00:00 UTC = 1725148800000 ms
-- 2024-10-01 00:00:00 UTC = 1727740800000 ms
-- 2024-11-01 00:00:00 UTC = 1730419200000 ms
-- 2024-12-01 00:00:00 UTC = 1733011200000 ms
-- 2025-01-01 00:00:00 UTC = 1735689600000 ms

CREATE TABLE klines_2024_01 PARTITION OF klines
    FOR VALUES FROM (1704067200000) TO (1706745600000);
CREATE TABLE klines_2024_02 PARTITION OF klines
    FOR VALUES FROM (1706745600000) TO (1709251200000);
CREATE TABLE klines_2024_03 PARTITION OF klines
    FOR VALUES FROM (1709251200000) TO (1711929600000);
CREATE TABLE klines_2024_04 PARTITION OF klines
    FOR VALUES FROM (1711929600000) TO (1714521600000);
CREATE TABLE klines_2024_05 PARTITION OF klines
    FOR VALUES FROM (1714521600000) TO (1717200000000);
CREATE TABLE klines_2024_06 PARTITION OF klines
    FOR VALUES FROM (1717200000000) TO (1719792000000);
CREATE TABLE klines_2024_07 PARTITION OF klines
    FOR VALUES FROM (1719792000000) TO (1722470400000);
CREATE TABLE klines_2024_08 PARTITION OF klines
    FOR VALUES FROM (1722470400000) TO (1725148800000);
CREATE TABLE klines_2024_09 PARTITION OF klines
    FOR VALUES FROM (1725148800000) TO (1727740800000);
CREATE TABLE klines_2024_10 PARTITION OF klines
    FOR VALUES FROM (1727740800000) TO (1730419200000);
CREATE TABLE klines_2024_11 PARTITION OF klines
    FOR VALUES FROM (1730419200000) TO (1733011200000);
CREATE TABLE klines_2024_12 PARTITION OF klines
    FOR VALUES FROM (1733011200000) TO (1735689600000);

-- 創建索引（分區表上的索引）
CREATE UNIQUE INDEX idx_klines_unique ON klines(symbol_id, interval_type, open_time);
CREATE INDEX idx_klines_symbol_time ON klines(symbol_id, open_time);
CREATE INDEX idx_klines_time_range ON klines(open_time, close_time);
CREATE INDEX idx_klines_trading_type ON klines(trading_type);

-- 期貨索引價格K線表（簡化版，不使用分區）
CREATE TABLE index_price_klines (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER REFERENCES symbols(id),
    interval_type VARCHAR(10) NOT NULL,
    open_time BIGINT NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_index_klines_unique ON index_price_klines(symbol_id, interval_type, open_time);

-- 期貨標記價格K線表
CREATE TABLE mark_price_klines (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER REFERENCES symbols(id),
    interval_type VARCHAR(10) NOT NULL,
    open_time BIGINT NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_mark_klines_unique ON mark_price_klines(symbol_id, interval_type, open_time);

-- 期貨資金費率K線表
CREATE TABLE premium_index_klines (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER REFERENCES symbols(id),
    interval_type VARCHAR(10) NOT NULL,
    open_time BIGINT NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_premium_klines_unique ON premium_index_klines(symbol_id, interval_type, open_time);

-- ==============================================
-- 3. 交易資料表 (Trading Data Tables)
-- ==============================================

-- 交易資料表（簡化版，不使用分區）
CREATE TABLE trades (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER REFERENCES symbols(id),
    trade_id BIGINT NOT NULL,
    price DECIMAL(20,8) NOT NULL,
    quantity DECIMAL(20,8) NOT NULL,
    quote_quantity DECIMAL(20,8) NOT NULL,
    timestamp BIGINT NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL,
    trading_type VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 創建索引
CREATE UNIQUE INDEX idx_trades_unique ON trades(symbol_id, trade_id, timestamp);
CREATE INDEX idx_trades_timestamp ON trades(timestamp);
CREATE INDEX idx_trades_symbol_time ON trades(symbol_id, timestamp);

-- 聚合交易資料表
CREATE TABLE agg_trades (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER REFERENCES symbols(id),
    agg_trade_id BIGINT NOT NULL,
    price DECIMAL(20,8) NOT NULL,
    quantity DECIMAL(20,8) NOT NULL,
    first_trade_id BIGINT NOT NULL,
    last_trade_id BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL,
    trading_type VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_agg_trades_unique ON agg_trades(symbol_id, agg_trade_id, timestamp);
CREATE INDEX idx_agg_trades_timestamp ON agg_trades(timestamp);

-- ==============================================
-- 4. 期貨專用資料表 (Futures-specific Tables)
-- ==============================================

-- 訂單簿深度表
CREATE TABLE book_depth (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER REFERENCES symbols(id),
    timestamp BIGINT NOT NULL,
    percentage DECIMAL(10,4) NOT NULL,
    depth DECIMAL(20,8) NOT NULL,
    notional DECIMAL(20,8) NOT NULL,
    trading_type VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_book_depth_symbol_time ON book_depth(symbol_id, timestamp);

-- 最佳買賣價表
CREATE TABLE book_ticker (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER REFERENCES symbols(id),
    update_id BIGINT NOT NULL,
    best_bid_price DECIMAL(20,8) NOT NULL,
    best_bid_qty DECIMAL(20,8) NOT NULL,
    best_ask_price DECIMAL(20,8) NOT NULL,
    best_ask_qty DECIMAL(20,8) NOT NULL,
    transaction_time BIGINT NOT NULL,
    event_time BIGINT NOT NULL,
    trading_type VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_book_ticker_symbol_time ON book_ticker(symbol_id, transaction_time);

-- 交易指標表
CREATE TABLE trading_metrics (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER REFERENCES symbols(id),
    create_time BIGINT NOT NULL,
    sum_open_interest DECIMAL(20,8),
    sum_open_interest_value DECIMAL(20,8),
    count_toptrader_long_short_ratio DECIMAL(10,6),
    sum_toptrader_long_short_ratio DECIMAL(10,6),
    count_long_short_ratio DECIMAL(10,6),
    sum_taker_long_short_vol_ratio DECIMAL(10,6),
    trading_type VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_metrics_unique ON trading_metrics(symbol_id, create_time);

-- 資金費率表
CREATE TABLE funding_rates (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER REFERENCES symbols(id),
    calc_time BIGINT NOT NULL,
    funding_interval_hours INTEGER NOT NULL,
    last_funding_rate DECIMAL(10,8) NOT NULL,
    trading_type VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_funding_rates_unique ON funding_rates(symbol_id, calc_time);

-- ==============================================
-- 5. 輔助函數和觸發器
-- ==============================================

-- 更新 updated_at 的觸發器函數
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 為 symbols 表創建觸發器
CREATE TRIGGER update_symbols_updated_at 
    BEFORE UPDATE ON symbols 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ==============================================
-- 6. 視圖 (Views) - 便於查詢
-- ==============================================

-- K線數據彙總視圖
CREATE VIEW v_klines_summary AS
SELECT 
    s.symbol,
    s.trading_type,
    k.interval_type,
    COUNT(*) as record_count,
    MIN(to_timestamp(k.open_time/1000)) as earliest_time,
    MAX(to_timestamp(k.close_time/1000)) as latest_time,
    AVG(k.volume) as avg_volume
FROM klines k
JOIN symbols s ON k.symbol_id = s.id
GROUP BY s.symbol, s.trading_type, k.interval_type;

-- 同步狀態概覽視圖
CREATE VIEW v_sync_overview AS
SELECT 
    s.symbol,
    s.trading_type,
    ss.data_type,
    ss.time_period,
    ss.interval_type,
    ss.last_sync_date,
    ss.records_count,
    ss.status
FROM sync_status ss
JOIN symbols s ON ss.symbol_id = s.id
ORDER BY s.symbol, ss.data_type, ss.time_period;

-- ==============================================
-- 7. 自動分區創建函數 (用於未來擴展)
-- ==============================================

-- 創建自動分區函數
CREATE OR REPLACE FUNCTION create_klines_partition(start_timestamp BIGINT, end_timestamp BIGINT, partition_suffix VARCHAR)
RETURNS VOID AS $$
DECLARE
    partition_name VARCHAR;
    sql_text TEXT;
BEGIN
    partition_name := 'klines_' || partition_suffix;
    
    sql_text := format('CREATE TABLE %I PARTITION OF klines FOR VALUES FROM (%L) TO (%L)',
                      partition_name, start_timestamp, end_timestamp);
    
    EXECUTE sql_text;
    
    RAISE NOTICE 'Created partition: %', partition_name;
END;
$$ LANGUAGE plpgsql;

-- ==============================================
-- 8. 權限設置（使用 DO 塊處理角色存在的情況）
-- ==============================================

-- 安全創建角色
DO $$
BEGIN
    -- 創建 binance_reader 角色
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'binance_reader') THEN
        CREATE ROLE binance_reader;
    END IF;
    
    -- 創建 binance_writer 角色
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'binance_writer') THEN
        CREATE ROLE binance_writer;
    END IF;
END
$$;

-- 授予讀取權限
GRANT USAGE ON SCHEMA binance_data TO binance_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA binance_data TO binance_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA binance_data TO binance_reader;

-- 授予寫入權限
GRANT USAGE ON SCHEMA binance_data TO binance_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA binance_data TO binance_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA binance_data TO binance_writer;

-- 為將來創建的表自動授權
ALTER DEFAULT PRIVILEGES IN SCHEMA binance_data GRANT SELECT ON TABLES TO binance_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA binance_data GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO binance_writer;

-- ==============================================
-- 9. 性能優化設置
-- ==============================================

-- 設置適當的統計目標
ALTER TABLE klines ALTER COLUMN open_time SET STATISTICS 1000;
ALTER TABLE klines ALTER COLUMN symbol_id SET STATISTICS 1000;

-- ==============================================
-- 10. 註釋說明
-- ==============================================

COMMENT ON SCHEMA binance_data IS 'Binance 公開數據存儲架構';
COMMENT ON TABLE symbols IS '交易對基本資訊表';
COMMENT ON TABLE sync_status IS '資料同步狀態追蹤表';
COMMENT ON TABLE klines IS '主要K線數據表，支援所有交易類型（按時間分區）';
COMMENT ON TABLE trades IS '原始交易數據表';
COMMENT ON TABLE agg_trades IS '聚合交易數據表';

-- 添加列註釋
COMMENT ON COLUMN klines.open_time IS '開盤時間 (毫秒時間戳)';
COMMENT ON COLUMN klines.close_time IS '收盤時間 (毫秒時間戳)';
COMMENT ON COLUMN klines.volume IS '交易量 (基礎資產)';
COMMENT ON COLUMN klines.quote_asset_volume IS '交易量 (報價資產)';

-- 顯示創建完成信息
SELECT 'Binance 數據庫架構創建完成！包含 2024 年全年分區' as status;
