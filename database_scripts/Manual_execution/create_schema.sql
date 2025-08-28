-- 支援所有 Binance 公開資料類型，包含新的數據源管理
-- 所有時間序列表都使用分區，插入時自動創建分區

-- 創建專用的 schema
CREATE SCHEMA IF NOT EXISTS binance_data;

-- 設置默認 schema
SET search_path TO binance_data, public;

-- ==============================================
-- 1. 元數據表 (Metadata Tables)
-- ==============================================

-- 數據源管理表（新增）
CREATE TABLE data_sources (
    id SERIAL PRIMARY KEY,
    trading_type VARCHAR(20) NOT NULL,
    market_data_type VARCHAR(50) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    supports_intervals BOOLEAN DEFAULT false,
    default_intervals TEXT[], -- JSON array of supported intervals
    time_column VARCHAR(30) NOT NULL, -- 用於分區的時間欄位名稱
    base_url_path VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trading_type, market_data_type)
);

-- 插入預設數據源配置
INSERT INTO data_sources (trading_type, market_data_type, description, supports_intervals, default_intervals, time_column, base_url_path) VALUES
-- Spot 現貨市場
('spot', 'klines', 'Spot市場K線數據', true, '{\"1m\",\"3m\",\"5m\",\"15m\",\"30m\",\"1h\",\"2h\",\"4h\",\"6h\",\"8h\",\"12h\",\"1d\",\"3d\",\"1w\",\"1mo\"}', 'open_time', 'data/spot'),
('spot', 'trades', 'Spot市場交易數據', false, NULL, 'timestamp', 'data/spot'),
('spot', 'aggTrades', 'Spot市場聚合交易數據', false, NULL, 'timestamp', 'data/spot'),

-- UM 合約市場
('um', 'klines', 'USDT合約K線數據', true, '{\"1m\",\"3m\",\"5m\",\"15m\",\"30m\",\"1h\",\"2h\",\"4h\",\"6h\",\"8h\",\"12h\",\"1d\",\"3d\",\"1w\",\"1mo\"}', 'open_time', 'data/futures/um'),
('um', 'trades', 'USDT合約交易數據', false, NULL, 'timestamp', 'data/futures/um'),
('um', 'aggTrades', 'USDT合約聚合交易數據', false, NULL, 'timestamp', 'data/futures/um'),
('um', 'indexPriceKlines', 'USDT合約指數價格K線', true, '{\"1m\",\"3m\",\"5m\",\"15m\",\"30m\",\"1h\",\"2h\",\"4h\",\"6h\",\"8h\",\"12h\",\"1d\",\"3d\",\"1w\",\"1mo\"}', 'open_time', 'data/futures/um'),
('um', 'markPriceKlines', 'USDT合約標記價格K線', true, '{\"1m\",\"3m\",\"5m\",\"15m\",\"30m\",\"1h\",\"2h\",\"4h\",\"6h\",\"8h\",\"12h\",\"1d\",\"3d\",\"1w\",\"1mo\"}', 'open_time', 'data/futures/um'),
('um', 'premiumIndexKlines', 'USDT合約資金費率K線', true, '{\"1m\",\"3m\",\"5m\",\"15m\",\"30m\",\"1h\",\"2h\",\"4h\",\"6h\",\"8h\",\"12h\",\"1d\",\"3d\",\"1w\",\"1mo\"}', 'open_time', 'data/futures/um'),
('um', 'bookDepth', 'USDT合約訂單簿深度', false, NULL, 'timestamp', 'data/futures/um'),
('um', 'bookTicker', 'USDT合約最佳買賣價', false, NULL, 'transaction_time', 'data/futures/um'),
('um', 'metrics', 'USDT合約交易指標', false, NULL, 'create_time', 'data/futures/um'),
('um', 'fundingRate', 'USDT合約資金費率', false, NULL, 'calc_time', 'data/futures/um'),

-- CM 合約市場  
('cm', 'klines', '幣本位合約K線數據', true, '{\"1m\",\"3m\",\"5m\",\"15m\",\"30m\",\"1h\",\"2h\",\"4h\",\"6h\",\"8h\",\"12h\",\"1d\",\"3d\",\"1w\",\"1mo\"}', 'open_time', 'data/futures/cm'),
('cm', 'trades', '幣本位合約交易數據', false, NULL, 'timestamp', 'data/futures/cm'),
('cm', 'aggTrades', '幣本位合約聚合交易數據', false, NULL, 'timestamp', 'data/futures/cm'),
('cm', 'indexPriceKlines', '幣本位合約指數價格K線', true, '{\"1m\",\"3m\",\"5m\",\"15m\",\"30m\",\"1h\",\"2h\",\"4h\",\"6h\",\"8h\",\"12h\",\"1d\",\"3d\",\"1w\",\"1mo\"}', 'open_time', 'data/futures/cm'),
('cm', 'markPriceKlines', '幣本位合約標記價格K線', true, '{\"1m\",\"3m\",\"5m\",\"15m\",\"30m\",\"1h\",\"2h\",\"4h\",\"6h\",\"8h\",\"12h\",\"1d\",\"3d\",\"1w\",\"1mo\"}', 'open_time', 'data/futures/cm'),
('cm', 'premiumIndexKlines', '幣本位合約資金費率K線', true, '{\"1m\",\"3m\",\"5m\",\"15m\",\"30m\",\"1h\",\"2h\",\"4h\",\"6h\",\"8h\",\"12h\",\"1d\",\"3d\",\"1w\",\"1mo\"}', 'open_time', 'data/futures/cm'),
('cm', 'bookDepth', '幣本位合約訂單簿深度', false, NULL, 'timestamp', 'data/futures/cm'),
('cm', 'bookTicker', '幣本位合約最佳買賣價', false, NULL, 'transaction_time', 'data/futures/cm'),
('cm', 'metrics', '幣本位合約交易指標', false, NULL, 'create_time', 'data/futures/cm'),
('cm', 'fundingRate', '幣本位合約資金費率', false, NULL, 'calc_time', 'data/futures/cm'),

-- Option 期權市場（新增）
('option', 'BVOLIndex', '波動率指數數據', false, NULL, 'calc_time', 'data/option');

-- 交易對資訊表
CREATE TABLE symbols (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL UNIQUE,
    base_asset VARCHAR(20) NOT NULL,
    quote_asset VARCHAR(20) NOT NULL,
    trading_type VARCHAR(20) NOT NULL CHECK (trading_type IN ('spot', 'um', 'cm', 'option')),
    status VARCHAR(20) DEFAULT 'TRADING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 創建索引
CREATE INDEX idx_symbols_trading_type ON symbols(trading_type);
CREATE INDEX idx_symbols_status ON symbols(status);
CREATE INDEX idx_symbols_base_quote ON symbols(base_asset, quote_asset);

-- 資料同步狀態表
CREATE TABLE sync_status (
    id SERIAL PRIMARY KEY,
    symbol_id INTEGER REFERENCES symbols(id),
    data_source_id INTEGER REFERENCES data_sources(id),
    time_period VARCHAR(10) NOT NULL CHECK (time_period IN ('daily', 'monthly')),
    interval_type VARCHAR(10), -- K線資料的時間間隔
    last_sync_date DATE NOT NULL,
    last_sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_count BIGINT DEFAULT 0,
    file_format VARCHAR(10) DEFAULT '.csv',
    status VARCHAR(20) DEFAULT 'active',
    UNIQUE(symbol_id, data_source_id, time_period, interval_type)
);

-- 創建索引
CREATE INDEX idx_sync_status_symbol_data ON sync_status(symbol_id, data_source_id);
CREATE INDEX idx_sync_status_last_sync ON sync_status(last_sync_date);

-- ==============================================
-- 2. 分區管理函數（插入時自動創建分區）
-- ==============================================

-- 創建自動分區函數
CREATE OR REPLACE FUNCTION create_partition_if_not_exists(
    table_name TEXT,
    partition_key BIGINT,
    time_column TEXT DEFAULT 'open_time'
)
RETURNS BOOLEAN AS $$
DECLARE
    partition_date DATE;
    partition_year INTEGER;
    partition_month INTEGER;
    partition_name TEXT;
    start_timestamp BIGINT;
    end_timestamp BIGINT;
    sql_text TEXT;
BEGIN
    -- 將毫秒時間戳轉換為日期
    partition_date := to_date(to_timestamp(partition_key / 1000)::text, 'YYYY-MM-DD');
    partition_year := EXTRACT(YEAR FROM partition_date);
    partition_month := EXTRACT(MONTH FROM partition_date);
    
    -- 生成分區名稱
    partition_name := table_name || '_' || partition_year || '_' || LPAD(partition_month::text, 2, '0');
    
    -- 檢查分區是否已存在
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'binance_data' 
        AND table_name = partition_name
    ) THEN
        RETURN TRUE;
    END IF;
    
    -- 計算分區邊界（月份邊界）
    start_timestamp := EXTRACT(EPOCH FROM date_trunc('month', partition_date)) * 1000;
    
    IF partition_month = 12 THEN
        end_timestamp := EXTRACT(EPOCH FROM date_trunc('month', 
            make_date(partition_year + 1, 1, 1))) * 1000;
    ELSE
        end_timestamp := EXTRACT(EPOCH FROM date_trunc('month', 
            make_date(partition_year, partition_month + 1, 1))) * 1000;
    END IF;
    
    -- 創建分區
    sql_text := format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                      partition_name, table_name, start_timestamp, end_timestamp);
    
    EXECUTE sql_text;
    
    RAISE NOTICE '自動創建分區: %', partition_name;
    RETURN TRUE;
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING '創建分區失敗 %: %', partition_name, SQLERRM;
        RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

-- 分區插入觸發器函數
CREATE OR REPLACE FUNCTION partition_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
    time_column TEXT;
    partition_key BIGINT;
BEGIN
    -- 根據表名確定正確的時間欄位
    time_column := CASE TG_TABLE_NAME
        WHEN 'klines' THEN 'open_time'
        WHEN 'index_price_klines' THEN 'open_time'
        WHEN 'mark_price_klines' THEN 'open_time'
        WHEN 'premium_index_klines' THEN 'open_time'
        WHEN 'book_ticker' THEN 'transaction_time'
        WHEN 'trading_metrics' THEN 'create_time'
        WHEN 'funding_rates' THEN 'calc_time'
        WHEN 'bvol_index' THEN 'calc_time'
        ELSE 'timestamp'
    END;
    
    -- 獲取正確的時間戳值
    CASE time_column
        WHEN 'open_time' THEN partition_key := NEW.open_time;
        WHEN 'transaction_time' THEN partition_key := NEW.transaction_time;
        WHEN 'create_time' THEN partition_key := NEW.create_time;
        WHEN 'calc_time' THEN partition_key := NEW.calc_time;
        WHEN 'timestamp' THEN partition_key := NEW.timestamp;
    END CASE;
    
    -- 創建分區
    PERFORM create_partition_if_not_exists(TG_TABLE_NAME, partition_key, time_column);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ==============================================
-- 3. K線資料表 (Klines Data Tables) - 分區表
-- ==============================================

-- 基本 K線資料表 (支援所有交易類型) - 分區表
CREATE TABLE klines (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    trading_type VARCHAR(20) NOT NULL,
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

-- 創建插入觸發器
CREATE TRIGGER klines_partition_trigger
    BEFORE INSERT ON klines
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();

-- 期貨索引價格K線表 - 分區表
CREATE TABLE index_price_klines (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    interval_type VARCHAR(10) NOT NULL,
    open_time BIGINT NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, open_time)  -- 包含分區鍵的複合主鍵
) PARTITION BY RANGE (open_time);

CREATE TRIGGER index_price_klines_partition_trigger
    BEFORE INSERT ON index_price_klines
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();

-- 期貨標記價格K線表 - 分區表
CREATE TABLE mark_price_klines (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    interval_type VARCHAR(10) NOT NULL,
    open_time BIGINT NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, open_time)  -- 包含分區鍵的複合主鍵
) PARTITION BY RANGE (open_time);

CREATE TRIGGER mark_price_klines_partition_trigger
    BEFORE INSERT ON mark_price_klines
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();

-- 期貨資金費率K線表 - 分區表
CREATE TABLE premium_index_klines (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    interval_type VARCHAR(10) NOT NULL,
    open_time BIGINT NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, open_time)  -- 包含分區鍵的複合主鍵
) PARTITION BY RANGE (open_time);

CREATE TRIGGER premium_index_klines_partition_trigger
    BEFORE INSERT ON premium_index_klines
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();

-- ==============================================
-- 4. 交易資料表 (Trading Data Tables) - 分區表
-- ==============================================

-- 交易資料表 - 分區表 (第一個時間欄位是 timestamp)
CREATE TABLE trades (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    trade_id BIGINT NOT NULL,
    price DECIMAL(20,8) NOT NULL,
    quantity DECIMAL(20,8) NOT NULL,
    quote_quantity DECIMAL(20,8) NOT NULL,
    timestamp BIGINT NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL,
    trading_type VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, timestamp)  -- 包含分區鍵的複合主鍵
) PARTITION BY RANGE (timestamp);

CREATE TRIGGER trades_partition_trigger
    BEFORE INSERT ON trades
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();

-- 聚合交易資料表 - 分區表 (第一個時間欄位是 timestamp)
CREATE TABLE agg_trades (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    agg_trade_id BIGINT NOT NULL,
    price DECIMAL(20,8) NOT NULL,
    quantity DECIMAL(20,8) NOT NULL,
    first_trade_id BIGINT NOT NULL,
    last_trade_id BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL,
    trading_type VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, timestamp)  -- 包含分區鍵的複合主鍵
) PARTITION BY RANGE (timestamp);

CREATE TRIGGER agg_trades_partition_trigger
    BEFORE INSERT ON agg_trades
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();

-- ==============================================
-- 5. 期貨專用資料表 (Futures-specific Tables) - 分區表
-- ==============================================

-- 訂單簿深度表 - 分區表 (第一個時間欄位是 timestamp)
CREATE TABLE book_depth (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    timestamp BIGINT NOT NULL,
    percentage DECIMAL(10,4) NOT NULL,
    depth DECIMAL(20,8) NOT NULL,
    notional DECIMAL(20,8) NOT NULL,
    trading_type VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, timestamp)  -- 包含分區鍵的複合主鍵
) PARTITION BY RANGE (timestamp);

CREATE TRIGGER book_depth_partition_trigger
    BEFORE INSERT ON book_depth
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();

-- 最佳買賣價表 - 分區表 (第一個時間欄位是 transaction_time)
CREATE TABLE book_ticker (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    update_id BIGINT NOT NULL,
    best_bid_price DECIMAL(20,8) NOT NULL,
    best_bid_qty DECIMAL(20,8) NOT NULL,
    best_ask_price DECIMAL(20,8) NOT NULL,
    best_ask_qty DECIMAL(20,8) NOT NULL,
    transaction_time BIGINT NOT NULL,
    event_time BIGINT NOT NULL,
    trading_type VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, transaction_time)  -- 包含分區鍵的複合主鍵
) PARTITION BY RANGE (transaction_time);

CREATE TRIGGER book_ticker_partition_trigger
    BEFORE INSERT ON book_ticker
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();

-- 交易指標表 - 分區表 (第一個時間欄位是 create_time)
CREATE TABLE trading_metrics (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    create_time BIGINT NOT NULL,
    sum_open_interest DECIMAL(20,8),
    sum_open_interest_value DECIMAL(20,8),
    count_toptrader_long_short_ratio DECIMAL(10,6),
    sum_toptrader_long_short_ratio DECIMAL(10,6),
    count_long_short_ratio DECIMAL(10,6),
    sum_taker_long_short_vol_ratio DECIMAL(10,6),
    trading_type VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, create_time)  -- 包含分區鍵的複合主鍵
) PARTITION BY RANGE (create_time);

CREATE TRIGGER trading_metrics_partition_trigger
    BEFORE INSERT ON trading_metrics
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();

-- 資金費率表 - 分區表 (第一個時間欄位是 calc_time)
CREATE TABLE funding_rates (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    calc_time BIGINT NOT NULL,
    funding_interval_hours INTEGER NOT NULL,
    last_funding_rate DECIMAL(10,8) NOT NULL,
    trading_type VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, calc_time)  -- 包含分區鍵的複合主鍵
) PARTITION BY RANGE (calc_time);

CREATE TRIGGER funding_rates_partition_trigger
    BEFORE INSERT ON funding_rates
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();

-- ==============================================
-- 6. 期權市場資料表 (Options-specific Tables) - 分區表
-- ==============================================

-- BVOL指數表 - 分區表 (第一個時間欄位是 calc_time)
CREATE TABLE bvol_index (
    id BIGSERIAL,
    symbol_id INTEGER REFERENCES symbols(id),
    calc_time BIGINT NOT NULL,
    symbol VARCHAR(50) NOT NULL, -- 冗餘欄位，方便查詢
    base_asset VARCHAR(20) NOT NULL,
    quote_asset VARCHAR(20) NOT NULL,
    index_value DECIMAL(20,8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, calc_time)  -- 包含分區鍵的複合主鍵
) PARTITION BY RANGE (calc_time);

CREATE TRIGGER bvol_index_partition_trigger
    BEFORE INSERT ON bvol_index
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();

-- ==============================================
-- 7. 索引創建
-- ==============================================

-- klines 表索引
CREATE INDEX idx_klines_symbol_time ON klines(symbol_id, open_time);
CREATE INDEX idx_klines_time_range ON klines(open_time, close_time);
CREATE INDEX idx_klines_trading_type ON klines(trading_type);

-- index_price_klines 表索引
CREATE INDEX idx_index_klines_symbol_time ON index_price_klines(symbol_id, open_time);

-- mark_price_klines 表索引
CREATE INDEX idx_mark_klines_symbol_time ON mark_price_klines(symbol_id, open_time);

-- premium_index_klines 表索引
CREATE INDEX idx_premium_klines_symbol_time ON premium_index_klines(symbol_id, open_time);

-- trades 表索引
CREATE INDEX idx_trades_timestamp ON trades(timestamp);
CREATE INDEX idx_trades_symbol_time ON trades(symbol_id, timestamp);

-- agg_trades 表索引
CREATE INDEX idx_agg_trades_timestamp ON agg_trades(timestamp);
CREATE INDEX idx_agg_trades_symbol_time ON agg_trades(symbol_id, timestamp);

-- book_depth 表索引
CREATE INDEX idx_book_depth_symbol_time ON book_depth(symbol_id, timestamp);
CREATE INDEX idx_book_depth_timestamp ON book_depth(timestamp);

-- book_ticker 表索引
CREATE INDEX idx_book_ticker_symbol_time ON book_ticker(symbol_id, transaction_time);
CREATE INDEX idx_book_ticker_transaction_time ON book_ticker(transaction_time);

-- trading_metrics 表索引
CREATE INDEX idx_metrics_symbol_time ON trading_metrics(symbol_id, create_time);
CREATE INDEX idx_metrics_create_time ON trading_metrics(create_time);

-- funding_rates 表索引
CREATE INDEX idx_funding_rates_symbol_time ON funding_rates(symbol_id, calc_time);
CREATE INDEX idx_funding_rates_calc_time ON funding_rates(calc_time);

-- bvol_index 表索引
CREATE INDEX idx_bvol_index_symbol_time ON bvol_index(symbol_id, calc_time);
CREATE INDEX idx_bvol_index_calc_time ON bvol_index(calc_time);
CREATE INDEX idx_bvol_index_symbol ON bvol_index(symbol);

-- ==============================================
-- 8. 輔助函數和觸發器
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

-- 為 data_sources 表創建觸發器
CREATE TRIGGER update_data_sources_updated_at 
    BEFORE UPDATE ON data_sources 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ==============================================
-- 9. 視圖 (Views) - 便於查詢
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
    ds.market_data_type,
    ss.time_period,
    ss.interval_type,
    ss.last_sync_date,
    ss.records_count,
    ss.status
FROM sync_status ss
JOIN symbols s ON ss.symbol_id = s.id
JOIN data_sources ds ON ss.data_source_id = ds.id
ORDER BY s.symbol, ds.market_data_type, ss.time_period;

-- 數據源配置視圖
CREATE VIEW v_data_sources AS
SELECT 
    ds.id,
    ds.trading_type,
    ds.market_data_type,
    ds.description,
    ds.is_active,
    ds.supports_intervals,
    ds.default_intervals,
    ds.time_column,
    COUNT(ss.id) as synced_symbols
FROM data_sources ds
LEFT JOIN sync_status ss ON ds.id = ss.data_source_id
GROUP BY ds.id, ds.trading_type, ds.market_data_type, ds.description, 
         ds.is_active, ds.supports_intervals, ds.default_intervals, ds.time_column
ORDER BY ds.trading_type, ds.market_data_type;

-- 分區統計視圖
-- CREATE VIEW v_partition_summary AS
-- SELECT 
--     schemaname,
--     tablename as partition_name,
--     CASE 
--         WHEN tablename ~ '_\\d{4}_\\d{2}$' THEN 'partition'
--         ELSE 'main_table'
--     END as table_type,
--     CASE 
--         WHEN tablename ~ '_\\d{4}_\\d{2}$' THEN 
--             LEFT(tablename, LENGTH(tablename) - 8)
--         ELSE tablename
--     END as base_table,
--     CASE 
--         WHEN tablename ~ '_\\d{4}_\\d{2}$' THEN 
--             SUBSTRING(tablename FROM '_(\\d{4})_\\d{2}$')
--         ELSE NULL
--     END as partition_year,
--     CASE 
--         WHEN tablename ~ '_\\d{4}_\\d{2}$' THEN 
--             SUBSTRING(tablename FROM '_\\d{4}_(\\d{2})$')
--         ELSE NULL
--     END as partition_month,
--     n_tup_ins as rows_inserted,
--     n_tup_upd as rows_updated,
--     n_tup_del as rows_deleted,
--     n_live_tup as live_rows,
--     n_dead_tup as dead_rows
-- FROM pg_stat_user_tables 
-- WHERE schemaname = 'binance_data'
-- ORDER BY tablename;

-- ==============================================
-- 10. 管理函數
-- ==============================================

-- 獲取或創建數據源ID
CREATE OR REPLACE FUNCTION get_data_source_id(
    p_trading_type VARCHAR(20),
    p_market_data_type VARCHAR(50)
)
RETURNS INTEGER AS $$
DECLARE
    source_id INTEGER;
BEGIN
    SELECT id INTO source_id
    FROM data_sources
    WHERE trading_type = p_trading_type 
    AND market_data_type = p_market_data_type;
    
    IF source_id IS NULL THEN
        RAISE EXCEPTION 'Data source not found: % %', p_trading_type, p_market_data_type;
    END IF;
    
    RETURN source_id;
END;
$$ LANGUAGE plpgsql;

-- 批量創建年度分區函數
CREATE OR REPLACE FUNCTION create_year_partitions(target_year INTEGER)
RETURNS INTEGER AS $$
DECLARE
    table_names TEXT[] := ARRAY[
        'klines', 'index_price_klines', 'mark_price_klines', 'premium_index_klines',
        'trades', 'agg_trades', 'book_depth', 'book_ticker', 
        'trading_metrics', 'funding_rates', 'bvol_index'
    ];
    table_name TEXT;
    month_num INTEGER;
    start_ts BIGINT;
    end_ts BIGINT;
    partition_suffix VARCHAR(10);
    partition_name VARCHAR(100);
    created_count INTEGER := 0;
    sql_text TEXT;
BEGIN
    -- 為每個表創建 12 個月的分區
    FOREACH table_name IN ARRAY table_names
    LOOP
        FOR month_num IN 1..12 LOOP
            -- 計算月份時間戳
            start_ts := EXTRACT(EPOCH FROM date_trunc('month', 
                make_date(target_year, month_num, 1))) * 1000;
            
            IF month_num = 12 THEN
                end_ts := EXTRACT(EPOCH FROM date_trunc('month', 
                    make_date(target_year + 1, 1, 1))) * 1000;
            ELSE
                end_ts := EXTRACT(EPOCH FROM date_trunc('month', 
                    make_date(target_year, month_num + 1, 1))) * 1000;
            END IF;
            
            partition_suffix := target_year || '_' || LPAD(month_num::TEXT, 2, '0');
            partition_name := table_name || '_' || partition_suffix;
            
            -- 檢查分區是否已存在
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'binance_data' 
                AND table_name = partition_name
            ) THEN
                sql_text := format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                                  partition_name, table_name, start_ts, end_ts);
                
                EXECUTE sql_text;
                created_count := created_count + 1;
                RAISE NOTICE 'Created partition: %', partition_name;
            END IF;
        END LOOP;
    END LOOP;
    
    RAISE NOTICE 'Created % partitions for year %', created_count, target_year;
    RETURN created_count;
END;
$$ LANGUAGE plpgsql;

-- 清理舊分區函數
CREATE OR REPLACE FUNCTION cleanup_old_partitions(months_to_keep INTEGER DEFAULT 24)
RETURNS INTEGER AS $$
DECLARE
    partition_record RECORD;
    cutoff_date DATE;
    partition_date DATE;
    sql_text TEXT;
    deleted_count INTEGER := 0;
BEGIN
    cutoff_date := CURRENT_DATE - (months_to_keep || ' months')::INTERVAL;
    
    -- 查找需要刪除的分區
    FOR partition_record IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'binance_data'
        AND tablename ~ '_\d{4}_\d{2}$'
    LOOP
        -- 從分區名稱提取日期
        BEGIN
            partition_date := TO_DATE(
                SUBSTRING(partition_record.tablename FROM '_(\d{4}_\d{2})$'), 
                'YYYY_MM'
            );
            
            IF partition_date < cutoff_date THEN
                sql_text := format('DROP TABLE IF EXISTS %I', partition_record.tablename);
                EXECUTE sql_text;
                deleted_count := deleted_count + 1;
                RAISE NOTICE 'Dropped old partition: %', partition_record.tablename;
            END IF;
        EXCEPTION 
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to process partition: %', partition_record.tablename;
        END;
    END LOOP;
    
    RAISE NOTICE 'Dropped % old partitions', deleted_count;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ==============================================
-- 11. 權限設置
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
-- 12. 性能優化設置
-- ==============================================

-- 設置適當的統計目標
ALTER TABLE klines ALTER COLUMN open_time SET STATISTICS 1000;
ALTER TABLE klines ALTER COLUMN symbol_id SET STATISTICS 1000;
ALTER TABLE trades ALTER COLUMN timestamp SET STATISTICS 1000;
ALTER TABLE trades ALTER COLUMN symbol_id SET STATISTICS 1000;

-- 設置分區表的約束排除
SET constraint_exclusion = partition;

-- ==============================================
-- 13. 註釋說明
-- ==============================================

COMMENT ON SCHEMA binance_data IS 'Binance 公開數據存儲架構 - 所有時間序列表都使用分區，插入時自動創建分區';

COMMENT ON TABLE data_sources IS '數據源配置表，管理所有支援的交易類型和市場數據類型';
COMMENT ON TABLE symbols IS '交易對基本資訊表';
COMMENT ON TABLE sync_status IS '資料同步狀態追蹤表';

COMMENT ON TABLE klines IS '主要K線數據表，支援所有交易類型（按 open_time 分區）';
COMMENT ON TABLE trades IS '原始交易數據表（按 timestamp 分區）';
COMMENT ON TABLE agg_trades IS '聚合交易數據表（按 timestamp 分區）';
COMMENT ON TABLE book_depth IS '訂單簿深度表（按 timestamp 分區）';
COMMENT ON TABLE book_ticker IS '最佳買賣價表（按 transaction_time 分區）';
COMMENT ON TABLE trading_metrics IS '交易指標表（按 create_time 分區）';
COMMENT ON TABLE funding_rates IS '資金費率表（按 calc_time 分區）';
COMMENT ON TABLE bvol_index IS 'BVOL波動率指數表（按 calc_time 分區）';

-- 添加列註釋
COMMENT ON COLUMN data_sources.time_column IS '用於分區的時間欄位名稱';
COMMENT ON COLUMN data_sources.supports_intervals IS '是否支援時間間隔（K線類數據）';
COMMENT ON COLUMN data_sources.default_intervals IS '預設支援的時間間隔列表';

COMMENT ON COLUMN klines.open_time IS '開盤時間 (毫秒時間戳) - 分區鍵';
COMMENT ON COLUMN trades.timestamp IS '交易時間 (毫秒時間戳) - 分區鍵';
COMMENT ON COLUMN agg_trades.timestamp IS '聚合交易時間 (毫秒時間戳) - 分區鍵';
COMMENT ON COLUMN book_ticker.transaction_time IS '交易時間 (毫秒時間戳) - 分區鍵';
COMMENT ON COLUMN trading_metrics.create_time IS '創建時間 (毫秒時間戳) - 分區鍵';
COMMENT ON COLUMN funding_rates.calc_time IS '計算時間 (毫秒時間戳) - 分區鍵';
COMMENT ON COLUMN bvol_index.calc_time IS '計算時間 (毫秒時間戳) - 分區鍵';

-- ==============================================
-- 14. 初始化示例數據
-- ==============================================

-- 插入一些常用的交易對示例
-- INSERT INTO symbols (symbol, base_asset, quote_asset, trading_type) VALUES
-- ('BTCUSDT', 'BTC', 'USDT', 'spot'),
-- ('ETHUSDT', 'ETH', 'USDT', 'spot'),
-- ('BTCUSDT', 'BTC', 'USDT', 'um'),
-- ('ETHUSDT', 'ETH', 'USDT', 'um'),
-- ('BTCUSD_PERP', 'BTC', 'USD', 'cm'),
-- ('ETHUSD_PERP', 'ETH', 'USD', 'cm'),
-- ('BTCBVOLUSDT', 'BTCBVOL', 'USDT', 'option'),
-- ('ETHBVOLUSDT', 'ETHBVOL', 'USDT', 'option')
-- ON CONFLICT (symbol) DO NOTHING;

-- 顯示創建完成信息
DO $$
BEGIN
    RAISE NOTICE '===========================================';
    RAISE NOTICE 'Binance 數據庫架構創建完成！';
    RAISE NOTICE '特色功能:';
    RAISE NOTICE '• 所有時間序列表都支援分區';
    RAISE NOTICE '• 插入資料時自動創建分區';
    RAISE NOTICE '• 支援新的期權市場數據源';
    RAISE NOTICE '• 完整的數據源管理系統';
    RAISE NOTICE '===========================================';
END
$$;

-- 顯示數據源統計
-- SELECT 
--     '數據源配置總覽' as info,
--     trading_type,
--     COUNT(*) as data_types
-- FROM data_sources 
-- GROUP BY trading_type
-- ORDER BY trading_type;
