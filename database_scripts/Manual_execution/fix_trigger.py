#!/usr/bin/env python3
"""
執行分區觸發器修復的腳本
確保分區觸發器函數被正確修復
"""

import os
import sys
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()


def get_database_connection():
    """獲取資料庫連接"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "binance_data"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", ""),
            options=f"-c search_path={os.getenv('DB_SCHEMA', 'binance_data')}",
        )
        return conn
    except Exception as e:
        print(f"❌ 資料庫連接失敗: {e}")
        return None


def execute_sql_script(conn, sql_content):
    """執行 SQL 腳本"""
    try:
        cursor = conn.cursor()
        cursor.execute(sql_content)
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        print(f"❌ SQL 執行失敗: {e}")
        conn.rollback()
        return False


def check_trigger_function():
    """檢查觸發器函數是否正確"""
    conn = get_database_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # 檢查函數是否存在
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.routines 
                WHERE routine_schema = 'binance_data' 
                AND routine_name = 'partition_insert_trigger'
                AND routine_type = 'FUNCTION'
            );
        """
        )

        function_exists = cursor.fetchone()[0]

        if not function_exists:
            print("❌ partition_insert_trigger 函數不存在")
            return False

        # 檢查觸發器數量
        cursor.execute(
            """
            SELECT COUNT(*) 
            FROM information_schema.triggers 
            WHERE trigger_schema = 'binance_data' 
            AND trigger_name LIKE '%partition%';
        """
        )

        trigger_count = cursor.fetchone()[0]
        print(f"✅ 找到 {trigger_count} 個分區觸發器")

        # 檢查 klines 表的觸發器
        cursor.execute(
            """
            SELECT trigger_name 
            FROM information_schema.triggers 
            WHERE trigger_schema = 'binance_data' 
            AND event_object_table = 'klines'
            AND trigger_name LIKE '%partition%';
        """
        )

        klines_triggers = cursor.fetchall()
        if klines_triggers:
            print(f"✅ klines 表有分區觸發器: {[t[0] for t in klines_triggers]}")
        else:
            print("❌ klines 表缺少分區觸發器")
            return False

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ 檢查觸發器函數失敗: {e}")
        return False


def test_trigger_function():
    """測試觸發器函數是否能正常工作"""
    conn = get_database_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # 首先確保有測試符號
        cursor.execute(
            """
            INSERT INTO symbols (symbol, base_asset, quote_asset, trading_type) 
            VALUES ('TESTUSDT', 'TEST', 'USDT', 'um')
            ON CONFLICT (symbol) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
            RETURNING id;
        """
        )

        symbol_id = cursor.fetchone()[0]

        # 嘗試插入測試數據到 klines 表
        import time

        current_timestamp = int(time.time() * 1000)

        cursor.execute(
            """
            INSERT INTO klines (
                symbol_id, trading_type, interval_type, open_time, open_price, 
                high_price, low_price, close_price, volume, close_time, 
                quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, 
                taker_buy_quote_asset_volume, data_source
            ) VALUES (
                %s, 'um', '1m', %s, 50000.0, 50100.0, 49900.0, 50050.0, 
                10.0, %s, 500000.0, 100, 5.0, 250000.0, 'test'
            );
        """,
            (symbol_id, current_timestamp, current_timestamp + 60000),
        )

        conn.commit()
        print("✅ 測試數據插入成功，觸發器函數工作正常")

        # 清理測試數據
        cursor.execute("DELETE FROM klines WHERE data_source = 'test';")
        cursor.execute("DELETE FROM symbols WHERE symbol = 'TESTUSDT';")
        conn.commit()

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ 測試觸發器函數失敗: {e}")
        conn.rollback()
        return False


def main():
    print("🔧 開始修復分區觸發器函數...")

    # 修復 SQL 腳本內容
    fix_sql = """
-- 完整的分區觸發器修復腳本
-- 確保完全修復 partition_insert_trigger 函數

-- 設置正確的 schema
SET search_path TO binance_data, public;

-- 首先完全刪除所有相關的觸發器和函數
DO $$
DECLARE
    r RECORD;
BEGIN
    -- 刪除所有分區觸發器
    FOR r IN (
        SELECT trigger_name, event_object_table
        FROM information_schema.triggers 
        WHERE trigger_schema = 'binance_data' 
        AND trigger_name LIKE '%partition%'
    ) LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', r.trigger_name, r.event_object_table);
    END LOOP;
END
$$;

-- 刪除舊的觸發器函數
DROP FUNCTION IF EXISTS partition_insert_trigger() CASCADE;
DROP FUNCTION IF EXISTS create_partition_if_not_exists(TEXT, BIGINT, TEXT) CASCADE;

-- 重新創建分區創建函數
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
    -- 驗證輸入參數
    IF partition_key IS NULL OR partition_key <= 0 THEN
        RETURN FALSE;
    END IF;
    
    -- 將毫秒時間戳轉換為日期
    BEGIN
        partition_date := to_date(to_timestamp(partition_key / 1000)::text, 'YYYY-MM-DD');
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END;
    
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
    
    BEGIN
        EXECUTE sql_text;
        RETURN TRUE;
    EXCEPTION
        WHEN duplicate_table THEN
            RETURN TRUE;
        WHEN OTHERS THEN
            RETURN FALSE;
    END;
END;
$$ LANGUAGE plpgsql;

-- 重新創建修復後的分區插入觸發器函數
CREATE OR REPLACE FUNCTION partition_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
    time_column TEXT;
    partition_key BIGINT;
    table_name TEXT;
BEGIN
    -- 獲取表名
    table_name := TG_TABLE_NAME;
    
    -- 根據表名確定正確的時間欄位
    time_column := CASE table_name
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
    
    -- 根據時間欄位獲取分區鍵值
    BEGIN
        CASE time_column
            WHEN 'open_time' THEN 
                partition_key := NEW.open_time;
            WHEN 'transaction_time' THEN 
                partition_key := NEW.transaction_time;
            WHEN 'create_time' THEN 
                partition_key := NEW.create_time;
            WHEN 'calc_time' THEN 
                partition_key := NEW.calc_time;
            WHEN 'timestamp' THEN 
                partition_key := NEW.timestamp;
            ELSE
                RETURN NEW;
        END CASE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NEW;
    END;
    
    -- 驗證分區鍵
    IF partition_key IS NULL OR partition_key <= 0 THEN
        RETURN NEW;
    END IF;
    
    -- 嘗試創建分區（如果需要）
    BEGIN
        PERFORM create_partition_if_not_exists(table_name, partition_key, time_column);
    EXCEPTION
        WHEN OTHERS THEN
            -- 不中斷插入操作，即使分區創建失敗
            NULL;
    END;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 為 klines 表創建觸發器
CREATE TRIGGER klines_partition_trigger
    BEFORE INSERT ON klines
    FOR EACH ROW
    EXECUTE FUNCTION partition_insert_trigger();
"""

    # 連接資料庫並執行修復
    conn = get_database_connection()
    if not conn:
        print("❌ 無法連接到資料庫")
        return False

    print("📝 執行修復腳本...")
    if not execute_sql_script(conn, fix_sql):
        print("❌ 修復腳本執行失敗")
        conn.close()
        return False

    conn.close()
    print("✅ 修復腳本執行成功")

    # 檢查修復結果
    print("🔍 檢查修復結果...")
    if not check_trigger_function():
        print("❌ 觸發器函數檢查失敗")
        return False

    # 測試觸發器功能
    print("🧪 測試觸發器功能...")
    if not test_trigger_function():
        print("❌ 觸發器功能測試失敗")
        return False

    print("🎉 分區觸發器修復完成！現在可以正常導入數據了。")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
