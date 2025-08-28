"""
修復 database_config.py 中的分區管理邏輯
主要修復 UniversalPartitionManager 類中的問題
"""

import os
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
import logging
from datetime import datetime, date, timedelta
from decimal import Decimal
import json
from dotenv import load_dotenv
import calendar
import pandas as pd

# 載入環境變數
load_dotenv()

# 設置日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConfig:
    """資料庫配置類"""

    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.database = os.getenv("DB_NAME", "binance_data")
        self.username = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASSWORD", "")
        self.schema = os.getenv("DB_SCHEMA", "binance_data")

        # 連接池設置
        self.min_connections = int(os.getenv("DB_MIN_CONNECTIONS", "1"))
        self.max_connections = int(os.getenv("DB_MAX_CONNECTIONS", "10"))

    @property
    def connection_string(self):
        """生成連接字符串"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_connection_params(self):
        """獲取連接參數字典"""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.username,
            "password": self.password,
            "options": f"-c search_path={self.schema}",
        }


class UniversalPartitionManager:
    """通用分區管理器 - 修復版本"""

    # 定義所有支援分區的表及其時間欄位
    PARTITIONED_TABLES = {
        "klines": "open_time",
        "index_price_klines": "open_time",
        "mark_price_klines": "open_time",
        "premium_index_klines": "open_time",
        "trades": "timestamp",
        "agg_trades": "timestamp",
        "book_depth": "timestamp",
        "book_ticker": "transaction_time",
        "trading_metrics": "create_time",
        "funding_rates": "calc_time",
        "bvol_index": "calc_time",
    }

    def __init__(self, db_manager):
        self.db = db_manager
        # 嘗試從數據庫載入數據源配置來更新分區表映射
        try:
            self._load_data_source_config()
        except Exception as e:
            logger.warning(f"無法載入數據源配置，使用預設配置: {e}")

    def _load_data_source_config(self):
        """從數據庫載入數據源配置，動態更新分區表映射"""
        try:
            with self.db.get_connection() as conn:
                with self.db.get_cursor(conn) as cursor:
                    cursor.execute(
                        """
                        SELECT market_data_type, time_column 
                        FROM data_sources 
                        WHERE is_active = true
                    """
                    )

                    data_sources = cursor.fetchall()

                    # 更新分區表映射
                    for market_data_type, time_column in data_sources:
                        # 映射數據類型到表名
                        table_name = self._get_table_name(market_data_type)
                        if table_name and time_column:
                            self.PARTITIONED_TABLES[table_name] = time_column
                            logger.debug(
                                f"Updated partition mapping: {table_name} -> {time_column}"
                            )

        except Exception as e:
            logger.warning(f"無法載入數據源配置: {e}")
            # 使用預設配置

    def _get_table_name(self, market_data_type):
        """將市場數據類型映射到表名"""
        mapping = {
            "klines": "klines",
            "indexPriceKlines": "index_price_klines",
            "markPriceKlines": "mark_price_klines",
            "premiumIndexKlines": "premium_index_klines",
            "trades": "trades",
            "aggTrades": "agg_trades",
            "bookDepth": "book_depth",
            "bookTicker": "book_ticker",
            "metrics": "trading_metrics",
            "fundingRate": "funding_rates",
            "BVOLIndex": "bvol_index",
        }
        return mapping.get(market_data_type)

    def get_month_bounds_ms(self, year, month):
        """獲取指定月份的開始和結束時間戳（毫秒）"""
        # 月份開始時間
        start_date = datetime(year, month, 1)

        # 月份結束時間（下個月的第一天）
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)

        return int(start_date.timestamp() * 1000), int(end_date.timestamp() * 1000)

    def partition_exists(self, partition_name):
        """檢查分區是否存在"""
        try:
            with self.db.get_connection() as conn:
                with self.db.get_cursor(conn) as cursor:
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = %s 
                            AND table_name = %s
                        )
                    """,
                        (self.db.config.schema, partition_name),
                    )
                    return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"檢查分區失敗: {e}")
            return False

    def create_monthly_partition(self, table_name, year, month):
        """為指定表創建指定年月的分區"""
        try:
            if table_name not in self.PARTITIONED_TABLES:
                logger.error(f"表 {table_name} 不支援分區")
                return False

            partition_name = f"{table_name}_{year}_{month:02d}"

            # 檢查分區是否已存在
            if self.partition_exists(partition_name):
                logger.debug(f"分區 {partition_name} 已存在")
                return True

            # 獲取時間範圍
            start_ts, end_ts = self.get_month_bounds_ms(year, month)

            # 創建分區
            with self.db.get_connection() as conn:
                with self.db.get_cursor(conn, dict_cursor=False) as cursor:
                    sql = f"""
                    CREATE TABLE {self.db.config.schema}.{partition_name} PARTITION OF {self.db.config.schema}.{table_name}
                    FOR VALUES FROM ({start_ts}) TO ({end_ts})
                    """

                    cursor.execute(sql)
                    conn.commit()

                    logger.info(
                        f"成功創建分區: {partition_name} 時間範圍: {datetime.fromtimestamp(start_ts/1000)} 到 {datetime.fromtimestamp(end_ts/1000)}"
                    )
                    return True

        except Exception as e:
            logger.error(f"創建分區失敗 {partition_name}: {e}")
            return False

    def ensure_partition_for_timestamp(self, table_name, timestamp_ms):
        """確保給定時間戳在指定表的分區存在"""
        try:
            if table_name not in self.PARTITIONED_TABLES:
                logger.warning(f"表 {table_name} 不支援分區，跳過分區創建")
                return True

            # 檢查 timestamp_ms 是否為有效值
            if timestamp_ms is None or timestamp_ms <= 0:
                logger.warning(f"無效的時間戳: {timestamp_ms}")
                return False

            # 將毫秒時間戳轉換為 datetime
            dt = datetime.fromtimestamp(timestamp_ms / 1000)
            year = dt.year
            month = dt.month

            # 創建必要的分區
            return self.create_monthly_partition(table_name, year, month)

        except Exception as e:
            logger.error(
                f"確保分區失敗 表: {table_name}, 時間戳: {timestamp_ms}, 錯誤: {e}"
            )
            return False

    def auto_create_partitions_for_data(self, table_name, df, timestamp_column=None):
        """為 DataFrame 中的數據自動創建所需的分區"""
        try:
            if table_name not in self.PARTITIONED_TABLES:
                logger.info(f"表 {table_name} 不支援分區，跳過分區創建")
                return True

            if df.empty:
                logger.debug("DataFrame 為空，無需創建分區")
                return True

            # 確定時間欄位
            if timestamp_column is None:
                timestamp_column = self.PARTITIONED_TABLES[table_name]

            if timestamp_column not in df.columns:
                logger.warning(f"DataFrame 中找不到時間欄位 {timestamp_column}")
                logger.debug(f"可用的欄位: {list(df.columns)}")
                return True

            # 獲取所有唯一的年月組合
            timestamps = df[timestamp_column].dropna()  # 移除 NaN 值

            if timestamps.empty:
                logger.warning(f"所有 {timestamp_column} 值都是 NaN")
                return True

            # 確保時間戳是數字類型
            timestamps = pd.to_numeric(timestamps, errors="coerce")
            timestamps = timestamps.dropna()  # 再次移除無法轉換的值

            if timestamps.empty:
                logger.warning(f"所有 {timestamp_column} 值都無法轉換為數字")
                return True

            unique_months = set()

            for ts in timestamps:
                try:
                    # 檢查時間戳是否合理（在合理的範圍內）
                    if ts <= 0 or ts > 9999999999999:  # 超過合理範圍
                        logger.warning(f"時間戳超出合理範圍: {ts}")
                        continue

                    dt = datetime.fromtimestamp(ts / 1000)
                    unique_months.add((dt.year, dt.month))
                except (ValueError, OSError) as e:
                    logger.warning(f"無法轉換時間戳 {ts}: {e}")
                    continue

            if not unique_months:
                logger.warning("沒有找到有效的時間戳")
                return True

            # 為每個唯一的年月創建分區
            success_count = 0
            for year, month in unique_months:
                if self.create_monthly_partition(table_name, year, month):
                    success_count += 1

            logger.info(
                f"為表 {table_name} 創建了 {success_count}/{len(unique_months)} 個必要的分區"
            )
            return success_count == len(unique_months)

        except Exception as e:
            logger.error(f"自動創建分區失敗: {e}")
            import traceback

            logger.error(f"詳細錯誤: {traceback.format_exc()}")
            return False

    def create_year_partitions(self, year):
        """為所有分區表創建指定年份的分區"""
        try:
            total_created = 0
            for table_name in self.PARTITIONED_TABLES.keys():
                logger.info(f"為表 {table_name} 創建 {year} 年分區")

                for month in range(1, 13):
                    if self.create_monthly_partition(table_name, year, month):
                        total_created += 1

            logger.info(f"為 {year} 年總共創建了 {total_created} 個分區")
            return total_created

        except Exception as e:
            logger.error(f"創建年度分區失敗: {e}")
            return 0

    def cleanup_old_partitions(self, months_to_keep=24):
        """清理舊的分區（保留指定月份數）"""
        try:
            cutoff_date = date.today() - timedelta(days=months_to_keep * 30)
            deleted_count = 0

            with self.db.get_connection() as conn:
                with self.db.get_cursor(conn) as cursor:
                    # 查找所有分區
                    cursor.execute(
                        """
                        SELECT tablename 
                        FROM pg_tables 
                        WHERE schemaname = %s
                        AND tablename ~ '_\\d{4}_\\d{2}$'
                    """,
                        (self.db.config.schema,),
                    )

                    partitions = cursor.fetchall()

                    for partition in partitions:
                        partition_name = partition[0]
                        try:
                            # 從分區名稱提取日期
                            year_month = partition_name.split("_")[
                                -2:
                            ]  # ['2023', '01']
                            if len(year_month) == 2:
                                year, month = int(year_month[0]), int(year_month[1])
                                partition_date = date(year, month, 1)

                                if partition_date < cutoff_date:
                                    cursor.execute(
                                        f"DROP TABLE IF EXISTS {self.db.config.schema}.{partition_name}"
                                    )
                                    deleted_count += 1
                                    logger.info(f"刪除舊分區: {partition_name}")
                        except Exception as e:
                            logger.warning(f"處理分區 {partition_name} 時出錯: {e}")

                    conn.commit()

            logger.info(f"清理完成，刪除了 {deleted_count} 個舊分區")
            return deleted_count

        except Exception as e:
            logger.error(f"清理舊分區失敗: {e}")
            return 0

    def get_partition_info(self):
        """獲取所有分區的信息"""
        try:
            with self.db.get_connection() as conn:
                with self.db.get_cursor(conn) as cursor:
                    cursor.execute(
                        """
                        SELECT 
                            schemaname,
                            tablename,
                            CASE 
                                WHEN tablename ~ '_\\d{4}_\\d{2}
                             THEN 'partition'
                                ELSE 'main_table'
                            END as table_type,
                            n_tup_ins as rows_inserted,
                            n_live_tup as live_rows
                        FROM pg_stat_user_tables 
                        WHERE schemaname = %s
                        ORDER BY tablename
                    """,
                        (self.db.config.schema,),
                    )

                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"獲取分區信息失敗: {e}")
            return []


class DataSourceManager:
    """數據源管理類 - 管理所有支援的數據源類型"""

    def __init__(self, db_manager):
        self.db = db_manager

    def get_data_source_id(self, trading_type, market_data_type):
        """獲取數據源ID"""
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id FROM data_sources 
                    WHERE trading_type = %s AND market_data_type = %s
                """,
                    (trading_type, market_data_type),
                )
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"獲取數據源ID失敗: {e}")
            return None

    def get_all_data_sources(self, trading_type=None):
        """獲取所有數據源配置"""
        query = "SELECT * FROM data_sources WHERE is_active = true"
        params = []

        if trading_type:
            query += " AND trading_type = %s"
            params.append(trading_type)

        query += " ORDER BY trading_type, market_data_type"

        with self.db.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def get_supported_intervals(self, trading_type, market_data_type):
        """獲取支援的時間間隔"""
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute(
                    """
                    SELECT supports_intervals, default_intervals 
                    FROM data_sources 
                    WHERE trading_type = %s AND market_data_type = %s
                """,
                    (trading_type, market_data_type),
                )
                result = cursor.fetchone()

                if result and result[0]:  # supports_intervals = True
                    return result[1] if result[1] else []
                return []
        except Exception as e:
            logger.error(f"獲取支援間隔失敗: {e}")
            return []

    def add_data_source(
        self,
        trading_type,
        market_data_type,
        description,
        supports_intervals=False,
        default_intervals=None,
        time_column="timestamp",
        base_url_path=None,
    ):
        """添加新的數據源"""
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO data_sources 
                    (trading_type, market_data_type, description, supports_intervals, 
                     default_intervals, time_column, base_url_path)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trading_type, market_data_type) DO UPDATE SET
                        description = EXCLUDED.description,
                        supports_intervals = EXCLUDED.supports_intervals,
                        default_intervals = EXCLUDED.default_intervals,
                        time_column = EXCLUDED.time_column,
                        base_url_path = EXCLUDED.base_url_path,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id;
                """,
                    (
                        trading_type,
                        market_data_type,
                        description,
                        supports_intervals,
                        default_intervals,
                        time_column,
                        base_url_path,
                    ),
                )

                source_id = cursor.fetchone()[0]
                logger.info(
                    f"添加/更新數據源: {trading_type}/{market_data_type} (ID: {source_id})"
                )
                return source_id
        except Exception as e:
            logger.error(f"添加數據源失敗: {e}")
            return None


class DatabaseManager:
    """增強的資料庫管理類 - 修復版本"""

    def __init__(self):
        self.config = DatabaseConfig()
        self.connection_pool = None
        self.partition_manager = None
        self.data_source_manager = None
        self._initialize_pool()
        # 延遲初始化分區管理器，避免循環依賴
        self.partition_manager = UniversalPartitionManager(self)
        self.data_source_manager = DataSourceManager(self)

    def _initialize_pool(self):
        """初始化連接池"""
        try:
            self.connection_pool = SimpleConnectionPool(
                self.config.min_connections,
                self.config.max_connections,
                **self.config.get_connection_params(),
            )
            logger.info("資料庫連接池初始化成功")
        except Exception as e:
            logger.error(f"連接池初始化失敗: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """獲取資料庫連接的上下文管理器"""
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"資料庫操作錯誤: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    @contextmanager
    def get_cursor(self, connection=None, dict_cursor=True):
        """獲取資料庫游標的上下文管理器"""
        if connection:
            cursor_factory = psycopg2.extras.DictCursor if dict_cursor else None
            cursor = connection.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()
        else:
            with self.get_connection() as conn:
                cursor_factory = psycopg2.extras.DictCursor if dict_cursor else None
                cursor = conn.cursor(cursor_factory=cursor_factory)
                try:
                    yield cursor
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    cursor.close()

    def execute_script_file(self, script_path):
        """執行 SQL 腳本文件"""
        try:
            with open(script_path, "r", encoding="utf-8") as file:
                script_content = file.read()

            with self.get_connection() as conn:
                with self.get_cursor(conn, dict_cursor=False) as cursor:
                    cursor.execute(script_content)
                    conn.commit()
                    logger.info(f"成功執行腳本: {script_path}")
        except Exception as e:
            logger.error(f"執行腳本失敗 {script_path}: {e}")
            raise

    def test_connection(self):
        """測試資料庫連接"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                logger.info(f"資料庫連接成功，版本: {version}")
                return True
        except Exception as e:
            logger.error(f"資料庫連接測試失敗: {e}")
            return False

    def close_pool(self):
        """關閉連接池"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("資料庫連接池已關閉")

    def timestamp_to_ms(self, dt):
        """將 datetime 轉換為毫秒時間戳"""
        return int(dt.timestamp() * 1000)

    # 為了向後兼容，保留原有的分區方法
    def auto_create_partitions_for_data(self, df, timestamp_column):
        """為 DataFrame 中的 klines 數據自動創建所需的分區（向後兼容）"""
        return self.partition_manager.auto_create_partitions_for_data(
            "klines", df, timestamp_column
        )

    def ensure_partition_for_timestamp(self, timestamp_ms):
        """確保給定時間戳的 klines 分區存在（向後兼容）"""
        return self.partition_manager.ensure_partition_for_timestamp(
            "klines", timestamp_ms
        )

    def create_monthly_partition(self, year, month):
        """創建指定年月的 klines 分區（向後兼容）"""
        return self.partition_manager.create_monthly_partition("klines", year, month)


class SymbolManager:
    """交易對管理類"""

    def __init__(self, db_manager):
        self.db = db_manager

    def add_symbol(
        self, symbol, base_asset, quote_asset, trading_type, status="TRADING"
    ):
        """添加新的交易對"""
        query = """
        INSERT INTO symbols (symbol, base_asset, quote_asset, trading_type, status)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE SET
            base_asset = EXCLUDED.base_asset,
            quote_asset = EXCLUDED.quote_asset,
            trading_type = EXCLUDED.trading_type,
            status = EXCLUDED.status,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id;
        """

        with self.db.get_cursor() as cursor:
            cursor.execute(
                query, (symbol, base_asset, quote_asset, trading_type, status)
            )
            symbol_id = cursor.fetchone()[0]
            logger.info(f"添加/更新交易對: {symbol} (ID: {symbol_id})")
            return symbol_id

    def get_symbol_id(self, symbol):
        """根據交易對名稱獲取ID"""
        query = "SELECT id FROM symbols WHERE symbol = %s;"

        with self.db.get_cursor() as cursor:
            cursor.execute(query, (symbol,))
            result = cursor.fetchone()
            return result[0] if result else None

    def get_all_symbols(self, trading_type=None):
        """獲取所有交易對"""
        query = "SELECT * FROM symbols"
        params = []

        if trading_type:
            query += " WHERE trading_type = %s"
            params.append(trading_type)

        query += " ORDER BY symbol;"

        with self.db.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def batch_add_symbols(self, symbols_data):
        """批量添加交易對"""
        query = """
        INSERT INTO symbols (symbol, base_asset, quote_asset, trading_type, status)
        VALUES %s
        ON CONFLICT (symbol) DO UPDATE SET
            base_asset = EXCLUDED.base_asset,
            quote_asset = EXCLUDED.quote_asset,
            trading_type = EXCLUDED.trading_type,
            status = EXCLUDED.status,
            updated_at = CURRENT_TIMESTAMP;
        """

        with self.db.get_connection() as conn:
            with self.db.get_cursor(conn) as cursor:
                psycopg2.extras.execute_values(
                    cursor, query, symbols_data, template=None, page_size=100
                )
                conn.commit()
                logger.info(f"批量添加了 {len(symbols_data)} 個交易對")


class SyncStatusManager:
    """同步狀態管理類"""

    def __init__(self, db_manager):
        self.db = db_manager

    def update_sync_status_with_source(
        self,
        symbol_id,
        data_source_id,
        time_period,
        last_sync_date,
        records_count=0,
        interval_type=None,
        file_format=".csv",
    ):
        """更新同步狀態（使用數據源ID）"""
        query = """
        INSERT INTO sync_status 
        (symbol_id, data_source_id, time_period, interval_type, last_sync_date, records_count, file_format)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol_id, data_source_id, time_period, interval_type) DO UPDATE SET
            last_sync_date = EXCLUDED.last_sync_date,
            last_sync_timestamp = CURRENT_TIMESTAMP,
            records_count = EXCLUDED.records_count,
            file_format = EXCLUDED.file_format;
        """

        with self.db.get_cursor() as cursor:
            cursor.execute(
                query,
                (
                    symbol_id,
                    data_source_id,
                    time_period,
                    interval_type,
                    last_sync_date,
                    records_count,
                    file_format,
                ),
            )

    def update_sync_status(
        self,
        symbol_id,
        data_type,
        time_period,
        last_sync_date,
        records_count=0,
        interval_type=None,
        file_format=".csv",
    ):
        """更新同步狀態（向後相容方法）"""
        # 嘗試獲取數據源ID
        logger.warning(
            "使用舊版 update_sync_status 方法，建議使用 update_sync_status_with_source"
        )

        # 簡化處理：嘗試根據 data_type 推斷
        trading_type_map = {
            "klines": "um",  # 預設為 um (因為您的測試是期貨數據)
            "trades": "um",
            "aggTrades": "um",
        }

        trading_type = trading_type_map.get(data_type, "um")
        data_source_id = self.db.data_source_manager.get_data_source_id(
            trading_type, data_type
        )

        if data_source_id:
            self.update_sync_status_with_source(
                symbol_id,
                data_source_id,
                time_period,
                last_sync_date,
                records_count,
                interval_type,
                file_format,
            )
        else:
            logger.error(f"無法找到數據源: {trading_type}/{data_type}")

    def get_last_sync_date(self, symbol_id, data_type, time_period, interval_type=None):
        """獲取最後同步日期"""
        # 注意：這個方法可能需要更新以使用新的數據源結構
        query = """
        SELECT ss.last_sync_date 
        FROM sync_status ss
        JOIN data_sources ds ON ss.data_source_id = ds.id
        WHERE ss.symbol_id = %s 
        AND ds.market_data_type = %s 
        AND ss.time_period = %s
        """
        params = [symbol_id, data_type, time_period]

        if interval_type:
            query += " AND ss.interval_type = %s"
            params.append(interval_type)
        else:
            query += " AND ss.interval_type IS NULL"

        with self.db.get_cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result else None

    def get_sync_overview(self):
        """獲取同步狀態概覽"""
        query = "SELECT * FROM v_sync_overview;"

        with self.db.get_cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()


if __name__ == "__main__":
    # 測試修復後的配置
    print("測試修復後的資料庫配置...")

    try:
        db_manager = DatabaseManager()

        if db_manager.test_connection():
            print("✅ 資料庫連接測試成功")

            # 測試分區管理器
            partition_manager = db_manager.partition_manager
            print("✅ 分區管理器初始化成功")

            # 測試創建分區
            success = partition_manager.create_monthly_partition("klines", 2024, 1)
            if success:
                print("✅ 測試分區創建成功")
            else:
                print("⚠️ 測試分區創建失敗，但這可能是因為分區已存在")

        else:
            print("❌ 資料庫連接測試失敗")

    except Exception as e:
        print(f"❌ 測試失敗: {e}")
        import traceback

        print(traceback.format_exc())
    finally:
        if "db_manager" in locals():
            db_manager.close_pool()
            print("✅ 資料庫連接池已關閉")
