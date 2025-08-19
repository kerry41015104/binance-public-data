"""
Binance 公開資料庫連接和配置管理模組
負責處理 PostgreSQL 資料庫連接、配置管理和基本操作
"""

import os
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
import logging
from datetime import datetime, date
from decimal import Decimal
import json
from dotenv import load_dotenv
import calendar

# 載入環境變數
load_dotenv()

# 設置日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """資料庫配置類"""
    
    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'binance_data')
        self.username = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '')
        self.schema = os.getenv('DB_SCHEMA', 'binance_data')
        
        # 連接池設置
        self.min_connections = int(os.getenv('DB_MIN_CONNECTIONS', '1'))
        self.max_connections = int(os.getenv('DB_MAX_CONNECTIONS', '10'))
        
    @property
    def connection_string(self):
        """生成連接字符串"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_connection_params(self):
        """獲取連接參數字典"""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.username,
            'password': self.password,
            'options': f'-c search_path={self.schema}'
        }

class DatabaseManager:
    """資料庫管理類"""
    
    def __init__(self):
        self.config = DatabaseConfig()
        self.connection_pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """初始化連接池"""
        try:
            self.connection_pool = SimpleConnectionPool(
                self.config.min_connections,
                self.config.max_connections,
                **self.config.get_connection_params()
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
            with open(script_path, 'r', encoding='utf-8') as file:
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
    
    def get_month_bounds(self, year, month):
        """獲取指定月份的開始和結束時間戳（毫秒）"""
        # 月份開始時間
        start_date = datetime(year, month, 1)
        
        # 月份結束時間（下個月的第一天）
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)
        
        return self.timestamp_to_ms(start_date), self.timestamp_to_ms(end_date)
    
    def partition_exists(self, partition_name):
        """檢查分區是否存在"""
        try:
            with self.get_connection() as conn:
                with self.get_cursor(conn) as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = %s 
                            AND table_name = %s
                        )
                    """, (self.config.schema, partition_name))
                    return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"檢查分區失敗: {e}")
            return False
    
    def create_monthly_partition(self, year, month):
        """創庺指定年月的分區"""
        try:
            partition_name = f"klines_{year}_{month:02d}"
            
            # 檢查分區是否已存在
            if self.partition_exists(partition_name):
                logger.debug(f"分區 {partition_name} 已存在")
                return True
            
            # 獲取時間範圍
            start_ts, end_ts = self.get_month_bounds(year, month)
            
            # 創建分區
            with self.get_connection() as conn:
                with self.get_cursor(conn, dict_cursor=False) as cursor:
                    sql = f"""
                    CREATE TABLE {self.config.schema}.{partition_name} PARTITION OF {self.config.schema}.klines
                    FOR VALUES FROM ({start_ts}) TO ({end_ts})
                    """
                    
                    cursor.execute(sql)
                    conn.commit()
                    
                    logger.info(f"成功創庺分區: {partition_name} 時間範圍: {datetime.fromtimestamp(start_ts/1000)} 到 {datetime.fromtimestamp(end_ts/1000)}")
                    return True
                    
        except Exception as e:
            logger.error(f"創庺分區失敗 {partition_name}: {e}")
            return False
    
    def ensure_partition_for_timestamp(self, timestamp_ms):
        """確保給定時間戳的分區存在"""
        try:
            # 將毫秒時間戳轉換為 datetime
            dt = datetime.fromtimestamp(timestamp_ms / 1000)
            year = dt.year
            month = dt.month
            
            # 創建必要的分區
            return self.create_monthly_partition(year, month)
            
        except Exception as e:
            logger.error(f"確保分區失敗 時間戳: {timestamp_ms}, 錯誤: {e}")
            return False
    
    def auto_create_partitions_for_data(self, df, timestamp_column='open_time'):
        """為 DataFrame 中的數據自動創庺所需的分區"""
        try:
            if df.empty or timestamp_column not in df.columns:
                return True
            
            # 獲取所有唯一的年月組合
            timestamps = df[timestamp_column].astype(int)
            unique_months = set()
            
            for ts in timestamps:
                dt = datetime.fromtimestamp(ts / 1000)
                unique_months.add((dt.year, dt.month))
            
            # 為每個唯一的年月創庺分區
            success_count = 0
            for year, month in unique_months:
                if self.create_monthly_partition(year, month):
                    success_count += 1
            
            logger.info(f"為數據創建了 {success_count}/{len(unique_months)} 個必要的分區")
            return success_count == len(unique_months)
            
        except Exception as e:
            logger.error(f"自動創庺分區失敗: {e}")
            return False

class SymbolManager:
    """交易對管理類"""
    
    def __init__(self, db_manager):
        self.db = db_manager
    
    def add_symbol(self, symbol, base_asset, quote_asset, trading_type, status='TRADING'):
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
            cursor.execute(query, (symbol, base_asset, quote_asset, trading_type, status))
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
    
    def update_sync_status(self, symbol_id, data_type, time_period, last_sync_date, 
                          records_count=0, interval_type=None, file_format='.csv'):
        """更新同步狀態"""
        query = """
        INSERT INTO sync_status 
        (symbol_id, data_type, time_period, interval_type, last_sync_date, records_count, file_format)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol_id, data_type, time_period, interval_type) DO UPDATE SET
            last_sync_date = EXCLUDED.last_sync_date,
            last_sync_timestamp = CURRENT_TIMESTAMP,
            records_count = EXCLUDED.records_count,
            file_format = EXCLUDED.file_format;
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (symbol_id, data_type, time_period, interval_type, 
                                 last_sync_date, records_count, file_format))
    
    def get_last_sync_date(self, symbol_id, data_type, time_period, interval_type=None):
        """獲取最後同步日期"""
        query = """
        SELECT last_sync_date FROM sync_status 
        WHERE symbol_id = %s AND data_type = %s AND time_period = %s
        """
        params = [symbol_id, data_type, time_period]
        
        if interval_type:
            query += " AND interval_type = %s"
            params.append(interval_type)
        else:
            query += " AND interval_type IS NULL"
        
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

def create_database_config_file():
    """創建 .env 配置文件範本"""
    config_template = """# PostgreSQL 資料庫配置
DB_HOST=localhost
DB_PORT=5432
DB_NAME=binance_data
DB_USER=postgres
DB_PASSWORD=your_password_here
DB_SCHEMA=binance_data

# 連接池設置
DB_MIN_CONNECTIONS=1
DB_MAX_CONNECTIONS=10

# 數據存儲目錄 (與下載腳本共用)
STORE_DIRECTORY=D:\\code\\Trading-Universe\\crypto-data-overall\\binance-public-data
"""
    
    env_path = os.path.join(os.path.dirname(__file__), '.env.example')
    with open(env_path, 'w', encoding='utf-8') as f:
        f.write(config_template)
    
    print(f"已創建配置文件範本: {env_path}")
    print("請將其重命名為 .env 並填入正確的資料庫連接資訊")

# 使用示例
if __name__ == "__main__":
    # 測試資料庫連接
    try:
        db_manager = DatabaseManager()
        
        # 測試連接
        if db_manager.test_connection():
            print("✅ 資料庫連接測試成功")
            
            # 創建 symbol 管理器
            symbol_manager = SymbolManager(db_manager)
            
            # 添加測試交易對
            symbol_id = symbol_manager.add_symbol(
                'BTCUSDT', 'BTC', 'USDT', 'spot'
            )
            print(f"✅ 測試交易對添加成功，ID: {symbol_id}")
            
            # 創建同步狀態管理器
            sync_manager = SyncStatusManager(db_manager)
            
            # 更新同步狀態
            sync_manager.update_sync_status(
                symbol_id, 'klines', 'daily', date.today(), 100, '1h'
            )
            print("✅ 同步狀態更新成功")
            
        else:
            print("❌ 資料庫連接測試失敗")
            
    except Exception as e:
        print(f"❌ 錯誤: {e}")
            
    finally:
        if 'db_manager' in locals():
            db_manager.close_pool()
