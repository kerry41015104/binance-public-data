"""
PostgreSQL 分區管理器
自動創建和管理 klines 表的分區
"""

import psycopg2
from datetime import datetime, timedelta
import calendar
import logging
from database_config import DatabaseManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PartitionManager:
    """分區管理器"""
    
    def __init__(self, db_manager=None):
        self.db = db_manager or DatabaseManager()
    
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
            with self.db.get_connection() as conn:
                with self.db.get_cursor(conn) as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = 'binance_data' 
                            AND table_name = %s
                        )
                    """, (partition_name,))
                    return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"檢查分區失敗: {e}")
            return False
    
    def create_monthly_partition(self, year, month):
        """創建指定年月的分區"""
        try:
            partition_name = f"klines_{year}_{month:02d}"
            
            # 檢查分區是否已存在
            if self.partition_exists(partition_name):
                logger.info(f"分區 {partition_name} 已存在")
                return True
            
            # 獲取時間範圍
            start_ts, end_ts = self.get_month_bounds(year, month)
            
            # 創建分區
            with self.db.get_connection() as conn:
                with self.db.get_cursor(conn) as cursor:
                    sql = f"""
                    CREATE TABLE binance_data.{partition_name} PARTITION OF binance_data.klines
                    FOR VALUES FROM ({start_ts}) TO ({end_ts})
                    """
                    
                    cursor.execute(sql)
                    conn.commit()
                    
                    logger.info(f"成功創建分區: {partition_name}")
                    logger.info(f"時間範圍: {datetime.fromtimestamp(start_ts/1000)} 到 {datetime.fromtimestamp(end_ts/1000)}")
                    return True
                    
        except Exception as e:
            logger.error(f"創建分區失敗 {partition_name}: {e}")
            return False
    
    def create_partitions_for_year(self, year):
        """為指定年份創建所有月份的分區"""
        logger.info(f"開始為 {year} 年創建分區")
        
        success_count = 0
        for month in range(1, 13):
            if self.create_monthly_partition(year, month):
                success_count += 1
        
        logger.info(f"成功創建 {success_count}/12 個分區 ({year} 年)")
        return success_count == 12
    
    def create_future_partitions(self, months_ahead=12):
        """創建未來幾個月的分區"""
        logger.info(f"開始創建未來 {months_ahead} 個月的分區")
        
        current_date = datetime.now()
        success_count = 0
        
        for i in range(months_ahead):
            # 計算目標月份
            target_month = current_date.month + i
            target_year = current_date.year
            
            while target_month > 12:
                target_month -= 12
                target_year += 1
            
            if self.create_monthly_partition(target_year, target_month):
                success_count += 1
        
        logger.info(f"成功創建 {success_count}/{months_ahead} 個未來分區")
        return success_count
    
    def list_existing_partitions(self):
        """列出現有的分區"""
        try:
            with self.db.get_connection() as conn:
                with self.db.get_cursor(conn) as cursor:
                    cursor.execute("""
                        SELECT 
                            schemaname,
                            tablename,
                            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                        FROM pg_tables 
                        WHERE schemaname = 'binance_data' 
                        AND tablename LIKE 'klines_%'
                        AND tablename != 'klines'
                        ORDER BY tablename
                    """)
                    
                    partitions = cursor.fetchall()
                    
                    logger.info(f"現有分區列表 (共 {len(partitions)} 個):")
                    for schema, table, size in partitions:
                        logger.info(f"  {table} - {size}")
                    
                    return partitions
                    
        except Exception as e:
            logger.error(f"列出分區失敗: {e}")
            return []
    
    def get_partition_info(self, partition_name):
        """獲取分區的詳細信息"""
        try:
            with self.db.get_connection() as conn:
                with self.db.get_cursor(conn) as cursor:
                    cursor.execute("""
                        SELECT 
                            pg_get_expr(c.relpartbound, c.oid) as partition_bounds,
                            pg_size_pretty(pg_total_relation_size(c.oid)) as size,
                            n.nspname as schema_name
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = %s AND n.nspname = 'binance_data'
                    """, (partition_name,))
                    
                    result = cursor.fetchone()
                    if result:
                        bounds, size, schema = result
                        logger.info(f"分區 {partition_name} 信息:")
                        logger.info(f"  範圍: {bounds}")
                        logger.info(f"  大小: {size}")
                        return result
                    else:
                        logger.warning(f"分區 {partition_name} 不存在")
                        return None
                        
        except Exception as e:
            logger.error(f"獲取分區信息失敗: {e}")
            return None
    
    def auto_maintain_partitions(self):
        """自動維護分區 - 創建未來的分區"""
        logger.info("開始自動維護分區")
        
        # 列出現有分區
        self.list_existing_partitions()
        
        # 創建未來 12 個月的分區
        future_count = self.create_future_partitions(12)
        
        # 創建 2025 年的分區（如果還沒有）
        current_year = datetime.now().year
        for year in range(current_year, current_year + 2):
            self.create_partitions_for_year(year)
        
        logger.info("自動維護分區完成")
        
        # 再次列出分區查看結果
        self.list_existing_partitions()


def main():
    """主函數 - 可以用命令行調用"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PostgreSQL 分區管理工具')
    parser.add_argument('--action', choices=['list', 'create-year', 'create-future', 'auto-maintain'], 
                       default='auto-maintain', help='執行動作')
    parser.add_argument('--year', type=int, help='指定年份 (create-year 動作)')
    parser.add_argument('--months', type=int, default=12, help='未來月份數量 (create-future 動作)')
    
    args = parser.parse_args()
    
    try:
        manager = PartitionManager()
        
        if args.action == 'list':
            manager.list_existing_partitions()
        
        elif args.action == 'create-year':
            if not args.year:
                print("錯誤: 請指定年份 --year")
                return
            manager.create_partitions_for_year(args.year)
        
        elif args.action == 'create-future':
            manager.create_future_partitions(args.months)
        
        elif args.action == 'auto-maintain':
            manager.auto_maintain_partitions()
            
    except Exception as e:
        logger.error(f"執行失敗: {e}")
    
    finally:
        if 'manager' in locals():
            manager.db.close_pool()


if __name__ == "__main__":
    main()
