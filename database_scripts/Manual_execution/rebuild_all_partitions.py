#!/usr/bin/env python3
"""
完整檢查和重建分區系統
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone
import re

# 添加當前目錄到 Python 路徑
sys.path.append(str(Path(__file__).parent))

from database_config import DatabaseManager
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_all_partitions():
    """分析所有現有分區的邊界"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.get_connection() as conn:
            with db_manager.get_cursor(conn) as cursor:
                
                print("=== 分析所有現有分區 ===")
                
                # 獲取所有 klines 分區
                cursor.execute("""
                    SELECT 
                        c.relname as partition_name,
                        pg_get_expr(c.relpartbound, c.oid) as partition_bounds
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'binance_data' 
                    AND c.relname LIKE 'klines_%'
                    AND c.relname != 'klines'
                    ORDER BY c.relname
                """)
                
                partitions = cursor.fetchall()
                
                print(f"找到 {len(partitions)} 個分區:")
                
                partition_info = []
                overlaps = []
                
                for partition_name, bounds in partitions:
                    print(f"\n{partition_name}: {bounds}")
                    
                    # 解析邊界
                    start_ts, end_ts = parse_partition_bounds(bounds)
                    if start_ts and end_ts:
                        start_dt = datetime.fromtimestamp(start_ts / 1000)
                        end_dt = datetime.fromtimestamp(end_ts / 1000)
                        
                        partition_info.append({
                            'name': partition_name,
                            'start_ts': start_ts,
                            'end_ts': end_ts,
                            'start_dt': start_dt,
                            'end_dt': end_dt
                        })
                        
                        print(f"  開始: {start_ts} ({start_dt})")
                        print(f"  結束: {end_ts} ({end_dt})")
                
                # 檢查重疊
                print(f"\n=== 檢查分區重疊 ===")
                partition_info.sort(key=lambda x: x['start_ts'])
                
                for i in range(len(partition_info) - 1):
                    current = partition_info[i]
                    next_partition = partition_info[i + 1]
                    
                    if current['end_ts'] > next_partition['start_ts']:
                        overlap = {
                            'current': current['name'],
                            'next': next_partition['name'],
                            'current_end': current['end_ts'],
                            'next_start': next_partition['start_ts'],
                            'overlap': current['end_ts'] - next_partition['start_ts']
                        }
                        overlaps.append(overlap)
                        
                        print(f"❌ 重疊: {current['name']} 結束時間 ({current['end_ts']}) > {next_partition['name']} 開始時間 ({next_partition['start_ts']})")
                        print(f"   重疊 {overlap['overlap']} 毫秒")
                    elif current['end_ts'] < next_partition['start_ts']:
                        gap = next_partition['start_ts'] - current['end_ts']
                        print(f"⚠️  間隙: {current['name']} 和 {next_partition['name']} 之間有 {gap} 毫秒間隙")
                    else:
                        print(f"✅ 正常: {current['name']} 和 {next_partition['name']} 邊界正確")
                
                return partition_info, overlaps
                
    except Exception as e:
        logger.error(f"分析分區失敗: {e}")
        return [], []
    
    finally:
        db_manager.close_pool()

def parse_partition_bounds(bounds_str):
    """解析分區邊界字符串"""
    try:
        # 使用正則表達式解析 "FOR VALUES FROM ('1733011200000') TO ('1735689600000')"
        pattern = r"FOR VALUES FROM \('?(\d+)'?\) TO \('?(\d+)'?\)"
        match = re.search(pattern, bounds_str)
        
        if match:
            start_ts = int(match.group(1))
            end_ts = int(match.group(2))
            return start_ts, end_ts
        else:
            print(f"無法解析邊界: {bounds_str}")
            return None, None
            
    except Exception as e:
        print(f"解析邊界失敗: {e}")
        return None, None

def backup_all_partition_data():
    """備份所有分區數據"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.get_connection() as conn:
            with db_manager.get_cursor(conn) as cursor:
                
                print("=== 備份所有分區數據 ===")
                
                # 獲取所有分區表
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables 
                    WHERE table_schema = 'binance_data' 
                    AND table_name LIKE 'klines_%'
                    AND table_name != 'klines'
                    ORDER BY table_name
                """)
                
                partitions = [row[0] for row in cursor.fetchall()]
                
                print(f"準備備份 {len(partitions)} 個分區的數據")
                
                # 創建總備份表
                cursor.execute("DROP TABLE IF EXISTS temp_all_klines_backup")
                cursor.execute("""
                    CREATE TEMP TABLE temp_all_klines_backup AS 
                    SELECT * FROM binance_data.klines WHERE 1=0
                """)
                
                total_records = 0
                
                for partition in partitions:
                    print(f"  備份 {partition}...")
                    
                    # 檢查分區是否有數據
                    cursor.execute(f"SELECT COUNT(*) FROM binance_data.{partition}")
                    count = cursor.fetchone()[0]
                    
                    if count > 0:
                        # 備份數據
                        cursor.execute(f"""
                            INSERT INTO temp_all_klines_backup 
                            SELECT * FROM binance_data.{partition}
                        """)
                        
                        total_records += count
                        print(f"    備份了 {count} 條記錄")
                    else:
                        print(f"    分區為空，跳過")
                
                print(f"✅ 總共備份了 {total_records} 條記錄")
                return total_records
                
    except Exception as e:
        logger.error(f"備份分區數據失敗: {e}")
        return 0
    
    finally:
        db_manager.close_pool()

def drop_all_partitions():
    """刪除所有分區"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.get_connection() as conn:
            with db_manager.get_cursor(conn) as cursor:
                
                print("=== 刪除所有分區 ===")
                
                # 獲取所有分區表
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables 
                    WHERE table_schema = 'binance_data' 
                    AND table_name LIKE 'klines_%'
                    AND table_name != 'klines'
                    ORDER BY table_name DESC
                """)
                
                partitions = [row[0] for row in cursor.fetchall()]
                
                print(f"準備刪除 {len(partitions)} 個分區")
                
                for partition in partitions:
                    print(f"  刪除 {partition}...")
                    cursor.execute(f"DROP TABLE binance_data.{partition} CASCADE")
                
                conn.commit()
                print(f"✅ 成功刪除所有分區")
                return True
                
    except Exception as e:
        logger.error(f"刪除分區失敗: {e}")
        return False
    
    finally:
        db_manager.close_pool()

def recreate_partitions_properly():
    """正確重新創建分區"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.get_connection() as conn:
            with db_manager.get_cursor(conn) as cursor:
                
                print("=== 重新創建分區（正確的邊界） ===")
                
                # 定義要創建的分區（從 2024年1月到 2025年12月）
                partitions_to_create = []
                
                for year in range(2024, 2026):
                    for month in range(1, 13):
                        # 計算正確的時間邊界
                        start_date = datetime(year, month, 1, tzinfo=timezone.utc)
                        
                        if month == 12:
                            end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
                        else:
                            end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)
                        
                        start_ts = int(start_date.timestamp() * 1000)
                        end_ts = int(end_date.timestamp() * 1000)
                        
                        partitions_to_create.append({
                            'name': f'klines_{year}_{month:02d}',
                            'start_ts': start_ts,
                            'end_ts': end_ts,
                            'start_dt': start_date,
                            'end_dt': end_date
                        })
                
                print(f"計劃創建 {len(partitions_to_create)} 個分區")
                
                # 驗證分區邊界沒有重疊
                print("\\n驗證分區邊界...")
                for i in range(len(partitions_to_create) - 1):
                    current = partitions_to_create[i]
                    next_partition = partitions_to_create[i + 1]
                    
                    if current['end_ts'] != next_partition['start_ts']:
                        print(f"❌ 邊界錯誤: {current['name']} 結束 ({current['end_ts']}) != {next_partition['name']} 開始 ({next_partition['start_ts']})")
                        return False
                
                print("✅ 分區邊界驗證通過")
                
                # 創建分區
                print("\\n開始創建分區...")
                success_count = 0
                
                for partition in partitions_to_create:
                    print(f"  創建 {partition['name']}...")
                    print(f"    時間範圍: {partition['start_dt']} 到 {partition['end_dt']}")
                    print(f"    時間戳: {partition['start_ts']} 到 {partition['end_ts']}")
                    
                    try:
                        sql = f"""
                        CREATE TABLE binance_data.{partition['name']} PARTITION OF binance_data.klines
                        FOR VALUES FROM ({partition['start_ts']}) TO ({partition['end_ts']})
                        """
                        
                        cursor.execute(sql)
                        success_count += 1
                        print(f"    ✅ 成功創建")
                        
                    except Exception as e:
                        print(f"    ❌ 創建失敗: {e}")
                
                conn.commit()
                print(f"\\n✅ 成功創建 {success_count}/{len(partitions_to_create)} 個分區")
                
                return success_count == len(partitions_to_create)
                
    except Exception as e:
        logger.error(f"重新創建分區失敗: {e}")
        return False
    
    finally:
        db_manager.close_pool()

def restore_data():
    """恢復數據"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.get_connection() as conn:
            with db_manager.get_cursor(conn) as cursor:
                
                print("=== 恢復數據 ===")
                
                # 檢查備份表是否存在
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_tables 
                        WHERE schemaname = 'pg_temp_1' 
                        AND tablename = 'temp_all_klines_backup'
                    )
                """)
                
                if not cursor.fetchone()[0]:
                    print("❌ 沒有找到備份數據")
                    return False
                
                # 獲取備份數據量
                cursor.execute("SELECT COUNT(*) FROM temp_all_klines_backup")
                backup_count = cursor.fetchone()[0]
                
                print(f"找到 {backup_count} 條備份記錄")
                
                if backup_count > 0:
                    # 恢復數據
                    print("開始恢復數據...")
                    cursor.execute("""
                        INSERT INTO binance_data.klines 
                        SELECT * FROM temp_all_klines_backup
                    """)
                    
                    restored_count = cursor.rowcount
                    conn.commit()
                    
                    print(f"✅ 成功恢復 {restored_count} 條記錄")
                    return True
                else:
                    print("沒有數據需要恢復")
                    return True
                
    except Exception as e:
        logger.error(f"恢復數據失敗: {e}")
        return False
    
    finally:
        db_manager.close_pool()

def complete_partition_rebuild():
    """完整的分區重建流程"""
    
    print("=== 完整的分區重建流程 ===")
    print("⚠️  警告: 這將刪除所有現有分區並重新創建")
    print("    數據將被備份和恢復，但請確保有完整備份")
    
    response = input("\\n確定要繼續嗎? (y/N): ")
    if response.lower() != 'y':
        print("操作已取消")
        return False
    
    try:
        # 1. 分析現有分區
        print("\\n步驟 1: 分析現有分區")
        partition_info, overlaps = analyze_all_partitions()
        
        if overlaps:
            print(f"發現 {len(overlaps)} 個重疊問題，需要重建")
        else:
            print("沒有發現重疊問題")
            return True
        
        # 2. 備份數據
        print("\\n步驟 2: 備份所有分區數據")
        backup_count = backup_all_partition_data()
        
        if backup_count == 0:
            print("沒有數據需要備份，繼續...")
        else:
            print(f"備份了 {backup_count} 條記錄")
        
        # 3. 刪除所有分區
        print("\\n步驟 3: 刪除所有現有分區")
        if not drop_all_partitions():
            print("❌ 刪除分區失敗")
            return False
        
        # 4. 重新創建分區
        print("\\n步驟 4: 重新創建分區")
        if not recreate_partitions_properly():
            print("❌ 重新創建分區失敗")
            return False
        
        # 5. 恢復數據
        if backup_count > 0:
            print("\\n步驟 5: 恢復數據")
            if not restore_data():
                print("❌ 數據恢復失敗")
                return False
        
        # 6. 最終驗證
        print("\\n步驟 6: 最終驗證")
        _, final_overlaps = analyze_all_partitions()
        
        if final_overlaps:
            print(f"❌ 仍然存在 {len(final_overlaps)} 個重疊問題")
            return False
        else:
            print("✅ 分區重建完成，沒有重疊問題")
            return True
        
    except Exception as e:
        logger.error(f"分區重建失敗: {e}")
        return False

if __name__ == "__main__":
    success = complete_partition_rebuild()
    
    if success:
        print("\\n🎉 分區系統重建成功！")
        print("\\n現在可以正常導入數據了:")
        print('python import_data.py --action import-dir --directory "你的目錄路徑"')
    else:
        print("\\n❌ 分區重建失敗，請檢查錯誤信息")
