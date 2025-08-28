#!/usr/bin/env python3
"""
重置並設置 Binance 資料庫（增強版）
安全地清理現有架構並重新創建，支援所有表分區和新的數據源管理
"""

import os
import sys
from pathlib import Path

# 添加當前目錄到 Python 路徑
sys.path.append(str(Path(__file__).parent.parent))

from database_config import DatabaseManager

def reset_and_setup_database():
    """重置並設置資料庫架構"""
    try:
        print("🔗 連接到資料庫...")
        db_manager = DatabaseManager()
        
        # 測試連接
        if not db_manager.test_connection():
            print("❌ 資料庫連接失敗")
            return False
        
        print("✅ 資料庫連接成功")
        
        # Step 1: 重置資料庫
        print("\n🗑️ 清理現有架構...")
        reset_script = Path(__file__).parent / 'reset_database.sql'
        
        if reset_script.exists():
            db_manager.execute_script_file(str(reset_script))
            print("✅ 現有架構已清理")
        else:
            print("⚠️ 重置腳本未找到，跳過清理")
        
        # Step 2: 創建新架構
        print("\n🏗️ 創建新的資料庫架構...")
        schema_script = Path(__file__).parent / 'create_schema.sql'
        
        if not schema_script.exists():
            print(f"❌ 找不到架構腳本: {schema_script}")
            return False
        
        db_manager.execute_script_file(str(schema_script))
        print("✅ 新架構創建完成")
        
        # Step 3: 驗證設置
        print("\n🔍 驗證架構設置...")
        success = verify_database_setup(db_manager)
        
        # Step 4: 初始化分區管理器
        print("\n⚙️ 初始化分區管理器...")
        initialize_partition_manager(db_manager)
        
        db_manager.close_pool()
        return success
        
    except Exception as e:
        print(f"❌ 重置失敗: {e}")
        import traceback
        print(f"詳細錯誤: {traceback.format_exc()}")
        return False

def initialize_partition_manager(db_manager):
    """初始化分區管理器和觸發器"""
    try:
        print("  🔧 測試自動分區功能...")
        
        # 測試分區觸發器是否正常工作
        with db_manager.get_cursor() as cursor:
            # 測試插入一筆 klines 數據來觸發分區創建
            test_timestamp = int(1704067200000)  # 2024-01-01 00:00:00 UTC
            
            cursor.execute("""
                INSERT INTO binance_data.klines 
                (symbol_id, trading_type, interval_type, open_time, open_price, high_price, 
                 low_price, close_price, volume, close_time, quote_asset_volume, 
                 number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume)
                SELECT 
                    1, 'spot', '1h', %s, 50000, 51000, 49000, 50500, 100, 
                    %s, 5000000, 1000, 60, 3000000
                WHERE EXISTS (SELECT 1 FROM binance_data.symbols WHERE id = 1)
                ON CONFLICT DO NOTHING;
            """, (test_timestamp, test_timestamp + 3600000))
            
            # 檢查分區是否自動創建
            cursor.execute("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname = 'binance_data' 
                AND tablename LIKE 'klines_2024_%'
                ORDER BY tablename;
            """)
            partitions = cursor.fetchall()
            
            if partitions:
                print(f"  ✅ 自動分區功能正常 ({len(partitions)} 個分區已創建)")
                
                # 清理測試數據
                cursor.execute("DELETE FROM binance_data.klines WHERE symbol_id = 1;")
                print("  ✅ 測試數據已清理")
            else:
                print("  ⚠️ 分區創建可能需要手動觸發")
        
        print("  ✅ 分區管理器初始化完成")
        
    except Exception as e:
        print(f"  ⚠️ 分區管理器初始化失敗: {e}")

def verify_database_setup(db_manager):
    """驗證資料庫設置"""
    try:
        # 檢查 schema
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = 'binance_data';
            """)
            schema_exists = cursor.fetchone()
            
            if not schema_exists:
                print("❌ Schema binance_data 未創建")
                return False
            
            print("✅ Schema binance_data 已創建")
        
        # 檢查數據源配置表
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as source_count
                FROM binance_data.data_sources;
            """)
            source_count = cursor.fetchone()[0]
            
            print(f"📋 數據源配置: {source_count} 個")
            if source_count > 0:
                print("  ✅ 數據源配置已載入")
            else:
                print("  ❌ 數據源配置未載入")
        
        # 檢查主要表
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'binance_data'
                AND table_name IN ('symbols', 'sync_status', 'klines', 'trades', 'agg_trades', 'bvol_index', 'data_sources')
                ORDER BY table_name;
            """)
            core_tables = cursor.fetchall()
            
            expected_tables = ['agg_trades', 'bvol_index', 'data_sources', 'klines', 'symbols', 'sync_status', 'trades']
            found_tables = [table[0] for table in core_tables]
            
            print(f"📊 核心表檢查 ({len(found_tables)}/{len(expected_tables)}):")
            for table in expected_tables:
                if table in found_tables:
                    print(f"  ✅ {table}")
                else:
                    print(f"  ❌ {table}")
        
        # 檢查分區表設置（主表）
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    t.tablename,
                    pg_get_partkeydef(c.oid) as partition_key
                FROM pg_tables t
                JOIN pg_class c ON c.relname = t.tablename
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'binance_data'
                AND c.relkind = 'p'  -- 分區表
                ORDER BY t.tablename;
            """)
            partitioned_tables = cursor.fetchall()
            
            expected_partitioned_tables = [
                'agg_trades', 'book_depth', 'book_ticker', 'bvol_index',
                'funding_rates', 'index_price_klines', 'klines', 
                'mark_price_klines', 'premium_index_klines', 'trades', 'trading_metrics'
            ]
            found_partitioned = [table[0] for table in partitioned_tables]
            
            print(f"\n🔀 分區表檢查 ({len(found_partitioned)}/{len(expected_partitioned_tables)}):")
            for table in expected_partitioned_tables:
                if table in found_partitioned:
                    print(f"  ✅ {table}")
                else:
                    print(f"  ❌ {table}")
        
        # 檢查觸發器
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    trigger_name,
                    event_object_table
                FROM information_schema.triggers
                WHERE trigger_schema = 'binance_data'
                AND trigger_name LIKE '%partition%'
                ORDER BY event_object_table;
            """)
            triggers = cursor.fetchall()
            
            print(f"\n🔧 分區觸發器檢查 ({len(triggers)} 個):")
            for trigger_name, table_name in triggers:
                print(f"  ✅ {table_name}: {trigger_name}")
        
        # 檢查視圖
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = 'binance_data'
                ORDER BY table_name;
            """)
            views = cursor.fetchall()
            
            expected_views = ['v_data_sources', 'v_klines_summary', 'v_partition_summary', 'v_sync_overview']
            found_views = [v[0] for v in views]
            
            print(f"\n👁️ 視圖檢查 ({len(found_views)}/{len(expected_views)}):")
            for view in expected_views:
                if view in found_views:
                    print(f"  ✅ {view}")
                else:
                    print(f"  ❌ {view}")
        
        # 檢查函數
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT routine_name
                FROM information_schema.routines
                WHERE routine_schema = 'binance_data'
                AND routine_type = 'FUNCTION'
                ORDER BY routine_name;
            """)
            functions = cursor.fetchall()
            
            expected_functions = [
                'cleanup_old_partitions', 'create_partition_if_not_exists', 
                'create_year_partitions', 'get_data_source_id', 'partition_insert_trigger'
            ]
            found_functions = [f[0] for f in functions]
            
            print(f"\n⚙️ 函數檢查 ({len(found_functions)}/{len(expected_functions)}):")
            for func in expected_functions:
                if func in found_functions:
                    print(f"  ✅ {func}")
                else:
                    print(f"  ❌ {func}")
        
        # 檢查索引
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as index_count
                FROM pg_indexes
                WHERE schemaname = 'binance_data';
            """)
            index_count = cursor.fetchone()[0]
            
            print(f"\n🔍 索引檢查: {index_count} 個索引")
            if index_count > 0:
                print("  ✅ 索引已創建")
            else:
                print("  ❌ 未找到索引")
        
        # 檢查示例數據
        with db_manager.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM binance_data.symbols;")
            symbol_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM binance_data.data_sources;")
            source_count = cursor.fetchone()[0]
            
            print(f"\n📝 初始數據:")
            print(f"  📊 交易對: {symbol_count} 個")
            print(f"  📋 數據源: {source_count} 個")
        
        # 測試數據源查詢
        print(f"\n🧪 測試數據源查詢...")
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT trading_type, COUNT(*) as count
                FROM binance_data.data_sources 
                WHERE is_active = true
                GROUP BY trading_type
                ORDER BY trading_type;
            """)
            active_sources = cursor.fetchall()
            
            for trading_type, count in active_sources:
                print(f"  ✅ {trading_type}: {count} 個數據源")
        
        return True
        
    except Exception as e:
        print(f"❌ 驗證失敗: {e}")
        return False

def show_data_source_summary(db_manager):
    """顯示數據源配置總覽"""
    try:
        print("\n📊 數據源配置總覽:")
        print("-" * 60)
        
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    trading_type,
                    market_data_type,
                    description,
                    CASE WHEN supports_intervals THEN '支援間隔' ELSE '固定格式' END as interval_support,
                    time_column
                FROM binance_data.data_sources
                ORDER BY trading_type, market_data_type;
            """)
            sources = cursor.fetchall()
            
            current_type = None
            for source in sources:
                trading_type, market_data_type, description, interval_support, time_column = source
                
                if current_type != trading_type:
                    print(f"\n📈 {trading_type.upper()} 市場:")
                    current_type = trading_type
                
                print(f"  • {market_data_type:<20} - {description} ({interval_support}, 分區鍵: {time_column})")
        
        print("-" * 60)
        
    except Exception as e:
        print(f"❌ 無法顯示數據源總覽: {e}")

def interactive_setup():
    """互動式設置"""
    print("=" * 70)
    print("🚀 Binance 資料庫重置和設置工具（增強版）")
    print("=" * 70)
    
    print("\n🌟 新功能:")
    print("  • 所有時間序列表都支援分區")
    print("  • 插入資料時自動創建分區")
    print("  • 支援新的期權市場數據源（BVOL指數）")
    print("  • 完整的數據源管理系統")
    
    print("\n⚠️ 警告：此操作將刪除現有的 binance_data schema 及其所有數據！")
    
    while True:
        response = input("\n是否繼續？(y/n): ").lower().strip()
        if response in ['y', 'yes', '是']:
            break
        elif response in ['n', 'no', '否']:
            print("操作已取消")
            return False
        else:
            print("請輸入 y 或 n")
    
    success = reset_and_setup_database()
    
    if success:
        # 創建 DatabaseManager 來顯示數據源總覽
        db_manager = DatabaseManager()
        show_data_source_summary(db_manager)
        db_manager.close_pool()
    
    return success

def quick_partition_test():
    """快速分區功能測試"""
    try:
        print("\n🧪 執行分區功能測試...")
        db_manager = DatabaseManager()
        
        # 測試自動創建 2024 年分區
        print("  🔧 測試批量創建分區...")
        with db_manager.get_cursor() as cursor:
            cursor.execute("SELECT binance_data.create_year_partitions(2024);")
            created_count = cursor.fetchone()[0]
            print(f"  ✅ 成功創建 {created_count} 個 2024 年分區")
        
        # 測試分區統計
        print("  📊 檢查分區統計...")
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT base_table, COUNT(*) as partition_count
                FROM binance_data.v_partition_summary 
                WHERE table_type = 'partition' AND partition_year = '2024'
                GROUP BY base_table
                ORDER BY base_table;
            """)
            partition_stats = cursor.fetchall()
            
            for base_table, count in partition_stats:
                print(f"    📅 {base_table}: {count} 個月分區")
        
        db_manager.close_pool()
        print("  ✅ 分區功能測試完成")
        
    except Exception as e:
        print(f"  ❌ 分區測試失敗: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--force':
        # 強制執行，不詢問
        print("🔥 強制模式：跳過確認")
        success = reset_and_setup_database()
        if success:
            quick_partition_test()
    elif len(sys.argv) > 1 and sys.argv[1] == '--test-partition':
        # 只測試分區功能
        quick_partition_test()
    else:
        # 互動式執行
        success = interactive_setup()
        if success:
            quick_partition_test()
    
    if success:
        print("\n" + "🎉" * 25)
        print("🎉 資料庫重置和設置完成！")
        print("🎉" * 25)
        
        print("\n📖 下一步操作:")
        print("1. 測試基本功能:")
        print("   python database_config.py")
        print("\n2. 測試資料導入:")
        print("   python data_importer.py")
        print("\n3. 測試分區管理:")
        print("   python setup_database.py --test-partition")
        print("\n4. 查看數據源配置:")
        print("   SELECT * FROM binance_data.v_data_sources;")
        print("\n5. 批量導入資料:")
        print("   python import_data.py --action bulk-import")
        
        print("\n🔧 管理命令:")
        print("• 創建年度分區: SELECT binance_data.create_year_partitions(2025);")
        print("• 清理舊分區: SELECT binance_data.cleanup_old_partitions(24);")
        print("• 查看分區統計: SELECT * FROM binance_data.v_partition_summary;")
        
    else:
        print("\n💥 設置失敗")
        print("\n🔧 故障排除:")
        print("1. 檢查 PostgreSQL 服務是否運行")
        print("2. 檢查 .env 文件中的資料庫配置")
        print("3. 確認有足夠的資料庫權限")
        print("4. 查看上方的詳細錯誤信息")
        print("5. 嘗試手動執行 create_schema.sql")
