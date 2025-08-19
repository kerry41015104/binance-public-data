#!/usr/bin/env python3
"""
重置並設置 Binance 資料庫
安全地清理現有架構並重新創建
"""

import os
import sys
from pathlib import Path

# 添加當前目錄到 Python 路徑
sys.path.append(str(Path(__file__).parent))

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
        schema_script = Path(__file__).parent / 'create_schema_fixed.sql'
        
        if not schema_script.exists():
            print(f"❌ 找不到架構腳本: {schema_script}")
            return False
        
        db_manager.execute_script_file(str(schema_script))
        print("✅ 新架構創建完成")
        
        # Step 3: 驗證設置
        print("\n🔍 驗證架構設置...")
        success = verify_database_setup(db_manager)
        
        db_manager.close_pool()
        return success
        
    except Exception as e:
        print(f"❌ 重置失敗: {e}")
        import traceback
        print(f"詳細錯誤: {traceback.format_exc()}")
        return False

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
        
        # 檢查主要表
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'binance_data'
                AND table_name IN ('symbols', 'sync_status', 'klines', 'trades', 'agg_trades')
                ORDER BY table_name;
            """)
            core_tables = cursor.fetchall()
            
            expected_tables = ['agg_trades', 'klines', 'symbols', 'sync_status', 'trades']
            found_tables = [table[0] for table in core_tables]
            
            print(f"📊 核心表檢查 ({len(found_tables)}/{len(expected_tables)}):")
            for table in expected_tables:
                if table in found_tables:
                    print(f"  ✅ {table}")
                else:
                    print(f"  ❌ {table}")
        
        # 檢查分區表
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables 
                WHERE table_schema = 'binance_data' 
                AND table_name LIKE 'klines_2024_%'
                ORDER BY table_name;
            """)
            partitions = cursor.fetchall()
            
            print(f"\n📅 分區表檢查 ({len(partitions)}/12):")
            expected_partitions = [f"klines_2024_{i:02d}" for i in range(1, 13)]
            found_partitions = [p[0] for p in partitions]
            
            for partition in expected_partitions:
                if partition in found_partitions:
                    print(f"  ✅ {partition}")
                else:
                    print(f"  ❌ {partition}")
        
        # 檢查視圖
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = 'binance_data'
                ORDER BY table_name;
            """)
            views = cursor.fetchall()
            
            print(f"\n👁️ 視圖檢查 ({len(views)}/2):")
            expected_views = ['v_klines_summary', 'v_sync_overview']
            found_views = [v[0] for v in views]
            
            for view in expected_views:
                if view in found_views:
                    print(f"  ✅ {view}")
                else:
                    print(f"  ❌ {view}")
        
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
        
        # 測試插入權限
        print(f"\n🧪 測試基本功能...")
        with db_manager.get_cursor() as cursor:
            # 測試插入一個測試交易對
            cursor.execute("""
                INSERT INTO binance_data.symbols (symbol, base_asset, quote_asset, trading_type)
                VALUES ('TEST', 'TEST', 'USDT', 'spot')
                ON CONFLICT (symbol) DO NOTHING
                RETURNING id;
            """)
            
            test_id = cursor.fetchone()
            if test_id:
                print("  ✅ 數據插入測試成功")
                
                # 清理測試數據
                cursor.execute("DELETE FROM binance_data.symbols WHERE symbol = 'TEST';")
                print("  ✅ 測試數據已清理")
            else:
                print("  ⚠️ 測試交易對可能已存在")
        
        return True
        
    except Exception as e:
        print(f"❌ 驗證失敗: {e}")
        return False

def interactive_setup():
    """互動式設置"""
    print("=" * 60)
    print("🚀 Binance 資料庫重置和設置工具")
    print("=" * 60)
    
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
    
    return reset_and_setup_database()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--force':
        # 強制執行，不詢問
        success = reset_and_setup_database()
    else:
        # 互動式執行
        success = interactive_setup()
    
    if success:
        print("\n" + "🎉" * 20)
        print("🎉 資料庫重置和設置完成！")
        print("🎉" * 20)
        
        print("\n📖 下一步操作:")
        print("1. 測試基本功能:")
        print("   python database_config.py")
        print("\n2. 測試資料導入:")
        print("   python import_data.py --action import-file --file 'your_file.csv'")
        print("\n3. 批量導入:")
        print("   python import_data.py --action bulk-import")
        
    else:
        print("\n💥 設置失敗")
        print("\n🔧 故障排除:")
        print("1. 檢查 PostgreSQL 服務是否運行")
        print("2. 檢查 .env 文件中的資料庫配置")
        print("3. 確認有足夠的資料庫權限")
        print("4. 查看上方的詳細錯誤信息")
