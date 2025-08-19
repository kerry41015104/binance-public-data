#!/usr/bin/env python3
"""
Binance 資料導入腳本 (改進版)
用於將下載的資料導入到 PostgreSQL 資料庫
支援多種檔案格式：CSV, ZIP, Parquet, GZ 等
"""

import sys
import os
from pathlib import Path

# 添加當前目錄到 Python 路徑
sys.path.append(str(Path(__file__).parent))

from data_importer import DataImporter
from enhanced_bulk_import import EnhancedBulkImportManager
from database_config import DatabaseManager, SymbolManager
from universal_logger import create_logger
import argparse
import logging


def main():
    parser = argparse.ArgumentParser(description="Binance 資料導入工具 (改進版)")
    parser.add_argument(
        "--action",
        choices=["import-file", "import-dir", "bulk-import", "incremental", "bulk-incremental", "test-parse", "test-dir"],
        required=True,
        help="執行動作",
    )
    parser.add_argument("--file", help="要導入的文件路徑")
    parser.add_argument("--directory", help="要導入的目錄路徑")
    parser.add_argument("--trading-type", choices=["spot", "um", "cm"], help="交易類型")
    parser.add_argument("--symbol", help="交易對符號")
    parser.add_argument("--data-type", help="資料類型")
    parser.add_argument("--interval", help="時間間隔 (僅K線資料)")
    parser.add_argument("--days-back", type=int, default=7, help="增量更新回溯天數")
    parser.add_argument("--max-workers", type=int, default=2, help="並行處理線程數")

    args = parser.parse_args()

    try:
        # 初始化資料庫管理器
        db_manager = DatabaseManager()

        if args.action == "import-file":
            if not args.file:
                print("錯誤: 請指定要導入的文件")
                return

            # 創建通用日誌記錄器
            logger = create_logger("import_file")
            logger.logger.info(f"開始執行 import-file 動作: {args.file}")

            importer = DataImporter(db_manager)
            success = importer.import_single_file(args.file, args.trading_type)

            if success:
                logger.log_file_processing(args.file, success=True)
                print("文件導入成功")
            else:
                logger.log_file_processing(args.file, success=False)
                print("文件導入失敗")

            # 完成日誌記錄
            logger.finalize_log()

        elif args.action == "import-dir":
            if not args.directory:
                print("錯誤: 請指定要導入的目錄")
                return

            # 創建通用日誌記錄器
            logger = create_logger("import_dir")
            logger.logger.info(f"開始執行 import-dir 動作: {args.directory}")

            # 創建導入器並設置外部日誌記錄器
            importer = DataImporter(db_manager)
            importer.set_external_logger(logger)
            
            # 執行導入並獲取結果
            result = importer.import_directory(args.directory, max_workers=args.max_workers)
            
            # 直接更新統計信息 - 使用 DataImporter 返回的實際結果
            if result:
                successful_files = result.get('successful_imports', 0)
                failed_files = result.get('failed_imports', 0)
                total_files = successful_files + failed_files
                failed_files_list = result.get('failed_files', [])
                
                # 直接設置統計值
                logger.stats['successful_files'] = successful_files
                logger.stats['failed_files'] = failed_files  
                logger.stats['total_files'] = total_files
                logger.stats['failed_files_list'] = failed_files_list
                
                # 記錄最終結果
                logger.logger.info(f"導入結果: 成功 {successful_files}, 失敗 {failed_files}, 總計 {total_files}")
                
                # 如果有失敗文件，記錄前幾個
                if failed_files_list:
                    logger.logger.error(f"失敗文件数量: {len(failed_files_list)}")
                    for i, failed_file in enumerate(failed_files_list[:5], 1):
                        logger.logger.error(f"  {i}. {os.path.basename(failed_file)}")
                    if len(failed_files_list) > 5:
                        logger.logger.error(f"  ... 及其他 {len(failed_files_list) - 5} 個文件")

            # 完成日誌記錄
            logger.finalize_log()

        elif args.action == "bulk-import":
            # 使用增強版批量導入管理器
            bulk_manager = EnhancedBulkImportManager(
                db_manager, action_name="bulk_import"
            )

            base_dir = args.directory or os.getenv("STORE_DIRECTORY")
            if not base_dir:
                print("錯誤: 請指定基礎目錄或設置 STORE_DIRECTORY 環境變數")
                return

            # 設置初始交易對（可選）
            try:
                print("正在設置初始交易對...")
                symbol_manager = SymbolManager(db_manager)
                # 這里可以添加設置逻輯，或者略過
            except Exception as e:
                print(f"警告: 設置初始交易對失敗: {e}")

            # 執行批量導入
            bulk_manager.import_all_data(
                base_dir, trading_types=["um"], max_workers=args.max_workers
            )

        elif args.action == "incremental":
            if not all([args.symbol, args.data_type, args.trading_type]):
                print("錯誤: 增量更新需要指定 symbol, data-type, trading-type")
                return

            # 創建通用日誌記錄器
            logger = create_logger("incremental")
            logger.logger.info(
                f"開始執行 incremental 動作: {args.symbol} {args.data_type} {args.trading_type}"
            )

            importer = DataImporter(db_manager)
            success = importer.incremental_update(
                args.symbol,
                args.data_type,
                args.trading_type,
                args.interval,
                args.days_back,
            )

            if success:
                logger.logger.info("增量更新成功")
                print("增量更新成功")
            else:
                logger.logger.error("增量更新失敗")
                print("增量更新失敗")

            # 完成日誌記錄
            logger.finalize_log()

        elif args.action == "bulk-incremental":
            if not args.directory:
                print("錯誤: 批量增量更新需要指定目錄")
                print("例如: --directory D:/code/Trading-Universe/binance-public-data/data/futures/um/daily/klines")
                return

            # 創建通用日誌記錄器
            logger = create_logger("bulk_incremental")
            logger.logger.info(f"開始執行 bulk-incremental 動作: {args.directory}")

            # 導入批量增量更新器
            from bulk_incremental_updater import BulkIncrementalUpdater

            # 創建批量增量更新器
            bulk_updater = BulkIncrementalUpdater(
                db_manager=db_manager,
                days_back=args.days_back,
                max_workers=args.max_workers
            )

            print(f"開始批量增量更新目錄: {args.directory}")
            print(f"回溯天數: {args.days_back}")
            print(f"並行線程數: {args.max_workers}")

            success = bulk_updater.bulk_incremental_update(args.directory)

            if success:
                logger.logger.info("批量增量更新成功")
                print("批量增量更新完成")
            else:
                logger.logger.error("批量增量更新失敗")
                print("批量增量更新失敗")

            # 完成日誌記錄
            logger.finalize_log()

        elif args.action == "test-parse":
            if not args.directory:
                print("錯誤: 請指定要測試的目錄")
                return

            print(f"測試解析目錄: {args.directory}")
            
            # 導入批量增量更新器
            from bulk_incremental_updater import BulkIncrementalUpdater
            
            # 創建臨時更新器進行測試
            bulk_updater = BulkIncrementalUpdater(
                db_manager=db_manager,
                days_back=args.days_back,
                max_workers=args.max_workers
            )
            
            directory_info = bulk_updater.parse_directory_structure(args.directory)
            
            if directory_info:
                print("✓ 目錄解析成功")
                print(f"  交易類型: {directory_info['trading_type']}")
                print(f"  資料類型: {directory_info['data_type']}")
                print(f"  標的數量: {len(directory_info['symbols'])}")
                print(f"  標的樣例: {directory_info['symbols'][:10]}")
                if len(directory_info['symbols']) > 10:
                    print(f"  ... 及其他 {len(directory_info['symbols']) - 10} 個標的")
                print(f"  時間間隔: {directory_info['intervals']}")
                
                total_tasks = len(directory_info['symbols']) * len(directory_info['intervals'])
                print(f"  預計任務數: {total_tasks}")
            else:
                print("✗ 目錄解析失敗")

        elif args.action == "test-dir":
            if not args.directory:
                print("錯誤: 請指定要測試的目錄")
                return

            # 創建通用日誌記錄器
            logger = create_logger("test_dir")
            logger.logger.info(f"開始執行 test-dir 動作: {args.directory}")

            importer = DataImporter(db_manager)
            # 只列出目錄內容，不實際導入
            importer._list_directory_contents(args.directory, max_files=50)

            logger.logger.info("目錄測試完成")

            # 完成日誌記錄
            logger.finalize_log()

    except Exception as e:
        print(f"錯誤: {e}")
        import traceback

        traceback.print_exc()

    finally:
        if "db_manager" in locals():
            db_manager.close_pool()


if __name__ == "__main__":
    main()
