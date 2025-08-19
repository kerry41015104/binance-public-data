#!/usr/bin/env python3
"""
批量增量更新器
自動掃描目錄下的所有標的，並根據目錄結構自動推斷參數進行增量更新
"""

import os
import sys
from pathlib import Path
import glob
from datetime import datetime, timedelta
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# 添加當前目錄到 Python 路徑
sys.path.append(str(Path(__file__).parent))

from data_importer import DataImporter
from database_config import DatabaseManager
from universal_logger import create_logger


class BulkIncrementalUpdater:
    """批量增量更新器"""
    
    def __init__(self, db_manager=None, days_back=7, max_workers=2):
        self.db = db_manager or DatabaseManager()
        self.days_back = days_back
        self.max_workers = max_workers
        self.importer = DataImporter(self.db)
        self.logger = create_logger(f"bulk_incremental_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
    def parse_directory_structure(self, data_directory):
        """
        解析目錄結構，自動推斷參數
        例如: D:/crypto-data-overall/binance-public-data/data/futures/um/daily/klines
        返回: {
            'trading_type': 'um',
            'data_type': 'klines',
            'symbols': ['BTCUSDT', 'ETHUSDT', ...],
            'intervals': ['1m', '5m', ...] (如果適用)
        }
        """
        try:
            # 標準化路徑
            data_path = Path(data_directory).resolve()
            
            # 檢查路徑是否存在
            if not data_path.exists():
                self.logger.logger.error(f"目錄不存在: {data_path}")
                return None
            
            # 解析路徑結構
            path_parts = data_path.parts
            
            # 查找關鍵路徑組件
            trading_type = None
            data_type = None
            
            # 從路徑中提取 trading_type
            if 'spot' in path_parts:
                trading_type = 'spot'
            elif 'um' in path_parts:
                trading_type = 'um' 
            elif 'cm' in path_parts:
                trading_type = 'cm'
            
            # 從路徑中提取 data_type (通常是最後一個目錄)
            data_type = data_path.name
            
            self.logger.logger.info(f"解析路徑: {data_path}")
            self.logger.logger.info(f"推斷的交易類型: {trading_type}")
            self.logger.logger.info(f"推斷的資料類型: {data_type}")
            
            # 獲取所有標的目錄
            symbols = []
            intervals = []
            
            # 遍歷子目錄以找到標的
            for item in data_path.iterdir():
                if item.is_dir():
                    # 檢查是否是標的目錄 (通常包含USDT等)
                    dir_name = item.name
                    if self._is_symbol_directory(dir_name):
                        symbols.append(dir_name)
                        
                        # 如果是 klines 類型，還需要檢查時間間隔
                        if data_type in ['klines', 'indexPriceKlines', 'markPriceKlines', 'premiumIndexKlines']:
                            symbol_intervals = self._get_intervals_for_symbol(item)
                            intervals.extend(symbol_intervals)
            
            # 去重並排序
            symbols = sorted(list(set(symbols)))
            intervals = sorted(list(set(intervals)))
            
            self.logger.logger.info(f"找到 {len(symbols)} 個標的: {symbols[:10]}{'...' if len(symbols) > 10 else ''}")
            if intervals:
                self.logger.logger.info(f"找到間隔: {intervals}")
            
            return {
                'trading_type': trading_type,
                'data_type': data_type,
                'symbols': symbols,
                'intervals': intervals or [None],  # 如果沒有間隔，設為 None
                'base_path': str(data_path)
            }
            
        except Exception as e:
            self.logger.logger.error(f"解析目錄結構失敗: {e}")
            return None
    
    def _is_symbol_directory(self, dir_name):
        """判斷是否是標的目錄"""
        # 常見的加密貨幣標的模式
        patterns = ['USDT', 'BTC', 'ETH', 'BNB', 'USD', 'BUSD']
        return any(pattern in dir_name.upper() for pattern in patterns)
    
    def _get_intervals_for_symbol(self, symbol_path):
        """獲取特定標的的時間間隔"""
        intervals = []
        for item in symbol_path.iterdir():
            if item.is_dir():
                # 檢查是否是時間間隔目錄
                interval_name = item.name
                if self._is_interval_directory(interval_name):
                    intervals.append(interval_name)
        return intervals
    
    def _is_interval_directory(self, dir_name):
        """判斷是否是時間間隔目錄"""
        # 常見的時間間隔模式
        interval_patterns = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        return dir_name in interval_patterns
    
    def bulk_incremental_update(self, data_directory):
        """
        批量增量更新
        自動掃描目錄並更新所有標的
        """
        try:
            self.logger.logger.info(f"開始批量增量更新: {data_directory}")
            
            # 解析目錄結構
            directory_info = self.parse_directory_structure(data_directory)
            if not directory_info:
                self.logger.logger.error("無法解析目錄結構")
                return False
            
            trading_type = directory_info['trading_type']
            data_type = directory_info['data_type']
            symbols = directory_info['symbols']
            intervals = directory_info['intervals']
            
            if not trading_type or not data_type:
                self.logger.logger.error("無法推斷交易類型或資料類型")
                return False
            
            if not symbols:
                self.logger.logger.error("未找到任何標的")
                return False
            
            total_tasks = 0
            successful_updates = 0
            failed_updates = 0
            failed_symbols = []
            
            # 計算總任務數
            for symbol in symbols:
                for interval in intervals:
                    total_tasks += 1
            
            self.logger.logger.info(f"準備更新 {len(symbols)} 個標的，{len(intervals)} 個間隔，總共 {total_tasks} 個任務")
            
            # 使用線程池進行並行處理
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交所有任務
                future_to_task = {}
                
                for symbol in symbols:
                    for interval in intervals:
                        future = executor.submit(
                            self._update_single_symbol,
                            symbol, data_type, trading_type, interval
                        )
                        future_to_task[future] = {
                            'symbol': symbol,
                            'interval': interval,
                            'data_type': data_type,
                            'trading_type': trading_type
                        }
                
                # 處理完成的任務
                for future in as_completed(future_to_task):
                    task_info = future_to_task[future]
                    try:
                        success = future.result()
                        if success:
                            successful_updates += 1
                            self.logger.logger.info(
                                f"✓ 成功更新: {task_info['symbol']} "
                                f"{task_info['interval'] or ''} "
                                f"({successful_updates}/{total_tasks})"
                            )
                        else:
                            failed_updates += 1
                            failed_symbols.append(f"{task_info['symbol']}_{task_info['interval'] or 'no_interval'}")
                            self.logger.logger.error(
                                f"✗ 更新失敗: {task_info['symbol']} "
                                f"{task_info['interval'] or ''}"
                            )
                    except Exception as e:
                        failed_updates += 1
                        failed_symbols.append(f"{task_info['symbol']}_{task_info['interval'] or 'no_interval'}")
                        self.logger.logger.error(
                            f"✗ 更新異常: {task_info['symbol']} "
                            f"{task_info['interval'] or ''} - {e}"
                        )
            
            # 記錄最終統計
            self.logger.logger.info(f"批量更新完成:")
            self.logger.logger.info(f"  成功: {successful_updates}")
            self.logger.logger.info(f"  失敗: {failed_updates}")
            self.logger.logger.info(f"  總計: {total_tasks}")
            
            if failed_symbols:
                self.logger.logger.error(f"失敗的標的: {', '.join(failed_symbols[:10])}")
                if len(failed_symbols) > 10:
                    self.logger.logger.error(f"... 及其他 {len(failed_symbols) - 10} 個")
            
            # 完成日誌記錄
            self.logger.finalize_log()
            
            return successful_updates > 0
            
        except Exception as e:
            self.logger.logger.error(f"批量更新失敗: {e}")
            self.logger.finalize_log()
            return False
    
    def _update_single_symbol(self, symbol, data_type, trading_type, interval):
        """更新單個標的"""
        try:
            return self.importer.incremental_update(
                symbol=symbol,
                data_type=data_type,
                trading_type=trading_type,
                interval=interval,
                days_back=self.days_back
            )
        except Exception as e:
            self.logger.logger.error(f"更新 {symbol} 失敗: {e}")
            return False


def main():
    """主函數"""
    import argparse
    
    parser = argparse.ArgumentParser(description="批量增量更新工具")
    parser.add_argument(
        "--directory",
        required=True,
        help="資料目錄路徑，例如: D:/crypto-data-overall/binance-public-data/data/futures/um/daily/klines"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="回溯天數 (預設: 7)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="最大並行工作線程數 (預設: 2)"
    )
    parser.add_argument(
        "--test-parse",
        action='store_true',
        help="只測試目錄解析，不執行更新"
    )
    
    args = parser.parse_args()
    
    try:
        # 創建資料庫管理器
        db_manager = DatabaseManager()
        
        # 創建批量更新器
        updater = BulkIncrementalUpdater(
            db_manager=db_manager,
            days_back=args.days_back,
            max_workers=args.max_workers
        )
        
        if args.test_parse:
            # 只測試目錄解析
            print(f"測試解析目錄: {args.directory}")
            directory_info = updater.parse_directory_structure(args.directory)
            
            if directory_info:
                print(f"交易類型: {directory_info['trading_type']}")
                print(f"資料類型: {directory_info['data_type']}")
                print(f"標的數量: {len(directory_info['symbols'])}")
                print(f"標的樣例: {directory_info['symbols'][:5]}")
                print(f"間隔: {directory_info['intervals']}")
            else:
                print("目錄解析失敗")
        else:
            # 執行批量更新
            print(f"開始批量增量更新...")
            success = updater.bulk_incremental_update(args.directory)
            
            if success:
                print("批量更新完成")
            else:
                print("批量更新失敗")
    
    except Exception as e:
        print(f"錯誤: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if 'db_manager' in locals():
            db_manager.close_pool()


if __name__ == "__main__":
    main()
