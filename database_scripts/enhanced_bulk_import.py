"""
改進的批量導入管理器
支援詳細日誌記錄和失敗文件追蹤
"""

import os
import logging
from datetime import datetime
from pathlib import Path
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
from data_importer import DataImporter
from database_config import DatabaseManager, SymbolManager

class EnhancedBulkImportManager:
    """增強版批量導入管理器 - 支援詳細日誌和失敗追蹤"""
    
    def __init__(self, db_manager=None, action_name="bulk_import"):
        self.db = db_manager or DatabaseManager()
        self.importer = DataImporter(self.db)
        self.symbol_manager = SymbolManager(self.db)
        
        # 統計信息
        self.stats = {
            'total_files': 0,
            'successful_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'successful_symbols': set(),
            'failed_files_list': [],
            'start_time': None,
            'end_time': None
        }
        
        # 設置專用日誌
        self.setup_detailed_logging(action_name)
    
    def setup_detailed_logging(self, action_name):
        """設置詳細日誌記錄"""
        # 創建日誌文件名：{action_name}_{今天日期}.log
        today = datetime.now().strftime('%Y%m%d')
        log_filename = f"{action_name}_{today}.log"
        log_path = os.path.join(os.path.dirname(__file__), 'logs', log_filename)
        
        # 確保日誌目錄存在
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        
        # 創建專用日誌記錄器
        self.bulk_logger = logging.getLogger(f'bulk_import_{action_name}')
        self.bulk_logger.setLevel(logging.INFO)
        
        # 避免重複處理器
        if not self.bulk_logger.handlers:
            # 文件處理器
            file_handler = logging.FileHandler(log_path, encoding='utf-8')
            file_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            self.bulk_logger.addHandler(file_handler)
            
            # 控制台處理器
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            self.bulk_logger.addHandler(console_handler)
        
        self.log_file = log_path
        self.bulk_logger.info(f"=== 批量導入任務開始 ===")
        self.bulk_logger.info(f"日誌文件: {log_path}")
    
    def scan_directory_structure(self, base_directory):
        """掃描並分析目錄結構"""
        self.bulk_logger.info(f"掃描目錄結構: {base_directory}")
        
        structure_info = {
            'symbols': set(),
            'intervals': set(),
            'file_types': set(),
            'total_files': 0,
            'directories': []
        }
        
        # 支援的檔案格式
        file_patterns = ["*.csv", "*.zip", "*.parquet", "*.gz", "*.feather", "*.h5"]
        
        for root, dirs, files in os.walk(base_directory):
            if files:  # 只記錄有文件的目錄
                structure_info['directories'].append(root)
                
                # 分析路徑結構提取信息
                path_parts = Path(root).parts
                
                # 提取交易對名稱（通常在路徑中）
                for part in path_parts:
                    if any(currency in part for currency in ['USDT', 'BTC', 'ETH', 'USD']):
                        structure_info['symbols'].add(part)
                    
                    # 提取時間間隔
                    if part in ['1m', '5m', '15m', '30m', '1h', '4h', '1d']:
                        structure_info['intervals'].add(part)
                
                # 統計文件
                for pattern in file_patterns:
                    matching_files = glob.glob(os.path.join(root, pattern))
                    for file_path in matching_files:
                        structure_info['total_files'] += 1
                        file_ext = Path(file_path).suffix.lower()
                        structure_info['file_types'].add(file_ext)
        
        # 記錄統計信息
        self.bulk_logger.info(f"發現的交易對: {len(structure_info['symbols'])} 個")
        self.bulk_logger.info(f"交易對列表: {sorted(structure_info['symbols'])}")
        self.bulk_logger.info(f"時間間隔: {sorted(structure_info['intervals'])}")
        self.bulk_logger.info(f"文件格式: {sorted(structure_info['file_types'])}")
        self.bulk_logger.info(f"總文件數: {structure_info['total_files']}")
        self.bulk_logger.info(f"包含文件的目錄數: {len(structure_info['directories'])}")
        
        return structure_info
    
    def import_all_data(self, base_directory, trading_types=["um"], max_workers=4):
        """導入所有資料 - 增強版"""
        self.stats['start_time'] = datetime.now()
        
        try:
            self.bulk_logger.info("=== 開始批量導入所有資料 ===")
            self.bulk_logger.info(f"基礎目錄: {base_directory}")
            self.bulk_logger.info(f"交易類型: {trading_types}")
            self.bulk_logger.info(f"並行線程數: {max_workers}")
            
            # 先掃描目錄結構
            structure_info = self.scan_directory_structure(base_directory)
            self.stats['total_files'] = structure_info['total_files']
            
            # 收集所有需要處理的目錄
            all_directories = []
            
            for trading_type in trading_types:
                self.bulk_logger.info(f"=== 處理交易類型: {trading_type} ===")
                
                if trading_type == "spot":
                    data_path = os.path.join(base_directory, "data", "spot")
                else:
                    data_path = os.path.join(base_directory, "data", "futures", trading_type)
                
                if os.path.exists(data_path):
                    directories = self._collect_import_directories(data_path, trading_type)
                    all_directories.extend(directories)
                    self.bulk_logger.info(f"找到 {len(directories)} 個數據目錄")
                else:
                    self.bulk_logger.warning(f"路徑不存在: {data_path}")
            
            # 批量處理所有目錄
            if all_directories:
                self.bulk_logger.info(f"=== 開始並行處理 {len(all_directories)} 個目錄 ===")
                self._process_directories_parallel(all_directories, max_workers)
            
            # 記錄最終統計
            self._log_final_statistics()
            
        except Exception as e:
            self.bulk_logger.error(f"批量導入失敗: {e}")
            raise
        
        finally:
            self.stats['end_time'] = datetime.now()
    
    def _collect_import_directories(self, data_path, trading_type):
        """收集需要導入的目錄"""
        directories = []
        
        try:
            # 處理 daily 和 monthly 資料
            for time_period in ["daily", "monthly"]:
                period_path = os.path.join(data_path, time_period)
                if os.path.exists(period_path):
                    period_dirs = self._collect_period_directories(period_path, trading_type, time_period)
                    directories.extend(period_dirs)
        
        except Exception as e:
            self.bulk_logger.error(f"收集 {trading_type} 目錄失敗: {e}")
        
        return directories
    
    def _collect_period_directories(self, period_path, trading_type, time_period):
        """收集特定時期的目錄"""
        directories = []
        
        try:
            # 遍歷所有資料類型目錄
            for data_type_dir in os.listdir(period_path):
                data_type_path = os.path.join(period_path, data_type_dir)
                
                if os.path.isdir(data_type_path):
                    # 遞歸收集所有包含文件的子目錄
                    for root, dirs, files in os.walk(data_type_path):
                        if files:  # 只處理包含文件的目錄
                            # 檢查是否有支援的文件格式
                            supported_files = []
                            file_patterns = ["*.csv", "*.zip", "*.parquet", "*.gz", "*.feather", "*.h5"]
                            for pattern in file_patterns:
                                supported_files.extend(glob.glob(os.path.join(root, pattern)))
                            
                            if supported_files:
                                directories.append({
                                    'path': root,
                                    'trading_type': trading_type,
                                    'time_period': time_period,
                                    'data_type': data_type_dir,
                                    'file_count': len(supported_files)
                                })
        
        except Exception as e:
            self.bulk_logger.error(f"收集 {time_period} 目錄失敗: {e}")
        
        return directories
    
    def _process_directories_parallel(self, directories, max_workers):
        """並行處理目錄"""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_dir = {
                executor.submit(self._process_single_directory, dir_info): dir_info
                for dir_info in directories
            }
            
            for future in as_completed(future_to_dir):
                dir_info = future_to_dir[future]
                try:
                    result = future.result()
                    if result['success']:
                        self.stats['successful_files'] += result['successful_files']
                        self.stats['total_records'] += result['total_records']
                        self.stats['successful_symbols'].add(result['symbol'])
                    else:
                        self.stats['failed_files'] += result['failed_files']
                        self.stats['failed_files_list'].extend(result['failed_files_list'])
                    
                except Exception as e:
                    self.bulk_logger.error(f"處理目錄失敗 {dir_info['path']}: {e}")
                    self.stats['failed_files'] += dir_info['file_count']
    
    def _process_single_directory(self, dir_info):
        """處理單個目錄"""
        path = dir_info['path']
        trading_type = dir_info['trading_type']
        
        # 從路徑中提取交易對名稱
        path_parts = Path(path).parts
        symbol = None
        for part in path_parts:
            if any(currency in part for currency in ['USDT', 'BTC', 'ETH', 'USD']):
                symbol = part
                break
        
        result = {
            'success': True,
            'successful_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'symbol': symbol or 'UNKNOWN',
            'failed_files_list': []
        }
        
        try:
            self.bulk_logger.info(f"處理目錄: {path}")
            
            # 使用 DataImporter 的 import_directory 方法
            original_method = self.importer.import_directory
            
            # 暫時重寫方法來收集統計信息
            def enhanced_import_directory(directory_path, file_patterns=None, max_workers=1):
                try:
                    if file_patterns is None:
                        file_patterns = ["*.csv", "*.zip", "*.parquet", "*.gz", "*.feather", "*.h5"]
                    elif isinstance(file_patterns, str):
                        file_patterns = [file_patterns]
                    
                    # 查找所有匹配的文件
                    all_files = []
                    for pattern in file_patterns:
                        files = glob.glob(os.path.join(directory_path, "**", pattern), recursive=True)
                        all_files.extend(files)
                    
                    all_files = list(set(all_files))
                    
                    if not all_files:
                        return
                    
                    # 處理每個文件
                    for file_path in all_files:
                        try:
                            success = self.importer.import_single_file(file_path, trading_type)
                            if success:
                                result['successful_files'] += 1
                                # 這裡可以添加記錄數統計
                            else:
                                result['failed_files'] += 1
                                result['failed_files_list'].append(file_path)
                                
                        except Exception as file_error:
                            self.bulk_logger.error(f"文件處理失敗 {file_path}: {file_error}")
                            result['failed_files'] += 1
                            result['failed_files_list'].append(file_path)
                            result['success'] = False
                
                except Exception as e:
                    self.bulk_logger.error(f"目錄處理失敗 {directory_path}: {e}")
                    result['success'] = False
            
            # 執行增強的導入
            enhanced_import_directory(path)
            
        except Exception as e:
            self.bulk_logger.error(f"處理目錄失敗 {path}: {e}")
            result['success'] = False
        
        return result
    
    def _log_final_statistics(self):
        """記錄最終統計信息"""
        duration = self.stats['end_time'] - self.stats['start_time'] if self.stats['end_time'] else datetime.now() - self.stats['start_time']
        
        self.bulk_logger.info("=== 批量導入完成 - 最終統計 ===")
        self.bulk_logger.info(f"總處理時間: {duration}")
        self.bulk_logger.info(f"總文件數: {self.stats['total_files']}")
        self.bulk_logger.info(f"成功文件數: {self.stats['successful_files']}")
        self.bulk_logger.info(f"失敗文件數: {self.stats['failed_files']}")
        self.bulk_logger.info(f"成功率: {(self.stats['successful_files'] / max(self.stats['total_files'], 1) * 100):.2f}%")
        self.bulk_logger.info(f"總記錄數: {self.stats['total_records']}")
        self.bulk_logger.info(f"成功處理的交易對: {len(self.stats['successful_symbols'])} 個")
        self.bulk_logger.info(f"交易對列表: {sorted(self.stats['successful_symbols'])}")
        
        # 記錄失敗文件
        if self.stats['failed_files_list']:
            self.bulk_logger.error("=== 失敗文件列表 ===")
            for failed_file in self.stats['failed_files_list']:
                self.bulk_logger.error(f"失敗文件: {failed_file}")
        
        # 控制台顯示摘要
        print("\n" + "="*60)
        print("📊 批量導入完成摘要")
        print("="*60)
        print(f"⏱️  總處理時間: {duration}")
        print(f"📁 總文件數: {self.stats['total_files']}")
        print(f"✅ 成功文件數: {self.stats['successful_files']}")
        print(f"❌ 失敗文件數: {self.stats['failed_files']}")
        print(f"📈 成功率: {(self.stats['successful_files'] / max(self.stats['total_files'], 1) * 100):.2f}%")
        print(f"📄 日誌文件: {self.log_file}")
        
        if self.stats['failed_files_list']:
            print(f"\n❌ 失敗文件列表 ({len(self.stats['failed_files_list'])} 個):")
            for i, failed_file in enumerate(self.stats['failed_files_list'][:10]):  # 只顯示前10個
                print(f"   {i+1}. {failed_file}")
            if len(self.stats['failed_files_list']) > 10:
                print(f"   ... 及其他 {len(self.stats['failed_files_list']) - 10} 個文件")
            print(f"完整失敗列表請查看日誌文件: {self.log_file}")
        
        print("="*60)
