"""
通用日誌管理器
為所有 import_data.py 的動作提供統一的日誌功能
"""

import os
import logging
from datetime import datetime
from pathlib import Path

class UniversalLogger:
    """通用日誌管理器 - 為所有動作提供日誌功能"""
    
    def __init__(self, action_name):
        self.action_name = action_name
        self.stats = {
            'start_time': datetime.now(),
            'end_time': None,
            'total_files': 0,
            'successful_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'failed_files_list': [],
            'processed_symbols': set(),
            'processed_directories': []
        }
        
        # 設置日誌
        self.setup_logging()
    
    def setup_logging(self):
        """設置日誌記錄"""
        # 創建日誌文件名：{action_name}_{今天日期}.log
        today = datetime.now().strftime('%Y%m%d')
        log_filename = f"{self.action_name}_{today}.log"
        log_path = os.path.join(os.path.dirname(__file__), 'logs', log_filename)
        
        # 確保日誌目錄存在
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        
        # 創建專用日誌記錄器
        self.logger = logging.getLogger(f'import_data_{self.action_name}')
        self.logger.setLevel(logging.INFO)
        
        # 避免重複處理器
        if not self.logger.handlers:
            # 文件處理器
            file_handler = logging.FileHandler(log_path, encoding='utf-8')
            file_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
            
            # 控制台處理器（可選，避免重複輸出）
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            # 只對某些動作添加控制台輸出
            if self.action_name in ['bulk_import', 'test_dir']:
                self.logger.addHandler(console_handler)
        
        self.log_file = log_path
        
        # 記錄開始信息
        self.logger.info(f"=== {self.action_name.replace('_', '-').upper()} 任務開始 ===")
        self.logger.info(f"日誌文件: {log_path}")
        self.logger.info(f"開始時間: {self.stats['start_time']}")
    
    def log_directory_scan(self, directory, file_count=0, file_types=None):
        """記錄目錄掃描信息"""
        self.logger.info(f"掃描目錄: {directory}")
        if file_count > 0:
            self.logger.info(f"發現文件數: {file_count}")
            self.stats['total_files'] += file_count
        
        if file_types:
            self.logger.info(f"文件格式: {sorted(file_types)}")
        
        self.stats['processed_directories'].append(directory)
    
    def log_file_processing(self, file_path, success=True, records=0, error_msg=None):
        """記錄文件處理結果"""
        if success:
            self.logger.info(f"✅ 文件處理成功: {file_path} ({records} 條記錄)")
            self.stats['successful_files'] += 1
            self.stats['total_records'] += records
            
            # 提取交易對名稱
            file_name = os.path.basename(file_path)
            if '-' in file_name:
                symbol = file_name.split('-')[0]
                self.stats['processed_symbols'].add(symbol)
        else:
            self.logger.error(f"❌ 文件處理失敗: {file_path}")
            if error_msg:
                self.logger.error(f"   錯誤詳情: {error_msg}")
            self.stats['failed_files'] += 1
            self.stats['failed_files_list'].append(file_path)
    
    def log_partition_creation(self, partition_name, success=True, error_msg=None):
        """記錄分區創建"""
        if success:
            self.logger.info(f"🏗️  分區創建成功: {partition_name}")
        else:
            self.logger.error(f"❌ 分區創建失敗: {partition_name}")
            if error_msg:
                self.logger.error(f"   錯誤詳情: {error_msg}")
    
    def log_symbol_creation(self, symbol, success=True, error_msg=None):
        """記錄交易對創建"""
        if success:
            self.logger.info(f"📊 交易對創建/更新成功: {symbol}")
            self.stats['processed_symbols'].add(symbol)
        else:
            self.logger.error(f"❌ 交易對創建失敗: {symbol}")
            if error_msg:
                self.logger.error(f"   錯誤詳情: {error_msg}")
    
    def log_incremental_update(self, symbol, data_type, start_date, end_date, files_found=0):
        """記錄增量更新信息"""
        self.logger.info(f"🔄 增量更新: {symbol} {data_type}")
        self.logger.info(f"   時間範圍: {start_date} 到 {end_date}")
        self.logger.info(f"   找到文件: {files_found} 個")
    
    def finalize_log(self):
        """完成日誌記錄並生成摘要"""
        self.stats['end_time'] = datetime.now()
        duration = self.stats['end_time'] - self.stats['start_time']
        
        # 記錄最終統計
        self.logger.info(f"=== {self.action_name.replace('_', '-').upper()} 任務完成 ===")
        self.logger.info(f"結束時間: {self.stats['end_time']}")
        self.logger.info(f"總處理時間: {duration}")
        self.logger.info(f"處理的目錄數: {len(self.stats['processed_directories'])}")
        self.logger.info(f"總文件數: {self.stats['total_files']}")
        self.logger.info(f"成功文件數: {self.stats['successful_files']}")
        self.logger.info(f"失敗文件數: {self.stats['failed_files']}")
        
        if self.stats['total_files'] > 0:
            success_rate = (self.stats['successful_files'] / self.stats['total_files']) * 100
            self.logger.info(f"成功率: {success_rate:.2f}%")
        
        self.logger.info(f"總記錄數: {self.stats['total_records']}")
        self.logger.info(f"處理的交易對: {len(self.stats['processed_symbols'])} 個")
        
        if self.stats['processed_symbols']:
            self.logger.info(f"交易對列表: {sorted(self.stats['processed_symbols'])}")
        
        # 記錄失敗文件
        if self.stats['failed_files_list']:
            self.logger.error("=== 失敗文件列表 ===")
            for i, failed_file in enumerate(self.stats['failed_files_list'], 1):
                self.logger.error(f"{i:3d}. {failed_file}")
        
        # 控制台摘要（對所有動作）
        self._print_console_summary(duration)
        
        return self.stats
    
    def _print_console_summary(self, duration):
        """在控制台打印摘要"""
        action_display = self.action_name.replace('_', '-').upper()
        
        print("\n" + "="*60)
        print(f"📊 {action_display} 完成摘要")
        print("="*60)
        print(f"⏱️  總處理時間: {duration}")
        print(f"📁 處理目錄數: {len(self.stats['processed_directories'])}")
        print(f"📄 總文件數: {self.stats['total_files']}")
        print(f"✅ 成功文件數: {self.stats['successful_files']}")
        print(f"❌ 失敗文件數: {self.stats['failed_files']}")
        
        if self.stats['total_files'] > 0:
            success_rate = (self.stats['successful_files'] / self.stats['total_files']) * 100
            print(f"📈 成功率: {success_rate:.2f}%")
        
        print(f"📊 總記錄數: {self.stats['total_records']}")
        print(f"🏷️  處理交易對: {len(self.stats['processed_symbols'])} 個")
        print(f"📄 日誌文件: {self.log_file}")
        
        if self.stats['failed_files_list']:
            print(f"\n❌ 失敗文件列表 ({len(self.stats['failed_files_list'])} 個):")
            for i, failed_file in enumerate(self.stats['failed_files_list'][:5]):  # 只顯示前5個
                print(f"   {i+1}. {os.path.basename(failed_file)}")
            if len(self.stats['failed_files_list']) > 5:
                print(f"   ... 及其他 {len(self.stats['failed_files_list']) - 5} 個文件")
            print(f"   完整失敗列表請查看: {self.log_file}")
        
        print("="*60)
    
    def get_log_file_path(self):
        """獲取日誌文件路徑"""
        return self.log_file
    
    def get_stats(self):
        """獲取統計信息"""
        return self.stats.copy()

# 便捷函數
def create_logger(action_name):
    """創建通用日誌記錄器"""
    return UniversalLogger(action_name)

# 為了兼容性，提供舊的接口
def setup_action_logging(action_name):
    """設置動作日誌（兼容性函數）"""
    return create_logger(action_name)
