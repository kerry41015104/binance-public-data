import os
import glob
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

def import_directory(self, directory_path, file_patterns=None, max_workers=4):
    """批量導入目錄中的文件 - 支援多種檔案格式並集成外部日誌"""
    successful_imports = 0
    failed_imports = 0
    failed_files = []
    
    try:
        if file_patterns is None:
            file_patterns = [
                "*.csv", "*.zip", "*.parquet", "*.gz", "*.feather", "*.h5"
            ]
        elif isinstance(file_patterns, str):
            file_patterns = [file_patterns]

        self._log(f"正在掃描目錄: {directory_path}")
        self._log(f"文件模式: {file_patterns}")
        
        # 記錄到外部日誌
        if self.external_logger:
            self.external_logger.log_directory_scan(directory_path)

        # 查找所有匹配的文件
        all_files = []
        file_types = set()
        
        for pattern in file_patterns:
            files = glob.glob(os.path.join(directory_path, "**", pattern), recursive=True)
            all_files.extend(files)
            
            # 記錄文件類型
            for file in files:
                file_types.add(Path(file).suffix.lower())
            
            self._log(f"模式 {pattern} 找到 {len(files)} 個文件")

        # 去除重複的文件
        all_files = list(set(all_files))

        if not all_files:
            self._log(f"在目錄 {directory_path} 中沒有找到匹配的文件", "warning")
            self._list_directory_contents(directory_path)
            
            if self.external_logger:
                self.external_logger.log_directory_scan(directory_path, 0, file_types)
            return {'successful_imports': 0, 'failed_imports': 0, 'failed_files': []}

        self._log(f"總共找到 {len(all_files)} 個文件待導入")
        
        # 更新外部日誌
        if self.external_logger:
            self.external_logger.log_directory_scan(directory_path, len(all_files), file_types)

        # 使用線程池並行處理
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(self.import_single_file, file_path): file_path
                for file_path in all_files
            }

            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    success = future.result()
                    if success:
                        successful_imports += 1
                        self._log(f"✅ 成功導入: {os.path.basename(file_path)}")
                        
                        if self.external_logger:
                            self.external_logger.log_file_processing(file_path, success=True)
                    else:
                        failed_imports += 1
                        failed_files.append(file_path)
                        self._log(f"❌ 導入失敗: {os.path.basename(file_path)}", "error")
                        
                        if self.external_logger:
                            self.external_logger.log_file_processing(file_path, success=False)
                            
                    # 每處理10個文件輸出一次進度
                    total_processed = successful_imports + failed_imports
                    if total_processed % 10 == 0:
                        self._log(f"進度: {total_processed}/{len(all_files)} 文件已處理")
                        
                except Exception as e:
                    failed_imports += 1
                    failed_files.append(file_path)
                    error_msg = str(e)
                    self._log(f"處理文件失敗 {file_path}: {error_msg}", "error")
                    
                    if self.external_logger:
                        self.external_logger.log_file_processing(file_path, success=False, error_msg=error_msg)

        # 最終統計
        self._log(f"批量導入完成: 成功 {successful_imports}, 失敗 {failed_imports}")
        
        if failed_files:
            self._log("失敗的文件列表:", "error")
            for i, failed_file in enumerate(failed_files[:10], 1):
                self._log(f"  {i}. {os.path.basename(failed_file)}", "error")
            if len(failed_files) > 10:
                self._log(f"  ... 及其他 {len(failed_files) - 10} 個文件", "error")

    except Exception as e:
        self._log(f"批量導入失敗: {e}", "error")
        
    return {
        'successful_imports': successful_imports,
        'failed_imports': failed_imports,
        'failed_files': failed_files
    }