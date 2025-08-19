"""
Binance 資料導入器模組 (完整改進版)
負責將下載的 CSV/ZIP/Parquet 文件導入到 PostgreSQL 資料庫
支援首次導入和增量更新
支援多種檔案格式：CSV, ZIP, Parquet, GZ 等
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, date, timedelta
from decimal import Decimal
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import zipfile
import tempfile
from database_config import DatabaseManager, SymbolManager, SyncStatusManager

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=f"logs/{datetime.now().strftime('%Y%m%d %H%M')}_data_importer.log",
)
logger = logging.getLogger(__name__)


class DataImporter:
    """資料導入器主類"""

    def __init__(self, db_manager=None):
        self.db = db_manager or DatabaseManager()
        self.symbol_manager = SymbolManager(self.db)
        self.sync_manager = SyncStatusManager(self.db)
        self.external_logger = None  # 外部日誌記錄器

        # 資料類型映射
        self.data_type_mapping = {
            "klines": "klines",
            "trades": "trades",
            "aggTrades": "agg_trades",
            "indexPriceKlines": "index_price_klines",
            "markPriceKlines": "mark_price_klines",
            "premiumIndexKlines": "premium_index_klines",
            "bookDepth": "book_depth",
            "bookTicker": "book_ticker",
            "metrics": "trading_metrics",
            "fundingRate": "funding_rates",
        }

        # 列映射定義
        self.column_mappings = {
            "klines": {
                "open_time": "open_time",
                "open": "open_price",
                "high": "high_price",
                "low": "low_price",
                "close": "close_price",
                "volume": "volume",
                "close_time": "close_time",
                "quote_asset_volume": "quote_asset_volume",
                "number_of_trades": "number_of_trades",
                "taker_buy_base_asset_volume": "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume": "taker_buy_quote_asset_volume",
            },
            "trades": {
                "trade_id": "trade_id",
                "price": "price",
                "quantity": "quantity",
                "quote_quantity": "quote_quantity",
                "timestamp": "timestamp",
                "is_buyer_maker": "is_buyer_maker",
            },
            "agg_trades": {
                "agg_trade_id": "agg_trade_id",
                "price": "price",
                "quantity": "quantity",
                "first_trade_id": "first_trade_id",
                "last_trade_id": "last_trade_id",
                "timestamp": "timestamp",
                "is_buyer_maker": "is_buyer_maker",
            },
        }

    def set_external_logger(self, external_logger):
        """設置外部日誌記錄器"""
        self.external_logger = external_logger
    
    def _log(self, message, level="info"):
        """統一的日誌記錄方法"""
        # 記錄到內部日誌
        if level == "info":
            logger.info(message)
        elif level == "error":
            logger.error(message)
        elif level == "warning":
            logger.warning(message)
        
        # 記錄到外部日誌
        if self.external_logger:
            if level == "info":
                self.external_logger.logger.info(message)
            elif level == "error":
                self.external_logger.logger.error(message)
            elif level == "warning":
                self.external_logger.logger.warning(message)

    def parse_filename(self, filename, file_path=None):
        """解析文件名獲取資訊，同時從路徑中推斷資料類型"""
        try:
            # 移除文件擴展名
            name_parts = Path(filename).stem.split("-")

            # 從路徑中推斷資料類型
            data_type = "klines"  # 預設值
            if file_path:
                path_parts = Path(file_path).parts
                for part in path_parts:
                    if part in [
                        "klines",
                        "trades",
                        "aggTrades",
                        "bookDepth",
                        "bookTicker",
                        "metrics",
                        "fundingRate",
                    ]:
                        data_type = part
                        break
                    elif "indexPrice" in part:
                        data_type = "indexPriceKlines"
                        break
                    elif "markPrice" in part:
                        data_type = "markPriceKlines"
                        break
                    elif "premiumIndex" in part:
                        data_type = "premiumIndexKlines"
                        break

            if len(name_parts) >= 3:
                symbol = name_parts[0]

                # 檢查文件名格式
                # 格式1: SYMBOL-INTERVAL-YYYY-MM-DD (e.g., ADAUSDT-1m-2024-01-01)
                # 格式2: SYMBOL-DATATYPE-INTERVAL-YYYY-MM-DD (e.g., ADAUSDT-klines-1m-2024-01-01)
                # 格式3: SYMBOL-DATATYPE-YYYY-MM-DD (e.g., ADAUSDT-trades-2024-01-01)

                if len(name_parts) == 5:  # SYMBOL-INTERVAL-YYYY-MM-DD
                    interval = name_parts[1]
                    year = name_parts[2]
                    month = name_parts[3]
                    day = name_parts[4]
                elif len(name_parts) == 6:  # SYMBOL-DATATYPE-INTERVAL-YYYY-MM-DD
                    if name_parts[1] in [
                        "klines",
                        "trades",
                        "aggTrades",
                        "bookDepth",
                        "bookTicker",
                        "metrics",
                        "fundingRate",
                    ]:
                        data_type = name_parts[1]
                        interval = name_parts[2] if data_type == "klines" else None
                        year = name_parts[3]
                        month = name_parts[4]
                        day = name_parts[5]
                    else:
                        # 可能是 SYMBOL-INTERVAL-YYYY-MM-DD-EXTRA
                        interval = name_parts[1]
                        year = name_parts[2]
                        month = name_parts[3]
                        day = name_parts[4]
                elif (
                    len(name_parts) == 4
                ):  # SYMBOL-DATATYPE-YYYY-MM-DD or SYMBOL-YYYY-MM-DD
                    if name_parts[1] in [
                        "trades",
                        "aggTrades",
                        "bookDepth",
                        "bookTicker",
                        "metrics",
                        "fundingRate",
                    ]:
                        data_type = name_parts[1]
                        interval = None
                        year = name_parts[2]
                        month = (
                            name_parts[3][:2]
                            if len(name_parts[3]) > 2
                            else name_parts[3]
                        )
                        day = name_parts[3][2:] if len(name_parts[3]) > 2 else "01"
                    else:
                        # SYMBOL-YYYY-MM-DD
                        interval = None
                        year = name_parts[1]
                        month = name_parts[2]
                        day = name_parts[3]
                else:
                    # 回退到原始解析邏輯
                    interval = name_parts[1] if len(name_parts) > 1 else None
                    year = (
                        name_parts[-2]
                        if len(name_parts) >= 2
                        else str(datetime.now().year)
                    )
                    month_or_date = name_parts[-1] if len(name_parts) >= 1 else "0101"

                    if len(month_or_date) == 2:
                        month = month_or_date
                        day = "01"
                    elif len(month_or_date) == 4:
                        month = month_or_date[:2]
                        day = month_or_date[2:]
                    else:
                        month = "01"
                        day = "01"

                # 構建日期
                try:
                    file_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    # 驗證日期格式
                    datetime.strptime(file_date, "%Y-%m-%d")
                except:
                    file_date = datetime.now().strftime("%Y-%m-%d")

                # 決定時間周期
                if len(month) == 2 and day == "01":
                    time_period = "monthly"
                else:
                    time_period = "daily"

                logger.info(
                    f"解析文件: {filename} -> 符號:{symbol}, 類型:{data_type}, 間隔:{interval}, 日期:{file_date}"
                )

                return {
                    "symbol": symbol,
                    "data_type": data_type,
                    "interval": interval,
                    "time_period": time_period,
                    "date": file_date,
                }

            logger.error(f"文件名格式不正確: {filename}")
            return None

        except Exception as e:
            logger.error(f"解析文件名失敗 {filename}: {e}")
            return None

    def read_data_file(self, file_path):
        """讀取資料文件 - 支援多種格式"""
        try:
            file_ext = Path(file_path).suffix.lower()
            logger.info(f"正在讀取文件: {file_path} (格式: {file_ext})")

            if file_ext == ".csv":
                df = pd.read_csv(file_path)
            elif file_ext == ".zip":
                # 處理 ZIP 檔案
                df = self._read_zip_file(file_path)
                if df is None:
                    return None
            elif file_ext == ".parquet":
                df = pd.read_parquet(file_path)
            elif file_ext == ".feather":
                df = pd.read_feather(file_path)
            elif file_ext == ".h5":
                df = pd.read_hdf(file_path, key="data")
            elif file_ext == ".gz":
                # 處理 gzip 壓縮的 CSV 檔案
                df = pd.read_csv(file_path, compression="gzip")
            else:
                logger.error(f"不支援的文件格式: {file_ext}")
                return None

            logger.info(f"成功讀取 {len(df)} 行資料")

            # 處理空值和無限值
            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.dropna()

            logger.info(f"清理後剩餘 {len(df)} 行有效資料")
            return df

        except Exception as e:
            logger.error(f"讀取文件失敗 {file_path}: {e}")
            return None

    def _read_zip_file(self, zip_path):
        """讀取 ZIP 檔案中的 CSV 資料"""
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # 獲取 ZIP 檔案中的所有檔案
                file_list = zip_ref.namelist()
                logger.info(f"ZIP 檔案包含: {file_list}")

                # 尋找 CSV 檔案
                csv_files = [f for f in file_list if f.endswith(".csv")]

                if not csv_files:
                    logger.error(f"ZIP 檔案中沒有找到 CSV 檔案: {zip_path}")
                    return None

                # 讀取第一個 CSV 檔案
                csv_file = csv_files[0]
                logger.info(f"從 ZIP 檔案讀取: {csv_file}")

                with zip_ref.open(csv_file) as csv_data:
                    df = pd.read_csv(csv_data)
                    return df

        except Exception as e:
            logger.error(f"讀取 ZIP 檔案失敗 {zip_path}: {e}")
            return None

    def prepare_klines_data(self, df, symbol_id, trading_type, interval_type):
        """準備 K線資料"""
        try:
            # 檢查列是否存在
            expected_columns = [
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
            ]

            # 如果沒有列名，使用數字索引
            if df.columns.dtype == "int64" or len(df.columns) == 12:
                df.columns = expected_columns + ["ignore"]

            # 重命名列
            column_mapping = self.column_mappings["klines"]
            df_renamed = df.rename(columns=column_mapping)

            # 添加必要的列
            df_renamed["symbol_id"] = symbol_id
            df_renamed["trading_type"] = trading_type
            df_renamed["interval_type"] = interval_type
            df_renamed["data_source"] = "binance"

            # 選擇需要的列
            required_columns = [
                "symbol_id",
                "trading_type",
                "interval_type",
                "open_time",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "data_source",
            ]

            return df_renamed[required_columns]

        except Exception as e:
            logger.error(f"準備 K線資料失敗: {e}")
            logger.error(f"DataFrame 列: {df.columns.tolist()}")
            return None

    def import_single_file(self, file_path, trading_type=None):
        """導入單個文件"""
        try:
            self._log(f"開始導入文件: {file_path}")

            # 解析文件名
            file_info = self.parse_filename(os.path.basename(file_path), file_path)
            if not file_info:
                self._log(f"無法解析文件名: {file_path}", "error")
                return False

            # 確定交易類型
            if not trading_type:
                if "USDT" in file_info["symbol"]:
                    trading_type = (
                        "um"
                        if any(x in file_path for x in ["futures", "um"])
                        else "spot"
                    )
                elif "USD" in file_info["symbol"]:
                    trading_type = "cm"
                else:
                    trading_type = "spot"

            # 獲取或創建交易對
            symbol_id = self.symbol_manager.get_symbol_id(file_info["symbol"])
            if not symbol_id:
                # 解析交易對
                symbol = file_info["symbol"]
                if trading_type == "cm":
                    base_asset = symbol.replace("USD", "")
                    quote_asset = "USD"
                elif "USDT" in symbol:
                    base_asset = symbol.replace("USDT", "")
                    quote_asset = "USDT"
                else:
                    # 簡單解析，可能需要更複雜的邏輯
                    base_asset = symbol[:-3] if len(symbol) > 3 else symbol
                    quote_asset = symbol[-3:] if len(symbol) > 3 else "USDT"

                symbol_id = self.symbol_manager.add_symbol(
                    symbol, base_asset, quote_asset, trading_type
                )

            # 讀取資料
            df = self.read_data_file(file_path)
            if df is None or df.empty:
                self._log(f"文件為空或讀取失敗: {file_path}", "warning")
                return False

            # 根據資料類型準備資料
            data_type = file_info["data_type"]
            table_name = self.data_type_mapping.get(data_type)

            if not table_name:
                self._log(f"不支援的資料類型: {data_type}", "error")
                return False

            # 準備資料
            if data_type == "klines":
                prepared_df = self.prepare_klines_data(
                    df, symbol_id, trading_type, file_info["interval"]
                )
            else:
                self._log(f"暫不支援資料類型 {data_type} 的詳細處理", "warning")
                return False

            if prepared_df is None or prepared_df.empty:
                self._log(f"資料準備失敗: {file_path}", "warning")
                return False

            # 插入資料
            records_count = self.batch_insert_data(prepared_df, table_name)

            if records_count > 0:
                self._log(f"成功導入 {file_path}: {records_count} 條記錄")
                return True
            else:
                self._log(f"導入失敗: {file_path}", "error")
                return False

        except Exception as e:
            self._log(f"導入文件失敗 {file_path}: {e}", "error")
            return False

    def batch_insert_data(self, df, table_name, batch_size=1000):
        """批量插入資料 - 自動創建必要的分區"""
        try:
            records_inserted = 0
            total_records = len(df)

            # 如果是 klines 表，先自動創建所需的分區
            if table_name == "klines" and "open_time" in df.columns:
                logger.info("正在檢查和創建必要的分區...")
                if not self.db.auto_create_partitions_for_data(df, "open_time"):
                    logger.warning("部分分區創建失敗，但將繼續嘗試插入")

            # 將 DataFrame 轉換為記錄列表
            records = df.to_dict("records")

            with self.db.get_connection() as conn:
                with self.db.get_cursor(conn, dict_cursor=False) as cursor:
                    for i in range(0, total_records, batch_size):
                        batch = records[i : i + batch_size]

                        # 構建插入查詢
                        if batch:
                            columns = list(batch[0].keys())
                            placeholders = ", ".join(["%s"] * len(columns))
                            column_names = ", ".join(columns)

                            query = f"""
                            INSERT INTO {table_name} ({column_names})
                            VALUES ({placeholders})
                            ON CONFLICT DO NOTHING;
                            """

                            # 準備資料
                            values = [
                                [record[col] for col in columns] for record in batch
                            ]

                            try:
                                # 執行批量插入
                                psycopg2.extras.execute_batch(
                                    cursor, query, values, page_size=batch_size
                                )
                                records_inserted += len(batch)

                                if records_inserted % (batch_size * 10) == 0:
                                    logger.info(
                                        f"已插入 {records_inserted}/{total_records} 記錄"
                                    )

                            except Exception as batch_error:
                                # 如果是分區問題，嘗試針對這個批次創建分區
                                if (
                                    "no partition of relation" in str(batch_error)
                                    and table_name == "klines"
                                ):
                                    logger.warning(
                                        f"找不到分區，嘗試為當前批次創庺分區..."
                                    )

                                    # 為當前批次的數據創庺分區
                                    batch_df = pd.DataFrame(batch)
                                    if self.db.auto_create_partitions_for_data(
                                        batch_df, "open_time"
                                    ):
                                        # 重新嘗試插入
                                        try:
                                            psycopg2.extras.execute_batch(
                                                cursor,
                                                query,
                                                values,
                                                page_size=batch_size,
                                            )
                                            records_inserted += len(batch)
                                            logger.info(
                                                f"創庺分區後成功插入 {len(batch)} 條記錄"
                                            )
                                        except Exception as retry_error:
                                            logger.error(
                                                f"重試插入仍然失敗: {retry_error}"
                                            )
                                            raise retry_error
                                    else:
                                        logger.error(f"創庺分區失敗: {batch_error}")
                                        raise batch_error
                                else:
                                    raise batch_error

                    conn.commit()
                    logger.info(f"成功插入 {records_inserted} 條記錄到 {table_name}")
                    return records_inserted

        except Exception as e:
            logger.error(f"批量插入失敗: {e}")
            return 0

    def import_directory(self, directory_path, file_patterns=None, max_workers=4):
        """批量導入目錄中的文件 - 支援多種檔案格式並集成外部日誌"""
        successful_imports = 0
        failed_imports = 0
        failed_files = []
        
        try:
            if file_patterns is None:
                file_patterns = [
                    "*.csv",
                    "*.zip", 
                    "*.parquet",
                    "*.gz",
                    "*.feather",
                    "*.h5",
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
                files = glob.glob(
                    os.path.join(directory_path, "**", pattern), recursive=True
                )
                all_files.extend(files)
                
                # 記錄文件類型
                for file in files:
                    file_types.add(Path(file).suffix.lower())
                
                self._log(f"模式 {pattern} 找到 {len(files)} 個文件")

            # 去除重複的文件
            all_files = list(set(all_files))

            if not all_files:
                self._log(f"在目錄 {directory_path} 中沒有找到匹配的文件", "warning")
                # 列出目錄中的所有文件來協助除錯
                self._list_directory_contents(directory_path)
                
                # 更新外部日誌
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
                            
                            # 記錄到外部日誌
                            if self.external_logger:
                                self.external_logger.log_file_processing(file_path, success=True)
                        else:
                            failed_imports += 1
                            failed_files.append(file_path)
                            self._log(f"❌ 導入失敗: {os.path.basename(file_path)}", "error")
                            
                            # 記錄到外部日誌
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
                        
                        # 記錄到外部日誌
                        if self.external_logger:
                            self.external_logger.log_file_processing(file_path, success=False, error_msg=error_msg)

            # 最終統計
            self._log(f"批量導入完成: 成功 {successful_imports}, 失敗 {failed_imports}")
            
            if failed_files:
                self._log("失敗的文件列表:", "error")
                for i, failed_file in enumerate(failed_files[:10], 1):  # 只顯示前10個
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

    def _list_directory_contents(self, directory_path, max_files=20):
        """列出目錄內容來協助除錯"""
        try:
            logger.info(f"目錄 {directory_path} 的內容:")

            if not os.path.exists(directory_path):
                logger.error(f"目錄不存在: {directory_path}")
                return

            file_count = 0
            for root, dirs, files in os.walk(directory_path):
                logger.info(f"目錄: {root}")
                for file in files:
                    if file_count >= max_files:
                        logger.info(f"...及更多文件 (只顯示前 {max_files} 個)")
                        return

                    file_path = os.path.join(root, file)
                    file_ext = Path(file).suffix.lower()
                    file_size = os.path.getsize(file_path)

                    logger.info(f"  {file} ({file_ext}, {file_size} bytes)")
                    file_count += 1

            if file_count == 0:
                logger.info(f"  目錄為空")

        except Exception as e:
            logger.error(f"無法列出目錄內容: {e}")

    def incremental_update(
        self, symbol, data_type, trading_type, interval=None, days_back=7
    ):
        """增量更新：檢查並導入最近的資料"""
        try:
            symbol_id = self.symbol_manager.get_symbol_id(symbol)
            if not symbol_id:
                logger.error(f"找不到交易對: {symbol}")
                return False

            # 獲取最後同步日期
            last_sync_date = self.sync_manager.get_last_sync_date(
                symbol_id, data_type, "daily", interval
            )

            if last_sync_date:
                start_date = last_sync_date + timedelta(days=1)
            else:
                start_date = date.today() - timedelta(days=days_back)

            end_date = date.today()

            logger.info(f"增量更新 {symbol} {data_type} 從 {start_date} 到 {end_date}")

            # 構建文件路徑模式
            store_directory = os.getenv("STORE_DIRECTORY")
            if not store_directory:
                logger.error("未設置 STORE_DIRECTORY 環境變數")
                return False

            # 根據交易類型構建路徑
            if trading_type == "spot":
                base_path = os.path.join(
                    store_directory, "data", "spot", "daily", data_type, symbol
                )
            else:
                base_path = os.path.join(
                    store_directory,
                    "data",
                    "futures",
                    trading_type,
                    "daily",
                    data_type,
                    symbol,
                )

            if interval and data_type in [
                "klines",
                "indexPriceKlines",
                "markPriceKlines",
                "premiumIndexKlines",
            ]:
                base_path = os.path.join(base_path, interval)

            # 查找需要更新的文件
            current_date = start_date
            files_to_import = []

            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                file_pattern = f"{symbol}-*-{date_str}.*"
                matching_files = glob.glob(os.path.join(base_path, file_pattern))
                files_to_import.extend(matching_files)
                current_date += timedelta(days=1)

            if files_to_import:
                logger.info(f"找到 {len(files_to_import)} 個文件需要增量更新")
                for file_path in files_to_import:
                    self.import_single_file(file_path, trading_type)
                return True
            else:
                logger.info(f"沒有找到需要更新的文件")
                return False

        except Exception as e:
            logger.error(f"增量更新失敗: {e}")
            return False


class BulkImportManager:
    """批量導入管理器"""

    def __init__(self, db_manager=None):
        self.db = db_manager or DatabaseManager()
        self.importer = DataImporter(self.db)
        self.symbol_manager = SymbolManager(self.db)

    def import_all_data(self, base_directory, trading_types=["spot", "um", "cm"]):
        """導入所有資料"""
        try:
            logger.info("開始批量導入所有資料")

            for trading_type in trading_types:
                logger.info(f"處理交易類型: {trading_type}")

                if trading_type == "spot":
                    data_path = os.path.join(base_directory, "data", "spot")
                else:
                    data_path = os.path.join(
                        base_directory, "data", "futures", trading_type
                    )

                if os.path.exists(data_path):
                    self._import_trading_type_data(data_path, trading_type)
                else:
                    logger.warning(f"路徑不存在: {data_path}")

            logger.info("批量導入完成")

        except Exception as e:
            logger.error(f"批量導入失敗: {e}")

    def _import_trading_type_data(self, data_path, trading_type):
        """導入特定交易類型的資料"""
        try:
            # 處理 daily 和 monthly 資料
            for time_period in ["daily", "monthly"]:
                period_path = os.path.join(data_path, time_period)
                if os.path.exists(period_path):
                    self._import_period_data(period_path, trading_type, time_period)

        except Exception as e:
            logger.error(f"導入 {trading_type} 資料失敗: {e}")

    def _import_period_data(self, period_path, trading_type, time_period):
        """導入特定時期的資料"""
        try:
            # 遍歷所有資料類型目錄
            for data_type_dir in os.listdir(period_path):
                data_type_path = os.path.join(period_path, data_type_dir)

                if os.path.isdir(data_type_path):
                    logger.info(f"處理 {trading_type} {time_period} {data_type_dir}")

                    # 支援的檔案格式
                    file_patterns = [
                        "*.csv",
                        "*.zip",
                        "*.parquet",
                        "*.gz",
                        "*.feather",
                        "*.h5",
                    ]

                    self.importer.import_directory(
                        data_type_path, file_patterns, max_workers=2
                    )

        except Exception as e:
            logger.error(f"導入 {time_period} 資料失敗: {e}")

    def setup_initial_symbols(self):
        """設置初始交易對"""
        try:
            # 從 Binance API 獲取所有交易對
            import sys

            sys.path.append("../python_download_data")
            from python_download_data.utility import get_all_symbols

            for trading_type in ["spot", "um", "cm"]:
                logger.info(f"獲取 {trading_type} 交易對")
                symbols = get_all_symbols(trading_type)

                symbols_data = []
                for symbol in symbols:
                    if trading_type == "cm":
                        base_asset = symbol.replace("USD", "")
                        quote_asset = "USD"
                    elif "USDT" in symbol:
                        base_asset = symbol.replace("USDT", "")
                        quote_asset = "USDT"
                    elif "BTC" in symbol and symbol.endswith("BTC"):
                        base_asset = symbol.replace("BTC", "")
                        quote_asset = "BTC"
                    elif "ETH" in symbol and symbol.endswith("ETH"):
                        base_asset = symbol.replace("ETH", "")
                        quote_asset = "ETH"
                    else:
                        # 預設處理
                        base_asset = symbol[:-3] if len(symbol) > 3 else symbol
                        quote_asset = symbol[-3:] if len(symbol) > 3 else "USDT"

                    symbols_data.append(
                        (symbol, base_asset, quote_asset, trading_type, "TRADING")
                    )

                if symbols_data:
                    self.symbol_manager.batch_add_symbols(symbols_data)
                    logger.info(
                        f"批量添加了 {len(symbols_data)} 個 {trading_type} 交易對"
                    )

        except Exception as e:
            logger.error(f"設置初始交易對失敗: {e}")


# 使用示例
if __name__ == "__main__":
    # 創建資料庫管理器
    db_manager = DatabaseManager()

    try:
        # 測試導入器
        importer = DataImporter(db_manager)

        # 示例：導入單個文件
        # importer.import_single_file("/path/to/BTCUSDT-1h-2024-01-01.csv", "spot")

        # 示例：批量導入
        # importer.import_directory("/path/to/binance/data")

        print("資料導入器初始化成功")

    except Exception as e:
        print(f"錯誤: {e}")

    finally:
        db_manager.close_pool()
