"""
Binance 資料導入器模組 (增強版)
支援所有 Binance 資料類型的完整導入
包括 klines, trades, aggTrades, indexPriceKlines, markPriceKlines,
premiumIndexKlines, bookDepth, bookTicker, metrics, fundingRate 等
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
    """增強版資料導入器主類 - 支援所有資料類型"""

    def __init__(self, db_manager=None):
        self.db = db_manager or DatabaseManager()
        self.symbol_manager = SymbolManager(self.db)
        self.sync_manager = SyncStatusManager(self.db)
        self.external_logger = None  # 外部日誌記錄器

        # 完整的資料類型映射
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
            "BVOLIndex": "bvol_index",
        }

        # 完整的列映射定義
        self.column_mappings = {
            # K線類數據 (所有 K線 類型使用相同結構)
            "klines": {
                0: "open_time",
                1: "open_price",
                2: "high_price",
                3: "low_price",
                4: "close_price",
                5: "volume",
                6: "close_time",
                7: "quote_asset_volume",
                8: "number_of_trades",
                9: "taker_buy_base_asset_volume",
                10: "taker_buy_quote_asset_volume",
            },
            "index_price_klines": {
                0: "open_time",
                1: "open_price",
                2: "high_price",
                3: "low_price",
                4: "close_price",
                5: "ignore",  # 第6列通常是忽略的
            },
            "mark_price_klines": {
                0: "open_time",
                1: "open_price",
                2: "high_price",
                3: "low_price",
                4: "close_price",
                5: "ignore",
            },
            "premium_index_klines": {
                0: "open_time",
                1: "open_price",
                2: "high_price",
                3: "low_price",
                4: "close_price",
                5: "ignore",
            },
            # 交易數據
            "trades": {
                0: "trade_id",
                1: "price",
                2: "quantity",
                3: "quote_quantity",
                4: "timestamp",
                5: "is_buyer_maker",
            },
            "agg_trades": {
                0: "agg_trade_id",
                1: "price",
                2: "quantity",
                3: "first_trade_id",
                4: "last_trade_id",
                5: "timestamp",
                6: "is_buyer_maker",
            },
            # 期貨專用數據
            "book_depth": {0: "timestamp", 1: "percentage", 2: "depth", 3: "notional"},
            "book_ticker": {
                0: "update_id",
                1: "best_bid_price",
                2: "best_bid_qty",
                3: "best_ask_price",
                4: "best_ask_qty",
                5: "transaction_time",
                6: "event_time",
            },
            "trading_metrics": {
                0: "create_time",
                1: "sum_open_interest",
                2: "sum_open_interest_value",
                3: "count_toptrader_long_short_ratio",
                4: "sum_toptrader_long_short_ratio",
                5: "count_long_short_ratio",
                6: "sum_taker_long_short_vol_ratio",
            },
            "funding_rates": {
                0: "calc_time",
                1: "funding_interval_hours",
                2: "last_funding_rate",
            },
            "bvol_index": {
                0: "calc_time",
                1: "symbol",
                2: "base_asset",
                3: "quote_asset",
                4: "index_value",
            },
        }

        # 必需的欄位定義 (用於驗證)
        self.required_columns = {
            "klines": [
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
            ],
            "index_price_klines": [
                "symbol_id",
                "interval_type",
                "open_time",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
            ],
            "mark_price_klines": [
                "symbol_id",
                "interval_type",
                "open_time",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
            ],
            "premium_index_klines": [
                "symbol_id",
                "interval_type",
                "open_time",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
            ],
            "trades": [
                "symbol_id",
                "trade_id",
                "price",
                "quantity",
                "quote_quantity",
                "timestamp",
                "is_buyer_maker",
                "trading_type",
            ],
            "agg_trades": [
                "symbol_id",
                "agg_trade_id",
                "price",
                "quantity",
                "first_trade_id",
                "last_trade_id",
                "timestamp",
                "is_buyer_maker",
                "trading_type",
            ],
            "book_depth": [
                "symbol_id",
                "timestamp",
                "percentage",
                "depth",
                "notional",
                "trading_type",
            ],
            "book_ticker": [
                "symbol_id",
                "update_id",
                "best_bid_price",
                "best_bid_qty",
                "best_ask_price",
                "best_ask_qty",
                "transaction_time",
                "event_time",
                "trading_type",
            ],
            "trading_metrics": [
                "symbol_id",
                "create_time",
                "sum_open_interest",
                "sum_open_interest_value",
                "count_toptrader_long_short_ratio",
                "sum_toptrader_long_short_ratio",
                "count_long_short_ratio",
                "sum_taker_long_short_vol_ratio",
                "trading_type",
            ],
            "funding_rates": [
                "symbol_id",
                "calc_time",
                "funding_interval_hours",
                "last_funding_rate",
                "trading_type",
            ],
            "bvol_index": [
                "symbol_id",
                "calc_time",
                "symbol",
                "base_asset",
                "quote_asset",
                "index_value",
            ],
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
        """增強版文件名解析 - 支援所有資料類型"""
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
                    elif "BVOLIndex" in part:
                        data_type = "BVOLIndex"
                        break

            if len(name_parts) >= 3:
                symbol = name_parts[0]

                # 處理不同的檔案命名格式
                if len(name_parts) >= 4:
                    # 檢查是否包含間隔信息
                    interval = None
                    if data_type in [
                        "klines",
                        "indexPriceKlines",
                        "markPriceKlines",
                        "premiumIndexKlines",
                    ]:
                        # 對於 K線類型，第二個部分可能是間隔
                        if name_parts[1] in [
                            "1s",
                            "1m",
                            "3m",
                            "5m",
                            "15m",
                            "30m",
                            "1h",
                            "2h",
                            "4h",
                            "6h",
                            "8h",
                            "12h",
                            "1d",
                            "3d",
                            "1w",
                            "1mo",
                        ]:
                            interval = name_parts[1]
                        elif len(name_parts) >= 5 and name_parts[2] in [
                            "1s",
                            "1m",
                            "3m",
                            "5m",
                            "15m",
                            "30m",
                            "1h",
                            "2h",
                            "4h",
                            "6h",
                            "8h",
                            "12h",
                            "1d",
                            "3d",
                            "1w",
                            "1mo",
                        ]:
                            interval = name_parts[2]

                    # 提取日期部分
                    date_parts = [p for p in name_parts if p.isdigit() and len(p) >= 4]
                    if len(date_parts) >= 3:
                        year, month, day = date_parts[0], date_parts[1], date_parts[2]
                    else:
                        # 回退到最後幾個部分
                        year = (
                            name_parts[-3]
                            if len(name_parts) >= 3
                            else str(datetime.now().year)
                        )
                        month = name_parts[-2] if len(name_parts) >= 2 else "01"
                        day = name_parts[-1] if len(name_parts) >= 1 else "01"
                else:
                    interval = None
                    year = str(datetime.now().year)
                    month = "01"
                    day = "01"

                # 構建日期
                try:
                    file_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    datetime.strptime(file_date, "%Y-%m-%d")  # 驗證日期格式
                except:
                    file_date = datetime.now().strftime("%Y-%m-%d")

                # 決定時間周期
                time_period = "monthly" if day == "01" and len(month) == 2 else "daily"

                self._log(
                    f"解析文件: {filename} -> 符號:{symbol}, 類型:{data_type}, 間隔:{interval}, 日期:{file_date}"
                )

                return {
                    "symbol": symbol,
                    "data_type": data_type,
                    "interval": interval,
                    "time_period": time_period,
                    "date": file_date,
                }

            self._log(f"文件名格式不正確: {filename}", "error")
            return None

        except Exception as e:
            self._log(f"解析文件名失敗 {filename}: {e}", "error")
            return None

    def read_data_file(self, file_path):
        """讀取資料文件 - 修復版本，不過度刪除數據"""
        try:
            file_ext = Path(file_path).suffix.lower()
            self._log(f"正在讀取文件: {file_path} (格式: {file_ext})")

            if file_ext == ".csv":
                # 不使用表頭，因為 Binance 數據通常沒有列名
                df = pd.read_csv(file_path, header=None)
            elif file_ext == ".zip":
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
                df = pd.read_csv(file_path, compression="gzip", header=None)
            else:
                self._log(f"不支援的文件格式: {file_ext}", "error")
                return None

            self._log(f"成功讀取 {len(df)} 行資料，{len(df.columns)} 列")

            # 修復：不要過度清理數據
            # 只處理無限值，但保留 NaN（後續階段會妥善處理）
            df = df.replace([np.inf, -np.inf], np.nan)
            original_rows = len(df)

            # 只刪除完全空的行（所有列都是 NaN）
            df = df.dropna(how="all")

            if len(df) < original_rows:
                self._log(f"清理完全空行：{original_rows} -> {len(df)} 行")
            else:
                self._log(f"無需清理空行，保留 {len(df)} 行")

            return df

        except Exception as e:
            self._log(f"讀取文件失敗 {file_path}: {e}", "error")
            return None

    def _read_zip_file(self, zip_path):
        """讀取 ZIP 檔案中的 CSV 資料"""
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                file_list = zip_ref.namelist()
                self._log(f"ZIP 檔案包含: {file_list}")

                csv_files = [f for f in file_list if f.endswith(".csv")]
                if not csv_files:
                    self._log(f"ZIP 檔案中沒有找到 CSV 檔案: {zip_path}", "error")
                    return None

                csv_file = csv_files[0]
                self._log(f"從 ZIP 檔案讀取: {csv_file}")

                with zip_ref.open(csv_file) as csv_data:
                    df = pd.read_csv(csv_data, header=None)
                    return df

        except Exception as e:
            self._log(f"讀取 ZIP 檔案失敗 {zip_path}: {e}", "error")
            return None

    def prepare_data_by_type(
        self, df, data_type, symbol_id, trading_type, interval_type=None
    ):
        """根據資料類型準備數據 - 修復版本"""
        try:
            table_name = self.data_type_mapping.get(data_type)
            if not table_name:
                self._log(f"不支援的資料類型: {data_type}", "error")
                return None

            self._log(f"準備 {data_type} 數據，表名: {table_name}")

            # === 調試信息 ===
            self._log(f"原始數據形狀: {df.shape}")
            self._log(f"原始列名: {list(df.columns)}")

            # === 修復 trading_metrics 的列映射 ===
            if table_name == "trading_metrics":
                self._log("處理 trading_metrics 列映射...")

                # 檢查原始數據的列結構
                original_columns = list(df.columns)
                self._log(f"原始列: {original_columns}")

                # 處理兩種可能的數據格式
                if "create_time" in original_columns and "symbol" in original_columns:
                    # 格式1：已有列名 (Parquet 格式)
                    self._log("檢測到 Parquet 格式，已有列名")
                    df_renamed = df.copy()

                    # 移除 symbol 列（因為我們用 symbol_id 替代）
                    if "symbol" in df_renamed.columns:
                        df_renamed = df_renamed.drop(columns=["symbol"])
                        self._log("移除原始 symbol 列")

                else:
                    # 格式2：無列名 (CSV 格式)
                    self._log("檢測到 CSV 格式，需要映射列名")
                    expected_columns = [
                        "create_time",  # 0
                        "sum_open_interest",  # 1
                        "sum_open_interest_value",  # 2
                        "count_toptrader_long_short_ratio",  # 3
                        "sum_toptrader_long_short_ratio",  # 4
                        "count_long_short_ratio",  # 5
                        "sum_taker_long_short_vol_ratio",  # 6
                    ]

                    df_renamed = df.copy()
                    actual_col_count = len(df.columns)

                    if actual_col_count >= len(expected_columns):
                        # 使用前N列並重命名
                        df_renamed = df_renamed.iloc[:, : len(expected_columns)].copy()
                        df_renamed.columns = expected_columns
                        self._log(f"使用前 {len(expected_columns)} 列並重命名")
                    else:
                        # 列數不足，需要補充
                        new_columns = []
                        for i in range(actual_col_count):
                            if i < len(expected_columns):
                                new_columns.append(expected_columns[i])
                            else:
                                new_columns.append(f"extra_col_{i}")

                        df_renamed.columns = new_columns

                        # 補充缺失的列
                        for i in range(actual_col_count, len(expected_columns)):
                            col_name = expected_columns[i]
                            df_renamed[col_name] = 0.0
                            self._log(f"補充缺失列: {col_name}")

            else:
                # 其他表的處理邏輯保持不變
                column_mapping = self.column_mappings.get(table_name, {})

                if column_mapping and all(
                    isinstance(k, int) for k in column_mapping.keys()
                ):
                    df_renamed = df.copy()
                    new_columns = []

                    for i in range(len(df.columns)):
                        if i in column_mapping:
                            new_columns.append(column_mapping[i])
                        else:
                            new_columns.append(f"skip_col_{i}")
                            self._log(f"跳過未映射的列 {i}", "warning")

                    df_renamed.columns = new_columns

                    # 移除 ignore 列和 skip 列
                    columns_to_drop = [
                        col
                        for col in df_renamed.columns
                        if col in ["ignore"] or col.startswith("skip_col_")
                    ]
                    if columns_to_drop:
                        df_renamed = df_renamed.drop(columns=columns_to_drop)
                        self._log(f"移除列: {columns_to_drop}")
                else:
                    df_renamed = df.copy()

            # === 智能空值處理策略 ===
            if table_name == "trading_metrics":
                self._log("智能處理 trading_metrics 空值...")

                numeric_fields = [
                    "sum_open_interest",
                    "sum_open_interest_value",
                    "count_toptrader_long_short_ratio",
                    "sum_toptrader_long_short_ratio",
                    "count_long_short_ratio",
                    "sum_taker_long_short_vol_ratio",
                ]

                for field in numeric_fields:
                    if field in df_renamed.columns:
                        before_null = df_renamed[field].isna().sum()
                        total_rows = len(df_renamed)

                        if before_null > 0:
                            # 根據不同情況採用不同策略
                            if before_null == total_rows:
                                # 整列都是空值，填充為 0
                                df_renamed[field] = 0.0
                                self._log(f"整列空值 {field}，填充為 0.0")
                            elif before_null > total_rows * 0.5:
                                # 超過一半是空值，填充為 0
                                df_renamed[field] = df_renamed[field].fillna(0.0)
                                self._log(
                                    f"大量空值 {field}（{before_null}/{total_rows}），填充為 0.0"
                                )
                            else:
                                # 少量空值，可以考慮其他策略
                                # 這裡仍然填充為 0，但可以改為插值等方法
                                df_renamed[field] = df_renamed[field].fillna(0.0)
                                self._log(f"填充 {field} 的 {before_null} 個空值為 0.0")

                        # 確保沒有遺漏的 NaN
                        remaining_nulls = df_renamed[field].isna().sum()
                        if remaining_nulls > 0:
                            self._log(
                                f"警告：{field} 仍有 {remaining_nulls} 個 NaN",
                                "warning",
                            )

                # 檢查關鍵字段
                if "create_time" in df_renamed.columns:
                    null_timestamps = df_renamed["create_time"].isna().sum()
                    if null_timestamps > 0:
                        self._log(
                            f"錯誤：create_time 有 {null_timestamps} 個空值", "error"
                        )
                        return None

            # 添加必要的元數據
            df_renamed["symbol_id"] = symbol_id
            if trading_type:
                df_renamed["trading_type"] = trading_type
            if interval_type and data_type in [
                "klines",
                "indexPriceKlines",
                "markPriceKlines",
                "premiumIndexKlines",
            ]:
                df_renamed["interval_type"] = interval_type

            # === 最終列篩選：只保留資料庫中存在的欄位 ===
            if table_name == "trading_metrics":
                valid_db_columns = [
                    "symbol_id",
                    "create_time",
                    "sum_open_interest",
                    "sum_open_interest_value",
                    "count_toptrader_long_short_ratio",
                    "sum_toptrader_long_short_ratio",
                    "count_long_short_ratio",
                    "sum_taker_long_short_vol_ratio",
                    "trading_type",
                ]

                # 只保留存在的欄位
                final_columns = [
                    col for col in valid_db_columns if col in df_renamed.columns
                ]
                df_renamed = df_renamed[final_columns]

                self._log(f"最終欄位: {final_columns}")

            # 數據類型轉換
            df_renamed = self._convert_data_types(df_renamed, table_name)

            # 最終驗證
            final_rows = len(df_renamed)
            if final_rows == 0:
                self._log(f"❌ 準備後數據為空", "error")
                return None

            # 最終 NaN 檢查
            total_nans = df_renamed.isna().sum().sum()
            if total_nans > 0:
                self._log(f"❌ 仍有 {total_nans} 個 NaN 值", "warning")
                # 強制清理所有 NaN
                df_renamed = df_renamed.fillna(0.0)
                self._log("強制清理所有 NaN 為 0.0")

            self._log(
                f"✅ 成功準備 {data_type} 數據: {len(df_renamed)} 行, {len(df_renamed.columns)} 列"
            )
            return df_renamed

        except Exception as e:
            self._log(f"❌ 準備數據失敗: {e}", "error")
            import traceback

            self._log(f"詳細錯誤: {traceback.format_exc()}", "error")
            return None

    def _convert_data_types(self, df, table_name):
        """轉換數據類型"""
        try:
            # 時間戳欄位轉換
            timestamp_columns = {
                "klines": ["open_time", "close_time"],
                "index_price_klines": ["open_time"],
                "mark_price_klines": ["open_time"],
                "premium_index_klines": ["open_time"],
                "trades": ["timestamp"],
                "agg_trades": ["timestamp"],
                "book_depth": ["timestamp"],
                "book_ticker": ["transaction_time", "event_time"],
                "trading_metrics": ["create_time"],
                "funding_rates": ["calc_time"],
                "bvol_index": ["calc_time"],
            }

            # 數字欄位轉換
            numeric_columns = {
                "klines": [
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                    "volume",
                    "quote_asset_volume",
                    "taker_buy_base_asset_volume",
                    "taker_buy_quote_asset_volume",
                ],
                "index_price_klines": [
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                ],
                "mark_price_klines": [
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                ],
                "premium_index_klines": [
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                ],
                "trades": ["price", "quantity", "quote_quantity"],
                "agg_trades": ["price", "quantity"],
                "book_depth": ["percentage", "depth", "notional"],
                "book_ticker": [
                    "best_bid_price",
                    "best_bid_qty",
                    "best_ask_price",
                    "best_ask_qty",
                ],
                "trading_metrics": [
                    "sum_open_interest",
                    "sum_open_interest_value",
                    "count_toptrader_long_short_ratio",
                    "sum_toptrader_long_short_ratio",
                    "count_long_short_ratio",
                    "sum_taker_long_short_vol_ratio",
                ],
                "funding_rates": ["last_funding_rate"],
                "bvol_index": ["index_value"],
            }

            # 整數欄位轉換
            integer_columns = {
                "klines": ["number_of_trades"],
                "trades": ["trade_id"],
                "agg_trades": ["agg_trade_id", "first_trade_id", "last_trade_id"],
                "book_ticker": ["update_id"],
                "funding_rates": ["funding_interval_hours"],
            }

            # 布爾值欄位轉換
            boolean_columns = {
                "trades": ["is_buyer_maker"],
                "agg_trades": ["is_buyer_maker"],
            }

            # 執行轉換 - 支援字符串時間格式
            for col in timestamp_columns.get(table_name, []):
                if col in df.columns:
                    self._log(f"轉換時間戳欄位: {col}")

                    # 檢查數據格式
                    if len(df) > 0:
                        sample_value = df[col].iloc[0]
                        self._log(
                            f"原始 {col} 樣本: {sample_value} (類型: {type(sample_value)})"
                        )

                    # 智能轉換時間戳
                    def convert_to_timestamp_ms(value):
                        if pd.isna(value) or value is None:
                            return None

                        try:
                            # 如果已經是數字
                            if isinstance(value, (int, float)):
                                timestamp = int(value)
                                # 檢查是否是秒級時間戳
                                if timestamp < 1e12:
                                    timestamp = timestamp * 1000
                                return timestamp

                            # 如果是字符串日期
                            if isinstance(value, str):
                                value = value.strip()
                                # 嘗試解析常見格式
                                try:
                                    dt = pd.to_datetime(value)
                                    return int(dt.timestamp() * 1000)
                                except:
                                    pass

                            # 最後嘗試直接轉數字
                            timestamp = int(float(str(value)))
                            if timestamp < 1e12:
                                timestamp = timestamp * 1000
                            return timestamp

                        except Exception as e:
                            self._log(f"時間戳轉換失敗: {value} -> {e}", "warning")
                            return None

                    # 應用轉換
                    df[col] = df[col].apply(convert_to_timestamp_ms)

                    # 檢查結果
                    valid_count = df[col].notna().sum()
                    total_count = len(df)
                    self._log(f"{col} 轉換成功: {valid_count}/{total_count}")

                    if valid_count == 0:
                        self._log(
                            f"警告: {col} 全部轉換失敗，請檢查數據格式", "warning"
                        )

            for col in numeric_columns.get(table_name, []):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # 數字欄位轉換 - 特別處理 trading_metrics
            if table_name == "trading_metrics":
                self._log("處理 trading_metrics 數值轉換...")

                for col in numeric_columns.get(table_name, []):
                    if col in df.columns:
                        # 先轉換為數值
                        original_nan_count = df[col].isna().sum()
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                        after_nan_count = df[col].isna().sum()

                        if after_nan_count > original_nan_count:
                            new_nans = after_nan_count - original_nan_count
                            self._log(f"{col} 數值轉換產生了 {new_nans} 個新 NaN")

                        # 立即處理 NaN
                        if df[col].isna().any():
                            df[col] = df[col].fillna(0.0)
                            self._log(f"立即清理 {col} 的 NaN 值")

                # 精度和溢出檢查
                ratio_columns = [
                    "count_toptrader_long_short_ratio",
                    "sum_toptrader_long_short_ratio",
                    "count_long_short_ratio",
                    "sum_taker_long_short_vol_ratio",
                ]

                for col in ratio_columns:
                    if col in df.columns:
                        # 檢查溢出值
                        overflow_mask = df[col].abs() > 9999.999999
                        overflow_count = overflow_mask.sum()

                        if overflow_count > 0:
                            self._log(f"修正 {col} 的 {overflow_count} 個溢出值")
                            df[col] = df[col].clip(-9999.999999, 9999.999999).round(6)

                # 大數值欄位處理
                big_columns = ["sum_open_interest", "sum_open_interest_value"]
                for col in big_columns:
                    if col in df.columns:
                        df[col] = df[col].round(8)

                self._log("trading_metrics 數值轉換完成")

            for col in integer_columns.get(table_name, []):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

            for col in boolean_columns.get(table_name, []):
                if col in df.columns:
                    df[col] = df[col].astype(bool)

            return df

        except Exception as e:
            self._log(f"數據類型轉換失敗: {e}", "warning")
            return df

    def import_single_file(self, file_path, trading_type=None):
        """導入單個文件 - 支援所有資料類型"""
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
                elif "USD" in file_info["symbol"] and "USDT" not in file_info["symbol"]:
                    trading_type = "cm"
                elif "BVOL" in file_info["symbol"]:
                    trading_type = "option"
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
                elif "BVOL" in symbol:
                    base_asset = symbol.replace("BVOLUSDT", "BVOL")
                    quote_asset = "USDT"
                else:
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
            prepared_df = self.prepare_data_by_type(
                df, data_type, symbol_id, trading_type, file_info["interval"]
            )

            if prepared_df is None or prepared_df.empty:
                self._log(f"資料準備失敗: {file_path}", "warning")
                return False

            # 插入資料
            records_count = self.batch_insert_data(prepared_df, table_name)

            if records_count > 0:
                self._log(
                    f"成功導入 {file_path}: {records_count} 條記錄到 {table_name}"
                )
                return True
            else:
                self._log(f"導入失敗: {file_path}", "error")
                return False

        except Exception as e:
            self._log(f"導入文件失敗 {file_path}: {e}", "error")
            import traceback

            self._log(f"詳細錯誤: {traceback.format_exc()}", "error")
            return False

    def batch_insert_data(self, df, table_name, batch_size=1000):
        """批量插入資料 - 自動創建必要的分區（支援所有分區表）"""
        try:
            records_inserted = 0
            total_records = len(df)

            # === 插入前最終 NaN 檢查和清理 ===
            self._log("執行插入前 NaN 檢查...")

            # 檢查所有 NaN
            total_nans = df.isna().sum().sum()

            if total_nans > 0:
                self._log(f"發現 {total_nans} 個 NaN 值，強制清理...", "warning")

                # 詳細統計
                for col in df.columns:
                    nan_count = df[col].isna().sum()
                    if nan_count > 0:
                        self._log(f"  {col}: {nan_count} 個 NaN")

                # 強制清理策略
                if table_name == "trading_metrics":
                    # 數值欄位填 0.0
                    numeric_cols = [
                        "sum_open_interest",
                        "sum_open_interest_value",
                        "count_toptrader_long_short_ratio",
                        "sum_toptrader_long_short_ratio",
                        "count_long_short_ratio",
                        "sum_taker_long_short_vol_ratio",
                    ]

                    for col in numeric_cols:
                        if col in df.columns and df[col].isna().any():
                            before_nan = df[col].isna().sum()
                            df[col] = df[col].fillna(0.0)
                            self._log(f"    清理 {col}: {before_nan} NaN -> 0.0")

                    # 時間戳欄位不能為空
                    if "create_time" in df.columns and df["create_time"].isna().any():
                        self._log("❌ create_time 有 NaN 值，數據無效", "error")
                        return 0

                # 通用清理：所有剩餘的 NaN
                df = df.fillna(0.0)

                # 最終確認
                final_nans = df.isna().sum().sum()
                if final_nans > 0:
                    self._log(f"❌ 仍有 {final_nans} 個 NaN，取消插入", "error")
                    return 0
                else:
                    self._log("✅ 所有 NaN 已清理完成")
            else:
                self._log("✅ 無 NaN 值")

            # 檢查是否是分區表並自動創建分區
            if table_name in self.db.partition_manager.PARTITIONED_TABLES:
                timestamp_column = self.db.partition_manager.PARTITIONED_TABLES[
                    table_name
                ]
                self._log(f"正在檢查和創建 {table_name} 表的必要分區...")

                if timestamp_column in df.columns:
                    if not self.db.partition_manager.auto_create_partitions_for_data(
                        table_name, df, timestamp_column
                    ):
                        self._log("部分分區創建失敗，但將繼續嘗試插入", "warning")
                else:
                    self._log(
                        f"找不到時間戳列 {timestamp_column} 在 DataFrame 中", "warning"
                    )

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
                                    self._log(
                                        f"已插入 {records_inserted}/{total_records} 記錄"
                                    )

                            except Exception as batch_error:
                                # 如果是分區問題，嘗試針對這個批次創建分區
                                if "no partition of relation" in str(batch_error):
                                    self._log(
                                        f"找不到 {table_name} 分區，嘗試為當前批次創建分區...",
                                        "warning",
                                    )

                                    # 為當前批次的數據創建分區
                                    if (
                                        table_name
                                        in self.db.partition_manager.PARTITIONED_TABLES
                                    ):
                                        timestamp_column = self.db.partition_manager.PARTITIONED_TABLES[
                                            table_name
                                        ]
                                        batch_df = pd.DataFrame(batch)

                                        if self.db.partition_manager.auto_create_partitions_for_data(
                                            table_name, batch_df, timestamp_column
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
                                                self._log(
                                                    f"創建分區後成功插入 {len(batch)} 條記錄"
                                                )
                                            except Exception as retry_error:
                                                self._log(
                                                    f"重試插入仍然失敗: {retry_error}",
                                                    "error",
                                                )
                                                raise retry_error
                                        else:
                                            self._log(
                                                f"創建分區失敗: {batch_error}", "error"
                                            )
                                            raise batch_error
                                    else:
                                        self._log(
                                            f"表 {table_name} 不支援自動分區創建",
                                            "error",
                                        )
                                        raise batch_error
                                else:
                                    raise batch_error

                    conn.commit()
                    self._log(f"成功插入 {records_inserted} 條記錄到 {table_name}")
                    return records_inserted

        except Exception as e:
            self._log(f"批量插入失敗: {e}", "error")
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
                self._list_directory_contents(directory_path)

                if self.external_logger:
                    self.external_logger.log_directory_scan(
                        directory_path, 0, file_types
                    )
                return {
                    "successful_imports": 0,
                    "failed_imports": 0,
                    "failed_files": [],
                }

            self._log(f"總共找到 {len(all_files)} 個文件待導入")

            if self.external_logger:
                self.external_logger.log_directory_scan(
                    directory_path, len(all_files), file_types
                )

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
                                self.external_logger.log_file_processing(
                                    file_path, success=True
                                )
                        else:
                            failed_imports += 1
                            failed_files.append(file_path)
                            self._log(
                                f"❌ 導入失敗: {os.path.basename(file_path)}", "error"
                            )

                            if self.external_logger:
                                self.external_logger.log_file_processing(
                                    file_path, success=False
                                )

                        # 每處理10個文件輸出一次進度
                        total_processed = successful_imports + failed_imports
                        if total_processed % 10 == 0:
                            self._log(
                                f"進度: {total_processed}/{len(all_files)} 文件已處理"
                            )

                    except Exception as e:
                        failed_imports += 1
                        failed_files.append(file_path)
                        error_msg = str(e)
                        self._log(f"處理文件失敗 {file_path}: {error_msg}", "error")

                        if self.external_logger:
                            self.external_logger.log_file_processing(
                                file_path, success=False, error_msg=error_msg
                            )

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
            "successful_imports": successful_imports,
            "failed_imports": failed_imports,
            "failed_files": failed_files,
        }

    def _list_directory_contents(self, directory_path, max_files=20):
        """列出目錄內容來協助除錯"""
        try:
            self._log(f"目錄 {directory_path} 的內容:")

            if not os.path.exists(directory_path):
                self._log(f"目錄不存在: {directory_path}", "error")
                return

            file_count = 0
            for root, dirs, files in os.walk(directory_path):
                self._log(f"目錄: {root}")
                for file in files:
                    if file_count >= max_files:
                        self._log(f"...及更多文件 (只顯示前 {max_files} 個)")
                        return

                    file_path = os.path.join(root, file)
                    file_ext = Path(file).suffix.lower()
                    file_size = os.path.getsize(file_path)

                    self._log(f"  {file} ({file_ext}, {file_size} bytes)")
                    file_count += 1

            if file_count == 0:
                self._log(f"  目錄為空")

        except Exception as e:
            self._log(f"無法列出目錄內容: {e}", "error")

    def incremental_update(
        self, symbol, data_type, trading_type, interval=None, days_back=7
    ):
        """增量更新：檢查並導入最近的資料"""
        try:
            symbol_id = self.symbol_manager.get_symbol_id(symbol)
            if not symbol_id:
                self._log(f"找不到交易對: {symbol}", "error")
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

            self._log(f"增量更新 {symbol} {data_type} 從 {start_date} 到 {end_date}")

            # 構建文件路徑模式
            store_directory = os.getenv("STORE_DIRECTORY")
            if not store_directory:
                self._log("未設置 STORE_DIRECTORY 環境變數", "error")
                return False

            # 根據交易類型構建路徑
            if trading_type == "spot":
                base_path = os.path.join(
                    store_directory, "data", "spot", "daily", data_type, symbol
                )
            elif trading_type == "option":
                base_path = os.path.join(
                    store_directory, "data", "option", "daily", data_type, symbol
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
                self._log(f"找到 {len(files_to_import)} 個文件需要增量更新")
                for file_path in files_to_import:
                    self.import_single_file(file_path, trading_type)
                return True
            else:
                self._log(f"沒有找到需要更新的文件")
                return False

        except Exception as e:
            self._log(f"增量更新失敗: {e}", "error")
            return False

    def get_supported_data_types(self):
        """獲取支援的資料類型列表"""
        return list(self.data_type_mapping.keys())

    def validate_data_structure(self, df, data_type):
        """驗證數據結構是否符合預期"""
        try:
            table_name = self.data_type_mapping.get(data_type)
            if not table_name:
                return False, f"不支援的資料類型: {data_type}"

            expected_columns = len(self.column_mappings.get(table_name, {}))
            actual_columns = len(df.columns)

            if expected_columns > 0 and actual_columns != expected_columns:
                return (
                    False,
                    f"{data_type} 預期 {expected_columns} 列，實際 {actual_columns} 列",
                )

            return True, "數據結構驗證通過"

        except Exception as e:
            return False, f"驗證失敗: {e}"


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
