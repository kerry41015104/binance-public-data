#!/usr/bin/env python

"""
Improved script to download klines with enhanced date tracking and error handling.
Features:
- Enhanced date tracking for each symbol
- Better memory management
- Improved error handling to prevent system crashes
- Download progress tracking and resumption
- Fixed path handling to prevent NoneType errors
"""
import sys
import os
import json
import gc
import psutil
import time
from datetime import date, datetime, timedelta, timezone
import pandas as pd
from pathlib import Path
from enums import *
from utility import (
    download_file,
    get_all_symbols,
    get_parser,
    get_start_end_date_objects,
    convert_to_date_object,
    get_path,
    check_existing_files,
    download_missing_files,
    get_destination_dir,
)
from dotenv import load_dotenv
from get_oldest_date import OptimizedBinanceDataScraper


class DownloadProgressTracker:
    """追蹤下載進度和日期記錄"""

    def __init__(self, base_folder):
        self.base_folder = base_folder
        self.scraper = OptimizedBinanceDataScraper()

    def get_symbol_status_file(self, trading_type, symbol, interval):
        """獲取標的狀態檔案路徑"""
        symbol_dir = os.path.join(
            self.base_folder,
            "data",
            "futures" if trading_type != "spot" else "spot",
            trading_type if trading_type != "spot" else "",
            "daily",
            "klines",
            symbol.upper(),
            interval,
        )
        if trading_type == "spot":
            symbol_dir = os.path.join(
                self.base_folder,
                "data",
                "spot",
                "daily",
                "klines",
                symbol.upper(),
                interval,
            )

        os.makedirs(symbol_dir, exist_ok=True)
        return os.path.join(symbol_dir, f"{symbol.upper()}_{interval}_status.json")

    def load_symbol_status(self, trading_type, symbol, interval):
        """載入標的下載狀態"""
        status_file = self.get_symbol_status_file(trading_type, symbol, interval)
        default_status = {
            "symbol": symbol.upper(),
            "interval": interval,
            "earliest_date": None,
            "latest_date": None,
            "last_download_date": None,
            "failed_dates": [],
            "conversion_failed_dates": [],
            "total_downloaded": 0,
            "last_updated": None,
            "trading_type": trading_type,
        }

        if os.path.exists(status_file):
            try:
                with open(status_file, "r", encoding="utf-8") as f:
                    status = json.load(f)
                    # 確保所有必要欄位存在
                    for key, value in default_status.items():
                        if key not in status:
                            status[key] = value
                    return status
            except Exception as e:
                print(f"   ⚠️ 無法讀取狀態檔案 {status_file}: {e}")

        return default_status

    def save_symbol_status(self, trading_type, symbol, interval, status):
        """儲存標的下載狀態"""
        status_file = self.get_symbol_status_file(trading_type, symbol, interval)
        status["last_updated"] = datetime.now(timezone.utc).isoformat()

        try:
            with open(status_file, "w", encoding="utf-8") as f:
                json.dump(status, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"   ⚠️ 無法儲存狀態檔案 {status_file}: {e}")

    def update_download_status(
        self, trading_type, symbol, interval, date_str, success, is_conversion=False
    ):
        """更新下載狀態"""
        status = self.load_symbol_status(trading_type, symbol, interval)

        if success:
            # 更新最早和最新日期
            current_date = convert_to_date_object(date_str)

            if not status["earliest_date"] or current_date < convert_to_date_object(
                status["earliest_date"]
            ):
                status["earliest_date"] = date_str

            if not status["latest_date"] or current_date > convert_to_date_object(
                status["latest_date"]
            ):
                status["latest_date"] = date_str

            status["last_download_date"] = date_str
            status["total_downloaded"] += 1

            # 從失敗列表中移除（如果存在）
            if is_conversion:
                if date_str in status["conversion_failed_dates"]:
                    status["conversion_failed_dates"].remove(date_str)
            else:
                if date_str in status["failed_dates"]:
                    status["failed_dates"].remove(date_str)
        else:
            # 記錄失敗日期
            if is_conversion:
                if date_str not in status["conversion_failed_dates"]:
                    status["conversion_failed_dates"].append(date_str)
            else:
                if date_str not in status["failed_dates"]:
                    status["failed_dates"].append(date_str)

        self.save_symbol_status(trading_type, symbol, interval, status)
        return status

    def get_dates_to_download(
        self, trading_type, symbol, interval, all_dates, start_date, end_date
    ):
        """獲取需要下載的日期列表"""
        status = self.load_symbol_status(trading_type, symbol, interval)

        # 獲取失敗的日期（合併下載失敗和轉換失敗）
        failed_dates = set(status["failed_dates"] + status["conversion_failed_dates"])

        # 如果有最新日期記錄，從下一天開始下載新資料
        dates_to_download = []

        if status["latest_date"]:
            latest_date = convert_to_date_object(status["latest_date"])
            next_date = latest_date + timedelta(days=1)

            # 添加新日期（從最新日期的下一天開始）
            for date_str in all_dates:
                current_date = convert_to_date_object(date_str)
                if (
                    current_date >= next_date
                    and current_date >= start_date
                    and current_date <= end_date
                ):
                    dates_to_download.append(date_str)
        else:
            # 沒有記錄，下載所有日期
            try:
                start_date_from_web = self.scraper.get_earliest_date_for_symbol(
                    trading_type, "klines", symbol, interval
                )
            except Exception as e:
                print(f"   ⚠️ 獲取 {symbol} 網頁最早日期失敗: {e}")
                start_date_from_web = start_date

            # 若回傳 None（標的不存在於頁面），直接跳過，不下載任何日期
            if start_date_from_web is None:
                print(f"   ⚠️ {symbol} 在資料來源頁面上找不到，略過下載")
                return []

            try:
                if type(start_date_from_web) is str:
                    start_date_from_web = convert_to_date_object(start_date_from_web)
                # 若轉換後仍為 None，fallback 到 start_date
                if start_date_from_web is None:
                    print(f"   ⚠️ {symbol} 最早日期轉換結果為 None，使用預設起始日期")
                    start_date_from_web = start_date
            except Exception as e:
                print(f"   ⚠️ 解析 {symbol} 網頁最早日期失敗: {e}")
                start_date_from_web = start_date

            for date_str in all_dates:
                current_date = convert_to_date_object(date_str)
                if (
                    current_date >= start_date
                    and current_date <= end_date
                    and current_date >= start_date_from_web
                ):
                    dates_to_download.append(date_str)

        # 添加失敗的日期
        for date_str in failed_dates:
            current_date = convert_to_date_object(date_str)
            if (
                current_date >= start_date
                and current_date <= end_date
                and date_str not in dates_to_download
            ):
                dates_to_download.append(date_str)

        # 排序日期
        dates_to_download.sort()

        print(f"   📊 {symbol} {interval} 狀態:")
        print(f"      最早日期: {status['earliest_date'] or 'N/A'}")
        print(f"      最新日期: {status['latest_date'] or 'N/A'}")
        print(f"      已下載: {status['total_downloaded']} 個檔案")
        print(f"      失敗日期: {len(failed_dates)} 個")
        print(f"      需下載: {len(dates_to_download)} 個")

        return dates_to_download


def check_system_resources():
    """檢查系統資源使用情況"""
    try:
        memory = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=1)

        print(f"\n🖥️  系統資源狀況:")
        print(
            f"   💾 記憶體使用: {memory.percent:.1f}% ({memory.available // (1024**3):.1f}GB 可用)"
        )
        print(f"   🔄 CPU使用: {cpu:.1f}%")

        # 如果記憶體使用超過80%，執行垃圾回收
        if memory.percent > 80:
            print(f"   ⚠️ 記憶體使用過高，執行垃圾回收...")
            gc.collect()
            time.sleep(2)

        return memory.percent < 90  # 如果記憶體使用超過90%則返回False
    except Exception as e:
        print(f"   ⚠️ 無法檢查系統資源: {e}")
        return True


def ensure_folder_path(folder):
    """確保 folder 路徑正確設定，修正 NoneType 問題"""
    if folder is None:
        load_dotenv()
        folder = os.environ.get("STORE_DIRECTORY")
        if not folder:
            folder = os.path.dirname(os.path.realpath(__file__))
            print(f"⚠️ 未設定 STORE_DIRECTORY，使用當前目錄: {folder}")
        else:
            print(f"📁 使用環境變數設定的儲存目錄: {folder}")

    # 確保目錄存在
    if not os.path.exists(folder):
        try:
            os.makedirs(folder, exist_ok=True)
            print(f"📁 創建儲存目錄: {folder}")
        except Exception as e:
            print(f"❌ 無法創建目錄 {folder}: {e}")
            folder = os.path.dirname(os.path.realpath(__file__))
            print(f"📁 回退到當前目錄: {folder}")

    return folder


def download_daily_klines_improved(
    trading_type,
    symbols,
    num_symbols,
    intervals,
    dates,
    start_date,
    end_date,
    folder,
    checksum,
    data_format=".zip",
):
    """改進的日資料下載函數"""
    current = 0

    # 確保 folder 路徑正確
    folder = ensure_folder_path(folder)
    # 初始化進度追蹤器
    progress_tracker = DownloadProgressTracker(folder)

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        # 使用UTC時間，減去2小時作為安全邊界
        utc_now = datetime.now(timezone.utc)
        end_date = (utc_now - timedelta(hours=2)).date()
    else:
        end_date = convert_to_date_object(end_date)

    # 獲取有效的日間隔
    intervals = list(set(intervals) & set(DAILY_INTERVALS))
    print(f"Found {num_symbols} symbols")

    for symbol in symbols:
        current += 1
        print(f"\n[{current}/{num_symbols}] - 開始處理 {symbol} 的日資料")

        # 每處理5個標的檢查一次系統資源
        if current % 5 == 0:
            if not check_system_resources():
                print(f"\n⚠️ 系統資源不足，暫停10秒...")
                time.sleep(10)
                gc.collect()

        for interval in intervals:
            print(f"\n  ⏰ 處理 {symbol} {interval} 時間間隔")

            # 獲取需要下載的日期
            dates_to_download = progress_tracker.get_dates_to_download(
                trading_type, symbol, interval, dates, start_date, end_date
            )

            if not dates_to_download:
                print(f"      ✅ {symbol} {interval} 所有資料已是最新")
                continue

            # 批次下載（每次最多50個檔案）
            batch_size = 50
            total_batches = (len(dates_to_download) + batch_size - 1) // batch_size

            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(dates_to_download))
                batch_dates = dates_to_download[start_idx:end_idx]

                print(
                    f"      📦 批次 {batch_idx + 1}/{total_batches}: 下載 {len(batch_dates)} 個檔案"
                )

                for date_str in batch_dates:
                    try:
                        path = get_path(
                            trading_type, "klines", "daily", symbol, interval
                        )
                        file_name = f"{symbol.upper()}-{interval}-{date_str}.zip"

                        # 檢查檔案是否已存在（包括轉換後的格式）
                        save_path = get_destination_dir(
                            os.path.join(path, file_name), folder
                        )

                        # 檢查最終格式檔案是否存在
                        if data_format != ".zip":
                            base_name = os.path.splitext(save_path)[0]
                            final_path = base_name + data_format
                            if os.path.exists(final_path):
                                progress_tracker.update_download_status(
                                    trading_type, symbol, interval, date_str, True
                                )
                                continue
                        elif os.path.exists(save_path):
                            progress_tracker.update_download_status(
                                trading_type, symbol, interval, date_str, True
                            )
                            continue

                        # 下載檔案
                        print(f"         📅 {date_str}", end="")
                        success = download_file(
                            path, file_name, None, folder, data_format, timeout=120
                        )

                        # 更新狀態
                        if success is not False:
                            progress_tracker.update_download_status(
                                trading_type, symbol, interval, date_str, True
                            )
                            print(" ✅")
                        else:
                            progress_tracker.update_download_status(
                                trading_type, symbol, interval, date_str, False
                            )
                            print(" ❌")

                        # 下載checksum檔案
                        if checksum == 1 and success is not False:
                            checksum_file_name = file_name + ".CHECKSUM"
                            download_file(
                                path,
                                checksum_file_name,
                                None,
                                folder,
                                ".zip",
                                timeout=60,
                            )

                        # 每下載10個檔案休息一下，避免系統負荷過重
                        if (batch_dates.index(date_str) + 1) % 10 == 0:
                            time.sleep(1)

                    except Exception as e:
                        print(f"         📅 {date_str} ❌ 錯誤: {str(e)}")
                        progress_tracker.update_download_status(
                            trading_type, symbol, interval, date_str, False
                        )
                        continue

                # 批次完成後強制垃圾回收
                gc.collect()
                time.sleep(0.5)

        # 每個標的完成後檢查記憶體
        if current % 3 == 0:
            gc.collect()


def download_monthly_klines_improved(
    trading_type,
    symbols,
    num_symbols,
    intervals,
    years,
    months,
    start_date,
    end_date,
    folder,
    checksum,
    data_format=".zip",
):
    """改進的月資料下載函數"""
    current = 0

    # 確保 folder 路徑正確
    folder = ensure_folder_path(folder)

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    print(f"Found {num_symbols} symbols")

    for symbol in symbols:
        current += 1
        print(f"\n[{current}/{num_symbols}] - 開始處理 {symbol} 的月資料")

        # 系統資源檢查
        if current % 5 == 0:
            if not check_system_resources():
                print(f"\n⚠️ 系統資源不足，暫停10秒...")
                time.sleep(10)
                gc.collect()

        for interval in intervals:
            print(f"  ⏰ 處理 {symbol} {interval} 時間間隔")

            for year in years:
                for month in months:
                    try:
                        current_date = convert_to_date_object(f"{year}-{month:02d}-01")
                        if current_date >= start_date and current_date <= end_date:
                            path = get_path(
                                trading_type, "klines", "monthly", symbol, interval
                            )
                            file_name = (
                                f"{symbol.upper()}-{interval}-{year}-{month:02d}.zip"
                            )

                            # 檢查檔案是否已存在
                            save_path = get_destination_dir(
                                os.path.join(path, file_name), folder
                            )

                            if data_format != ".zip":
                                base_name = os.path.splitext(save_path)[0]
                                final_path = base_name + data_format
                                if os.path.exists(final_path):
                                    continue
                            elif os.path.exists(save_path):
                                continue

                            print(f"    📅 {year}-{month:02d}", end="")
                            success = download_file(
                                path, file_name, None, folder, data_format, timeout=180
                            )

                            if success is not False:
                                print(" ✅")

                                # 下載checksum檔案
                                if checksum == 1:
                                    checksum_file_name = file_name + ".CHECKSUM"
                                    download_file(
                                        path,
                                        checksum_file_name,
                                        None,
                                        folder,
                                        ".zip",
                                        timeout=60,
                                    )
                            else:
                                print(" ❌")

                    except Exception as e:
                        print(f"    📅 {year}-{month:02d} ❌ 錯誤: {str(e)}")
                        continue

            # 間隔休息
            time.sleep(0.5)

        # 每個標的完成後垃圾回收
        if current % 3 == 0:
            gc.collect()


# 原有函數保持向下相容
def download_monthly_klines(
    trading_type,
    symbols,
    num_symbols,
    intervals,
    years,
    months,
    start_date,
    end_date,
    folder,
    checksum,
    data_format=".zip",
):
    return download_monthly_klines_improved(
        trading_type,
        symbols,
        num_symbols,
        intervals,
        years,
        months,
        start_date,
        end_date,
        folder,
        checksum,
        data_format,
    )


def download_daily_klines(
    trading_type,
    symbols,
    num_symbols,
    intervals,
    dates,
    start_date,
    end_date,
    folder,
    checksum,
    data_format=".zip",
):
    return download_daily_klines_improved(
        trading_type,
        symbols,
        num_symbols,
        intervals,
        dates,
        start_date,
        end_date,
        folder,
        checksum,
        data_format,
    )


if __name__ == "__main__":
    print("🚀 啟動改進版 Binance K線資料下載器")
    print("=" * 50)

    parser = get_parser("klines")
    args = parser.parse_args(sys.argv[1:])

    # 檢查系統資源
    check_system_resources()

    # 確保 folder 路徑正確設定
    args.folder = ensure_folder_path(args.folder)

    if not args.symbols:
        print("fetching all symbols from exchange")
        symbols = get_all_symbols(args.type)
        num_symbols = len(symbols)
    else:
        symbols = args.symbols
        num_symbols = len(symbols)

    if args.dates:
        dates = args.dates
    else:
        period = convert_to_date_object(
            datetime.today().strftime("%Y-%m-%d")
        ) - convert_to_date_object(PERIOD_START_DATE)
        dates = (
            pd.date_range(end=datetime.today(), periods=period.days + 1)
            .to_pydatetime()
            .tolist()
        )
        dates = [date.strftime("%Y-%m-%d") for date in dates]

    print(f"\n📋 下載設定:")
    print(f"   📊 標的數量: {num_symbols}")
    print(f"   📁 儲存路徑: {args.folder}")
    print(f"   📄 檔案格式: {args.data_format}")
    print(f"   ⏰ 時間間隔: {args.intervals}")

    try:
        # 月資料下載
        if args.skip_monthly == 0:
            print(f"\n🗓️ 開始下載月資料...")
            download_monthly_klines_improved(
                args.type,
                symbols,
                num_symbols,
                args.intervals,
                args.years,
                args.months,
                args.startDate,
                args.endDate,
                args.folder,
                args.checksum,
                args.data_format,
            )
            print(f"✅ 月資料下載完成")

        # 日資料下載
        if args.skip_daily == 0:
            print(f"\n📅 開始下載日資料...")
            download_daily_klines_improved(
                args.type,
                symbols,
                num_symbols,
                args.intervals,
                dates,
                args.startDate,
                args.endDate,
                args.folder,
                args.checksum,
                args.data_format,
            )
            print(f"✅ 日資料下載完成")

    except KeyboardInterrupt:
        print(f"\n\n⚠️ 使用者中斷下載")
        print(f"💾 已下載的資料和進度已保存")
    except Exception as e:
        print(f"\n\n❌ 下載過程發生錯誤: {str(e)}")
        print(f"💾 已下載的資料和進度已保存")
        import traceback

        traceback.print_exc()
    finally:
        # 最終清理
        gc.collect()
        print(f"\n🎉 下載程序結束")
