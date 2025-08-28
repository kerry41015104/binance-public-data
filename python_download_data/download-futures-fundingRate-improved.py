#!/usr/bin/env python

"""
Improved script to download fundingRate with enhanced date tracking and error handling.
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
    raise_arg_error,
)
from dotenv import load_dotenv
from get_oldest_date import OptimizedBinanceDataScraper


class FundingRateDownloadProgressTracker:
    """追蹤下載進度和日期記錄"""

    def __init__(self, base_folder):
        self.base_folder = base_folder
        self.scraper = OptimizedBinanceDataScraper()

    def get_symbol_status_file(self, trading_type, symbol):
        """獲取標的狀態檔案路徑"""
        symbol_dir = os.path.join(
            self.base_folder,
            "data",
            "futures",
            trading_type,
            "monthly",
            "fundingRate",
            symbol.upper(),
        )

        os.makedirs(symbol_dir, exist_ok=True)
        return os.path.join(symbol_dir, f"{symbol.upper()}_fundingRate_status.json")

    def load_symbol_status(self, trading_type, symbol):
        """載入標的下載狀態"""
        status_file = self.get_symbol_status_file(trading_type, symbol)
        default_status = {
            "symbol": symbol.upper(),
            "data_type": "fundingRate",
            "earliest_month": None,
            "latest_month": None,
            "last_download_month": None,
            "failed_months": [],
            "conversion_failed_months": [],
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

    def save_symbol_status(self, trading_type, symbol, status):
        """儲存標的下載狀態"""
        status_file = self.get_symbol_status_file(trading_type, symbol)
        status["last_updated"] = datetime.now(timezone.utc).isoformat()

        try:
            with open(status_file, "w", encoding="utf-8") as f:
                json.dump(status, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"   ⚠️ 無法儲存狀態檔案 {status_file}: {e}")

    def update_download_status(
        self, trading_type, symbol, year_month_str, success, is_conversion=False
    ):
        """更新下載狀態"""
        status = self.load_symbol_status(trading_type, symbol)

        if success:
            # 更新最早和最新月份
            current_month = year_month_str

            if not status["earliest_month"] or current_month < status["earliest_month"]:
                status["earliest_month"] = current_month

            if not status["latest_month"] or current_month > status["latest_month"]:
                status["latest_month"] = current_month

            status["last_download_month"] = current_month
            status["total_downloaded"] += 1

            # 從失敗列表中移除（如果存在）
            if is_conversion:
                if current_month in status["conversion_failed_months"]:
                    status["conversion_failed_months"].remove(current_month)
            else:
                if current_month in status["failed_months"]:
                    status["failed_months"].remove(current_month)
        else:
            # 記錄失敗月份
            if is_conversion:
                if year_month_str not in status["conversion_failed_months"]:
                    status["conversion_failed_months"].append(year_month_str)
            else:
                if year_month_str not in status["failed_months"]:
                    status["failed_months"].append(year_month_str)

        self.save_symbol_status(trading_type, symbol, status)
        return status

    def get_months_to_download(
        self, trading_type, symbol, years, months, start_date, end_date
    ):
        """獲取需要下載的月份列表"""
        status = self.load_symbol_status(trading_type, symbol)

        # 獲取失敗的月份（合併下載失敗和轉換失敗）
        failed_months = set(status["failed_months"] + status["conversion_failed_months"])

        # 生成所有需要檢查的月份
        all_months = []
        for year in years:
            for month in months:
                month_str = f"{year}-{month:02d}"
                current_date = convert_to_date_object(f"{year}-{month:02d}-01")
                if current_date >= start_date and current_date <= end_date:
                    all_months.append(month_str)

        # 如果有最新月份記錄，從下一月開始下載新資料
        months_to_download = []

        if status["latest_month"]:
            latest_year, latest_month = map(int, status["latest_month"].split("-"))
            next_month = latest_month + 1
            next_year = latest_year
            if next_month > 12:
                next_month = 1
                next_year += 1
            
            next_month_str = f"{next_year}-{next_month:02d}"

            # 添加新月份（從最新月份的下一月開始）
            for month_str in all_months:
                if month_str >= next_month_str:
                    months_to_download.append(month_str)
        else:
            # 沒有記錄，使用網頁爬取的最早日期來判斷起始月份
            try:
                start_date_from_web = self.scraper.get_earliest_date_for_symbol(
                    trading_type, "fundingRate", symbol, None
                )
                if start_date_from_web:
                    if isinstance(start_date_from_web, str):
                        start_date_from_web = convert_to_date_object(start_date_from_web)
                    earliest_month_str = f"{start_date_from_web.year}-{start_date_from_web.month:02d}"
                    
                    for month_str in all_months:
                        if month_str >= earliest_month_str:
                            months_to_download.append(month_str)
                else:
                    months_to_download = all_months
            except Exception as e:
                print(f"   ⚠️ 獲取 {symbol} 網頁最早日期失敗: {e}")
                months_to_download = all_months

        # 添加失敗的月份
        for month_str in failed_months:
            if month_str in all_months and month_str not in months_to_download:
                months_to_download.append(month_str)

        # 排序月份
        months_to_download.sort()

        print(f"   📊 {symbol} fundingRate 狀態:")
        print(f"      最早月份: {status['earliest_month'] or 'N/A'}")
        print(f"      最新月份: {status['latest_month'] or 'N/A'}")
        print(f"      已下載: {status['total_downloaded']} 個檔案")
        print(f"      失敗月份: {len(failed_months)} 個")
        print(f"      需下載: {len(months_to_download)} 個")

        return months_to_download


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


def download_monthly_fundingRate_improved(
    trading_type,
    symbols,
    num_symbols,
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
    # 初始化進度追蹤器
    progress_tracker = FundingRateDownloadProgressTracker(folder)

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
        print(f"\n[{current}/{num_symbols}] - 開始處理 {symbol} 的月 FundingRate 資料")

        # 每處理5個標的檢查一次系統資源
        if current % 5 == 0:
            if not check_system_resources():
                print(f"\n⚠️ 系統資源不足，暫停10秒...")
                time.sleep(10)
                gc.collect()

        # 獲取需要下載的月份
        months_to_download = progress_tracker.get_months_to_download(
            trading_type, symbol, years, months, start_date, end_date
        )

        if not months_to_download:
            print(f"      ✅ {symbol} FundingRate 資料已是最新")
            continue

        # 批次下載（每次最多12個檔案，約一年的數據）
        batch_size = 12
        total_batches = (len(months_to_download) + batch_size - 1) // batch_size

        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(months_to_download))
            batch_months = months_to_download[start_idx:end_idx]

            print(
                f"      📦 批次 {batch_idx + 1}/{total_batches}: 下載 {len(batch_months)} 個檔案"
            )

            for month_str in batch_months:
                try:
                    year, month = month_str.split("-")
                    year = int(year)
                    month = int(month)

                    path = get_path(trading_type, "fundingRate", "monthly", symbol)
                    file_name = f"{symbol.upper()}-fundingRate-{year}-{month:02d}.zip"

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
                                trading_type, symbol, month_str, True
                            )
                            continue
                    elif os.path.exists(save_path):
                        progress_tracker.update_download_status(
                            trading_type, symbol, month_str, True
                        )
                        continue

                    # 下載檔案
                    print(f"         📅 {year}-{month:02d}", end="")
                    success = download_file(
                        path, file_name, None, folder, data_format, timeout=180
                    )

                    # 更新狀態
                    if success is not False:
                        progress_tracker.update_download_status(
                            trading_type, symbol, month_str, True
                        )
                        print(" ✅")
                    else:
                        progress_tracker.update_download_status(
                            trading_type, symbol, month_str, False
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

                    # 每下載5個檔案休息一下，避免系統負荷過重
                    if (batch_months.index(month_str) + 1) % 5 == 0:
                        time.sleep(1)

                except Exception as e:
                    print(f"         📅 {month_str} ❌ 錯誤: {str(e)}")
                    progress_tracker.update_download_status(
                        trading_type, symbol, month_str, False
                    )
                    continue

            # 批次完成後強制垃圾回收
            gc.collect()
            time.sleep(0.5)

        # 每個標的完成後檢查記憶體
        if current % 3 == 0:
            gc.collect()


# 原有函數保持向下相容
def download_monthly_fundingRate(
    trading_type,
    symbols,
    num_symbols,
    years,
    months,
    start_date,
    end_date,
    folder,
    checksum,
    data_format=".zip",
):
    return download_monthly_fundingRate_improved(
        trading_type,
        symbols,
        num_symbols,
        years,
        months,
        start_date,
        end_date,
        folder,
        checksum,
        data_format,
    )


if __name__ == "__main__":
    print("🚀 啟動改進版 Binance FundingRate 資料下載器")
    print("=" * 50)

    parser = get_parser("fundingRate")
    args = parser.parse_args(sys.argv[1:])

    if args.type not in ["um", "cm"]:
        raise_arg_error("FundingRate is only for the um or cm trading type")

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

    try:
        # Check existing files before downloading
        print("\n=== 📊 FUNDING RATE 資料檢查 ===")
        
        # FundingRate only has monthly data
        if args.skip_monthly == 0:
            print("\n🗓️ 檢查月資料...")
            need_monthly = check_existing_files(
                args.type, "fundingRate", symbols, "monthly", 
                None, args.years, args.months, 
                None, args.startDate, args.endDate, args.folder
            )
            if need_monthly:
                print(f"\n🗓️ 開始下載月 FundingRate 資料...")
                download_monthly_fundingRate_improved(
                    args.type,
                    symbols,
                    num_symbols,
                    args.years,
                    args.months,
                    args.startDate,
                    args.endDate,
                    args.folder,
                    args.checksum,
                    args.data_format,
                )
                print(f"✅ 月 FundingRate 資料下載完成")
            else:
                print("✅ 月資料已完整，跳過下載")

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
