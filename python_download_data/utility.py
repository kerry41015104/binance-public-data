import os, sys, re, shutil
import json
import zipfile
import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
import urllib.request
import time
import signal
from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentTypeError
from enums import *
from dotenv import load_dotenv


def get_destination_dir(file_url, folder=None):
    load_dotenv()  # 會讀取 .env 檔
    store_directory = os.environ.get("STORE_DIRECTORY")
    if folder:
        store_directory = folder
    if not store_directory:
        store_directory = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(store_directory, file_url)


def get_download_url(file_url):
    return "{}{}".format(BASE_URL, file_url)


def get_all_symbols(type):
    if type == "um":
        response = urllib.request.urlopen(
            "https://fapi.binance.com/fapi/v1/exchangeInfo"
        ).read()
    elif type == "cm":
        response = urllib.request.urlopen(
            "https://dapi.binance.com/dapi/v1/exchangeInfo"
        ).read()
    else:
        response = urllib.request.urlopen(
            "https://api.binance.com/api/v3/exchangeInfo"
        ).read()
    return list(map(lambda symbol: symbol["symbol"], json.loads(response)["symbols"]))


def check_existing_files(trading_type, market_data_type, symbols, time_period, intervals=None, years=None, months=None, dates=None, start_date=None, end_date=None, folder=None):
    """
    檢查已存在的檔案，找出缺失的日期
    返回: (need_download: bool, missing_files: list)
    """
    print(f"\n🔍 檢查已存在的 {market_data_type} 檔案...")
    
    missing_files = []
    existing_count = 0
    
    for symbol in symbols:
        print(f"\n📊 檢查 {symbol} 的 {market_data_type} 檔案:")
        
        if time_period == "daily":
            missing_daily = check_daily_files(trading_type, market_data_type, symbol, intervals, dates, start_date, end_date, folder)
            missing_files.extend(missing_daily)
            
        elif time_period == "monthly":
            missing_monthly = check_monthly_files(trading_type, market_data_type, symbol, intervals, years, months, start_date, end_date, folder)
            missing_files.extend(missing_monthly)
            
        # 計算現有檔案數量
        symbol_path = get_destination_dir(get_path(trading_type, market_data_type, time_period, symbol, intervals[0] if intervals else None), folder)
        if os.path.exists(os.path.dirname(symbol_path)):
            existing_files = [f for f in os.listdir(os.path.dirname(symbol_path)) if f.endswith(('.zip', '.csv', '.parquet', '.feather', '.h5'))]
            existing_count += len(existing_files)
            print(f"   ✅ 現有檔案: {len(existing_files)} 個")
        else:
            print(f"   📁 目錄不存在，將創建新目錄")
    
    print(f"\n📈 檢查結果:")
    print(f"   📁 現有檔案總數: {existing_count}")
    print(f"   📥 需要下載: {len(missing_files)} 個檔案")
    
    if len(missing_files) == 0:
        print(f"   🎉 所有檔案都已存在！")
        return False, []  # 不需要下載，空的缺失列表
    else:
        print(f"   🔄 將下載缺失的檔案")
        return True, missing_files  # 需要下載，返回缺失檔案列表


def check_daily_files(trading_type, market_data_type, symbol, intervals, dates, start_date, end_date, folder):
    """
    檢查日資料檔案的缺失情況
    """
    missing_files = []
    
    if not dates:
        # 使用UTC時區調整的日期範圍
        start_date, end_date = get_utc_date_range(start_date, end_date)
        
        current_date = start_date
        date_list = []
        while current_date <= end_date:
            date_list.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        dates = date_list
        
        print(f"   📅 日期範圍: {start_date} 到 {end_date} (UTC調整後)")
    
    if intervals:  # K線類型資料
        for interval in intervals:
            missing_dates = []
            path = get_path(trading_type, market_data_type, "daily", symbol, interval)
            full_path = get_destination_dir(path, folder)
            base_dir = os.path.dirname(full_path)
            
            for date_str in dates:
                expected_files = [
                    f"{symbol.upper()}-{interval}-{date_str}.zip",
                    f"{symbol.upper()}-{interval}-{date_str}.csv",
                    f"{symbol.upper()}-{interval}-{date_str}.parquet",
                    f"{symbol.upper()}-{interval}-{date_str}.feather",
                    f"{symbol.upper()}-{interval}-{date_str}.h5"
                ]
                
                if not any(os.path.exists(os.path.join(base_dir, f)) for f in expected_files):
                    missing_dates.append(date_str)
            
            if missing_dates:
                print(f"   ⏰ {interval}: 缺失 {len(missing_dates)} 天 (總共 {len(dates)} 天)")
                missing_files.extend([(symbol, interval, date_str) for date_str in missing_dates])
            else:
                print(f"   ✅ {interval}: 完整 ({len(dates)} 天)")
    else:  # 非K線類型資料
        missing_dates = []
        path = get_path(trading_type, market_data_type, "daily", symbol)
        full_path = get_destination_dir(path, folder)
        base_dir = os.path.dirname(full_path)
        
        for date_str in dates:
            expected_files = [
                f"{symbol.upper()}-{market_data_type}-{date_str}.zip",
                f"{symbol.upper()}-{market_data_type}-{date_str}.csv",
                f"{symbol.upper()}-{market_data_type}-{date_str}.parquet",
                f"{symbol.upper()}-{market_data_type}-{date_str}.feather",
                f"{symbol.upper()}-{market_data_type}-{date_str}.h5"
            ]
            
            if not any(os.path.exists(os.path.join(base_dir, f)) for f in expected_files):
                missing_dates.append(date_str)
        
        if missing_dates:
            print(f"   ⏰ 缺失 {len(missing_dates)} 天 (總共 {len(dates)} 天)")
            missing_files.extend([(symbol, None, date_str) for date_str in missing_dates])
        else:
            print(f"   ✅ 完整 ({len(dates)} 天)")
    
    return missing_files


def check_monthly_files(trading_type, market_data_type, symbol, intervals, years, months, start_date, end_date, folder):
    """
    檢查月資料檔案的缺失情況
    """
    missing_files = []
    
    # 生成年月組合
    year_month_list = []
    for year in years:
        for month in months:
            year_month_list.append((year, month))
    
    # 過濾日期範圍（使用UTC調整）
    if start_date or end_date:
        start_date, end_date = get_utc_date_range(start_date, end_date)
        print(f"   📅 月資料範圍: {start_date} 到 {end_date} (UTC調整後)")
        
        filtered_list = []
        for year, month in year_month_list:
            month_date = date(int(year), month, 1)
            if start_date <= month_date <= end_date:
                filtered_list.append((year, month))
        year_month_list = filtered_list
    
    if intervals:  # K線類型資料
        for interval in intervals:
            missing_months = []
            path = get_path(trading_type, market_data_type, "monthly", symbol, interval)
            full_path = get_destination_dir(path, folder)
            base_dir = os.path.dirname(full_path)
            
            for year, month in year_month_list:
                expected_files = [
                    f"{symbol.upper()}-{interval}-{year}-{month:02d}.zip",
                    f"{symbol.upper()}-{interval}-{year}-{month:02d}.csv",
                    f"{symbol.upper()}-{interval}-{year}-{month:02d}.parquet",
                    f"{symbol.upper()}-{interval}-{year}-{month:02d}.feather",
                    f"{symbol.upper()}-{interval}-{year}-{month:02d}.h5"
                ]
                
                if not any(os.path.exists(os.path.join(base_dir, f)) for f in expected_files):
                    missing_months.append(f"{year}-{month:02d}")
            
            if missing_months:
                print(f"   ⏰ {interval}: 缺失 {len(missing_months)} 個月 (總共 {len(year_month_list)} 個月)")
                missing_files.extend([(symbol, interval, year, month) for year, month in year_month_list if f"{year}-{month:02d}" in missing_months])
            else:
                print(f"   ✅ {interval}: 完整 ({len(year_month_list)} 個月)")
    else:  # 非K線類型資料
        missing_months = []
        path = get_path(trading_type, market_data_type, "monthly", symbol)
        full_path = get_destination_dir(path, folder)
        base_dir = os.path.dirname(full_path)
        
        for year, month in year_month_list:
            expected_files = [
                f"{symbol.upper()}-{market_data_type}-{year}-{month:02d}.zip",
                f"{symbol.upper()}-{market_data_type}-{year}-{month:02d}.csv",
                f"{symbol.upper()}-{market_data_type}-{year}-{month:02d}.parquet",
                f"{symbol.upper()}-{market_data_type}-{year}-{month:02d}.feather",
                f"{symbol.upper()}-{market_data_type}-{year}-{month:02d}.h5"
            ]
            
            if not any(os.path.exists(os.path.join(base_dir, f)) for f in expected_files):
                missing_months.append(f"{year}-{month:02d}")
        
        if missing_months:
            print(f"   ⏰ 缺失 {len(missing_months)} 個月 (總共 {len(year_month_list)} 個月)")
            missing_files.extend([(symbol, None, year, month) for year, month in year_month_list if f"{year}-{month:02d}" in missing_months])
        else:
            print(f"   ✅ 完整 ({len(year_month_list)} 個月)")
    
    return missing_files


def download_missing_files(trading_type, market_data_type, missing_files, time_period, folder, checksum, data_format=".zip", timeout=300):
    """
    下載缺失的檔案
    """
    if not missing_files:
        return
    
    total_missing = len(missing_files)
    print(f"\n📥 開始下載 {total_missing} 個缺失檔案...")
    success_count = 0
    skip_count = 0
    
    for i, missing_item in enumerate(missing_files):
        print(f"\n[{i+1}/{total_missing}] 正在處理...")
        
        if time_period == "daily":
            if len(missing_item) == 3:  # (symbol, interval, date_str) for klines
                symbol, interval, date_str = missing_item
                path = get_path(trading_type, market_data_type, "daily", symbol, interval)
                file_name = f"{symbol.upper()}-{interval}-{date_str}.zip"
                print(f"   📅 {symbol} {interval} {date_str}")
            else:  # (symbol, None, date_str) for non-klines
                symbol, _, date_str = missing_item
                path = get_path(trading_type, market_data_type, "daily", symbol)
                file_name = f"{symbol.upper()}-{market_data_type}-{date_str}.zip"
                print(f"   📅 {symbol} {market_data_type} {date_str}")
                
        elif time_period == "monthly":
            if len(missing_item) == 4:  # (symbol, interval, year, month) for klines
                symbol, interval, year, month = missing_item
                path = get_path(trading_type, market_data_type, "monthly", symbol, interval)
                file_name = f"{symbol.upper()}-{interval}-{year}-{month:02d}.zip"
                print(f"   📅 {symbol} {interval} {year}-{month:02d}")
            else:  # (symbol, None, year, month) for non-klines
                symbol, _, year, month = missing_item
                path = get_path(trading_type, market_data_type, "monthly", symbol)
                file_name = f"{symbol.upper()}-{market_data_type}-{year}-{month:02d}.zip"
                print(f"   📅 {symbol} {market_data_type} {year}-{month:02d}")
        
        # 下載檔案（帶超時設定）
        success = download_file(path, file_name, None, folder, data_format, timeout)
        if success is not False:  # None 或 True 都表示成功
            success_count += 1
            
            # 下載 checksum 檔案
            if checksum == 1:
                checksum_file_name = file_name + ".CHECKSUM"
                download_file(path, checksum_file_name, None, folder, ".zip", timeout)
        else:
            skip_count += 1
    
    print(f"\n📊 下載結果:")
    print(f"   ✅ 成功: {success_count}/{total_missing}")
    if skip_count > 0:
        print(f"   ⚠️ 跳過: {skip_count} 個檔案（超時或錯誤）")


def convert_zip_to_format(zip_path, target_format):
    """
    Convert ZIP file (containing CSV) to specified format
    Always use custom column names regardless of existing headers
    """
    if target_format == ".zip":
        return  # No conversion needed

    try:
        # Extract the CSV from ZIP
        with zipfile.ZipFile(zip_path, "r") as zip_file:
            csv_files = [f for f in zip_file.namelist() if f.endswith(".csv")]
            if not csv_files:
                print(f"No CSV file found in {zip_path}")
                return

            csv_filename = csv_files[0]

            # Extract CSV to temporary location first
            temp_csv_path = zip_path.replace(".zip", "_temp.csv")
            zip_file.extract(csv_filename, os.path.dirname(zip_path))
            extracted_csv_path = os.path.join(os.path.dirname(zip_path), csv_filename)

            # Move to temporary name to avoid conflicts
            os.rename(extracted_csv_path, temp_csv_path)

        # Define column names based on data type
        def get_column_names(zip_path):
            """Get appropriate column names based on data type"""
            if "klines" in zip_path or "Klines" in zip_path:
                return [
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
                    "ignore",
                ]
            elif "aggTrades" in zip_path:
                return [
                    "agg_trade_id",
                    "price",
                    "quantity",
                    "first_trade_id",
                    "last_trade_id",
                    "timestamp",
                    "is_buyer_maker",
                    "ignore",
                ]
            elif "trades" in zip_path:
                return [
                    "trade_id",
                    "price",
                    "quantity",
                    "quote_quantity",
                    "timestamp",
                    "is_buyer_maker",
                    "ignore",
                ]
            elif "bookDepth" in zip_path:
                return [
                    "timestamp", "percentage", "depth", "notional"
                ]
            elif "bookTicker" in zip_path:
                return [
                    "update_id", "best_bid_price", "best_bid_qty", "best_ask_price", 
                    "best_ask_qty", "transaction_time", "event_time"
                ]
            elif "metrics" in zip_path:
                return [
                    "create_time", "symbol", "sum_open_interest", "sum_open_interest_value",
                    "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio",
                    "count_long_short_ratio", "sum_taker_long_short_vol_ratio"
                ]
            elif "fundingRate" in zip_path:
                return [
                    "calc_time", "funding_interval_hours", "last_funding_rate"
                ]
            else:
                return None

        # Get column names for this data type
        column_names = get_column_names(zip_path)

        # Always use our custom column names if available
        if column_names:
            # First, check if the file has a header row by examining the first row
            first_row = pd.read_csv(temp_csv_path, nrows=1, header=None).iloc[0]
            first_value = str(first_row.iloc[0])

            # Determine if first row looks like data or headers
            looks_like_data = (
                first_value.isdigit()
                and len(first_value) >= 10  # Timestamp
                or first_value.replace(".", "").replace("-", "").isdigit()  # Number
                or first_value.lower() in ["true", "false"]  # Boolean
            )

            if looks_like_data:
                # First row is data, read all rows with our column names
                df = pd.read_csv(temp_csv_path, header=None, names=column_names)
                print(f"Applied custom column names (no header): {column_names[:3]}...")
            else:
                # First row looks like headers, skip it and use our column names
                df = pd.read_csv(
                    temp_csv_path, header=None, names=column_names, skiprows=1
                )
                print(
                    f"Replaced existing headers with custom names: {column_names[:3]}..."
                )
        else:
            # Unknown data type, read normally
            df = pd.read_csv(temp_csv_path)
            print(f"Unknown data type, using original format")

        # Generate new filename with target format
        base_name = os.path.splitext(zip_path)[0]
        new_filename = base_name + target_format

        # Save in the specified format
        if target_format == ".csv":
            df.to_csv(new_filename, index=False)
        elif target_format == ".parquet":
            df.to_parquet(new_filename, index=False)
        elif target_format == ".feather":
            df.to_feather(new_filename)
        elif target_format == ".h5":
            df.to_hdf(new_filename, key="data", mode="w", index=False)

        print(f"Converted {zip_path} to {new_filename}")

        # Clean up temporary files
        try:
            # Remove temporary CSV file
            if os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)
                print(f"Cleaned up temporary file: {temp_csv_path}")
            
            # Small delay to ensure file handles are released
            time.sleep(0.2)
            
            # Remove original ZIP file
            if os.path.exists(zip_path):
                os.remove(zip_path)
                print(f"Removed original ZIP file: {zip_path}")
            
        except PermissionError as pe:
            print(f"Permission error cleaning up files: {pe}")
            print(f"Please manually delete: {temp_csv_path} and {zip_path}")
        except Exception as cleanup_error:
            print(f"Warning: Could not clean up temporary files: {cleanup_error}")
            if os.path.exists(temp_csv_path):
                print(f"Please manually delete: {temp_csv_path}")
            if os.path.exists(zip_path):
                print(f"Please manually delete: {zip_path}")

    except Exception as e:
        print(f"Error converting {zip_path}: {str(e)}")
        # Clean up any temporary files on error
        temp_csv_path = zip_path.replace(".zip", "_temp.csv")
        if os.path.exists(temp_csv_path):
            try:
                os.remove(temp_csv_path)
            except:
                pass


def get_utc_date_range(start_date=None, end_date=None):
    """
    獲取調整後的UTC日期範圍，避免下載不存在的未來資料
    """
    # 獲取當前UTC時間
    utc_now = datetime.now(timezone.utc)
    # 幣安資料通常有1-2小時延遲，所以減去2小時作為安全邊界
    safe_end_date = (utc_now - timedelta(hours=2)).date()
    
    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date) if isinstance(start_date, str) else start_date
    
    if not end_date:
        end_date = safe_end_date
    else:
        end_date = convert_to_date_object(end_date) if isinstance(end_date, str) else end_date
        # 確保結束日期不超過安全日期
        if end_date > safe_end_date:
            print(f"⚠️ 結束日期 {end_date} 超過UTC當前日期 {safe_end_date}，自動調整為 {safe_end_date}")
            end_date = safe_end_date
    
    return start_date, end_date


class TimeoutError(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutError("Download timeout")


def download_file(
    base_path, file_name, date_range=None, folder=None, data_format=".zip", timeout=300
):
    download_path = "{}{}".format(base_path, file_name)
    if folder:
        base_path = os.path.join(folder, base_path)
    if date_range:
        date_range = date_range.replace(" ", "_")
        base_path = os.path.join(base_path, date_range)
    save_path = get_destination_dir(os.path.join(base_path, file_name), folder)

    # 檢查最終格式的檔案是否存在
    if data_format != ".zip":
        # 如果要轉換格式，檢查目標格式檔案是否已存在
        base_name = os.path.splitext(save_path)[0]
        final_path = base_name + data_format
        if os.path.exists(final_path):
            print("\nfinal format file already exists! {}".format(final_path))
            return True  # 檔案已存在，返回成功
    else:
        # 如果是 ZIP 格式，檢查 ZIP 檔案是否存在
        if os.path.exists(save_path):
            print("\nfile already exists! {}".format(save_path))
            return True  # 檔案已存在，返回成功

    # make the directory
    save_dir = os.path.dirname(save_path)
    if not os.path.exists(save_dir):
        Path(save_dir).mkdir(parents=True, exist_ok=True)

    # 設定超時處理
    if hasattr(signal, 'SIGALRM'):  # Unix/Linux/Mac
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)
    
    try:
        download_url = get_download_url(download_path)
        print(f"\n💾 正在下載: {file_name}")
        print(f"   URL: {download_url}")
        
        try:
            dl_file = urllib.request.urlopen(download_url, timeout=60)  # 60秒連線超時
        except Exception as e:
            print(f"   ❗ 連線失敗: {str(e)}")
            if "404" in str(e) or "Not Found" in str(e):
                print(f"   🙅 檔案不存在，跳過: {file_name}")
                return False  # 檔案不存在
            else:
                print(f"   🙅 連線錯誤，跳過: {file_name}")
                return False  # 連線錯誤
        
        length = dl_file.getheader("content-length")
        if length:
            length = int(length)
            blocksize = max(4096, length // 100)
        else:
            blocksize = 8192
            print(f"   ⚠️ 無法獲取檔案大小，使用預設區塊大小")

        start_time = time.time()
        with open(save_path, "wb") as out_file:
            dl_progress = 0
            last_progress_time = start_time
            
            while True:
                # 檢查是否超時（無進度超過30秒）
                current_time = time.time()
                if current_time - last_progress_time > 30:
                    print(f"\n   ⚠️ 下載停滞超過30秒，跳過: {file_name}")
                    return False  # 下載停滞
                
                try:
                    buf = dl_file.read(blocksize)
                except Exception as e:
                    print(f"\n   ❗ 讀取數據錯誤: {str(e)}")
                    print(f"   🙅 跳過: {file_name}")
                    return False  # 讀取錯誤
                    
                if not buf:
                    break
                    
                dl_progress += len(buf)
                out_file.write(buf)
                last_progress_time = current_time
                
                if length:
                    done = int(50 * dl_progress / length)
                    percent = int(100 * dl_progress / length)
                    sys.stdout.write(f"\r   [{('#' * done).ljust(50, '.')}] {percent}%")
                else:
                    # 無法顯示百分比時，顯示已下載大小
                    sys.stdout.write(f"\r   已下載: {dl_progress // 1024} KB")
                sys.stdout.flush()

        elapsed_time = time.time() - start_time
        file_size = dl_progress // 1024 if dl_progress else 0
        print(f"\n   ✅ 下載完成: {file_size} KB ({elapsed_time:.1f}秒)")

        # Convert format if needed
        if data_format != ".zip":
            print(f"   🔄 轉換為 {data_format} 格式...")
            convert_zip_to_format(save_path, data_format)
        
        return True  # 下載成功

    except TimeoutError:
        print(f"\n   ⚠️ 下載超時 ({timeout}秒)，跳過: {file_name}")
        # 清理未完成的檔案
        if os.path.exists(save_path):
            try:
                os.remove(save_path)
            except:
                pass
        return False  # 下載超時
        
    except Exception as e:
        print(f"\n   ❗ 下載錯誤: {str(e)}")
        print(f"   🙅 跳過: {file_name}")
        # 清理未完成的檔案
        if os.path.exists(save_path):
            try:
                os.remove(save_path)
            except:
                pass
        return False  # 下載錯誤
    finally:
        # 取消超時設定
        if hasattr(signal, 'SIGALRM'):
            signal.alarm(0)


def convert_to_date_object(d):
    year, month, day = [int(x) for x in d.split("-")]
    date_obj = date(year, month, day)
    return date_obj


def get_start_end_date_objects(date_range):
    start, end = date_range.split()
    start_date = convert_to_date_object(start)
    end_date = convert_to_date_object(end)
    return start_date, end_date


def match_date_regex(arg_value, pat=re.compile(r"\d{4}-\d{2}-\d{2}")):
    if not pat.match(arg_value):
        raise ArgumentTypeError
    return arg_value


def check_directory(arg_value):
    if os.path.exists(arg_value):
        while True:
            option = input("Folder already exists! Do you want to overwrite it? y/n  ")
            if option != "y" and option != "n":
                print("Invalid Option!")
                continue
            elif option == "y":
                shutil.rmtree(arg_value)
                break
            else:
                break
    return arg_value


def raise_arg_error(msg):
    raise ArgumentTypeError(msg)


def get_path(trading_type, market_data_type, time_period, symbol, interval=None):
    trading_type_path = "data/spot"
    if trading_type != "spot":
        trading_type_path = f"data/futures/{trading_type}"
    if interval is not None:
        path = f"{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/{interval}/"
    else:
        path = f"{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/"
    return path


def get_parser(parser_type):
    parser = ArgumentParser(
        description=("This is a script to download historical {} data").format(
            parser_type
        ),
        formatter_class=RawTextHelpFormatter,
    )
    parser.add_argument(
        "-s",
        dest="symbols",
        nargs="+",
        help="Single symbol or multiple symbols separated by space",
    )
    parser.add_argument(
        "-y",
        dest="years",
        default=YEARS,
        nargs="+",
        choices=YEARS,
        help="Single year or multiple years separated by space\n-y 2019 2021 means to download {} from 2019 and 2021".format(
            parser_type
        ),
    )
    parser.add_argument(
        "-m",
        dest="months",
        default=MONTHS,
        nargs="+",
        type=int,
        choices=MONTHS,
        help="Single month or multiple months separated by space\n-m 2 12 means to download {} from feb and dec".format(
            parser_type
        ),
    )
    parser.add_argument(
        "-d",
        dest="dates",
        nargs="+",
        type=match_date_regex,
        help="Date to download in [YYYY-MM-DD] format\nsingle date or multiple dates separated by space\ndownload from 2020-01-01 if no argument is parsed",
    )
    parser.add_argument(
        "-startDate",
        dest="startDate",
        type=match_date_regex,
        help="Starting date to download in [YYYY-MM-DD] format",
    )
    parser.add_argument(
        "-endDate",
        dest="endDate",
        type=match_date_regex,
        help="Ending date to download in [YYYY-MM-DD] format",
    )
    parser.add_argument(
        "-folder",
        dest="folder",
        type=check_directory,
        help="Directory to store the downloaded data",
    )
    parser.add_argument(
        "-skip-monthly",
        dest="skip_monthly",
        default=0,
        type=int,
        choices=[0, 1],
        help="1 to skip downloading of monthly data, default 0",
    )
    parser.add_argument(
        "-skip-daily",
        dest="skip_daily",
        default=0,
        type=int,
        choices=[0, 1],
        help="1 to skip downloading of daily data, default 0",
    )
    parser.add_argument(
        "-c",
        dest="checksum",
        default=0,
        type=int,
        choices=[0, 1],
        help="1 to download checksum file, default 0",
    )
    parser.add_argument(
        "-t",
        dest="type",
        required=True,
        choices=TRADING_TYPE,
        help="Valid trading types: {}".format(TRADING_TYPE),
    )
    parser.add_argument(
        "-f",
        "--format",
        dest="data_format",
        default=".zip",
        choices=DATA_FORMATS,
        help="Data format to save (default: .zip)\n.zip - Original ZIP format\n.csv - Extract and save as CSV\n.parquet - Convert to Parquet format\n.feather - Convert to Feather format\n.h5 - Convert to HDF5 format",
    )

    if parser_type in ["klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"]:
        parser.add_argument(
            "-i",
            dest="intervals",
            default=INTERVALS,
            nargs="+",
            choices=INTERVALS,
            help="single kline interval or multiple intervals separated by space\n-i 1m 1w means to download klines interval of 1minute and 1week",
        )

    return parser
