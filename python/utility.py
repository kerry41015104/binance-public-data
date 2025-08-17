import os, sys, re, shutil
import json
import zipfile
import pandas as pd
from pathlib import Path
from datetime import date, datetime
import urllib.request
import time
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


def download_file(
    base_path, file_name, date_range=None, folder=None, data_format=".zip"
):
    download_path = "{}{}".format(base_path, file_name)
    if folder:
        base_path = os.path.join(folder, base_path)
    if date_range:
        date_range = date_range.replace(" ", "_")
        base_path = os.path.join(base_path, date_range)
    save_path = get_destination_dir(os.path.join(base_path, file_name), folder)

    if os.path.exists(save_path):
        print("\nfile already exists! {}".format(save_path))
        return

    # make the directory
    save_dir = os.path.dirname(save_path)
    if not os.path.exists(save_dir):
        Path(save_dir).mkdir(parents=True, exist_ok=True)

    try:
        download_url = get_download_url(download_path)
        dl_file = urllib.request.urlopen(download_url)
        length = dl_file.getheader("content-length")
        if length:
            length = int(length)
            blocksize = max(4096, length // 100)

        with open(save_path, "wb") as out_file:
            dl_progress = 0
            print("\nFile Download: {}".format(save_path))
            while True:
                buf = dl_file.read(blocksize)
                if not buf:
                    break
                dl_progress += len(buf)
                out_file.write(buf)
                done = int(50 * dl_progress / length)
                sys.stdout.write("\r[%s%s]" % ("#" * done, "." * (50 - done)))
                sys.stdout.flush()

        print("\n")  # Add newline after download

        # Convert format if needed
        if data_format != ".zip":
            convert_zip_to_format(save_path, data_format)

    except urllib.error.HTTPError:
        print("\nFile not found: {}".format(download_url))
        pass


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
