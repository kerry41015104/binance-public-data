#!/usr/bin/env python

"""
  script to download markPriceKlines.
  set the absoluate path destination folder for STORE_DIRECTORY, and run

  e.g. STORE_DIRECTORY=/data/ ./download-kline.py

"""
import sys
from datetime import date, datetime

import pandas as pd

from enums import START_DATE, END_DATE, DAILY_INTERVALS, PERIOD_START_DATE
from utility import download_file, get_all_symbols, get_parser, convert_to_date_object, \
    get_path, raise_arg_error, check_existing_files


def download_monthly_markPriceKlines(trading_type, symbols, num_symbols, intervals, years, months, start_date,
                                      end_date, folder, checksum, data_format=".zip"):
    current = 0
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    print("Found {} symbols".format(num_symbols))

    for symbol in symbols:
        print("[{}/{}] - start download monthly {} klines ".format(current + 1, num_symbols, symbol))
        for interval in intervals:
            for year in years:
                for month in months:
                    current_date = convert_to_date_object('{}-{}-01'.format(year, month))
                    if start_date <= current_date <= end_date:
                        path = get_path(trading_type, "markPriceKlines", "monthly", symbol, interval)
                        file_name = "{}-{}-{}-{}.zip".format(symbol.upper(), interval, year, '{:02d}'.format(month))
                        download_file(path, file_name, date_range, folder, data_format)

                        if checksum == 1:
                            checksum_path = get_path(trading_type, "markPriceKlines", "monthly", symbol, interval)
                            checksum_file_name = "{}-{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, year,
                                                                                   '{:02d}'.format(month))
                            download_file(checksum_path, checksum_file_name, date_range, folder, ".zip")

        current += 1


def download_daily_markPriceKlines(trading_type, symbols, num_symbols, intervals, dates, start_date, end_date, folder,
                                    checksum, data_format=".zip"):
    current = 0
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    # Get valid intervals for daily
    intervals = list(set(intervals) & set(DAILY_INTERVALS))
    print("Found {} symbols".format(num_symbols))

    for symbol in symbols:
        print("[{}/{}] - start download daily {} klines ".format(current + 1, num_symbols, symbol))
        for interval in intervals:
            for date in dates:
                current_date = convert_to_date_object(date)
                if start_date <= current_date <= end_date:
                    path = get_path(trading_type, "markPriceKlines", "daily", symbol, interval)
                    file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, date)
                    download_file(path, file_name, date_range, folder, data_format)

                    if checksum == 1:
                        checksum_path = get_path(trading_type, "markPriceKlines", "daily", symbol, interval)
                        checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, date)
                        download_file(checksum_path, checksum_file_name, date_range, folder, ".zip")

        current += 1


if __name__ == "__main__":
    parser = get_parser('markPriceKlines')
    args = parser.parse_args(sys.argv[1:])

    if args.type not in ["um", "cm"]:
        raise_arg_error("MarkPriceKlines is only for the um or cm trading type")

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
        period = convert_to_date_object(datetime.today().strftime('%Y-%m-%d')) - convert_to_date_object(
            PERIOD_START_DATE)
        dates = pd.date_range(end=datetime.today(), periods=period.days + 1).to_pydatetime().tolist()
        dates = [date.strftime("%Y-%m-%d") for date in dates]
    
    # Check existing files before downloading
    print("\n=== 📊 MARK PRICE KLINES 資料檢查 ===")
    
    if args.skip_monthly == 0:
        print("\n🗓️ 檢查月資料...")
        need_monthly = check_existing_files(
            args.type, "markPriceKlines", symbols, "monthly", 
            args.intervals, args.years, args.months, 
            None, args.startDate, args.endDate, args.folder
        )
        if need_monthly:
            download_monthly_markPriceKlines(args.type, symbols, num_symbols, args.intervals, args.years,
                                              args.months, args.startDate, args.endDate, args.folder, args.checksum, args.data_format)
        else:
            print("✅ 月資料已完整，跳過下載")
            
    if args.skip_daily == 0:
        print("\n📅 檢查日資料...")
        need_daily = check_existing_files(
            args.type, "markPriceKlines", symbols, "daily", 
            args.intervals, None, None, 
            dates, args.startDate, args.endDate, args.folder
        )
        if need_daily:
            download_daily_markPriceKlines(args.type, symbols, num_symbols, args.intervals, dates, args.startDate,
                                            args.endDate, args.folder, args.checksum, args.data_format)
        else:
            print("✅ 日資料已完整，跳過下載")
