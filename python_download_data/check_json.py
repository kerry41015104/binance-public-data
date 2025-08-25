import os
import json
from datetime import datetime, timedelta

base_dir = r"D:\code\Trading-Universe\crypto-data-overall\binance-public-data\data\futures\um\daily\klines"


def check_json_files():
    has_json = []
    no_json = []

    # 遍歷標的資料夾
    for symbol in os.listdir(base_dir):
        symbol_path = os.path.join(base_dir, symbol)
        if not os.path.isdir(symbol_path):
            continue

        found_json = False

        # 遍歷時間資料夾 (ex: 1m, 5m...)
        for timeframe in os.listdir(symbol_path):
            tf_path = os.path.join(symbol_path, timeframe)
            if not os.path.isdir(tf_path):
                continue

            # 檢查是否有 .json 檔
            if any(f.endswith(".json") for f in os.listdir(tf_path)):
                found_json = True
                break

        if found_json:
            has_json.append(symbol)
        else:
            no_json.append(symbol)

    print(f"有 .json 的標的數量: {len(has_json)}")
    print(f"沒有 .json 的標的數量: {len(no_json)}")

    if no_json:
        print("\n沒有 .json 的標的:")
        for s in no_json:
            print(s)


import os
import json
from datetime import datetime


def edit_json_files(
    base_dir,
    mode="both",  # "with_json", "without_json", "both"
    symbols=None,  # 預設處理全部標的
    intervals=None,  # 預設處理全部時間
):
    modified = []
    created = []
    unchanged = []

    for symbol in os.listdir(base_dir):
        symbol_path = os.path.join(base_dir, symbol)
        if not os.path.isdir(symbol_path):
            continue
        if symbols and symbol not in symbols:
            continue

        for interval in os.listdir(symbol_path):
            interval_path = os.path.join(symbol_path, interval)
            if not os.path.isdir(interval_path):
                continue
            if intervals and interval not in intervals:
                continue

            # 找現有的 json 檔
            json_files = [
                f for f in os.listdir(interval_path) if f.endswith("_status.json")
            ]

            if json_files and mode in ["with_json", "both"]:
                json_path = os.path.join(interval_path, json_files[0])
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # 🛡️ 防呆：檢查 earliest_date / latest_date 是否存在
                earliest_date_str = data.get("earliest_date")
                latest_date_str = data.get("latest_date")

                if not earliest_date_str or not latest_date_str:
                    print(
                        f"[跳過] {symbol}/{interval} 缺少 earliest_date 或 latest_date"
                    )
                    unchanged.append(f"{symbol}/{interval}")
                    continue

                try:
                    earliest_date = datetime.strptime(
                        earliest_date_str, "%Y-%m-%d"
                    ).date()
                    latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
                except Exception as e:
                    print(f"[錯誤] {symbol}/{interval} 日期解析失敗: {e}")
                    unchanged.append(f"{symbol}/{interval}")
                    continue

                # 保留 earliest_date <= d <= latest_date 的 failed_dates
                new_failed_dates = [
                    d
                    for d in data.get("failed_dates", [])
                    if d
                    and earliest_date
                    <= datetime.strptime(d, "%Y-%m-%d").date()
                    <= latest_date
                ]

                if new_failed_dates != data.get("failed_dates", []):
                    data["failed_dates"] = new_failed_dates
                    data["last_updated"] = datetime.utcnow().isoformat()
                    with open(json_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    modified.append(f"{symbol}/{interval}")
                else:
                    unchanged.append(f"{symbol}/{interval}")

            elif not json_files and mode in ["without_json", "both"]:
                parquet_files = [
                    f for f in os.listdir(interval_path) if f.endswith(".parquet")
                ]
                if parquet_files:
                    dates = []
                    for f in parquet_files:
                        try:
                            parts = f.replace(".parquet", "").split("-")
                            date_str = "-".join(parts[-3:])  # YYYY-MM-DD
                            dates.append(datetime.strptime(date_str, "%Y-%m-%d").date())
                        except Exception as e:
                            print(f"[檔名解析失敗] {f}, error={e}")
                            continue

                    if dates:
                        earliest_date = min(dates)
                        latest_date = max(dates)

                        new_json = {
                            "symbol": symbol,
                            "interval": interval,
                            "earliest_date": earliest_date.strftime("%Y-%m-%d"),
                            "latest_date": latest_date.strftime("%Y-%m-%d"),
                            "last_download_date": latest_date.strftime("%Y-%m-%d"),
                            "failed_dates": [],
                            "conversion_failed_dates": [],
                            "total_downloaded": len(parquet_files),
                            "last_updated": datetime.utcnow().isoformat(),
                            "trading_type": "um",
                        }

                        json_path = os.path.join(
                            interval_path, f"{symbol}_{interval}_status.json"
                        )
                        with open(json_path, "w", encoding="utf-8") as f:
                            json.dump(new_json, f, indent=2, ensure_ascii=False)

                        created.append(f"{symbol}/{interval}")
                    else:
                        unchanged.append(f"{symbol}/{interval}")
                else:
                    unchanged.append(f"{symbol}/{interval}")

    # 統計結果輸出
    report = {"modified": modified, "created": created, "unchanged": unchanged}

    report_path = os.path.join(base_dir, "json_edit_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(
        f"完成！修改: {len(modified)}, 新增: {len(created)}, 無異動: {len(unchanged)}"
    )
    print(f"報告已輸出到 {report_path}")


def find_json_with_empty_earliest(base_dir, symbols=None, intervals=None):
    """
    尋找有 JSON 但 earliest_date 是空值的標的
    """
    problem_list = []

    for symbol in os.listdir(base_dir):
        symbol_path = os.path.join(base_dir, symbol)
        if not os.path.isdir(symbol_path):
            continue
        if symbols and symbol not in symbols:
            continue

        for interval in os.listdir(symbol_path):
            interval_path = os.path.join(symbol_path, interval)
            if not os.path.isdir(interval_path):
                continue
            if intervals and interval not in intervals:
                continue

            json_files = [
                f for f in os.listdir(interval_path) if f.endswith("_status.json")
            ]
            if not json_files:
                continue

            json_path = os.path.join(interval_path, json_files[0])
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                earliest_date = data.get("earliest_date")
                if not earliest_date or earliest_date.strip() == "":
                    problem_list.append(f"{symbol}/{interval}")
            except Exception as e:
                print(f"[錯誤] 無法讀取 {json_path}: {e}")
                continue

    print(f"找到 {len(problem_list)} 個 earliest_date 空值的標的")
    for item in problem_list:
        print(item)

    return problem_list


def remove_duplicate_failed_dates(base_dir: str):
    """
    掃描 base_dir 下所有 JSON 檔案，去除 failed_dates 的重複項。
    """
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)

                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    if "failed_dates" in data and isinstance(
                        data["failed_dates"], list
                    ):
                        original_len = len(data["failed_dates"])
                        # 保持順序但去重
                        unique_failed_dates = list(dict.fromkeys(data["failed_dates"]))
                        if len(unique_failed_dates) != original_len:
                            data["failed_dates"] = unique_failed_dates
                            with open(file_path, "w", encoding="utf-8") as f:
                                json.dump(data, f, ensure_ascii=False, indent=2)
                            print(
                                f"✅ 更新 {file_path}：去除 {original_len - len(unique_failed_dates)} 個重複日期"
                            )
                        else:
                            print(f"無異動 {file_path}")

                except Exception as e:
                    print(f"處理 {file_path} 時發生錯誤: {e}")


if __name__ == "__main__":
    # check_json_files()
    edit_json_files(base_dir, mode="with_json")
    # find_json_with_empty_earliest(base_dir)
    # remove_duplicate_failed_dates(base_dir)
