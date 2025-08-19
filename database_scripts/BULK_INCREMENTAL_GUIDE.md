# 批量增量更新功能使用指南

## 概述

新增的批量增量更新功能能夠自動掃描指定目錄下的所有交易對標的，並根據目錄結構自動推斷參數（交易類型、資料類型、時間間隔），然後對所有標的進行增量更新。

## 功能特色

1. **自動目錄解析**: 根據目錄結構自動推斷交易類型、資料類型和時間間隔
2. **批量處理**: 一次性更新目錄下所有標的
3. **並行處理**: 支援多線程並行更新，提高效率
4. **進度追蹤**: 實時顯示更新進度和結果統計
5. **詳細日誌**: 完整的操作日誌和錯誤記錄

## 新增的命令選項

### 1. 批量增量更新 (`bulk-incremental`)

```bash
python import_data.py --action bulk-incremental --directory <目錄路徑> [選項]
```

**參數說明:**
- `--directory`: 必須，指定包含資料的目錄路徑
- `--days-back`: 可選，回溯天數 (預設: 7)
- `--max-workers`: 可選，並行線程數 (預設: 2)

**使用範例:**
```bash
# 更新所有 klines 資料，回溯 7 天
python import_data.py --action bulk-incremental --directory "D:/crypto-data-overall/binance-public-data/data/futures/um/daily/klines"

# 更新所有資料，回溯 14 天，使用 4 個並行線程
python import_data.py --action bulk-incremental --directory "D:/crypto-data-overall/binance-public-data/data/futures/um/daily/klines" --days-back 14 --max-workers 4
```

### 2. 測試目錄解析 (`test-parse`)

```bash
python import_data.py --action test-parse --directory <目錄路徑>
```

**功能:**
- 只測試目錄解析，不執行實際更新
- 顯示解析出的交易類型、資料類型、標的列表和時間間隔
- 預估任務數量

**使用範例:**
```bash
python import_data.py --action test-parse --directory "D:/crypto-data-overall/binance-public-data/data/futures/um/daily/klines"
```

## 支援的目錄結構

### 標準目錄結構範例:
```
binance-public-data/data/
├── futures/
│   ├── um/                    # 交易類型: um
│   │   ├── daily/
│   │   │   ├── klines/        # 資料類型: klines
│   │   │   │   ├── BTCUSDT/   # 標的
│   │   │   │   │   ├── 1m/    # 時間間隔
│   │   │   │   │   ├── 5m/
│   │   │   │   │   └── 1h/
│   │   │   │   ├── ETHUSDT/
│   │   │   │   └── ...
│   │   │   ├── trades/        # 其他資料類型
│   │   │   └── ...
│   │   └── cm/                # 其他交易類型
│   └── spot/
└── ...
```

### 自動推斷規則:

1. **交易類型推斷:**
   - 路徑包含 `spot` → `spot`
   - 路徑包含 `um` → `um` (USD-M futures)
   - 路徑包含 `cm` → `cm` (Coin-M futures)

2. **資料類型推斷:**
   - 取目標目錄名稱作為資料類型 (如 `klines`, `trades`, `aggTrades` 等)

3. **標的識別:**
   - 目錄名包含 `USDT`, `BTC`, `ETH`, `BNB`, `USD`, `BUSD` 等關鍵字

4. **時間間隔識別:**
   - 支援 `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1M`

## 輸出示例

### 測試解析輸出:
```
測試解析目錄: D:/crypto-data-overall/binance-public-data/data/futures/um/daily/klines
解析路徑: D:\crypto-data-overall\binance-public-data\data\futures\um\daily\klines
交易類型: um
資料類型: klines
找到 150 個標的
標的樣例: ['1000LUNCUSDT', '1000PEPEUSDT', '1000SHIBUSDT', '1INCHUSDT', 'AAVEUSDT']
時間間隔: ['1d', '1h', '1m', '3d', '3m', '5m']

✓ 目錄解析成功
  交易類型: um
  資料類型: klines
  標的數量: 150
  標的樣例: ['1000LUNCUSDT', '1000PEPEUSDT', '1000SHIBUSDT', '1INCHUSDT', 'AAVEUSDT', '1000XECUSDT', 'AAVEUSDT', 'ACHUSDT', 'ADAUSDT', 'AGIXUSDT']
  ... 及其他 140 個標的
  時間間隔: ['1d', '1h', '1m', '3d', '3m', '5m']
  預計任務數: 900
```

### 批量更新輸出:
```
開始批量增量更新目錄: D:/crypto-data-overall/binance-public-data/data/futures/um/daily/klines
回溯天數: 7
並行線程數: 2

準備更新 150 個標的，6 個間隔，總共 900 個任務

✓ 成功更新: BTCUSDT 1m (1/900)
✓ 成功更新: BTCUSDT 5m (2/900)
✗ 更新失敗: ETHUSDT 1m
✓ 成功更新: ETHUSDT 5m (3/900)
...

批量更新完成:
  成功: 850
  失敗: 50
  總計: 900
```

## 日誌記錄

所有操作都會記錄詳細日誌，日誌文件位於 `logs/` 目錄下:
- `bulk_incremental_YYYYMMDD_HHMMSS.log`: 批量更新日誌
- 包含每個標的的更新狀態和錯誤信息

## 注意事項

1. **目錄路徑**: 確保指定的目錄路徑正確且可存取
2. **資料庫連接**: 確保資料庫配置正確且可連接
3. **並行線程**: 根據系統性能調整 `--max-workers` 參數
4. **回溯天數**: 根據需要調整 `--days-back` 參數，避免重複處理過多資料
5. **磁碟空間**: 確保有足夠的磁碟空間存儲日誌文件

## 比較: 舊方法 vs 新方法

### 舊方法 (單個標的):
```bash
python import_data.py --action incremental --symbol ADAUSDT --data-type klines --trading-type um --interval 1m --days-back 7
```

### 新方法 (批量更新):
```bash
python import_data.py --action bulk-incremental --directory "D:/crypto-data-overall/binance-public-data/data/futures/um/daily/klines" --days-back 7
```

新方法自動處理目錄下所有標的和時間間隔，大大提高了效率！

## 錯誤排除

### 常見問題:

1. **目錄解析失敗**
   - 檢查目錄路徑是否正確
   - 確認目錄結構符合預期格式

2. **找不到標的**
   - 檢查子目錄命名是否包含正確的標的格式
   - 確認目錄不為空

3. **更新失敗**
   - 檢查資料庫連接
   - 查看日誌文件獲取詳細錯誤信息
   - 確認原始增量更新功能正常工作

4. **權限問題**
   - 確保對目錄有讀取權限
   - 確保對日誌目錄有寫入權限
