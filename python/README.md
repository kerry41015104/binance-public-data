# Binance 公開資料下載工具

這是一個增強版的 Binance 公開資料下載工具，支援多種資料格式和完整的期貨資料類型。

## 🚀 主要功能

### 支援的資料格式
- **`.zip`** - 原始 ZIP 格式（默認）
- **`.csv`** - CSV 格式，直接可用
- **`.parquet`** - Parquet 格式，高效壓縮和快速查詢
- **`.feather`** - Feather 格式，快速讀寫的二進制格式
- **`.h5`** - HDF5 格式，適合科學計算和大數據分析

### 支援的資料類型
- **現貨 (spot)**: K線、交易、聚合交易
- **USDⓈ-M 期貨 (um)**: K線、交易、聚合交易 + 6種期貨專用資料
- **COIN-M 期貨 (cm)**: K線、交易、聚合交易 + 6種期貨專用資料

## 📦 安裝依賴

```bash
pip install -r requirements.txt
```

依賴包括：
- `pandas` - 資料處理
- `pyarrow` - Parquet/Feather 格式支援
- `fastparquet` - 替代的 Parquet 實現
- `tables` - HDF5 格式支援
- `python-dotenv` - 環境變數管理

## 📋 可用的下載腳本

### 通用腳本 (支援 spot/um/cm)
1. **`download-kline.py`** - K線資料
2. **`download-trade.py`** - 交易資料
3. **`download-aggTrade.py`** - 聚合交易資料

### 期貨 K線腳本 (支援 um/cm)
4. **`download-futures-indexPriceKlines.py`** - 索引價格K線
5. **`download-futures-markPriceKlines.py`** - 標記價格K線
6. **`download-futures-premiumIndexKlines.py`** - 資金費率K線

### 期貨額外資料腳本 (支援 um/cm)
7. **`download-futures-bookDepth.py`** - 訂單簿深度 (僅 daily)
8. **`download-futures-bookTicker.py`** - 最佳買賣價 (daily + monthly)
9. **`download-futures-metrics.py`** - 交易指標 (僅 daily)
10. **`download-futures-fundingRate.py`** - 資金費率 (僅 monthly)

## 🔧 使用方法

### 基本語法
```bash
python [腳本名稱] -t [交易類型] -s [交易對] [其他參數] -f [格式]
```

### 主要參數
- **`-t`** - 交易類型：`spot`, `um`, `cm`
- **`-s`** - 交易對：如 `BTCUSDT`, `ETHUSDT` (um), `BTCUSD`, `ETHUSD` (cm)
- **`-i`** - 時間間隔：`1m`, `5m`, `1h`, `1d` 等 (僅 K線資料)
- **`-f`** - 資料格式：`.zip`, `.csv`, `.parquet`, `.feather`, `.h5`
- **`-startDate`** - 開始日期：`2024-01-01`
- **`-endDate`** - 結束日期：`2024-01-02`
- **`-skip-monthly`** - 跳過月資料：`1`
- **`-skip-daily`** - 跳過日資料：`1`

## 📊 使用範例

### 現貨資料
```bash
# K線資料
python download-kline.py -t spot -s BTCUSDT -i 1h -startDate 2024-01-01 -endDate 2024-01-02 -f .csv

# 交易資料
python download-trade.py -t spot -s ETHUSDT -startDate 2024-01-01 -endDate 2024-01-01 -f .parquet

# 聚合交易資料
python download-aggTrade.py -t spot -s ADAUSDT -startDate 2024-01-01 -endDate 2024-01-01 -f .feather
```

### USDⓈ-M 期貨資料
```bash
# 基本K線
python download-kline.py -t um -s BTCUSDT -i 1h -startDate 2024-01-01 -endDate 2024-01-01 -f .csv

# 標記價格K線
python download-futures-markPriceKlines.py -t um -s ETHUSDT -i 1h -startDate 2024-01-01 -endDate 2024-01-01 -f .parquet

# 訂單簿深度
python download-futures-bookDepth.py -t um -s BTCUSDT -startDate 2024-01-01 -endDate 2024-01-01 -f .csv

# 資金費率
python download-futures-fundingRate.py -t um -s BTCUSDT -y 2024 -m 1 -f .h5
```

### COIN-M 期貨資料
```bash
# 基本K線
python download-kline.py -t cm -s BTCUSD -i 1h -startDate 2024-01-01 -endDate 2024-01-01 -f .csv

# 索引價格K線
python download-futures-indexPriceKlines.py -t cm -s ETHUSD -i 1h -startDate 2024-01-01 -endDate 2024-01-01 -f .parquet

# 交易指標
python download-futures-metrics.py -t cm -s BTCUSD -startDate 2024-01-01 -endDate 2024-01-01 -f .feather
```

## 📋 資料欄位說明

### K線資料 (klines)
```
open_time, open, high, low, close, volume, close_time, quote_asset_volume, 
number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore
```

### 交易資料 (trades)
```
trade_id, price, quantity, quote_quantity, timestamp, is_buyer_maker, ignore
```

### 聚合交易資料 (aggTrades)
```
agg_trade_id, price, quantity, first_trade_id, last_trade_id, timestamp, is_buyer_maker, ignore
```

### 訂單簿深度 (bookDepth)
```
timestamp, percentage, depth, notional
```

### 最佳買賣價 (bookTicker)
```
update_id, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, transaction_time, event_time
```

### 交易指標 (metrics)
```
create_time, symbol, sum_open_interest, sum_open_interest_value, count_toptrader_long_short_ratio, 
sum_toptrader_long_short_ratio, count_long_short_ratio, sum_taker_long_short_vol_ratio
```

### 資金費率 (fundingRate)
```
calc_time, funding_interval_hours, last_funding_rate
```

## 📁 檔案結構

下載的檔案會按照以下結構組織：

```
data/
├── spot/
│   ├── daily/
│   │   ├── klines/
│   │   ├── trades/
│   │   └── aggTrades/
│   └── monthly/
│       ├── klines/
│       ├── trades/
│       └── aggTrades/
└── futures/
    ├── um/
    │   ├── daily/
    │   │   ├── klines/
    │   │   ├── trades/
    │   │   ├── aggTrades/
    │   │   ├── indexPriceKlines/
    │   │   ├── markPriceKlines/
    │   │   ├── premiumIndexKlines/
    │   │   ├── bookDepth/
    │   │   ├── bookTicker/
    │   │   └── metrics/
    │   └── monthly/
    │       ├── klines/
    │       ├── trades/
    │       ├── aggTrades/
    │       ├── indexPriceKlines/
    │       ├── markPriceKlines/
    │       ├── premiumIndexKlines/
    │       ├── bookTicker/
    │       └── fundingRate/
    └── cm/
        └── [與 um 相同結構]
```

## ⚙️ 環境設定

在 `.env` 檔案中設定資料儲存目錄：

```
STORE_DIRECTORY=/path/to/your/data/directory
```

## 🔍 格式特性比較

| 格式 | 檔案大小 | 讀取速度 | 壓縮率 | 相容性 | 適用場景 |
|------|----------|----------|---------|---------|----------|
| ZIP | 中等 | 慢 | 好 | 通用 | 原始資料保存 |
| CSV | 大 | 中等 | 無 | 最佳 | 人類可讀，廣泛支援 |
| Parquet | 小 | 快 | 極佳 | 好 | 大數據分析，雲端 |
| Feather | 中等 | 極快 | 中等 | 中等 | 快速原型開發 |
| HDF5 | 小 | 快 | 好 | 中等 | 科學計算，複雜查詢 |

## 📝 重要特性

### 統一的欄位命名
- 自動檢測和處理有/無標題的 CSV 檔案
- 統一使用標準欄位名稱，便於後續分析
- 支援多種資料類型的自動識別

### 智能檔案處理
- 自動解壓縮和格式轉換
- 完整的錯誤處理和檔案清理
- 支援中斷和重新開始下載

### 靈活的時間範圍
- 支援日期範圍 (`-startDate`, `-endDate`)
- 支援年月選擇 (`-y`, `-m`)
- 可選擇跳過 daily 或 monthly 資料

## 🚨 注意事項

### 交易對格式
- **現貨/UM期貨**: BTCUSDT, ETHUSDT, ADAUSDT (以 USDT 結尾)
- **CM期貨**: BTCUSD, ETHUSD, ADAUSD (以 USD 結尾)

### 資料可用性
- **bookDepth, metrics**: 僅 daily 資料
- **fundingRate**: 僅 monthly 資料
- **bookTicker**: daily + monthly 資料
- **其他資料類型**: 通常都有 daily + monthly

### 效能建議
- **大量資料**: 建議使用 Parquet 格式
- **頻繁讀寫**: 建議使用 Feather 格式
- **人工檢查**: 建議使用 CSV 格式
- **科學計算**: 建議使用 HDF5 格式

## 🛠️ 故障排除

### 常見問題
1. **找不到檔案**: 檢查日期範圍和交易對是否存在
2. **格式轉換失敗**: 檢查磁碟空間和檔案權限
3. **參數錯誤**: 檢查交易類型和交易對格式是否匹配

### 快速檢查
```bash
# 檢查腳本參數
python download-kline.py --help

# 測試下載單日資料
python download-kline.py -t spot -s BTCUSDT -i 1h -startDate 2024-01-01 -endDate 2024-01-01 -f .csv -skip-monthly 1
```

## 📄 版本資訊

- **支援的資料格式**: 5 種 (ZIP, CSV, Parquet, Feather, HDF5)
- **支援的交易類型**: 3 種 (spot, um, cm)
- **下載腳本數量**: 10 個
- **資料類型覆蓋**: Binance 所有公開資料類型

---

更多資訊請參考 [Binance 公開資料文檔](https://github.com/binance/binance-public-data)
