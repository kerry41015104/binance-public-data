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
- **`-s`** - 交易對： (cm) 的標的名稱需要注意 [幣本位頁面](https://data.binance.vision/?prefix=data/futures/cm/daily/klines/)
- **`-i`** - 僅 klines資料<br>
    daily: ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
    monthly: ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1mo"]
- **`-startDate` + `-endDate `** - 通常不使用，會新增一個日期區間的資料夾
- **`-skip-monthly`** - 通常要加，以下載日頻為主

| Argument        | Explanation | Default | Mandatory |      
| :---------------: | ---------------- | :----------------: | :----------------: |
| -t              | Market type: **spot**, **um** (USD-M Futures), **cm** (COIN-M Futures) | spot | Yes |
| -s              | Single **symbol** or multiple **symbols** separated by space | All symbols | No |
| -i              | single kline **interval** or multiple **intervals** separated by space      | All intervals | No |
| -y              | Single **year** or multiple **years** separated by space| All available years from 2020 to current year | No |
| -m              | Single **month** or multiple **months** separated by space | All available months | No |
| -d              | single **date** or multiple **dates** separated by space    | All available dates from 2020-01-01 | No |
| -startDate      | **Starting date** to download in [YYYY-MM-DD] format    | 2020-01-01 | No |
| -endDate        | **Ending date** to download in [YYYY-MM-DD] format     | Current date | No |
| -skip-monthly   | 1 to skip downloading of monthly data | 0 | No |
| -skip-daily     | 1 to skip downloading of daily data | 0 | No |
| -folder         | **Directory** to store the downloaded data    | Current directory | No |
| -c              | 1 to download **checksum file** | 0 | No |
| -f              | data type: `.zip`, `.csv`, `.parquet`, `.feather`, `.h5` | `.zip` | No |
| -h              | show help messages| - | No |

## 📊 使用範例

### 現貨資料
```bash
# K線資料(常用)
python download-kline.py -t spot -i 1m -f .parquet

# 交易資料
python download-trade.py -t spot -s ETHUSDT -startDate 2024-01-01 -endDate 2024-01-01 -f .parquet

# 聚合交易資料
python download-aggTrade.py -t spot -s ADAUSDT -startDate 2024-01-01 -endDate 2024-01-01 -f .feather
```

### USDⓈ-M 期貨資料
```bash
# 基本K線
python download-kline.py -t um -i 1m -f .parquet

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
python download-kline.py -t cm -i 1m -f .parquet

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

## 🚨 注意事項

### 交易對格式
- **現貨/UM期貨**: BTCUSDT, ETHUSDT, ADAUSDT (以 USDT 結尾)
- **CM期貨**: BTCUSD, ETHUSD, ADAUSD (以 USD 結尾)

### 資料可用性
- **bookDepth, metrics**: 僅 daily 資料
- **fundingRate**: 僅 monthly 資料
- **bookTicker**: daily + monthly 資料
- **其他資料類型**: 通常都有 daily + monthly

## 📄 版本資訊

- **支援的資料格式**: 5 種 (ZIP, CSV, Parquet, Feather, HDF5)
- **支援的交易類型**: 3 種 (spot, um, cm)
- **下載腳本數量**: 10 個
- **資料類型覆蓋**: Binance 所有公開資料類型

---

更多資訊請參考 [Binance 原始文檔](https://github.com/binance/binance-public-data)
