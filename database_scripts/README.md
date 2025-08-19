# Binance 公開資料庫存儲系統 v2.0

這個模組負責將下載的 Binance 公開資料導入到 PostgreSQL 資料庫中，支援首次批量導入和後續增量更新。

## 🏗️ 架構概覽

### 資料庫設計
- **完整的 PostgreSQL 架構**：支援所有 Binance 公開資料類型
- **分區表設計**：K線資料按月分區，提高大量資料的查詢性能
- **完整性約束**：確保資料一致性和防止重複
- **索引優化**：針對常用查詢模式進行優化

### 支援的資料類型
- **現貨 (Spot)**：K線、交易、聚合交易
- **USDⓈ-M 期貨 (UM)**：K線、交易、聚合交易 + 6種期貨專用資料
- **COIN-M 期貨 (CM)**：K線、交易、聚合交易 + 6種期貨專用資料

### 檔案格式支援
- ✅ **CSV** - 人類可讀，廣泛支援
- ✅ **Parquet** - 高效壓縮，快速查詢，適合大數據
- ✅ **Feather** - 快速讀寫的二進制格式
- ✅ **HDF5** - 科學計算和複雜查詢

## 📦 安裝和設置

### 1. 安裝依賴
```bash
cd database_scripts
pip install -r requirements.txt
```

### 2. 配置環境變數
```bash
cp .env.example .env
# 編輯 .env 文件，填入你的資料庫連接資訊
```

### 📋 詳細配置參數說明

#### 資料庫連接配置
```bash
DB_HOST=localhost          # PostgreSQL 服務器地址
DB_PORT=5432              # PostgreSQL 服務端口
DB_NAME=binance_data      # 資料庫名稱
DB_USER=postgres          # PostgreSQL 用戶名
DB_PASSWORD=your_password # 資料庫密碼
DB_SCHEMA=binance_data    # Schema 名稱

# 連接池設置
DB_MIN_CONNECTIONS=1      # 最小連接數
DB_MAX_CONNECTIONS=10     # 最大連接數

# 數據存儲目錄
STORE_DIRECTORY=D:\code\Trading-Universe\crypto-data-overall\binance-public-data
```

### 3. 創建資料庫架構
```bash
# 推薦：使用 Python 腳本
python setup_database.py

# 或者：使用 psql 命令行
psql -U postgres -d binance_data -f create_schema.sql
```

## 🚀 使用方法

### 命令行工具詳細說明

#### 1. 導入單個文件 (`import-file`)
```bash
python import_data.py --action import-file --file "文件路徑" [--trading-type spot]
```

#### 2. 批量導入目錄 (`import-dir`)
```bash
python import_data.py --action import-dir --directory "目錄路徑"
```

#### 3. 完整批量導入 (`bulk-import`)
```bash
python import_data.py --action bulk-import [--directory "基礎目錄"]
```

#### 4. 增量更新 (`incremental`)
```bash
python import_data.py --action incremental \
    --symbol BTCUSDT \
    --data-type klines \
    --trading-type spot \
    --interval 1h \
    --days-back 7
```

## 📊 參數詳細說明

### 必要參數
- `--action` - 執行動作：
  - `import-file` - 導入單個文件
  - `import-dir` - 批量導入目錄
  - `bulk-import` - 完整批量導入
  - `incremental` - 增量更新

### 文件相關參數
- `--file` - 要導入的文件完整路徑
- `--directory` - 要掃描的目錄路徑

### 交易參數
- `--trading-type` - 交易類型：
  - `spot` - 現貨交易
  - `um` - USDⓈ-M 期貨
  - `cm` - COIN-M 期貨

- `--symbol` - 交易對符號（如：BTCUSDT、ETHUSDT）

- `--data-type` - 資料類型：
  - `klines` - K線資料
  - `trades` - 原始交易資料
  - `aggTrades` - 聚合交易資料
  - `indexPriceKlines` - 期貨索引價格K線
  - `markPriceKlines` - 期貨標記價格K線
  - `premiumIndexKlines` - 期貨資金費率K線
  - `bookDepth` - 訂單簿深度
  - `bookTicker` - 最佳買賣價
  - `metrics` - 交易指標
  - `fundingRate` - 資金費率

### 時間參數
- `--interval` - 時間間隔（僅K線資料）：
  - 分鐘級別：`1m`, `3m`, `5m`, `15m`, `30m`
  - 小時級別：`1h`, `2h`, `4h`, `6h`, `8h`, `12h`
  - 日/週/月級別：`1d`, `3d`, `1w`, `1mo`

- `--days-back` - 回溯天數（默認：7天）
  - 範圍：1-365
  - 用途：處理可能遺漏的歷史資料

## 🗂️ 目錄結構

```
database_scripts/
├── create_schema.sql      # 資料庫架構定義
├── database_config.py     # 資料庫連接和配置管理
├── data_importer.py       # 核心導入邏輯
├── import_data.py         # 命令行工具
├── setup_database.py     # 一鍵設置和重置腳本
├── reset_database.sql    # 資料庫重置腳本
├── requirements.txt       # Python 依賴
├── .env                  # 資料庫配置（用戶創建）
├── README.md             # 此文檔
└── __pycache__/          # Python 編譯緩存
```

## 🔍 使用場景

### 首次設置
1. **環境準備**：`pip install -r requirements.txt`
2. **配置資料庫**：編輯 `.env` 文件
3. **初始化架構**：`python setup_database.py`
4. **批量導入**：`python import_data.py --action bulk-import`

### 日常維護
1. **增量更新**：
   ```bash
   python import_data.py --action incremental \
       --symbol BTCUSDT --data-type klines \
       --trading-type spot --interval 1h
   ```

2. **監控狀態**：
   ```sql
   SELECT * FROM v_sync_overview 
   WHERE last_sync_date < CURRENT_DATE - INTERVAL '1 day';
   ```

---

**文檔版本**：v2.0  
**最後更新**：2024年12月  
**相容性**：PostgreSQL 12+, Python 3.8+