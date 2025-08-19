-- ====================================================
-- Binance 資料庫完整重置腳本
-- 安全地清理所有相關對象並重新創建資料庫架構
-- ====================================================

-- 設置客戶端編碼
SET client_encoding = 'UTF8';

-- 1. 刪除現有的 schema（包括所有對象）
DROP SCHEMA IF EXISTS binance_data CASCADE;

-- 2. 刪除現有的角色（如果存在）
DROP ROLE IF EXISTS binance_reader;
DROP ROLE IF EXISTS binance_writer;

-- 顯示清理完成信息
SELECT 'Schema 和角色已清理完成，可以重新創建' as status;
