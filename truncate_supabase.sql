-- ⚠️ 警告：這將會刪除所有雲端資料！
-- 請在 Supabase SQL Editor 中執行

TRUNCATE TABLE stock_history;
TRUNCATE TABLE institutional_investors;
TRUNCATE TABLE stock_data;
TRUNCATE TABLE sync_status;

-- 重置序列 (可選)
ALTER SEQUENCE sync_status_id_seq RESTART WITH 1;
