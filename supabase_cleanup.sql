-- 清除 1 年前的舊資料以釋放空間
-- 您可以在 Supabase Dashboard -> SQL Editor 中執行此腳本

BEGIN;

-- 1. 清除舊的股價歷史 (保留最近 1 年)
DELETE FROM stock_history 
WHERE date < CURRENT_DATE - INTERVAL '1 year';

-- 2. 清除舊的法人買賣超 (保留最近 1 年)
DELETE FROM institutional_investors 
WHERE date < CURRENT_DATE - INTERVAL '1 year';

-- 3. 清除舊的每日行情 (保留最近 1 年)
DELETE FROM stock_data 
WHERE date < CURRENT_DATE - INTERVAL '1 year';

-- 4. 真空清理 (釋放磁碟空間)
-- 注意: 這可能會鎖定資料表一段時間，建議在股市收盤後執行
VACUUM FULL stock_history;
VACUUM FULL institutional_investors;
VACUUM FULL stock_data;

COMMIT;
