-- ============================================
-- Supabase 自動清理舊資料 (Cron Job SQL)
-- ============================================
-- 
-- 使用方式:
-- 1. 登入 Supabase Dashboard
-- 2. 前往 SQL Editor
-- 3. 創建一個 pg_cron 任務來每日執行此 SQL
--
-- 設定 Cron Job:
-- SELECT cron.schedule(
--     'daily-cleanup',           -- 任務名稱
--     '0 0 * * *',               -- 每日 00:00 UTC 執行
--     $$DELETE FROM stock_data WHERE date < CURRENT_DATE - INTERVAL '450 days'$$
-- );
--
-- 查看 Cron Job:
-- SELECT * FROM cron.job;
--
-- 刪除 Cron Job:
-- SELECT cron.unschedule('daily-cleanup');
-- ============================================

-- 手動執行清理 (一次性)
DELETE FROM stock_data 
WHERE date < CURRENT_DATE - INTERVAL '450 days';

-- 查看資料量統計
SELECT 
    COUNT(*) as total_rows,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    pg_size_pretty(pg_total_relation_size('stock_data')) as table_size
FROM stock_data;
