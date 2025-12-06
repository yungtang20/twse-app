-- ============================================
-- Supabase 資料庫初始化腳本
-- 包含: 資料表建立、索引、RLS 策略、自動排程
-- ============================================

-- 1. 啟用必要的擴充功能
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 2. 建立股票資料表 (stock_data)
CREATE TABLE IF NOT EXISTS stock_data (
    code TEXT NOT NULL,
    date DATE NOT NULL,
    name TEXT,
    
    -- 價格資料
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    
    -- 比較資料
    close_prev NUMERIC,
    vol_prev BIGINT,
    
    -- 均線
    ma3 NUMERIC, ma5 NUMERIC, ma10 NUMERIC, ma20 NUMERIC, 
    ma60 NUMERIC, ma120 NUMERIC, ma200 NUMERIC,
    
    ma3_prev NUMERIC, ma20_prev NUMERIC, ma60_prev NUMERIC, 
    ma120_prev NUMERIC, ma200_prev NUMERIC,
    
    -- 加權均線
    wma3 NUMERIC, wma20 NUMERIC, wma60 NUMERIC, wma120 NUMERIC, wma200 NUMERIC,
    wma3_prev NUMERIC, wma20_prev NUMERIC, wma60_prev NUMERIC, 
    wma120_prev NUMERIC, wma200_prev NUMERIC,
    
    -- 技術指標
    mfi14 NUMERIC, mfi14_prev NUMERIC,
    vwap20 NUMERIC, vwap20_prev NUMERIC,
    chg14_pct NUMERIC, chg14_pct_prev NUMERIC,
    rsi NUMERIC,
    macd NUMERIC, signal NUMERIC,
    
    -- KD 指標
    month_k NUMERIC, month_d NUMERIC,
    month_k_prev NUMERIC, month_d_prev NUMERIC,
    week_k NUMERIC, week_d NUMERIC,
    week_k_prev NUMERIC, week_d_prev NUMERIC,
    daily_k NUMERIC, daily_d NUMERIC,
    daily_k_prev NUMERIC, daily_d_prev NUMERIC,
    
    -- 籌碼指標
    vp_poc NUMERIC, vp_upper NUMERIC, vp_lower NUMERIC,
    
    -- 聰明錢指標
    smi NUMERIC, smi_signal INTEGER, smi_prev NUMERIC,
    svi NUMERIC, svi_signal INTEGER, svi_prev NUMERIC,
    nvi NUMERIC, nvi_signal INTEGER, nvi_prev NUMERIC,
    vsa_signal INTEGER,
    smart_score NUMERIC, smart_score_prev NUMERIC,
    clv NUMERIC, pvi NUMERIC,
    
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (code, date)
);

-- 建立索引
CREATE INDEX IF NOT EXISTS idx_stock_data_date ON stock_data(date DESC);
CREATE INDEX IF NOT EXISTS idx_stock_data_volume ON stock_data(volume DESC);

-- 3. 建立自選股資料表 (watchlist)
CREATE TABLE IF NOT EXISTS watchlist (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    device_id TEXT NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    notes TEXT,
    added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(device_id, code)
);

-- 建立索引
CREATE INDEX IF NOT EXISTS idx_watchlist_device ON watchlist(device_id);

-- 4. 設定 RLS (Row Level Security)
ALTER TABLE stock_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE watchlist ENABLE ROW LEVEL SECURITY;

-- 開放讀取權限 (公開讀取)
CREATE POLICY "Allow public read access" ON stock_data FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON watchlist FOR SELECT USING (true);

-- 開放寫入權限 (需驗證 Service Role Key 或特定條件，這裡簡化為允許所有寫入以便同步腳本運作)
-- 注意: 生產環境應限制寫入權限
CREATE POLICY "Allow insert/update/delete" ON stock_data FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow insert/update/delete" ON watchlist FOR ALL USING (true) WITH CHECK (true);

-- 5. 設定自動排程 (pg_cron)

-- 每日清理舊資料 (保留 450 天)
-- 每天 UTC 00:00 (台灣時間 08:00) 執行
SELECT cron.schedule(
    'daily-cleanup',
    '0 0 * * *',
    $$DELETE FROM stock_data WHERE date < CURRENT_DATE - INTERVAL '450 days'$$
);

-- 6. 建立輔助函數 (可選)
-- 取得最新資料日期
CREATE OR REPLACE FUNCTION get_latest_date()
RETURNS DATE AS $$
BEGIN
    RETURN (SELECT MAX(date) FROM stock_data);
END;
$$ LANGUAGE plpgsql;
