-- ============================================
-- 台股分析 App - Supabase 資料表設計
-- ============================================

-- 1. 日K資料表
CREATE TABLE IF NOT EXISTS stock_data (
    id BIGSERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    name TEXT,
    date DATE NOT NULL,
    
    -- OHLCV
    open REAL, high REAL, low REAL, close REAL,
    volume BIGINT,
    close_prev REAL, vol_prev BIGINT,
    
    -- 基礎均線
    ma3 REAL, ma20 REAL, ma60 REAL, ma120 REAL, ma200 REAL,
    wma3 REAL, wma20 REAL, wma60 REAL, wma120 REAL, wma200 REAL,
    
    -- 技術指標
    mfi14 REAL, vwap20 REAL, chg14_pct REAL,
    rsi REAL, macd REAL, signal REAL,
    
    -- 籌碼/量價
    vp_poc REAL, vp_upper REAL, vp_lower REAL,
    
    -- 月KD
    month_k REAL, month_d REAL,
    
    -- 日KD / 週KD
    daily_k REAL, daily_d REAL,
    week_k REAL, week_d REAL,
    
    -- 聰明錢指標
    smi REAL, smi_signal INTEGER,
    svi REAL, svi_signal INTEGER,
    nvi REAL, nvi_signal INTEGER,
    vsa_signal INTEGER,
    smart_score INTEGER,
    clv REAL,
    
    -- 前日值
    ma3_prev REAL, ma20_prev REAL, ma60_prev REAL, ma120_prev REAL, ma200_prev REAL,
    wma3_prev REAL, wma20_prev REAL, wma60_prev REAL, wma120_prev REAL, wma200_prev REAL,
    mfi14_prev REAL, vwap20_prev REAL, chg14_pct_prev REAL,
    month_k_prev REAL, month_d_prev REAL,
    daily_k_prev REAL, daily_d_prev REAL,
    week_k_prev REAL, week_d_prev REAL,
    smi_prev REAL, svi_prev REAL, nvi_prev REAL, smart_score_prev INTEGER,
    pvi REAL,
    
    -- 時間戳
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(code, date)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_stock_data_code ON stock_data(code);
CREATE INDEX IF NOT EXISTS idx_stock_data_date ON stock_data(date DESC);
CREATE INDEX IF NOT EXISTS idx_stock_data_smart_score ON stock_data(smart_score) WHERE smart_score >= 3;
CREATE INDEX IF NOT EXISTS idx_stock_data_code_date ON stock_data(code, date DESC);

-- ============================================

-- 2. 週K資料表
CREATE TABLE IF NOT EXISTS stock_data_weekly (
    id BIGSERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    name TEXT,
    year_week TEXT NOT NULL,  -- 格式: '2025-W49'
    date_start DATE,
    date_end DATE,
    
    -- OHLCV
    open REAL, high REAL, low REAL, close REAL,
    volume BIGINT,
    
    -- 完整指標
    ma3 REAL, ma20 REAL, ma60 REAL, ma120 REAL, ma200 REAL,
    wma3 REAL, wma20 REAL, wma60 REAL, wma120 REAL, wma200 REAL,
    mfi14 REAL, vwap20 REAL, chg14_pct REAL,
    rsi REAL, macd REAL, signal REAL,
    vp_poc REAL, vp_upper REAL, vp_lower REAL,
    week_k REAL, week_d REAL,
    smi REAL, smi_signal INTEGER,
    svi REAL, svi_signal INTEGER,
    nvi REAL, nvi_signal INTEGER,
    vsa_signal INTEGER,
    smart_score INTEGER,
    clv REAL,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(code, year_week)
);

CREATE INDEX IF NOT EXISTS idx_weekly_code ON stock_data_weekly(code);
CREATE INDEX IF NOT EXISTS idx_weekly_year_week ON stock_data_weekly(year_week DESC);

-- ============================================

-- 3. 月K資料表
CREATE TABLE IF NOT EXISTS stock_data_monthly (
    id BIGSERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    name TEXT,
    year_month TEXT NOT NULL,  -- 格式: '2025-12'
    date_start DATE,
    date_end DATE,
    
    -- OHLCV
    open REAL, high REAL, low REAL, close REAL,
    volume BIGINT,
    
    -- 完整指標
    ma3 REAL, ma20 REAL, ma60 REAL, ma120 REAL, ma200 REAL,
    wma3 REAL, wma20 REAL, wma60 REAL, wma120 REAL, wma200 REAL,
    mfi14 REAL, vwap20 REAL, chg14_pct REAL,
    rsi REAL, macd REAL, signal REAL,
    vp_poc REAL, vp_upper REAL, vp_lower REAL,
    month_k REAL, month_d REAL,
    smi REAL, smi_signal INTEGER,
    svi REAL, svi_signal INTEGER,
    nvi REAL, nvi_signal INTEGER,
    vsa_signal INTEGER,
    smart_score INTEGER,
    clv REAL,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(code, year_month)
);

CREATE INDEX IF NOT EXISTS idx_monthly_code ON stock_data_monthly(code);
CREATE INDEX IF NOT EXISTS idx_monthly_year_month ON stock_data_monthly(year_month DESC);

-- ============================================

-- 4. 自選股資料表
CREATE TABLE IF NOT EXISTS watchlist (
    id BIGSERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,  -- UUID，本地產生
    code TEXT NOT NULL,
    name TEXT,
    notes TEXT,  -- 備註
    added_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(device_id, code)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_device ON watchlist(device_id);

-- ============================================

-- 5. 股票清單
CREATE TABLE IF NOT EXISTS stock_list (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    market TEXT,  -- 'TWSE' or 'TPEX'
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
