-- Update Supabase Schema to match local SQLite
-- Run this in Supabase SQL Editor

-- 1. stock_history table
CREATE TABLE IF NOT EXISTS stock_history (
    code TEXT NOT NULL,
    date_int BIGINT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    amount BIGINT,
    foreign_buy DOUBLE PRECISION,
    trust_buy DOUBLE PRECISION,
    dealer_buy DOUBLE PRECISION,
    tdcc_count BIGINT,
    large_shareholder_pct DOUBLE PRECISION,
    PRIMARY KEY (code, date_int)
);

-- 2. institutional_investors table
CREATE TABLE IF NOT EXISTS institutional_investors (
    code TEXT NOT NULL,
    date_int BIGINT NOT NULL,
    foreign_buy BIGINT,
    foreign_sell BIGINT,
    trust_buy BIGINT,
    trust_sell BIGINT,
    dealer_buy BIGINT,
    dealer_sell BIGINT,
    foreign_net BIGINT,
    trust_net BIGINT,
    dealer_net BIGINT,
    foreign_holding_shares BIGINT,
    foreign_holding_pct DOUBLE PRECISION,
    trust_holding_shares BIGINT,
    trust_holding_pct DOUBLE PRECISION,
    dealer_holding_shares BIGINT,
    dealer_holding_pct DOUBLE PRECISION,
    PRIMARY KEY (code, date_int)
);

-- 3. stock_meta table (renamed from stock_list to match code)
CREATE TABLE IF NOT EXISTS stock_meta (
    code TEXT PRIMARY KEY,
    name TEXT,
    industry TEXT,
    list_date TEXT,
    delist_date TEXT,
    market_type TEXT,
    status TEXT,
    total_shares BIGINT
);

-- 4. stock_snapshot table (for latest values)
CREATE TABLE IF NOT EXISTS stock_snapshot (
    code TEXT PRIMARY KEY,
    name TEXT,
    date TEXT,
    close DOUBLE PRECISION,
    volume BIGINT,
    close_prev DOUBLE PRECISION,
    vol_prev BIGINT,
    amount DOUBLE PRECISION,
    pe DOUBLE PRECISION,
    pb DOUBLE PRECISION,
    yield DOUBLE PRECISION,
    ma5 DOUBLE PRECISION,
    ma20 DOUBLE PRECISION,
    ma60 DOUBLE PRECISION,
    ma120 DOUBLE PRECISION,
    ma200 DOUBLE PRECISION,
    rsi DOUBLE PRECISION,
    mfi14 DOUBLE PRECISION,
    daily_k DOUBLE PRECISION,
    daily_d DOUBLE PRECISION,
    vp_poc DOUBLE PRECISION,
    vp_high DOUBLE PRECISION,
    vp_low DOUBLE PRECISION,
    foreign_buy BIGINT,
    trust_buy BIGINT,
    dealer_buy BIGINT,
    total_shareholders BIGINT,
    major_holders_pct DOUBLE PRECISION
);

-- 5. stock_shareholding_all table (TDCC)
CREATE TABLE IF NOT EXISTS stock_shareholding_all (
    code TEXT NOT NULL,
    date_int BIGINT NOT NULL,
    level INTEGER NOT NULL,
    holders BIGINT,
    shares BIGINT,
    proportion DOUBLE PRECISION,
    PRIMARY KEY (code, date_int, level)
);

-- 6. margin_data table
CREATE TABLE IF NOT EXISTS margin_data (
    code TEXT NOT NULL,
    date_int BIGINT NOT NULL,
    margin_buy BIGINT,
    margin_sell BIGINT,
    margin_redemp BIGINT,
    margin_balance BIGINT,
    margin_util_rate DOUBLE PRECISION,
    short_buy BIGINT,
    short_sell BIGINT,
    short_redemp BIGINT,
    short_balance BIGINT,
    short_util_rate DOUBLE PRECISION,
    PRIMARY KEY (code, date_int)
);
