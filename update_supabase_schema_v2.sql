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
    PRIMARY KEY (code, date_int)
);

-- 3. stock_list table (ensure it exists)
CREATE TABLE IF NOT EXISTS stock_list (
    code TEXT PRIMARY KEY,
    name TEXT,
    industry TEXT,
    list_date TEXT,
    type TEXT,
    status TEXT
);

-- 4. stock_snapshot table (for latest values)
CREATE TABLE IF NOT EXISTS stock_snapshot (
    code TEXT PRIMARY KEY,
    name TEXT,
    date TEXT,
    close DOUBLE PRECISION,
    volume BIGINT,
    change DOUBLE PRECISION,
    pct_change DOUBLE PRECISION,
    pe DOUBLE PRECISION,
    pb DOUBLE PRECISION,
    yield DOUBLE PRECISION,
    FOREIGN KEY (code) REFERENCES stock_list(code)
);
