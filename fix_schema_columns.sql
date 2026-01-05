-- Fix missing columns in institutional_investors
-- Run this in Supabase SQL Editor

ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS foreign_holding_shares BIGINT;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS foreign_holding_pct DOUBLE PRECISION;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS trust_holding_shares BIGINT;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS trust_holding_pct DOUBLE PRECISION;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS dealer_holding_shares BIGINT;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS dealer_holding_pct DOUBLE PRECISION;
