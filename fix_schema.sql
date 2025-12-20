-- Fix missing columns in institutional_investors table
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS dealer_net BIGINT;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS foreign_net BIGINT;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS trust_net BIGINT;

-- Ensure other columns exist just in case
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS foreign_buy BIGINT;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS foreign_sell BIGINT;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS trust_buy BIGINT;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS trust_sell BIGINT;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS dealer_buy BIGINT;
ALTER TABLE institutional_investors ADD COLUMN IF NOT EXISTS dealer_sell BIGINT;
