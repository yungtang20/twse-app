# Change: Add Supabase Rolling Window Sync

## Why
目前本地資料庫 (`taiwan_stock.db`) 存放了 3 年的歷史資料 (約 800 MB)，超過 Supabase 免費版的 500 MB 限制。為了讓 Android App 能使用 Supabase 作為雲端資料來源，需要實作「滾動視窗」機制：只保留最近 450 天的資料，並設定自動清理舊資料的 Cron Job。

## What Changes
- **ADDED**: Supabase 資料庫綱要 (PostgreSQL 相容)
- **ADDED**: Python 同步腳本 (`sync_to_supabase.py`)，將最近 450 天資料上傳至 Supabase
- **ADDED**: Supabase Edge Function (Cron Job)，每日自動清理超過 450 天的舊資料
- **ADDED**: 初次同步與增量同步邏輯

## Impact
- Affected specs: `supabase-sync` (新建)
- Affected code:
  - `d:/twse/twse_app/supabase_schema.sql` (已存在，需更新)
  - `d:/twse/sync_to_supabase.py` (新建)
  - Supabase Dashboard: Edge Function / Cron Job 配置
