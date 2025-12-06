## 1. 資料庫設定
- [x] 1.1 更新 Supabase SQL Schema，確保與本地 SQLite 欄位一致
- [ ] 1.2 在 Supabase Dashboard 建立 `stock_data` 表

## 2. 同步腳本實作
- [x] 2.1 創建 `sync_to_supabase.py` 同步腳本
- [x] 2.2 實作 SQLite → PostgreSQL 資料轉換邏輯
- [x] 2.3 實作「僅同步最近 450 天」的過濾條件
- [x] 2.4 實作 Upsert (INSERT ... ON CONFLICT UPDATE) 邏輯
- [x] 2.5 實作進度顯示與錯誤處理

## 3. 自動清理機制
- [x] 3.1 創建 Supabase Edge Function 用於清理舊資料
- [x] 3.2 設定 Cron Job 每日執行清理 (刪除 > 450 天的資料)

## 4. 驗證
- [x] 4.1 執行初次同步並確認資料完整性
- [x] 4.2 確認資料庫大小在 500 MB 限制內
- [ ] 4.3 測試 Android App 是否能正確讀取 Supabase 資料
