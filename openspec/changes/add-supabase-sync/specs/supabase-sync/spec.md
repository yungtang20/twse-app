## ADDED Requirements

### Requirement: Data Retention Window
系統 SHALL 只在 Supabase 保留最近 450 個交易日的股票資料，以確保儲存空間不超過 500 MB 免費限制，同時滿足 MA200 指標計算需求。

#### Scenario: Initial Sync
- **WHEN** 使用者首次執行同步腳本
- **THEN** 系統只上傳本地資料庫中最近 450 天的資料到 Supabase

#### Scenario: Incremental Sync
- **WHEN** 使用者每日執行同步腳本
- **THEN** 系統只上傳當天新增的資料 (Upsert)

---

### Requirement: Automatic Cleanup
系統 SHALL 每日自動刪除超過 450 天的舊資料，確保資料庫大小維持穩定。

#### Scenario: Daily Cleanup
- **WHEN** Cron Job 於每日指定時間執行
- **THEN** 系統刪除所有日期早於 (今日 - 450 天) 的資料

#### Scenario: Manual Trigger
- **WHEN** 使用者手動觸發清理 Edge Function
- **THEN** 系統執行相同的清理邏輯

---

### Requirement: Sync Progress Reporting
同步腳本 SHALL 顯示進度與狀態，讓使用者了解同步執行情況。

#### Scenario: Progress Display
- **WHEN** 同步腳本正在執行
- **THEN** 系統每 100 筆資料顯示一次進度百分比

#### Scenario: Error Handling
- **WHEN** 同步過程發生錯誤
- **THEN** 系統記錄錯誤並繼續處理剩餘資料
