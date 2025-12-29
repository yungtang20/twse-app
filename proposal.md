# 執行目標
1. 實作市場掃描的詳細篩選過程記錄 (Screening Log)。
2. 讓側邊欄 (Sidebar) 可調整寬度。

# 修改內容
1.  **Backend (`backend/routers/scan.py`)**:
    *   新增 `ScanResponse` 模型中的 `process_log` 欄位。
    *   實作 `execute_step_scan` 函數，支援分步執行 SQL 查詢並計算每一步的剩餘檔數。
    *   修改 `scan_2560` 端點以使用 `execute_step_scan`，並定義了具體的篩選步驟 (趨勢條件、股價支撐、動能確認)。
    *   修復了 `scan.py` 中的語法錯誤。

2.  **Frontend (`frontend/src/pages/Scan.jsx`)**:
    *   新增 `processLog` 狀態變數。
    *   修改 `fetchScanResults` 以接收後端回傳的 `process_log`。
    *   在篩選面板中新增顯示篩選步驟與剩餘檔數的區塊 (Step -> Count)。
    *   修復了 `Scan.jsx` 中的語法錯誤。

3.  **Frontend (`frontend/src/components/layout/Sidebar.jsx`)**:
    *   新增 `width` 狀態與拖曳調整寬度的邏輯 (Resize Handle)。
    *   使用 `useRef` 和滑鼠事件 (`mousemove`, `mouseup`) 實作側邊欄寬度調整。

# 修改原因
*   使用者希望看到篩選過程的詳細漏斗數據，了解每一層過濾掉了多少股票。
*   使用者希望可以調整區塊大小 (側邊欄)。

# 修改進度
*   [x] Backend: `execute_step_scan` 實作完成。
*   [x] Backend: `scan_2560` 整合完成。
*   [x] Frontend: `Sidebar` 可調整寬度完成。
*   [x] Frontend: `Scan.jsx` 顯示篩選過程完成。
*   [x] Debug: 修復語法錯誤完成。

# 2025-12-26 優化任務
## 執行目標
1.  依據 `optimization.md` 與 `rule.md` 優化 `最終修正.py` 代碼結構。
2.  提升代碼可讀性與維護性 (Extract Method, Guard Clauses, Table-Driven)。

## 修改內容
1.  **Backend (`最終修正.py`)**:
    *   重構 `_handle_stock_query`: 拆分為 `fetch_realtime_data`, `fetch_and_display_history`, `handle_chart_interaction`。
    *   重構 `_display_ranking`: 拆分為 `fetch_ranking_data`, `process_ranking_data`, `render_ranking_table`。
    *   優化 `_worker_calc_indicators`: 使用衛語句簡化邏輯。
    *   應用表驅動法優化部分邏輯。

## 修改原因
*   使用者要求參考 `optimization.md` 進行代碼優化。
*   提升系統穩定性與開發效率。

## 修改進度
*   [x] 重構 `_handle_stock_query`
*   [x] 重構 `_display_ranking`
*   [x] 優化 `_worker_calc_indicators`

# 2025-12-26 假日跳過修復
## 執行目標
修復法人資料回補嘗試抓取休市日資料的問題。

## 修改內容
*   修改 `is_market_holiday` 函數，新增週末檢查邏輯。
*   整合 TWSE 官方 API (`/holidaySchedule/holidaySchedule`) 動態取得休市日。
*   新增 `_fetch_holidays_from_twse` 函數，支援快取機制 (24小時)。
*   静態備援表 `MARKET_HOLIDAYS_FALLBACK` 作為 API 失敗時的後備。

## 修改原因
*   使用者回報回補功能嘗試抓取 12-25 等無資料日期。
*   使用者指出 TWSE 有提供休市日 API。

## 修改進度
*   [x] 修復 `is_market_holiday` 函數
*   [x] 新增 12-25 到休市表
*   [x] 整合 TWSE 休市日 API

# 2025-12-29 資料庫損毀自動修復
## 執行目標
解決 `sqlite3.DatabaseError: database disk image is malformed` 導致程式崩潰的問題。

## 修改內容
1.  **Backend (`最終修正.py`)**:
    *   新增 `check_and_repair_db` 函數：
        *   在程式啟動時執行 `PRAGMA integrity_check`。
        *   若檢測到損毀，自動將壞檔重新命名為 `*.corrupted_YYYYMMDD_HHMMSS`。
        *   允許系統重新建立全新的資料庫檔案。
    *   強化 `SingleWriterDBManager` 的 `_writer_loop`：
        *   在建立連線與設定 PRAGMA 時加入 `try-except` 保護，避免線程崩潰。

## 修改原因
*   使用者回報資料庫磁碟映像損毀，導致 `DBWriter` 線程崩潰且無法啟動系統。

## 修改進度
*   [x] 實作啟動時自動檢查與修復機制
*   [x] 強化 DBWriter 錯誤處理

# 2025-12-29 啟動效能優化
## 執行目標
解決程式啟動緩慢的問題。

## 修改內容
1.  **Backend (`最終修正.py`)**:
    *   修改 `_should_update_twstock` 函數，暫時關閉自動檢查更新 (pip install check)，改為直接返回 False。

## 修改原因
*   使用者回報啟動變好慢。
*   經查發現 `pip install` 檢查為阻塞式操作，嚴重影響啟動速度。

## 修改進度
*   [x] 關閉自動更新檢查
*   [x] (2025-12-29) 依使用者要求重新開啟自動更新檢查
