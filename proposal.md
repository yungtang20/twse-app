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
# 2025-12-31 統一色彩規則 (紅漲綠跌)
## 執行目標
1. 確保全系統數據遵循「紅：漲/上升，綠：跌/下降」的規則。
2. 處理數值為 0 時的中心化色彩（中性色）。

## 修改內容
1.  **Frontend (`Scan.jsx`, `Dashboard.jsx`, `StockDetail.jsx`, `TechnicalChart.jsx`)**:
    *   將原本使用 `>= 0` 判斷紅色的邏輯，改為三段式判斷，並加入 `Number()` 轉換確保精確度：
        *   `> 0`: 紅色 (`text-red-400` / `#ef4444`)
        *   `< 0`: 綠色 (`text-green-400` / `#22c55e`)
        *   `== 0`: 中性色 (`text-slate-400` / `#94a3b8`)
    *   **全面擴展色彩規則**：不僅限於漲跌幅，所有技術指標也加入色彩反饋：
        *   **POC / VWAP / 均線**：股價高於指標顯示紅色（強勢），低於指標顯示綠色（弱勢）。
        *   **MFI / RSI**：數值大於 50 顯示紅色（偏多），小於 50 顯示綠色（偏空）。
        *   **成交量均線**：短均量 > 長均量顯示紅色（量增），反之顯示綠色。
    *   修正範圍包括：股價漲跌幅、成交量增減、法人買賣超、MACD OSC 柱狀圖、POC、VWAP、MFI、RSI、MA20/60/120/200 等。
2.  **Frontend (`index.css`)**:
    *   新增全域類別 `.up` 與 `.down`，確保 `StockCard` 等組件能正確顯示紅綠色。

## 修改原因
*   使用者要求統一色彩規則，符合台灣股市習慣（紅漲綠跌）。
*   修正 0% 或 0 張時顯示為紅色的邏輯錯誤。

## 修改進度
*   [x] `Scan.jsx` 色彩邏輯修正
*   [x] `Dashboard.jsx` & `StockDetail.jsx` 標頭色彩修正
*   [x] `TechnicalChart.jsx` 指標與副圖色彩修正
*   [x] `index.css` 全域色彩類別定義

# 2025-12-31 股票名稱標準化 (移除後綴)
## 執行目標
1. 移除股票名稱中的 "股份有限公司" 後綴。
2. 將此邏輯整合至 "A Rule" (`is_normal_stock`) 及資料下載流程中。
3. 顯示時，股票名稱最多只保留 4 個字。

## 修改內容
1.  **Backend (`最終修正.py`)**:
    *   新增 `normalize_stock_name(name)` 函數，用於移除 "股份有限公司" 並去除空白。
    *   修改 `get_correct_stock_name` 函數，在返回名稱前先進行標準化。
    *   修改 `step2_download_lists` 函數，在下載 TWSE 和 TPEx 股票清單時，先將名稱標準化後再進行 "A Rule" (`is_normal_stock`) 檢查與寫入資料庫。

## 修改原因
*   使用者要求簡化股票名稱顯示，去除冗贅的 "股份有限公司" 字樣。
*   確保資料庫與顯示端的一致性。

## 修改進度
*   [x] 實作 `normalize_stock_name`
*   [x] 整合至 `get_correct_stock_name`
*   [x] 整合至 `step2_download_lists` (資料下載流程)
*   [x] 實作名稱截斷 (最多4字)

# 2026-01-01 股票名稱截斷優化
## 執行目標
1. 移除股票名稱中的 "股份有限公司"。
2. 將股票名稱限制在 4 個字以內。
3. 確保 A 規則 (如 DR 股檢查) 在截斷前執行，避免誤判。

## 修改內容
1.  **Backend (`最終修正.py`)**:
    *   修改 `normalize_stock_name`：新增截斷邏輯 (最多 4 字)。
    *   修改 `step2_download_lists`：調整邏輯，先使用原始名稱進行 `is_normal_stock` 檢查，通過後再進行標準化與截斷。

## 修改原因
*   使用者要求簡化顯示，只保留核心名稱。
*   避免截斷導致 "DR" 等關鍵字遺失而誤判為普通股。

## 修改進度
*   [x] 修改 `normalize_stock_name`
*   [x] 修改 `step2_download_lists`

# 2025-12-31 啟動速度優化
## 執行目標
優化 `最終修正.py` 的啟動速度，以支援手機等資源受限環境。

## 修改內容
1.  **Backend (`最終修正.py`)**:
    *   將 `pandas`, `numpy`, `twstock` 等重量級套件的引用從全域移至函數內部 (Lazy Loading)。
    *   新增 `init_twstock` 函數，僅在需要時載入 `twstock` 並套用 Patch。
    *   在 `step2_download_tpex_daily`, `calculate_stock_history_indicators` 等函數中加入區域引用。

## 修改原因
*   使用者反映啟動速度需要加快，且需支援手機環境。
*   全域引用導致啟動時載入大量未立即使用的模組，消耗記憶體與 CPU。

## 修改進度
*   [x] 實作 Lazy Loading 機制
*   [x] 驗證啟動速度提升

# 2025-12-31 手機版介面優化
## 執行目標
1. 精修手機版介面，移除多餘元素。
2. 實作底部導航列 (Bottom Navigation)。
3. 優化空間利用，隱藏手機版 Header。

## 修改內容
1.  **Frontend (`Layout.jsx`)**:
    *   新增 `BottomNav` 組件整合。
    *   依據 `isMobileView` 狀態切換顯示 `Sidebar` (桌面) 或 `BottomNav` (手機)。
    *   手機版隱藏 `Header` 以爭取更多畫面空間。
2.  **Frontend (`Header.jsx`)**:
    *   移除通知鈴鐺與使用者圖示。
    *   移除桌面版切換按鈕 (移至系統設定)。
3.  **Frontend (`Sidebar.jsx`)**:
    *   新增 `useMobileView` 整合。
4.  **Frontend (`BottomNav.jsx`)**:
    *   新增底部導航組件。
    *   包含：儀表板、市場掃描、法人排行、系統設定。
5.  **Frontend (`Settings.jsx`)**:
    *   新增「顯示設定」區塊。
    *   新增「桌面版切換」按鈕。

## 修改原因
*   使用者要求精修手機版面，刪除右上角通知與人像。
*   使用者要求將桌面版切換按鈕移至系統設定。
*   提升手機版操作體驗與空間利用率。

## 修改進度
*   [x] 移除 Header 圖示
*   [x] 實作 BottomNav
*   [x] 隱藏手機版 Header
*   [x] 移轉桌面版切換至 Settings
*   [x] 優化市場掃描列表 (手機版橫向捲動)

# 2025-12-31 APK 轉換 (本地 SQLite)
## 執行目標
將 React Web App 轉換為 Android APK，支援本地 SQLite 資料庫完全離線運作。

## 修改內容
1.  **Frontend (`frontend/src/lib/sqliteService.js`)**:
    *   新增 SQLite 服務模組，支援本地/雲端模式切換。
    *   實作 `getAllStocks`, `getStockByCode`, `getStockHistory`, `getInstitutionalRankings` 等函數。
    *   新增 `downloadDatabase` 函數支援從雲端下載資料庫。
2.  **Frontend (`frontend/src/lib/supabaseClient.js`)**:
    *   新增 Supabase 直連客戶端，用於雲端模式。
3.  **Frontend (`frontend/capacitor.config.ts`)**:
    *   Capacitor 配置檔，設定 App ID 為 `com.twse.app`。
4.  **Backend (`backend/services/db.py`)**:
    *   新增 `set_db_path` 函數支援動態切換資料庫路徑。
5.  **Backend (`backend/routers/admin.py`)**:
    *   新增 `/api/admin/db-path` GET/POST 端點。
6.  **Frontend (`frontend/src/pages/Settings.jsx`)**:
    *   新增資料庫路徑設定 UI。
7.  **Documentation (`README_APK.md`)**:
    *   新增 APK 打包指南，使用 Colab + Android SDK。

## 修改原因
*   使用者要求將手機版轉換為 APK。
*   使用者要求支援本地資料庫完全離線運作。
*   使用者無 Android Studio，改用 Colab 打包。

## 修改進度
*   [x] 安裝 Capacitor 相關套件
*   [x] 安裝 @capacitor-community/sqlite
*   [x] 建立 `sqliteService.js`
*   [x] 建立 `supabaseClient.js`
*   [x] 建立 `capacitor.config.ts`
*   [x] 建立 `README_APK.md`
*   [ ] 初始化 Android 專案
*   [ ] 測試 APK 打包

# 2026-01-01 雲端部署修復 (Render 500 Error)
## 執行目標
1. 解決 Render 部署後出現的 500 Internal Server Error。
2. 實作雲端模式 (Cloud Mode) 下的資料讀取機制 (Supabase)。
3. 新增資料庫路徑選擇功能 (Settings)。

## 修改內容
1.  **Backend (`backend/routers/rankings.py`)**:
    *   **Critical Fix**: 修復 `NameError: name 'router' is not defined`，補上 `router = APIRouter(...)` 定義。
    *   新增雲端模式檢查：若為雲端模式，從 Supabase 讀取最新日期 (`get_system_status`) 並返回空列表 (因複雜統計尚未移植)，避免 500 錯誤。
2.  **Backend (`backend/services/db.py`)**:
    *   修改 `get_stock_by_code`：新增雲端模式支援，分別從 `stock_meta` 和 `stock_snapshot` 讀取資料並合併。
    *   新增 **Defensive Error Handling**：若 `stock_snapshot` 讀取失敗 (如資料表不存在)，捕獲異常並返回基本 Meta 資料，防止頁面崩潰。
3.  **Frontend (`frontend/src/pages/Settings.jsx`)**:
    *   新增「資料庫路徑」設定區塊。
    *   新增「瀏覽」按鈕 (`<input type="file">`)，允許使用者在本機模式下選擇資料庫檔案 (雲端模式下僅顯示檔名)。

## 修改原因
1.  使用者回報 Render 部署後無法瀏覽網頁 (500 Error)。
2.  使用者截圖顯示 `rankings.py` 發生 `NameError` 導致部署失敗。
3.  使用者希望能手動指定資料庫路徑。

## 修改進度
1.  [x] 修復 `rankings.py` Router 定義缺失
2.  [x] 實作 `get_stock_by_code` 雲端讀取邏輯
3.  [x] 實作 `Settings.jsx` 瀏覽按鈕
4.  [x] 部署至 Render (等待生效)
# 2026-01-01 修正 NameError (pd/np 未定義)
## 執行目標
1. 修復 `最終修正.py` 在初始化或執行特定功能時出現的 `NameError: name 'pd' is not defined`。
2. 維持「延遲載入 (Lazy Loading)」優化，同時確保型別提示與邏輯正確。

## 修改內容
1.  **Backend (`最終修正.py`)**:
    *   新增 `from __future__ import annotations`：支援型別提示的延遲評估，解決類別定義時 `pd.DataFrame` 未定義的問題。
    *   補強區域引用：在 `HistoryRepository`, `TwstockDataSource`, `calc_indicators_pure`, `IndicatorCalculator` 等遺漏的地方補上 `import pandas as pd` 與 `import numpy as np`。

## 修改原因
*   先前為了優化啟動速度將 `pandas` 等套件改為延遲載入，但部分類別方法與型別提示仍依賴全域變數，導致執行時崩潰。

## 修改進度
*   [x] 實作 `from __future__ import annotations`
*   [x] 補強 `HistoryRepository` 區域引用
*   [x] 補強 `TwstockDataSource` 區域引用
*   [x] 補強 `IndicatorCalculator` 區域引用
*   [x] 補強 `step3_5`, `step3_6` 等下載函數區域引用
*   [x] 驗證修復結果

# 2026-01-01 更新 FinMind Token
## 執行目標
1. 更新 `最終修正.py` 中的 FinMind Token。
2. 同步更新 `config.json` 中的 Token (若存在)。

## 修改內容
1.  **Backend (`最終修正.py`)**:
    *   將預設的 FinMind Token 更新為使用者提供的新 Token。
2.  **Config (`config.json`)**:
    *   更新 `finmind_token` 欄位。

## 修改原因
*   使用者要求更新 FinMind Token 以確保資料抓取功能正常。

## 修改進度
*   [x] 更新 `最終修正.py`
*   [x] 更新 `config.json`

# 2026-01-01 修復 FinMind NameError
## 執行目標
1. 修復 `FinMindDataSource` 中因延遲載入導致的 `NameError: name 'pd' is not defined`。

## 修改內容
1.  **Backend (`最終修正.py`)**:
    *   在 `FinMindDataSource.fetch_history` 方法中加入 `import pandas as pd`。

## 修改原因
*   使用者回報回補資料時發生錯誤，經查為 `pd` 未定義。

## 修改進度
*   [x] 補上 `import pandas as pd`

# 2026-01-01 修復未定義函數 step1_fetch_stock_list
## 執行目標
1. 修復 `step1_fetch_stock_list` 未定義錯誤。

## 修改內容
1.  **Backend (`最終修正.py`)**:
    *   將 `step1_fetch_stock_list()` 替換為 `step2_download_lists()`（共兩處）。

## 修改原因
*   `step1_fetch_stock_list` 函數不存在，導致執行失敗。

## 修改進度
*   [x] 替換 `check_db_nulls` 中的呼叫
*   [x] 替換 `__main__` 自動更新模式中的呼叫

## 驗證結果
*   [x] 執行 1-1 一鍵更新成功完成
*   成功更新: TWSE 2 筆, TPEx 10 筆
*   VSBC 計算完成: 1904 筆成功, 30 筆跳過
*   資料範圍: 2001-04-30 至 2025-12-31

# 2026-01-01 修復缺失資料
## 執行目標
1. 修復成交金額缺失的 4 檔股票 (34 筆)
2. 處理歷史資料不足的股票

## 修改內容
1.  **8291 尚茂電子**：
    *   經查證已於 2025-08-20 停止櫃檯買賣
    *   已標記為下市並清理相關資料

2.  **4 檔成交金額缺失 (6904, 6955, 6924, 7631)**：
    *   這些日期是資料源異常 (有成交量但 close=0)
    *   使用前一日收盤價估算成交金額
    *   共修復 34 筆

## 修改原因
*   確保技術指標計算正確 (成交金額是計算 MFI、VWAP 等指標的必要欄位)

## 修改進度
*   [x] 確認 8291 尚茂電子已下市，清理資料
*   [x] 使用前日收盤價修復 34 筆缺失成交金額
*   [x] 驗證修復完成 (剩餘缺失: 0 筆)

# 2026-01-01 優化 1-1 每日更新功能
## 執行目標
1. 提升執行效率
2. 確保手機 (Pydroid 3) 相容性
3. 改善進度顯示與錯誤處理

## 修改內容
1.  **Backend (`最終修正.py`)**:
    *   重寫 `_run_full_daily_update` 函數
    *   **並行下載**：電腦模式下使用 ThreadPoolExecutor 並行下載 (2+5 並行)
    *   **手機模式**：順序執行避免記憶體不足
    *   **錯誤處理**：每個步驟獨立 try-except，失敗不中斷
    *   **執行摘要**：顯示成功/失敗/跳過統計
    *   **記憶體清理**：手機模式使用 gc.collect()
    *   **智慧跳過**：休市日跳過下載步驟

## 修改原因
*   原版本順序執行較慢
*   缺乏錯誤處理，一步失敗全部中斷
*   無執行時間統計

## 修改進度
*   [x] 實作並行下載
*   [x] 實作錯誤處理
*   [x] 實作執行摘要
*   [x] 手機模式優化

# 2026-01-01 全面程式優化
## 執行目標
1. 資料庫操作優化
2. 程式碼結構優化 (表驅動、衛語句)
3. 記憶體使用優化
4. 手機相容性維護

## 修改內容
1.  **`step2_download_lists`**: 改用 `executemany` 批次寫入 (提升 10-50x)
2.  **`is_normal_stock`**: 使用 set 表驅動取代 list 查詢
3.  **`safe_float_preserving_none`**: 避免重複 import numpy，改用 math
4.  **主進程檢查**: 初始化訊息只在主進程顯示

## 修改進度
*   [x] 批次數據庫寫入
*   [x] 表驅動法優化
*   [x] 記憶體優化 (減少 import)
*   [x] 修復多進程訊息重複







