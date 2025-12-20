# 台灣股市分析系統 v5.0 Web - 網頁版開發規格書

> **專案名稱**: TWSE Stock Analysis Web App
> **版本**: v5.0 Web (網頁應用架構版)
> **更新日期**: 2025-12-21
> **核心架構**: React (Frontend) + FastAPI (Backend) + SQLite (Database)

---

## 📋 目錄

1. [系統架構與設計原則](#系統架構與設計原則)
2. [網頁介面結構 (UI/UX)](#網頁介面結構-uiux)
3. [功能模組規格](#功能模組規格)
    - [Dashboard (儀表板)](#1-dashboard-儀表板)
    - [Data Management (資料管理)](#2-data-management-資料管理)
    - [Market Scan (市場掃描)](#3-market-scan-市場掃描)
    - [Institutional Ranking (法人排行)](#4-institutional-ranking-法人排行)
    - [Stock Analysis (個股分析)](#5-stock-analysis-個股分析)
    - [System Settings (系統設定)](#6-system-settings-系統設定)
4. [API 規格概覽](#api-規格概覽)
5. [開發規範與規則](#開發規範與規則)

---

## 系統架構與設計原則

### 技術堆疊 (Tech Stack)
-   **Frontend**: React 18, Vite, TailwindCSS, Recharts/Plotly.js (圖表), Framer Motion (動畫)
-   **Backend**: Python FastAPI, Pydantic, SQLAlchemy/Raw SQL
-   **Database**: SQLite (單一寫入員模式 Single Writer Pattern)
-   **Task Queue**: BackgroundTasks (FastAPI原生) 或簡易 ThreadPool

### 資料庫策略 (Database Strategy)
> **核心原則**: 直接沿用現有 `taiwan_stock.db`，無需遷移資料。

1.  **共用資料庫**: Web Backend 直接連接現有的 SQLite 檔案。
2.  **併發控制 (Concurrency)**:
    -   採用 **Single Writer Pattern (單一寫入員模式)**。
    -   所有「寫入」操作 (更新股價、計算指標) 統一由一個背景執行緒處理，避免 `Database Locked` 錯誤。
    -   所有「讀取」操作 (API 查詢) 可多執行緒並行，確保網頁回應速度。
3.  **資料表擴充 (Future)**:
    -   初期沿用既有 Schema。
    -   **新增資料表**:
        -   `web_watchlists`: 儲存使用者自選股清單 (id, name, stock_codes)。
        -   `web_settings`: 儲存使用者偏好設定 (theme, default_strategy, notifications)。

### 雲端同步與自動化 (Cloud & Automation)
1.  **雲端資料庫 (Supabase)**:
    -   **角色**: 異地備份與遠端存取 (Read-Only Replica)。
    -   **同步機制**: 每日更新完成後，自動觸發 `step8_sync_supabase` 將差異資料上傳。
    -   **優勢**: 讓手機 App 或其他裝置可直接讀取最新數據，不需連回本地電腦。

2.  **排程自動化 (Scheduler)**:
    -   **工具**: 使用 `APScheduler` (Advanced Python Scheduler) 整合於 FastAPI 中。
    -   **任務**: 每日 15:30 (台股收盤後) 自動執行全量更新 (Steps 1-8)。
    -   **失敗重試**: 若更新失敗，自動重試 3 次並發送通知 (Line/Discord Webhook)。

### 設計風格 (Design Aesthetics)
-   **主題**: 深色模式 (Dark Mode) 為主，營造專業金融終端機質感。
-   **配色**:
    -   **漲 (Up)**: 紅色 (#EF4444 / Red-500)
    -   **跌 (Down)**: 綠色 (#10B981 / Green-500) - *符合台股習慣*
    -   **背景**: 深灰/黑 (#0F172A / Slate-900)
    -   **卡片**: 半透明玻璃質感 (Glassmorphism)
-   **互動**:
    -   Hover 效果明顯
    -   數據載入使用 Skeleton Loading
    -   即時搜尋 (Instant Search)

---

## 網頁介面結構 (UI/UX)

### 全域導航 (Global Navigation)
採用 **左側側邊欄 (Sidebar)** 設計，包含以下項目：

| 圖示 | 名稱 | 路由 | 說明 |
|-----|------|------|------|
| 🏠 | **總覽 (Dashboard)** | `/` | 市場概況、自選股快照 |
| 📊 | **市場掃描 (Scanner)** | `/scan` | 各類技術指標篩選器 (VP, MFI, MA...) |
| 🏆 | **法人排行 (Ranking)** | `/ranking` | 外資/投信/自營商買賣超排行 |
| 📈 | **個股分析 (Analysis)** | `/stock/:id` | K線圖、詳細指標、籌碼分析 |
| ⚙️ | **資料管理 (Data)** | `/admin` | 數據更新、API 狀態、系統維護 |

### 頂部導航 (Top Bar)
-   **全域搜尋框**: 輸入代號或名稱 (如 "2330", "台積電")，下拉選單即時顯示結果，點擊跳轉至個股分析頁。
-   **系統狀態燈**: 顯示後端 API 連線狀態 (綠燈/紅燈)。

---

## 功能模組規格

### [1] Dashboard (儀表板)
**路由**: `/`

#### 功能區塊
1.  **大盤指數卡片**:
    -   加權指數 (TSE) 與 櫃買指數 (OTC) 的即時點數、漲跌幅、成交量。
    -   簡易走勢圖 (Mini Chart)。
2.  **市場寬度 (Market Breadth)**:
    -   上漲/下跌家數比例圖。
    -   資金流向分佈 (電子/金融/傳產)。
3.  **自選股觀察 (Watchlist)**:
    -   **多群組管理**: 使用者可建立多個群組 (如 "核心持股", "觀察名單")。
    -   **拖拉排序**: 支援 Drag-and-Drop 調整順序。
    -   **即時監控**: 顯示欄位包含 代號、名稱、現價、漲跌幅、量能比、法人買賣超(近1日)。
    -   **操作**: 快速新增/刪除，點擊跳轉分析頁。

---

### [2] Data Management (資料管理)
**路由**: `/admin`
**對應原 CLI**: `[1] 資料管理與更新`

#### 介面設計
-   **一鍵更新面板**:
    -   巨大按鈕「執行每日全量更新」。
    -   **進度條 (Progress Bar)**: 顯示目前步驟 (Step 1-7) 與詳細百分比。
    -   **即時日誌視窗 (Log Console)**: 顯示後端回傳的執行細節 (WebSocket 或 Polling)。
-   **分項維護工具**:
    -   卡片式佈局，每個步驟獨立一個卡片 (Step 1 更新清單, Step 2 下載 TPEx...)。
    -   每個卡片上有「執行」按鈕與「上次更新時間」。
-   **資料庫狀態**:
    -   顯示 DB 大小、總筆數、最後備份時間。
    -   **雲端同步狀態**: 顯示 Supabase 連線狀態與最後同步時間。
    -   功能按鈕: 「備份資料庫」、「清理下市股票」、「手動同步雲端」。

#### API 需求
-   `POST /api/update/full`: 觸發全量更新。
-   `POST /api/update/step/{step_id}`: 觸發單一步驟。
-   `GET /api/system/status`: 取得目前任務狀態與進度。

---

### [3] Market Scan (市場掃描)
**路由**: `/scan`
**對應原 CLI**: `[2] 市場掃描`

#### 介面設計
-   **策略選擇器 (Tabs/Sidebar)**:
    -   VP 籌碼分布 (支撐/壓力)
    -   MFI 資金流向
    -   均線策略 (多頭/乖離)
    -   KD/MACD 指標
    -   VSBC 籌碼策略
    -   聰明錢 (Smart Money)
    -   2560 戰法
    -   五階篩選 (Five Filters)
-   **互動式資料表格 (Data Grid)**:
    -   **排序**: 點擊標題排序 (如依「量能比」由大到小)。
    -   **篩選**: 欄位過濾器 (如「收盤價 > 100」)。
    -   **操作**: 點擊列 (Row) 跳轉至個股分析頁。
    -   **視覺化欄位**:
        -   漲跌幅: 紅/綠顏色標示。
        -   VP位置: 進度條顯示 (接近下緣/上緣)。
        -   量能比: 數字 + 顏色 (大於 1.5x 亮紅)。

#### API 需求
-   `GET /api/scan/vp`: 取得 VP 掃描結果。
-   `GET /api/scan/mfi`: 取得 MFI 掃描結果。
-   `GET /api/scan/strategies/{strategy_name}`: 通用策略端點。

---

### [4] Institutional Ranking (法人排行)
**路由**: `/ranking`
**對應原 CLI**: `[3] 法人買賣超排行`

#### 介面設計
-   **篩選控制列**:
    -   **對象**: 外資 / 投信 / 自營商 (Toggle Button)。
    -   **方向**: 買超 / 賣超。
    -   **排序依據**: 張數 / 金額。
    -   **期間**: 1日 / 3日 / 5日 / 10日 / 連續 N 日。
-   **排行榜表格**:
    -   顯示排名 (1, 2, 3...)。
    -   包含「佔股本比重」或「佔成交量比重」等進階欄位。

#### API 需求
-   `GET /api/ranking/institutional`: 參數 `type=foreign`, `days=1`, `sort=amount`。

---

### [5] Stock Analysis (個股分析)
**路由**: `/stock/{symbol}` (如 `/stock/2330`)
**對應原 CLI**: `個股查詢`

#### 介面設計
-   **股票頭部資訊 (Header)**:
    -   代號/名稱 (2330 台積電)。
    -   即時報價 (大字體)、漲跌幅、成交量。
    -   產業類別、本益比、股價淨值比。
-   **K線圖表 (Interactive Chart)**:
    -   使用 Plotly.js 或 TradingView Lightweight Charts。
    -   功能: 縮放、平移、切換週期 (日/周/月)。
    -   疊加指標: MA, Bollinger Bands, SAR。
    -   副圖指標: Volume, KD, MACD, RSI, MFI。
-   **詳細數據卡片 (Grid Layout)**:
    -   **籌碼分析**: 三大法人近 10 日買賣超長條圖。
    -   **主力動向**: 集保戶數變化趨勢圖。
    -   **融資融券**: 資券餘額變化圖。
    -   **關鍵價位**: VP 籌碼大量區、近期高低點。
    -   **基本面**: 營收、EPS (若有資料)。

#### API 需求
-   `GET /api/stock/{symbol}/quote`: 即時報價。
-   `GET /api/stock/{symbol}/history`: 歷史 K 線資料 (OHLCV)。
-   `GET /api/stock/{symbol}/indicators`: 技術指標數據。
-   `GET /api/stock/{symbol}/chips`: 籌碼數據 (法人/資券/集保)。

---

### [6] System Settings (系統設定)
**路由**: `/settings` (或合併於 `/admin`)
**對應原 CLI**: `[4] 系統維護`

#### 功能
-   **API 連線檢查**: 測試 TWSE, TPEx, Supabase 連線。
-   **使用者偏好 (User Preferences)**:
    -   **主題設定**: 切換 深色 (Dark) / 淺色 (Light) / 系統跟隨。
    -   **預設首頁**: 設定登入後進入 Dashboard 或 Market Scan。
    -   **通知設定**: 設定是否開啟漲跌幅警示 (Browser Notification)。
    -   **雲端設定**: 設定 Supabase URL 與 Key。
    -   **排程設定**: 設定每日自動更新時間 (預設 15:30)。
-   **參數設定 (Advanced)**:
    -   調整 `HISTORY_DAYS_LOOKBACK` (預設 3年)。
    -   調整 `MIN_VOLUME` (預設 500張)。
-   **日誌檢視**: 查看 `system.log`。

---

## API 規格概覽

所有 API 回傳格式應統一為 JSON：

```json
{
  "status": "success", // or "error"
  "data": { ... },     // 實際數據
  "message": "..."     // 提示訊息
}
```

### [7] Deployment Strategy (部署策略)
> **核心問題**: 如何確保每日 15:30 自動更新不中斷？

#### 選項 A: 本地電腦 (Local Machine)
-   **適用**: 測試階段或有 24/7 開機且網路穩定的電腦。
-   **缺點**: 電腦關機、休眠或斷網時，排程會失敗。
-   **需求**: 必須保持開機 + 連網。

#### 選項 B: 雲端主機 (VPS - Virtual Private Server)
-   **定義**: 一台位於網際網路上、24小時運作的虛擬電腦 (如 Google Cloud, AWS, Linode)。
-   **優點**:
    -   **永不斷線**: 即使您家裡停電或沒網路，它仍在運作。
    -   **自動化**: 適合放置 `APScheduler` 排程任務。
    -   **遠端存取**: 您可以用任何電腦或手機連進去操作。
-   **成本**: 每月約 $5-10 USD (也有免費方案，詳見下文)。

#### 選項 C: 免費雲端方案 (Free Tier)
如果您不想花錢，可以嘗試以下「永久免費」或「試用免費」的方案：
1.  **Google Cloud (GCP) Free Tier**: 提供 `e2-micro` 機器 (美國地區)，每月免費。需綁信用卡驗證。
2.  **Oracle Cloud Always Free**: 提供 2台 AMD VM 或 4核 ARM VM，資源給得很大方，但註冊審核較嚴。
3.  **AWS Free Tier**: 新戶首年免費使用 `t2.micro` 或 `t3.micro`。
4.  **GitHub Actions (進階)**: 雖然不是 VPS，但可以設定排程 (Cron) 每天跑一次 Script，完全免費。適合不需要存大量檔案的輕量任務。

---

### 主要 Endpoints

| Method | Endpoint | 描述 |
|--------|----------|------|
| GET | `/api/health` | 系統健康檢查 |
| GET | `/api/stocks/list` | 取得所有股票清單 (供搜尋用) |
| GET | `/api/stock/{id}/details` | 個股綜合資訊 |
| GET | `/api/watchlist` | 取得自選股清單 |
| POST | `/api/watchlist` | 新增/修改自選股 |
| GET | `/api/settings` | 取得使用者設定 |
| POST | `/api/settings` | 更新使用者設定 |
| POST | `/api/tasks/update` | 啟動更新任務 |
| GET | `/api/tasks/status` | 查詢任務進度 |
| GET | `/api/scan/{strategy}` | 執行特定策略掃描 |

---

## 開發規範與規則

### 1. 股票篩選規則 (A規則)
> **嚴格執行**: 所有「掃描」與「排行」功能，後端必須預先過濾資料。
-   **僅包含**: 普通股 (TWSE 上市 + TPEx 上櫃 + KY 公司)。
-   **排除**: ETF (00開頭)、權證、DR (91開頭)、ETN、債券、指數、創新板、特別股。
-   **實作**: 在 Backend 的 `StockRepository` 層級統一實作 `filter_a_rule()` 方法。

### 2. 數值與格式
-   **金額**: 顯示時加上千分位 (如 `1,234,567`)。
-   **小數點**: 價格保留 2 位，漲跌幅保留 2 位 (如 `+1.23%`)。
-   **空值處理**: 前端顯示 `-` 或 `N/A`，不可顯示 `null` 或 `undefined`。

### 3. 效能優化
-   **分頁 (Pagination)**: 掃描結果與排行榜若資料過多，需支援分頁或無限捲動 (Infinite Scroll)。
-   **快照 (Caching)**: 每日更新後，後端應將掃描結果計算並快取 (Cache)，避免使用者請求時才即時運算。

### 4. 錯誤處理
-   前端需優雅處理 API 錯誤 (如網路斷線、後端忙碌)，顯示友善的 Toast 通知或 Error Boundary，不可白屏。

---

*本規格書取代原 CLI 版本計畫書，作為 Web 版開發的唯一依據。*
