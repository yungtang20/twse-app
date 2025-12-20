# 變更提案：將 CLI 轉換為網頁應用程式

## 為什麼 (Why)
目前的 Python CLI 系統雖然功能強大，但在手機上操作不便，且缺乏圖形化介面。使用者需要隨時隨地透過手機存取股市分析，並查看互動式圖表，而不需要在電腦上執行腳本。

## 變更內容 (What Changes)
- **前端 (Frontend)**: 使用 React 網頁介面取代 CLI 選單 (包含儀表板、掃描器、分析頁)。
- **後端 (Backend)**: 將現有的 Python 邏輯封裝為 FastAPI 伺服器。
- **資料庫 (Database)**: 整合 Supabase 以實現雲端同步與遠端存取。
- **自動化 (Automation)**: 透過 GitHub Actions 或本地排程器實作每日自動更新。

## 影響範圍 (Impact)
- **受影響規格**: `web-app`, `data-sync`, `automation`
- **受影響程式碼**: `frontend/`, `backend/`, `最終修正.py`
