# 部署指南 (Deployment Guide)

本專案已整合前後端，可透過單一入口啟動服務。

## 本地啟動 (Windows)

最簡單的方式是直接執行根目錄下的 `build_and_run.bat` 腳本：

1. 雙擊 `build_and_run.bat`
2. 腳本會自動：
    - 安裝前端套件 (若需要)
    - 編譯前端網頁 (Build)
    - 安裝後端 Python 套件 (若需要)
    - 啟動伺服器
3. 開啟瀏覽器訪問: `http://localhost:8000`

## 雲端部署 (以 Render 為例)

本專案可部署至支援 Python 的雲端平台 (如 Render, Railway, Heroku)。

### 設定步驟

1. **Build Command (建置指令)**:
   ```bash
   # 安裝前端依賴 -> 編譯前端 -> 安裝後端依賴
   cd frontend && npm install && npm run build && cd .. && pip install -r backend/requirements.txt
   ```

2. **Start Command (啟動指令)**:
   ```bash
   # 啟動 FastAPI 伺服器
   python backend/main.py
   ```

3. **Environment Variables (環境變數)**:
   - `PYTHON_VERSION`: 3.10.0 (或更高)
   - `NODE_VERSION`: 18.0.0 (或更高)

## 手動開發模式

若要分別開發前後端，請參考 `WEB_README.md`。
