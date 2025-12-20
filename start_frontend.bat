@echo off
echo ========================================
echo 台灣股市分析系統 - 前端啟動
echo ========================================

cd /d %~dp0frontend

if not exist node_modules (
    echo 安裝依賴...
    npm install
)

echo.
echo 啟動開發伺服器...
echo 網頁: http://localhost:5173
echo.
npm run dev
