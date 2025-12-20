@echo off
echo ========================================
echo 台灣股市分析系統 - 後端啟動
echo ========================================

cd /d %~dp0backend

if not exist venv (
    echo 建立虛擬環境...
    python -m venv venv
)

call venv\Scripts\activate

echo 安裝依賴...
pip install -r requirements.txt -q

echo.
echo 啟動 API 伺服器...
echo API 文件: http://localhost:8000/docs
echo.
uvicorn main:app --reload --port 8000
