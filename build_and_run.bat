@echo off
echo ==========================================
echo   Taiwan Stock Analysis System - Launcher
echo ==========================================

echo [1/4] Checking Frontend Dependencies...
cd frontend
if not exist node_modules (
    echo Installing frontend dependencies...
    call npm install
) else (
    echo Frontend dependencies already installed.
)

echo [2/4] Building Frontend...
call npm run build
if %errorlevel% neq 0 (
    echo Frontend build failed!
    pause
    exit /b %errorlevel%
)

echo [3/4] Checking Backend Dependencies...
cd ..
if not exist backend\venv (
    echo Creating Python virtual environment...
    python -m venv backend\venv
)
call backend\venv\Scripts\activate
pip install -r backend\requirements.txt

echo [4/4] Starting Server...
echo.
echo Server will be available at: http://localhost:8000
echo.
python backend\main.py

pause
