@echo off
echo Starting TWSE Mobile Server...

:: Find Local IP
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr "IPv4"') do set IP=%%a
set IP=%IP: =%
echo.
echo ========================================================
echo  Mobile Access URL: http://%IP%:5173
echo ========================================================
echo.

:: Start Backend
start "TWSE Backend" cmd /k "cd backend && call venv\Scripts\activate && uvicorn main:app --host 0.0.0.0 --port 8000"

:: Start Frontend
start "TWSE Frontend" cmd /k "cd frontend && npm run dev"

echo Server started! Please type the URL above on your phone connected to the same Wi-Fi.
pause
