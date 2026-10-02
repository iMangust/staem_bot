@echo off
chcp 65001 >nul
title Steam Free Games Bot v2
cd /d "%~dp0"
echo ============================================
echo   Steam Free Games Bot v2 (stable)
echo ============================================
:loop
python steam_bot_v2.py
if errorlevel 3 goto crash
if "%errorlevel%"=="2" goto fatal
goto restart

:fatal
echo.
echo [FATAL] Configuration error (invalid Telegram token etc.).
echo         Fix the .env file and start again. Window stays open.
pause
exit /b 2

:crash
set EC=%errorlevel%
echo.
echo [CRASH] Bot exited with code %EC%. Full traceback is above and in steam_bot.log.
pause
exit /b %EC%

:restart
echo.
echo Bot stopped (code %errorlevel%). Restart in 5 sec... Ctrl+C to exit.
timeout /t 5 /nobreak >nul
goto loop
