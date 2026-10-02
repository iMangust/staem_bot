@echo off
chcp 65001 >nul
title Steam Free Games Bot v2
cd /d "%~dp0"
echo ============================================
echo   Steam Free Games Bot v2 (stable)
echo ============================================
:loop
python steam_bot_v2.py
echo.
echo Bot stopped (code %errorlevel%). Restart in 5 sec... Ctrl+C to exit.
timeout /t 5 /nobreak >nul
goto loop
