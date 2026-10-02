@echo off
chcp 65001 >nul
cd /d "%~dp0"
:loop
python steam_bot_v2.py
if %errorlevel%==2 goto end
echo.
echo Bot stopped (code %errorlevel%). Restart in 5 sec... Ctrl+C to exit.
ping -n 6 127.0.0.1 >nul
goto loop
:end
pause
