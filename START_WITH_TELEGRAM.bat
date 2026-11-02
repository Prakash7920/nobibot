@echo off
cd /d "%~dp0"
echo Token entry below is visible locally. Do not screen-share this terminal.
set /p NOBI_TELEGRAM_TOKEN=Telegram bot token: 
set /p NOBI_TELEGRAM_CHAT_ID=Destination chat ID: 
py -3 app.py
set NOBI_TELEGRAM_TOKEN=
set NOBI_TELEGRAM_CHAT_ID=
pause
