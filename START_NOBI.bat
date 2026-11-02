@echo off
cd /d "%~dp0"
py -3 app.py
if errorlevel 1 (
  echo.
  echo App could not start. Check Python installation or whether port 8765 is in use.
)
pause
