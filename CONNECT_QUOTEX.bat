@echo off
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
 echo Run INSTALL_CONNECTOR.bat first.
 pause
 exit /b 1
)
.venv\Scripts\python.exe app.py --quotex
pause
