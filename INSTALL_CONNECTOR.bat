@echo off
cd /d "%~dp0"
echo Installing the optional unofficial read-only broker connector in a separate environment.
py -3 -c "import sys; assert sys.version_info >= (3,12), 'Python 3.12 or newer is required'"
if errorlevel 1 goto failed
py -3 -m venv .venv
if errorlevel 1 goto failed
.venv\Scripts\python.exe -m pip install -r requirements-connector.txt
if errorlevel 1 goto failed
echo Connector installed. Close the offline app, then run CONNECT_QUOTEX.bat.
pause
exit /b 0
:failed
echo Installation failed. Keep the error text and share it with credentials removed.
pause
exit /b 1
