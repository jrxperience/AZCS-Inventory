@echo off
cd /d "%~dp0"
where pythonw >nul 2>nul
if %errorlevel%==0 (
    start "" pythonw inventory_dashboard.py
) else (
    python inventory_dashboard.py
)
