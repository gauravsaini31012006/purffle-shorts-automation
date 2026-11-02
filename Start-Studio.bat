@echo off
setlocal
cd /d "%~dp0"

where uv >nul 2>&1
if errorlevel 1 (
    echo [ERROR] uv was not found. Install uv and reopen this window.
    pause
    exit /b 1
)

uv run purffle-shorts studio
if errorlevel 1 (
    echo.
    echo [ERROR] Studio stopped with an error. Review the message above.
    pause
)
