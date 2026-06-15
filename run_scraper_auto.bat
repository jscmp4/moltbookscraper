@echo off
setlocal
cd /d "%~dp0"

:: ============================================================
:: Moltbook Scraper - Adaptive Unattended Daily Run
:: Uses auto_scheduler.py to analyze backlog, pick the best
:: min-comments threshold, and auto-calibrate from past runs.
::
:: Override budget: set MOLT_BUDGET=8 (hours)
:: ============================================================

if not defined MOLT_BUDGET set MOLT_BUDGET=10

:: Log file: data/logs/auto_YYYYMMDD_HHMMSS.log
set "LOGDIR=%~dp0data\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
:: wmic is deprecated/removed on Win11 24H2+; use PowerShell for the timestamp
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "DT=%%I"
if not defined DT set "DT=unknown_%RANDOM%"
set "LOGFILE=%LOGDIR%\auto_%DT%.log"

echo [%date% %time%] Starting adaptive scraper run (budget=%MOLT_BUDGET%h) >> "%LOGFILE%"

:: Use the same interpreter as manual runs (PATH python). The old conda-base
:: fallback had a broken numpy/matplotlib pair that killed the dashboard step
:: nightly since 2026-04-18.
set "PY=python -X utf8"

:: Run adaptive scheduler (it calls scraper.py internally)
%PY% auto_scheduler.py --budget %MOLT_BUDGET% >> "%LOGFILE%" 2>&1

set "EXIT_CODE=%errorlevel%"
echo [%date% %time%] Finished with exit code %EXIT_CODE% >> "%LOGFILE%"

:: Single line so %EXIT_CODE% expands before endlocal clears it
endlocal & exit /b %EXIT_CODE%
