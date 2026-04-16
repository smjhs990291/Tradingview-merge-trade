@echo off
setlocal

REM Start Streamlit UI

set APP_DIR=%~dp0
set APP_PATH=%APP_DIR%streamlit_app.py

set PORT=
for %%P in (10251 10252 10253 10254 10255 10256 10257 10258 10259 10260) do (
  powershell -NoProfile -Command "try { $l=[System.Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,%%P); $l.Start(); $l.Stop(); exit 0 } catch { exit 1 }" >nul 2>&1
  if not errorlevel 1 (
    set PORT=%%P
    goto :run
  )
)

echo No available port found in range 10251-10260
pause
exit /b 1

:run

echo Starting Streamlit UI...
echo If this is the first time, ensure dependencies are installed:
echo   pip install -r "%APP_DIR%requirements.txt"
echo.

echo URL: http://127.0.0.1:%PORT%
start "" "http://127.0.0.1:%PORT%"
python -m streamlit run "%APP_PATH%" --server.address 127.0.0.1 --server.port %PORT%

endlocal
