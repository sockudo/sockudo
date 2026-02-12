@echo off
REM Simple Subscription Profiling Script for Windows
REM Profiles Sockudo during k6-burst.js benchmark

setlocal EnableDelayedExpansion

echo ========================================
echo Sockudo Subscription Profiling
echo ========================================
echo.

REM Check for cargo
where cargo >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Error: cargo not found
    exit /b 1
)

REM Check for k6
where k6 >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Error: k6 not found. Install from https://k6.io/docs/get-started/installation/
    exit /b 1
)

REM Build with debug symbols for profiling
echo Building Sockudo with profiling symbols...
set CARGO_PROFILE_RELEASE_DEBUG=true
cargo build --release --features local
if %ERRORLEVEL% NEQ 0 (
    echo Build failed
    exit /b 1
)

REM Create output directory
set TIMESTAMP=%date:~-4%%date:~-7,2%%date:~-10,2%-%time:~0,2%%time:~3,2%%time:~6,2%
set TIMESTAMP=%TIMESTAMP: =0%
set PROFILE_DIR=profile-results-%TIMESTAMP%
mkdir "%PROFILE_DIR%" 2>nul

echo.
echo Profile results will be saved to: %PROFILE_DIR%
echo.

REM Start Sockudo in background
echo Starting Sockudo...
start /B "" target\release\sockudo.exe --config config\config.json
timeout /t 10 /nobreak >nul

echo Sockudo started
echo.

REM Run k6 benchmark
echo Running k6 burst test (10,000 concurrent subscriptions)...
cd benchmark
k6 run k6-burst.js --out json="..\%PROFILE_DIR%\k6-results.json" > "..\%PROFILE_DIR%\k6-output.txt" 2>&1
type "..\%PROFILE_DIR%\k6-output.txt"
cd ..

echo.
echo k6 test completed!
echo.

REM Stop Sockudo
echo Stopping Sockudo...
taskkill /IM sockudo.exe /F >nul 2>nul
timeout /t 2 /nobreak >nul

echo.
echo ========================================
echo Profiling Complete
echo ========================================
echo.
echo Results saved to: %PROFILE_DIR%
echo   - k6-results.json (detailed k6 metrics)
echo   - k6-output.txt (k6 console output)
echo.
echo For detailed CPU profiling, consider:
echo   1. Visual Studio Profiler
echo   2. Windows Performance Recorder (WPR + WPA)
echo   3. dotTrace (JetBrains)
echo   4. samply (cargo install samply)
echo.
echo Key functions to analyze:
echo   - handle_subscribe_request
echo   - execute_subscription
echo   - ChannelManager::subscribe
echo   - Namespace::add_channel_to_socket
echo   - ConnectionManager operations
echo.

REM Show quick summary
if exist "%PROFILE_DIR%\k6-output.txt" (
    echo ========================================
    echo Quick Summary
    echo ========================================
    findstr /C:"subscription_time_ms" /C:"subscription_success_rate" "%PROFILE_DIR%\k6-output.txt"
)

echo.
echo Done!
