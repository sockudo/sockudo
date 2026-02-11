@echo off
REM Check Windows TCP/IP Limits for High-Concurrency Testing
REM Run this to see current configuration

echo ================================================================================
echo Windows TCP/IP Configuration Check
echo ================================================================================
echo.

echo Checking dynamic port range...
echo.
netsh int ipv4 show dynamicport tcp
echo.
netsh int ipv6 show dynamicport tcp
echo.

echo ================================================================================
echo Current TCP/IP Registry Settings
echo ================================================================================
echo.

reg query "HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" /v MaxUserPort 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo MaxUserPort: Not set ^(default: ~5000^)
)
echo.

reg query "HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" /v TcpTimedWaitDelay 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo TcpTimedWaitDelay: Not set ^(default: 240 seconds^)
)
echo.

reg query "HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" /v TcpNumConnections 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo TcpNumConnections: Not set ^(default: system dependent^)
)
echo.

reg query "HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" /v MaxFreeTcbs 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo MaxFreeTcbs: Not set ^(default: system dependent^)
)
echo.

reg query "HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" /v MaxHashTableSize 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo MaxHashTableSize: Not set ^(default: system dependent^)
)
echo.

echo ================================================================================
echo Current Connection Statistics
echo ================================================================================
echo.
netstat -an | find /c "ESTABLISHED"
echo Active TCP connections:

echo.
netstat -an | find /c "TIME_WAIT"
echo Connections in TIME_WAIT:

echo.
echo ================================================================================
echo Recommendations for 10K+ Concurrent Connections
echo ================================================================================
echo.
echo 1. Run PowerShell as Administrator
echo 2. Execute: Set-ExecutionPolicy Bypass -Scope Process
echo 3. Run: .\scripts\tune-windows-limits.ps1 -Apply
echo 4. Reboot your computer
echo.
echo Or manually apply these settings:
echo.
echo   reg add "HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" /v MaxUserPort /t REG_DWORD /d 65535 /f
echo   reg add "HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" /v TcpTimedWaitDelay /t REG_DWORD /d 30 /f
echo   netsh int ipv4 set dynamicport tcp start=1025 num=64510
echo   netsh int ipv6 set dynamicport tcp start=1025 num=64510
echo.
echo ================================================================================
pause
