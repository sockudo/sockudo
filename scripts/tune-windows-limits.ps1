# Windows TCP/IP Tuning Script for High-Concurrency WebSocket Servers
# Run as Administrator to configure Windows for handling 10K+ concurrent connections

param(
    [switch]$Apply,
    [switch]$Revert,
    [switch]$Check
)

$ErrorActionPreference = "Stop"

function Write-ColorOutput($Color, $Message) {
    $fc = $host.UI.RawUI.ForegroundColor
    $host.UI.RawUI.ForegroundColor = $Color
    Write-Output $Message
    $host.UI.RawUI.ForegroundColor = $fc
}

function Test-Administrator {
    $currentUser = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentUser)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Backup-RegistryKey {
    param([string]$Path)

    $backupPath = "$PSScriptRoot\registry-backup-$(Get-Date -Format 'yyyyMMdd-HHmmss').reg"
    Write-ColorOutput Yellow "Creating registry backup: $backupPath"

    try {
        reg export $Path $backupPath /y | Out-Null
        Write-ColorOutput Green "✓ Backup created successfully"
        return $backupPath
    } catch {
        Write-ColorOutput Red "Warning: Could not create backup of $Path"
        return $null
    }
}

function Get-CurrentSettings {
    Write-ColorOutput Cyan "`n=== Current TCP/IP Settings ==="

    $tcpParams = "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters"

    $settings = @{
        "MaxUserPort" = (Get-ItemProperty -Path $tcpParams -Name MaxUserPort -ErrorAction SilentlyContinue).MaxUserPort
        "TcpTimedWaitDelay" = (Get-ItemProperty -Path $tcpParams -Name TcpTimedWaitDelay -ErrorAction SilentlyContinue).TcpTimedWaitDelay
        "TcpNumConnections" = (Get-ItemProperty -Path $tcpParams -Name TcpNumConnections -ErrorAction SilentlyContinue).TcpNumConnections
        "MaxFreeTcbs" = (Get-ItemProperty -Path $tcpParams -Name MaxFreeTcbs -ErrorAction SilentlyContinue).MaxFreeTcbs
        "MaxHashTableSize" = (Get-ItemProperty -Path $tcpParams -Name MaxHashTableSize -ErrorAction SilentlyContinue).MaxHashTableSize
    }

    foreach ($key in $settings.Keys) {
        $value = $settings[$key]
        if ($null -eq $value) {
            Write-Output "  $key : (default)"
        } else {
            Write-Output "  $key : $value"
        }
    }

    Write-Output ""
    return $settings
}

function Apply-Optimizations {
    Write-ColorOutput Cyan "`n=== Applying TCP/IP Optimizations for High-Concurrency ==="
    Write-Output ""

    if (-not (Test-Administrator)) {
        Write-ColorOutput Red "ERROR: This script must be run as Administrator!"
        Write-Output "Right-click PowerShell and select 'Run as Administrator'"
        exit 1
    }

    $tcpParams = "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters"

    # Backup current settings
    Backup-RegistryKey "HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters"

    Write-ColorOutput Yellow "`nApplying optimizations..."
    Write-Output ""

    # 1. Increase dynamic port range (default: 1024-5000, we set to 1024-65535)
    Write-Output "1. MaxUserPort: Expanding dynamic port range to 65535"
    Set-ItemProperty -Path $tcpParams -Name MaxUserPort -Value 65535 -Type DWord

    # 2. Reduce TIME_WAIT delay (default: 240 seconds, we set to 30 seconds)
    Write-Output "2. TcpTimedWaitDelay: Reducing TIME_WAIT to 30 seconds"
    Set-ItemProperty -Path $tcpParams -Name TcpTimedWaitDelay -Value 30 -Type DWord

    # 3. Increase max connections (default: limited, we set to 16777214)
    Write-Output "3. TcpNumConnections: Setting max connections to 16777214"
    Set-ItemProperty -Path $tcpParams -Name TcpNumConnections -Value 16777214 -Type DWord

    # 4. Increase TCB (Transmission Control Block) pool
    Write-Output "4. MaxFreeTcbs: Increasing TCB pool to 65536"
    Set-ItemProperty -Path $tcpParams -Name MaxFreeTcbs -Value 65536 -Type DWord

    # 5. Increase TCP hash table size
    Write-Output "5. MaxHashTableSize: Increasing hash table to 65536"
    Set-ItemProperty -Path $tcpParams -Name MaxHashTableSize -Value 65536 -Type DWord

    # 6. Use netsh to configure additional limits
    Write-Output "`n6. Configuring dynamic port range via netsh..."
    try {
        netsh int ipv4 set dynamicport tcp start=1025 num=64510 | Out-Null
        Write-ColorOutput Green "   ✓ IPv4 dynamic ports configured"

        netsh int ipv6 set dynamicport tcp start=1025 num=64510 | Out-Null
        Write-ColorOutput Green "   ✓ IPv6 dynamic ports configured"
    } catch {
        Write-ColorOutput Yellow "   Warning: Could not configure netsh settings"
    }

    Write-Output ""
    Write-ColorOutput Green "=== Optimizations Applied Successfully ==="
    Write-Output ""
    Write-ColorOutput Yellow "⚠️  IMPORTANT: You must RESTART YOUR COMPUTER for changes to take effect!"
    Write-Output ""
    Write-Output "After reboot, you should be able to handle 10K+ concurrent connections."
    Write-Output ""
}

function Revert-Optimizations {
    Write-ColorOutput Cyan "`n=== Reverting TCP/IP Settings to Defaults ==="
    Write-Output ""

    if (-not (Test-Administrator)) {
        Write-ColorOutput Red "ERROR: This script must be run as Administrator!"
        exit 1
    }

    $tcpParams = "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters"

    # Backup before reverting
    Backup-RegistryKey "HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters"

    Write-ColorOutput Yellow "Removing custom settings..."
    Write-Output ""

    try {
        Remove-ItemProperty -Path $tcpParams -Name MaxUserPort -ErrorAction SilentlyContinue
        Remove-ItemProperty -Path $tcpParams -Name TcpTimedWaitDelay -ErrorAction SilentlyContinue
        Remove-ItemProperty -Path $tcpParams -Name TcpNumConnections -ErrorAction SilentlyContinue
        Remove-ItemProperty -Path $tcpParams -Name MaxFreeTcbs -ErrorAction SilentlyContinue
        Remove-ItemProperty -Path $tcpParams -Name MaxHashTableSize -ErrorAction SilentlyContinue

        Write-ColorOutput Green "✓ Registry settings reverted to defaults"
    } catch {
        Write-ColorOutput Red "Error reverting settings: $_"
    }

    # Reset netsh settings
    try {
        netsh int ipv4 reset | Out-Null
        netsh int ipv6 reset | Out-Null
        Write-ColorOutput Green "✓ Network stack reset to defaults"
    } catch {
        Write-ColorOutput Yellow "Warning: Could not reset network stack"
    }

    Write-Output ""
    Write-ColorOutput Green "=== Settings Reverted ==="
    Write-Output ""
    Write-ColorOutput Yellow "⚠️  RESTART YOUR COMPUTER for changes to take effect!"
    Write-Output ""
}

# Main script logic
Write-ColorOutput Cyan @"

╔═══════════════════════════════════════════════════════════════════╗
║  Windows TCP/IP Tuning for High-Concurrency WebSocket Servers    ║
║  Sockudo Performance Optimization                                 ║
╚═══════════════════════════════════════════════════════════════════╝

"@

if ($Check -or (-not $Apply -and -not $Revert)) {
    Get-CurrentSettings

    Write-Output ""
    Write-ColorOutput Cyan "=== Recommended Actions ==="
    Write-Output ""
    Write-Output "To optimize for 10K+ concurrent connections:"
    Write-Output "  .\tune-windows-limits.ps1 -Apply"
    Write-Output ""
    Write-Output "To revert to default settings:"
    Write-Output "  .\tune-windows-limits.ps1 -Revert"
    Write-Output ""
    Write-Output "To check current settings:"
    Write-Output "  .\tune-windows-limits.ps1 -Check"
    Write-Output ""

    Write-ColorOutput Yellow "Note: You must run PowerShell as Administrator"
    Write-Output ""
}

if ($Apply) {
    Apply-Optimizations
}

if ($Revert) {
    Revert-Optimizations
}
