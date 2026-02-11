# Subscription Profiling Script for Windows
# Profiles Sockudo during k6-burst.js benchmark to identify bottlenecks

$ErrorActionPreference = "Stop"

function Write-ColorOutput($ForegroundColor) {
    $fc = $host.UI.RawUI.ForegroundColor
    $host.UI.RawUI.ForegroundColor = $ForegroundColor
    if ($args) {
        Write-Output $args
    }
    $host.UI.RawUI.ForegroundColor = $fc
}

Write-ColorOutput Green "=== Sockudo Subscription Profiling (Windows) ==="
Write-Output ""

# Check dependencies
Write-ColorOutput Yellow "Checking dependencies..."

if (!(Get-Command cargo -ErrorAction SilentlyContinue)) {
    Write-ColorOutput Red "Error: cargo not found"
    exit 1
}

if (!(Get-Command k6 -ErrorAction SilentlyContinue)) {
    Write-ColorOutput Red "Error: k6 not found. Install from https://k6.io/docs/get-started/installation/"
    exit 1
}

# Check for cargo-profiler or samply
$profilerAvailable = $false
$profilerCmd = ""

if (Get-Command samply -ErrorAction SilentlyContinue) {
    $profilerAvailable = $true
    $profilerCmd = "samply"
    Write-ColorOutput Green "Found samply profiler"
} elseif (Get-Command cargo-profiler -ErrorAction SilentlyContinue) {
    $profilerAvailable = $true
    $profilerCmd = "cargo-profiler"
    Write-ColorOutput Green "Found cargo-profiler"
} else {
    Write-ColorOutput Yellow "No profiler found. Installing samply (recommended for Windows)..."
    cargo install samply
    if ($LASTEXITCODE -eq 0) {
        $profilerAvailable = $true
        $profilerCmd = "samply"
    }
}

if (!$profilerAvailable) {
    Write-ColorOutput Red "Failed to install profiler. Running without profiling..."
    $profilerCmd = "none"
}

# Build with release + debug symbols
Write-ColorOutput Yellow "Building Sockudo with profiling symbols..."
$env:CARGO_PROFILE_RELEASE_DEBUG = "true"
cargo build --release --features local

if ($LASTEXITCODE -ne 0) {
    Write-ColorOutput Red "Build failed"
    exit 1
}

# Create profiling output directory
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$profileDir = "profile-results-$timestamp"
New-Item -ItemType Directory -Force -Path $profileDir | Out-Null

Write-ColorOutput Green "Profile results will be saved to: $profileDir"
Write-Output ""

# Start Sockudo with or without profiler
Write-ColorOutput Yellow "Starting Sockudo..."

if ($profilerCmd -eq "samply") {
    Write-Output "Starting with samply profiler..."
    Write-Output "Samply will open Firefox with live profiling view"
    Write-Output ""

    # Start samply in background
    $sockudoJob = Start-Job -ScriptBlock {
        param($profileDir)
        Set-Location $using:PWD
        samply record -o "$profileDir/profile.json" ./target/release/sockudo.exe --config config/config.json
    } -ArgumentList $profileDir

    $sockudoPID = $null

} elseif ($profilerCmd -eq "cargo-profiler") {
    Write-Output "Starting with cargo-profiler..."

    $sockudoJob = Start-Job -ScriptBlock {
        param($profileDir)
        Set-Location $using:PWD
        cargo profiler callgrind --release --features local --bin sockudo -- --config config/config.json
    } -ArgumentList $profileDir

    $sockudoPID = $null

} else {
    Write-Output "Starting without profiler (manual profiling)..."
    Write-Output "Use Windows Performance Recorder or VS Profiler for detailed analysis"
    Write-Output ""

    # Start without profiler
    $sockudoProcess = Start-Process -FilePath ".\target\release\sockudo.exe" -ArgumentList "--config", "config/config.json" -PassThru -NoNewWindow
    $sockudoPID = $sockudoProcess.Id
    $sockudoJob = $null
}

# Wait for Sockudo to start
Write-ColorOutput Yellow "Waiting for Sockudo to start (10 seconds)..."
Start-Sleep -Seconds 10

# Check if Sockudo is running
$running = $false
if ($sockudoPID) {
    $running = Get-Process -Id $sockudoPID -ErrorAction SilentlyContinue
} elseif ($sockudoJob) {
    $running = ($sockudoJob.State -eq "Running")
}

if (!$running) {
    Write-ColorOutput Red "Error: Sockudo failed to start"
    if ($sockudoJob) {
        Receive-Job -Job $sockudoJob
        Remove-Job -Job $sockudoJob
    }
    exit 1
}

Write-ColorOutput Green "Sockudo started successfully"
Write-Output ""

# Run k6 benchmark
Write-ColorOutput Yellow "Running k6 burst test (10,000 concurrent subscriptions)..."
Push-Location benchmark

$k6Output = k6 run k6-burst.js --out "json=../$profileDir/k6-results.json" 2>&1
$k6Output | Tee-Object -FilePath "../$profileDir/k6-output.txt"
Write-Output $k6Output

Pop-Location

Write-Output ""
Write-ColorOutput Green "k6 test completed!"
Write-ColorOutput Yellow "Waiting 5 seconds for remaining operations..."
Start-Sleep -Seconds 5

# Stop Sockudo
Write-ColorOutput Yellow "Stopping Sockudo..."

if ($sockudoPID) {
    Stop-Process -Id $sockudoPID -Force -ErrorAction SilentlyContinue
    Write-Output "Stopped process $sockudoPID"
} elseif ($sockudoJob) {
    Stop-Job -Job $sockudoJob -ErrorAction SilentlyContinue
    Wait-Job -Job $sockudoJob -Timeout 10 -ErrorAction SilentlyContinue
    Receive-Job -Job $sockudoJob -ErrorAction SilentlyContinue
    Remove-Job -Job $sockudoJob -Force -ErrorAction SilentlyContinue
    Write-Output "Stopped profiling job"
}

Start-Sleep -Seconds 2

Write-Output ""
Write-ColorOutput Green "=== Profiling Complete ==="
Write-Output ""
Write-ColorOutput Green "Results saved to: $profileDir"
Write-Output "  - k6-results.json (detailed k6 metrics)"
Write-Output "  - k6-output.txt (k6 console output)"

if ($profilerCmd -eq "samply") {
    Write-Output "  - profile.json (Firefox Profiler format)"
    Write-Output ""
    Write-ColorOutput Yellow "View the profile:"
    Write-Output "  1. Open Firefox"
    Write-Output "  2. Go to https://profiler.firefox.com"
    Write-Output "  3. Load $profileDir/profile.json"
} elseif ($profilerCmd -eq "cargo-profiler") {
    Write-Output "  - callgrind output files"
    Write-Output ""
    Write-ColorOutput Yellow "Analyze with kcachegrind or qcachegrind"
}

Write-Output ""
Write-ColorOutput Yellow "Manual profiling options:"
Write-Output "  1. Windows Performance Recorder (WPR + WPA)"
Write-Output "  2. Visual Studio Profiler"
Write-Output "  3. dotTrace/dotMemory (JetBrains)"
Write-Output ""
Write-ColorOutput Yellow "Key functions to analyze:"
Write-Output "  - handle_subscribe_request"
Write-Output "  - execute_subscription"
Write-Output "  - ChannelManager::subscribe"
Write-Output "  - ConnectionManager operations"
Write-Output ""

# Parse k6 results for quick summary
if (Test-Path "$profileDir/k6-output.txt") {
    Write-ColorOutput Yellow "=== Quick Summary from k6 ==="
    Get-Content "$profileDir/k6-output.txt" | Select-String -Pattern "(subscription_time_ms|subscription_success_rate|✅|⏱️)" | ForEach-Object { Write-Output $_.Line }
}

Write-Output ""
Write-ColorOutput Green "Done!"
