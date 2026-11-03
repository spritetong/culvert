# PowerShell: Start Micro-Tunnel Benchmark Server
param(
    [string]$Config = "$PSScriptRoot/server-bench.yaml",
    [switch]$StartIperf
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot/.."
Set-Location $ProjectRoot

$BinPath = "$ProjectRoot/target/release/micro-tunnel.exe"
if (-not (Test-Path $BinPath)) {
    Write-Host "[*] Compiling micro-tunnel in release mode..." -ForegroundColor Cyan
    cargo build --release
}

# Check and optionally start iperf3 backend
if ($StartIperf) {
    if (Get-Command iperf3 -ErrorAction SilentlyContinue) {
        Write-Host "[*] Starting iperf3 server on port 5201 in background..." -ForegroundColor Green
        Start-Process iperf3 -ArgumentList "-s", "-p", "5201" -NoNewWindow
    } else {
        Write-Warning "[!] iperf3 not found in PATH. You can run any test backend on port 5201 manually."
    }
}

Write-Host "[*] Starting micro-tunnel server with config: $Config" -ForegroundColor Cyan
& $BinPath -c $Config
