# PowerShell: Start Micro-Tunnel Benchmark Client
param(
    [string]$Config = "$PSScriptRoot/client-bench.yaml"
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot/.."
Set-Location $ProjectRoot

$BinPath = "$ProjectRoot/target/release/micro-tunnel.exe"
if (-not (Test-Path $BinPath)) {
    Write-Host "[*] Compiling micro-tunnel in release mode..." -ForegroundColor Cyan
    cargo build --release
}

Write-Host "[*] Starting micro-tunnel client with config: $Config" -ForegroundColor Cyan
Write-Host "[*] Benchmark ingress tunnel port: 127.0.0.1:15201" -ForegroundColor Yellow
& $BinPath -c $Config
