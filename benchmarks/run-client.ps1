# Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
# SPDX-License-Identifier: MPL-2.0
# culvert is licensed under the Mozilla Public License Version 2.0.

# PowerShell: Start Culvert Benchmark Client
param(
    [string]$Config = "$PSScriptRoot/client-bench.yaml"
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot/.."
Set-Location $ProjectRoot

$BinPath = "$ProjectRoot/target/release/culvert.exe"
if (-not (Test-Path $BinPath)) {
    Write-Host "[*] Compiling culvert in release mode..." -ForegroundColor Cyan
    cargo build --release
}

Write-Host "[*] Starting culvert client with config: $Config" -ForegroundColor Cyan
Write-Host "[*] Benchmark ingress tunnel port: 127.0.0.1:15201" -ForegroundColor Yellow
& $BinPath -c $Config
