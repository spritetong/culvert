# Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
# SPDX-License-Identifier: MPL-2.0
# culvert is licensed under the Mozilla Public License Version 2.0.

# PowerShell: Start Culvert Benchmark Server
param(
    [string]$Config = "$PSScriptRoot/server-bench.yaml",
    [switch]$StartIperf,
    [switch]$NoSink
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot/.."
Set-Location $ProjectRoot

$BinPath = "$ProjectRoot/target/release/culvert.exe"
if (-not (Test-Path $BinPath)) {
    Write-Host "[*] Compiling culvert in release mode..." -ForegroundColor Cyan
    cargo build --release
}

# Helper to check if a TCP port is listening
function Test-PortListening([string]$HostName, [int]$Port) {
    try {
        $client = New-Object System.Net.Sockets.TcpClient
        $iar = $client.BeginConnect($HostName, $Port, $null, $null)
        if ($iar.AsyncWaitHandle.WaitOne(500, $false)) {
            $client.EndConnect($iar)
            $client.Close()
            return $true
        }
        $client.Close()
        return $false
    } catch {
        return $false
    }
}

$sinkServer = $null
$iperfProc = $null

if (-not $NoSink) {
    if (Test-PortListening "127.0.0.1" 5201) {
        Write-Host "[*] Backend service already listening on 127.0.0.1:5201" -ForegroundColor Green
    } else {
        # Check if native iperf3 is available
        if (Get-Command iperf3 -ErrorAction SilentlyContinue) {
            Write-Host "[*] Starting native iperf3 server on port 5201 in background..." -ForegroundColor Green
            $iperfProc = Start-Process iperf3 -ArgumentList "-s", "-p", "5201" -NoNewWindow -PassThru
        } elseif (Get-Command wsl.exe -ErrorAction SilentlyContinue -and ((wsl.exe which iperf3 2>$null) -and $LASTEXITCODE -eq 0)) {
            Write-Host "[*] Starting WSL iperf3 server on port 5201 in background..." -ForegroundColor Green
            $iperfProc = Start-Process wsl.exe -ArgumentList "iperf3", "-s", "-p", "5201" -NoNewWindow -PassThru
        } else {
            Write-Host "[*] iperf3 not found. Starting built-in high-performance .NET discard sink on 127.0.0.1:5201..." -ForegroundColor Green
            $SinkCode = @'
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace CulvertBench {
    public class DiscardServer {
        private TcpListener _listener;
        private CancellationTokenSource _cts;

        public void Start(string ipStr, int port) {
            _cts = new CancellationTokenSource();
            _listener = new TcpListener(IPAddress.Parse(ipStr), port);
            _listener.Start(128);

            Task.Run(async () => {
                while (!_cts.IsCancellationRequested) {
                    try {
                        var client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
                        _ = Task.Run(async () => {
                            using (client) {
                                client.NoDelay = true;
                                using (var stream = client.GetStream()) {
                                    byte[] localBuf = new byte[65536];
                                    while (!_cts.IsCancellationRequested) {
                                        int n = await stream.ReadAsync(localBuf, 0, localBuf.Length, _cts.Token).ConfigureAwait(false);
                                        if (n <= 0) break;
                                    }
                                }
                            }
                        }, _cts.Token);
                    } catch {
                        if (_cts.IsCancellationRequested) break;
                    }
                }
            });
        }

        public void Stop() {
            try {
                _cts?.Cancel();
                _listener?.Stop();
            } catch {}
        }
    }
}
'@
            if (-not ([System.Management.Automation.PSTypeName]'CulvertBench.DiscardServer').Type) {
                Add-Type -TypeDefinition $SinkCode -Language CSharp
            }
            $sinkServer = [CulvertBench.DiscardServer]::new()
            $sinkServer.Start("127.0.0.1", 5201)
            Start-Sleep -Milliseconds 100
        }
    }
}

try {
    Write-Host "[*] Starting culvert server with config: $Config" -ForegroundColor Cyan
    & $BinPath -c $Config
} finally {
    if ($sinkServer) {
        Write-Host "[*] Stopping backend discard server..." -ForegroundColor Cyan
        $sinkServer.Stop()
    }
    if ($iperfProc -and -not $iperfProc.HasExited) {
        Write-Host "[*] Stopping iperf3 server..." -ForegroundColor Cyan
        Stop-Process -Id $iperfProc.Id -Force -ErrorAction SilentlyContinue
    }
}
