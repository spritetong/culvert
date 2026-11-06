# Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
# SPDX-License-Identifier: MPL-2.0
# culvert is licensed under the Mozilla Public License Version 2.0.

# PowerShell: Run Traffic Benchmark against the Tunnel
param(
    [string]$TargetHost = "127.0.0.1",
    [int]$TargetPort = 15201,
    [int]$Streams = 4,
    [int]$DurationSec = 10,
    [int]$DataSizeMB = 200,
    [switch]$UseNetTester
)

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host " Culvert Performance Benchmark Driver" -ForegroundColor Cyan
Write-Host " Target Tunnel Endpoint: ${TargetHost}:${TargetPort}" -ForegroundColor Yellow
Write-Host "==================================================================" -ForegroundColor Cyan

# Pre-flight check: Ensure target tunnel ingress port is accepting connections
$isReachable = $false
try {
    $probe = New-Object System.Net.Sockets.TcpClient
    $iar = $probe.BeginConnect($TargetHost, $TargetPort, $null, $null)
    if ($iar.AsyncWaitHandle.WaitOne(2000, $false)) {
        $probe.EndConnect($iar)
        $isReachable = $true
    }
    $probe.Close()
} catch {
    $isReachable = $false
}

if (-not $isReachable) {
    Write-Host "[!] Error: Unable to connect to target endpoint ${TargetHost}:${TargetPort}." -ForegroundColor Red
    Write-Host "    Please ensure the culvert client is running (e.g. .\benchmarks\run-client.ps1)" -ForegroundColor Yellow
    exit 1
}

# Determine benchmarking engine
$engine = "net"
if (-not $UseNetTester) {
    if (Get-Command iperf3 -ErrorAction SilentlyContinue) {
        $engine = "iperf3"
    } elseif (Get-Command wsl.exe -ErrorAction SilentlyContinue) {
        $wslCheck = wsl.exe which iperf3 2>$null
        if ($LASTEXITCODE -eq 0 -and $wslCheck) {
            $engine = "wsl-iperf3"
        }
    }
}

if ($engine -eq "iperf3") {
    Write-Host "[*] Executing native iperf3 benchmark (-P $Streams parallel streams, $DurationSec s duration)..." -ForegroundColor Green
    iperf3 -c $TargetHost -p $TargetPort -P $Streams -t $DurationSec
    exit $LASTEXITCODE
} elseif ($engine -eq "wsl-iperf3") {
    Write-Host "[*] Found iperf3 in WSL. Executing WSL iperf3 benchmark (-P $Streams parallel streams, $DurationSec s duration)..." -ForegroundColor Green
    wsl.exe iperf3 -c $TargetHost -p $TargetPort -P $Streams -t $DurationSec
    exit $LASTEXITCODE
}

# Fallback to built-in high-performance compiled .NET TCP stream tester
Write-Host "[!] iperf3 not found in PATH or -UseNetTester specified." -ForegroundColor Yellow
Write-Host "[*] Using built-in compiled .NET stream tester..." -ForegroundColor Cyan
Write-Host "[*] Sending $DataSizeMB MB payload across $Streams concurrent TCP connections..." -ForegroundColor Cyan

$Code = @'
using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace CulvertBench {
    public class BenchResult {
        public double ElapsedSeconds;
        public long BytesTransferred;
        public int SuccessfulStreams;
        public int FailedStreams;
        public string LastError = "";
    }

    public class BenchDriver {
        public static BenchResult Run(string host, int port, int streams, long totalBytes) {
            var result = new BenchResult();
            long bytesPerStream = totalBytes / streams;
            int chunkSize = 64 * 1024;
            byte[] buffer = new byte[chunkSize];
            new Random().NextBytes(buffer);

            long totalSent = 0;
            int successCount = 0;
            int failCount = 0;
            string lastError = "";

            IPAddress ip;
            bool isIp = IPAddress.TryParse(host, out ip);

            var sw = Stopwatch.StartNew();
            Task[] tasks = new Task[streams];
            for (int i = 0; i < streams; i++) {
                int streamIndex = i + 1;
                tasks[i] = Task.Run(() => {
                    long streamSent = 0;
                    try {
                        TcpClient client;
                        if (isIp) {
                            client = new TcpClient(ip.AddressFamily);
                            client.NoDelay = true;
                            client.Connect(ip, port);
                        } else {
                            client = new TcpClient();
                            client.NoDelay = true;
                            client.Connect(host, port);
                        }

                        using (client) {
                            using (var stream = client.GetStream()) {
                                while (streamSent < bytesPerStream) {
                                    int toWrite = (int)Math.Min((long)chunkSize, bytesPerStream - streamSent);
                                    stream.Write(buffer, 0, toWrite);
                                    streamSent += toWrite;
                                    Interlocked.Add(ref totalSent, toWrite);
                                }
                                stream.Flush();

                                try {
                                    client.Client.Shutdown(SocketShutdown.Send);
                                    client.ReceiveTimeout = 2000;
                                    byte[] drain = new byte[4096];
                                    while (stream.Read(drain, 0, drain.Length) > 0) {}
                                } catch {
                                }
                            }
                        }
                        Interlocked.Increment(ref successCount);
                    } catch (Exception ex) {
                        Interlocked.Increment(ref failCount);
                        lastError = ex.Message;
                        Console.WriteLine("[Stream " + streamIndex + " Error] " + ex.Message);
                    }
                });
            }
            Task.WaitAll(tasks);
            sw.Stop();

            result.ElapsedSeconds = sw.Elapsed.TotalSeconds;
            result.BytesTransferred = Interlocked.Read(ref totalSent);
            result.SuccessfulStreams = successCount;
            result.FailedStreams = failCount;
            result.LastError = lastError;
            return result;
        }
    }
}
'@

if (-not ([System.Management.Automation.PSTypeName]'CulvertBench.BenchDriver').Type) {
    Add-Type -TypeDefinition $Code -Language CSharp
}

$TotalBytes = [int64]$DataSizeMB * 1024 * 1024
$result = [CulvertBench.BenchDriver]::Run($TargetHost, $TargetPort, $Streams, $TotalBytes)

if ($result.BytesTransferred -eq 0 -or $result.SuccessfulStreams -eq 0) {
    Write-Host "[-] Benchmark failed: 0 bytes transferred across all streams." -ForegroundColor Red
    if ($result.LastError) {
        Write-Host "    Last Error: $($result.LastError)" -ForegroundColor Red
    }
    exit 1
}

$ActualMB = [math]::Round($result.BytesTransferred / (1024 * 1024), 2)
if ($result.ElapsedSeconds -gt 0) {
    $ThroughputMBps = $ActualMB / $result.ElapsedSeconds
    $ThroughputGbps = ($ThroughputMBps * 8) / 1024

    Write-Host "------------------------------------------------------------------" -ForegroundColor Cyan
    Write-Host " Benchmark Results:" -ForegroundColor Green
    Write-Host " Streams      : $($result.SuccessfulStreams)/$Streams completed ($($result.FailedStreams) failed)"
    Write-Host " Transferred  : $ActualMB MB in $([math]::Round($result.ElapsedSeconds, 2)) seconds"
    Write-Host " Throughput   : $([math]::Round($ThroughputMBps, 2)) MB/s ($([math]::Round($ThroughputGbps, 2)) Gbps)" -ForegroundColor Green
    Write-Host "------------------------------------------------------------------" -ForegroundColor Cyan
}

if ($result.FailedStreams -gt 0) {
    Write-Host "[-] Some streams encountered errors during the benchmark." -ForegroundColor Yellow
    exit 1
}
