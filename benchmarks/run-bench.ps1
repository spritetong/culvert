# PowerShell: Run Traffic Benchmark against the Tunnel
param(
    [string]$TargetHost = "127.0.0.1",
    [int]$TargetPort = 15201,
    [int]$Streams = 4,
    [int]$DurationSec = 10,
    [int]$DataSizeMB = 200
)

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host " Micro-Tunnel Performance Benchmark Driver" -ForegroundColor Cyan
Write-Host " Target Tunnel Endpoint: ${TargetHost}:${TargetPort}" -ForegroundColor Yellow
Write-Host "==================================================================" -ForegroundColor Cyan

# 1. Prefer iperf3 if available
if (Get-Command iperf3 -ErrorAction SilentlyContinue) {
    Write-Host "[*] Executing iperf3 benchmark (-P $Streams parallel streams, $DurationSec s duration)..." -ForegroundColor Green
    iperf3 -c $TargetHost -p $TargetPort -P $Streams -t $DurationSec
    exit $LASTEXITCODE
}

# 2. Fallback to built-in high-performance compiled .NET TCP stream tester
Write-Host "[!] iperf3 not found in PATH. Using built-in compiled .NET stream tester..." -ForegroundColor Yellow
Write-Host "[*] Sending $DataSizeMB MB payload across $Streams concurrent TCP connections..." -ForegroundColor Cyan

$Code = @'
using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace MicroTunnelBench {
    public class Driver {
        public static double Run(string host, int port, int streams, long totalBytes) {
            long bytesPerStream = totalBytes / streams;
            int chunkSize = 64 * 1024;
            byte[] buffer = new byte[chunkSize];
            new Random().NextBytes(buffer);

            var sw = Stopwatch.StartNew();
            Task[] tasks = new Task[streams];
            for (int i = 0; i < streams; i++) {
                tasks[i] = Task.Run(() => {
                    try {
                        using (var client = new TcpClient()) {
                            client.NoDelay = true;
                            client.Connect(host, port);
                            using (var stream = client.GetStream()) {
                                long sent = 0;
                                while (sent < bytesPerStream) {
                                    int toWrite = (int)Math.Min((long)chunkSize, bytesPerStream - sent);
                                    stream.Write(buffer, 0, toWrite);
                                    sent += toWrite;
                                }
                                stream.Flush();
                            }
                        }
                    } catch (Exception ex) {
                        Console.WriteLine("[Stream Error] " + ex.Message);
                    }
                });
            }
            Task.WaitAll(tasks);
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }
    }
}
'@

if (-not ([System.Management.Automation.PSTypeName]'MicroTunnelBench.Driver').Type) {
    Add-Type -TypeDefinition $Code -Language CSharp
}

$TotalBytes = [int64]$DataSizeMB * 1024 * 1024
$ElapsedSec = [MicroTunnelBench.Driver]::Run($TargetHost, $TargetPort, $Streams, $TotalBytes)

if ($ElapsedSec -gt 0) {
    $ThroughputMBps = ($DataSizeMB / $ElapsedSec)
    $ThroughputGbps = ($ThroughputMBps * 8) / 1024

    Write-Host "------------------------------------------------------------------" -ForegroundColor Cyan
    Write-Host " Benchmark Results:" -ForegroundColor Green
    Write-Host " Transferred : $DataSizeMB MB in $([math]::Round($ElapsedSec, 2)) seconds"
    Write-Host " Throughput  : $([math]::Round($ThroughputMBps, 2)) MB/s ($([math]::Round($ThroughputGbps, 2)) Gbps)" -ForegroundColor Green
    Write-Host "------------------------------------------------------------------" -ForegroundColor Cyan
}
