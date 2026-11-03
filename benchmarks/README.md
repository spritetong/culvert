# Micro-Tunnel Benchmark & Stress-Testing Suite

This directory contains configurations, startup scripts, and driver tools to evaluate the performance, throughput, concurrency, and latency of `micro-tunnel`.

---

## 1. Benchmark Architecture

```
[Traffic Generator]
(iperf3 / wrk / .NET driver)
       │ (TCP on 127.0.0.1:15201)
       ▼
[micro-tunnel Client]
  - Pre-warmed Pool (pool_size: 16)
  - Optional Frame Multiplexing (sing-box smux)
  - Mimalloc allocator
       │
       │ (ws://, wss://, h3://, quic:// over :8443)
       ▼
[micro-tunnel Server]
  - Token-authenticated Handshake
  - Fast-path Splice / Zero-copy Stream Copy
       │ (TCP to 127.0.0.1:5201)
       ▼
[Backend Target]
(iperf3 -s / Nginx / Echo Service)
```

---

## 2. Quickstart

### On Windows (PowerShell)

#### Step 1: Start Server
Open PowerShell Terminal 1:
```powershell
./benchmarks/run-server.ps1
```
*Note: If `iperf3.exe` is installed, it will automatically start `iperf3 -s -p 5201` in the background.*

#### Step 2: Start Client
Open PowerShell Terminal 2:
```powershell
./benchmarks/run-client.ps1
```

#### Step 3: Run Benchmark Driver
Open PowerShell Terminal 3:
```powershell
# Default test (4 streams, 10s duration or 200MB stream test):
./benchmarks/run-bench.ps1

# Custom test with 8 parallel streams:
./benchmarks/run-bench.ps1 -Streams 8 -DurationSec 15
```
*Note: If `iperf3` is not found, `run-bench.ps1` automatically uses a built-in high-performance multi-threaded `.NET` stream tester.*

---

### On Linux / macOS (Bash)

#### Step 1: Start Server
```bash
./benchmarks/run-server.sh
```

#### Step 2: Start Client
```bash
./benchmarks/run-client.sh
```

#### Step 3: Run Benchmark Driver
```bash
# iperf3 client or python fallback:
./benchmarks/run-bench.sh 127.0.0.1 15201 4 10
```

---

## 3. Benchmark Scenarios & Matrix

You can test different transport protocols and features by modifying `benchmarks/client-bench.yaml`:

### Scenario A: Raw WebSocket (`ws://`)
```yaml
endpoint: "ws://127.0.0.1:8080/tunnel"
```
*Evaluates baseline frame parsing and async proxy throughput without TLS encryption overhead.*

### Scenario B: Encrypted WebSocket (`wss://`)
```yaml
endpoint: "wss://127.0.0.1:8443/tunnel"
```
*Evaluates Rustls TLS 1.3 performance with ring/mimalloc.*

### Scenario C: HTTP/3 & QUIC (`h3://` or `quic://`)
```yaml
endpoint: "h3://127.0.0.1:8443/tunnel"
# or
endpoint: "quic://127.0.0.1:8443/raw"
```
*Evaluates Quinn QUIC engine with 0-RTT connection re-use and UDP packet pacing.*

### Scenario D: Connection Pool vs. Multiplexing
- **Pre-warmed Pool (`pool_size: 16`, `multiplex: { enabled: false }`)**:
  Ideal for dedicated high-bandwidth TCP stream throughput.
- **Frame Multiplexing (`multiplex: { enabled: true, max_streams: 256 }`)**:
  Ideal for high connection churn (thousands of short-lived connections multiplexed over a single physical transport pipe).

---

## 4. Key Performance Optimizations Enabled

1. **`mimalloc` Global Allocator**: Zero lock-contention memory management for high-throughput async packet buffers.
2. **Log Level `warn` & `no_color: true`**: Eliminates console formatting and I/O locking bottlenecks during high-stress testing.
3. **`tcp_nodelay: true`**: Disables Nagle's algorithm to eliminate latency jitter on streaming data.
4. **Configurable `worker_threads`**: Pins Tokio multi-threaded work stealing runtime to match hardware CPU topology.
