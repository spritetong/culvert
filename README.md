# micro-tunnel

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-blue.svg)](https://opensource.org/licenses/MPL-2.0)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

**micro-tunnel** is a high-performance, lightweight, production-ready bidirectional TCP tunneling system engineered to pair seamlessly with modern edge reverse proxies (such as **Caddy** or **Nginx**).

Designed as a modern, clean replacement for tools like `wstunnel`, `micro-tunnel` eliminates protocol bloat, complex frame encapsulation state machines, and unnecessary internal certificate managers. It delivers near-bare-metal throughput and microsecond latency over HTTP/3 (QUIC) and WebSocket (WSS) while disguising traffic behind standard HTTPS ports.

---

## Key Highlights & Design Philosophy

- **Zero ACME Bloat**: `micro-tunnel` delegates public ACME certificates to reverse proxies like Caddy. The server ingress listens on Unix Domain Sockets (UDS) or internal loopback with zero public TLS certificate renewal complexity. (Direct standalone TLS termination with auto-generated or custom certificates is also supported).
- **Native QUIC & High-Speed WebSocket**:
  - **HTTP/3**: Multiplexes tunnels directly over native QUIC streams. Packet loss on one connection never blocks other streams (zero TCP Head-of-Line blocking).
  - **WSS**: Built on `fastwebsockets` with AVX2/NEON SIMD-accelerated masking.
- **Decoupled 0-RTT Pre-warmed Connection Pool**: Pre-establishes idle tunnel streams in the background. Client connections acquire a ready stream immediately without waiting for transport or TLS handshakes.
- **Multi-Mapping Cluster Sharing a Single Pool**: Multiple forward and reverse port mappings can share a single tunnel endpoint and connection pool. Remote targets are dialed on-demand only when client traffic arrives, preventing premature socket exhaustion.
- **Unified Server & Client Coexistence**: Run server ingress listeners and multiple tunnel clients simultaneously in a single process from a single declarative YAML file.
- **Optional Constant-Time Authentication**: High-entropy Pre-Shared Key (PSK) token validation using constant-time comparison immune to timing side-channel attacks. Token can be omitted for trusted or reverse-proxy-authenticated environments.
- **Advanced DNS & Socket Tuning**: Supports libc system DNS, standard UDP DNS, DNS-over-HTTPS (DoH), and DNS-over-TLS (DoT), with configurable IP family selection (`prefer_ipv4`, `prefer_ipv6`, `ipv4_only`, `ipv6_only`).

---

## Architecture Overview

```mermaid
flowchart LR
    subgraph ClientHost["Client Host (Local Network / Behind NAT)"]
        App["App / Client"]
        MTClient["micro-tunnel Client"]
        LocalSvc["Local Service\n(e.g., SSH :22)"]
    end

    subgraph Internet["Public Network / CDN"]
        Caddy["Caddy Reverse Proxy\n(HTTPS :443 / QUIC UDP :443)"]
    end

    subgraph ServerHost["Server Host (Cloud VPS / Gateway)"]
        MTServer["micro-tunnel Server Ingress\n(UDS / Loopback)"]
        RemoteSvc["Remote Service\n(e.g., MySQL :3306)"]
        PublicPort["Public Port\n(e.g., :2222)"]
    end

    %% Forward Tunnel Traffic
    App -->|Connect :13306| MTClient
    MTClient -->|"WSS / HTTP/3 (POST /tunnel)"| Caddy
    Caddy -->|"Proxy (UDS / TCP)"| MTServer
    MTServer -->|Dial| RemoteSvc

    %% Reverse Tunnel Traffic
    PublicPort -->|External User| MTServer
    MTServer -.->|"Reverse Stream"| MTClient
    MTClient -->|Forward| LocalSvc
```

---

## Installation & Build

### Prerequisites
- [Rust 1.85+](https://www.rust-lang.org/tools/install) (Edition 2024 support)
- C/C++ build toolchain (for `ring` / `mimalloc`)

### Build from Source
```bash
# Clone the repository
git clone https://github.com/spritetong/micro-tunnel.git
cd micro-tunnel

# Build optimized release binary
cargo build --release

# The compiled binary is located at:
./target/release/micro-tunnel
```

---

## Command-Line Usage

```bash
micro-tunnel [OPTIONS]
```

### Options

| Flag | Short | Environment / Argument | Description | Default |
| :--- | :---: | :--- | :--- | :--- |
| `--config` | `-c` | `<PATH>` | Path to the YAML configuration file | `config.yaml` |
| `--worker-threads` | `-w` | `<NUM>` | Override number of Tokio runtime worker threads (`1` for single-threaded runtime) | Auto-detect CPU cores |
| `--dns-resolver` | | `<URI>` | Override or append upstream DNS resolver URI. Can be specified multiple times. | Config file values |
| `--help` | `-h` | | Print command-line help message | |
| `--version` | `-V` | | Print version information | |

#### Example CLI Commands:
```bash
# Run with default config.yaml
./micro-tunnel

# Run with a custom config path and 2 worker threads
./micro-tunnel -c /etc/micro-tunnel/server.yaml -w 2

# Override DNS resolvers via CLI
./micro-tunnel -c client.yaml --dns-resolver "dns+https://1.1.1.1?sni=cloudflare-dns.com"
```

---

## Configuration Guide

Configuration files are written in declarative **YAML**. All top-level and nested sections are described below.

### 1. Global Settings

#### `log` (Logging Options)
Configures structured log output using Tokio `tracing`.
```yaml
log:
  level: "info"      # Log verbosity level: "trace", "debug", "info", "warn", "error" (default: "info")
  output: "stdout"    # Output destination: "stdout" or "stderr" (default: "stdout")
  no_color: false     # Set to true to disable ANSI terminal colors (default: false)
```

#### `worker_threads` (Runtime Concurrency)
```yaml
worker_threads: 4     # Number of Tokio worker threads.
                      # Omit or set to null for auto-detecting CPU cores.
                      # Set to 1 for lightweight single-threaded (current_thread) runtime.
```

---

### 2. DNS Settings (`dns`)
Global DNS resolution settings compatible with wstunnel and sing-box URL conventions. Can be overridden per-endpoint.

```yaml
dns:
  strategy: "prefer_ipv4"   # IP resolution preference:
                            #   "prefer_ipv4" (default): prefer IPv4, fallback to IPv6
                            #   "prefer_ipv6": prefer IPv6, fallback to IPv4
                            #   "ipv4_only": resolve A records only; ignore AAAA
                            #   "ipv6_only": resolve AAAA records only; ignore A
  resolvers:                # Upstream DNS resolvers (alias: `servers`)
    - "system://0.0.0.0"                                  # OS libc local resolver
    - "dns://1.1.1.1:53"                                  # Standard UDP DNS
    - "dns+https://1.1.1.1?sni=cloudflare-dns.com"       # DNS-over-HTTPS (DoH)
    - "dns+tls://8.8.8.8?sni=dns.google"                 # DNS-over-TLS (DoT)
  cache_capacity: 1024      # In-memory DNS cache capacity (default: 1024 entries)
```

---

### 3. Socket Dialer Settings (`dialer`)
Low-level TCP socket configuration for outbound dialing. Can be overridden per-endpoint.

```yaml
dialer:
  connect_timeout: "5s"     # Socket connection timeout (e.g., "5s", "500ms", or raw seconds: 5)
  tcp_keepalive: "15s"      # TCP keepalive interval (e.g., "15s", "30s")
  tcp_nodelay: true         # Disable Nagle's algorithm for low-latency transmission (default: true)
  bind_interface: "eth0"    # Bind outbound traffic to a specific local network interface or IP (optional)
```

---

### 4. TLS Settings (`tls`)
Global default TLS configuration for outbound client connections and fallback server termination.

```yaml
tls:
  enabled: true                        # Enable TLS / HTTPS / WSS
  server_name: "tunnel.example.com"    # Override SNI hostname in TLS ClientHello
  insecure: false                      # Skip remote certificate verification (default: false)
  alpn: ["h3", "http/1.1"]             # Application-Layer Protocol Negotiation list
  ca_certificate: "/etc/ssl/ca.crt"    # Custom CA certificate file path for server verification
  client_certificate: "/etc/ssl/client.crt" # Client certificate for mutual TLS (mTLS)
  client_key: "/etc/ssl/client.key"         # Client private key for mutual TLS (mTLS)
  certificate: "/etc/ssl/server.crt"   # Server certificate for direct TLS termination (alias: cert)
  key: "/etc/ssl/server.key"           # Server private key for direct TLS termination
  min_version: "1.2"                   # Minimum TLS version ("1.2" or "1.3")
  max_version: "1.3"                   # Maximum TLS version ("1.2" or "1.3")
```

---

### 5. Server Ingress (`server`)
Configures `micro-tunnel` as an ingress gateway on a cloud VPS or host.

> **Note**: In a typical setup behind Caddy or Nginx, configure `listen` to a Unix Domain Socket (`uds:///run/tunnel.sock`) or loopback (`ws://127.0.0.1:8080/tunnel`).

```yaml
server:
  # Listening URLs with explicit protocol schemes and path routing.
  # Can be a single string or an array of URLs.
  # Supported schemes: ws, wss, h3, quic, uds
  listen:
    - "ws://127.0.0.1:8080/tunnel"       # Behind local reverse proxy (Caddy/Nginx)
    - "uds:///run/tunnel.sock"           # Ultra-fast zero-network-stack IPC behind Caddy
    - "wss://0.0.0.0:8443/tunnel"        # Direct standalone WSS (TLS terminated by micro-tunnel)
    - "h3://0.0.0.0:8443/tunnel"         # Direct standalone HTTP/3 over QUIC/UDP

  # Pre-shared authentication token (PSK).
  # Optional: omit or set to null/empty for unauthenticated mode.
  # Maximum length: 256 bytes.
  token: "SECRET_PRESHARED_TOKEN"

  # Fallback target address when the client does not specify one.
  default_target: "127.0.0.1:5201"

  # Optional direct TLS termination certificates (used for wss/h3 if not behind a reverse proxy).
  # If omitted on wss/h3, a temporary self-signed certificate is automatically generated.
  certificate: "/etc/ssl/server.crt"    # alias: cert
  key: "/etc/ssl/server.key"

  # Optional advanced TLS settings overriding global tls block
  tls:
    min_version: "1.3"
```

#### Server Scheme Reference

| Scheme | Description | Example URL |
| :--- | :--- | :--- |
| `ws` | Unencrypted WebSocket over TCP | `ws://127.0.0.1:8080/tunnel` |
| `wss` | TLS-encrypted WebSocket over TCP | `wss://0.0.0.0:8443/tunnel` |
| `h3` | HTTP/3 bidirectional stream over QUIC/UDP | `h3://0.0.0.0:8443/tunnel` |
| `quic` | Raw QUIC bidirectional stream | `quic://0.0.0.0:8443/tunnel` |
| `uds` | Unix Domain Socket (IPC) | `uds:///run/tunnel.sock` |

> [!IMPORTANT]
> **Path Matching**: Client requests must match the server URL's path exactly (e.g., `/tunnel`). Mismatched paths receive an HTTP `404 Not Found` response to defend against scanner probes.

---

### 6. Tunnel Client (`tunnels`)
Defines one or more outbound tunnel client instances.

```yaml
tunnels:
  - tag: "corp-cluster"                         # Unique identifier for the tunnel
    endpoint: "wss://tunnel.example.com/tunnel" # Remote ingress server URL (or detailed endpoint object)
    token: "SECRET_PRESHARED_TOKEN"             # PSK token matching server.token (optional)
    pool_size: 8                                # Pre-warmed idle connection pool size (default: 0)

    # Reconnection & self-healing supervisor settings
    retry:
      delay: "3s"            # Wait time between reconnect attempts (e.g. "3s", "500ms")
      max_retries: 10        # Maximum retry attempts before giving up (omit or null for infinite)

    # Port mappings: Multiple forward and reverse mappings share this endpoint & connection pool
    mappings:
      # Local Forward Mapping: Clients connect to 127.0.0.1:15201, forwarded to 127.0.0.1:5201 on the server
      - mode: "forward"
        listen: "127.0.0.1:15201"
        target: "127.0.0.1:5201"

      # Local Forward Mapping: Forward local 13306 to internal database behind server
      - mode: "forward"
        listen: "127.0.0.1:13306"
        target: "10.0.0.5:3306"

      # Remote Reverse Mapping: Server opens port 2222; connections are routed back to local SSH (127.0.0.1:22)
      - mode: "reverse"
        listen: "0.0.0.0:2222"
        target: "127.0.0.1:22"
```

#### Detailed Endpoint Configuration (Per-Endpoint Overrides)
Instead of a simple URL string, `endpoint` can be declared as an object with dedicated `dns`, `dialer`, and `tls` overrides:

```yaml
tunnels:
  - tag: "special-node"
    token: "SECRET_PRESHARED_TOKEN"
    endpoint:
      url: "wss://special.domain.com/tunnel"
      dns:
        strategy: "ipv4_only"
        resolvers:
          - "dns://1.1.1.1"
      dialer:
        connect_timeout: "10s"
        tcp_keepalive: "30s"
      tls:
        enabled: true
        server_name: "fronting-camouflage.com" # SNI domain fronting override
        insecure: false
        ca_certificate: "/path/to/custom-ca.pem"
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:8080"
        target: "127.0.0.1:80"
```

#### Single-Mapping Legacy Syntax (Backward Compatibility)
For simple tunnels, single mappings can be declared directly at the tunnel root:

```yaml
tunnels:
  - tag: "simple-forward"
    endpoint: "wss://tunnel.example.com/tunnel"
    token: "SECRET_PRESHARED_TOKEN"
    mode: "forward"
    listen: "127.0.0.1:15201"
    target: "127.0.0.1:5201"
```

---

## Configuration Recipes

### Recipe 1: Production Server Behind Caddy Reverse Proxy (Recommended)

Let Caddy handle public HTTPS, HTTP/3, and automated Let's Encrypt certificates. Micro-tunnel listens securely over Unix Domain Sockets.

#### `server.yaml` (micro-tunnel configuration)
```yaml
log:
  level: "info"

server:
  listen: "uds:///run/micro-tunnel.sock"
  token: "MY_SECURE_TOKEN_32_BYTES_OR_MORE"
  default_target: "127.0.0.1:5201"
```

#### `Caddyfile` (Caddy reverse proxy configuration)
```caddy
tunnel.yourdomain.com {
    # Match micro-tunnel path
    @tunnel path /tunnel*

    # Reverse proxy WebSocket and HTTP traffic to micro-tunnel via Unix Domain Socket
    handle @tunnel {
        reverse_proxy unix//run/micro-tunnel.sock
    }

    # Optional camouflage / decoy website on root
    handle {
        respond "System Operational" 200
    }
}
```

---

### Recipe 2: Client Accessing Cloud Database (Local Forwarding)

Access an internal MySQL database (`10.0.0.5:3306`) in a private cloud network through a local port (`127.0.0.1:13306`).

#### `client.yaml`
```yaml
log:
  level: "info"

tunnels:
  - tag: "cloud-mysql"
    endpoint: "wss://tunnel.yourdomain.com/tunnel"
    token: "MY_SECURE_TOKEN_32_BYTES_OR_MORE"
    pool_size: 4
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:13306"
        target: "10.0.0.5:3306"
```

Now connect locally using any database tool:
```bash
mysql -h 127.0.0.1 -P 13306 -u root -p
```

---

### Recipe 3: Exposing Local Home / NAT Machine to VPS (Remote Reverse Forwarding)

Expose a home computer's SSH server (`127.0.0.1:22`) on the public VPS port `2222` without configuring port forwarding on your home router.

#### `client.yaml` (running on home computer)
```yaml
log:
  level: "info"

tunnels:
  - tag: "home-ssh-reverse"
    endpoint: "wss://tunnel.yourdomain.com/tunnel"
    token: "MY_SECURE_TOKEN_32_BYTES_OR_MORE"
    pool_size: 4
    retry:
      delay: "3s"
    mappings:
      - mode: "reverse"
        listen: "0.0.0.0:2222"     # Bound on the VPS server
        target: "127.0.0.1:22"     # Local SSH server on home machine
```

Anyone connecting to `vps-ip:2222` is transparently routed back to your home machine's SSH server.

---

### Recipe 4: Standalone High-Speed HTTP/3 Direct Ingress

Run `micro-tunnel` directly without a reverse proxy, leveraging HTTP/3 over UDP port `8443`.

#### `server.yaml`
```yaml
log:
  level: "info"

server:
  listen:
    - "h3://0.0.0.0:8443/tunnel"
    - "wss://0.0.0.0:8443/tunnel"
  token: "MY_SECURE_TOKEN"
  # Omit cert/key to automatically generate self-signed TLS certificates,
  # or specify your own certificates:
  # certificate: "/etc/ssl/cert.pem"
  # key: "/etc/ssl/key.pem"
```

#### `client.yaml`
```yaml
log:
  level: "info"

# Trust self-signed certificates for direct connections
tls:
  insecure: true

tunnels:
  - tag: "direct-h3-tunnel"
    endpoint: "h3://vps.example.com:8443/tunnel"
    token: "MY_SECURE_TOKEN"
    pool_size: 8
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:15201"
        target: "127.0.0.1:5201"
```

---

## Benchmarks & Performance

`micro-tunnel` comes equipped with automated Criterion micro-benchmarks and end-to-end `iperf3` benchmark suites.

### Running Benchmarks

```bash
# Run internal Criterion component benchmarks (zero-copy relay, codecs, sessions)
cargo bench

# Run end-to-end multi-stream iperf3 throughput tests (Linux / macOS)
./benchmarks/run-bench.sh 127.0.0.1 15201 8 10

# Run end-to-end throughput tests (Windows PowerShell)
.\benchmarks\run-bench.ps1 -TargetHost 127.0.0.1 -TargetPort 15201 -ParallelStreams 8 -DurationSecs 10
```

### Performance Characteristics
- **Memory Footprint**: Under 15 MB RSS under high load.
- **Throughput**: Saturated 10 Gbps+ line rate over loopback and LAN with minimal CPU overhead.
- **Zero-Copy I/O**: Direct 64 KB kernel buffer splicing with `mimalloc` zero-fragmentation allocator.
- **SIMD Acceleration**: AVX2 and ARM NEON hardware acceleration for WebSocket masking and unmasking.

---

## License

This project is licensed under the **Mozilla Public License Version 2.0 (MPL-2.0)**.
See the [LICENSE](LICENSE) file for details.
