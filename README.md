# culvert

`culvert` forwards TCP ports between hosts over WebSocket, HTTP/3, or raw QUIC.
One YAML file configures an ingress server, one or more tunnel clients, or both
in the same process. Forwarded data is an opaque byte stream; Unix domain
sockets can be used as listeners and targets.

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-blue.svg)](https://opensource.org/licenses/MPL-2.0)

---

## Why

- Reach a service that is only visible from a remote host: open a local port
  that is relayed through the tunnel (`forward` mapping).
- Publish a service on a host behind NAT: register a listener on the remote
  ingress host and relay its connections back to the local service (`reverse`
  mapping).
- Carry several ports through one endpoint: multiple mappings share one tunnel
  configuration and, when enabled, one pre-warmed stream pool.
- Terminate public TLS at an existing reverse proxy over a Unix domain socket,
  or let culvert terminate TLS itself. No certificate issuance is built in.

Tools such as `wstunnel`, `frp`, `Xray`, and `Hysteria` also provide TCP port
forwarding over WebSocket or QUIC-based transports. `culvert` covers that
forwarding use case with a single YAML file and both roles in one binary.

## What

A `server` accepts tunnel streams on one or more `listen` URLs and dials the
forward targets. A `tunnels` entry connects to one `endpoint` URL and declares
`mappings` with a `mode`, a `listen` address, and a `target` address.

| Scheme | Transport | Default port |
| :--- | :--- | :--- |
| `ws` | WebSocket over TCP | 80 |
| `wss` | WebSocket over TCP with TLS | 443 |
| `https` | HTTP/3 `CONNECT` over QUIC/UDP | 443 |
| `h3` | HTTP/3 `CONNECT` over QUIC/UDP | 443 |
| `quic` | Raw QUIC bidirectional streams | 443 |
| `ws+unix` | WebSocket over a Unix domain socket | — |
| `wss+unix` | WebSocket over a Unix domain socket with TLS | — |

`server.listen` and `tunnels[].endpoint` use the same scheme set. Unix socket
URLs take `?path=/tunnel` for the HTTP path and `?chmod=0o660` for socket
permissions (`660`, `0660`, and `0o660` are all octal).

| Mapping mode | Listener | Target is dialed by | Stream pool |
| :--- | :--- | :--- | :--- |
| `forward` (default) | Client host | Server | Shared when `pool_size` is set |
| `reverse` | Server, registered over a control channel | Client host | Not used |

Not implemented: UDP tunneling, SOCKS5 or HTTP proxy semantics, ACME
certificate management, P2P/STUN, automatic HTTP/3-to-WSS fallback, and
seamless transport failover. The `multiplex` and `failover` configuration
sections are rejected at load time because they are not wired into the data
path.

---

## Quick start

Requires Rust 1.85+ (edition 2024) and a C/C++ toolchain.

```bash
cargo build --release          # binary: target/release/culvert
culvert --check -c config.yaml # validate without starting anything
culvert -c config.yaml         # start
```

**`server.yaml`** — ingress on a TCP port, with a fallback target:

```yaml
log:
  level: "info"
server:
  listen: "ws://0.0.0.0:8080/tunnel"
  token: "CHANGE_ME"
  default_target: "127.0.0.1:5201"
```

**`client.yaml`** — local port `18080` reaches port `8080` in the server's
network:

```yaml
log:
  level: "info"
tunnels:
  - tag: "remote-http"
    endpoint: "ws://203.0.113.10:8080/tunnel"
    token: "CHANGE_ME"
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:18080"
        target: "127.0.0.1:8080"
```

```bash
culvert -c server.yaml   # on the ingress host
culvert -c client.yaml   # on the client host
curl http://127.0.0.1:18080/
```

---

## Command-line reference

```
culvert [OPTIONS]
```

| Option | Argument | Description | Default |
| :--- | :--- | :--- | :--- |
| `-c`, `--config` | `<PATH>` | YAML configuration file | `config.yaml` |
| `-w`, `--worker-threads` | `<NUM>` | Tokio worker threads; `1` selects a current-thread runtime. Overrides `worker_threads` in the file | auto-detected CPU cores |
| `--check` | — | Parse and validate the configuration, then exit. Does not bind addresses or start the runtime | off |
| `-h`, `--help` | — | Print command-line help | — |
| `-V`, `--version` | — | Print version | — |

```bash
culvert --check -c /etc/culvert/config.yaml
culvert -c /etc/culvert/config.yaml -w 2
```

Configuration errors exit with code `1`. The first termination signal cancels
all tasks and waits for them; a second signal exits with code `130`. A 10-second
hard deadline applies to shutdown.

---

## Configuration reference

Configuration is YAML. All sections reject unknown fields. A file must define
at least one server listener or one tunnel.

### Top-level

| Key | Type | Required | Default | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `log` | object | no | `info` / `stdout` / `false` | See `log`. |
| `worker_threads` | integer | no | auto-detect | Must be `>= 1`. `1` selects a current-thread runtime. |
| `dns` | object | no | system resolver, `prefer_ipv4` | Global defaults; overridable per endpoint. |
| `dialer` | object | no | see `dialer` | Global defaults; overridable per endpoint. |
| `tls` | object | no | — | Global defaults for TLS endpoints and server termination. |
| `server` | object | no | — | Ingress listeners. |
| `tunnels` | list | no | `[]` | Tunnel clients. |

### `log`

| Key | Type | Default | Values / constraints |
| :--- | :--- | :--- | :--- |
| `level` | string | `info` | `trace`, `debug`, `info`, `warn`, `error`. `RUST_LOG` overrides this value. |
| `output` | string | `stdout` | `stdout`, `stderr`. |
| `no_color` | bool | `false` | Disable ANSI colors in log output. |

### `dns`

| Key | Type | Default | Values / constraints |
| :--- | :--- | :--- | :--- |
| `strategy` | string | `prefer_ipv4` | `prefer_ipv4`, `prefer_ipv6`, `ipv4_only`, `ipv6_only`. |
| `resolvers` | list | system resolver | Each item is an URL string or a structured object. |
| `cache_capacity` | integer | `1024` | Must be `>= 1`; cache TTL is 300 s. |

Structured resolver object:

| Key | Type | Required | Notes |
| :--- | :--- | :--- | :--- |
| `type` | string | yes | `system`, `udp`, `dns`, `tcp`, `tls` (DoT), `https` (DoH). |
| `server` | string | for all types except `system` | IP or hostname, optionally with `host:port`. |
| `port` | integer | no | Defaults: 53 for `udp`/`dns`/`tcp`, 853 for `tls`, 443 for `https`. |
| `path` | string | no | DoH request path, e.g. `/dns-query`. |
| `tls.server_name` | string | no | SNI for DoH/DoT; defaults to the server address. |

Resolver URL forms:

```yaml
resolvers:
  - "system://0.0.0.0"                              # also accepted: "system"
  - "udp://1.1.1.1:53"                              # "dns://..." is equivalent
  - "tcp://1.1.1.1:53"
  - "tls://8.8.8.8?sni=dns.google"
  - "https://1.1.1.1/dns-query?sni=cloudflare-dns.com"
  - type: "https"                                   # structured form
    server: "1.1.1.1"
    tls:
      server_name: "cloudflare-dns.com"
```

### `dialer`

| Key | Type | Default | Values / constraints |
| :--- | :--- | :--- | :--- |
| `connect_timeout` | duration | `10s` | Must be greater than 0. |
| `tcp_nodelay` | bool | `true` | Sets `TCP_NODELAY` on outbound TCP sockets. |
| `bind_interface` | string | — | Outbound interface via `SO_BINDTODEVICE`; rejected where unsupported. |
| `heartbeat_interval` | duration | `15s` | Must be greater than 0. Probes are sent only when the stream is quiet. |
| `idle_timeout` | duration | `30s` | Must be greater than 0 and greater than the effective `heartbeat_interval`. |

Durations accept a plain integer (seconds), `"10s"`, or `"500ms"`.

### `tls`

TLS is activated by the endpoint scheme (`wss`, `https`, `h3`, `quic`,
`wss+unix`), not by the presence of this section. A `tls` block on a `ws` or
`ws+unix` endpoint is an error.

| Key | Type | Default | Notes |
| :--- | :--- | :--- | :--- |
| `server_name` | string | endpoint host | Overrides the SNI hostname. |
| `insecure` | bool | `false` | Skip server certificate verification. |
| `alpn` | list of strings | `h3` for HTTP/3, `culvert-quic` for `quic` | ALPN identifiers. |
| `ca_certificate` | path | — | Custom CA PEM for server verification. |
| `client_certificate` | path | — | mTLS client certificate; requires `client_key`. |
| `client_key` | path | — | mTLS private key; requires `client_certificate`. |
| `certificate` | path | — | Server certificate for TLS termination; requires `key`. |
| `key` | path | — | Server private key for TLS termination; requires `certificate`. |
| `min_version` | string | — | `1.2` or `1.3`; must not exceed `max_version`. |
| `max_version` | string | — | `1.2` or `1.3`. |

Certificate, key, and CA files are checked for readability and valid PEM at
load time.

### `server`

| Key | Type | Default | Notes |
| :--- | :--- | :--- | :--- |
| `listen` | string or list | `[]` | One or more URLs. Required when no tunnels are configured. A port is mandatory for TCP URLs and must not be `0`. |
| `token` | string | — | Optional pre-shared token; empty strings are rejected; max 256 bytes. |
| `default_target` | string | — | Fallback forward target when the client supplies none. |
| `certificate` | path | — | TLS server certificate; requires `key`. Omit both for an ephemeral self-signed certificate. |
| `key` | path | — | TLS server private key; requires `certificate`. |
| `tls` | object | — | Advanced TLS options for server termination; overrides global `tls`. |
| `heartbeat_interval` | duration | `15s` | Transport keepalive for accepted connections. |
| `idle_timeout` | duration | `30s` | Must be greater than the effective `heartbeat_interval`. |

WebSocket and Unix-socket clients send the token as `x-tunnel-auth: <token>`.
HTTP/3 clients send both `authorization: Bearer <token>` and
`x-tunnel-auth: <token>`; HTTP/3 listeners accept either header, in raw or
`Bearer` form. Raw QUIC carries the token in its binary handshake. A missing or
non-matching token is rejected during the handshake. When an HTTP path is
configured on the listener, requests with a different path receive `404`.

### `tunnels[]`

| Key | Type | Required | Notes |
| :--- | :--- | :--- | :--- |
| `tag` | string | yes | Non-empty and unique across all tunnels. |
| `endpoint` | string or object | yes | Ingress URL, or an object with per-endpoint overrides. |
| `token` | string | no | Must match `server.token`; empty strings are rejected; max 256 bytes. |
| `pool_size` | integer | no | Positive value enables a pre-warmed stream pool shared by all `forward` mappings of this tunnel. |
| `retry` | object | no | See below. |
| `mappings` | list | yes | At least one mapping; see below. |
| `multiplex`, `failover` | object | — | Rejected at load time; not wired into the data path. |

`endpoint` object:

| Key | Type | Required | Notes |
| :--- | :--- | :--- | :--- |
| `url` | string | yes | Any supported scheme. Unix socket endpoints must include a socket path. |
| `dns` | object | no | Endpoint-specific `dns`, same keys as the global section. |
| `dialer` | object | no | Endpoint-specific `dialer`, same keys as the global section. |
| `tls` | object | no | Endpoint-specific `tls`. Only valid on TLS schemes. |

Effective values are merged field by field: endpoint override, then global
value, then built-in default.

`retry` object:

| Key | Type | Default | Notes |
| :--- | :--- | :--- | :--- |
| `delay` | duration | `3s` | Fixed wait between attempts; must be greater than 0. |
| `max_retries` | integer | unlimited | Must be `>= 1`. Omit to retry indefinitely. |

`mappings[]` object:

| Key | Type | Default | Notes |
| :--- | :--- | :--- | :--- |
| `mode` | string | `forward` | `forward` or `reverse`. |
| `listen` | string | — | Address bound locally (`forward`) or on the server (`reverse`). |
| `target` | string | — | Address dialed by the server (`forward`) or by the client (`reverse`). |

Address formats for `listen` and `target`: `ip:port`, `hostname:port`,
`[::1]:port`, or `unix://path`. A port is mandatory and non-zero for non-Unix
forms. Forward `listen` addresses must be unique across all tunnels.

### Validation summary

`culvert --check` and every startup reject: unknown fields, missing schemes or
ports, unsupported schemes, empty or overlong tokens, duplicate tags and
duplicate forward listen addresses, missing mappings, unpaired certificates
and keys, invalid PEM files, invalid TLS version ranges, invalid DNS resolver
definitions, `tls` on non-TLS endpoints, certificates with only plaintext
listeners, and effective `idle_timeout <= heartbeat_interval`.

---

## Examples

### Reverse mapping (publish a local SSH service)

Client on the machine behind NAT; the server binds `2222` on the ingress host:

```yaml
tunnels:
  - tag: "home-ssh"
    endpoint: "wss://tunnel.example.com/tunnel"
    token: "CHANGE_ME"
    mappings:
      - mode: "reverse"
        listen: "0.0.0.0:2222"
        target: "127.0.0.1:22"
```

```bash
ssh -p 2222 user@vps.example.com
```

### Ingress behind a reverse proxy over a Unix socket

**`server.yaml`**

```yaml
log:
  level: "info"
server:
  listen: "ws+unix:///run/culvert.sock?path=/tunnel&chmod=0o660"
  token: "CHANGE_ME"
```

**Caddyfile** (public TLS is terminated by Caddy):

```caddy
tunnel.example.com {
    @tunnel path /tunnel
    handle @tunnel {
        reverse_proxy unix//run/culvert.sock
    }
    handle {
        respond "OK" 200
    }
}
```

Client endpoint: `wss://tunnel.example.com/tunnel`.

### Standalone TLS ingress (no reverse proxy)

**`server.yaml`** — omit `certificate` / `key` to use an ephemeral self-signed
certificate:

```yaml
server:
  listen:
    - "wss://0.0.0.0:8443/tunnel"
    - "h3://0.0.0.0:8443/tunnel"
    - "quic://0.0.0.0:8443/tunnel"
  token: "CHANGE_ME"
  # certificate: "/etc/ssl/certs/tunnel.pem"
  # key: "/etc/ssl/private/tunnel.key"
```

**`client.yaml`** — `insecure: true` is needed only when the server uses a
self-signed certificate; otherwise supply `ca_certificate`:

```yaml
tls:
  insecure: true
tunnels:
  - tag: "direct-h3"
    endpoint: "h3://vps.example.com:8443/tunnel"
    token: "CHANGE_ME"
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:15201"
        target: "127.0.0.1:5201"
```

### Multi-mapping cluster with a shared pool

All `forward` mappings use the same endpoint and pool; the `reverse` mapping
uses its own control and data streams.

```yaml
tunnels:
  - tag: "corp-cluster"
    endpoint: "wss://tunnel.example.com/tunnel"
    token: "CHANGE_ME"
    pool_size: 8
    retry:
      delay: "3s"
      max_retries: 10
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:15201"
        target: "127.0.0.1:5201"
      - mode: "forward"
        listen: "127.0.0.1:13306"
        target: "10.0.0.5:3306"
      - mode: "forward"
        listen: "127.0.0.1:18000"
        target: "unix:///var/run/docker.sock"
      - mode: "reverse"
        listen: "0.0.0.0:2222"
        target: "127.0.0.1:22"
```

Pooled streams are opened without a backend target. The client writes an
activation packet (`0xA5`, a two-byte big-endian length, and the target
string), and the server dials the target only after receiving it. Acquired
streams are used once and replenished in the background.

### Per-endpoint DNS, dialer, and mTLS overrides

Global values apply unless the endpoint overrides them:

```yaml
dns:
  strategy: "prefer_ipv4"
  resolvers:
    - "system://0.0.0.0"
dialer:
  connect_timeout: "10s"
  heartbeat_interval: "15s"
  idle_timeout: "30s"
tls:
  min_version: "1.3"
tunnels:
  - tag: "special-node"
    token: "CHANGE_ME"
    endpoint:
      url: "wss://special.example.com/tunnel"
      dns:
        strategy: "ipv4_only"
        resolvers:
          - type: "https"
            server: "1.1.1.1"
            path: "/dns-query"
            tls:
              server_name: "cloudflare-dns.com"
          - type: "system"
      dialer:
        connect_timeout: "5s"
        tcp_nodelay: true
      tls:
        server_name: "cdn.example.com"
        alpn: ["http/1.1"]
        ca_certificate: "/etc/ssl/certs/ca.pem"
        client_certificate: "/etc/ssl/certs/client.pem"
        client_key: "/etc/ssl/private/client.key"
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:18080"
        target: "10.0.0.8:8080"
```

The referenced PEM files must exist when the configuration is loaded.

### Server and clients in one file

`server` and `tunnels` can coexist, so a host can be an ingress for some peers
and a client of another ingress:

```yaml
log:
  level: "info"
worker_threads: 4
server:
  listen:
    - "ws://127.0.0.1:8080/tunnel"
    - "h3://0.0.0.0:8443/tunnel"
  token: "CHANGE_ME"
  default_target: "127.0.0.1:5201"
tunnels:
  - tag: "upstream"
    endpoint: "wss://upstream.example.com/tunnel"
    token: "CHANGE_ME"
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:13306"
        target: "10.0.0.5:3306"
```

The full example file with all listener schemes and two tunnels is
[`config-example.yaml`](config-example.yaml).

### Library usage

`culvert` is a library crate; `culvert-cli` is the binary. `run()` starts the
supervisor with a caller-owned cancellation token. `run_daemon()` additionally
installs signal handling and the shutdown watchdog.

```rust
use culvert::config::ConfigLoader;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ConfigLoader::from_file("config.yaml")?;
    let cancel = CancellationToken::new();
    culvert::run(config, cancel).await?;
    Ok(())
}
```

Other public entry points: `culvert::run_server(ServerConfig, token)`,
`culvert::run_client(TunnelConfig, token)`, `culvert::run_daemon(AppConfig)`,
and the `ServerRunner` / `ClientRunner` / `TunnelSupervisor` types.

---

## Development

```bash
cargo test --workspace       # unit and end-to-end tests
cargo bench                  # Criterion micro-benchmarks
```

End-to-end throughput scripts are in `benchmarks/` (`run-bench.sh`,
`run-bench.ps1`). Architecture and protocol details are in
[DESIGN.md](docs/DESIGN.md); requirements and scope are in
[PRD.md](docs/PRD.md).

## License

Mozilla Public License Version 2.0. See [LICENSE](LICENSE).
