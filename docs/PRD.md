# Product Requirements Document — culvert

| Field | Value |
| :--- | :--- |
| Document | Product Requirements Document |
| Product | culvert (workspace version 0.1.0) |
| Status | Current — describes the implemented behavior |
| Scope | `crates/culvert` (library) and `crates/culvert-cli` (binary) |
| Source of truth | Source code, `config-example.yaml`, and the test suites under `crates/culvert/tests` |
| Related documents | [DESIGN.md](DESIGN.md), [README.md](../README.md) |

> This document merges the early architectural draft previously kept in `docs/culvert.md`.
> Content in that draft that does not exist in the implementation is listed in
> [Appendix A](#appendix-a--disposition-of-the-early-draft) and is not a requirement.

---

## 1. Purpose and Scope

culvert is a bidirectional TCP port-forwarding tool. A single process reads one
declarative YAML file and can act as an ingress server, as one or more tunnel
clients, or as both at the same time. A tunnel client connects to an ingress
server over WebSocket, HTTP/3, or raw QUIC; the ingress server then connects to
the requested target address, or the client registers a listener on the server
and serves incoming connections back through the tunnel.

The data carried over a tunnel is an opaque byte stream. culvert does not
interpret the application protocol, does not proxy UDP datagrams, and does not
implement SOCKS5 or HTTP proxy semantics. Targets may be TCP addresses
(`ip:port`, `hostname:port`) or Unix domain sockets (`unix://path`).

### 1.1 Why this product exists

- A service behind NAT or a restrictive firewall has no public address, but a
  host with a reachable ingress endpoint does. Reverse mappings expose the
  former through the latter.
- A client host needs access to a service reachable only from the ingress host.
  Forward mappings provide a local listener that is relayed to that service.
- Several ports need to traverse the same tunnel. One tunnel declaration can
  carry multiple forward and reverse mappings and share one pre-warmed stream
  pool.
- In a typical deployment a reverse proxy terminates public TLS and forwards
  to culvert over a Unix domain socket or loopback. culvert therefore does not
  need to manage ACME certificates. Direct TLS termination is also available
  for deployments without a reverse proxy.

### 1.2 Comparable functionality

Tools such as `wstunnel`, `frp`, `Xray`, and `Hysteria` also provide TCP port
forwarding over WebSocket or QUIC-based transports. This document records only
what culvert implements; it does not evaluate or rank other tools.

### 1.3 Requirements notation

`FR-n` entries are functional requirements. `NFR-n` entries are operational
constraints. All statements are verifiable against the implementation; where a
behavior is configurable, the configuration key is given.

---

## 2. Users and Use Cases

| ID | Use case | Flow |
| :--- | :--- | :--- |
| UC-1 | Publish a local service on a remote host (reverse mapping) | Client behind NAT connects to the ingress endpoint, sends `RegisterReverse`, the server binds the requested address, and each inbound connection is relayed back to the client's local target. |
| UC-2 | Reach a private service from a local port (forward mapping) | Client binds `mappings.listen` locally; each local connection opens a tunnel stream to the ingress endpoint; the server dials `mappings.target` and relays bytes in both directions. |
| UC-3 | Carry several ports over one endpoint (cluster) | One `tunnels` entry declares several `mappings`. All forward mappings share one connection pool when `pool_size` is set; reverse mappings use dedicated control and data streams. |
| UC-4 | Reach a Unix domain socket through a tunnel | `mappings.target` or `mappings.listen` uses `unix://path`; the ingress side or client side binds/connects the socket directly. |
| UC-5 | Deploy behind a reverse proxy | `server.listen` uses `ws+unix://...` or `ws://127.0.0.1:...`; the proxy handles public TLS and forwards the configured HTTP path to culvert. |
| UC-6 | Deploy without a reverse proxy | `server.listen` uses a TLS scheme (`wss`, `h3`, `https`, `quic`, `wss+unix`). culvert terminates TLS using the configured certificate and key, or an ephemeral self-signed certificate when neither is provided. |
| UC-7 | Apply per-endpoint network policy | `tunnels[].endpoint` can be an object with its own `dns`, `dialer`, and `tls` blocks, overriding the global defaults for that endpoint only. |
| UC-8 | Validate a deployment file before starting | `culvert --check -c <file>` parses and validates the configuration without starting listeners or tunnels. |

---

## 3. Product Surface

### 3.1 Process and entry points

- One binary, `culvert`, built from `crates/culvert-cli`.
- One configuration file (YAML), default `config.yaml`.
- Public library API in `crates/culvert`: `run_server`, `run_client`, `run`,
  `run_daemon`, `ConfigLoader`, `ClientRunner`, `ServerRunner`,
  `TunnelSupervisor`.
- A configuration may define `server`, `tunnels`, or both. At least one server
  listener or one tunnel is required.

### 3.2 Endpoint schemes

`server.listen` and `tunnels[].endpoint` accept the same scheme set.

| Scheme | Underlying transport | Notes |
| :--- | :--- | :--- |
| `ws` | WebSocket over TCP | HTTP/1.1 Upgrade |
| `wss` | WebSocket over TCP + TLS | TLS terminated by culvert when used on the server |
| `https`, `h3` | HTTP/3 `CONNECT` over QUIC/UDP | Both schemes map to the same HTTP/3 driver |
| `quic` | Raw QUIC bidirectional streams | Metadata handshake instead of HTTP headers |
| `ws+unix`, `wss+unix` | WebSocket over a Unix domain socket | URL query supports `path=` and `chmod=` |

### 3.3 Mapping modes

| Mode | Listener location | Target location | Pool |
| :--- | :--- | :--- | :--- |
| `forward` (default) | Client host | Dialed by the ingress server | Shared when `pool_size > 0` |
| `reverse` | Ingress server (registered over a control channel) | Dialed by the client host | Not used |

---

## 4. Functional Requirements

### FR-1 Ingress listeners

- **FR-1.1** `server.listen` accepts a single URL string or a list. Each URL is
  parsed into a scheme, bind address, optional HTTP path, and optional UDS
  socket mode. A port is mandatory for TCP URLs and must not be `0`.
- **FR-1.2** The scheme must be explicit. An URL without `://` is rejected.
  Only `ws`, `wss`, `https`, `h3`, `quic`, `ws+unix`, and `wss+unix` are
  accepted; `uds://`, `unix://`, and `ws+uds://` are not server schemes.
- **FR-1.3** Each listener is dispatched to a registered transport driver.
  Multiple heterogeneous listeners run concurrently in one process.
- **FR-1.4** When an HTTP path is configured, a request whose path does not
  match is answered with `404 Not Found`.
- **FR-1.5** On Unix domain socket listeners, `chmod=` sets the socket mode.
  Values are interpreted as octal (`0o660`, `0660`, `660`).
- **FR-1.6** TLS schemes terminate TLS in-process. `server.certificate` and
  `server.key` (or `server.tls.certificate` / `server.tls.key`) supply the
  PEM material; when neither pair is present, an ephemeral self-signed
  certificate is generated. Certificate and key must be supplied together.
  Configuring certificates while every listener uses a plaintext scheme is
  rejected at load time.
- **FR-1.7** `server.default_target` supplies the target for a forward stream
  that arrives without a target in its handshake and without an activation
  packet.

### FR-2 Authentication

- **FR-2.1** `server.token` and `tunnels[].token` are optional. Omitting them
  disables token verification. An empty string is rejected at load time.
- **FR-2.2** Tokens are at most 256 bytes; longer values are rejected at load
  time.
- **FR-2.3** WebSocket and Unix-socket clients send the token as
  `x-tunnel-auth: <token>`. HTTP/3 clients send both
  `authorization: Bearer <token>` and `x-tunnel-auth: <token>`; HTTP/3
  listeners read `authorization` first and fall back to `x-tunnel-auth`,
  accepting either the raw token or the `Bearer `-prefixed value. Raw QUIC
  carries the token in its length-prefixed handshake. The server compares the
  received token with the configured value using a constant-time comparison
  over fixed-size buffers.
- **FR-2.4** A missing or non-matching token is rejected during the handshake
  (`401` for malformed headers, `403` for a token mismatch on WebSocket
  listeners; `401` on HTTP/3 listeners). A valid tunnel stream is not created.

### FR-3 Configuration

- **FR-3.1** The configuration is YAML. All top-level and nested sections use
  `deny_unknown_fields`; unknown keys are errors.
- **FR-3.2** Global sections are `log`, `worker_threads`, `dns`, `dialer`,
  `tls`, `server`, and `tunnels`. The complete key reference is in
  [README.md](../README.md#configuration-reference).
- **FR-3.3** `tunnels[].endpoint` is either an URL string or an object with
  `url` plus optional `dns`, `dialer`, and `tls` blocks. Effective values are
  merged field-by-field: endpoint value, then global value, then built-in
  default.
- **FR-3.4** A `tls` block on a non-TLS endpoint scheme is rejected.
- **FR-3.5** Every tunnel requires a unique non-empty `tag` and a non-empty
  `mappings` list. Each mapping's `listen` and `target` must parse as
  `ip:port`, `hostname:port`, `[ipv6]:port`, or `unix://path`; the port must be
  present and non-zero for TCP forms.
- **FR-3.6** Duplicate `listen` addresses across forward mappings are
  rejected. Reverse listen addresses are not subject to this check.
- **FR-3.7** Load-time validation covers log level and output, worker thread
  count, token length, pool size, retry bounds, TLS version range
  (`1.2`/`1.3`, min not above max), PEM certificate/key/CA files, DNS
  resolver definitions, and `EndpointContext` construction. Effective
  `idle_timeout` must be greater than effective `heartbeat_interval` after
  defaults are merged.
- **FR-3.8** `culvert --check` performs this static validation only; it does
  not bind addresses or start the runtime.
- **FR-3.9** `tunnels[].multiplex` and `tunnels[].failover` are present in the
  data model but are rejected by the loader because they are not wired into
  the data path.

### FR-4 Forward mappings and the stream pool

- **FR-4.1** A forward mapping binds `mappings.listen` on the client host and
  relays each accepted connection to `mappings.target` through the ingress
  endpoint.
- **FR-4.2** When `pool_size` is set to a positive value, all forward mappings
  of that tunnel share one `ReplenishingStreamPool`. Without `pool_size`, each
  accepted connection opens a new transport stream through a connector shared
  by that mapping.
- **FR-4.3** Pooled streams are pre-warmed without a fixed backend target.
  The server keeps them idle and does not dial any target service until the
  client sends an activation packet.
- **FR-4.4** Activation packet: one magic byte `0xA5`, a two-byte big-endian
  target length, then the UTF-8 target string. The server reads it with a
  10-second timeout and rejects payloads of 512 bytes or more. An invalid
  magic byte closes the stream.
- **FR-4.5** The pool is an actor with idle streams, pending acquire requests,
  concurrent dial limits, heartbeat sweeps, dead-stream eviction, and backoff
  replenishment. Acquire requests wait at most 15 seconds. The pool caps total
  allocated streams at 16 and concurrent dials at 4.
- **FR-4.6** An acquired stream is used once. Closing it decrements the pool's
  active-stream counter through a guard; the actor replenishes idle streams in
  the background. Streams are not returned to the idle queue after use.
- **FR-4.7** Reverse mappings never use the forward pool.

### FR-5 Reverse mappings and control protocol

- **FR-5.1** The client opens a control stream in `reverse-ctrl` mode and
  sends `RegisterReverse { listen_addr, target_addr }`. The server binds the
  requested listener and replies with `RegisterAck { listen_addr, success,
  error_msg }`.
- **FR-5.2** Control messages are encoded with MessagePack
  (`to_vec_named`) over a length-delimited frame. Variants are
  `RegisterReverse`, `RegisterAck`, `NewConnection`, `Ping`, `Pong`.
- **FR-5.3** For each inbound connection on the registered listener, the
  server allocates a session id, registers a pending session, and sends
  `NewConnection { session_id, client_addr }` over the control channel.
- **FR-5.4** The client dials its local `mappings.target`, opens a
  `reverse-data` stream carrying `x-session-id`, and the server pairs it with
  the waiting inbound connection through `SessionManager`. Pairing has a
  10-second timeout.
- **FR-5.5** Both sides then relay bytes bidirectionally. The control channel
  stays open with activity-gated `Ping`/`Pong` and an idle timeout; any
  inbound frame refreshes the activity deadline, while outbound probes alone
  do not.
- **FR-5.6** Control-channel reconnection uses an exponential backoff from
  500 ms up to 30 s, reset after 30 s of stable operation.

### FR-6 Relay engine

- **FR-6.1** `RelayEngine::relay_with_heartbeat` drives both directions from a
  single `poll_fn` state machine with two 64 KB buffers.
- **FR-6.2** A probe is emitted only when neither direction has moved data for
  one `heartbeat_interval`; a stream silent for `idle_timeout` is closed.
  `idle_timeout` must exceed `heartbeat_interval`.
- **FR-6.3** Half-close is supported: a finished read direction flushes its
  buffered bytes and shuts down its write side while the other direction can
  continue.
- **FR-6.4** On cancellation, the relay drains in-flight data for at most
  500 ms before terminating.

### FR-7 Supervision, retry, and shutdown

- **FR-7.1** `TunnelSupervisor` owns one server runner and one worker per
  tunnel, all tracked in a `TaskTracker` bound to a `CancellationToken`.
- **FR-7.2** A tunnel worker that fails is restarted after the fixed delay
  `tunnels[].retry.delay` (default 3 s), up to `retry.max_retries` attempts.
  Omitting `max_retries` retries indefinitely. Permanent errors
  (`Config`, `UnsupportedScheme`, `AuthFailed`, `Tls`) are not retried.
- **FR-7.3** Shutdown cancels the token and awaits every tracked task.
  `TaskTracker` shares one 5-second grace window across nested trackers;
  abort is a deadline fallback only, after which handles are still awaited.
  Tasks are never detached on the normal shutdown path.
- **FR-7.4** The daemon installs a shutdown handler on a dedicated OS thread.
  The first termination signal cancels the token and starts a 10-second hard
  deadline; a second signal exits with code `130`. The watchdog is disarmed and
  joined on every clean exit path.

### FR-8 Network policy

- **FR-8.1** DNS strategies: `prefer_ipv4` (default), `prefer_ipv6`,
  `ipv4_only`, `ipv6_only`. Resolver backends: system/libc, UDP DNS, TCP DNS,
  DNS-over-TLS, DNS-over-HTTPS, as URL strings or structured objects. A
  bounded in-memory cache is used (default capacity 1024 entries, TTL 300 s).
- **FR-8.2** Dialer options: `connect_timeout` (default 10 s),
  `tcp_nodelay` (default true), `bind_interface`, `heartbeat_interval`
  (default 15 s), `idle_timeout` (default 30 s). `bind_interface` relies on
  `SO_BINDTODEVICE` and is rejected on platforms without it.
- **FR-8.3** TLS client options: `server_name`, `insecure`, `alpn`,
  `ca_certificate`, `client_certificate`, `client_key`, `certificate`, `key`,
  `min_version`, `max_version`. mTLS requires certificate and key together.
  ALPN is set to `h3` for HTTP/3 endpoints and `culvert-quic` for raw QUIC
  endpoints.
- **FR-8.4** Logging uses `tracing`. `log.level` accepts `trace`, `debug`,
  `info`, `warn`, `error`; `log.output` accepts `stdout` or `stderr`;
  `log.no_color` disables ANSI colors. `RUST_LOG` overrides `log.level`.
- **FR-8.5** `worker_threads` sets the Tokio worker count. `1` selects a
  current-thread runtime; other values select a multi-thread runtime with that
  count; omitting it auto-detects CPU cores. The CLI `-w / --worker-threads`
  value takes precedence.

---

## 5. Non-Functional and Operational Constraints

| ID | Constraint | Implementation basis |
| :--- | :--- | :--- |
| NFR-1 | Byte streams are relayed without interpreting application protocols. | `RelayEngine` moves `Bytes` between two `TunnelStream` trait objects. |
| NFR-2 | Every asynchronous task is owned and awaitable; shutdown cancels then awaits. | `TaskTracker`, `CancelFutureExt`, explicit `shutdown()` on listeners; no `detach_all()` on the normal path. |
| NFR-3 | Every await is cancellation-aware or bounded by a timeout. | Handshake, activation read, session pairing, relay drain, and pool acquire all have explicit bounds in `consts.rs`. |
| NFR-4 | Configuration errors are surfaced before the daemon starts. | `ConfigLoader::validate` runs on every startup and on `--check`. |
| NFR-5 | Unix domain sockets are available through a single platform abstraction. | `platform::GenericListener` / `PlatformUdsListener`, with a dedicated accept thread on Windows. |
| NFR-6 | The relay hot path uses a single `poll_fn` state machine rather than a per-direction task. | `core/relay.rs`. This is a deliberate performance constraint; it must not be replaced with `tokio::select!` in the hot path. |
| NFR-7 | Documentation states implemented behavior only. | Unwired options are rejected by the loader and listed in Section 6. |

Performance figures, throughput targets, memory budgets, and latency
commitments are not specified: they depend on the deployment and are not
verified by the repository's test suite.

---

## 6. Out of Scope

The following are **not** implemented. Some appear in older drafts or in the
configuration data model; none are part of the runtime data path.

| Item | Status |
| :--- | :--- |
| `tunnels[].multiplex` / `tunnels[].failover` | Present in the model, explicitly rejected by `ConfigLoader::validate`. |
| `mux` module and `core/failover.rs` | Modules exist but are not wired into the config or data path. |
| Seamless failover / stream migration / ring-buffer replay | Not implemented. |
| Automatic HTTP/3-to-WSS fallback | Not implemented. Each tunnel uses exactly the scheme in its endpoint URL. |
| P2P, STUN/ICE, NAT hole punching, dual-channel hot standby | Not implemented. |
| TOML configuration files | Not implemented. Configuration is YAML only. |
| UDP tunneling, DNS proxying for tunneled clients, SOCKS5, HTTP proxy | Not implemented. |
| ACME certificate issuance and renewal | Not implemented. Certificates come from files, an ephemeral self-signed fallback, or an external reverse proxy. |
| Protocol-level `CMD_TUNNEL_RESET` / `CMD_RESET_ACK` draining and returning streams to the idle pool | Not implemented. Pooled streams are one-shot. |
| WebSocket SIMD masking claims, hardware acceleration claims | No such implementation is exposed by the product. |

---

## 7. Configuration Example

The checked-in [`config-example.yaml`](../config-example.yaml) is the canonical
example and is asserted to parse cleanly by the loader tests. It defines a
server with five heterogeneous listeners, a multi-mapping tunnel sharing a
pool, and a second HTTP/3 tunnel. The complete parameter reference is in
[README.md](../README.md#configuration-reference).

```yaml
log:
  level: "info"
  output: "stdout"
tls:
  insecure: false
server:
  listen:
    - "ws+unix:///run/tunnel.sock?path=/tunnel&chmod=0o660"
    - "wss://0.0.0.0:8443/tunnel"
    - "h3://0.0.0.0:8443/tunnel"
    - "ws://127.0.0.1:8080/tunnel"
  token: "SECURE_PRESHARED_TOKEN"
  default_target: "127.0.0.1:5201"
tunnels:
  - tag: "corp-cluster"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    token: "SECURE_PRESHARED_TOKEN"
    pool_size: 8
    retry:
      delay: "3s"
      max_retries: 10
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:13306"
        target: "10.0.0.5:3306"
      - mode: "reverse"
        listen: "0.0.0.0:2222"
        target: "127.0.0.1:22"
```

---

## 8. Acceptance and Verification

| ID | Criterion | Verification |
| :--- | :--- | :--- |
| AC-1 | A valid configuration parses and an invalid one is rejected before any listener binds. | `culvert --check -c config-example.yaml`; `config::loader` unit tests. |
| AC-2 | Forward and reverse mappings carry bidirectional TCP traffic over WebSocket, WSS, and HTTP/3/QUIC listeners. | `crates/culvert/tests/e2e_ws_tunnel.rs`, `test_h3_quic.rs`, `test_wss_tls.rs`, `test_standalone_wss.rs`. |
| AC-3 | Unix domain socket listeners and mapping targets work end to end. | `test_uds_end_to_end.rs`. |
| AC-4 | Multiple mappings under one tunnel share the pool and duplicate binds are rejected. | `test_multi_mapping_and_retry.rs`, loader tests. |
| AC-5 | Pooled streams do not deliver stale or in-flight data to a subsequent connection. | `test_duplex_draining_pool.rs`. |
| AC-6 | QUIC flow control does not stall unrelated streams. | `test_quic_flow_control_hol.rs`. |
| AC-7 | Shutdown cancels and awaits all owned tasks; the watchdog is joined on clean exit. | `daemon.rs` tests, `common/task.rs` tests. |
| AC-8 | DNS and configuration options parse as documented. | `test_config_and_dns.rs`. |

Run the suite with `cargo test --workspace`.

---

## Appendix A — Disposition of the early draft

`docs/culvert.md` was an early architectural draft marked deprecated. Its
content is merged as follows.

**Retained, restated against the implementation:** reverse-proxy deployment
with an ingress on a Unix domain socket or loopback; no ACME certificate
management inside culvert; optional pre-shared token authentication; absence of
UDP tunneling, DNS proxying, and SOCKS5/HTTP proxy semantics; one configuration
file combining server ingress and client tunnels; forward and reverse port
forwarding; per-endpoint DNS/TLS/dialer settings; pre-warmed streams whose
backend target is dialed only on activation; path-based request matching.

**Retired (not implemented):** P2P readiness, STUN/ICE candidates, dual-channel
hot standby, primary/standby failover, seamless migration with ring-buffer
replay, `CMD_TUNNEL_RESET` / `CMD_RESET_ACK` draining and pool reuse,
HTTP/3 `POST /tunnel` extended CONNECT framing (the implementation uses
`CONNECT` for `h3`/`https` and a MessagePack metadata handshake for `quic`),
`server.toml` / `client.toml` TOML configuration, automatic QUIC-to-WSS
fallback, Caddyfile and sysctl tuning recipes, throughput/latency/memory
targets, SIMD masking claims, milestone plans, and subjective evaluations of
other tools.

The design details of what replaced that draft are in
[DESIGN.md](DESIGN.md); usage and the full parameter reference are in
[README.md](../README.md).
