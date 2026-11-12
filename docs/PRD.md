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

culvert is a bidirectional TCP and UDP port-forwarding tool. A single process reads one
declarative YAML file and can act as an ingress server, as one or more tunnel
clients, or as both at the same time. A tunnel client connects to an ingress
server over WebSocket, HTTP/3, or raw QUIC; the ingress server then connects to
the requested target address, or the client registers a listener on the server
and serves incoming connections or datagram flows back through the tunnel.

The data carried over a tunnel is an opaque byte stream (for TCP/UDS) or
length-framed datagrams (`[u16 payload_len (big-endian) | payload]`, for UDP).
culvert does not interpret the application protocol and does not implement
SOCKS5, DNS-specific proxying, or HTTP proxy semantics. TCP mappings support
IP/hostname targets (`ip:port`, `hostname:port`) and Unix domain sockets
(`unix://path`); UDP mappings support IP/hostname addresses (`ip:port`,
`hostname:port`) with per-peer NAT flow demultiplexing and idle expiration.

The runtime is split into three independent parts — frontend (port mapping
service), core (tunnel session lifecycle), and backend (uniform I/O over relay
channels) — sharing a neutral `io` vocabulary. Core dispatches I/O through a
`PumpRegistry` keyed by channel kind and never moves bytes itself. Both TCP
(`TcpFrontend` + `StreamStreamPump`) and UDP (`UdpFrontend` + `ConnectedUdpSocket`
+ `DatagramStreamPump` / `StreamDatagramPump` / `DatagramDatagramPump`) are wired
through those registries without changing `core::serve`. See
[DESIGN.md](DESIGN.md#57-three-layer-boundaries-and-the-pump-registry).

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

Tools such as `wstunnel`, `frp`, `Xray`, and `Hysteria` also provide TCP and UDP port
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
| UC-1 | Publish a local service on a remote host (reverse mapping) | Client behind NAT connects to the ingress endpoint, sends `RegisterReverse`, the server binds the requested TCP/UDS or UDP address, and each inbound connection or UDP NAT flow is relayed back to the client's local target. |
| UC-2 | Reach a private service from a local port (forward mapping) | Client binds `mappings.listen` locally (TCP or UDP); each local connection or UDP client flow opens a tunnel stream to the ingress endpoint; the server opens the corresponding TCP/UDS connection or `ConnectedUdpSocket` to `mappings.target` and relays in both directions. |
| UC-3 | Carry several ports over one endpoint (cluster) | One `tunnels` entry declares `mappings` containing `forward` and/or `reverse` groups (TCP and/or UDP, even sharing the same port). Forward mappings share the outbound pool, and reverse mappings share a unified inbound pool keyed by Tunnel UUID when `pool_size` is set; in-band 0-RTT target activation (`[magic][len][target]` symmetrically with forward mode with `0xAC` ACK) is used when idle streams exist, eliminating per-mapping connection proliferation and falling back to signaling when the pool is empty or activation fails. |
| UC-4 | Reach a Unix domain socket through a tunnel | TCP `mappings.target` or `mappings.listen` uses `unix://path`; the ingress side or client side binds/connects the socket directly. |
| UC-5 | Deploy behind a reverse proxy | `server.listen` uses `ws+unix://...` or `ws://127.0.0.1:...`; the proxy handles public TLS and forwards the configured HTTP path to culvert (capable of carrying both TCP and UDP mappings). |
| UC-6 | Deploy without a reverse proxy | `server.listen` uses a TLS scheme (`wss`, `h3`, `https`, `quic`, `wss+unix`). culvert terminates TLS using the configured certificate and key, or an ephemeral self-signed certificate when neither is provided. |
| UC-7 | Apply per-endpoint network policy | `tunnels[].endpoint` can be an object with its own `dns`, `dialer`, and `tls` blocks, overriding the global defaults for that endpoint only. |
| UC-8 | Validate a deployment file before starting | `culvert --check -c <file>` parses and validates the configuration without starting listeners or tunnels. |
| UC-9 | Forward or expose UDP services | Setting `protocol: "UDP"` demultiplexes inbound UDP datagrams per peer `SocketAddr` into `IoChannel::Datagram` flows, frames packets with `[u16 len \| payload]` over pooled or on-demand relay streams, and expires idle NAT flows after `udp_idle_timeout`. |

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

| Mode | Listener location | Target location | Protocols | Pool |
| :--- | :--- | :--- | :--- | :--- |
| `forward` (default) | Client host | Dialed by the ingress server | `tcp` (default; IP/host/UDS), `udp` (IP/host) | Shared outbound pool when `pool_size > 0` (`0xA5` TCP / `0xA7` UDP) |
| `reverse` | Ingress server (registered over a control channel) | Dialed by the client host | `tcp` (default; IP/host/UDS), `udp` (IP/host) | Unified Tunnel-scoped inbound pool when `pool_size > 0` or `pool.reverse > 0` (`[magic][len][target]` symmetric 0-RTT with `0xAC` round-trip ACK and signaling fallback; legacy peers fall back to per-`(protocol, listen)` pool) |

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
- **FR-3.5** Every tunnel requires a unique non-empty `tag` and a `mappings` object containing `forward` and/or `reverse` groups with at least one mapping rule. Each mapping rule requires a mandatory `protocol` (`"TCP"` or `"UDP"`, case-insensitive, with no default allowed) and an optional `udp_idle_timeout`. `listen` represents a listening socket and supports `:port` (IPv6/IPv4 dual-stack binding on `[::]:port` via `socket2` with `set_only_v6(false)` on Windows, Linux, and macOS), `addr:port` (OS default, e.g. `127.0.0.1:8080`), `hostname:port`, or (for TCP mappings only) `unix://path`; the port must be present and non-zero for non-UDS forms. `target` represents the outbound dial destination (`connect`) and requires host and port: `ip:port`, `hostname:port`, `[ipv6]:port`, or (for TCP mappings only) `unix://path` (`:port` is strictly rejected for `target`). UDP mappings must not specify `unix://` domain socket addresses for `listen` or `target` (though UDP mappings may tunnel over `ws+unix://` / `wss+unix://` transport endpoints). Server listen URLs (`server.listen`) also support `:port` dual-stack binding (e.g. `wss://:8443/tunnel`, `h3://:8443/tunnel`, or plain `:8443`).
- **FR-3.6** Duplicate `(protocol, listen)` pairs across forward mappings are
  rejected; a TCP mapping and a UDP mapping may bind the same local port.
  Reverse listen addresses are not subject to this duplicate check.
- **FR-3.7** Load-time validation covers log level and output, worker thread
  count, token length, pool size, retry bounds, TLS version range
  (`1.2`/`1.3`, min not above max), PEM certificate/key/CA files, DNS
  resolver definitions, and `EndpointContext` construction. Effective
  `idle_timeout` must be greater than effective `heartbeat_interval` after
  defaults are merged. Any configured `udp_idle_timeout` (under `dialer`,
  `server`, `endpoint.dialer`, or `mappings[]`) must be greater than `0`
  (defaulting to `30s`, `DEFAULT_UDP_IDLE_TIMEOUT`).
- **FR-3.8** `culvert --check` performs this static validation only; it does
  not bind addresses or start the runtime.
- **FR-3.9** `tunnels[].multiplex` and `tunnels[].failover` are present in the
  data model but are rejected by the loader because they are not wired into
  the data path.

### FR-4 Forward mappings and the stream pool

- **FR-4.1** A forward mapping binds `mappings.listen` on the client host
  (`TcpFrontend` for `tcp`, `UdpFrontend` for `udp`) and relays each accepted
  connection or per-peer UDP NAT flow to `mappings.target` through the ingress
  endpoint.
- **FR-4.2** When `mappings.forward.pool_size` is set to a positive value, all forward mappings
  of that tunnel share one `ReplenishingStreamPool`. Without pool configuration, each
  accepted connection or UDP flow opens a new transport stream through a
  connector shared by that mapping, tagging the request with `protocol`
  (`x-tunnel-proto: tcp | udp` on HTTP/WS, or `StreamMetadata.protocol` on raw QUIC).
  Tunnels configure direction-specific pools under `mappings.forward.pool_size` and `mappings.reverse.pool_size`.
- **FR-4.3** Pooled forward streams are pre-warmed without a fixed backend
  target or L4 protocol. The server keeps them idle and does not dial any
  target service or allocate a `ConnectedUdpSocket` until the client sends an
  activation packet.
- **FR-4.4** Activation packet: one magic byte (`0xA5` `ACTIVATE_STREAM_MAGIC`
  for TCP, `0xA7` `ACTIVATE_STREAM_UDP` for UDP; `0xA6` `ACTIVATE_STREAM_PROMOTE`
  is reserved for reverse control promotion), a two-byte big-endian target
  length, then the UTF-8 target string. The server reads it with a 10-second
  timeout and rejects payloads of 512 bytes or more. An invalid magic byte
  closes the stream.
- **FR-4.5** The pool is an actor with idle streams, pending acquire requests,
  concurrent dial limits, heartbeat sweeps, dead-stream eviction, and backoff
  replenishment. Acquire requests wait at most 15 seconds. The pool caps idle
  pre-warmed inventory at 64 (`POOL_MAX_CAPACITY`), decouples active relay
  throughput from replenishment gating, and supports up to 16 concurrent dials
  (`POOL_MAX_CONCURRENT_CONNECTS`). Acquire prefers the freshest idle stream
  (LIFO); a stream with a probe write in flight is skipped, not destroyed. Only
  over-age or confirmed-dead streams are evicted, and shared transport sessions
  are invalidated at most once per Acquire on a real eviction.
- **FR-4.6** An acquired stream is used once. Closing it decrements the pool's
  atomic active-stream counter through a guard; the actor replenishes idle streams in
  the background. Streams are not returned to the idle queue after use. Cancelled
  acquire requests restore any popped idle stream back to the pool or waiters.
- **FR-4.7** In a cluster with both forward and reverse mappings, forward mappings share the Outbound Pool, while reverse mappings use the Inbound Pool. Reverse mappings never draw from the forward pool.
- **FR-4.8** Tunnels support custom `http_headers: { <HeaderName>: <HeaderValue> }` configured at the tunnel root or under `endpoint`. Handshake connectors (WebSocket, HTTP/3, and UDS) attach these headers to outgoing connection requests. A configured `Host` (case-insensitive) overrides the default `host:port` authority header, enabling transparent routing through upstream reverse proxies (e.g. Caddy, Nginx, Traefik, Cloudflare). Reserved transport framing headers (e.g. `Upgrade`, `Sec-WebSocket-Key`) cannot be overwritten by custom headers. Header names and values are validated fail-fast during config loading against syntax and CRLF injection.

### FR-5 Reverse mappings, 0-RTT inbound pool, and control protocol

- **FR-5.1** The client opens a control stream in `reverse-ctrl` mode and
  sends `RegisterReverse { listen_addr, target_addr, pool_size, protocol, tunnel_id }`,
  where `pool_size` is the effective clamped worker count, `protocol` is
  `L4Protocol::Tcp` or `L4Protocol::Udp`, and `tunnel_id` is the Tunnel UUID
  (all `#[serde(default)]` on the wire so older peers interoperate). The server derives
  the inbound-queue soft cap, binds the requested TCP/UDS or UDP frontend, and replies with
  `RegisterAck { listen_addr, success, error_msg }`.
- **FR-5.2** Control messages are encoded with MessagePack
  (`to_vec_named`) over a length-delimited frame. Variants are
  `RegisterReverse`, `RegisterAck`, `NewConnection`, `Ping`, `Pong`.
- **FR-5.3** When `mappings.reverse.pool_size` is configured on a tunnel with reverse mappings,
  the client pre-warms a single shared pool of idle reverse streams for the entire tunnel
  (`mode = reverse`, `pool_idle = true`, `tunnel_id = tunnel_uuid`).
  On the server side, `SessionManager` maintains a unified `ReverseInboundPool` per Tunnel UUID (`tunnel_id`).
  All reverse mappings belonging to the same tunnel draw from this shared inbound pool, eliminating the
  multiplicative $(M \times S)$ stream proliferation. Legacy connections without `tunnel_id` fall back
  to per-`(protocol, listen_addr)` pools for backward compatibility.
- **FR-5.4** Fast path (0-RTT): For each inbound connection or new UDP peer
  flow on any reverse listener of the tunnel, if an idle stream is available in the shared inbound
  pool, the server pops it and sends the activation packet symmetrically with forward mode:
  one magic byte (`0xA5` for TCP, `0xA7` for UDP), a two-byte big-endian target length, and the UTF-8
  target string (`mappings.target`), followed by reading back `0xAC` (`ACTIVATE_STREAM_ACK`) bounded by
  `reverse_frame_timeout(idle_timeout)`. The client worker reads the activation packet using the unified
  activation codec, echoes `0xAC` with a 2-second timeout, dials the dynamically received local
  `mappings.target` (TCP/UDS or `ConnectedUdpSocket`), starts relaying, and replenishes the tunnel's
  inbound pool in the background. This guarantees that silently-dead (half-open) idle streams are detected
  rather than blackholing the visitor, while enabling late-binding target routing across mappings.
- **FR-5.5** Fallback path (on-demand signaling): If no idle stream is
  available (e.g. pool exhausted under burst or `pool_size` not set), or if
  fast-path 0-RTT activation fails (missing `0xAC` ACK, timeout, or I/O error),
  the server routes the visitor through an internal failover channel to
  `signal_new_connection`: it allocates a session id, registers a pending session
  guard in `SessionManager`, and sends `NewConnection { session_id, client_addr }`
  over the control channel. The client opens the local target channel matching
  its `protocol` (`Tcp` or `Udp`), opens a `reverse-data` stream carrying
  `x-session-id` and `x-tunnel-proto`, and the server pairs it with the waiting
  inbound channel through `SessionManager`. Pairing has a 10-second timeout
  (`DEFAULT_SESSION_PAIR_TIMEOUT`).
- **FR-5.6** Both sides are handed to `core::serve` as `IoChannel`s. The
  control channel stays open inside a `SignalingChannel` actor with
  activity-gated `Ping`/`Pong` and an idle timeout, backed by a `SignalingRouter`
  capable of server-initiated emergency promotion (`0xA6`), reverse-listener
  grace retention (`DEFAULT_REVERSE_GRACE_PERIOD`, 10 s capped by `idle_timeout`,
  resumable via `ReverseInboundPool` promotion or `RegisterReverse` reattach),
  and sliding-window `session_id` deduplication (`CommandDeduplicator`); any
  inbound frame refreshes the activity deadline, while outbound probes alone
  do not.
- **FR-5.7** Control-channel reconnection uses an exponential backoff from
  500 ms up to 30 s, reset after 30 s of stable operation.
- **FR-5.8** Inbound pool bounds and single-owner lifetime: the client caps
  pre-warm workers at 64 (`POOL_MAX_CAPACITY`) and advertises that clamped value;
  the server registers each pool before `RegisterAck` with a soft cap of twice
  the negotiated count, FIFO evicts the oldest entry on overflow, drains the
  previous generation on re-registration, and keys pools by host and port (never
  guesses a target into an unrelated listener). Only streams younger than the
  effective idle window and passing a liveness check are paired. The server is the
  sole owner of an idle stream's lifetime — the client worker has no independent
  recycle timer — so a connection the server has committed to a visitor cannot be
  closed by the peer. Activation writes and `NewConnection` sends share a deadline
  derived from the configured idle window (1/3, clamped to 5–15 s and never above
  the idle window), so high-RTT / low-bandwidth links do not tear down the listener.
  Pre-warm dials are throttled to at most 16 concurrent handshakes across workers
  (`POOL_MAX_CONCURRENT_CONNECTS`) with staggered exponential backoff up to 10 s
  (capped at 1 s when waiters queue), protecting low-bandwidth links from dial storms.
  Listener teardown immediately drops the listening socket and unregisters the
  pool before waiting for in-flight visitor relays, allowing prompt reconnection
  without port binding conflicts. An empty or stale pool always falls back to
  signaling instead of stalling visitors. Relay tasks are owned and awaited by
  their worker; none are detached.

### FR-6 Stream and datagram pumps

- **FR-6.1** `core::serve` selects a `Pump` by channel-kind pair. The registered
  `(Stream, Stream)` pump, `StreamStreamPump::relay_with_heartbeat`, drives both
  directions from a single `poll_fn` state machine with two 64 KB buffers.
- **FR-6.2** A probe is emitted only when neither direction has moved data for
  one `heartbeat_interval`; a stream silent for `idle_timeout` is closed.
  `idle_timeout` must exceed `heartbeat_interval`. An OS-level `SO_ERROR`
  counts as death only for terminal connection failures; a failed socket query
  or transient status falls through to the peek/EOF check.
- **FR-6.3** Half-close is supported: a finished read direction flushes its
  buffered bytes and shuts down its write side while the other direction can
  continue. A half-closed stream must remain `Alive` under liveness sweeps
  until the opposite direction finishes; treating local FIN flags as dead
  truncates the remaining response tail.
- **FR-6.4** On cancellation, the relay drains in-flight data for at most
  500 ms before terminating.
- **FR-6.5** UDP relay pairs are handled by the registered `(Datagram, Stream)`
  (`DatagramStreamPump`) and `(Stream, Datagram)` (`StreamDatagramPump`) pumps,
  as well as `(Datagram, Datagram)` (`DatagramDatagramPump`). Over byte streams,
  each datagram is encoded in-band as `[u16 payload_len (big-endian) | payload]`,
  preserving exact packet boundaries and 0-byte datagrams up to
  `MAX_UDP_DATAGRAM_SIZE` (65,507 bytes), with batched send/recv (`DATAGRAM_BATCH_SIZE = 16`).
- **FR-6.6** Datagram/stream relay timers track `last_dgram_activity` using the
  configured `udp_idle_timeout` (default 30 s, independent of `idle_timeout`):
  when no datagram is received or sent within `udp_idle_timeout`, the pump exits
  cleanly (`Ok`), closing the relay stream (propagating FIN to tear down the
  remote paired `UdpFlowChannel` / `ConnectedUdpSocket`). Subsequent datagrams
  from the same client port transparently open a fresh tunnel session. While
  active, transport keepalive probes still fire at `heartbeat_interval` and
  clamp pre-activation stream activity to `pump_start_ms` so long-idle pooled
  streams are never killed immediately on promotion.

### FR-7 Supervision, retry, and shutdown

- **FR-7.1** `TunnelSupervisor` owns one server runner and one worker per
  tunnel, all tracked in a `TaskTracker` bound to a `CancellationToken`.
- **FR-7.2** A tunnel worker that fails is restarted after the fixed delay
  `tunnels[].retry.delay` (default 3 s), up to `retry.max_retries` attempts.
  Omitting `max_retries` retries indefinitely. Permanent errors
  (`Config`, `UnsupportedScheme`, `UnsupportedChannel`, `AuthFailed`, `Tls`)
  are not retried.
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
  (default 15 s), `idle_timeout` (default 30 s), `udp_idle_timeout` (default 30 s,
  also configurable on `server` and per mapping). `bind_interface` relies on
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

### FR-9 Three-layer independence

- **FR-9.1** `core::serve` depends only on the neutral `io` vocabulary; it never
  names a `Frontend` or `Transport*` trait. The runtime layers build a
  `PumpRequest` from those traits. Adding a new frontend capability must not
  require a change to core business logic (adding UDP left `core::serve`
  completely unchanged).
- **FR-9.2** Both frontend and backend yield `IoChannel = Stream | Datagram`.
  `core::serve` selects a `Pump` from `PumpRegistry` by
  `(frontend.kind(), backend.kind())` and contains no match on channel variants
  and no byte-copying code.
- **FR-9.3** `frontend::bind` dispatches on `MappingSpec.protocol` through
  `FrontendRegistry`, with both `L4Protocol::Tcp` (`TcpFrontendFactory`) and
  `L4Protocol::Udp` (`UdpFrontendFactory`) registered. `PumpRegistry` seeds all
  four combinations: `(Stream, Stream)`, `(Datagram, Stream)`, `(Stream, Datagram)`,
  and `(Datagram, Datagram)`.
- **FR-9.4** A `TunnelStream` implementation advertises its write/half-close
  capabilities through `StreamCaps` (`owned_write`, `half_close`); `IoChannel`
  forwards the query. The pump reads them once per direction so an owned `Bytes`
  write path cannot be left unselected.
- **FR-9.5** The reverse control plane (`control/`, `session/`, `pool/`) is
  stream-scoped for control framing and pre-warming while forwarding `L4Protocol`
  hints (`RegisterReverse.protocol`, `x-tunnel-proto`, `ACTIVATE_STREAM_UDP`);
  datagram payload transport is handled solely by the registered pumps.
- **FR-9.6** UDP port mapping demultiplexes incoming datagrams on one bound
  `UdpSocket` per client `SocketAddr` into `UdpFlowChannel` (`IoChannel::Datagram`)
  and connects outbound targets with `ConnectedUdpSocket`. When a session drops on
  `UdpFlowChannel`, its NAT entry is atomically removed from the listener map so
  subsequent packets from the same source address open a fresh flow cleanly.

---

## 5. Non-Functional and Operational Constraints

| ID | Constraint | Implementation basis |
| :--- | :--- | :--- |
| NFR-1 | Streams and datagrams are relayed without interpreting application protocols. | `StreamStreamPump` moves `Bytes` between two `TunnelStream`s; `DatagramStreamPump` / `StreamDatagramPump` / `DatagramDatagramPump` transfer opaque datagram payloads with `[u16 len \| payload]` framing. |
| NFR-2 | Every asynchronous task is owned and awaitable; shutdown cancels then awaits. | `TaskTracker`, `CancelFutureExt`, explicit `shutdown()` on listeners; no `detach_all()` on the normal path. |
| NFR-3 | Every await is cancellation-aware or bounded by a timeout. | Handshake, activation read, session pairing, relay drain, UDP NAT expiry, and pool acquire all have explicit bounds in `consts.rs`. |
| NFR-4 | Configuration errors are surfaced before the daemon starts. | `ConfigLoader::validate` runs on every startup and on `--check`. |
| NFR-5 | Unix domain sockets are available through a single platform abstraction. | `frontend::tcp::TcpFrontend` wraps `platform::GenericListener` / `PlatformUdsListener`, with a dedicated accept thread on Windows. |
| NFR-6 | The relay hot path uses a single `poll_fn` state machine rather than a per-direction task. | `io/pump.rs` (`StreamStreamPump`, `DatagramStreamPump`, `DatagramDatagramPump`). This is a deliberate performance constraint; it must not be replaced with `tokio::select!` in the hot path. |
| NFR-7 | Documentation states implemented behavior only. | Unwired options are rejected by the loader and listed in Section 6. |
| NFR-8 | Frontend, core, and backend are independently replaceable. | `io/`, `frontend/`, `backend/`, `core::serve`, `PumpRegistry`; see FR-9. |
| NFR-9 | I/O capability is queried, not guessed. | `StreamCaps` / `poll_write_owned` replace the former dead `consumes_owned_write()` + `poll_write_bytes()` pair; `caps()` is read once per direction. |

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
| Seamless failover / stream migration / ring-buffer replay / cross-endpoint switching | Not implemented (`SignalingRouter` handles reverse control-channel emergency promotion via `0xA6` within one endpoint). |
| Automatic HTTP/3-to-WSS fallback | Not implemented. Each tunnel uses exactly the scheme in its endpoint URL. |
| P2P, STUN/ICE, NAT hole punching | Not implemented. |
| TOML configuration files | Not implemented. Configuration is YAML only. |
| DNS-specific resolver proxying for tunneled clients, SOCKS5, HTTP proxy | Not implemented (raw DNS UDP traffic can be forwarded via `protocol: "udp"`). |
| ACME certificate issuance and renewal | Not implemented. Certificates come from files, an ephemeral self-signed fallback, or an external reverse proxy. |
| Protocol-level `CMD_TUNNEL_RESET` / `CMD_RESET_ACK` draining and returning streams to the idle pool | Not implemented. Pooled streams are one-shot. |
| WebSocket SIMD masking claims, hardware acceleration claims | No such implementation is exposed by the product. |

---

## 7. Configuration Example

The checked-in [`config-example.yaml`](../config-example.yaml) is the canonical
example and is asserted to parse cleanly by the loader tests. It defines a
server with five heterogeneous listeners, a multi-mapping tunnel sharing a
pool (carrying forward/reverse TCP, UDS, and UDP mappings), and a second HTTP/3 tunnel.
The complete parameter reference is in
[README.md](../README.md#configuration-reference).

```yaml
log:
  level: "info"
  output: "stdout"
dialer:
  udp_idle_timeout: "30s"
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
    retry:
      delay: "3s"
      max_retries: 10
    mappings:
      forward:
        pool_size: 8
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:13306"
            target: "10.0.0.5:3306"
          - protocol: "UDP"
            listen: "127.0.0.1:15353"
            target: "1.1.1.1:53"
            udp_idle_timeout: "15s"
      reverse:
        pool_size: 4
        rules:
          - protocol: "TCP"
            listen: "0.0.0.0:2222"
            target: "127.0.0.1:22"
          - protocol: "UDP"
            listen: "0.0.0.0:51820"
            target: "127.0.0.1:51820"
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
| AC-9 | Custom HTTP headers and Host override are applied to outgoing handshakes and validated fail-fast. | `test_http_headers.rs`, `test_config_and_dns.rs`. |
| AC-10 | Frontend, core, and backend are independently replaceable; `core::serve` dispatches by channel kind across registered stream and datagram pairs, and an unregistered channel pair fails with `UnsupportedChannel`. | `io::pump`, `frontend`, and `core::serve` unit tests (`test_serve_recognizes_registered_channel_pairs`, `test_registry_extensibility`). |
| AC-11 | Forward and reverse UDP port mapping (0-byte and multi-KB datagrams, multi-client NAT demux, 0-RTT `0xA7` pool & `NewConnection` fallback, same-port TCP+UDP coexistence, and `udp_idle_timeout` flow expiration/recreation) works end to end across WS, HTTP/3, and raw QUIC. | `crates/culvert/tests/test_udp_tunnel.rs`, `config::loader` & `session::manager` UDP unit tests. |

Run the suite with `cargo test --workspace`.

---

## Appendix A — Disposition of the early draft

`docs/culvert.md` was an early architectural draft marked deprecated. Its
content is merged as follows.

**Retained, restated against the implementation:** reverse-proxy deployment
with an ingress on a Unix domain socket or loopback; no ACME certificate
management inside culvert; optional pre-shared token authentication; absence of
DNS-specific proxying logic and SOCKS5/HTTP proxy semantics; one configuration
file combining server ingress and client tunnels; forward and reverse port
forwarding (TCP/UDS and UDP, with UDP port mapping built on the three-layer
`Frontend`/`PumpRegistry` split); per-endpoint DNS/TLS/dialer settings; pre-warmed
streams whose backend target is dialed only on activation; path-based request
matching.

**Retired (not implemented):** P2P readiness, STUN/ICE candidates, cross-endpoint
primary/standby failover, seamless migration with ring-buffer replay,
`CMD_TUNNEL_RESET` / `CMD_RESET_ACK` draining and pool reuse, HTTP/3 `POST /tunnel`
extended CONNECT framing (the implementation uses `CONNECT` for `h3`/`https` and
a MessagePack metadata handshake for `quic`), `server.toml` / `client.toml` TOML
configuration, automatic QUIC-to-WSS fallback, Caddyfile and sysctl tuning
recipes, throughput/latency/memory targets, SIMD masking claims, milestone plans,
and subjective evaluations of other tools.

The design details of what replaced that draft are in
[DESIGN.md](DESIGN.md); usage and the full parameter reference are in
[README.md](../README.md).
