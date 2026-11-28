# Product Requirements Document — culvert

| Field | Value |
| :--- | :--- |
| Document | Product Requirements Document |
| Product | culvert (workspace version 0.3.8) |
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
inbound SOCKS5, DNS-specific proxying, or HTTP proxy server semantics for
tunneled clients (outbound client endpoints can connect through intermediate
HTTP CONNECT or SOCKS5 proxies via `dialer.proxy`). TCP mappings support
IP/hostname targets (`ip:port`, `hostname:port`) and Unix domain sockets
(`unix://path`); UDP mappings support IP/hostname addresses (`ip:port`,
`hostname:port`) with per-peer NAT flow demultiplexing and idle expiration.

The runtime is split into three layers — frontend (port mapping service), the
Coordinator + IO Task runtime (tunnel session lifecycle), and backend
(uniform I/O over relay channels) — sharing a neutral `io` vocabulary. A
single-writer Coordinator owns task state and resource relationships; each IO
Task runs the data-plane relay as a `poll_fn` state machine without Coordinator
involvement. Both TCP and UDP mappings run through the same relay stages
(`ChannelRelay` dispatches stream/datagram channel pairs). See
[DESIGN.md](DESIGN.md#6-architecture-coordinator-io-task-and-state).

### 1.1 Why this product exists

+ A service behind NAT or a restrictive firewall has no public address, but a
  host with a reachable ingress endpoint does. Reverse mappings expose the
  former through the latter.
+ A client host needs access to a service reachable only from the ingress host.
  Forward mappings provide a local listener that is relayed to that service.
+ Several ports need to traverse the same tunnel. One tunnel declaration can
  carry multiple forward and reverse mappings and share one pre-warmed stream
  pool.
+ In a typical deployment a reverse proxy terminates public TLS and forwards
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
| UC-3 | Carry several ports over one endpoint (cluster) | One `tunnels` entry declares `mappings` containing `forward` and/or `reverse` groups (TCP and/or UDP, even sharing the same port). Forward mappings share one idle Coordinator, and reverse mappings share one reverse Coordinator keyed by `(protocol, listen_addr, tunnel_id)` when `pool_size` is set; in-band 0-RTT target activation (`[magic][len][target]` symmetrically with forward mode with `0xAC` ACK) is used when idle streams exist, eliminating per-mapping connection proliferation and falling back to signaling when the pool is empty or activation fails. |
| UC-4 | Reach a Unix domain socket through a tunnel | TCP `mappings.target` or `mappings.listen` uses `unix://path`; the ingress side or client side binds/connects the socket directly. |
| UC-5 | Deploy behind a reverse proxy | `server.listen` uses `ws+unix://...` or `ws://127.0.0.1:...`; the proxy handles public TLS and forwards the configured HTTP path to culvert (capable of carrying both TCP and UDP mappings). |
| UC-6 | Deploy without a reverse proxy | `server.listen` uses a TLS scheme (`wss`, `h3`, `https`, `quic`, `wss+unix`). culvert terminates TLS using the configured certificate and key, or an ephemeral self-signed certificate when neither is provided. |
| UC-7 | Apply per-endpoint network policy | `tunnels[].endpoint` can be an object with its own `dns`, `dialer` (including outbound proxy), and `tls` blocks, overriding the global defaults for that endpoint only. |
| UC-8 | Validate a deployment file before starting | `culvert --check -c <file>` parses and validates the configuration without starting listeners or tunnels. |
| UC-9 | Forward or expose UDP services | Setting `protocol: "UDP"` demultiplexes inbound UDP datagrams per peer `SocketAddr` into `IoChannel::Datagram` flows, frames packets with `[u16 len \| payload]` over pooled or on-demand relay streams, and expires idle NAT flows after `udp_idle_timeout`. |
| UC-10 | Connect through an outbound HTTP/SOCKS5 proxy | Client tunnels dial through an intermediate HTTP/HTTPS CONNECT or SOCKS5 proxy via `dialer.proxy`, keeping target endpoint resolution delegated to the remote proxy. |

---

## 3. Product Surface

### 3.1 Process and entry points

+ One binary, `culvert`, built from `crates/culvert-cli`.
+ One configuration file (YAML), default `config.yaml`.
+ Public library API in `crates/culvert`: `run_server`, `run_client`, `run`,
  `run_daemon`, `ConfigLoader`, `ClientRunner`, `ServerRunner`,
  `TunnelSupervisor`.
+ A configuration may define `server`, `tunnels`, or both. At least one server
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

+ **FR-1.1** `server.listen` accepts a single URL string or a list. Each URL is
  parsed into a scheme, bind address, optional HTTP path, and optional UDS
  socket mode. A port is mandatory for TCP URLs and must not be `0`.
+ **FR-1.2** The scheme must be explicit. An URL without `://` is rejected.
  Only `ws`, `wss`, `https`, `h3`, `quic`, `ws+unix`, and `wss+unix` are
  accepted; `uds://`, `unix://`, and `ws+uds://` are not server schemes.
+ **FR-1.3** Each listener is dispatched to a registered transport driver.
  Multiple heterogeneous listeners run concurrently in one process.
+ **FR-1.4** When an HTTP path is configured, a request whose path does not
  match is answered with `404 Not Found`.
+ **FR-1.5** On Unix domain socket listeners, `chmod=` sets the socket mode.
  Values are interpreted as octal (`0o660`, `0660`, `660`).
+ **FR-1.6** TLS schemes terminate TLS in-process. `server.certificate` and
  `server.key` (or `server.tls.certificate` / `server.tls.key`) supply the
  PEM material; when neither pair is present, an ephemeral self-signed
  certificate is generated. Certificate and key must be supplied together.
  Configuring certificates while every listener uses a plaintext scheme is
  rejected at load time.
+ **FR-1.7** `server.default_target` supplies the target for a forward stream
  that arrives without a target in its handshake and without an activation
  packet.
+ **FR-1.8 Dynamic Certificate Management & Hot-Reloading**
  All reading, parsing, and lifecycle management of certificate and private key
  files are unified through a dedicated certificate management module. When server
  certificates (`certificate`, `key`) or client CA certificates (`client_ca_certificate`)
  are supplied as file paths, the certificate manager automatically monitors them for
  changes on disk. To prevent race conditions during certificate renewal (e.g. Caddy or
  external scripts copying or writing key and cert files asynchronously), updates are
  processed through an asynchronous debouncing pipeline (default 1 s quiet window). The
  reloader executes two-phase in-memory validation (PEM syntax parsing, private key
  and certificate public key pairing verification, and validity bounds) before committing
  changes atomically. Invalid or incomplete files never displace the currently active
  certificate. Dynamic certificate resolution integrates natively with Rustls
  (`ResolvesServerCert`), ensuring zero downtime: active sessions are uninterrupted,
  new handshakes immediately use updated certificates, and listeners require no restarts
  or socket rebinds. A decoupled event bus broadcasts certificate reload outcomes.

### FR-2 Authentication

+ **FR-2.1** `server.token` and `tunnels[].token` are optional. Omitting them
  disables token verification. An empty string is rejected at load time.
+ **FR-2.2** Tokens are at most 256 bytes; longer values are rejected at load
  time.
+ **FR-2.3** WebSocket and Unix-socket clients send the token as
  `x-tunnel-auth: <token>`. HTTP/3 clients send both
  `authorization: Bearer <token>` and `x-tunnel-auth: <token>`; HTTP/3
  listeners read `authorization` first and fall back to `x-tunnel-auth`,
  accepting either the raw token or the `Bearer`-prefixed value. Raw QUIC
  carries the token in its length-prefixed handshake. The server compares the
  received token with the configured value using a constant-time comparison
  over fixed-size buffers.
+ **FR-2.4** A missing or non-matching token is rejected during the handshake
  (`401` for malformed headers, `403` for a token mismatch on WebSocket
  listeners; `401` on HTTP/3 listeners). A valid tunnel stream is not created.

### FR-3 Configuration

+ **FR-3.1** The configuration is YAML. All top-level and nested sections use
  `deny_unknown_fields`; unknown keys are errors.
+ **FR-3.2** Global sections are `log`, `worker_threads`, `dns`, `dialer`,
  `tls`, `server`, `tunnels`, and `quic`. The complete key reference is in
  [README.md](../README.md#configuration-reference).
+ **FR-3.3** `tunnels[].endpoint` is either an URL string or an object with
  `url` plus optional `dns`, `dialer`, and `tls` blocks. Effective values are
  merged field-by-field: endpoint value, then global value, then built-in
  default.
+ **FR-3.4** A `tls` block on a non-TLS endpoint scheme is rejected.
+ **FR-3.5** Every tunnel requires a unique non-empty `tag` and a `mappings` object containing `forward` and/or `reverse` groups with at least one mapping rule. Each mapping rule requires a mandatory `protocol` (`"TCP"` or `"UDP"`, case-insensitive, with no default allowed) and an optional `udp_idle_timeout`. `listen` represents a listening socket and supports `:port` (IPv6/IPv4 dual-stack binding on `[::]:port` via `socket2` with `set_only_v6(false)` on Windows, Linux, and macOS), `addr:port` (OS default, e.g. `127.0.0.1:8080`), `hostname:port`, or (for TCP mappings only) `unix://path`; the port must be present and non-zero for non-UDS forms. `target` represents the outbound dial destination (`connect`) and requires host and port: `ip:port`, `hostname:port`, `[ipv6]:port`, or (for TCP mappings only) `unix://path` (`:port` is strictly rejected for `target`). UDP mappings must not specify `unix://` domain socket addresses for `listen` or `target` (though UDP mappings may tunnel over `ws+unix://` / `wss+unix://` transport endpoints). Server listen URLs (`server.listen`) also support `:port` dual-stack binding (e.g. `wss://:8443/tunnel`, `h3://:8443/tunnel`, or plain `:8443`).
+ **FR-3.6** Duplicate `(protocol, listen)` pairs across forward mappings are
  rejected; a TCP mapping and a UDP mapping may bind the same local port.
  Reverse listen addresses are not subject to this duplicate check.
+ **FR-3.7** Load-time validation covers log level and output, worker thread
  count, token length, pool size, retry bounds, TLS version range
  (`1.2`/`1.3`, min not above max), PEM certificate/key/CA files, DNS
  resolver definitions, and `EndpointContext` construction. Effective
  `idle_timeout` must be greater than effective `heartbeat_interval` after
  defaults are merged. Any configured `udp_idle_timeout` (under `dialer`,
  `server`, `endpoint.dialer`, or `mappings[]`) must be greater than `0`
  (defaulting to `30s`, `DEFAULT_UDP_IDLE_TIMEOUT`).
+ **FR-3.8** `culvert --check` performs this static validation only; it does
  not bind addresses or start the runtime.
+ **FR-3.9** `tunnels[].multiplex` and `tunnels[].failover` are present in the
  data model but are rejected by the loader because they are not wired into
  the data path.
+ **FR-3.10** Outbound client proxies: `dialer.proxy` (global or per-endpoint)
  accepts an HTTP, HTTPS, or SOCKS5 proxy URL (`http://`, `https://`, `socks5://`,
  `socks5h://`), with optional username/password credentials. When a proxy is
  configured, the client's local DNS resolver resolves the proxy server's own
  host only; the target endpoint hostname is passed verbatim to the proxy
  (via HTTP `CONNECT` or SOCKS5 domain name `ATYP = 0x03`) to eliminate local DNS
  poisoning and DNS leaks. Transport connectors (`ws://`, `wss://`) tunnel
  transparently through the established proxy stream with end-to-end TLS and
  WebSocket upgrade. Configuring a proxy on a QUIC/HTTP/3 endpoint (`h3://`,
  `quic://`) is rejected at validation time.

### FR-4 Forward mappings and idle stream reuse

+ **FR-4.1** A forward mapping binds `mappings.listen` on the client host
  (`TcpFrontend` for `tcp`, `UdpFrontend` for `udp`) and relays each accepted
  connection or per-peer UDP NAT flow to `mappings.target` through the ingress
  endpoint. The frontend uses a reusable accept slot so an in-flight accept or
  TLS handshake survives a competing shutdown branch.
+ **FR-4.2** Every accepted input is assigned to a tunnel `Coordinator`, which
  hands it to the freshest `Idle` IO Task, then an `Empty` slot, or spawns a new
  task. A task that owns no pre-warmed stream requests one from the Coordinator
  or dials directly; the request carries `L4Protocol` metadata
  (`x-tunnel-proto: tcp | udp` on HTTP/WS, or `StreamMetadata.protocol` on raw
  QUIC). `mappings.forward.pool_size` controls how many idle streams are
  pre-warmed; `mappings.reverse.pool_size` controls the reverse direction.
+ **FR-4.3** Pre-warmed forward streams have no fixed backend target. The
  server holds them in `ExternalState::Idle` and does not dial a target or
  allocate a `ConnectedUdpSocket` until activation. Each idle task drives its
  own liveness check and age-based eviction; there is no separate pool actor.
+ **FR-4.4** Activation packet: one magic byte (`0xA5` `ACTIVATE_STREAM_MAGIC`
  for TCP, `0xA7` `ACTIVATE_STREAM_UDP` for UDP; `0xA6` `ACTIVATE_STREAM_PROMOTE`
  is reserved for reverse control promotion), a two-byte big-endian target
  length, then the UTF-8 target string. All three fields are bounded; payloads
  of 512 bytes or more are rejected, and an invalid magic byte closes the
  stream. Forward activation is followed by a protocol-level acknowledgment
  step inside the task's `Connecting` stage.
+ **FR-4.5** Idle capacity is bounded by `POOL_MAX_CAPACITY` (64) and background
  dialing by `POOL_MAX_CONCURRENT_CONNECTS` (16). The Coordinator fills all
  available dial slots per reactor turn and backs off on failure, with a 1-second
  cap while callers wait. Idle acquisition prefers the freshest stream (LIFO)
  and skips streams whose liveness probe is in flight. Active relay count does
  not gate replenishment. Evicting one idle stream never closes the shared
  QUIC/H3 session — only the shared `QuicSessionHandle` can mark it dead.
+ **FR-4.6** A stream is used once: after a relay ends the `Closing` stage
  drains buffered bytes within a bounded window and drops the stream; it is not
  returned to the idle set. If a stream is delivered to a task in the wrong
  state, the task returns it to the Coordinator without blocking. Cancelled
  transport requests are removed from the waiter list and any in-flight stream
  is reassigned. Application-level stream recycling via a full half-close FIN
  handshake is not part of this release.
+ **FR-4.7** In a cluster with both directions, forward and reverse idle
  streams are kept in separate Coordinators. Reverse visitors never draw from
  the forward Coordinator's idle tasks.
+ **FR-4.8** Tunnels support custom `http_headers: { <HeaderName>: <HeaderValue> }` configured at the tunnel root or under `endpoint`. Handshake connectors (WebSocket, HTTP/3, and UDS) attach these headers to outgoing connection requests. A configured `Host` (case-insensitive) overrides the default `host:port` authority header, enabling transparent routing through upstream reverse proxies (e.g. Caddy, Nginx, Traefik, Cloudflare). Reserved transport framing headers (e.g. `Upgrade`, `Sec-WebSocket-Key`) cannot be overwritten by custom headers. Header names and values are validated fail-fast during config loading against syntax and CRLF injection.

### FR-5 Reverse mappings, 0-RTT inbound pool, and control protocol

+ **FR-5.1** The client opens a control stream in `reverse-ctrl` mode and
  sends `RegisterReverse { listen_addr, target_addr, pool_size, protocol, tunnel_id }`,
  where `pool_size` is the effective clamped worker count, `protocol` is
  `L4Protocol::Tcp` or `L4Protocol::Udp`, and `tunnel_id` is the Tunnel UUID
  (all `#[serde(default)]` on the wire so older peers interoperate). The server derives
  the inbound-queue soft cap, binds the requested TCP/UDS or UDP frontend, and replies with
  `RegisterAck { listen_addr, success, error_msg }`.
+ **FR-5.2** Control messages are encoded with MessagePack
  (`to_vec_named`) over a length-delimited frame. Variants are
  `RegisterReverse`, `RegisterAck`, `NewConnection`, `Ping`, `Pong`.
+ **FR-5.3** When `mappings.reverse.pool_size` is configured, the client
  pre-warms a single shared set of idle reverse streams for the whole tunnel
  (`mode = reverse`, `pool_idle = true`, `tunnel_id`). Their effective worker
  count is clamped once on the client and advertised in `RegisterReverse`, so
  the server derives its capacity from the same number. On the server, inbound
  idle streams are supplied to the reverse Coordinator and live as
  `ExternalState::Idle` IO Tasks. All reverse mappings of one tunnel share that
  Coordinator, avoiding per-mapping stream multiplication. `ReverseSessionHub`
  keys the Coordinator by `(protocol, listen_addr, tunnel_id)`; legacy peers
  without `tunnel_id` use a per-address key.
+ **FR-5.4** Fast path (0-RTT): for each inbound connection or UDP flow on a
  reverse listener, the Coordinator assigns the visitor to the freshest `Idle`
  task. Activation writes are performed inside the relay task, not the accept
  loop, so a burst of visitors is paired in parallel. The stream receives
  `0xA5` (TCP) or `0xA7` (UDP) plus the bound target, and the client echoes
  `0xAC` (`ACTIVATE_STREAM_ACK`) within `reverse_frame_timeout(idle_timeout)`.
  A missing ACK or stream error discards only that stream; the visitor falls
  back to signaling and the listener stays bound.
+ **FR-5.5** Fallback path (on-demand signaling): with no idle stream, the
  server allocates a session ID, registers a guarded pending entry in
  `ReverseSessionHub`, and sends `NewConnection` over the control channel. A
  single send timeout cleans up only that session. The client opens the local
  target (`Tcp` or `Udp`) and a `reverse-data` stream carrying `x-session-id`
  and `x-tunnel-proto`; the hub pairs it with the waiting visitor through a
  oneshot within `DEFAULT_SESSION_PAIR_TIMEOUT` (15 s). Both sides then run in
  an IO Task.
+ **FR-5.6** The control channel runs inside a `SignalingChannel` actor with
  activity-gated `Ping`/`Pong`, backed by a `SignalingRouter` with
  primary/standby routing and a `SendCommitGate` so pre-commit cancellation
  cannot emit a frame. The reverse server supports server-initiated emergency
  promotion (`0xA6`), listener grace retention (`reverse_grace_period`,
  10 s capped by `idle_timeout`, resumable by promotion or `RegisterReverse`
  reattach), and bounded sliding-window `session_id` deduplication
  (`CommandDeduplicator`, capacity 256). Any inbound frame refreshes the
  activity deadline; outbound probes alone do not.
+ **FR-5.7** Control-channel reconnection uses an exponential backoff from
  500 ms up to 30 s, reset after 30 s of stable operation.
+ **FR-5.8** Idle bounds and single-owner lifetime: the client caps pre-warm
  workers at 64 (`POOL_MAX_CAPACITY`); the server evicts the oldest idle task
  when its soft cap (`2 x pool_size`) is reached and raises a generation floor
  to reject stale inbound streams on reconnect. Only tasks that pass their
  liveness check and are younger than the effective idle window are assigned.
  The server is the sole owner of an idle stream's lifetime — the client has no
  independent recycle timer. Activation writes and `NewConnection` sends derive
  their deadline from the idle window (`idle_timeout / 3`, clamped to 5–15 s and
  never above the idle window), so high-RTT links do not tear down the listener.
  Background dials are capped at 16 concurrent handshakes with staggered
  exponential backoff up to 10 s (1 s when callers wait). All tasks are owned by
  a `JoinSet` and awaited; none are detached.

### FR-6 Stream and datagram relay

+ **FR-6.1** In the `Active` stage an IO Task runs one `ChannelRelay`, which
  statically dispatches to `DuplexRelay` (stream/stream), `DatagramStreamRelay`
  (datagram over stream, either direction), or `DatagramDatagramRelay`. A
  stream/stream relay drives both directions from a single `poll_fn` state
  machine with two 64 KB `TransferDirection` buffers. The hot path is a
  hand-written `poll_fn` state machine and is not replaced with `tokio::select!`.
+ **FR-6.2** A probe is emitted only when neither direction has moved data for
  one `heartbeat_interval`; a stream silent for `idle_timeout` is closed.
  `idle_timeout` must exceed `heartbeat_interval`. `poll_check_alive` drives
  the normal read state machine non-destructively and never polls an in-flight
  write future.
+ **FR-6.3** Half-close is supported: a finished read direction flushes its
  buffered bytes and shuts down its write side while the other direction can
  continue. A locally half-closed stream remains healthy under liveness sweeps
  until the opposite direction finishes; treating local FIN flags as dead
  truncates the remaining response tail.
+ **FR-6.4** On graceful drain, the `Closing` stage flushes in-flight bytes
  within a bounded `drain_timeout` and propagates genuine I/O errors instead of
  reporting them as a clean drain. Force stop drops resources immediately.
+ **FR-6.5** Datagrams over a byte stream use in-band framing
  `[u16 payload_len (big-endian) | payload]`, preserving exact packet boundaries
  and 0-byte datagrams up to `MAX_UDP_DATAGRAM_SIZE` (65,527 bytes). The relay
  batches up to 16 datagrams (`DATAGRAM_BATCH_SIZE`) and rejects an incomplete
  frame left in the buffer at EOF.
+ **FR-6.6** Datagram/stream relay tracks datagram activity with the configured
  `udp_idle_timeout` (default 30 s, independent of `idle_timeout`): when no
  datagram flows in either direction before the deadline, the task exits
  cleanly, closing the stream and propagating FIN so the paired
  `UdpFlowChannel` / `ConnectedUdpSocket` tears down. Subsequent datagrams from
  the same client port open a fresh tunnel flow. Transport keepalive still
  fires at `heartbeat_interval` while the flow is open.

### FR-7 Supervision, retry, and shutdown

+ **FR-7.1** `TunnelSupervisor` starts one `ServerRunner` (when a server is
  configured) and one `ClientRunner::run_tunnel_with_retry` worker per tunnel.
  All workers are owned by a `JoinSet`; the supervisor uses a biased
  cancellation branch and joins every handle before returning. `ClientRunner`
  uses the same pattern when one process runs several tunnels.
+ **FR-7.2** A tunnel worker that fails is restarted after the fixed delay
  `tunnels[].retry.delay` (default 3 s), up to `retry.max_retries` attempts.
  Omitting `max_retries` retries indefinitely. Permanent errors
  (`Config`, `UnsupportedScheme`, `UnsupportedChannel`, `AuthFailed`, `Tls`)
  are not retried.
+ **FR-7.3** Shutdown cancels the root token and its child tokens. Each
  Coordinator transitions `Running -> Draining` on `GracefulStop`: accepting
  and replenishment stop, `Active` tasks drain within `drain_timeout`, then
  `ForceStop` applies if the drain deadline expires. All `JoinSet` handles are
  awaited; tasks are never detached on the normal shutdown path.
+ **FR-7.4** The daemon installs a shutdown handler on a dedicated OS thread.
  The first termination signal cancels the token and starts a 10-second hard
  deadline; a second signal exits with code `130`. The watchdog is disarmed and
  joined on every clean exit path.

### FR-8 Network policy

+ **FR-8.1** DNS strategies: `prefer_ipv4` (default), `prefer_ipv6`,
  `ipv4_only`, `ipv6_only`. Resolver backends: system/libc, UDP DNS, TCP DNS,
  DNS-over-TLS, DNS-over-HTTPS, as URL strings or structured objects. A
  bounded in-memory cache is used (default capacity 1024 entries, TTL 300 s).
+ **FR-8.2** Dialer options: `connect_timeout` (default 10 s),
  `tcp_nodelay` (default true), `bind_interface`, `heartbeat_interval`
  (default 15 s), `idle_timeout` (default 30 s), `udp_idle_timeout` (default 30 s,
  also configurable on `server` and per mapping). `bind_interface` relies on
  `SO_BINDTODEVICE` and is rejected on platforms without it.
+ **FR-8.3** TLS options: `server_name`, `insecure`, `alpn`, `ca_certificate`,
  `client_certificate`, `client_key`, `certificate`, `key`, `client_ca_certificate`,
  `client_auth` (`none`, `optional`, `required`), `min_version`, `max_version`.
  Client mTLS requires certificate and key together; server mTLS requires a trusted
  client CA certificate (`client_ca_certificate`) to verify client certificates.
  ALPN is set to `h3` for HTTP/3 endpoints and `quic` for raw QUIC
  endpoints.
+ **FR-8.4** Logging uses `tracing`. `log.level` accepts `trace`, `debug`,
  `info`, `warn`, `error`; `log.output` accepts `stdout` or `stderr`;
  `log.no_color` disables ANSI colors. `RUST_LOG` overrides `log.level`.
+ **FR-8.5** `worker_threads` sets the Tokio worker count. `1` selects a
  current-thread runtime; other values select a multi-thread runtime with that
  count; omitting it auto-detects CPU cores. The CLI `-w / --worker-threads`
  value takes precedence.

### FR-9 Layer independence and relay dispatch

+ **FR-9.1** `io/` is the neutral vocabulary (`TargetAddr`, `PeerAddr`,
  `L4Protocol`, `IoChannel`, `TunnelStream`, `DatagramIo`, capability types).
  `frontend/` owns port listeners and yields `FrontendAccept { channel, peer }`;
  `backend/` owns the `Transport*` traits and registry; `transport/` supplies
  WS/WSS, H3/QUIC, and UDS drivers. `core/`, `tunnel/`, and `server/` consume
  `IoChannel` and never name a concrete frontend or transport stream type.
+ **FR-9.2** `frontend::bind` dispatches `MappingSpec.protocol` through
  `FrontendRegistry`, with `TcpFrontendFactory` and `UdpFrontendFactory`
  registered. `TransportRegistry` dispatches endpoint URL schemes to
  `WsTransportDriver`, `H3TransportDriver`, or `UdsTransportDriver`. Adding a
  mapping protocol or endpoint scheme is a registry change, not a change to the
  relay state machines.
+ **FR-9.3** `ChannelRelay::from_channels` statically dispatches all four
  `(Stream | Datagram) x (Stream | Datagram)` pairs: `(Stream, Stream)` uses
  `DuplexRelay`, `(Datagram, Stream)` and `(Stream, Datagram)` use
  `DatagramStreamRelay` with a direction flag, and `(Datagram, Datagram)` uses
  `DatagramDatagramRelay`.
+ **FR-9.4** A `TunnelStream` implementation advertises its write/half-close
  capabilities through `StreamCaps` (`owned_write`, `half_close`); `IoChannel`
  forwards the query. The relay reads them once per direction so a zero-copy
  owned `Bytes` write path cannot be left unselected. `IoChannel::into_stream()`
  and `into_datagram()` are explicit conversions for stream-only control and
  datagram consumers; a mismatched conversion returns `UnsupportedChannel`.
+ **FR-9.5** The reverse control plane lives in `control/` and the reverse
  workflows (`server/reverse.rs`, `tunnel/reverse/client.rs`,
  `tunnel/reverse/visitor.rs`); `pool/activation.rs` is only the 0-RTT codec.
  Control framing and pre-warming preserve `L4Protocol` hints
  (`RegisterReverse.protocol`, `x-tunnel-proto`, `ACTIVATE_STREAM_UDP`).
  Datagram payloads are relayed only by `ChannelRelay` variants.
+ **FR-9.6** UDP port mapping demultiplexes incoming datagrams on one bound
  `UdpSocket` per client `SocketAddr` into `UdpFlowChannel` (`IoChannel::Datagram`)
  and connects outbound targets with `ConnectedUdpSocket`. When a session drops on
  `UdpFlowChannel`, its NAT entry is atomically removed from the listener map so
  subsequent packets from the same source address open a fresh flow cleanly.

---

## 5. Non-Functional and Operational Constraints

| ID | Constraint | Implementation basis |
| :--- | :--- | :--- |
| NFR-1 | Streams and datagrams are relayed without interpreting application protocols. | `DuplexRelay` moves `Bytes` between two `TunnelStream`s; `DatagramStreamRelay` and `DatagramDatagramRelay` transfer opaque datagram payloads with `[u16 len \| payload]` framing. |
| NFR-2 | Every asynchronous task is owned and awaitable; shutdown cancels then awaits. | `JoinSet` ownership in `Coordinator`, `ClientRunner`, and `TunnelSupervisor`; `common::cancel` helpers, the generic `TaskTracker`, and explicit listener `shutdown()`; no detached task on the normal path. |
| NFR-3 | Every await is cancellation-aware or bounded by a timeout. | Handshake, activation read, session pairing, relay drain, UDP NAT expiry, and pool acquire all have explicit bounds in `consts.rs`. |
| NFR-4 | Configuration errors are surfaced before the daemon starts. | `ConfigLoader::validate` runs on every startup and on `--check`. |
| NFR-5 | Unix domain sockets are available through a single platform abstraction. | `frontend::tcp::TcpFrontend` wraps `platform::GenericListener` / `PlatformUdsListener`, with a dedicated accept thread on Windows. |
| NFR-6 | The relay hot path uses a single `poll_fn` state machine rather than a per-direction task. | `relay/` (`DuplexRelay`, `DatagramStreamRelay`, `DatagramDatagramRelay`) inside `ActiveStage`. This is a deliberate performance constraint; it must not be replaced with `tokio::select!` in the hot path. |
| NFR-7 | Documentation states implemented behavior only. | Unwired options are rejected by the loader and listed in Section 6. |
| NFR-8 | Frontend, transport, and runtime layers are independently replaceable. | `io/`, `frontend/` + `FrontendRegistry`, `backend/` + `TransportRegistry`, `transport/` drivers, and `core/` / `tunnel/` / `server/` / `relay/`; see FR-9. |
| NFR-9 | I/O capability is queried, not guessed. | `StreamCaps` / `poll_write_owned` expose owned-write and half-close capabilities; `caps()` is read once per direction. WS, QUIC, and H3 advertise owned writes. |

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
| Inbound SOCKS5 / HTTP proxy server for local traffic | Not implemented (culvert is not an inbound proxy server for local applications; client tunnels can dial through outbound HTTP/SOCKS5 proxies via `dialer.proxy`). |
| ACME certificate issuance and renewal | Not implemented. Certificates come from files, an ephemeral self-signed fallback, or an external reverse proxy (dynamic certificate file hot-reloading is supported). |
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
| AC-10 | Frontend, transport, and runtime layers are independently replaceable; `ChannelRelay::from_channels` covers all stream/datagram channel pairs and a mismatched channel conversion fails with `UnsupportedChannel`. | `crates/culvert/tests/relay.rs` (`duplex_relay_transfers_half_closes_and_recycles`, `channel_relay_datagram_stream_frames_and_half_closes`, `channel_relay_datagram_datagram_forwards_batches`), `frontend::test_registry_seeds_tcp_and_udp`, and `io::channel` tests. |
| AC-11 | Forward and reverse UDP port mapping (0-byte and multi-KB datagrams, multi-client NAT demux, 0-RTT `0xA7` pool & `NewConnection` fallback, same-port TCP+UDP coexistence, and `udp_idle_timeout` flow expiration/recreation) works end to end across WS, HTTP/3, and raw QUIC. | `crates/culvert/tests/test_udp_tunnel.rs`, `test_udp_limits_and_stress.rs`, `crates/culvert/tests/relay.rs`, and `config::loader` tests. |
| AC-12 | Client tunnels can connect to remote server endpoints through HTTP CONNECT and SOCKS5 proxies with domain delegation and optional authentication. | `test_proxy_e2e.rs`, `dialer::proxy` unit tests. |

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
forwarding (TCP/UDS and UDP, with UDP port mapping relayed through the
`Coordinator` + IO Tasks and `ChannelRelay`); per-endpoint DNS/TLS/dialer
settings; pre-warmed streams whose backend target is dialed only on activation;
path-based request matching.

**Retired (not implemented):** P2P readiness, STUN/ICE candidates, cross-endpoint
primary/standby failover, seamless migration with ring-buffer replay,
`CMD_TUNNEL_RESET` / `CMD_RESET_ACK` draining and pool reuse, HTTP/3 `POST /tunnel`
extended CONNECT framing (the implementation uses `CONNECT` for `h3`/`https` and
a MessagePack metadata handshake for `quic`), `server.toml` / `client.toml` TOML
configuration, automatic QUIC-to-WSS fallback, Caddyfile and sysctl tuning
recipes, throughput/latency/memory targets, SIMD masking claims, milestone plans,
and subjective evaluations of other tools. The v1 `core::serve` / `PumpRegistry`
/ pump stack and standalone pool or session actors were replaced by the
`Coordinator` + IO Task runtime and are not requirements.

The design details of what replaced that draft are in
[DESIGN.md](DESIGN.md); usage and the full parameter reference are in
[README.md](../README.md).
