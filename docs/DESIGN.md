# Technical Design — culvert

| Field | Value |
| :--- | :--- |
| Document | Technical Design / Architecture Specification |
| Product | culvert (workspace version 0.3.8, Rust edition 2024, MPL-2.0) |
| Status | Current — describes the implemented Coordinator + IO Task runtime |
| Companion documents | [PRD.md](PRD.md), [README.md](../README.md) |
| Normative source | `crates/culvert/src`, `crates/culvert-cli/src`, `config-example.yaml` |

This document describes the code as it exists after the v0.4 refactor. The v1
`core::serve` / `PumpRegistry` / `ReplenishingStreamPool` / `SessionManager`
stack was removed; all forwarding now runs on the Coordinator + IO Task state
machine described below. Modules present in the tree but absent from the
runtime data path are listed in [Section 13](#13-not-wired-into-the-runtime-data-path).

---

## 1. Design Summary

culvert is a TCP and UDP port-forwarding daemon. One process loads a YAML file,
optionally starts ingress listeners (`server`), optionally starts one or more
tunnel clients (`tunnels`), and supervises both. Traffic between a client and a
server travels over WS/WSS, HTTP/3/QUIC, or WS over a Unix domain socket, as an
opaque bidirectional byte stream (TCP/UDS) or `[u16 len | payload]` framed
datagrams (UDP).

The runtime is split into three layers:

| Layer | Owns | Module |
| :--- | :--- | :--- |
| **Frontend** | The mapping service: TCP/UDS (`TcpFrontend`) and UDP with per-peer NAT flow demultiplexing (`UdpFrontend`). | `frontend/` |
| **Coordinator + IO Task** | Session lifecycle, external state, resource ownership, relay scheduling, and shutdown. The runtime is a set of single-writer Coordinator Actors, each owning a set of recyclable IO Tasks. | `core/`, `tunnel/`, `server/` |
| **Backend / Transport** | A uniform I/O interface over relay channels: WS, WSS, H3, QUIC, UDS. | `backend/` (traits + registry), `transport/` (drivers), `relay/` (data-plane state machines) |

`io/` is a shared vocabulary with no business logic: `TargetAddr`, `PeerAddr`,
`L4Protocol`, `IoChannel`, `TunnelStream`, `DatagramIo`, `TunnelError`, and
capability types. `control/` owns the reverse signaling protocol;
`pool/activation.rs` owns the 0-RTT activation codec; `cert/` owns dynamic TLS
certificate management. `platform/` holds OS-level socket primitives,
`config/` holds the declarative schema, and `daemon.rs` owns process lifecycle.

### 1.1 Governing principle: internal async, boundary stateful

The v0.4 refactor is defined by one rule:

> **Internal IO units use ordinary `async`/`await`; explicit state machines exist
> only at lifecycle boundaries.**

Inside an IO Task, connecting, activation, and data transfer are asynchronous
functions and `poll_fn` loops. A single explicit state machine — the
`ExternalState` owned by the Coordinator and the paired `TaskPhase<W>` owned by
the IO Task — describes only the lifecycle phases visible to scheduling and
shutdown. The goal is not a large number of states or abstractions, but fewer
simultaneous states, branches, and implicit constraints the reader must hold.

### 1.2 What the previous architecture got wrong

The v1 design assembled each connection as a linear chain:

```text
accept -> spawn -> connect -> create channels -> forward
       -> handle errors -> cleanup -> wait for timeout
```

That shape caused long-running stability problems:

- Lifecycle was spread across many `async` functions, so a process-level
  shutdown deadline and `abort_all()` were needed as a backstop.
- `CancellationToken` was threaded through every layer, from the supervisor
  down into the relay pump.
- Each layer added its own `timeout` and `select!`, so exit paths could stack
  waits or disagree about boundaries.
- Cleanup lived in error branches and `scopeguard`s; an early abort could leave
  state and real resources out of sync.
- The pool combined untracked `tokio::spawn`, `Arc<AtomicUsize>`,
  `Arc<Mutex<PoolStats>>`, and `mpsc`/`oneshot` channels into implicit coupling.
- Understanding one connection required tracing several modules.

The replacement establishes a resource and lifecycle model first, then derives
the async implementation from it.

### 1.3 Design principles

1. **Internal async, boundary stateful.** `async/await` manages internal IO;
   explicit state machines manage only lifecycle boundaries.
2. **Single-writer control plane, independent data plane.** The Coordinator owns
   external state and resource relationships; it does not participate in each IO.
3. **Ownership before locks.** Define who owns a resource, then decide whether
   synchronization is needed.
4. **State may lag; resources may not.** External state need not mirror internal
   progress in real time, but must never permit double allocation, stale-task
   interference, or premature release.
5. **Stop is an architecture contract, not a per-layer timeout.** An IO Task is
   non-blocking at its boundary and stops within a defined window.
6. **Correctness is a consequence of the model.** Fix the model first; do not
   paper over a wrong model with more engineering mechanisms.

---

## 2. Repository Layout

```
Cargo.toml                     workspace: crates/culvert, crates/culvert-cli
config-example.yaml            canonical configuration example
crates/culvert-cli/src/main.rs CLI, logging setup, Tokio runtime, --check
crates/culvert/src/
  lib.rs                       public re-exports and run_* entry points
  consts.rs                    all constants and defaults
  io/                          neutral vocabulary: addr, channel, error, stream
  frontend/                    Frontend trait, registry, TCP/UDS and UDP frontends
  backend/                     Transport* traits and TransportRegistry
  core/                        Coordinator, StateEngine, task stages, state vocabulary
  config/                      schema and deserializers
  daemon.rs                    signal handling and hard shutdown deadline
  cert/                        certificate reader, watcher, manager, resolver, events
  control/                     SignalingChannel, SignalingRouter, control framing
  dialer/                      endpoint, DNS, TLS, proxy, unified dialer
  pool/activation.rs           0-RTT activation codec (no standalone pool actor)
  relay/                       ChannelRelay and duplex/datagram relay state machines
  transport/                   h3, uds, ws drivers
  platform/                    UDS listener, target dialing, OS primitives
  common/                      auth, cancel, net, task tracker
  server/                      ServerRunner, reverse control session, forward workflow
  tunnel/                      cluster, forward workflow/runner, reverse workflows/runner
  supervisor.rs                ClientRunner and TunnelSupervisor
  mux/                         standalone module, not used by the data path
crates/culvert/tests/          end-to-end and regression tests
```

The workspace has two crates: `culvert` (library) and `culvert-cli` (binary
named `culvert`). The binary uses the library through `culvert::run_daemon`.

---

## 3. Startup and Process Lifecycle

1. `Cli::parse()` reads `--config / -c`, `--worker-threads / -w`, and
   `--check`.
2. `ConfigLoader::from_file` parses YAML into `AppConfig` and runs
   `validate()`. Errors are printed to stderr and exit with code `1`.
3. `--check` prints a success line and returns without initializing logging,
   binding sockets, or starting the runtime. It is a static check only.
4. Logging is initialized with `tracing_subscriber`; `EnvFilter` from `RUST_LOG`
   takes precedence over `log.level`.
5. The Tokio runtime is built from `-w` or `worker_threads`: `1` selects
   `new_current_thread`, an explicit `n` selects `new_multi_thread` with
   `worker_threads(n)`, and absence selects `new_multi_thread` with
   auto-detected cores.
6. `run_daemon(app_config)` resets the shared shutdown deadline, creates the
   root `CancellationToken`, spawns the shutdown handler, and calls
   `TunnelSupervisor::run()`.

### 3.1 Shutdown

- `spawn_shutdown_handler` owns a signal task and a dedicated OS-thread
  watchdog (`shutdown-watchdog`). The thread exists so a hard deadline can fire
  even if every Tokio worker is busy.
- First termination signal: cancel the root token and start a 10-second deadline
  (`DEFAULT_SHUTDOWN_HARD_DEADLINE`). Second signal: exit with code `130`.
- `ShutdownWatchdog::finish()` disarms the deadline and joins both owned
  handles; it is called on every daemon exit path, including clean exits.
- `TaskTracker::shutdown_and_wait()` cancels child tokens and awaits its
  `JoinSet`. Nested trackers share one absolute 5-second grace deadline
  (`DEFAULT_GRACE_PERIOD`) rather than compounding windows. `abort_all()` is
  only the deadline fallback, and even then every handle is awaited.
- Design constraint: no task is detached on the normal path. Every tracked task
  observes a cancellation token and every await is cancellation-aware or has
  an explicit timeout. Listeners expose an async `shutdown()`; `Drop` is only an
  abnormal-path safety net.

---

## 4. Configuration Model

### 4.1 Schema

`AppConfig` is `#[serde(deny_unknown_fields)]`; all nested configuration
structs are too. `EndpointConfig` and `DnsResolverConfig` implement custom
`deserialize_any` visitors to accept either a string or a structured map.

| Section | Type | Purpose |
| :--- | :--- | :--- |
| `log` | `LogConfig` (defaulted) | `level`, `output`, `no_color`. |
| `worker_threads` | `Option<usize>` | Tokio worker count; `0` rejected. |
| `dns` | `Option<DnsConfig>` | `strategy`, `resolvers`, `cache_capacity`. |
| `dialer` | `Option<DialerConfig>` | `connect_timeout`, `tcp_nodelay`, `bind_interface`, `proxy`, `heartbeat_interval`, `idle_timeout`, `udp_idle_timeout`. |
| `tls` | `Option<TlsConfig>` | `server_name`, `insecure`, `alpn`, CA/client/leaf certificates, `client_auth`, version bounds. |
| `server` | `Option<ServerConfig>` | `listen`, `token`, `default_target`, TLS settings, `heartbeat_interval`, `idle_timeout`, `udp_idle_timeout`. |
| `tunnels` | `Vec<TunnelConfig>` (defaulted) | `tag`, `endpoint`, `token`, `uuid`, `http_headers`, `failover`, `multiplex`, `retry`, `mappings`. |

`TunnelConfig` requires a `mappings` object (`TunnelMappingsConfig`) containing
`forward` and/or `reverse` groups (`MappingGroupConfig`), each with an optional
`pool_size` and `MappingRuleConfig { protocol, listen, target, udp_idle_timeout }`.
`protocol` is mandatory (`"TCP"` or `"UDP"`, case-insensitive). Durations accept
a raw integer (seconds) or a string with `s` / `ms` suffix.

### 4.2 URL and address parsing

`ServerListenSpec::parse` requires an explicit `://` and lowercase scheme.
For TCP schemes it splits `host:port` from the HTTP path and requires a non-zero
`u16` port. For UDS schemes it normalizes the socket path and parses `?path=`
and `?chmod=` (octal, `<= 0o7777`).

`dialer::endpoint::Endpoint::parse` uses the `url` crate for `ws`, `wss`,
`https`, `h3`, `quic`, and a dedicated parser for `ws+unix` / `wss+unix`.
Default ports are 80 for `ws` and 443 for TLS/QUIC schemes.

`io::addr::ListenAddr` has `DualStack(u16)`, `Ip(SocketAddr)`,
`Domain(String, u16)`, and `Path(PathBuf)` variants. `parse_listen_addr` accepts
`:port` (dual-stack binding), `addr:port`, `hostname:port`, or `unix://path`.
Dual-stack sockets normalize IPv4-mapped IPv6 peers to canonical IPv4.

`io::addr::TargetAddr` has `Ip`, `Domain`, and `Path` variants and represents
outbound `connect` destinations only. `L4Protocol` is `Tcp | Udp`, serialized
lowercase. `MappingSpec { protocol, listen, target }` is the neutral mapping
description passed to `frontend::bind`. `PeerAddr` is `Socket | Path | P2p`.

### 4.3 EndpointContext merge

`EndpointContext::build_for_tunnel(endpoint_cfg, app_config)` produces the
effective runtime context for one endpoint: endpoint, DNS resolver, dialer,
effective dialer config, TLS config, and HTTP headers. Merge order for `dns`,
`dialer`, and `tls` is **endpoint override → global → built-in default**. TLS is
activated by the scheme (`https`, `wss`, `h3`, `quic`, `wss+unix`); a `tls` block
on any other scheme is an error. `http_headers` merge case-insensitively with
tunnel-level values overriding endpoint-level ones. Helpers return the
effective heartbeat, idle, connect, and UDP idle timeouts.

### 4.4 Validation rules

`ConfigLoader::validate` runs at every startup and under `--check`:

- At least one server listener or one tunnel must exist.
- `worker_threads >= 1`; log level and output are in their allowed sets.
- Tokens are non-empty and at most 256 bytes (`TOKEN_MAX_LEN`).
- `pool_size >= 1`, retry fields and DNS cache capacity are positive.
- HTTP header names and values are validated against RFC 9110 token syntax and
  CRLF/control-character injection.
- Server `default_target`, certificate/key pairs, PEM validity, key/cert
  matching, and TLS version bounds are checked.
- Mapping `listen` / `target` values parse; `unix://` is rejected for UDP.
- Forward `(protocol, listen)` pairs and tunnel tags are unique.
- Endpoint schemes are supported; DNS resolvers parse; `EndpointContext` builds.
- Effective `idle_timeout > heartbeat_interval` after defaults are merged;
  configured `udp_idle_timeout` must be positive.
- `dialer.proxy` must parse as `http`, `https`, `socks5`, or `socks5h`; proxies
  are rejected on QUIC/HTTP/3 endpoints.
- `multiplex` and `failover` sections are currently rejected as not wired into
  the data path (see [Section 13](#13-not-wired-into-the-runtime-data-path)).

---

## 5. Transport Layer

### 5.1 Backend traits

`backend/mod.rs` owns the relay-channel interface; `transport/` contains only
driver implementations and re-exports these traits for existing import paths.

```rust
pub struct StreamMetadata {
    pub mode: TunnelMode,
    pub session_id: Option<String>,
    pub target: Option<TargetAddr>,
    #[serde(default)] pub pool_idle: bool,
    #[serde(default)] pub protocol: L4Protocol,
}
pub type AcceptOutput = Result<(IoChannel, StreamMetadata), TunnelError>;

pub trait TransportConnector: Send + Sync {
    fn connect<'a>(&'a self, target: &'a TargetAddr,
                   session_id: Option<&'a str>)
        -> BoxFuture<'a, Result<IoChannel, TunnelError>>;
    fn connect_pooled<'a>(&'a self, target: &'a TargetAddr)
        -> BoxFuture<'a, Result<IoChannel, TunnelError>>;
    fn connect_with_protocol<'a>(&'a self, target: &'a TargetAddr,
                                 session_id: Option<&'a str>,
                                 protocol: L4Protocol)
        -> BoxFuture<'a, Result<IoChannel, TunnelError>>;
    fn connect_pooled_with_protocol<'a>(&'a self, target: &'a TargetAddr,
                                        protocol: L4Protocol)
        -> BoxFuture<'a, Result<IoChannel, TunnelError>>;
}

pub trait TransportListener: Send + Sync {
    fn accept(&mut self, slot: &mut ReusableFutureRef<'static, AcceptOutput>);
    fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, ()>;
}

pub trait TransportDriver: Send + Sync + 'static {
    fn schemes(&self) -> &[&'static str];
    fn build_connector(&self, endpoint: &EndpointContext, auth_token: &str,
                       mode: TunnelMode) -> Result<Box<dyn TransportConnector>, TunnelError>;
    fn build_listener(&self, listen_addr: &str, auth_token: Option<&str>,
                      path: Option<&str>, tls: Option<&TlsConfig>,
                      cancel: CancellationToken)
                      -> Result<Box<dyn TransportListener>, TunnelError>;
}
```

`BoxFuture` is a boxed, `Send`, lifetime-bound future; the traits avoid
`async_trait`. `connect_pooled` is mandatory: the old default silently omitted
the `x-tunnel-pool: idle` marker. `TransportListener::shutdown()` is the
explicit async termination boundary for listener-owned tasks. `IoChannel` is
the single I/O product on both sides; `IoChannel::into_stream()` is the explicit
by-value conversion for stream-only control paths and returns
`UnsupportedChannel` on a datagram channel.

### 5.2 TunnelStream and stream capabilities

`io::stream::TunnelStream` extends `AsyncRead + AsyncWrite + Send + Unpin +
'static` with liveness hooks and one capability query:

```rust
pub struct StreamCaps { pub owned_write: bool, pub half_close: bool }
// Default: owned_write = false, half_close = true.

pub trait TunnelStream: AsyncRead + AsyncWrite + Send + Unpin + 'static {
    fn caps(&self) -> StreamCaps;
    fn poll_write_owned(&mut self, cx: &mut Context<'_>, chunk: Bytes)
        -> Poll<io::Result<usize>>;
    fn poll_send_ping(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
    fn last_activity_ms(&self) -> Option<u64>;
    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus>;
}
```

The relay reads `caps()` once per direction and selects the owned or borrowed
write path. QUIC and H3 retain `Bytes` and advertise `owned_write = true`; byte
streams use the borrowed default.

QUIC tuning lives in the top-level `quic` config block: `mtu_upper_bound`
(default 8192), `immediate_ack` (default false), and `congestion`
(`bbr` / `cubic` / `new_reno`, default `bbr`). Transports enable round-robin
stream send fairness and an expanded 64 KiB crypto buffer. Dialing a QUIC
endpoint races all addresses returned by the configured DNS strategy, preserving
the strategy's family filtering and preference order.

Every QUIC connection carries one `QuicSessionHandle` shared by the connector
cache, accepted relay streams, and listener handlers. The handle owns the UDP
datagram-progress check, the `dead` flag, and the single `conn.close()` path.
The TX-without-RX detector uses a sticky latch: the first TX-only sample starts
`stalled_since` and anchors `stall_start_tx`; any later TX growth plus ten
seconds without RX marks the session dead. RX growth clears the latch. No
component outside the handle closes the connection.

`heartbeat_timing(last_activity_ms, now, heartbeat_interval, idle_timeout)`
computes whether a probe is needed. Probes never refresh liveness by themselves;
inbound frames and real outbound data do. `poll_check_alive` is non-destructive:
a zero-capacity `ReadBuf` drives the normal `poll_read` state machine and never
polls an in-flight write future. WebSocket encodes the logical half-close FIN as
an in-band zero-length binary frame; a local FIN or queued Close reply is not
liveness-dead.

### 5.3 Driver registry

`backend::registry::TransportRegistry::default()` registers three drivers:

| Driver | Schemes | Implementation |
| :--- | :--- | :--- |
| `WsTransportDriver` | `ws`, `wss` | hyper HTTP/1.1 + fastwebsockets Upgrade; `WsTunnelStream`. Optional rustls TLS. |
| `H3TransportDriver` | `h3`, `https`, `quic` | quinn + h3; `H3TunnelStream` for CONNECT streams, `QuinnTunnelStream` for raw bidirectional streams. |
| `UdsTransportDriver` | `ws+unix`, `wss+unix` | HTTP/1.1 Upgrade over a Unix domain socket; optional TLS; `chmod` handling. |

`create_connector` dispatches by endpoint scheme; `create_listener` dispatches by
the parsed `ServerListenSpec.scheme` and passes the root cancellation token.

### 5.4 Handshake metadata

| Header | Meaning |
| :--- | :--- |
| `x-tunnel-auth` | Pre-shared token on WebSocket/UDS; also sent by H3 clients. |
| `authorization: Bearer …` | Pre-shared token on HTTP/3 `CONNECT` requests. |
| `x-tunnel-mode` | `forward`, `reverse-ctrl`, `reverse-data`. |
| `x-session-id` | Reverse data-stream pairing key. |
| `x-target-host`, `x-target-port`, `x-target-path` | Split target form (WebSocket/UDS). |
| `x-tunnel-target` | Unified `TargetAddr` string (HTTP/3). |
| `x-tunnel-pool: idle` | Marks a pre-warmed stream awaiting activation. |
| `x-tunnel-proto` | `tcp` (default) or `udp`; sent on WS, UDS, and H3 handshakes. |

On raw QUIC the same `StreamMetadata` is serialized with MessagePack and sent
in a length-prefixed handshake:

```
4 bytes auth_len (big-endian) | auth_token bytes
4 bytes meta_len (big-endian) | MessagePack(StreamMetadata)
```

The server caps `auth_len` at 1024 bytes and `meta_len` at 65536 bytes.

### 5.5 Ingress acceptance

- WebSocket/UDS listeners check path (`404`), parse headers (`401`), compare
  the token in constant time (`403`), then perform the HTTP Upgrade.
- H3 listeners check path (`404`), read the token from `authorization` with
  `x-tunnel-auth` fallback (`401`), parse metadata, and respond `200 OK`.
- Raw QUIC listeners compare the binary token in constant time.
- `common::auth::constant_time_token_eq` hashes both values into fixed 256-byte
  buffers and compares with `subtle::ConstantTimeEq`.

### 5.6 TLS, DNS, and dialing

- `dialer::tls::TlsConfigBuilder` builds rustls configs with CA loading, mTLS,
  `WebPkiClientVerifier`, version bounds, ALPN, optional no-verification, and
  ephemeral self-signed server certificates (`rcgen`).
- ALPN defaults: `h3` for HTTP/3, `quic` for raw QUIC.
- `dialer::dns::CachedDnsResolver` wraps `hickory-resolver` with a bounded cache
  (1024 entries, 300 s TTL) and system/libc fallback. Resolver types are
  `system`, `udp`/`dns`, `tcp`, `tls` (DoT), and `https` (DoH).
- `dialer::unified::DefaultDialer` races resolved addresses with RFC 8305
  staggering (250 ms), applies `connect_timeout`, `tcp_nodelay`, and
  `bind_interface`.
- `dialer::proxy::ProxyDialer` wraps the inner dialer for `http`, `https`,
  `socks5`, and `socks5h` proxies. It resolves only the proxy host locally;
  the destination hostname is passed to the proxy, preventing DNS leaks.

### 5.7 Certificate hot reload

- `cert::reader` centralizes PEM parsing and validates key/certificate pairing.
- `cert::watcher` captures filesystem events via `notify` and debounces them in
  an async loop so multi-file updates are treated as one event.
- `cert::manager` stages and validates updates, then atomically commits new
  `Arc<CertifiedKey>` / `Arc<RootCertStore>` values. Invalid files never disrupt
  live traffic.
- `cert::resolver` implements `DynamicCertResolver` and
  `DynamicClientCertVerifier`; handshakes read the current in-memory value with
  no disk I/O and no listener restart.
- `cert::events` broadcasts `ReloadSuccess` / `ReloadFailed` to subscribers.

### 5.8 Frontend and platform primitives

- `frontend::Frontend` owns its listener and exports
  `accept(&mut ReusableFutureRef<'static, FrontendOutput>)` and async
  `shutdown()`. The reusable slot lets an in-flight accept survive a competing
  `select!` branch. `FrontendRegistry` maps `L4Protocol` to a factory.
- `frontend::udp::UdpFrontend` binds one UDP socket, demultiplexes flows by
  `SocketAddr`, and gives each peer a bounded `UdpFlowChannel`
  (`UDP_SESSION_CHANNEL_SIZE = 256`, drop-tail under burst).
- `platform::ConnectedUdpSocket` connects an ephemeral socket to the target
  address and implements zero-header batched `DatagramIo`.
- `platform::GenericListener` covers TCP and UDS; the Windows UDS listener uses
  an owned and joined OS accept thread.

---

## 6. Architecture: Coordinator, IO Task, and State

### 6.1 Two-level state model

**Internal state** belongs to the IO Task: read/write futures, pending buffers
and flush progress, TCP EOF and half-close handling, transport activity clocks
and keepalive probes, local errors, and waker state. Rust `async`/`await` and
`Future` manage this state; the design does not build a second explicit state
machine for it.

**External state** is owned by the Coordinator and describes the lifecycle
phase and resource ownership visible to scheduling and shutdown:

| State | Meaning |
| :--- | :--- |
| `Empty` | The task owns no I/O resource. It waits for `Assign`, `SupplyIdle`, or `Shutdown`. |
| `Idle` | The task owns a live `PooledTransport` and drives local keepalive and age-based eviction. |
| `Connecting` | The task owns a `JobId` and input resource; it acquires or activates a backend stream. |
| `Active` | Both ends are paired; the task exclusively runs the hot-path relay. |
| `Closing` | The task drains buffered work and releases resources before recycling or exit. |
| `Closed` | The task event loop has ended; the `JoinHandle` is being reaped. |

Half-close is an **internal** data-plane state, not an external task state.

The two levels may briefly disagree — for example, internal state may have
reached EOF while external state is still `Active`. That lag is acceptable only
if invalidated resources are never reallocated, old tasks never touch new-task
resources, resources are never released early or lost, and state updates never
break ownership.

### 6.2 Coordinator Actor

The Coordinator is the only writer of external state. It:

- owns the task collection and every `JoinSet` / `JoinHandle`; no task is detached;
- maintains each task's external state;
- handles lifecycle events (`Active`, completion, failure, cancellation);
- drives background forward dialing through an embedded `DialDriver`;
- coordinates tunnel-level `GracefulStop` and `ForceStop`.

It does **not** perform TCP/QUIC IO, manage buffers, schedule each read/write,
or implement protocol handling. Microtask handling is strictly synchronous:
every `Microtask` mutates `StateEngine` and replies through non-blocking
`oneshot` / `mpsc` operations, never `.await`. This is what lets the accept loop
be bounded without wrapping every Coordinator call in a defensive timeout.

The event loop uses two channels and one biased `select!`:

- `micro_rx` (unbounded): high-priority, synchronous state synchronization;
  drained with `while let Ok` at the top of every iteration.
- `event_rx` (bounded, capacity 1024): external `assign`, `prewarm`,
  `supply_transport`, queries, and shutdown requests.
- cancel, dial completion, task completion, and drain deadline branches.

The Coordinator must not become a new bottleneck: resource acquisition,
activation handshakes, keepalive probes, and relay branches run inside IO Tasks,
not serially in the Coordinator loop.

### 6.3 IO Task and resource ownership

`TaskPhase<W>` binds the external phase to its exact resource payload, so an
illegal combination such as `(Idle, no transport)` is unrepresentable.
`IoTask::run_loop` is a single event loop that dispatches directly on the
current phase. Every `select!` inside it is `biased` and gives the directive
channel priority over IO progress.

Ownership rules:

- Frontend listener: owned by the accept loop.
- Frontend stream: transferred to exactly one IO Task at `accept`.
- Idle backend stream: owned by the `Idle` IO Task until handed to `Connecting`.
- Underlying reusable connection: owned by the connector/listener session
  cache; stream and connection lifetimes are independent.
- External state: owned by the Coordinator. Internal state: owned by the task.

A transition commits only when the resource is in place. `Connecting -> Active`
happens only after the backend stream is acquired and activation has succeeded.

### 6.4 Identity and late-event protection

| Identity | Scope |
| :--- | :--- |
| `TaskId(u64)` | A long-lived, recyclable worker slot, from spawn until `Closed`. |
| `JobId(u64)` | One assigned connection cycle on that worker; cleared on recycle. |
| `IdleId(u64)` | One idle-transport residency on the worker. |

`Microtask::StateChanged` and `Microtask::RecycledToEmpty` carry `job_id`;
`StateEngine` drops any event whose job does not match `entry.current_job`.
Recycle (`* -> Empty`) and exit (`* -> Closed`) are distinct: recycling wakes
only waiters for `Empty` and resolves waiters for older phases as
`TargetClosed(Empty)`, preventing cross-generation hangs; waiters for `Closed`
wake only on real exit. `PooledTransport` carries `session_id` for QUIC session
sweep and `generation` for reverse reconnect filtering.

### 6.5 Transport lifecycle and the idle pool

Production traffic uses QUIC/HTTP/3, where a connection is long-lived and
carries many independently created streams. Stream lifetime is not connection
lifetime. This architecture therefore has no standalone `TransportPool` actor:

- Idle pre-warmed or inbound streams live in `TaskPhase::Idle` and run their own
  keepalive, activity clock, age limit, and eviction.
- Forward background dialing is driven by the embedded `DialDriver` inside the
  Coordinator's `select!`.
- On `Assign`, an `Idle` task hands its stream directly to `Connecting`; if the
  stream died locally in the same instant, it enters `Connecting` with
  `transport: None` and the Coordinator supplies a replacement.
- A stream whose activation fails is dropped, never returned to the idle pool.
- Wrong-state streams are returned through `Microtask::ReturnTransport` without
  blocking either side.
- QUIC session death and reverse reconnect generation changes sweep matching
  idle tasks through asynchronous `CloseTransport` directives.

Transport streams are **one-shot** in this release: after a relay ends the
stream is dropped rather than returned to `Idle`. A full application-layer
half-close FIN handshake for transport recycling and task-level multiplexing are
planned after the architecture stabilizes; the state-machine architecture no longer blocks
either feature.

### 6.6 State transition tables

Every explicit state transition is defined by `State`, `Event`, `Guard`,
`Action`, `Next State`, and `Failure`. Illegal transitions have an explicit
result — rejection or ignore — rather than relying on execution order.

`TunnelPhase` transitions:

| From | Event | Guard / action | To |
| :--- | :--- | :--- | :--- |
| `Running` | `GracefulStop` | Stop accept, clear `DialDriver`, `Shutdown` on `Empty`/`Idle`, `Drain` on `Connecting`/`Active` | `Draining` |
| `Running` / `Draining` | `ForceStop` or expiry of `drain_timeout` | Cancel root token | `ForceStopping` |
| `Draining` / `ForceStopping` | `JoinSet` empty | Clear queues | `Stopped` |

`ExternalState` transitions: `Empty -> Idle` (`SupplyIdle`), `Empty/Idle ->
Connecting` (`Assign`), `Connecting -> Active` (activation OK), `Active ->
Closing` (EOF, error, or drain), `Closing -> Empty` (recycle while running) or
`Closed` (shutdown), `Idle -> Empty` (eviction or `CloseTransport`), and any
phase to `Closed` on shutdown.

### 6.7 Shutdown and cancellation contract

Every IO Task is non-blocking at its boundary. Shutdown has distinct outcomes,
not one generic `Cancelled`:

1. **Normal end**: both directions reach EOF and write half-close.
2. **IO error**: read/write failure or reset; release both sides with a reason.
3. **Idle timeout**: no data activity within `idle_timeout` and no probe refresh.
4. **Graceful stop**: stop accepting new work; `Connecting` tasks are cancelled;
   `Active` tasks stop reading new data and flush buffered bytes within a
   bounded `drain_timeout`, completing half-close so a QUIC `SendStream` is not
   dropped without `poll_shutdown`.
5. **Force stop**: exit immediately, drop channels, and await no internal
   timeout or unfinished flush.

Cancellation-safety checklist:

- `Frontend::accept` / `TransportListener::accept` use `ReusableFutureRef` so an
  in-flight accept or TLS handshake survives a competing `select!` branch.
- Partially read bytes are either written inside the drain window or discarded
  with the task; they never leak across tasks.
- Stream wrappers must terminate background pump tasks promptly on drop rather
  than blocking the outer join.

Testable shutdown contract: a permanently `Pending` inner future still stops
within milliseconds; stop does not wait for long internal timeouts to expire;
no detached task or pool task remains; all handles are released deterministically;
graceful stop during partial I/O or half-close preserves resource state; repeated
stop is idempotent; connections arriving during shutdown are rejected before
`Active`.

---

## 7. Runtime Data Paths

### 7.1 Startup topology

`TunnelSupervisor::run` starts one `ServerRunner` (if configured) and one
`ClientRunner::run_tunnel_with_retry` worker per tunnel, all in a `JoinSet`.
`ClientRunner::supervise_workers` cancels siblings on permanent failure and
awaits every handle. `TunnelCluster::run` builds the forward and reverse runners
for one tunnel with a shared transport registry.

### 7.2 Forward mapping

`ForwardRunner` binds a frontend through `frontend::bind`, prewarms `Empty`
tasks if configured, then accepts into a `ReusableFutureRef` slot with bounded
backoff. Each accepted `ForwardInput` is assigned to a `Coordinator`, which
pops the freshest `Idle` task or an `Empty` slot, or spawns a new `Connecting`
task. `ForwardWorkflow::connect` uses the carried idle stream or requests one
from the Coordinator, activates it with `0xA5` / `0xA7` and waits for `0xAC`,
falling back to a direct dial on failure. The resulting `ChannelRelay` runs as
the `ActiveStage`.

### 7.3 Server ingress

`ServerRunner::run` creates one `Coordinator<ServerForwardWorkflow>` for all
forward visitors and starts one accept loop per listener. Each accepted
`(IoChannel, StreamMetadata)` is dispatched by mode:

| `metadata.mode` | Branch |
| :--- | :--- |
| `Forward` | Assign to the forward Coordinator. The workflow resolves target from activation payload, metadata, or `default_target`, dials it, and relays. |
| `Reverse` with `session_id` | Fulfill a pending reverse session in `ReverseSessionHub`. |
| `Reverse` with `pool_idle` | Supply the stream to the matching reverse Coordinator as an `Idle` task. |
| `Reverse` (control) | Run `ReverseControlSession`. |

Accept-loop branches are `biased`: cancellation first, then completed reverse
tasks, then accept. A fatal accept error stops the listener; transient errors
are backed off.

### 7.4 Reverse control channel

`ReverseControlSession` wraps the control stream in a `SignalingChannel` actor
that owns heartbeat `Ping`/`Pong`, bounded frame timeouts (`reverse_frame_timeout`),
and event delivery. The visitor loop consumes `RegisterReverse`, `RegisterAck`,
`NewConnection`, and disconnect events from `SignalingReceiver`.

Control-channel sends are bounded inside `SignalingChannel`; the router also
supports primary/standby failover and a `SendCommitGate` so pre-commit
cancellation cannot put a frame on the wire after the caller has abandoned it.
A `CommandDeduplicator` (capacity 256) drops repeated `NewConnection` session
IDs before any backend work.

`ReverseSessionHub` owns:

- pending oneshot sessions keyed by session ID, with a guarded drop path;
- reverse Coordinators keyed by `(protocol, listen_addr, tunnel_id)`, with a
  generation counter;
- grace slots for brief control-channel disconnects.

`RegisterReverse` binds the visitor listener and registers the Coordinator
before sending `RegisterAck`. The client clamps `pool_size` once and sends the
effective value; the server derives its soft cap as `2 x pool_size`. Control
events are prioritized above visitor accept in the biased select.

### 7.5 Reverse activation and emergency promotion

- **0-RTT pool path**: an idle reverse stream carries `0xA5` (TCP) or `0xA7`
  (UDP) plus target; the client echoes `0xAC`, then dials the local target and
  relays. One failed activation affects only that stream, never its QUIC
  siblings.
- **On-demand path**: the server allocates a session ID and sends
  `NewConnection`. A single timed-out `NewConnection` cleans up only that
  session; it never tears down the listener.
- **Emergency promotion (`0xA6`)**: if the control channel drops, the server
  promotes a pooled stream into a replacement signaling channel; the client
  attaches it to the `SignalingRouter` without a full reconnect.
- **Grace period**: if no pooled stream is available, the listener stays bound
  for `reverse_grace_period(idle_timeout)` while a reconnecting control stream
  reattaches, avoiding `EADDRINUSE`.

### 7.6 Activation packet

```
magic (1 byte) | length: u16 big-endian | UTF-8 target string (Forward mode only)
```

| Byte | Meaning |
| :--- | :--- |
| `0xA5` (`ACTIVATE_STREAM_MAGIC`) | TCP forward/reverse stream activation. |
| `0xA6` (`ACTIVATE_STREAM_PROMOTE`) | Emergency promotion to a replacement control channel. |
| `0xA7` (`ACTIVATE_STREAM_UDP`) | UDP forward/reverse stream activation. |
| `0xAC` (`ACTIVATE_STREAM_ACK`) | Round-trip activation acknowledgment. |

All magic, length, and payload handling is bounded. The ACK closes the
half-open window where a write succeeds only into the local send buffer.

### 7.7 Retry supervision

```
loop:
  run tunnel cluster
  Ok / Shutdown                  -> exit
  error while cancelled          -> exit
  permanent error                -> return error, no retry
  attempts >= max_retries        -> return error
  sleep_or_cancel(retry.delay)   -> next attempt
```

`retry.delay` is a fixed configured delay (default 3 s), not exponential
backoff. `TunnelError::is_permanent()` covers `Config`, `UnsupportedScheme`,
`UnsupportedChannel`, `AuthFailed`, and `Tls`.

---

## 8. Relay Implementation

`ChannelRelay` statically dispatches across `(Stream | Datagram) x
(Stream | Datagram)` without a boxed relay:

| Variant | Implementation |
| :--- | :--- |
| `StreamStream` | `DuplexRelay` with two `TransferDirection` state machines. |
| `DatagramStream` | `DatagramStreamRelay` framing datagrams over a byte stream. |
| `DatagramDatagram` | `DatagramDatagramRelay` batch relay. |

`TransferDirection` owns a 64 KiB buffer, an optional in-flight `Bytes` chunk,
`read_done`, `write_shutdown`, and byte counters. `poll_step` advances writes,
then reads, then flush and write half-close. A direction finishes only after
EOF, all buffered bytes written, and write shutdown complete. `caps().owned_write`
selects zero-copy owned writes; `caps().half_close` selects `poll_shutdown`.

Over byte streams, datagrams use `[u16 len | payload]` framing, preserving
message boundaries including zero-length datagrams. `DatagramQueue` batches up
to 16 datagrams with 64 pending entries. Incomplete frames at EOF are an error.

The hot paths are hand-written `poll_fn` state machines inside `ActiveStage`;
replacing them with `tokio::select!` is not permitted because of measured
performance impact. Each `Active` task owns its stages directly and preserves
ownership across cancellation and drain. `ClosingStage::run` is bounded by
`drain_timeout` and propagates genuine I/O errors instead of masking them as a
clean drain.

---

## 9. Error Model

`io::error::TunnelError` (re-exported as `core::error::TunnelError`) includes
`Io`, `UrlParse`, `Config`, `UnsupportedScheme`, `UnsupportedChannel`,
`DnsResolutionFailed`, `DnsNoAddresses`, `Tls`, `TlsHandshake`,
`ConnectTimeout`, `AuthFailed`, `SessionNotFound`, `SessionClosed`,
`SessionPairTimeout`, `ControlProtocol`, `Multiplex`, `PoolExhausted`,
`Shutdown`, and `Internal`. `UnsupportedChannel` is permanent.

`core::error::TaskError` models task-stage failures, including `CoordinatorGone`
and `StageFailed`. `is_permanent()` and `is_shutdown()` drive supervisor retry
and shutdown decisions. `common::cancel` provides `cancel_with`,
`sleep_or_cancel`, and `TimeoutOrCancel` for explicit cancellation boundaries.

---

## 10. Constants and Defaults

Selected values from `consts.rs`; all runtime timers come from these or from
effective configuration values.

| Constant | Value |
| :--- | :--- |
| `RELAY_BUFFER_SIZE` | 65536 |
| `MAX_UDP_DATAGRAM_SIZE` | 65527 bytes (IPv6 payload ceiling) |
| `UDP_SESSION_CHANNEL_SIZE` | 256 per peer NAT flow |
| `TOKEN_MAX_LEN` | 256 bytes |
| `DEFAULT_CONNECT_TIMEOUT` | 10 s |
| `DEFAULT_HANDSHAKE_TIMEOUT` | 15 s |
| `DEFAULT_HEARTBEAT_INTERVAL` | 15 s |
| `DEFAULT_IDLE_TIMEOUT` | 30 s |
| `DEFAULT_UDP_IDLE_TIMEOUT` | 30 s |
| `DEFAULT_RETRY_DELAY` | 3 s |
| `DEFAULT_SESSION_PAIR_TIMEOUT` | 15 s |
| `DEFAULT_RELAY_DRAIN_TIMEOUT` | 500 ms |
| `DEFAULT_GRACE_PERIOD` | 5 s (shared absolute deadline) |
| `DEFAULT_SHUTDOWN_HARD_DEADLINE` | 10 s |
| `POOL_ACQUIRE_TIMEOUT` | 15 s |
| `POOL_MAX_CAPACITY` / `POOL_MAX_CONCURRENT_CONNECTS` | 64 / 16 |
| `POOL_HEARTBEAT_TICK_INTERVAL` / `POOL_HEARTBEAT_INTERVAL` | 1 s / 5 s |
| `POOL_HEARTBEAT_TIMEOUT` | 5 s |
| `POOL_BACKOFF_BASE` / `POOL_BACKOFF_MAX` / `POOL_BACKOFF_ACTIVE_WAITERS_MAX` | 200 ms / 10 s / 1 s |
| `DEFAULT_REVERSE_BACKOFF_INITIAL` / `MAX` / reset | 500 ms / 30 s / 30 s |
| `DEFAULT_REVERSE_GRACE_PERIOD` / `DEFAULT_REVERSE_PROMOTE_WAIT` | 10 s / 1 s |
| `MAX_ACTIVATION_TARGET_LEN` | 512 bytes |
| `ACTIVATE_STREAM_MAGIC` / `PROMOTE` / `UDP` / `ACK` | `0xA5` / `0xA6` / `0xA7` / `0xAC` |
| `HEADER_TUNNEL_PROTO` | `x-tunnel-proto` (`tcp` / `udp`) |
| `ALPN_H3` / `ALPN_QUIC` | `h3` / `quic` |
| QUIC windows | stream 4 MiB, connection 128 MiB, send 128 MiB |
| QUIC keep-alive / max idle | 5 s / 30 s |
| DNS cache capacity / TTL | 1024 entries / 300 s |

Derived timing functions keep both ends consistent and avoid fixed LAN-scale
constants: `reverse_frame_timeout(idle_timeout)` clamps `idle_timeout / 3`
between 5 s and 15 s and never exceeds the idle window;
`reverse_grace_period(idle_timeout)` never exceeds the idle window;
`reverse_pool_soft_cap(pool_size)` is `2 x pool_size`.

---

## 11. Testing Strategy

Four classes of tests are used:

1. **Pure state tests** (no Tokio or network): every legal transition, illegal
   transitions, duplicate events, out-of-order and late events. `StateEngine`
   invariants, cycle detection, recycle-versus-exit waiter semantics, and
   generation filtering are tested directly.
2. **Resource ownership tests**: allocation success and failure, activation
   fallback, cancellation during `Connecting` releasing only the frontend,
   transport return on wrong-state delivery, and no double release.
3. **Async behavior tests** (in-memory `duplex` streams and mock streams):
   permanently `Pending` futures under force stop, EOF, half-close, partial
   reads/writes, flush and I/O errors, graceful stop versus normal completion.
4. **Integration and fault tests**: real WS/H3/QUIC tunnels, concurrent stream
   bursts, dead-session sweeps, pool capacity and waiter fairness, reverse
   promotion and reattachment, shutdown reclaiming all shared resources.

After the directory cleanup, `cargo test --workspace --lib` runs 185 library
tests, and the Coordinator/IO Task suites live as integration tests under
`crates/culvert/tests/`. `config-example.yaml` is asserted to parse cleanly,
and `culvert --check -c config-example.yaml` exercises the same validation path
as startup without binding an address.

---

## 12. Non-Goals

The v0.4 architecture deliberately does not:

- rewrite every `async` function as a hand-written `Future` or state machine;
- split internal IO artificially once its boundary is clear;
- mirror all internal state into the Coordinator;
- introduce a larger message system only to avoid necessary synchronization;
- build a general-purpose state-machine framework;
- change `Frontend`, `Transport`, or backend responsibilities or wire semantics;
- add features unrelated to lifecycle correctness.

---

## 13. Not Wired Into the Runtime Data Path

| Item | Location | Status |
| :--- | :--- | :--- |
| `MultiplexConfig` | `config/model.rs` | Rejected by `ConfigLoader::validate`. |
| `FailoverConfig` | `config/model.rs` | Rejected by `ConfigLoader::validate`. |
| `mux/` (frame, session, stream) | `crates/culvert/src/mux/` | Standalone module with its own tests; no data-path references. |
| `core/failover.rs` | `crates/culvert/src/core/` | Not referenced by config or runtime. |
| Transport-level application FIN handshake for stream recycling | — | Production uses QUIC streams; planned after the architecture stabilizes. |
| Task-level multiplexing (multiple frontend streams per IO Task) | — | Not wired into this runtime; the state-machine architecture supports it. |

Consequently there is no cross-endpoint primary/standby failover, no seamless
transport migration with stream-offset replay, no P2P/STUN/ICE path, and no
automatic HTTP/3-to-WSS fallback. Each tunnel uses the scheme in its endpoint URL.

---

## 14. Public Library API

```rust
// lib.rs
pub use daemon::run_daemon;
pub use server::{ReverseSessionHub, ServerRunner};
pub use supervisor::{ClientRunner, TunnelSupervisor};
pub use tunnel::TunnelCluster;

pub async fn run_server(config: ServerConfig, cancel_token: CancellationToken)
    -> Result<(), TunnelError>;
pub async fn run_client(tunnel: TunnelConfig, cancel_token: CancellationToken)
    -> Result<(), TunnelError>;
pub async fn run(config: AppConfig, cancel_token: CancellationToken)
    -> Result<(), TunnelError>;
```

All modules are public. `core/` exports the Coordinator, IO Task, stage traits,
and state vocabulary; `relay/` exports relay stages; `tunnel/` exports forward
and reverse workflows and runners; `server/` exports ingress workflows and
`ServerRunner`. `io/` exports the neutral vocabulary; `frontend/` exports
`Frontend`, `FrontendAccept`, and `bind`; `backend/` exports the `Transport*`
traits and `TransportRegistry`. Callers own the `CancellationToken` and are
responsible for cancelling and awaiting.

---

## 15. Verification and Benchmarks

- `cargo check --workspace --all-targets` is clean.
- `cargo test --workspace --lib` passes 185 library tests.
- Integration tests under `crates/culvert/tests/` cover real WS/H3/QUIC forward
  and reverse tunnels, UDP framed datagrams, NAT demultiplexing, 0-RTT pool plus
  signaling fallback, reverse promotion and reattachment, and shared-pool
  resource reclamation.
- `config-example.yaml` is parsed by a config test.
- `culvert --check -c config-example.yaml` exercises validation without binding.
- Benchmarks in `crates/culvert/benches/` are development tooling, not a
  throughput or latency specification.
