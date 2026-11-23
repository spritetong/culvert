# Lessons Learned

A concise, actionable knowledge base of non-obvious bugs, architectural traps, and reusable engineering solutions discovered in `culvert`, with particular focus on QUIC, HTTP/3, connection pooling, and multiplexed async I/O.

---

## 1. QUIC & Multiplexed Protocol Liveness

### 1.1 Connection-Level vs. Stream-Level Liveness Blindspot

- **Problem / Trap**: In QUIC (`quinn`), a QUIC connection (`quinn::Connection`) multiplexes multiple bidirectional streams (`quinn::SendStream`, `quinn::RecvStream`). Calling `quinn::Connection::close_reason()` only reports the state of the overall UDP/QUIC connection. An individual stream can be terminated remotely (via `FIN`, `RESET_STREAM`, or `STOP_SENDING`) while `conn.close_reason()` remains `None`.
- **Symptom**: Connection pool liveness probes (`poll_check_alive`) relying on connection-level state report dead or half-closed streams as `HeartbeatStatus::Alive`. When an incoming client acquires this stream from the idle pool, the very first write immediately fails with `BrokenPipe` or `ConnectionReset`, resulting in intermittent connection drops after long idle periods.
- **Root Cause**: `quinn::RecvStream` does not expose an OS-level non-destructive peek (unlike TCP `MSG_PEEK`). Passing a zero-byte buffer (`&mut []`) to `quinn::RecvStream::read` or `poll_read` immediately returns `Ready(Ok(0))` regardless of whether the stream is alive or dead, making zero-byte read useless for EOF detection.
- **Solution**:
  - Implement a 1-byte read-ahead buffer (`read_buf: Option<u8>`) on the stream wrapper (`QuinnTunnelStream`).
  - In `poll_check_alive`, if `read_buf.is_none()`, perform a 1-byte poll read on `quinn::RecvStream`:
    - `Poll::Ready(Ok(()))` with 0 bytes filled (EOF) or `Poll::Ready(Err(_))` (`Reset`/`StopSending`) $\rightarrow$ immediately return `Poll::Ready(HeartbeatStatus::Dead)`.
    - `Poll::Ready(Ok(()))` with 1 byte filled $\rightarrow$ store the byte into `read_buf` and return `Poll::Ready(HeartbeatStatus::Alive)`.
    - `Poll::Pending` $\rightarrow$ stream is healthy and idle, return `Poll::Ready(HeartbeatStatus::Alive)`.
  - In `poll_read`, drain `read_buf` first before delegating to `recv_stream`.
- **References**:
  - Implementation: [stream.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/stream.rs#L536-L565)
  - Probe Test: [test_idle_probe.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_idle_probe.rs#L121-L186)

### 1.2 Session Invalidation vs. Active Stream Destruction in Multiplexed Connection Pools

- **Problem / Trap**: When multiple streams share one multiplexed QUIC/H3 session, two traps can terminate live traffic:
  1. Treating routine idle age expiration (`expired = true`) as connection failure (`HeartbeatStatus::Dead`): a pre-warmed stream reaching `idle_timeout` is normal pool turnover, not a reason to invalidate siblings.
  2. Sending a hard `conn.close(0x00, ...)` from a single-stream failure: QUIC transmits `CONNECTION_CLOSE` over UDP and kills every in-flight stream sharing that connection.
- **Symptom**: At the 30s `DEFAULT_IDLE_TIMEOUT` mark during concurrent benchmarks, active relays fail with `BrokenPipe`, `ConnectionReset`, or `Remote reset: 0x0`; the iperf3 TCP frontend is dropped with unread data and reports `unable to write to stream socket: Connection reset by peer`.
- **Solution**:
  - `take_live_idle_stream` and the reactor sweep drop expired idle streams directly and only mark stream-level `Dead` evictions; either way the pool prunes individual streams and never closes the shared session.
  - Connector helpers (`invalidate_h3_session_if_same`, `invalidate_quic_session_if_same`) clear only the cache entry, never call `conn.close()`. `TransportConnector` no longer exposes `invalidate_broken_sessions`; stale sessions are replaced only in the connector's own dial/open/write failure paths.
- **References**:
  - Implementation: [connector.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/connector.rs#L302-L315), [connection_pool.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/pool/connection_pool.rs#L148-L200)
  - Probe Tests: [test_idle_probe.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_idle_probe.rs#L155-L275)

### 1.3 Stream-Level Activity Clock vs. NAT/Firewall UDP Pinholes

- **Problem / Trap**: Transport keep-alives (e.g. Quinn `keep_alive_interval`) maintain the stateful firewall/NAT UDP 4-tuple translation. However, idle application tunnel streams sitting inside the multiplexed connection must still be retired when their silence exceeds `idle_timeout`.
- **Symptom**: If `TunnelStream::last_activity_ms()` returns `None`, the pool's idle age calculation (`now - last_activity >= idle_timeout`) is permanently bypassed (`expired` is always `false`). Stale streams linger indefinitely in the pool until a middlebox or remote timeout silently drops them.
- **Solution**:
  - Maintain an atomic millisecond timestamp (`AtomicU64`) inside `QuinnTunnelStream` and `H3TunnelStream`.
  - Update `last_activity_ms` on every successful `poll_read` and `poll_write`.
  - Expose `last_activity_ms` through the `TunnelStream` trait so the connection pool reactor and relay pumps can deterministically retire silent streams before NAT/firewall boundaries expire them.
- **References**:
  - Implementation: [stream.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/stream.rs#L210-L215)

### 1.4 Relay Pump Heartbeat Tick Rate and Half-Close Liveness

- **Problem / Trap**:
  - In `StreamStreamPump::relay_with_heartbeat`, `heartbeat_timer` was originally set to sleep for the entire `heartbeat_interval` (15s). With `idle_timeout` at 30s (2x interval), any initial traffic offset caused the first 15s tick to see `elapsed < 15s` (skipping probe), and the second 30s tick to see `elapsed >= 30s`, triggering an idle timeout before any probe could ever run!
  - Furthermore, on half-closed streams (e.g. client finished sending upload data and issued TCP FIN), `stream.poll_check_alive` returns `Dead` on peek (EOF). Calling `poll_check_alive` unconditionally on every heartbeat killed the reverse downloading direction prematurely.
- **Solution**:
  - Decouple the timer tick rate from `heartbeat_interval`: poll `heartbeat_timer` on a high-frequency sub-second interval (`min(heartbeat_interval / 2, 500ms)`).
  - Probe streams before evaluating idle expiration, giving `poll_send_ping` the opportunity to verify underlying transport liveness and refresh `last_activity_ms` when the connection is healthy.
  - Gate liveness checks and ping probes with direction completion (`!a_to_b.read_done` and `!b_to_a.read_done`) so clean EOF in one direction never aborts the opposite direction.
- **References**:
  - Implementation: [pump.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/io/pump.rs#L200-L260)

### 1.5 Idle Pool Expiry Must Follow the Activity Clock, Not Enqueue Time

- **Problem / Trap**: Giving `ManagedStream` an `enqueued_at` deadline to work around a server-side activation timeout made every stream from the same prewarm batch expire at exactly `idle_timeout`. With `pool_size=32` and two tunnels this produced a batch `Evicted expired idle stream(s) during reactor sweep` storm, followed by a synchronized redial wave, on both WSS and QUIC. The server-side deadline was the actual bug; the wall-clock expiry only renamed the storm from `dead` to `expired`.
- **Solution**: expiry comes from `heartbeat_timing(last_activity_ms, ...)`: a stream retires only when its transport activity clock has been quiet for `idle_timeout`. Healthy probe traffic refreshes that clock, so the full pool stays warm indefinitely; a genuinely unresponsive stream retires on its own schedule. Keepalive probes never count as TX, so a dead peer cannot keep the clock fresh.
- **Server side**: the forward server calls `decode_activation_until_cancel`: it waits for activation, peer EOF, or cancellation, with no local timeout derived from its own `idle_timeout`. Unequal server/client idle windows can no longer make the server close a healthy waiting stream first, so no client-side wall-clock deadline is needed.
- **References**:
  - Implementation: [runner.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/server/runner.rs#L220-L230), [connection_pool.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/pool/connection_pool.rs#L116-L165)
  - Regression Test: [test_forward_pool_32.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_forward_pool_32.rs)

---

## 2. Flow Control & Multiplexing Head-of-Line Blocking

### 2.1 QUIC Stream vs. Connection Window Sizing

- **Problem / Trap**: QUIC eliminates transport-level TCP head-of-line packet loss blocking, but introduces flow-control head-of-line blocking if the stream receive window is too close to the connection receive window.
- **Symptom**: When multiple concurrent streams multiplex over a single QUIC connection, if 1 or 2 streams experience backpressure (e.g. slow client or delayed consumer), their unread receive buffers exhaust the shared connection receive window. All other healthy sibling streams on the connection are completely blocked from sending or receiving data (e.g. health probe packets time out).
- **Historical Trap**: Originally configured with `stream_receive_window = 16 MiB` and `receive_window = 32 MiB`. Two stalled streams ($2 \times 16\text{ MiB} = 32\text{ MiB}$) completely locked the entire connection flow control window.
- **Solution**:
  - Configure `stream_receive_window = 4 MiB`, `receive_window = 128 MiB`, and `send_window = 128 MiB`.
  - This 1:32 ratio guarantees that up to 32 concurrent unread streams can stall simultaneously before the shared connection window is exhausted, allowing probe packets and active streams to pass without starvation.
- **References**:
  - Implementation: [consts.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/consts.rs#L201-L208)
  - Flow Control Test: [test_quic_flow_control_hol.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_quic_flow_control_hol.rs)

### 2.2 Full-Duplex Decoupling & Half-Close Semantics

- **Problem / Trap**: In HTTP/3 and WebSocket, bidirectional streams support half-close (e.g., client finishes sending an HTTP POST body, but server continues streaming a multi-gigabyte response).
- **Symptom**: If the stream implementation couples read and write states or marks the stream as `HeartbeatStatus::Dead` upon local write shutdown (`fin_queued` / `fin_sent`), the relay pump aborts prematurely with "unresponsive to heartbeats", truncating the remaining response tail.
- **Solution**:
  - Decouple send and receive halves using `h3::client::RequestStream::split()` and bounded mpsc backpressure channels (`PollSender`).
  - Liveness checking is strictly a read-side / transport error determination. Local FIN means "this direction cannot write any more", but the stream remains `HeartbeatStatus::Alive` so the opposite reading direction can cleanly transfer to completion.
- **References**:
  - Implementation: [stream.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/stream.rs#L217-L245)
  - Architecture: [DESIGN.md](file:///d:/githome/spritetong/@github/culvert/docs/DESIGN.md#L390-L405)

---

## 3. Session Lifecycle, Reconnects & Session Reuse

### 3.1 Dropping All Sub-Streams Closes the Connection (`h3` & Quinn Pitfalls)

- **Problem / Trap (H3 Client Driver Teardown)**: In `h3::client`, the connection driver (`Connection`) is paired with `SendRequest` via a command channel. Each sub-stream requires a clone of `SendRequest`. If the application only distributes `SendRequest` clones to active streams without retaining a long-lived anchor clone:
  - When all active sub-streams finish and are dropped, the number of live `SendRequest` handles drops to zero.
  - The driver's command channel detects EOF (`rx.recv() == None`).
  - The driver task concludes that the client will never send another request, exits its event loop, and **automatically closes the underlying QUIC connection**.
  - Any subsequent incoming tunnel request finds the session terminated and is forced to renegotiate from scratch.
- **Problem / Trap (Quinn Connection Refcount & Idle Timeout)**:
  - In Quinn, streams (`SendStream`, `RecvStream`) hold internal references to the connection. If application code only passes streams to workers without retaining the `quinn::Connection` handle in a session cache, dropping all streams causes the connection's refcount to hit zero, immediately tearing down the UDP connection.
  - Furthermore, if all sub-streams are dropped and no application data flows, Quinn's RFC 9000 `max_idle_timeout` timer begins ticking. If `keep_alive_interval` is not explicitly set in `quinn::TransportConfig`, Quinn will **automatically close the connection with a timeout error** once the idle duration expires.
- **Solution**:
  - In `H3Session`, always retain a permanent **anchor `SendRequest`** clone alongside `quinn::Connection`:

    ```rust
    struct H3Session {
        id: usize,
        sender: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>, // Permanent anchor!
        conn: quinn::Connection,
        driver_alive: Arc<AtomicBool>,
    }
    ```

    This guarantees that the channel sender count is $\ge 1$ at all times, preventing the `h3` driver from exiting when stream count drops to zero.
  - In `RawQuicSession`, retain the owned `quinn::Connection` handle in `H3Connector`'s session cache (`cached_quic_session`).
  - Always configure `transport_config.keep_alive_interval(Some(DEFAULT_QUIC_KEEP_ALIVE_INTERVAL))` (5s) so PING frames maintain the connection even when zero sub-streams exist.
- **References**:
  - Implementation: [connector.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/connector.rs#L38-L50)

### 3.2 Abrupt Stream Drop Triggers `RESET_STREAM` / `STOP_SENDING` Instead of Clean EOF

- **Problem / Trap**: In TCP, closing or dropping a socket after writing flushes buffered data and sends FIN in kernel space. In QUIC (`quinn`):
  - Dropping a `quinn::SendStream` without awaiting `finish()` (or `AsyncWrite::poll_shutdown()`) causes Quinn's `Drop` implementation to emit an immediate QUIC `RESET_STREAM(0)` frame to the remote peer. The remote peer's read loop terminates with `io::ErrorKind::ConnectionReset` rather than receiving a clean EOF (`Ok(0)`).
  - Dropping a `quinn::RecvStream` while unread bytes remain in the receive window causes Quinn's `Drop` implementation to emit a `STOP_SENDING(0)` frame, which causes the remote sender's subsequent writes or flushes to fail with error code 0.
- **Solution**:
  - Always drive `poll_shutdown` / `finish()` to completion on stream writers before letting the stream drop.
  - In relay pumps, ensure both half-close directions drain buffered data to completion before tearing down stream handles.
- **References**:
  - Implementation: [stream.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/stream.rs#L127-L143)

### 3.3 Server Accept Loop Teardown Kills In-Flight Accepted Streams

- **Problem / Trap**: On the server side (`H3Listener`), if the request acceptance loop exits (e.g. on cancellation or transient accept error), returning from the handler task drops `h3_conn` and `conn: quinn::Connection`. This terminates the entire QUIC connection, killing all previously accepted streams that are still relaying in separate spawned tasks!
- **Solution**:
  - In `handle_h3_streams`, after the accept loop finishes, hold the connection open until all accepted request setup tasks drain and the peer actually terminates the connection:

    ```rust
    let _ = conn.closed().cancel_with(&cancel_token).await;
    ```

- **References**:
  - Implementation: [listener.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/listener.rs#L528-L531)

### 3.4 Persistent SendRequest Anchoring & `H3_STREAM_CREATION_ERROR`

- **Problem / Trap**: In the `h3` crate, reusing an existing raw QUIC connection to negotiate an HTTP/3 handshake triggers `H3_STREAM_CREATION_ERROR` due to control stream and QPACK stream ID collisions.
- **Solution**:
  - Keep a persistent single H3 session (`H3Session`) per endpoint, holding an anchor `SendRequest` clone alongside the underlying `quinn::Connection` and a driver liveness flag.
  - When establishing a new H3 session, always dial a dedicated fresh QUIC connection with ALPN `h3`.
  - Use compare-and-clear (`invalidate_h3_session_if_same(session_id)`) on reconnect to prevent lost updates or stomping over newer sessions dialed concurrently.
- **References**:
  - Implementation: [connector.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/connector.rs)

### 3.5 ALPN Strict Routing & TLS Mode Segregation

- **Problem / Trap**: A single server endpoint accepting both HTTP/3 and raw QUIC on UDP can misroute incoming connections if ALPN negotiation is ambiguous.
- **Solution**:
  - Enforce explicit ALPN differentiation: `h3` for HTTP/3 CONNECT tunnels, and `quic` for raw bidirectional QUIC byte tunnels.
  - Raw QUIC listeners compare binary pre-shared tokens in constant time (`common::auth::constant_time_token_eq`) over length-prefixed MessagePack metadata frames, avoiding HTTP header parsing overhead and length leakages.
- **References**:
  - Architecture: [DESIGN.md](file:///d:/githome/spritetong/@github/culvert/docs/DESIGN.md#L454-L493)

### 3.6 Zombie Session Reconnect & Fast Failover Bounds

- **Problem / Trap**: If a remote server process restarts or network routes reset without sending a QUIC `CONNECTION_CLOSE` frame, a cached QUIC connection becomes a "zombie". If the client waits for default transport timeouts (e.g. 30s) before marking the session invalid, visitor requests queue up and fail.
- **Solution**:
  - Invalidate cached H3/QUIC sessions unconditionally upon stream request/response timeouts or handshake errors without requiring `is_alive()` to return false.
  - Implement `QuicLivenessTracker` monitoring UDP datagram progress; if data is transmitted without acknowledgement for `DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD` (10s), signal connection unresponsiveness.
  - Clamp retry concurrency during failure recovery (limit `in_flight` to 1 probe) to prevent connection storms when a remote host is unreachable.
- **References**:
  - Implementation: [connector.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/connector.rs)
  - Integration Test: [test_h3_quic.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_h3_quic.rs)

---

## 4. Speculative Connection Pooling & Fault Tolerance

### 4.1 Speculative Pooled Stream Activation & First-Write Retry

- **Problem / Trap**: Pre-warmed idle streams in a pool are inherently speculative over WANs. Even with 1-second background heartbeat sweeps, a stream can be reset by a middlebox or remote peer in the millisecond window between `acquire()` and writing the activation header (`0xA5` / `0xA7`).
- **Symptom**: The first visitor connection arriving after an extended idle period occasionally experiences an immediate disconnect (`BrokenPipe` or `ConnectionReset`).
- **Solution**:
  - In client forward mapping (`crates/culvert/src/tunnel/forward.rs`), wrap pooled stream acquisition and activation in a 1-attempt retry loop (`attempts < max_attempts`, where `max_attempts = 2` when pooling is enabled).
  - If a pooled stream fails during target activation or initial write, do not drop the visitor; immediately fall back to dialing a fresh dedicated stream and retry activation.
- **References**:
  - Implementation: [forward.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/tunnel/forward.rs#L111-L168)

### 4.2 Reverse 0-RTT Pool: Half-Open Activation Trap & Round-Trip ACK Failover

- **Problem / Trap (Half-Open Socket Buffer Write Blindspot)**: In reverse tunnel mode, the server maintains pre-warmed idle streams in `ReverseInboundPool`. When a stream becomes silently dead (half-open: peer vanished due to NAT/firewall expiration, OS sleep, or network change without TCP FIN/RST), `poll_check_alive` returns `Poll::Pending` (`HeartbeatStatus::Alive`) and age is within bounds.
  - When the server activates the stream by writing `ACTIVATE_STREAM_MAGIC` (`0xA5`/`0xA7`) and calling `flush()`, the write succeeds immediately into the local OS kernel socket buffer / Quinn send queue!
  - The server erroneously assumes activation succeeded, never enters the fallback arm, and pairs the external visitor with the dead stream, resulting in dropped visitor connections after idle periods.
- **Solution (Round-Trip `0xAC` ACK + Select-Loop Failover Channel)**:
  1. Protocol Constant: Define `ACTIVATE_STREAM_ACK = 0xAC` in `consts.rs`.
  2. Client Worker: In `tunnel/reverse.rs`, after reading valid activation magic, the worker echoes `0xAC` with bounded timeout (2s) before connecting to the local target; failure discards and recycles the stream.
  3. Server Fast-Path: In `server/runner.rs`, the activation task writes the magic, flushes, and reads back `0xAC` within `reverse_frame_timeout(idle_timeout)`.
  4. Channel-Based Failover: Because `TaskTracker` is not `Clone` and background tasks cannot mutate accept loop state or trigger signaling disconnect handlers, missing ACK sends `(visitor_stream, visitor_addr)` through an `mpsc::channel` (`failover_tx`) into the main `tokio::select!` event loop.
  5. Unified Signaling Helper: The event loop routes failover visitors to `signal_new_connection`, completely unifying empty-pool fallback and half-open activation recovery without dropping visitors.
- **References**:
  - Implementation: [consts.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/consts.rs#L420-L428), [reverse.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/tunnel/reverse.rs#L315-L335), [runner.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/server/runner.rs#L710-L925)
  - Probe Tests: [runner.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/server/runner.rs#L2125-L2245) (`test_reverse_pool_half_open_stream_fails_over_to_signaling`, `test_reverse_pool_active_ack_fast_path`)

### 4.3 Lockless & Decoupled Active Stream Tracking

- **Problem / Trap**: Using mutex locks around connection pool inventory or coupling active relay throughput to pool replenishment capacity creates contention bottlenecks and thread starvation under high concurrency.
- **Solution**:
  - Decouple active stream tracking using an atomic counter (`Arc<AtomicUsize>`) wrapped in an RAII `ActiveStreamGuard`.
  - When an acquired stream closes (normally or via panic/cancellation), `ActiveStreamGuard::drop` atomically decrements the counter and sends an `ActiveReleased` hint to wake the pool actor.
  - Active relay throughput is completely unbounded by replenishment rate limits.
- **References**:
  - Implementation: [connection_pool.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/pool/connection_pool.rs)

### 4.4 Coordination Race Between Shared Reverse Pool Pre-Warming & Listener Registration

- **Problem / Trap**: When connection pooling is shared across multiple reverse mappings at the tunnel cluster level (`run_shared_reverse_pool`), if the shared pool pre-warmer task is spawned concurrently and dials pre-warmed idle connections before any reverse mapping's control channel has registered (`RegisterReverse`) and established `tunnel_pools[tunnel_id]`:
  1. The server receives the pooled stream tagged with `x-tunnel-id`, but finds no matching `TunnelPoolSlot` in `session_mgr.tunnel_pools` because the control channel has not completed its registration handshake (`RegisterReverse`). The server drops the idle stream with "Dropped reverse idle pool stream: no matching listener", causing dial churn and unnecessary backoff.
  2. In reverse test harnesses or proxy setups that track connection order (e.g. `conn 0` for control channel, `conn 1` for pool stream), concurrent dialing causes connection ordering inversion (`conn 0` becomes an idle pool stream while `conn 1` becomes the control channel), leading to tests killing active relays or asserting on the wrong connection.
- **Solution**:
  - Implement a dedicated coordination mechanism (`ReverseReadyNotifier` with atomic boolean flag and `tokio::sync::Notify`).
  - In `run_tunnel_cluster`, create `ReverseReadyNotifier` and pass it to both `run_shared_reverse_pool` and `run_reverse_mapping`.
  - `run_shared_reverse_pool` checks `reverse_ready.wait_ready()` before dialing any idle streams.
  - The first reverse mapping to receive `RegisterAck` invokes `reverse_ready.mark_ready()`, guaranteeing that `tunnel_pools[tunnel_id]` is already registered on the server before the first pre-warmed idle connection arrives.
- **References**:
  - Implementation: [reverse.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/tunnel/reverse.rs), [cluster.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/tunnel/cluster.rs)

### 4.5 Reverse Pool Identity Must Be Per-Client, Not Per-Tag

- **Problem / Trap**: The server routes pre-warmed reverse streams by `tunnel_id` only. Deriving that id from `tag` alone makes separately deployed clients with the same tag silently join one `TunnelPoolSlot`; the visitor target travels in the activation header, so any worker in the pool may be asked to dial a target only reachable from another client.
- **Solution**: `ClientRunner` and `TunnelSupervisor` generate a per-instance `client_uuid`; `effective_tunnel_id(client_uuid)` derives the default id from that UUID plus `tag`. Explicit `uuid` still pins a shared cluster identity. Grace reattachment is keyed by `(protocol, listen_addr)`, so it carries the incoming `tunnel_id` and replaces the pool generation under the new id.
- **References**: [model.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/config/model.rs) (`effective_tunnel_id`), [manager.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/session/manager.rs) (`ReverseGraceReattach`), [runner.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/server/runner.rs) (`attach_reconnected_control`), [test_multiple_reverse_clients.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_multiple_reverse_clients.rs)

### 4.6 Shared-Slot Removal Must Be Identity-and-Count Guarded

- **Problem / Trap**: `unregister_tunnel_pool()` dropped its `DashMap` read guard before an unconditional `remove(tunnel_id)`. A reconnect could register another listener in that window; the stale teardown then deleted and drained the live pool.
- **Solution**: Decrement under the write guard, and only `remove_if` when the slot is still the same pool generation and `listener_count == 0`. Identity checks alone are not enough; the count must be rechecked in the same lock.

### 4.7 Server Reverse Pool Has No Equivalent False-Dead Eviction

- **Conclusion**: `ReverseInboundPool::pop_live` treats `Poll::Pending` as healthy and uses `enqueued_at + margin >= max_age` as the half-open guard; it never runs a periodic sweep that labels a quiet stream `Dead`. Activation is followed by a round-trip `0xAC` ACK, so a stream popped after the client vanished fails over to signaling instead of blackholing the visitor. No client-forward-pool-style false-dead storm exists here.
- **Configuration boundary**: `max_age` and the ACK timeout come from the server's `idle_timeout`, while the client worker's activation wait comes from its own `idle_timeout`; `RegisterReverse` carries `pool_size` but not the timing. Operators must keep them aligned. The promotion-byte write uses `reverse_frame_timeout(max_age)` rather than a LAN-scale constant.

---

## 5. Async Concurrency & Probe-Driven Verification

### 5.1 Cancellation-Safety in Accept Loops (`ReusableFutureRef`)

- **Problem / Trap**: In `tokio::select!` loops, invoking `.accept().await` directly inside a branch cancels and drops the accept future whenever another branch (e.g. cancellation token or timer) completes first. Dropping an in-flight QUIC stream accept future drops the TLS handshake state and orphans the incoming connection.
- **Solution**:
  - Store accept futures in persistent `reusable_future::ReusableFutureRef` slots allocated outside the select loop.
  - In each loop iteration, poll `&mut slot`. If another select branch fires, the in-flight accept future remains safely preserved in its slot across iterations.
- **References**:
  - Implementation: [listener.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/listener.rs)

### 5.2 Probe-Driven Testing Methodology for Concurrency & Protocol Bugs

- **Problem / Trap**: Relying solely on happy-path integration tests or passive end-to-end tests fails to detect race conditions because retries or fast networks mask the timing window.
- **Methodology**: Construct dedicated probe tests that deterministically reproduce pathological states:
  1. **Blindspot Probe**: Intentionally close the remote stream half while keeping the underlying QUIC connection open; assert that `poll_check_alive` returns `Dead` rather than `Alive`.
  2. **Kill-Race Probe**: Populate an idle pool with multiple sibling streams sharing one multiplexed session; force session invalidation on the first acquire, and verify that subsequent acquires do not return dead sibling streams.
  3. **Flow Control HoL Probe**: Saturate multiple sibling streams with tens of megabytes of unread data; verify that a newly opened probe stream on the same connection completes prompt transfers without timing out.
- **References**:
  - Probe Tests: [test_idle_probe.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_idle_probe.rs), [test_quic_flow_control_hol.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_quic_flow_control_hol.rs)

### 5.3 Event Loop Await Constraints & Future Abort Drop Guards

- **Problem / Trap (Unconstrained Blocking Awaits in Event Loops)**:
  - Inside a `tokio::select!` event loop, if a branch executes an async call containing an unconstrained blocking `await` (e.g. `router.send(notify_msg).await`, `idle_pool.promote_idle(...).await`, or `event_tx.send(...).await`), execution suspends inside that sub-operation.
  - While suspended, the event loop cannot poll its cancellation branch (`_ = cancel_token.cancelled()`). If the remote peer, network buffer, or channel consumer stalls, the entire event loop hangs until timeout (or indefinitely), delaying or deadlocking graceful shutdown.
- **Problem / Trap (Future Abort Resource Leaks)**:
  - When parent task trackers (`TaskTracker::shutdown_and_wait`) enforce deadlines and abort outstanding tasks (`abort_all()`), the in-flight future is dropped at whatever `.await` point it was currently parked.
  - Any sequential cleanup code placed after the event loop (e.g. `session_mgr.unregister_pool(...)`, `session_mgr.remove(&session_id)`) is **completely skipped**.
  - Stale pools and abandoned session registrations remain orphaned in memory across subsequent reconnects or incoming streams.
- **Solution & Principles**:
  1. **Strict Cancel Token Constraints for Event Loops**:
     - All blocking awaits executed in or synchronously invoked by an event loop MUST be constrained by `cancel_with(cancel_token)` or bounded timeouts.
     - Extract complex, multi-branch visitor handling into dedicated helper functions (e.g. `handle_accepted_reverse_visitor`).
     - **Principle of Internal Cancellation Awareness**: If an async helper function internally constrains all its blocking awaits with `cancel_token`, the event loop caller does NOT need redundant outer `.cancel_with(cancel_token)` wrappers. Keep logic lean and precise.
  2. **Event Loops vs. Spawned Worker Tasks**:
     - Detached worker tasks spawned from the event loop run their own futures. When the event loop terminates, the task tracker automatically cancels and drops them. Not every await in spawned worker tasks requires artificial cancellation wrappers unless it holds shared state.
  3. **RAII Drop Guards for Future Abort Safety via `scopeguard`**:
     - Never rely solely on post-loop sequential cleanup statements for resources registered in shared state.
     - Avoid declaring verbose custom structs and repetitive `impl Drop` boilerplate for execution-scoped cleanups. Instead, use `scopeguard`:
       - Use `scopeguard::defer! { ... }` for local scope exits (e.g. `p.on_stream_closed()`, `active_actors.fetch_sub(...)`).
       - Use `scopeguard::guard(state, cleanup_fn)` (`PendingSessionGuard`, `ReversePoolGuard`) when the guard needs to be returned or passed across function boundaries, disarming with `scopeguard::ScopeGuard::into_inner(guard)` upon successful pairing.
     - If the future is aborted mid-flight, Rust's drop semantics guarantee that the scopeguard runs to unregister pools and clean up pending session entries.
- **References**:
  - Implementation: [manager.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/session/manager.rs) (`PendingSessionGuard`, `ReversePoolGuard` type aliases using `scopeguard`), [channel.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/control/channel.rs) (`scopeguard::defer!`), [forward.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/tunnel/forward.rs) (`scopeguard::defer!`), [runner.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/server/runner.rs)
  - Integration Tests: [runner.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/server/runner.rs#L1934-L2060) (`test_reverse_control_cancels_promptly_during_stalled_visitor_signaling`, `test_reverse_control_future_abort_cleans_up_resources`)

### 5.4 Scattered Background Spawn Tasks: Cancellation Responsiveness & Drop Lifecycle

- **Problem / Trap (Unconstrained Awaits in Scattered Spawn Tasks)**:
  - Beyond the main event loop, background tasks spawned via `tokio::spawn`, `JoinSet::spawn`, or `TaskTracker::spawn_with_cancel` can silently hold unconstrained awaits.
  - Examples:
    - HTTP/3 server `request_tasks.spawn`: `resolver.resolve_request().await` and `stream.send_response().await` were unconstrained by timeouts/cancellation. A slow or hanging client could pin a spawned task in the `JoinSet` until QUIC idle timeout, blocking prompt drain.
    - Multiplexer writer driver task: When the main loop breaks on cancellation, sequential cleanup `framed_writer.flush().await` can hang indefinitely if the underlying stream socket stalls or the remote peer stopped reading.
    - `MuxSession::accept_stream()`: Parking on `rx.recv().await` without checking session cancellation delays consumer response during shutdown.
    - Listener `accept()` slots: Calling `rx.lock().await` without cancellation bounding can stall if cancelled prior to lock acquisition.
- **Problem / Trap (Leaked Background Pump Tasks on Stream Drop)**:
  - When an abstraction wraps lower-level asynchronous streams by spawning background pump tasks (e.g. `H3TunnelStream::spawn_split_pump` splitting an HTTP/3 bidirectional stream into independent outbound and inbound tasks):
  - If the wrapper stream is dropped prematurely (due to relay completion, future abort, or timeout) while a background pump task is awaiting I/O from a stalled peer, the background tasks linger indefinitely until the transport connection closes.
- **Solution & Principles**:
  1. **Constrain All Inbound Handshakes and Request Resolutions**:
     - Spawned request/handshake tasks in listeners must wrap resolution and initial response handshake with `timeout_or_cancel(DEFAULT_HANDSHAKE_TIMEOUT, &cancel_task)`.
  2. **Post-Loop Socket Flush Timeout**:
     - Post-loop cleanup awaits like `framed_writer.flush().await` must be bounded with a short timeout (e.g. `Duration::from_millis(500)`).
  3. **Cancellation-Aware Locks in Reusable Future Slots**:
     - In `accept(&mut self, slot)` implementations, acquire receiver mutexes via `rx.lock().cancel_with(&cancel).await` so that cancellation immediately aborts without waiting for lock availability.
  4. **RAII Abort Guards on Stream Wrappers**:
     - Store `JoinHandle<()>` of spawned background pump tasks in the stream struct (`outbound_task`, `inbound_task`).
     - Implement `Drop` for the stream wrapper to call `task.abort()`, guaranteeing immediate cancellation and release of underlying QUIC/TCP stream resources on drop.
- **References**:
  - Implementation: [listener.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/listener.rs) (`request_tasks.spawn` timeout_or_cancel, `accept` cancel-aware lock), [session.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/mux/session.rs) (writer flush timeout, `accept_stream` cancellation), [stream.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/stream.rs) (`H3TunnelStream` Drop aborts background pump tasks), [uds.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/uds.rs), [ws/listener.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/ws/listener.rs), [udp.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/frontend/udp.rs)
  - Tests: [tests.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/mux/tests.rs#L398) (`test_mux_accept_stream_immediate_cancellation`), [stream.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/stream.rs#L713) (`test_h3_stream_drop_aborts_background_tasks`)

---

## 6. Configuration Schema Design & Deserialization Boundaries

### 6.1 Group-Partitioned Mappings with Canonical Flattening & Strict Enums

- **Problem / Trap (Redundancy & Decoupling)**:
  - Flattened mapping lists with repeated `mode: "forward"` or `mode: "reverse"` entries and ambiguous top-level `pool_size` lead to confusing configurations when forward and reverse rules coexist in one tunnel.
  - However, directly refactoring internal downstream runtime loops (which iterate over flat collections of `(mode, protocol, listen, target)`) to reflect hierarchical schema layouts risks widespread ripple effects and regressions across connection dispatchers, reverse supervisors, and stream pumps.
- **Problem / Trap (Case-Insensitive Mandatory Enums in Serde)**:
  - Using serde `rename_all = "lowercase"` or `rename_all = "UPPERCASE"` rejects case variations (e.g. `TCP` vs `tcp`).
  - Adding `#[serde(default)]` to fallback to TCP makes the protocol optional, violating requirements where protocol specification must be mandatory without silent defaults.
- **Solution**:
  1. **Hierarchical User Schema (`TunnelMappingsConfig`)**:
     - Partition configuration into `mappings.forward` and `mappings.reverse` (`MappingGroupConfig`), each with its own `pool_size` (or `pool`) and `rules` (or `ports`/`items`).
     - Remove redundant `mode` from individual rules.
  2. **Canonical Flattening Layer (`canonical_mappings`)**:
     - Provide `canonical_mappings(&self) -> Result<Vec<PortMappingConfig>, TunnelError>` on `TunnelConfig`.
     - Validates at least one rule exists across all groups, injects the implicit `TunnelMode::Forward` / `TunnelMode::Reverse`, and produces the flat representation expected by downstream runtimes.
     - Preserves existing relay pipelines, server registration loops, and frontend listeners untouched.
  3. **Explicit Case-Insensitive `FromStr` Deserializer without Defaults**:
     - Implement custom `Deserialize` for `L4Protocol` via `FromStr` that parses `"tcp"`, `"TCP"`, `"udp"`, `"UDP"` case-insensitively.
     - Omit `#[serde(default)]` on `MappingRuleConfig.protocol` to ensure serde strictly rejects missing protocol fields with descriptive errors.
- **References**:
  - Implementation: [model.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/config/model.rs), [addr.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/io/addr.rs), [loader.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/config/loader.rs)

---

## 7. Socket Binding & Dual-Stack Networking (`socket2` & Cross-Platform Conventions)

### 7.1 Cross-Platform IPv6/IPv4 Dual-Stack Sockets via `socket2`

- **Problem / Trap (OS Default Divergence on `IPV6_V6ONLY`)**:
  - On Linux, IPv6 dual-stack behavior depends on the kernel `sysctl net.ipv6.bindv6only` (typically `0`), whereas Windows strictly defaults `IPV6_V6ONLY` to `true` (`1`).
  - Calling standard library `TcpListener::bind("[::]:port")` or `UdpSocket::bind("[::]:port")` without configuring `IPV6_V6ONLY` results in IPv6-only sockets on Windows, completely rejecting incoming IPv4 traffic.
  - Furthermore, on OS installations or containers where IPv6 is completely disabled or unsupported, attempting to create `Domain::IPV6` sockets fails with `EAFNOSUPPORT` / `WSAEAFNOSUPPORT` (OS error 10047 on Windows, 97 on Linux).
- **Solution**:
  - Create sockets using `socket2::Socket::new(Domain::IPV6, ...)`:
    1. Call `socket.set_only_v6(false)` to explicitly enable dual-stack binding across both Windows and Unix.
    2. Configure address reuse via `configure_reuse()`:
       - **Unix**: `socket.set_reuse_address(true)` — permits rebind during `TIME_WAIT` while keeping a single live owner of `addr:port`.
       - **Windows**: `socket.set_exclusive_addr_use(true)` (`SO_EXCLUSIVEADDRUSE`) **instead of** `SO_REUSEADDR`. On Windows `SO_REUSEADDR` carries BSD `SO_REUSEPORT` semantics, letting a second process bind the same `addr:port` and **duplicate/intercept UDP datagrams**. `SO_EXCLUSIVEADDRUSE` blocks that while still allowing `TIME_WAIT` rebind, keeping the security posture identical across platforms.
    3. Bind to `[::]:port` (`SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, port, 0, 0)`).
  - Implement a graceful fallback: If IPv6 socket creation fails, treat it as "IPv6 unsupported / disabled" and fall back to an IPv4 socket on `0.0.0.0:port`. Detection covers `io::ErrorKind::Unsupported`, `io::ErrorKind::AddrNotAvailable`, and the raw OS codes: Linux `EAFNOSUPPORT` (97) / `EADDRNOTAVAIL` (99, e.g. `net.ipv6.conf.all.disable_ipv6=1`), Windows `WSAEAFNOSUPPORT` (10047) / `WSAEADDRNOTAVAIL` (10049), and macOS/BSD `EAFNOSUPPORT` (47) / `EADDRNOTAVAIL` (49). `EADDRINUSE` (98 / 10048) is deliberately **not** treated as unsupported so a genuine port conflict still fails loudly.
  - Reverse `:port` connect-back: advertise BOTH `[::]:port` and `0.0.0.0:port` as candidates and try them in order. A peer that fell back to IPv4-only would be unreachable via the literal `[::]:port` form, silently breaking the reverse inbound relay (TCP and UDP) — the IPv4 candidate restores it.
- **References**:
  - Implementation: [platform/mod.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/platform/mod.rs#L280-L335) (`create_dual_stack_tcp_listener`, `create_dual_stack_udp_socket`, `configure_reuse`)

### 7.2 Listen Socket (`ListenAddr`) vs. Connect Destination (`TargetAddr`) Separation

- **Problem / Trap**:
  - Conflating `listen` and `target` syntax by accepting `:port` for both. A listener binds locally to incoming network traffic, where `:port` naturally indicates "bind all local interfaces across IPv4 and IPv6".
  - In contrast, `target` is an outbound destination dialed via `connect()`. A target address of `:8080` lacks a destination host and is semantically invalid. Allowing it leads to vague DNS resolution errors or attempts to connect to `0.0.0.0` or localhost that fail unexpectedly in production.
- **Solution**:
  - Split address abstraction into two distinct vocabulary types:
    - `ListenAddr`: `DualStack(u16)`, `Ip(SocketAddr)`, `Domain(String, u16)`, `Path(PathBuf)`. Parses `:port` into `DualStack(port)`.
    - `TargetAddr`: `Ip(SocketAddr)`, `Domain(String, u16)`, `Path(PathBuf)`. Strictly rejects any address without a destination host (e.g. `:port`), failing fast at configuration parse and load time.
- **References**:
  - Implementation: [addr.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/io/addr.rs#L202-L250), [loader.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/config/loader.rs#L240-L280)

### 7.3 IPv4-Mapped IPv6 Peer Address Normalization

- **Problem / Trap**:
  - When an IPv4 client connects to a dual-stack socket bound to `[::]:port`, the operating system presents the client address in IPv4-mapped IPv6 format (`::ffff:192.168.1.100` / `SocketAddr::V6`).
  - Downstream components (such as reverse session registration routing, logging, metrics, or UDP NAT flow tables) compare or log peer addresses. Treating `::ffff:127.0.0.1` and `127.0.0.1` as distinct addresses causes key mismatches, duplicate mappings, and confusing access logs.
- **Solution**:
  - In `GenericListener::accept_owned` and `UdpFrontend` datagram loops, normalize accepted peer addresses using `v6.ip().to_canonical()`.
  - If the IPv6 address is an IPv4-mapped address, `to_canonical()` automatically unwraps it to `IpAddr::V4`, converting `SocketAddr::V6` into clean `SocketAddr::V4`.
- **References**:
  - Implementation: [platform/mod.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/platform/mod.rs#L88-L95), [udp.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/frontend/udp.rs#L135-L142)

---

## 8. Outbound Proxying & Transparent Dialing

### 8.1 Transparent Dialer Decoration & DNS Isolation

- **Problem / Trap**:
  - In a tunneling client, resolving the destination endpoint's hostname locally when dialing through a proxy causes DNS leaks, DNS cache poisoning, and fails completely for private or split-horizon internal hostnames (e.g. cloud VPC or Kubernetes internal hostnames reachable only through the proxy).
  - Additionally, modifying transport connectors (`WsConnector`) to support proxying introduces branch complexity into protocol handshakes (TLS, WebSocket Upgrade).
- **Solution**:
  - Implement outbound proxies as a transparent `Dialer` decorator (`ProxyDialer` wrapping `DefaultDialer`).
  - **DNS Isolation**: The inner dialer uses the client's configured `DnsResolver` solely to resolve the proxy server's own IP address (`proxy_host:proxy_port`).
  - **Target Delegation**: The destination endpoint's `target_host` is never resolved locally; it is passed verbatim to the proxy via HTTP `CONNECT` or SOCKS5 domain address type (`ATYP = 0x03`, SOCKS5h).
  - **Zero-Touch Transports**: Transports (such as `ws://` and `wss://`) dial via `endpoint.dialer.dial(host, port)`. Because the returned stream is an established, transparent byte tunnel, downstream TLS handshakes and WebSocket upgrades execute with zero modification.
- **References**:
  - Implementation: [proxy.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/dialer/proxy.rs)
  - Integration Test: [test_proxy_e2e.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_proxy_e2e.rs)

---

## 9. QUIC Throughput on High-MTU Paths

### 9.1 Discovery Bound and Owned Writes Must Match the Relay Pump

- **Problem / Trap**: quinn defaults to a 1472-byte `max_udp_payload_size` and a 1452-byte PLPMTUD upper bound. Even on loopback, that multiplies datagram, AEAD, and syscall counts. Separately, QUIC/H3 adapters previously left `caps().owned_write == false`, forcing the relay pump into borrowed writes; quinn-proto then copied each 64 KB chunk again inside `ByteSlice::pop_chunk`.
- **Symptom**: Isolated QUIC reached 210–655 MB/s while the production relay path stayed near 131 MB/s; H3 added almost no extra loss once the full path was measured.
- **Solution**:
  - Set `EndpointConfig::max_udp_payload_size` on both listener and connector, and raise `MtuDiscoveryConfig::upper_bound` on both transport configs (binding dual-stack sockets on connectors as well). Keep the initial MTU at 1200 so ordinary paths still handshake.
  - Implement `poll_write_owned` for `QuinnTunnelStream` via `SendStream::write_chunks`, and for `H3TunnelStream` by enqueueing the incoming `Bytes` without `copy_from_slice`.
  - Keep `immediate_ack` opt-in (default `false`) so RFC 9000 delayed ACKs protect asymmetric WAN and mobile return paths from ACK saturation. Default congestion control to BBR (`QuicCongestion::Bbr`) to sustain throughput through random packet loss without Cubic's aggressive window halving. Enable stream `send_fairness` to avoid bulk starvation on narrow pipes, and expand `crypto_buffer_size` to 64 KiB for jittery handshakes.
  - Expose only `quic.mtu_upper_bound`, `quic.immediate_ack`, and `quic.congestion` (`bbr`/`cubic`/`new_reno`) in config. Derive the endpoint's `max_udp_payload_size` from the MTU bound, and validate the bound before starting listeners.
  - Verify with `crates/culvert/tests/test_quic_throughput.rs`, especially the `full_relay` cases that use the production client/server config.
- **References**:
  - Constants: [consts.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/consts.rs) (`DEFAULT_QUIC_MTU_UPPER_BOUND`, `DEFAULT_QUIC_MAX_UDP_PAYLOAD_SIZE`)
  - Implementation: [mod.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/mod.rs), [stream.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/stream.rs)
  - Probe Test: [test_quic_throughput.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_quic_throughput.rs)

---

## 10. Server-Side mTLS & Client Certificate Verification

### 10.1 Isolated Client Root Store vs. Shared WebPKI Roots

- **Problem / Trap**: When adding server-side client certificate verification (`WebPkiClientVerifier`), seeding the server's client root store with default public web PKI roots (`webpki_roots::TLS_SERVER_ROOTS`) allows any client presenting a certificate signed by any public CA to pass verification.
- **Solution**:
  - Maintain strict separation: client outbound connections use public roots plus optional custom CA, while server-side client verification builds an empty `RootCertStore` containing ONLY the certificates from `client_ca_certificate`.
  - In `TlsConfigBuilder::build_server_config`, evaluate `client_ca_certificate` and `client_auth` mode (`Required`, `Optional`, `None`). Default to `Required` whenever `client_ca_certificate` is supplied.
  - Fail fast during configuration validation if `client_auth` is configured on plaintext (`ws://`) listeners, or if `client_auth` is set to `Required` without a client CA certificate.
- **References**:
  - Implementation: [tls.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/dialer/tls.rs#L250-L315), [model.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/config/model.rs#L550-L650)
  - Integration Tests: [test_wss_tls.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_wss_tls.rs#L1000-L1150), [test_h3_quic.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_h3_quic.rs#L1210-L1350)

---

## 11. Multiplexed QUIC Resiliency, Activation Handshakes & Generational Lifecycles

### 11.1 Local Allocation Blindspot & Inline Session Liveness

- **Problem / Trap**: In QUIC (`quinn`), `conn.open_bi()` allocates stream IDs locally and `write_all()` buffers frames into user-space queues without a network round-trip. When a sudden network failure drops the connection, stream creation and initial writes succeed locally in RAM while the remote peer never receives them.
- **Symptom**: Pool workers and fallback dialers open streams into a zombie session; `decode_activation` or relay pumps hang until max idle timeout. The tunnel becomes completely unusable until process restart, while `wss://` (unshared TCP dial) works immediately.
- **Solution**:
  - Invalidate connector session caches unconditionally upon any stream open or initial write error/timeout without gating on `is_alive()`.
  - Check `session.is_alive()` and `session.check_liveness()` directly inside `QuinnTunnelStream` and `H3TunnelStream`'s `poll_read`, `poll_write`, and `poll_write_owned`. Stalled UDP connections immediately abort pending readers and writers with `ConnectionReset` / `TimedOut`, waking all waiting pool workers.
- **References**:
  - Implementation: [connector.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/connector.rs), [stream.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/stream.rs)
  - Probe Test: [test_idle_probe.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_idle_probe.rs)

### 11.2 Control-Data Generational Decoupling in Reverse Tunnels

- **Problem / Trap**: Retaining a single `data_connector` outside the reverse mapping reconnect loop causes reconnected control channels to pair with a stale `data_connector` whose cached QUIC connection and local UDP socket are dead or blocked by NAT.
- **Solution**:
  - Scope `data_connector` inside the reverse control reconnect loop. Each control generation creates a fresh data connector with a new local UDP socket, clean NAT 4-tuple, and fresh session cache matching the server's new pool generation.
- **References**:
  - Implementation: [reverse.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/tunnel/reverse.rs)

### 11.3 Symmetric Round-Trip Activation ACK & Idle Boundary Grace

- **Problem / Trap**: Forward mode previously lacked round-trip activation ACK, causing half-open pooled streams to fail immediately in `serve()` without triggering the visitor retry loop. Simultaneously, tight `decode_activation` timeouts caused client workers to drop streams right as the server popped and activated them near the 30s idle boundary.
- **Solution**:
  - Unify forward and reverse pooling: servers echo `ACTIVATE_STREAM_ACK` (`0xAC`) on forward pooled activation, allowing client retry loops (`attempts < max_attempts`) to cleanly recover from stale streams without dropping visitors.
  - Server `pop_live` retires streams approaching expiration (`elapsed + margin >= max_age`), while reverse worker `decode_activation` adds a safety grace window (`idle_timeout + reverse_frame_timeout.max(5s)`), eliminating boundary race conditions on high-latency WANs.
- **References**:
  - Implementation: [forward.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/tunnel/forward.rs), [runner.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/server/runner.rs), [manager.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/session/manager.rs), [reverse.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/tunnel/reverse.rs)

### 11.4 QuicLiveness State Machine: Baseline Anchoring vs. Per-Poll TX Gating

- **Problem / Trap (Sticky Latch vs. Per-Poll TX Gating)**:
  1. *Sticky Latch*: In QUIC, non-ack-eliciting datagrams (e.g. pure ACK frames) increase `cur_tx` without eliciting an ACK. If liveness tracking latches `cur_tx > last_tx` without refreshing the TX baseline, routine idle periods $\ge 10s$ cause subsequent liveness checks to falsely declare healthy sessions dead and terminate shared connections.
  2. *Per-Poll TX Gating Trap*: Conversely, requiring `cur_tx > last_tx` on the exact evaluation call that inspects `now - stalled_since >= threshold` causes missed detections. If a packet was transmitted at 9.8s and evaluated at 10.5s without an intervening packet (e.g. under 5s keepalive intervals), the check observes `!tx_progressed` and reports `Alive`, delaying failover or completely missing dead sessions when TX ceases.
- **Solution (Anchor Baseline on Initial Stall)**:
  - In `QuicLiveness`, record `stall_start_tx` when `stalled_since` is first initialized.
  - Declare a session dead when `cur_tx > stall_start_tx` and `now.duration_since(since) >= DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD`.
  - Isolated single-TX events followed by silence never exceed `stall_start_tx` and remain healthy.
  - Truly unresponsive sessions that attempt subsequent transmissions (keepalives or retransmissions) reliably trigger `Dead` on any check past the threshold without requiring a coincident TX on the evaluation instant.
  - Clean RX progress clears `stalled_since` and resets `stall_start_tx` to 0.
- **References**:
  - Implementation: [mod.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/mod.rs)
  - Unit Tests: [mod.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/transport/h3/mod.rs#L300-L395)

---

## 12. Dynamic Certificate Monitoring & Zero-Downtime TLS Hot-Reloading

### 12.1 RAII Watcher Lifetime Anchoring in Dynamic TLS Resolvers

- **Problem / Trap**: In file-watcher architectures (e.g. `notify::RecommendedWatcher`), dropping the watcher handle immediately unregisters OS directory hooks and drops the underlying event channel/thread. If a temporary builder or factory registers the file watcher and returns only a `rustls::ServerConfig`, the watcher is dropped at the end of the helper function, silently killing dynamic file reload while leaving handshakes statically frozen.
- **Symptom**: Changing certificate files on disk generates zero filesystem events; TLS connections continue presenting the old certificate until full process restart.
- **Solution**:
  - Anchor the `CertFileWatcher` handle directly inside `DynamicCertResolver` (`set_watcher`) and `DynamicClientCertVerifier` (`set_watcher`).
  - Because `rustls::ServerConfig` holds an owned reference (`Arc<dyn ResolvesServerCert>`) to `DynamicCertResolver`, the watcher's lifetime is 100% bound to the active TLS listener without requiring a global singleton, static registry, or leaky background handles.
  - When the listener is cancelled or dropped, Rust's RAII drop semantics cleanly tear down the watcher and unregister directory hooks.
- **References**:
  - Implementation: [resolver.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/cert/resolver.rs), [manager.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/cert/manager.rs)

### 12.2 Multi-File Write Coalescing, Debouncing & In-Memory Key-Pair Consistency

- **Problem / Trap (Multi-File ACME Renewal Race)**: Automated certificate managers (such as Caddy event scripts or Certbot) copy or write certificate chains (`server.crt`) and private keys (`server.key`) sequentially. A naive filesystem watcher that immediately triggers reload upon the first event attempts to read files while:
  1. The new private key is paired with the old certificate (triggering cryptographic handshake failures); or
  2. The second file is partially written or locked by the OS writer (causing parse errors).
- **Symptom**: During routine certificate renewal, transient TLS handshake failures occur for active visitors, or error logs flood with "invalid PEM".
- **Solution**:
  - Implement an asynchronous Tokio debouncing loop: any write/create/modify event on watched files resets a quiet-period deadline (default 1000ms). Reload triggers only after the quiet window elapses without further events.
  - Execute two-phase staging and validation: parse PEM syntax, verify root stores, and strictly assert key-pair correspondence via `certified_key.keys_match()`.
  - If any check fails, abort the reload, log an actionable warning, and keep the currently active valid certificate in memory untouched. Corrupt or partial writes on disk never disrupt live traffic.
- **References**:
  - Implementation: [watcher.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/cert/watcher.rs), [reader.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/cert/reader.rs), [manager.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/src/cert/manager.rs)
  - Integration Tests: [test_cert_hot_reload.rs](file:///d:/githome/spritetong/@github/culvert/crates/culvert/tests/test_cert_hot_reload.rs)

---

## 13. Multi-Architecture Container Distribution & CI/CD Pipelines

### 13.1 Decoupling Native Cross-Compilation from Multi-Arch Docker Manifest Assembly

- **Problem / Trap (QEMU Emulation Compilation Bottleneck)**:
  - In Rust projects with heavy cryptographic and asynchronous dependencies (`quinn`, `rustls`, `h3`, `fastwebsockets`, `ring`), compiling directly inside multi-arch Docker Buildx using QEMU emulation (e.g. emulating `linux/arm64` on standard `ubuntu-latest` x86_64 runners) is 10x-15x slower than native compilation.
  - A build that takes 2-3 minutes natively often balloons to 40-50+ minutes under QEMU, leading to GitHub Actions runner timeouts or out-of-memory aborts.
- **Problem / Trap (Broken Local Developer Experience)**:
  - Replacing the standard Dockerfile with an image that only accepts prebuilt binaries breaks local developer workflows (`docker build -t culvert .` fails because no prebuilt binaries exist in the developer workspace).
- **Solution (Dual-Mode Dockerfile + Staged CI Assembly)**:
  1. **Dual-Mode Dockerfile**: Structure `Dockerfile` with multi-stage selector:
     - `builder-source` (default `BUILD_MODE=source`): standard `rust:bookworm` compilation for local builds.
     - `builder-prebuilt` (`BUILD_MODE=prebuilt`): copies pre-compiled binaries from `bin/${TARGETARCH}/culvert` without running cargo.
     - `final`: unprivileged `culvert` user, `dumb-init` (PID 1 signal forwarding), and `ca-certificates`.
  2. **CI Pipeline Staging**: In `release.yml`, reuse cross-compiled static musl binaries (`packages-x86_64-unknown-linux-musl` and `packages-aarch64-unknown-linux-musl`) already produced by the matrix `build` job.
  3. **Fast Manifest Assembly**: In the `docker` job, set up QEMU (for lightweight `apt-get` on base images only) and Buildx, then invoke `docker/build-push-action` with `BUILD_MODE=prebuilt` across `linux/amd64,linux/arm64`. Assembly and upload complete in under 30 seconds.
- **References**:
  - Implementation: [Dockerfile](file:///d:/githome/spritetong/@github/culvert/Dockerfile), [release.yml](file:///d:/githome/spritetong/@github/culvert/.github/workflows/release.yml)



