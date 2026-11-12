# Lessons Learned

A concise, actionable knowledge base of non-obvious bugs, architectural traps, and reusable engineering solutions discovered in `micro-tunnel` (`culvert`), with particular focus on QUIC, HTTP/3, connection pooling, and multiplexed async I/O.

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
  - Implementation: [stream.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/stream.rs#L536-L565)
  - Probe Test: [test_idle_probe.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/tests/test_idle_probe.rs#L121-L186)

### 1.2 Session Invalidation Kill Race in Multiplexed Connection Pools
- **Problem / Trap**: When multiple idle streams in a pool share a single multiplexed QUIC/H3 session, evicting an over-age or dead stream triggers session invalidation (`connector.invalidate_broken_sessions()`), which calls `conn.close(0x00, b"broken")`.
- **Symptom**: If the pool reactor pops an idle stream for a caller and simultaneously detects that an adjacent stream is dead, calling `invalidate_broken_sessions()` aborts the underlying QUIC connection. The caller receives a stream whose underlying session was just killed, causing the activation handshake to fail immediately.
- **Solution**:
  - In `take_live_idle_stream`: If any dead stream is encountered during LIFO scanning (`evicted_dead == true`), treat the entire cached session as compromised:
    1. Call `connector.invalidate_broken_sessions()`.
    2. Completely clear the remaining `idle_streams` queue.
    3. Discard any candidate stream popped during that sweep (`acquired = None`).
    4. Fall through to dial a brand-new connection rather than handing out a poisoned sibling.
  - In background heartbeat sweep (`PoolEvent::StreamsEvicted`): If a background eviction invalidates the session, unconditionally clear `idle_streams` and reset `idle_count = 0`.
- **References**:
  - Implementation: [connection_pool.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/pool/connection_pool.rs#L148-L200)
  - Probe Test: [test_idle_probe.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/tests/test_idle_probe.rs#L40-L119)

### 1.3 Stream-Level Activity Clock vs. NAT/Firewall UDP Pinholes
- **Problem / Trap**: Transport keep-alives (e.g. Quinn `keep_alive_interval`) maintain the stateful firewall/NAT UDP 4-tuple translation. However, idle application tunnel streams sitting inside the multiplexed connection must still be retired when their silence exceeds `idle_timeout`.
- **Symptom**: If `TunnelStream::last_activity_ms()` returns `None`, the pool's idle age calculation (`now - last_activity >= idle_timeout`) is permanently bypassed (`expired` is always `false`). Stale streams linger indefinitely in the pool until a middlebox or remote timeout silently drops them.
- **Solution**:
  - Maintain an atomic millisecond timestamp (`AtomicU64`) inside `QuinnTunnelStream` and `H3TunnelStream`.
  - Update `last_activity_ms` on every successful `poll_read` and `poll_write`.
  - Expose `last_activity_ms` through the `TunnelStream` trait so the connection pool reactor and relay pumps can deterministically retire silent streams before NAT/firewall boundaries expire them.
- **References**:
  - Implementation: [stream.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/stream.rs#L210-L215)

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
  - Implementation: [consts.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/consts.rs#L201-L208)
  - Flow Control Test: [test_quic_flow_control_hol.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/tests/test_quic_flow_control_hol.rs)

### 2.2 Full-Duplex Decoupling & Half-Close Semantics
- **Problem / Trap**: In HTTP/3 and WebSocket, bidirectional streams support half-close (e.g., client finishes sending an HTTP POST body, but server continues streaming a multi-gigabyte response).
- **Symptom**: If the stream implementation couples read and write states or marks the stream as `HeartbeatStatus::Dead` upon local write shutdown (`fin_queued` / `fin_sent`), the relay pump aborts prematurely with "unresponsive to heartbeats", truncating the remaining response tail.
- **Solution**:
  - Decouple send and receive halves using `h3::client::RequestStream::split()` and bounded mpsc backpressure channels (`PollSender`).
  - Liveness checking is strictly a read-side / transport error determination. Local FIN means "this direction cannot write any more", but the stream remains `HeartbeatStatus::Alive` so the opposite reading direction can cleanly transfer to completion.
- **References**:
  - Implementation: [stream.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/stream.rs#L217-L245)
  - Architecture: [DESIGN.md](file:///d:/githome/spritetong/micro-tunnel/docs/DESIGN.md#L390-L405)

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
  - Implementation: [connector.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/connector.rs#L38-L50)

### 3.2 Abrupt Stream Drop Triggers `RESET_STREAM` / `STOP_SENDING` Instead of Clean EOF
- **Problem / Trap**: In TCP, closing or dropping a socket after writing flushes buffered data and sends FIN in kernel space. In QUIC (`quinn`):
  - Dropping a `quinn::SendStream` without awaiting `finish()` (or `AsyncWrite::poll_shutdown()`) causes Quinn's `Drop` implementation to emit an immediate QUIC `RESET_STREAM(0)` frame to the remote peer. The remote peer's read loop terminates with `io::ErrorKind::ConnectionReset` rather than receiving a clean EOF (`Ok(0)`).
  - Dropping a `quinn::RecvStream` while unread bytes remain in the receive window causes Quinn's `Drop` implementation to emit a `STOP_SENDING(0)` frame, which causes the remote sender's subsequent writes or flushes to fail with error code 0.
- **Solution**:
  - Always drive `poll_shutdown` / `finish()` to completion on stream writers before letting the stream drop.
  - In relay pumps, ensure both half-close directions drain buffered data to completion before tearing down stream handles.
- **References**:
  - Implementation: [stream.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/stream.rs#L127-L143)

### 3.3 Server Accept Loop Teardown Kills In-Flight Accepted Streams
- **Problem / Trap**: On the server side (`H3Listener`), if the request acceptance loop exits (e.g. on cancellation or transient accept error), returning from the handler task drops `h3_conn` and `conn: quinn::Connection`. This terminates the entire QUIC connection, killing all previously accepted streams that are still relaying in separate spawned tasks!
- **Solution**:
  - In `handle_h3_streams`, after the accept loop finishes, hold the connection open until all accepted request setup tasks drain and the peer actually terminates the connection:
    ```rust
    let _ = conn.closed().cancel_with(&cancel_token).await;
    ```
- **References**:
  - Implementation: [listener.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/listener.rs#L528-L531)

### 3.4 Persistent SendRequest Anchoring & `H3_STREAM_CREATION_ERROR`
- **Problem / Trap**: In the `h3` crate, reusing an existing raw QUIC connection to negotiate an HTTP/3 handshake triggers `H3_STREAM_CREATION_ERROR` due to control stream and QPACK stream ID collisions.
- **Solution**:
  - Keep a persistent single H3 session (`H3Session`) per endpoint, holding an anchor `SendRequest` clone alongside the underlying `quinn::Connection` and a driver liveness flag.
  - When establishing a new H3 session, always dial a dedicated fresh QUIC connection with ALPN `h3`.
  - Use compare-and-clear (`invalidate_h3_session_if_same(session_id)`) on reconnect to prevent lost updates or stomping over newer sessions dialed concurrently.
- **References**:
  - Implementation: [connector.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/connector.rs)

### 3.5 ALPN Strict Routing & TLS Mode Segregation
- **Problem / Trap**: A single server endpoint accepting both HTTP/3 and raw QUIC on UDP can misroute incoming connections if ALPN negotiation is ambiguous.
- **Solution**:
  - Enforce explicit ALPN differentiation: `h3` for HTTP/3 CONNECT tunnels, and `culvert-quic` for raw bidirectional QUIC byte tunnels.
  - Raw QUIC listeners compare binary pre-shared tokens in constant time (`common::auth::constant_time_token_eq`) over length-prefixed MessagePack metadata frames, avoiding HTTP header parsing overhead and length leakages.
- **References**:
  - Architecture: [DESIGN.md](file:///d:/githome/spritetong/micro-tunnel/docs/DESIGN.md#L454-L493)

### 3.6 Zombie Session Reconnect & Fast Failover Bounds
- **Problem / Trap**: If a remote server process restarts or network routes reset without sending a QUIC `CONNECTION_CLOSE` frame, a cached QUIC connection becomes a "zombie". If the client waits for default transport timeouts (e.g. 30s) before marking the session invalid, visitor requests queue up and fail.
- **Solution**:
  - Invalidate cached H3/QUIC sessions unconditionally upon stream request/response timeouts or handshake errors without requiring `is_alive()` to return false.
  - Implement `QuicLivenessTracker` monitoring UDP datagram progress; if data is transmitted without acknowledgement for `DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD` (10s), signal connection unresponsiveness.
  - Clamp retry concurrency during failure recovery (limit `in_flight` to 1 probe) to prevent connection storms when a remote host is unreachable.
- **References**:
  - Implementation: [connector.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/connector.rs)
  - Integration Test: [test_h3_quic.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/tests/test_h3_quic.rs)

---

## 4. Speculative Connection Pooling & Fault Tolerance

### 4.1 Speculative Pooled Stream Activation & First-Write Retry
- **Problem / Trap**: Pre-warmed idle streams in a pool are inherently speculative over WANs. Even with 1-second background heartbeat sweeps, a stream can be reset by a middlebox or remote peer in the millisecond window between `acquire()` and writing the activation header (`0xA5` / `0xA7`).
- **Symptom**: The first visitor connection arriving after an extended idle period occasionally experiences an immediate disconnect (`BrokenPipe` or `ConnectionReset`).
- **Solution**:
  - In client forward mapping (`crates/culvert/src/tunnel/forward.rs`), wrap pooled stream acquisition and activation in a 1-attempt retry loop (`attempts < max_attempts`, where `max_attempts = 2` when pooling is enabled).
  - If a pooled stream fails during target activation or initial write, do not drop the visitor; immediately fall back to dialing a fresh dedicated stream and retry activation.
- **References**:
  - Implementation: [forward.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/tunnel/forward.rs#L111-L168)

### 4.2 Reverse 0-RTT Pool Fallback to Slow-Path Control Signaling
- **Problem / Trap**: In reverse tunnel mode, the server maintains pre-warmed streams from clients to achieve 0-RTT visitor dispatch. If a pre-warmed stream becomes stale or resets during activation, simply dropping the stream aborts the visitor connection.
- **Solution**:
  - In `crates/culvert/src/server/runner.rs`, if 0-RTT pool stream activation fails or times out:
    1. Discard the failed candidate stream.
    2. Fall back to slow-path signaling over the reverse control channel by emitting `ControlMessage::NewConnection { session_id, client_addr }`.
    3. Wait up to `DEFAULT_SESSION_PAIR_TIMEOUT` (10s) for the client to establish a fresh `reverse-data` stream and pair with the visitor.
- **References**:
  - Implementation: [runner.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/server/runner.rs#L944-L979)

### 4.3 Lockless & Decoupled Active Stream Tracking
- **Problem / Trap**: Using mutex locks around connection pool inventory or coupling active relay throughput to pool replenishment capacity creates contention bottlenecks and thread starvation under high concurrency.
- **Solution**:
  - Decouple active stream tracking using an atomic counter (`Arc<AtomicUsize>`) wrapped in an RAII `ActiveStreamGuard`.
  - When an acquired stream closes (normally or via panic/cancellation), `ActiveStreamGuard::drop` atomically decrements the counter and sends an `ActiveReleased` hint to wake the pool actor.
  - Active relay throughput is completely unbounded by replenishment rate limits.
- **References**:
  - Implementation: [connection_pool.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/pool/connection_pool.rs)

---

## 5. Async Concurrency & Probe-Driven Verification

### 5.1 Cancellation-Safety in Accept Loops (`ReusableFutureRef`)
- **Problem / Trap**: In `tokio::select!` loops, invoking `.accept().await` directly inside a branch cancels and drops the accept future whenever another branch (e.g. cancellation token or timer) completes first. Dropping an in-flight QUIC stream accept future drops the TLS handshake state and orphans the incoming connection.
- **Solution**:
  - Store accept futures in persistent `reusable_future::ReusableFutureRef` slots allocated outside the select loop.
  - In each loop iteration, poll `&mut slot`. If another select branch fires, the in-flight accept future remains safely preserved in its slot across iterations.
- **References**:
  - Implementation: [listener.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/listener.rs)

### 5.2 Probe-Driven Testing Methodology for Concurrency & Protocol Bugs
- **Problem / Trap**: Relying solely on happy-path integration tests or passive end-to-end tests fails to detect race conditions because retries or fast networks mask the timing window.
- **Methodology**: Construct dedicated probe tests that deterministically reproduce pathological states:
  1. **Blindspot Probe**: Intentionally close the remote stream half while keeping the underlying QUIC connection open; assert that `poll_check_alive` returns `Dead` rather than `Alive`.
  2. **Kill-Race Probe**: Populate an idle pool with multiple sibling streams sharing one multiplexed session; force session invalidation on the first acquire, and verify that subsequent acquires do not return dead sibling streams.
  3. **Flow Control HoL Probe**: Saturate multiple sibling streams with tens of megabytes of unread data; verify that a newly opened probe stream on the same connection completes prompt transfers without timing out.
- **References**:
  - Probe Tests: [test_idle_probe.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/tests/test_idle_probe.rs), [test_quic_flow_control_hol.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/tests/test_quic_flow_control_hol.rs)

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
  - Implementation: [manager.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/session/manager.rs) (`PendingSessionGuard`, `ReversePoolGuard` type aliases using `scopeguard`), [channel.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/control/channel.rs) (`scopeguard::defer!`), [forward.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/tunnel/forward.rs) (`scopeguard::defer!`), [runner.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/server/runner.rs)
  - Integration Tests: [runner.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/server/runner.rs#L1934-L2060) (`test_reverse_control_cancels_promptly_during_stalled_visitor_signaling`, `test_reverse_control_future_abort_cleans_up_resources`)

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
  - Implementation: [listener.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/listener.rs) (`request_tasks.spawn` timeout_or_cancel, `accept` cancel-aware lock), [session.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/mux/session.rs) (writer flush timeout, `accept_stream` cancellation), [stream.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/stream.rs) (`H3TunnelStream` Drop aborts background pump tasks), [uds.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/uds.rs), [ws/listener.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/ws/listener.rs), [udp.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/frontend/udp.rs)
  - Tests: [tests.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/mux/tests.rs#L398) (`test_mux_accept_stream_immediate_cancellation`), [stream.rs](file:///d:/githome/spritetong/micro-tunnel/crates/culvert/src/transport/h3/stream.rs#L713) (`test_h3_stream_drop_aborts_background_tasks`)
