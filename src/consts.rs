// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Centralized constants, defaults, and configuration parameters across all modules.
//! All comments and docstrings must be in English.

use std::time::Duration;

// =============================================================================
// 1. Buffer Sizes, Queue Capacities & Framing Limits
// =============================================================================

/// Buffer size for high-throughput zero-copy bidirectional byte relay (64 KB).
pub const RELAY_BUFFER_SIZE: usize = 65536;

/// Capacity of the ring buffer used in the seamless migration failover stream (512 KB).
pub const DEFAULT_FAILOVER_RING_BUFFER_CAPACITY: usize = 524288;

/// Capacity of the accepted-stream channel shared by ingress listeners.
pub const ACCEPTED_STREAM_QUEUE_SIZE: usize = 128;

/// Capacity of the incoming connection backlog channel for Windows UDS listener.
pub const WINDOWS_UDS_ACCEPT_QUEUE_SIZE: usize = 32;

/// Command channel capacity for the replenishing connection pool actor.
pub const POOL_COMMAND_QUEUE_SIZE: usize = 1024;

/// Hard upper bound on pending WebSocket control frames (Pong/Close) to prevent memory attacks.
pub const WS_MAX_CONTROL_QUEUE: usize = 64;

/// Queue capacity for HTTP/3 stream outbound/inbound chunk pump channels.
pub const H3_STREAM_CHUNK_QUEUE_SIZE: usize = 128;

/// Default inbound stream channel buffer size per logical multiplex stream.
pub const MUX_STREAM_BUFFER_SIZE: usize = 128;

/// Default outbound frame channel queue capacity in a multiplex session.
pub const MUX_SESSION_FRAME_QUEUE_SIZE: usize = 256;

/// Queue capacity for newly accepted logical streams in a multiplex session.
pub const MUX_NEW_STREAM_QUEUE_SIZE: usize = 64;

/// Maximum payload size per multiplex frame (64 KB) to ensure fair scheduling between streams.
pub const MAX_FRAME_PAYLOAD_SIZE: usize = 64 * 1024;

/// Frame header length: 1 byte type + 4 bytes stream_id + 4 bytes payload_len = 9 bytes.
pub const FRAME_HEADER_LEN: usize = 9;

/// Maximum byte length for raw QUIC authentication token payload in inbound handshake.
pub const RAW_QUIC_MAX_AUTH_LEN: usize = 1024;

/// Maximum byte length for raw QUIC metadata payload in inbound handshake.
pub const RAW_QUIC_MAX_META_LEN: usize = 65536;

/// Maximum byte length for dynamic target address in pre-warmed pool stream activation packet.
pub const MAX_ACTIVATION_TARGET_LEN: usize = 512;

/// Maximum supported token length in bytes for constant-time comparison.
pub const TOKEN_MAX_LEN: usize = 256;

// =============================================================================
// 2. Timeouts, Deadlines & Grace Periods
// =============================================================================

/// Default timeout for outbound connection dialing.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound for graceful shutdown. Once exceeded, the daemon exits hard.
pub const DEFAULT_SHUTDOWN_HARD_DEADLINE: Duration = Duration::from_secs(10);

/// Graceful shutdown grace period shared by nested task trackers.
pub const DEFAULT_GRACE_PERIOD: Duration = Duration::from_secs(5);

/// Default timeout to wait for a reverse data stream pairing handshake.
pub const DEFAULT_SESSION_PAIR_TIMEOUT: Duration = Duration::from_secs(5);

/// Standard timeout for TLS accept, WebSocket upgrade, and QUIC/H3 inbound handshakes.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Timeout for reading the activation packet and dynamic target payload on the server.
pub const DEFAULT_ACTIVATION_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounded drain period to flush in-flight relay tails on cancellation.
pub const DEFAULT_RELAY_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);

/// Timeout for finishing an HTTP/3 send stream upon shutdown.
pub const DEFAULT_H3_FINISH_STREAM_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for draining in-flight HTTP/3 request setup tasks during listener shutdown.
pub const DEFAULT_H3_REQUEST_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum timeout waiting to acquire an idle connection from the connection pool.
pub const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(15);

/// Timeout for ping frame acknowledgement or liveness probe completion in the connection pool.
pub const POOL_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);

// =============================================================================
// 3. Heartbeats, Intervals & Backoff Parameters
// =============================================================================

/// Default TCP keepalive idle time before initial probe.
pub const DEFAULT_TCP_KEEPALIVE: Duration = Duration::from_secs(15);

/// TCP keepalive probe interval on Unix platforms.
pub const DEFAULT_TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// Default heartbeat interval for idle connection pool and control streams.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Idle connection pool heartbeat inspection interval.
pub const POOL_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Ticker interval driving periodic heartbeat sweeps and timeout pruning in the connection pool.
pub const POOL_HEARTBEAT_TICK_INTERVAL: Duration = Duration::from_secs(1);

/// Default retry delay between reconnect attempts for tunnel supervisor workers.
pub const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(3);

/// RFC 8305 Happy Eyeballs connection attempt delay when racing dual-stack IPv6 and IPv4 addresses.
pub const DEFAULT_HAPPY_EYEBALLS_DELAY: Duration = Duration::from_millis(250);

/// Initial backoff delay for reverse tunnel control channel reconnection.
pub const DEFAULT_REVERSE_BACKOFF_INITIAL: Duration = Duration::from_millis(500);

/// Maximum backoff delay for reverse tunnel control channel reconnection.
pub const DEFAULT_REVERSE_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Operating duration after which the reverse tunnel backoff delay resets back to initial.
pub const REVERSE_BACKOFF_RESET_THRESHOLD: Duration = Duration::from_secs(30);

/// Initial backoff delay for connection pool background replenishment on failure.
pub const POOL_BACKOFF_BASE: Duration = Duration::from_millis(200);

/// Maximum backoff delay for connection pool background replenishment.
pub const POOL_BACKOFF_MAX: Duration = Duration::from_secs(10);

/// Backoff cap for connection pool replenishment when caller requests are actively waiting.
pub const POOL_BACKOFF_ACTIVE_WAITERS_MAX: Duration = Duration::from_secs(1);

/// Fast-fail cooldown period suppressing immediate reconnect retry storms after H3/QUIC dial failure.
pub const H3_FAILURE_SUPPRESSION_COOLDOWN: Duration = Duration::from_millis(500);

/// Periodic garbage collection interval for unregistering dropped multiplex streams.
pub const MUX_GC_INTERVAL: Duration = Duration::from_secs(5);

// =============================================================================
// 4. Connection Pool Capacity & Host Parameters
// =============================================================================

/// Maximum number of concurrent outbound dialing futures in the connection pool.
pub const POOL_MAX_CONCURRENT_CONNECTS: usize = 4;

/// Upper bound on the total allocated streams (idle + active) in the connection pool.
pub const POOL_MAX_CAPACITY: usize = 16;

/// Dummy host domain used for pre-warmed connections awaiting dynamic target specification.
pub const POOL_DUMMY_HOST: &str = "pool.idle";

// =============================================================================
// 5. QUIC & HTTP/3 Transport Parameters & Flow Control Windows
// =============================================================================

/// Quinn transport max idle timeout.
pub const DEFAULT_QUIC_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(8);

/// Quinn transport keep-alive probe interval.
pub const DEFAULT_QUIC_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(2);

/// Maximum concurrent bidirectional streams allowed on a QUIC connection.
pub const DEFAULT_QUIC_MAX_CONCURRENT_BIDI_STREAMS: u32 = 10_000;

/// Maximum concurrent unidirectional streams allowed on a QUIC connection.
pub const DEFAULT_QUIC_MAX_CONCURRENT_UNI_STREAMS: u32 = 10_000;

/// Flow control stream receive window (4 MiB).
/// Must remain modest relative to connection window to avoid head-of-line blocking stalled streams.
pub const DEFAULT_QUIC_STREAM_RECEIVE_WINDOW: u32 = 4 * 1024 * 1024;

/// Flow control connection receive window (128 MiB).
pub const DEFAULT_QUIC_RECEIVE_WINDOW: u32 = 128 * 1024 * 1024;

/// Flow control connection send window (128 MiB).
pub const DEFAULT_QUIC_SEND_WINDOW: u64 = 128 * 1024 * 1024;

/// Duration without received UDP datagrams while transmitting before a QUIC connection is deemed unresponsive.
pub const DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD: Duration = Duration::from_secs(4);

// =============================================================================
// 6. DNS Parameters & Default Network Ports
// =============================================================================

/// Default DNS cache entry count.
pub const DEFAULT_DNS_CACHE_CAPACITY: usize = 1024;

/// Default DNS time-to-live cache duration.
pub const DEFAULT_DNS_CACHE_TTL: Duration = Duration::from_secs(300);

/// Default port for HTTP and WebSocket traffic.
pub const DEFAULT_HTTP_PORT: u16 = 80;

/// Default port for HTTPS, WSS, H3, and QUIC traffic.
pub const DEFAULT_HTTPS_PORT: u16 = 443;

/// Default port for standard unencrypted UDP/TCP DNS.
pub const DEFAULT_DNS_PORT: u16 = 53;

/// Default port for DNS-over-TLS (DoT).
pub const DEFAULT_DOT_PORT: u16 = 853;

/// Default port for DNS-over-HTTPS (DoH).
pub const DEFAULT_DOH_PORT: u16 = 443;

// =============================================================================
// 7. Protocol Identifiers, ALPNs & URI Schemes
// =============================================================================

/// Application-Layer Protocol Negotiation (ALPN) identifier for HTTP/3 (bytes).
pub const ALPN_H3: &[u8] = b"h3";

/// ALPN identifier for HTTP/3 (str).
pub const ALPN_H3_STR: &str = "h3";

/// ALPN identifier for raw multiplexed QUIC streams (bytes).
pub const ALPN_QUIC: &[u8] = b"micro-tunnel-quic";

/// ALPN identifier for raw multiplexed QUIC streams (str).
pub const ALPN_QUIC_STR: &str = "micro-tunnel-quic";

/// URI scheme identifier for standard WebSocket.
pub const SCHEME_WS: &str = "ws";

/// URI scheme identifier for secure WebSocket over TLS.
pub const SCHEME_WSS: &str = "wss";

/// URI scheme identifier for HTTP/3 transport.
pub const SCHEME_H3: &str = "h3";

/// URI scheme identifier for raw QUIC transport.
pub const SCHEME_QUIC: &str = "quic";

/// URI scheme identifier for HTTPS transport.
pub const SCHEME_HTTPS: &str = "https";

/// URI scheme identifier for WebSocket over Unix Domain Sockets.
pub const SCHEME_WS_UNIX: &str = "ws+unix";

/// URI scheme identifier for secure WebSocket over Unix Domain Sockets.
pub const SCHEME_WSS_UNIX: &str = "wss+unix";

/// URI scheme prefix for raw Unix Domain Sockets.
pub const SCHEME_UNIX: &str = "unix";

/// Default URL path for HTTP/3 and WebSocket tunnel endpoints.
pub const DEFAULT_TUNNEL_PATH: &str = "/tunnel";

/// Target host tag used by reverse tunnel control streams.
pub const REVERSE_CONTROL_TARGET_HOST: &str = "control";

// =============================================================================
// 8. HTTP & WebSocket Headers and Signaling Constants
// =============================================================================

/// HTTP Header key for pre-shared token authentication.
pub const HEADER_TUNNEL_AUTH: &str = "x-tunnel-auth";

/// HTTP Header key for identifying the tunnel session ID.
pub const HEADER_SESSION_ID: &str = "x-session-id";

/// HTTP Header key for the tunnel operating mode.
pub const HEADER_TUNNEL_MODE: &str = "x-tunnel-mode";

/// HTTP Header key for dynamic target host forwarding.
pub const HEADER_TARGET_HOST: &str = "x-target-host";

/// HTTP Header key for dynamic target port forwarding.
pub const HEADER_TARGET_PORT: &str = "x-target-port";

/// HTTP Header key for dynamic target UDS path forwarding.
pub const HEADER_TARGET_PATH: &str = "x-target-path";

/// HTTP Header key for indicating a pre-warmed idle pool connection.
pub const HEADER_TUNNEL_POOL: &str = "x-tunnel-pool";

/// HTTP Header key for unified dynamic target forwarding.
pub const HEADER_TUNNEL_TARGET: &str = "x-tunnel-target";

/// Standard WebSocket upgrade header: Sec-WebSocket-Key.
pub const HEADER_SEC_WEBSOCKET_KEY: &str = "Sec-WebSocket-Key";

/// Standard WebSocket upgrade header: Sec-WebSocket-Version.
pub const HEADER_SEC_WEBSOCKET_VERSION: &str = "Sec-WebSocket-Version";

/// Standard WebSocket protocol version 13 identifier.
pub const SEC_WEBSOCKET_VERSION_13: &str = "13";

/// Header value for WebSocket upgrade.
pub const HEADER_VALUE_WEBSOCKET: &str = "websocket";

/// Header value for upgrade connection.
pub const HEADER_VALUE_UPGRADE: &str = "upgrade";

/// Header value for standard authorization header prefix.
pub const AUTH_BEARER_PREFIX: &str = "Bearer ";

/// Value for forward mode in header.
pub const TUNNEL_MODE_FORWARD: &str = "forward";

/// Value for reverse control stream mode in header.
pub const TUNNEL_MODE_REVERSE_CTRL: &str = "reverse-ctrl";

/// Value for reverse data stream mode in header.
pub const TUNNEL_MODE_REVERSE_DATA: &str = "reverse-data";

/// Generic reverse tunnel mode identifier in header.
pub const TUNNEL_MODE_REVERSE: &str = "reverse";

/// Value for pre-warmed idle pool connection in header.
pub const TUNNEL_POOL_IDLE: &str = "idle";

/// Activation magic byte sent over pre-warmed pool streams to trigger target dialing.
pub const ACTIVATE_STREAM_MAGIC: u8 = 0xA5;

// =============================================================================
// 9. Process, CLI & Diagnostics Defaults
// =============================================================================

/// Default path for the configuration file.
pub const DEFAULT_CONFIG_PATH: &str = "config.yaml";

/// Default log level for structured logging.
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// Default log output sink.
pub const DEFAULT_LOG_OUTPUT: &str = "stdout";

/// Thread name for the OS-thread graceful shutdown watchdog.
pub const WATCHDOG_THREAD_NAME: &str = "shutdown-watchdog";

/// Thread name for the Windows UDS accept worker thread.
pub const WINDOWS_UDS_THREAD_NAME: &str = "micro-tunnel-windows-uds";
