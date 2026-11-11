// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

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

/// Default capacity for the command deduplication sliding window across dual channels.
pub const DEFAULT_DEDUPLICATION_WINDOW_CAPACITY: usize = 256;

// =============================================================================
// 2. Timeouts, Deadlines & Grace Periods
// =============================================================================

/// Default timeout for outbound connection dialing.
///
/// 10 seconds provides adequate headroom for dual-stack Happy Eyeballs races (RFC 8305)
/// and up to 2-3 TCP SYN retransmissions on lossy/cellular weak networks before failing,
/// avoiding premature connection teardown while remaining responsive.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound for graceful shutdown. Once exceeded, the daemon exits hard.
pub const DEFAULT_SHUTDOWN_HARD_DEADLINE: Duration = Duration::from_secs(10);

/// Graceful shutdown grace period shared by nested task trackers.
pub const DEFAULT_GRACE_PERIOD: Duration = Duration::from_secs(5);

/// Default timeout to wait for a reverse data stream pairing handshake.
///
/// 15 seconds accommodates WAN latency, TLS/QUIC handshake roundtrips, and transient
/// packet retransmissions when a NATed client establishes a paired reverse data channel.
pub const DEFAULT_SESSION_PAIR_TIMEOUT: Duration = Duration::from_secs(15);

/// Standard timeout for TLS accept, WebSocket upgrade, and QUIC/H3 inbound handshakes.
///
/// 15 seconds ensures multi-roundtrip handshakes (TCP SYN + TLS 1.3 + HTTP/1.1 101 Upgrade)
/// do not abort prematurely under jittery or high-RTT weak network conditions.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// Timeout for reading the activation packet and dynamic target payload on the server.
pub const DEFAULT_ACTIVATION_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounded drain period to flush in-flight relay tails on cancellation.
///
/// 500 milliseconds covers 1-2 cross-border round-trip times (RTT) to ensure trailing
/// FIN / close frames and buffered bytes flush properly before hard abort.
pub const DEFAULT_RELAY_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);

/// Timeout for finishing an HTTP/3 send stream upon shutdown.
pub const DEFAULT_H3_FINISH_STREAM_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for draining in-flight HTTP/3 request setup tasks during listener shutdown.
pub const DEFAULT_H3_REQUEST_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum timeout waiting to acquire an idle connection from the connection pool.
pub const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(15);

/// Timeout for ping frame acknowledgement or liveness probe completion in the connection pool.
///
/// 5 seconds avoids false-positive dead-connection classification caused by temporary
/// packet loss or TCP/QUIC retransmission delay under weak network conditions.
pub const POOL_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// Grace period retention window for reverse listener sockets when the control channel drops.
///
/// 10 seconds retains the bound port while an emergency promoted stream attaches to resume signaling.
pub const DEFAULT_REVERSE_GRACE_PERIOD: Duration = Duration::from_secs(10);

// =============================================================================
// 3. Heartbeats, Intervals & Backoff Parameters
// =============================================================================

/// Default heartbeat interval for idle connection pool and control streams.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Default idle timeout before terminating silent or unresponsive control and transport sessions.
///
/// Under weak network conditions, 30 seconds covers temporary radio stalls and handover drops
/// while ensuring dead connections are detected in bounded time (2x DEFAULT_HEARTBEAT_INTERVAL).
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

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
pub const POOL_MAX_CONCURRENT_CONNECTS: usize = 16;

/// Upper bound on the total allocated streams (idle + active) in the connection pool.
pub const POOL_MAX_CAPACITY: usize = 64;

/// Dummy host domain used for pre-warmed connections awaiting dynamic target specification.
pub const POOL_DUMMY_HOST: &str = "pool.idle";

/// Clamps a configured reverse pool size to the worker bound shared by both ends.
///
/// `None` (or zero) means the client does not pre-warm at all. The clamped value
/// is what the client actually spawns and what it sends in `RegisterReverse`, so
/// the server never has to guess a second, divergent capacity.
pub fn clamp_reverse_pool_size(configured: Option<usize>) -> Option<usize> {
    configured
        .filter(|&size| size > 0)
        .map(|size| size.min(POOL_MAX_CAPACITY))
}

/// Soft upper bound on one reverse listener's idle queue, derived from the
/// negotiated worker count. Twice the worker count tolerates one overlapping
/// reconnect generation without an absolute, hard reservation.
pub fn reverse_pool_soft_cap(size: Option<usize>) -> usize {
    size.unwrap_or(POOL_MAX_CAPACITY).saturating_mul(2)
}

/// Deadline for one activation byte or one `NewConnection` frame on the reverse
/// path. Derived from the effective idle window instead of a LAN-scale constant:
/// a high-RTT / low-bandwidth link inherits the operator's timing, while the
/// weak-network probe allowance and the pairing deadline keep it bounded.
pub fn reverse_frame_timeout(idle_timeout: Duration) -> Duration {
    (idle_timeout / 3)
        .clamp(POOL_HEARTBEAT_TIMEOUT, DEFAULT_SESSION_PAIR_TIMEOUT)
        .min(idle_timeout)
}

// =============================================================================
// 5. QUIC & HTTP/3 Transport Parameters & Flow Control Windows
// =============================================================================

/// Quinn transport max idle timeout.
///
/// RFC 9000 Section 10.1 recommends at least 30 seconds for max idle timeout.
/// 30 seconds protects against premature disconnects during cellular handover or Wi-Fi roaming.
pub const DEFAULT_QUIC_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Quinn transport keep-alive probe interval.
///
/// Probing every 5 seconds keeps stateful NAT / firewall UDP pinholes alive without
/// incurring excessive cellular battery or bandwidth overhead.
pub const DEFAULT_QUIC_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

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
///
/// 10 seconds prevents false-positive failover or disconnect triggers on high-jitter,
/// packet-lossy, or bufferbloated mobile/satellite connections.
pub const DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD: Duration = Duration::from_secs(10);

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
pub const ALPN_QUIC: &[u8] = b"culvert-quic";

/// ALPN identifier for raw multiplexed QUIC streams (str).
pub const ALPN_QUIC_STR: &str = "culvert-quic";

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

/// Activation magic byte sent over pre-warmed pool streams to promote the stream into a signaling channel.
pub const ACTIVATE_STREAM_PROMOTE: u8 = 0xA6;

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
pub const WINDOWS_UDS_THREAD_NAME: &str = "culvert-windows-uds";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reverse_pool_size_is_clamped_once_and_shared() {
        assert_eq!(clamp_reverse_pool_size(None), None);
        assert_eq!(clamp_reverse_pool_size(Some(0)), None);
        assert_eq!(clamp_reverse_pool_size(Some(2)), Some(2));
        assert_eq!(clamp_reverse_pool_size(Some(1000)), Some(POOL_MAX_CAPACITY));
        assert_eq!(reverse_pool_soft_cap(Some(2)), 4);
        assert_eq!(reverse_pool_soft_cap(None), POOL_MAX_CAPACITY * 2);
    }

    #[test]
    fn test_reverse_frame_timeout_scales_with_idle_window() {
        // A default idle window yields 10 s, not a LAN-scale 2 s deadline.
        assert_eq!(
            reverse_frame_timeout(DEFAULT_IDLE_TIMEOUT),
            Duration::from_secs(10)
        );
        // A long, lossy WAN window stays bounded by the pairing deadline.
        assert_eq!(
            reverse_frame_timeout(Duration::from_secs(300)),
            DEFAULT_SESSION_PAIR_TIMEOUT
        );
        // A shorter configured window is never exceeded.
        assert_eq!(
            reverse_frame_timeout(Duration::from_secs(3)),
            Duration::from_secs(3)
        );
    }
}
