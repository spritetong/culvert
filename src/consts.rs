//! Centralized constants, defaults, and configuration keys.
//! All comments and docstrings must be in English.

use std::time::Duration;

/// Default buffer size for high-throughput zero-copy bidirectional relay (64 KB).
pub const RELAY_BUFFER_SIZE: usize = 65536;

/// Default timeout for outbound connection dialing.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default TCP keepalive probe interval.
pub const DEFAULT_TCP_KEEPALIVE: Duration = Duration::from_secs(15);

/// Default heartbeat interval for idle connection pool and control streams.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Default timeout to wait for a reverse data stream pairing handshake.
pub const DEFAULT_SESSION_PAIR_TIMEOUT: Duration = Duration::from_secs(5);

/// Capacity of the ring buffer used in the seamless migration failover stream (512 KB).
pub const DEFAULT_FAILOVER_RING_BUFFER_CAPACITY: usize = 524288;

/// Default DNS cache entry count.
pub const DEFAULT_DNS_CACHE_CAPACITY: usize = 1024;

/// Default DNS time-to-live cache duration.
pub const DEFAULT_DNS_CACHE_TTL: Duration = Duration::from_secs(300);

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

/// Value for forward mode in header.
pub const TUNNEL_MODE_FORWARD: &str = "forward";

/// Value for reverse control stream mode in header.
pub const TUNNEL_MODE_REVERSE_CTRL: &str = "reverse-ctrl";

/// Value for reverse data stream mode in header.
pub const TUNNEL_MODE_REVERSE_DATA: &str = "reverse-data";
