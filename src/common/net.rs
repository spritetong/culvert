//! Common networking utilities and socket tuning helpers.
//! All comments and docstrings must be in English.

use std::io;
use std::time::Duration;
use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;

/// Socket configuration helper.
pub struct SocketTuner;

impl SocketTuner {
    /// Applies recommended TCP socket performance parameters (TCP_NODELAY + TCP_KEEPALIVE).
    pub fn tune_tcp_stream(
        stream: &TcpStream,
        nodelay: bool,
        keepalive: Option<Duration>,
    ) -> io::Result<()> {
        if nodelay {
            stream.set_nodelay(true)?;
        }

        let sock_ref = SockRef::from(stream);
        let keepalive_time = keepalive.unwrap_or(Duration::from_secs(15));
        #[allow(unused_mut)]
        let mut ka = TcpKeepalive::new().with_time(keepalive_time);

        #[cfg(not(target_os = "windows"))]
        {
            ka = ka.with_interval(Duration::from_secs(5));
        }

        let _ = sock_ref.set_tcp_keepalive(&ka);
        Ok(())
    }
}
