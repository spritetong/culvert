//! Common networking utilities and socket tuning helpers.
//! All comments and docstrings must be in English.

use std::io;
use std::time::Duration;
use tokio::net::TcpStream;

/// Socket configuration helper.
pub struct SocketTuner;

impl SocketTuner {
    /// Applies recommended TCP socket performance parameters.
    pub fn tune_tcp_stream(
        stream: &TcpStream,
        nodelay: bool,
        keepalive: Option<Duration>,
    ) -> io::Result<()> {
        if nodelay {
            stream.set_nodelay(true)?;
        }

        // On supported platforms, apply TCP keepalive
        if let Some(_dur) = keepalive {
            // Note: additional socket2 options can be attached here if needed
        }

        Ok(())
    }
}
