// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Common networking utilities and socket tuning helpers.
//! All comments and docstrings must be in English.

use socket2::{SockRef, TcpKeepalive};
use std::io;
use std::time::Duration;
use tokio::net::TcpStream;

use crate::consts::DEFAULT_TCP_KEEPALIVE;

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
        let keepalive_time = keepalive.unwrap_or(DEFAULT_TCP_KEEPALIVE);
        let ka = TcpKeepalive::new().with_time(keepalive_time);
        let ka = crate::platform::configure_tcp_keepalive(ka);

        let _ = sock_ref.set_tcp_keepalive(&ka);
        Ok(())
    }
}
