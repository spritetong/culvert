// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Common networking utilities and socket tuning helpers.
//! All comments and docstrings must be in English.

use std::io;
use tokio::net::TcpStream;

/// Socket configuration helper.
pub struct SocketTuner;

impl SocketTuner {
    /// Applies recommended TCP socket performance parameters (TCP_NODELAY).
    pub fn tune_tcp_stream(stream: &TcpStream, nodelay: bool) -> io::Result<()> {
        if nodelay {
            stream.set_nodelay(true)?;
        }
        Ok(())
    }
}
