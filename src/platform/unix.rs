// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Unix-specific networking, IPC, and process signal handlers.
//! All comments and docstrings must be in English.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};

use crate::core::error::TunnelError;
use crate::core::stream::{BufferedTlsStream, TunnelStream};

/// Waits for shutdown signal (SIGINT or SIGTERM on Unix).
pub async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to install SIGINT handler: {:?}", e);
            return;
        }
    };
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to install SIGTERM handler: {:?}", e);
            return;
        }
    };

    tokio::select! {
        _ = sigint.recv() => {
            tracing::info!("Received SIGINT signal. Initiating graceful shutdown...");
        }
        _ = sigterm.recv() => {
            tracing::info!("Received SIGTERM signal. Initiating graceful shutdown...");
        }
    }
}

/// Validates interface binding configuration. Supported on Linux only.
pub fn validate_bind_interface(iface: &str) -> Result<(), TunnelError> {
    #[cfg(target_os = "linux")]
    {
        let _ = iface;
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(TunnelError::Config(format!(
            "bind_interface '{}' is only supported on Linux (SO_BINDTODEVICE)",
            iface
        )))
    }
}

/// Binds a socket to a network interface via SO_BINDTODEVICE (Linux only).
pub fn bind_device(sock_ref: &socket2::SockRef, iface: &str) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        sock_ref.bind_device(Some(iface.as_bytes()))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (sock_ref, iface);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("bind_device '{}' is only supported on Linux", iface),
        ))
    }
}

/// Configures TCP keepalive interval on Unix.
pub fn configure_tcp_keepalive(ka: socket2::TcpKeepalive) -> socket2::TcpKeepalive {
    ka.with_interval(Duration::from_secs(5))
}

/// Unix Domain Socket listener.
pub struct PlatformUdsListener {
    listener: UnixListener,
    path: PathBuf,
}

impl PlatformUdsListener {
    /// Binds a Unix domain socket at the specified path.
    pub fn bind(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::remove_file(path);

        let listener = UnixListener::bind(path)?;
        Ok(Self {
            listener,
            path: path.to_path_buf(),
        })
    }

    /// Asynchronously accepts an incoming connection from the Unix domain socket.
    pub async fn accept(&mut self) -> io::Result<(Box<dyn TunnelStream>, String)> {
        let (stream, _addr) = self.listener.accept().await?;
        let boxed: Box<dyn TunnelStream> = Box::new(BufferedTlsStream::new(stream));
        Ok((boxed, "uds".to_string()))
    }
}

impl Drop for PlatformUdsListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Connects to a Unix Domain Socket at the specified path.
pub async fn connect_uds(path: &Path) -> io::Result<Box<dyn TunnelStream>> {
    let stream = UnixStream::connect(path).await?;
    let boxed: Box<dyn TunnelStream> = Box::new(BufferedTlsStream::new(stream));
    Ok(boxed)
}

/// Sets filesystem permissions mode on a Unix socket or file.
pub fn set_socket_permissions(path: &Path, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = std::fs::Permissions::from_mode(mode);
    std::fs::set_permissions(path, perms)
}
