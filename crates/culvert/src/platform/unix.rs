// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Unix-specific networking, IPC, and process signal handlers.
//! All comments and docstrings must be in English.

use std::io;
use std::path::{Path, PathBuf};
use tokio::net::{UnixListener, UnixStream};

use crate::core::error::TunnelError;
use crate::core::stream::{BufferedTlsStream, TunnelStream};

/// Waits for shutdown signal (SIGINT or SIGTERM on Unix).
pub async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::error!("Failed to install SIGINT handler: {:?}", e);
            None
        }
    };
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::error!("Failed to install SIGTERM handler: {:?}", e);
            None
        }
    };

    // A failed registration must never look like a received signal: that would
    // shut the daemon down immediately (or force-exit on the re-armed waiter).
    // Park that branch forever and keep listening on the other one.
    tokio::select! {
        _ = async {
            match sigint.as_mut() {
                Some(s) => { let _ = s.recv().await; }
                None => std::future::pending::<()>().await,
            }
        } => {
            tracing::info!("Received SIGINT signal. Initiating graceful shutdown...");
        }
        _ = async {
            match sigterm.as_mut() {
                Some(s) => { let _ = s.recv().await; }
                None => std::future::pending::<()>().await,
            }
        } => {
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

/// Returns the filesystem identity of a socket file, used to make `Drop`
/// unlink only the file that this listener actually created.
fn socket_identity(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.dev(), meta.ino()))
}

/// Unix Domain Socket listener.
pub struct PlatformUdsListener {
    listener: UnixListener,
    path: PathBuf,
    identity: Option<(u64, u64)>,
}

impl PlatformUdsListener {
    /// Binds a Unix domain socket at the specified path.
    pub fn bind(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
        }

        // Never blindly unlink: a second listen spec pointing at the same path
        // used to steal the socket file from a live listener. Only a stale
        // socket (one that refuses connections) may be removed.
        if path.exists() {
            let in_use = std::os::unix::net::UnixStream::connect(path).is_ok();
            if in_use {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("Unix socket is already in use: {}", path.display()),
                ));
            }
            std::fs::remove_file(path)?;
        }

        let listener = UnixListener::bind(path)?;
        let identity = socket_identity(path);
        Ok(Self {
            listener,
            path: path.to_path_buf(),
            identity,
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
        // Do not delete a socket file that was replaced by someone else after
        // we bound it; that would unlink a still-live listener.
        match self.identity {
            Some(expected) if socket_identity(&self.path) != Some(expected) => {}
            _ => {
                let _ = std::fs::remove_file(&self.path);
            }
        }
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
