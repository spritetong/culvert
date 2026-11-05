// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Windows-specific networking, IPC, and process signal handlers.
//! All comments and docstrings must be in English.

use std::io;
use std::os::windows::io::{FromRawSocket, IntoRawSocket};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::net::TcpStream;

use crate::core::error::TunnelError;
use crate::core::stream::TunnelStream;

/// Waits for shutdown signal (Ctrl+C on Windows).
pub async fn wait_for_shutdown_signal() {
    if let Ok(()) = tokio::signal::ctrl_c().await {
        tracing::info!("Received termination signal (Ctrl+C). Initiating shutdown...");
    }
}

/// Validates interface binding configuration. SO_BINDTODEVICE is Linux only.
pub fn validate_bind_interface(iface: &str) -> Result<(), TunnelError> {
    Err(TunnelError::Config(format!(
        "bind_interface '{}' is only supported on Linux (SO_BINDTODEVICE)",
        iface
    )))
}

/// Binds a socket to a network interface. Unsupported on Windows.
pub fn bind_device(_sock_ref: &socket2::SockRef, iface: &str) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("bind_device '{}' is only supported on Linux", iface),
    ))
}

/// Configures TCP keepalive on Windows.
pub fn configure_tcp_keepalive(ka: socket2::TcpKeepalive) -> socket2::TcpKeepalive {
    ka
}

/// Cleans up a Windows Unix Domain Socket by unblocking any pending accept and deleting the file.
fn cleanup_windows_uds(p: &Path) {
    let _ = uds_windows::UnixStream::connect(p);
    let _ = std::fs::remove_file(p);
}

/// Windows Unix Domain Socket (AF_UNIX) listener.
pub struct PlatformUdsListener {
    rx: tokio::sync::mpsc::Receiver<uds_windows::UnixStream>,
    closed: Arc<AtomicBool>,
    path: PathBuf,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl PlatformUdsListener {
    /// Binds an AF_UNIX socket at the specified path on Windows.
    pub fn bind(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::remove_file(path);

        let listener = uds_windows::UnixListener::bind(path)?;
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let closed = Arc::new(AtomicBool::new(false));

        let l = Arc::new(listener);
        let closed_clone = closed.clone();
        let thread = std::thread::Builder::new()
            .name("micro-tunnel-windows-uds".to_string())
            .spawn(move || {
                while !closed_clone.load(Ordering::SeqCst) {
                    match l.accept() {
                        Ok((stream, _)) => {
                            if closed_clone.load(Ordering::SeqCst) {
                                break;
                            }
                            if tx.blocking_send(stream).is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            })?;

        Ok(Self {
            rx,
            closed,
            path: path.to_path_buf(),
            thread: Some(thread),
        })
    }

    /// Asynchronously accepts an incoming connection from the Windows AF_UNIX listener.
    pub async fn accept(&mut self) -> io::Result<(Box<dyn TunnelStream>, String)> {
        let std_stream = self
            .rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Interrupted, "Listener closed"))?;

        let raw = std_stream.into_raw_socket();
        let std_tcp = unsafe { std::net::TcpStream::from_raw_socket(raw) };
        std_tcp.set_nonblocking(true)?;
        let async_stream = TcpStream::from_std(std_tcp)?;
        Ok((Box::new(async_stream), "uds".to_string()))
    }
}

impl Drop for PlatformUdsListener {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        cleanup_windows_uds(&self.path);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

/// Connects to a Windows Unix Domain Socket (AF_UNIX) and returns an asynchronous stream.
pub async fn connect_uds(path: &Path) -> io::Result<Box<dyn TunnelStream>> {
    let std_stream = uds_windows::UnixStream::connect(path)?;
    let raw = std_stream.into_raw_socket();
    let std_tcp = unsafe { std::net::TcpStream::from_raw_socket(raw) };
    std_tcp.set_nonblocking(true)?;
    let stream = TcpStream::from_std(std_tcp)?;
    Ok(Box::new(stream))
}

/// Sets filesystem permissions mode on a socket (no-op on Windows).
pub fn set_socket_permissions(_path: &Path, _mode: u32) -> io::Result<()> {
    Ok(())
}
