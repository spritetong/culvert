// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Windows-specific networking, IPC, and process signal handlers.
//! All comments and docstrings must be in English.

use std::io;
use std::os::windows::io::{AsRawSocket, FromRawSocket, IntoRawSocket};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::TcpStream;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    FD_ACCEPT, FD_ACCEPT_BIT, SOCKET, WSA_INFINITE, WSA_INVALID_EVENT, WSA_WAIT_EVENT_0,
    WSA_WAIT_FAILED, WSACloseEvent, WSACreateEvent, WSAEVENT, WSAEnumNetworkEvents, WSAEventSelect,
    WSAGetLastError, WSANETWORKEVENTS, WSASetEvent, WSAWaitForMultipleEvents,
};

use crate::core::error::TunnelError;
use crate::core::stream::TunnelStream;

/// Maps the last Winsock error into an `io::Error`.
fn wsa_last_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}

/// Waits for shutdown signal (Ctrl+C on Windows).
pub async fn wait_for_shutdown_signal() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            tracing::info!("Received termination signal (Ctrl+C). Initiating shutdown...");
        }
        Err(e) => {
            // Never mistake a handler failure for a received signal.
            tracing::error!("Failed to install Ctrl+C handler: {:?}", e);
            std::future::pending::<()>().await;
        }
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

/// Windows Unix Domain Socket (AF_UNIX) listener.
pub struct PlatformUdsListener {
    /// `Option` so `Drop` can close the receiver *before* joining the accept
    /// thread: a `blocking_send` parked on a full queue is then released with an
    /// error instead of keeping the thread alive forever.
    rx: Option<tokio::sync::mpsc::Receiver<uds_windows::UnixStream>>,
    /// Manual-reset cancel event owned by this struct. The accept thread only
    /// borrows its handle value and never closes it, so `WSASetEvent` in `Drop`
    /// remains valid even if the thread has already exited on its own.
    cancel_event: WSAEVENT,
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
        let sock = AsRawSocket::as_raw_socket(&listener) as SOCKET;

        // Two Winsock events: FD_ACCEPT for the listener, plus a manual-reset
        // cancel event signalled by Drop. WSAEventSelect binds FD_ACCEPT and
        // switches the socket to nonblocking mode by itself.
        //
        // This is Winsock2 event selection, not AcceptEx: afunix.sys has no
        // overlapped/IOCP accept support, so AcceptEx fails with WSAEINVAL /
        // WSAEOPNOTSUPP. Plain accept() after an FD_ACCEPT event is supported.
        let cancel_event = unsafe { WSACreateEvent() };
        if cancel_event == WSA_INVALID_EVENT {
            return Err(wsa_last_error());
        }
        let socket_event = unsafe { WSACreateEvent() };
        if socket_event == WSA_INVALID_EVENT {
            let err = wsa_last_error();
            unsafe { WSACloseEvent(cancel_event) };
            return Err(err);
        }
        if unsafe { WSAEventSelect(sock, socket_event, FD_ACCEPT as i32) } != 0 {
            let err = wsa_last_error();
            unsafe {
                WSACloseEvent(socket_event);
                WSACloseEvent(cancel_event);
            }
            return Err(err);
        }

        let (tx, rx) = tokio::sync::mpsc::channel(crate::consts::WINDOWS_UDS_ACCEPT_QUEUE_SIZE);
        let l = Arc::new(listener);
        let thread = match std::thread::Builder::new()
            .name(crate::consts::WINDOWS_UDS_THREAD_NAME.to_string())
            .spawn(move || {
                // Index 0 is the cancel event, so it wins when both are signalled.
                let wait_handles: [HANDLE; 2] = [cancel_event as HANDLE, socket_event as HANDLE];

                'accept: loop {
                    let waited = unsafe {
                        WSAWaitForMultipleEvents(2, wait_handles.as_ptr(), 0, WSA_INFINITE, 0)
                    };
                    if waited == WSA_WAIT_EVENT_0 as u32 || waited == WSA_WAIT_FAILED {
                        break;
                    }
                    if waited != (WSA_WAIT_EVENT_0 as u32) + 1 {
                        break;
                    }

                    // WSAEnumNetworkEvents resets the event and re-arms FD_ACCEPT.
                    let mut net_events = WSANETWORKEVENTS::default();
                    if unsafe { WSAEnumNetworkEvents(sock, socket_event, &mut net_events) } != 0 {
                        break;
                    }
                    if (net_events.lNetworkEvents & FD_ACCEPT as i32) == 0
                        || net_events.iErrorCode[FD_ACCEPT_BIT as usize] != 0
                    {
                        continue;
                    }

                    // FD_ACCEPT is only re-posted after accept() is called, so
                    // drain the whole backlog before waiting again. The socket is
                    // nonblocking, hence accept() never blocks here.
                    loop {
                        match l.accept() {
                            Ok((stream, _)) => {
                                if tx.blocking_send(stream).is_err() {
                                    break 'accept;
                                }
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(_) => break 'accept,
                        }
                    }
                }
                unsafe { WSACloseEvent(socket_event) };
            }) {
            Ok(t) => t,
            Err(e) => {
                unsafe {
                    WSACloseEvent(socket_event);
                    WSACloseEvent(cancel_event);
                }
                return Err(e);
            }
        };

        Ok(Self {
            rx: Some(rx),
            cancel_event,
            path: path.to_path_buf(),
            thread: Some(thread),
        })
    }

    /// Asynchronously accepts an incoming connection from the Windows AF_UNIX listener.
    pub async fn accept(&mut self) -> io::Result<(Box<dyn TunnelStream>, String)> {
        let rx = self
            .rx
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Interrupted, "Listener closed"))?;
        let std_stream = rx
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
        // Signal the accept thread's WSAWaitForMultipleEvents. This touches only
        // our own event object, never the still-open listener socket, so there
        // is no closesocket(), no AcceptEx and no self-connect. A self-connect
        // is not a valid wake-up anyway: the socket file may already have been
        // unlinked (temp dir cleanup, config reload), and connect() then fails
        // with WSAECONNREFUSED while accept() stays blocked forever.
        unsafe { WSASetEvent(self.cancel_event) };

        // Release the receiver first. If the accept thread is parked in
        // `blocking_send` on a full channel, dropping the receiver makes that
        // send fail immediately, so the join below cannot hang on it.
        drop(self.rx.take());

        // The accept thread is the sole owner of the listener SOCKET and of the
        // FD_ACCEPT event. Wait for it to exit and close both by itself.
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }

        // cancel_event is owned here; close it only after the thread is gone.
        unsafe { WSACloseEvent(self.cancel_event) };

        // Only now is the listener unbound; unlinking the stale path cannot race
        // with a live listener.
        let _ = std::fs::remove_file(&self.path);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    /// Regression guard: the socket file can disappear before `Drop` runs (temp
    /// dir cleanup, config reload). A self-connect wake-up fails in that state
    /// and leaves the accept thread blocked forever, hanging `join()`.
    #[test]
    fn drop_returns_when_socket_file_was_unlinked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("drop_probe.sock");
        let listener = PlatformUdsListener::bind(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        let (tx, rx) = mpsc::sync_channel(1);
        let dropper = std::thread::spawn(move || {
            drop(listener);
            let _ = tx.send(());
        });
        assert!(
            rx.recv_timeout(std::time::Duration::from_secs(2)).is_ok(),
            "PlatformUdsListener::Drop hung after the socket file was unlinked"
        );
        dropper.join().unwrap();
    }
}
