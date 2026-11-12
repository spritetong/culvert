// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Platform-specific abstractions and operating system integrations.
//! All comments and docstrings must be in English.

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::*;

#[cfg(not(windows))]
mod unix;
#[cfg(not(windows))]
pub use unix::*;

use bytes::Bytes;
use reusable_future::ReusableFutureRef;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use crate::common::net::SocketTuner;
use crate::core::error::TunnelError;
use crate::core::stream::TunnelStream;
use crate::io::addr::{L4Protocol, TargetAddr};
use crate::io::channel::{Datagram, DatagramIo, FlowKey, IoChannel};

pub use crate::io::addr::{
    ListenAddr, PeerAddr, is_uds_path, normalize_uds_path, parse_listen_addr, parse_target_addr,
};

/// Result type produced when accepting a stream from a `GenericListener`.
pub type GenericAcceptOutput = Result<(Box<dyn TunnelStream>, PeerAddr), TunnelError>;

/// Generic listener capable of accepting incoming connections over TCP or Unix Domain Sockets.
#[derive(Clone)]
pub enum GenericListener {
    Tcp(Arc<TcpListener>),
    Uds(Arc<tokio::sync::Mutex<PlatformUdsListener>>),
}

impl GenericListener {
    /// Binds to a dual-stack TCP port (:port), standard TCP address (addr:port), or Unix Domain Socket path.
    pub async fn bind(addr: &str) -> Result<Self, TunnelError> {
        let listen_addr = parse_listen_addr(addr)?;
        match listen_addr {
            ListenAddr::DualStack(port) => {
                let std_listener = create_dual_stack_tcp_listener(port)
                    .map_err(TunnelError::Io)?;
                std_listener.set_nonblocking(true).map_err(TunnelError::Io)?;
                let listener = TcpListener::from_std(std_listener)
                    .map_err(TunnelError::Io)?;
                Ok(GenericListener::Tcp(Arc::new(listener)))
            }
            ListenAddr::Path(path) => {
                let listener = PlatformUdsListener::bind(&path).map_err(TunnelError::Io)?;
                Ok(GenericListener::Uds(Arc::new(tokio::sync::Mutex::new(
                    listener,
                ))))
            }
            ListenAddr::Ip(sa) => {
                let listener = TcpListener::bind(sa).await.map_err(TunnelError::Io)?;
                Ok(GenericListener::Tcp(Arc::new(listener)))
            }
            ListenAddr::Domain(host, port) => {
                let listener = TcpListener::bind((host.as_str(), port))
                    .await
                    .map_err(TunnelError::Io)?;
                Ok(GenericListener::Tcp(Arc::new(listener)))
            }
        }
    }

    /// Returns an owned `'static` future for the next incoming connection.
    /// Cloning the listener is two `Arc` clones; callers can embed this in a
    /// reused slot without allocating a throwaway future container per accept.
    pub fn accept_owned(&self) -> impl std::future::Future<Output = GenericAcceptOutput> + 'static {
        let listener = self.clone();
        async move {
            match &listener {
                GenericListener::Tcp(l) => {
                    let (stream, addr) = l.accept().await.map_err(TunnelError::Io)?;
                    let _ = SocketTuner::tune_tcp_stream(&stream, true);
                    let canonical_addr = match addr {
                        SocketAddr::V6(v6) => SocketAddr::new(v6.ip().to_canonical(), v6.port()),
                        v4 => v4,
                    };
                    Ok((
                        Box::new(stream) as Box<dyn TunnelStream>,
                        PeerAddr::Socket(canonical_addr),
                    ))
                }
                GenericListener::Uds(l) => {
                    let mut guard = l.lock().await;
                    let (stream, peer_addr) = guard.accept().await.map_err(TunnelError::Io)?;
                    let peer = if peer_addr.is_empty() {
                        PeerAddr::Path(std::path::PathBuf::from("unnamed"))
                    } else {
                        PeerAddr::Path(std::path::PathBuf::from(peer_addr))
                    };
                    Ok((stream, peer))
                }
            }
        }
    }

    /// Asynchronously awaits and accepts the next incoming connection stream into `slot`.
    ///
    /// Loop callers keep `slot` outside `tokio::select!` and poll `&mut slot` in a
    /// branch, so an in-flight accept survives other branches winning.
    pub fn accept(&mut self, slot: &mut ReusableFutureRef<'static, GenericAcceptOutput>) {
        if !slot.is_empty() {
            return;
        }
        let pending = self.accept_owned();
        slot.set(move || pending);
    }

    /// Convenience helper for single-shot / non-loop awaiting.
    pub async fn accept_one(&mut self) -> GenericAcceptOutput {
        self.accept_owned().await
    }
}

/// Connects to a destination TargetAddr (IP, Domain, or UDS Path) using TCP/UDS stream.
pub async fn connect_target(target: &TargetAddr) -> Result<Box<dyn TunnelStream>, TunnelError> {
    match target {
        TargetAddr::Ip(sa) => {
            let stream = TcpStream::connect(*sa).await.map_err(TunnelError::Io)?;
            let _ = SocketTuner::tune_tcp_stream(&stream, true);
            Ok(Box::new(stream))
        }
        TargetAddr::Domain(host, port) => {
            let stream = TcpStream::connect((host.as_str(), *port))
                .await
                .map_err(TunnelError::Io)?;
            let _ = SocketTuner::tune_tcp_stream(&stream, true);
            Ok(Box::new(stream))
        }
        TargetAddr::Path(path) => {
            let stream = connect_uds(path).await.map_err(TunnelError::Io)?;
            Ok(stream)
        }
    }
}

/// Connects to a target address or UDS path specified as a string.
pub async fn connect_target_str(target_str: &str) -> Result<Box<dyn TunnelStream>, TunnelError> {
    let target = parse_target_addr(target_str)?;
    connect_target(&target).await
}

/// Connected outbound UDP socket implementing `DatagramIo` for relaying to a specific backend target.
pub struct ConnectedUdpSocket {
    socket: UdpSocket,
    peer_addr: SocketAddr,
    recv_buf: Vec<u8>,
}

impl ConnectedUdpSocket {
    /// Resolves `target` if necessary, binds an ephemeral local UDP socket of matching address family,
    /// and connects it to the destination peer.
    pub async fn connect(target: &TargetAddr) -> Result<Self, TunnelError> {
        let peer_addr = match target {
            TargetAddr::Ip(sa) => *sa,
            TargetAddr::Domain(host, port) => {
                let mut addrs = tokio::net::lookup_host((host.as_str(), *port))
                    .await
                    .map_err(TunnelError::Io)?;
                addrs
                    .next()
                    .ok_or_else(|| TunnelError::DnsNoAddresses(host.clone(), "any".to_string()))?
            }
            TargetAddr::Path(p) => {
                return Err(TunnelError::Config(format!(
                    "UDP target cannot be a Unix domain socket path '{}'",
                    p.display()
                )));
            }
        };

        let bind_addr: SocketAddr = if peer_addr.is_ipv4() {
            "0.0.0.0:0"
                .parse()
                .map_err(|e| TunnelError::Internal(format!("Invalid IPv4 bind addr: {}", e)))?
        } else {
            "[::]:0"
                .parse()
                .map_err(|e| TunnelError::Internal(format!("Invalid IPv6 bind addr: {}", e)))?
        };

        let socket = UdpSocket::bind(bind_addr).await.map_err(TunnelError::Io)?;
        socket.connect(peer_addr).await.map_err(TunnelError::Io)?;

        Ok(Self {
            socket,
            peer_addr,
            recv_buf: vec![0u8; 65536],
        })
    }
}

impl DatagramIo for ConnectedUdpSocket {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        batch: &mut [Datagram],
    ) -> Poll<io::Result<usize>> {
        if batch.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let mut buf = tokio::io::ReadBuf::new(&mut self.recv_buf);
        match self.socket.poll_recv(cx, &mut buf) {
            Poll::Ready(Ok(())) => {
                let n = buf.filled().len();
                batch[0] = Datagram {
                    flow: FlowKey {
                        peer: PeerAddr::Socket(self.peer_addr),
                        target: None,
                        protocol: L4Protocol::Udp,
                    },
                    payload: Bytes::copy_from_slice(&self.recv_buf[..n]),
                };

                let mut count = 1;
                while count < batch.len() {
                    match self.socket.try_recv(&mut self.recv_buf) {
                        Ok(extra_n) => {
                            batch[count] = Datagram {
                                flow: FlowKey {
                                    peer: PeerAddr::Socket(self.peer_addr),
                                    target: None,
                                    protocol: L4Protocol::Udp,
                                },
                                payload: Bytes::copy_from_slice(&self.recv_buf[..extra_n]),
                            };
                            count += 1;
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(_) => break,
                    }
                }
                Poll::Ready(Ok(count))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_send(&mut self, cx: &mut Context<'_>, batch: &[Datagram]) -> Poll<io::Result<usize>> {
        let mut sent = 0;
        for dgram in batch {
            match self.socket.poll_send(cx, &dgram.payload) {
                Poll::Ready(Ok(_)) => {
                    sent += 1;
                }
                Poll::Ready(Err(e)) => {
                    if sent > 0 {
                        return Poll::Ready(Ok(sent));
                    }
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    if sent > 0 {
                        return Poll::Ready(Ok(sent));
                    }
                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(Ok(sent))
    }
}

/// Creates an IPv6/IPv4 dual-stack TCP listener using `socket2` with `set_only_v6(false)`.
/// Binds to `[::]:port`. Supported on Windows, Linux, and macOS.
/// If IPv6 is not supported by the OS, gracefully falls back to IPv4 `0.0.0.0:port`.
pub fn create_dual_stack_tcp_listener(port: u16) -> io::Result<std::net::TcpListener> {
    match create_ipv6_dual_stack_tcp(port) {
        Ok(l) => Ok(l),
        Err(e) if is_ipv6_unsupported(&e) => {
            let addr = std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, port);
            std::net::TcpListener::bind(addr)
        }
        Err(e) => Err(e),
    }
}

fn create_ipv6_dual_stack_tcp(port: u16) -> io::Result<std::net::TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV6,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_only_v6(false)?;
    socket.set_reuse_address(true)?;
    let bind_addr = std::net::SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, port, 0, 0);
    socket.bind(&socket2::SockAddr::from(bind_addr))?;
    socket.listen(1024)?;
    Ok(socket.into())
}

/// Creates an IPv6/IPv4 dual-stack UDP socket using `socket2` with `set_only_v6(false)`.
/// Binds to `[::]:port`. Supported on Windows, Linux, and macOS.
/// If IPv6 is not supported by the OS, gracefully falls back to IPv4 `0.0.0.0:port`.
pub fn create_dual_stack_udp_socket(port: u16) -> io::Result<std::net::UdpSocket> {
    match create_ipv6_dual_stack_udp(port) {
        Ok(s) => Ok(s),
        Err(e) if is_ipv6_unsupported(&e) => {
            let addr = std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, port);
            std::net::UdpSocket::bind(addr)
        }
        Err(e) => Err(e),
    }
}

fn create_ipv6_dual_stack_udp(port: u16) -> io::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV6,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_only_v6(false)?;
    socket.set_reuse_address(true)?;
    let bind_addr = std::net::SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, port, 0, 0);
    socket.bind(&socket2::SockAddr::from(bind_addr))?;
    Ok(socket.into())
}

fn is_ipv6_unsupported(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Unsupported
        || err.raw_os_error() == Some(97)
        || err.raw_os_error() == Some(10047)
}

/// Connects to a destination `TargetAddr` returning either a stream or datagram `IoChannel`
/// based on the specified L4 protocol.
pub async fn connect_target_channel(
    target: &TargetAddr,
    protocol: L4Protocol,
) -> Result<IoChannel, TunnelError> {
    match protocol {
        L4Protocol::Tcp => connect_target(target)
            .await
            .map(IoChannel::from_boxed_stream),
        L4Protocol::Udp => {
            let sock = ConnectedUdpSocket::connect(target).await?;
            Ok(IoChannel::from_datagram(sock))
        }
    }
}

/// Connects to a target address string returning an `IoChannel` matching `protocol`.
pub async fn connect_target_channel_str(
    target_str: &str,
    protocol: L4Protocol,
) -> Result<IoChannel, TunnelError> {
    let target = parse_target_addr(target_str)?;
    connect_target_channel(&target, protocol).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_is_uds_path() {
        assert!(is_uds_path("unix:///tmp/test.sock"));
        assert!(is_uds_path("ws+unix:///tmp/test.sock"));
        assert!(is_uds_path("wss+unix:///tmp/test.sock"));
        assert!(is_uds_path("unix://C:/temp/my.sock"));
        assert!(is_uds_path("unix://./relative.sock"));

        assert!(!is_uds_path("/var/run/my.sock"));
        assert!(!is_uds_path("test.sock"));
        assert!(!is_uds_path("./local.sock"));
        assert!(!is_uds_path("127.0.0.1:8080"));
        assert!(!is_uds_path("0.0.0.0:443"));
        assert!(!is_uds_path("[::1]:8080"));
        assert!(!is_uds_path("localhost:8080"));
        assert!(!is_uds_path("example.com:443"));
    }

    #[test]
    fn test_normalize_uds_path() {
        let p1 = normalize_uds_path("unix:///tmp/test.sock");
        assert_eq!(p1, PathBuf::from("/tmp/test.sock"));

        let p2 = normalize_uds_path("ws+unix:///tmp/test.sock");
        assert_eq!(p2, PathBuf::from("/tmp/test.sock"));

        let p3 = normalize_uds_path("wss+unix:///tmp/test.sock");
        assert_eq!(p3, PathBuf::from("/tmp/test.sock"));

        let p4 = normalize_uds_path("unix://C:/temp/test.sock");
        assert_eq!(p4, PathBuf::from("C:/temp/test.sock"));

        let p5 = normalize_uds_path("unix:///C:/temp/test.sock");
        assert_eq!(p5, PathBuf::from("C:/temp/test.sock"));
    }

    #[tokio::test]
    async fn test_platform_uds_bidirectional_transfer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let sock_path = temp_dir.path().join("test_pipe.sock");

        let mut listener = PlatformUdsListener::bind(&sock_path).unwrap();

        let server_task = tokio::spawn(async move {
            let (mut server_stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            server_stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping!");
            server_stream.write_all(b"pong!").await.unwrap();
            server_stream.flush().await.unwrap();
        });

        let mut client_stream = connect_uds(&sock_path).await.unwrap();
        client_stream.write_all(b"ping!").await.unwrap();
        client_stream.flush().await.unwrap();

        let mut buf = [0u8; 5];
        client_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong!");

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_generic_listener_and_target_connect() {
        let temp_dir = tempfile::tempdir().unwrap();
        let sock_path = temp_dir.path().join("generic_test.sock");
        let sock_str = format!("unix://{}", sock_path.display());

        let mut listener = GenericListener::bind(&sock_str).await.unwrap();

        let sock_str_clone = sock_str.clone();
        let srv_task = tokio::spawn(async move {
            let (mut stream, _peer) = listener.accept_one().await.unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"echo");
            stream.write_all(b"echo").await.unwrap();
            stream.flush().await.unwrap();
        });

        let mut client = connect_target_str(&sock_str_clone).await.unwrap();
        client.write_all(b"echo").await.unwrap();
        client.flush().await.unwrap();

        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"echo");

        srv_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_dual_stack_tcp_listener_accepts_v4_and_v6() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let listen_addr = format!(":{}", port);
        let mut listener = match GenericListener::bind(&listen_addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Skipping dual stack TCP test: {:?}", e);
                return;
            }
        };

        let srv_task = tokio::spawn(async move {
            let (mut s1, _p1) = listener.accept_one().await.unwrap();
            let mut buf1 = [0u8; 4];
            s1.read_exact(&mut buf1).await.unwrap();
            assert_eq!(&buf1, b"v4in");
            s1.write_all(b"v4ok").await.unwrap();

            let (mut s2, _p2) = listener.accept_one().await.unwrap();
            let mut buf2 = [0u8; 4];
            s2.read_exact(&mut buf2).await.unwrap();
            assert_eq!(&buf2, b"v6in");
            s2.write_all(b"v6ok").await.unwrap();
        });

        // Client 1 connects via IPv4
        let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
        c1.write_all(b"v4in").await.unwrap();
        let mut resp1 = [0u8; 4];
        c1.read_exact(&mut resp1).await.unwrap();
        assert_eq!(&resp1, b"v4ok");

        // Client 2 connects via IPv6
        let mut c2 = match TcpStream::connect(format!("[::1]:{}", port)).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("IPv6 loopback connect failed in environment: {:?}", e);
                srv_task.abort();
                return;
            }
        };
        c2.write_all(b"v6in").await.unwrap();
        let mut resp2 = [0u8; 4];
        c2.read_exact(&mut resp2).await.unwrap();
        assert_eq!(&resp2, b"v6ok");

        srv_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_dual_stack_udp_frontend_accepts_v4_and_v6() {
        use crate::frontend::Frontend;

        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let listen_addr = format!(":{}", port);
        let mut fe = match crate::frontend::udp::UdpFrontend::bind(&listen_addr).await {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Skipping dual stack UDP test: {:?}", e);
                return;
            }
        };

        // Client 1 (IPv4)
        let c1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        c1.send_to(b"udp4", format!("127.0.0.1:{}", port)).await.unwrap();

        let mut slot = ReusableFutureRef::default();
        fe.accept(&mut slot);
        let res = (&mut slot).await.unwrap();
        let mut ch1 = res.channel.into_datagram().unwrap();
        let mut batch = vec![Datagram {
            flow: FlowKey {
                peer: PeerAddr::Socket(format!("127.0.0.1:{}", port).parse().unwrap()),
                target: None,
                protocol: L4Protocol::Udp,
            },
            payload: Bytes::new(),
        }];
        let poll_res = std::future::poll_fn(|cx| {
            ch1.poll_recv(cx, &mut batch)
        }).await.unwrap();
        assert_eq!(poll_res, 1);
        assert_eq!(&batch[0].payload[..], b"udp4");

        // Client 2 (IPv6)
        if let Ok(c2) = UdpSocket::bind("[::1]:0").await {
            c2.send_to(b"udp6", format!("[::1]:{}", port)).await.unwrap();
            let mut slot2 = ReusableFutureRef::default();
            fe.accept(&mut slot2);
            let res2 = (&mut slot2).await.unwrap();
            let mut ch2 = res2.channel.into_datagram().unwrap();
            let mut batch2 = vec![Datagram {
                flow: FlowKey {
                    peer: PeerAddr::Socket(format!("[::1]:{}", port).parse().unwrap()),
                    target: None,
                    protocol: L4Protocol::Udp,
                },
                payload: Bytes::new(),
            }];
            let poll_res2 = std::future::poll_fn(|cx| {
                ch2.poll_recv(cx, &mut batch2)
            }).await.unwrap();
            assert_eq!(poll_res2, 1);
            assert_eq!(&batch2[0].payload[..], b"udp6");
        }
    }
}
