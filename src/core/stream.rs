//! Core stream abstractions and type-erased dynamic trait objects.
//! All comments and docstrings must be in English.

use std::future::Future;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};

/// Type alias for heap-pinned, sendable future without `async_trait` macro dependency.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Unified dynamic tunnel stream trait combining non-blocking asynchronous reading and writing.
///
/// Implementations must be `Send + Sync + Unpin + 'static` to allow thread-safe dispatching
/// and storage across Tokio task boundaries as `Box<dyn TunnelStream>`.
pub trait TunnelStream: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}

// Blanket implementation for any type satisfying the trait requirements.
impl<T> TunnelStream for T where T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}
