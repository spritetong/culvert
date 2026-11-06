// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Cancellation and timeout extension traits for ergonomic and zero-cost async flow.
//! All comments and docstrings must be in English.

use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Tri-state outcome for an operation racing against a timeout and a cancellation token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutOrCancel<T> {
    /// Future completed successfully before timeout or cancellation.
    Ok(T),
    /// Operation timed out before the future resolved.
    TimedOut,
    /// Operation was cancelled by the token before completing.
    Cancelled,
}

impl<T> TimeoutOrCancel<T> {
    /// Returns `true` if the outcome is `Ok`.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok(_))
    }

    /// Returns `true` if the operation timed out.
    #[inline]
    pub fn is_timed_out(&self) -> bool {
        matches!(self, Self::TimedOut)
    }

    /// Returns `true` if the operation was cancelled.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    /// Converts the `TimeoutOrCancel<T>` into an `Option<T>`, discarding timeout/cancel states.
    #[inline]
    pub fn ok(self) -> Option<T> {
        match self {
            Self::Ok(v) => Some(v),
            _ => None,
        }
    }
}

/// Extension trait for [`CancellationToken`] providing cancel-aware delays.
pub trait CancellationTokenExt {
    /// Sleeps for the specified duration or until cancelled.
    ///
    /// Returns `true` if sleep completed without cancellation, or `false` if cancelled.
    fn sleep_or_cancel(&self, duration: Duration) -> impl Future<Output = bool>;

    /// Sleeps until the specified instant deadline or until cancelled.
    ///
    /// Returns `true` if the deadline was reached without cancellation, or `false` if cancelled.
    fn sleep_until_or_cancel(&self, deadline: Instant) -> impl Future<Output = bool>;
}

impl CancellationTokenExt for CancellationToken {
    #[inline]
    async fn sleep_or_cancel(&self, duration: Duration) -> bool {
        tokio::select! {
            biased;
            _ = self.cancelled() => false,
            _ = tokio::time::sleep(duration) => true,
        }
    }

    #[inline]
    async fn sleep_until_or_cancel(&self, deadline: Instant) -> bool {
        tokio::select! {
            biased;
            _ = self.cancelled() => false,
            _ = tokio::time::sleep_until(deadline) => true,
        }
    }
}

/// Extension trait for [`Future`] providing zero-cost cancellation and timeout combinators.
pub trait CancelFutureExt: Future + Sized {
    /// Runs this future until completion or until the token is cancelled.
    ///
    /// If cancelled, returns `None` and drops the future.
    /// Uses `biased;` to ensure cancellation is prioritized over future completion.
    #[inline]
    fn cancel_with(
        self,
        token: &CancellationToken,
    ) -> impl Future<Output = Option<Self::Output>> + Send
    where
        Self: Send,
    {
        let cancel = token.cancelled();
        async move {
            tokio::select! {
                biased;
                _ = cancel => None,
                res = self => Some(res),
            }
        }
    }

    /// Runs this future racing against a timeout duration and a cancellation token.
    ///
    /// Flattens the race into a single `tokio::select!` with `biased;` priority:
    /// Cancellation > Timeout > Future completion.
    #[inline]
    fn timeout_or_cancel(
        self,
        duration: Duration,
        token: &CancellationToken,
    ) -> impl Future<Output = TimeoutOrCancel<Self::Output>> + Send
    where
        Self: Send,
    {
        let cancel = token.cancelled();
        async move {
            tokio::select! {
                biased;
                _ = cancel => TimeoutOrCancel::Cancelled,
                _ = tokio::time::sleep(duration) => TimeoutOrCancel::TimedOut,
                res = self => TimeoutOrCancel::Ok(res),
            }
        }
    }
}

impl<F: Future> CancelFutureExt for F {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sleep_or_cancel_success() {
        let token = CancellationToken::new();
        let elapsed = token.sleep_or_cancel(Duration::from_millis(10)).await;
        assert!(elapsed);
    }

    #[tokio::test]
    async fn test_sleep_or_cancel_aborted() {
        let token = CancellationToken::new();
        let token_clone = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            token_clone.cancel();
        });

        let elapsed = token.sleep_or_cancel(Duration::from_millis(500)).await;
        assert!(!elapsed);
    }

    #[tokio::test]
    async fn test_sleep_until_or_cancel() {
        let token = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_millis(10);
        let elapsed = token.sleep_until_or_cancel(deadline).await;
        assert!(elapsed);
    }

    #[tokio::test]
    async fn test_cancel_with_completed() {
        let token = CancellationToken::new();
        let fut = async { 42 };
        let res = fut.cancel_with(&token).await;
        assert_eq!(res, Some(42));
    }

    #[tokio::test]
    async fn test_cancel_with_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        let fut = std::future::pending::<i32>();
        let res = fut.cancel_with(&token).await;
        assert_eq!(res, None);
    }

    #[tokio::test]
    async fn test_timeout_or_cancel_ok() {
        let token = CancellationToken::new();
        let fut = async { "hello" };
        let res = fut
            .timeout_or_cancel(Duration::from_millis(100), &token)
            .await;
        assert_eq!(res, TimeoutOrCancel::Ok("hello"));
        assert!(res.is_ok());
        assert_eq!(res.ok(), Some("hello"));
    }

    #[tokio::test]
    async fn test_timeout_or_cancel_timed_out() {
        let token = CancellationToken::new();
        let fut = std::future::pending::<()>();
        let res = fut
            .timeout_or_cancel(Duration::from_millis(10), &token)
            .await;
        assert_eq!(res, TimeoutOrCancel::TimedOut);
        assert!(res.is_timed_out());
        assert_eq!(res.ok(), None);
    }

    #[tokio::test]
    async fn test_timeout_or_cancel_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        let fut = std::future::pending::<()>();
        let res = fut
            .timeout_or_cancel(Duration::from_millis(500), &token)
            .await;
        assert_eq!(res, TimeoutOrCancel::Cancelled);
        assert!(res.is_cancelled());
        assert_eq!(res.ok(), None);
    }
}
