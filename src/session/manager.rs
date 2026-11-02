//! Session manager for pairing inbound reverse tunnel data streams.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::oneshot;

use crate::core::error::TunnelError;
use crate::core::stream::TunnelStream;

/// Thread-safe in-memory routing table for matching incoming reverse data streams
/// with listening TCP server connections on the remote host.
#[derive(Clone, Default)]
pub struct SessionManager {
    pending_sessions: Arc<DashMap<String, oneshot::Sender<Box<dyn TunnelStream>>>>,
}

impl SessionManager {
    /// Creates a new, empty `SessionManager`.
    pub fn new() -> Self {
        Self {
            pending_sessions: Arc::new(DashMap::new()),
        }
    }

    /// Registers a pending session expectation and returns a receiver for the stream.
    pub fn register(&self, session_id: String) -> oneshot::Receiver<Box<dyn TunnelStream>> {
        let (tx, rx) = oneshot::channel();
        self.pending_sessions.insert(session_id, tx);
        rx
    }

    /// Delivers an incoming reverse data stream to the waiting task.
    pub fn fulfill(&self, session_id: &str, stream: Box<dyn TunnelStream>) -> Result<(), TunnelError> {
        if let Some((_, tx)) = self.pending_sessions.remove(session_id) {
            tx.send(stream).map_err(|_| TunnelError::SessionClosed)
        } else {
            Err(TunnelError::SessionNotFound(session_id.to_string()))
        }
    }

    /// Removes a pending session, for example when a wait times out.
    pub fn remove(&self, session_id: &str) {
        self.pending_sessions.remove(session_id);
    }

    /// Returns the current number of pending reverse sessions waiting for streams.
    pub fn len(&self) -> usize {
        self.pending_sessions.len()
    }

    /// Checks if there are no pending sessions.
    pub fn is_empty(&self) -> bool {
        self.pending_sessions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_session_register_and_fulfill() {
        let manager = SessionManager::new();
        let session_id = "test_sess_001".to_string();

        let rx = manager.register(session_id.clone());
        assert_eq!(manager.len(), 1);

        let (client, _server) = duplex(1024);
        manager.fulfill(&session_id, Box::new(client)).unwrap();

        assert_eq!(manager.len(), 0);
        let stream = rx.await.unwrap();
        drop(stream);
    }
}
