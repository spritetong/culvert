// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Session manager for pairing inbound reverse tunnel data streams and managing inbound pools.
//! All comments and docstrings must be in English.

use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::oneshot;

use crate::core::error::TunnelError;
use crate::core::stream::TunnelStream;
use crate::transport::TargetAddr;

/// Derives a canonical key for matching reverse listener inbound pools.
pub fn canonical_reverse_key(addr: &str) -> String {
    let s = addr.trim();
    if s.starts_with("unix://") {
        return s.to_string();
    }
    if let Some((_, port_str)) = s.rsplit_once(':') {
        if let Ok(port) = port_str.parse::<u16>() {
            return format!("tcp:{}", port);
        }
    }
    s.to_string()
}

/// Thread-safe in-memory routing table for matching incoming reverse data streams
/// and managing pre-warmed inbound stream pools.
#[derive(Clone, Default)]
pub struct SessionManager {
    pending_sessions: Arc<DashMap<String, oneshot::Sender<Box<dyn TunnelStream>>>>,
    reverse_pools: Arc<DashMap<String, Arc<Mutex<VecDeque<Box<dyn TunnelStream>>>>>>,
}

impl SessionManager {
    /// Creates a new, empty `SessionManager`.
    pub fn new() -> Self {
        Self {
            pending_sessions: Arc::new(DashMap::new()),
            reverse_pools: Arc::new(DashMap::new()),
        }
    }

    /// Registers a pending session expectation and returns a receiver for the stream.
    pub fn register(&self, session_id: String) -> oneshot::Receiver<Box<dyn TunnelStream>> {
        let (tx, rx) = oneshot::channel();
        self.pending_sessions.insert(session_id, tx);
        rx
    }

    /// Delivers an incoming reverse data stream to the waiting task.
    pub fn fulfill(
        &self,
        session_id: &str,
        stream: Box<dyn TunnelStream>,
    ) -> Result<(), TunnelError> {
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

    /// Registers an inbound pool for a reverse listener address.
    pub fn register_pool(
        &self,
        listen_addr: &str,
    ) -> Arc<Mutex<VecDeque<Box<dyn TunnelStream>>>> {
        let key = canonical_reverse_key(listen_addr);
        let pool = Arc::new(Mutex::new(VecDeque::new()));
        self.reverse_pools.insert(key, Arc::clone(&pool));
        pool
    }

    /// Unregisters an inbound pool for a reverse listener address.
    pub fn unregister_pool(&self, listen_addr: &str) {
        let key = canonical_reverse_key(listen_addr);
        self.reverse_pools.remove(&key);
    }

    /// Pushes a pre-warmed idle reverse stream into the matching listener's pool.
    pub fn push_idle_stream(
        &self,
        target_opt: Option<&TargetAddr>,
        stream: Box<dyn TunnelStream>,
    ) -> bool {
        if let Some(target) = target_opt {
            let key = canonical_reverse_key(&target.to_string());
            if let Some(entry) = self.reverse_pools.get(&key) {
                entry.lock().push_back(stream);
                return true;
            }
        }
        // Fallback: If exactly one reverse pool is active on the server, route to it
        if self.reverse_pools.len() == 1 {
            if let Some(entry) = self.reverse_pools.iter().next() {
                entry.value().lock().push_back(stream);
                return true;
            }
        }
        false
    }

    /// Returns the number of registered reverse inbound pools.
    pub fn pool_count(&self) -> usize {
        self.reverse_pools.len()
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

    #[tokio::test]
    async fn test_inbound_pool_register_and_push() {
        let manager = SessionManager::new();
        assert_eq!(manager.pool_count(), 0);

        let pool = manager.register_pool("0.0.0.0:8080");
        assert_eq!(manager.pool_count(), 1);

        let (client, _server) = duplex(1024);
        let target = TargetAddr::Domain("127.0.0.1".into(), 8080);
        let pushed = manager.push_idle_stream(Some(&target), Box::new(client));
        assert!(pushed, "Should match port 8080");
        assert_eq!(pool.lock().len(), 1);

        let popped = pool.lock().pop_front();
        assert!(popped.is_some());
        assert_eq!(pool.lock().len(), 0);

        manager.unregister_pool("0.0.0.0:8080");
        assert_eq!(manager.pool_count(), 0);
    }

    #[test]
    fn test_canonical_reverse_key() {
        assert_eq!(canonical_reverse_key("0.0.0.0:9090"), "tcp:9090");
        assert_eq!(canonical_reverse_key("127.0.0.1:9090"), "tcp:9090");
        assert_eq!(canonical_reverse_key("[::1]:9090"), "tcp:9090");
        assert_eq!(canonical_reverse_key("unix:///tmp/sock"), "unix:///tmp/sock");
    }
}
