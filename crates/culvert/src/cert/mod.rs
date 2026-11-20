// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Certificate management, unified reading, and dynamic hot-reloading.
//! All comments and docstrings must be in English.

pub mod events;
pub mod manager;
pub mod reader;
pub mod resolver;
pub mod watcher;

pub use events::{CertEvent, CertEventBus};
pub use manager::CertManager;
pub use reader::CertReader;
pub use resolver::{DynamicCertResolver, DynamicClientCertVerifier};
pub use watcher::{CertFileWatcher, DEFAULT_DEBOUNCE_INTERVAL};
