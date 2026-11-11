// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Control channel signaling protocol for reverse tunneling.
//! All comments and docstrings must be in English.

pub mod channel;
pub mod stream;

pub use channel::{SignalingChannel, SignalingConfig, SignalingHandle, SignalingReceiver};
pub use stream::{ControlMessage, ControlStream, FramedControlStream};
