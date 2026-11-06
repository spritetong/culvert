// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Transport-agnostic stream multiplexing protocol and session driver.
//! All comments and docstrings must be in English.

pub mod frame;
pub mod session;
pub mod stream;

pub use frame::{FrameType, MAX_FRAME_PAYLOAD_SIZE, MuxCodec, MuxFrame, MuxFrameError};
pub use session::MuxSession;
pub use stream::MuxStream;

#[cfg(test)]
mod tests;
