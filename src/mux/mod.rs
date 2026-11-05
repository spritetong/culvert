// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
