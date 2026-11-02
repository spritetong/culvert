//! Transport-agnostic stream multiplexing protocol and session driver.
//! All comments and docstrings must be in English.

pub mod frame;
pub mod session;
pub mod stream;

pub use frame::{FrameType, MuxCodec, MuxFrame, MuxFrameError, MAX_FRAME_PAYLOAD_SIZE};
pub use session::MuxSession;
pub use stream::MuxStream;

#[cfg(test)]
mod tests;
