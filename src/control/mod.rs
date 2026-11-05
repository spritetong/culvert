// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Control channel signaling protocol for reverse tunneling.
//! All comments and docstrings must be in English.

pub mod stream;

pub use stream::{ControlMessage, ControlStream, FramedControlStream};
