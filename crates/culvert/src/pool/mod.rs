// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Connection pool module for pre-warmed tunnel connections.
//! All comments and docstrings must be in English.

pub mod activation;

pub use activation::{
    ActivationHeader, activate_and_verify_stream, decode_activation, encode_activation_with_proto,
};
