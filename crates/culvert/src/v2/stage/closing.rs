// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! `Closing` stage contract: bounded drain and orderly resource release.
//! All comments and docstrings must be in English.

use crate::v2::error::TaskError;
use std::future::Future;

/// Internal stage module executed while a task's external state is `Closing`.
pub trait ClosingStage: Send + 'static {
    /// Performs bounded drain / shutdown cleanup before task termination.
    fn run(&mut self) -> impl Future<Output = Result<(), TaskError>> + Send;
}
