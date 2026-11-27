// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Internal stage abstractions mapped to external task states.
//! All comments and docstrings must be in English.

pub mod active;
pub mod closing;
pub mod connecting;

pub use active::ActiveStage;
pub use closing::ClosingStage;
pub use connecting::ConnectingContext;

use crate::v2::error::TaskError;
use std::future::Future;

/// OOP workflow bundle defining the internal modules executed for each task state.
pub trait TaskWorkflow: Send + Sync + 'static {
    /// Owned input payload delivered when transitioning `Empty -> Connecting`.
    type Input: Send + 'static;

    /// Low-abstraction hot-path stage executed during `Active`.
    type Active: ActiveStage;

    /// Executes the `Connecting` module asynchronously for `input` and yields `Active`.
    fn connect(
        &self,
        ctx: &mut ConnectingContext<'_>,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Active, TaskError>> + Send;
}
