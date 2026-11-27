// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! `Active` stage contract: hot-path execution allowing low-abstraction poll loops.
//! All comments and docstrings must be in English.

use crate::v2::error::TaskError;
use crate::v2::stage::closing::ClosingStage;
use crate::v2::state::CloseReason;
use std::task::{Context, Poll};

/// Internal stage module executed while a task's external state is `Active`.
///
/// Uses a direct `poll_drive` hook rather than boxed futures so hot-path I/O
/// implementations (such as duplex stream pumps) can run as zero-allocation
/// `poll_fn` state machines while preserving ownership across cancellation/drain.
pub trait ActiveStage: Send + 'static {
    /// Drain/cleanup stage produced when leaving `Active`.
    type Closing: ClosingStage;

    /// Drives the hot-path unit of work.
    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), TaskError>>;

    /// Moves remaining owned resources and buffers into the `Closing` stage.
    fn into_closing(self, reason: CloseReason) -> Self::Closing;
}
