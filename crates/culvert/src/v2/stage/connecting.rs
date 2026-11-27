// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! `Connecting` stage contract: acquires resources and builds the `ActiveStage`.
//! All comments and docstrings must be in English.

use crate::v2::error::TaskError;
use crate::v2::stage::StageContext;
use crate::v2::stage::active::ActiveStage;
use std::future::Future;

/// Internal stage module executed while a task's external state is `Connecting`.
pub trait ConnectingStage: Send + 'static {
    /// Target hot-path stage produced once pairing/setup succeeds.
    type Active: ActiveStage;

    /// Executes setup/pairing work asynchronously and yields the constructed `Active` stage.
    fn run(
        &mut self,
        ctx: &mut StageContext<'_>,
    ) -> impl Future<Output = Result<Self::Active, TaskError>> + Send;
}
