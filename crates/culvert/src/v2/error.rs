// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Error types for the v2 Coordinator and IO Task state machine.
//! All comments and docstrings must be in English.

use crate::v2::state::TunnelPhase;
use thiserror::Error;

/// Errors produced by coordination requests and task stage execution.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TaskError {
    #[error("coordinator is no longer running")]
    CoordinatorGone,

    #[error("operation rejected while tunnel is in phase {0}")]
    RejectedByCoordinator(TunnelPhase),

    #[error("task dependency coordination failed: {0}")]
    DependencyFailed(String),

    #[error("stage execution failed: {0}")]
    StageFailed(String),
}
