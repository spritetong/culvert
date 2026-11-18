#!/usr/bin/env bash
# Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
# SPDX-License-Identifier: MPL-2.0
# culvert is licensed under the Mozilla Public License Version 2.0.

# ==============================================================================
# Culvert Benchmark Client Startup Script (Linux / macOS)
# ==============================================================================

set -euo pipefail

export PATH="/root/.cargo/bin:${HOME}/.cargo/bin:${PATH}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN_PATH="${PROJECT_ROOT}/target/release/culvert"
CONFIG="${1:-${SCRIPT_DIR}/client-bench.yaml}"

cd "${PROJECT_ROOT}"

echo "[*] Building culvert in release mode..."
cargo build --release

echo "[*] Starting culvert client with config: ${CONFIG}"
exec "${BIN_PATH}" -c "${CONFIG}"
