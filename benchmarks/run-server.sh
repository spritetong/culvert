#!/usr/bin/env bash
# Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
# SPDX-License-Identifier: MPL-2.0
# culvert is licensed under the Mozilla Public License Version 2.0.

# ==============================================================================
# Culvert Benchmark Server Startup Script (Linux / macOS)
# ==============================================================================

set -euo pipefail

export PATH="/root/.cargo/bin:${HOME}/.cargo/bin:${PATH}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN_PATH="${PROJECT_ROOT}/target/release/culvert"
CONFIG="${1:-${SCRIPT_DIR}/server-bench.yaml}"

cd "${PROJECT_ROOT}"

if [ ! -f "${BIN_PATH}" ]; then
    echo "[*] Compiling culvert in release mode..."
    cargo build --release
fi

# Check and optionally start iperf3. Match a server command line only: a running
# `iperf3 -c` client has the same process name but is not a backend listener.
if command -v iperf3 >/dev/null 2>&1; then
    if ! pgrep -f "iperf3.*-s" >/dev/null 2>&1; then
        echo "[*] Starting iperf3 server on port 5201..."
        # Detach stdio so the daemon cannot hold the terminal open after exec.
        iperf3 -s -p 5201 -D </dev/null >/dev/null 2>&1 || true
    fi
fi

echo "[*] Starting culvert server with config: ${CONFIG}"
exec "${BIN_PATH}" -c "${CONFIG}"
