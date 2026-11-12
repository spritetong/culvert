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
BIN_PATH="./wstunnel"

cd "${PROJECT_ROOT}"

echo "[*] Starting wstunnel client:"
exec "${BIN_PATH}" client wss://127.0.0.1:8443 -L tcp://15201:127.0.0.1:5201
