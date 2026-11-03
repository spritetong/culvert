#!/usr/bin/env bash
# ==============================================================================
# Micro-Tunnel Benchmark Client Startup Script (Linux / macOS)
# ==============================================================================

set -euo pipefail

export PATH="/root/.cargo/bin:${HOME}/.cargo/bin:${PATH}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN_PATH="${PROJECT_ROOT}/target/release/micro-tunnel"
CONFIG="${1:-${SCRIPT_DIR}/client-bench.yaml}"

cd "${PROJECT_ROOT}"

if [ ! -f "${BIN_PATH}" ]; then
    echo "[*] Compiling micro-tunnel in release mode..."
    cargo build --release
fi

echo "[*] Starting micro-tunnel client with config: ${CONFIG}"
exec "${BIN_PATH}" -c "${CONFIG}"
