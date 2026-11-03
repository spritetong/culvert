#!/usr/bin/env bash
# ==============================================================================
# Micro-Tunnel Benchmark Server Startup Script (Linux / macOS)
# ==============================================================================

set -euo pipefail

export PATH="/root/.cargo/bin:${HOME}/.cargo/bin:${PATH}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN_PATH="${PROJECT_ROOT}/target/release/micro-tunnel"
CONFIG="${1:-${SCRIPT_DIR}/server-bench.yaml}"

cd "${PROJECT_ROOT}"

if [ ! -f "${BIN_PATH}" ]; then
    echo "[*] Compiling micro-tunnel in release mode..."
    cargo build --release
fi

# Check and optionally start iperf3
if command -v iperf3 >/dev/null 2>&1; then
    if ! pgrep -x "iperf3" >/dev/null 2>&1; then
        echo "[*] Starting iperf3 server on port 5201..."
        iperf3 -s -p 5201 -D || true
    fi
fi

echo "[*] Starting micro-tunnel server with config: ${CONFIG}"
exec "${BIN_PATH}" -c "${CONFIG}"
