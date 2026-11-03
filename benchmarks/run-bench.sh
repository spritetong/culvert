#!/usr/bin/env bash
# ==============================================================================
# Micro-Tunnel Benchmark Execution Script (Linux / macOS)
# ==============================================================================

set -euo pipefail

TARGET_HOST="${1:-127.0.0.1}"
TARGET_PORT="${2:-15201}"
PARALLEL_STREAMS="${3:-4}"
DURATION_SECS="${4:-10}"

echo "================================================================================"
echo " Starting Micro-Tunnel Benchmark (Linux / macOS)"
echo " Target: ${TARGET_HOST}:${TARGET_PORT} | Streams: ${PARALLEL_STREAMS} | Duration: ${DURATION_SECS}s"
echo "================================================================================"

if command -v iperf3 >/dev/null 2>&1; then
    echo "[*] Running iperf3 benchmark through tunnel..."
    iperf3 -c "${TARGET_HOST}" -p "${TARGET_PORT}" -P "${PARALLEL_STREAMS}" -t "${DURATION_SECS}"
else
    echo "[!] iperf3 not found on system path."
    echo "[*] Falling back to Python3 throughput test..."
    python3 -c "
import socket, time, threading

host = '${TARGET_HOST}'
port = int('${TARGET_PORT}')
streams = int('${PARALLEL_STREAMS}')
duration = int('${DURATION_SECS}')
chunk_size = 64 * 1024
payload = b'X' * chunk_size

total_bytes = 0
stop_flag = False
lock = threading.Lock()

def worker():
    global total_bytes, stop_flag
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        local_count = 0
        while not stop_flag:
            s.sendall(payload)
            local_count += chunk_size
        s.close()
        with lock:
            total_bytes += local_count
    except Exception as e:
        print(f'Stream error: {e}')

threads = [threading.Thread(target=worker) for _ in range(streams)]
for t in threads:
    t.start()

start_time = time.time()
time.sleep(duration)
stop_flag = True

for t in threads:
    t.join()

elapsed = time.time() - start_time
mb_transferred = total_bytes / (1024 * 1024)
mbps = mb_transferred / elapsed
gbps = (total_bytes * 8) / (elapsed * 1_000_000_000)

print('--------------------------------------------------------------------------------')
print(f'Duration:    {elapsed:.2f} s')
print(f'Transferred: {mb_transferred:.2f} MB')
print(f'Throughput:  {mbps:.2f} MB/s ({gbps:.3f} Gbps)')
print('--------------------------------------------------------------------------------')
"
fi
