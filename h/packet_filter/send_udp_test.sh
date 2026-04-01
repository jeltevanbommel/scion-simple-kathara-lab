#!/usr/bin/env bash
set -euo pipefail

DEST_IP="${1:-127.0.0.1}"
DEST_PORT="${2:-9999}"
BLOCKED_SRC_PORT="${BLOCKED_SRC_PORT:-5555}"
ALLOWED_SRC_PORT="${ALLOWED_SRC_PORT:-5556}"

python3 - <<PY
import socket

def send(src_port, label):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('', src_port))
    s.sendto(f'{label} from {src_port}'.encode(), ('${DEST_IP}', int('${DEST_PORT}')))
    s.close()

send(int('${BLOCKED_SRC_PORT}'), 'blocked')
send(int('${ALLOWED_SRC_PORT}'), 'allowed')
print('sent UDP packets to ${DEST_IP}:${DEST_PORT} from', '${BLOCKED_SRC_PORT}', 'and', '${ALLOWED_SRC_PORT}')
PY
