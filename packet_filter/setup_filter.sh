#!/usr/bin/env bash
set -euo pipefail

BPF_DIR="/shared/packet_filter"
SRC="${BPF_DIR}/icmp_egress.c"

# Determine NFQUEUE_NUM based on hostname if not set
QUEUE_NUM="${NFQUEUE_NUM:-$((${HOSTNAME##*_} >= 1 ? ${HOSTNAME##*_} : 10))}"

if [ -z "${CANDIDATE_MARK:-}" ]; then
    CANDIDATE_MARK="${QUEUE_NUM}"
fi

# Compile eBPF program
# Note, we add CANDIDATE_MARK as a compile-time constant to ensure that each instance of the TC Program 
# uses a different mark value, avoiding conflicts when multiple instances run on the same host.
OBJ="${BPF_DIR}/icmp_egress_${CANDIDATE_MARK}.o"

VMLINUX_INCLUDE=""
ARCH_DIR="/usr/include/$(uname -m)-linux-gnu"
if [ -d "${ARCH_DIR}" ]; then
    VMLINUX_INCLUDE="-I${ARCH_DIR}"
fi

arch="$(uname -m)"
case "${arch}" in
    x86_64) bpf_arch="x86" ;;
    aarch64) bpf_arch="arm64" ;;
    *) bpf_arch="x86" ;;
esac

if [ ! -f "${OBJ}" ] || [ "${SRC}" -nt "${OBJ}" ]; then
    clang -O2 -g -target bpf -D__TARGET_ARCH_${bpf_arch} -DCANDIDATE_MARK="${CANDIDATE_MARK}" \
        ${VMLINUX_INCLUDE} -c "${SRC}" -o "${OBJ}"
fi

# Attach eBPF program to eth0 egress
if ip link show eth0 >/dev/null 2>&1; then
    tc qdisc add dev eth0 clsact 2>/dev/null || true
    tc filter del dev eth0 egress 2>/dev/null || true
    tc filter replace dev eth0 egress bpf da obj "${OBJ}" sec tc
fi
# Now set up nftables to direct ICMP echo-request packets to NFQUEUE

# If existing, delete nftables table to avoid conflicts
if nft list table inet nfqueue_filter >/dev/null 2>&1; then
    nft delete table inet nfqueue_filter
fi

nft add table inet nfqueue_filter
nft add chain inet nfqueue_filter output "{ type filter hook output priority 0 ; policy accept ; }"
nft add rule inet nfqueue_filter output ip protocol icmp icmp type echo-request queue num "${QUEUE_NUM}" bypass

# Compile and run the NFQUEUE reader in Go
READER="${BPF_DIR}/nfqueue_reader"
SRC_READER="${BPF_DIR}/nfqueue_reader.go"

if [ ! -f "${READER}" ] || [ "${SRC_READER}" -nt "${READER}" ]; then
    CGO_ENABLED=1 go build -o "${READER}" "${SRC_READER}"
fi

# Kill any existing reader and start the new one
pkill -f "${BPF_DIR}/nfqueue_reader" 2>/dev/null || true
sleep 0.2
nohup "${READER}" "${QUEUE_NUM}" >/var/log/icmp-nfqueue.log 2>&1 &
