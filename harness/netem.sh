#!/bin/sh
# Impair this container's mesh link (172.30.0.0/24), never the control link
# the harness polls on. Egress only: every exchange is request/response, so
# dropping one direction is enough to lose it.
#
#   netem loss 20%
#   netem loss gemodel 20% 50%
#   netem loss 100%        (partition)
#   netem clear
#   netem txbytes          (bytes sent on the mesh link so far, headers and retransmits included)
set -eu

dev=$(ip -o -4 addr show | awk '$4 ~ /^172\.30\.0\./ { print $2; exit }')
if [ -z "$dev" ]; then
  echo "netem: no interface on the mesh network 172.30.0.0/24 in this container" >&2
  exit 1
fi

if [ "${1:-}" = "txbytes" ]; then
  cat "/sys/class/net/$dev/statistics/tx_bytes"
  exit 0
fi

if [ "${1:-}" = "clear" ]; then
  if tc qdisc show dev "$dev" | grep -q netem; then
    tc qdisc del dev "$dev" root
  fi
else
  tc qdisc replace dev "$dev" root netem "$@"
fi
tc qdisc show dev "$dev"
