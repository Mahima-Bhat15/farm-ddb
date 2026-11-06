#!/usr/bin/env bash
# Inject one-way delay between nodes of DIFFERENT regions (same-region traffic
# stays fast), simulating geographic distance on a single machine.
#   scripts/netem.sh 50     # 50 ms each way between regions (~100 ms round trip)
#   scripts/netem.sh 0      # remove all delays
# Uses a throwaway netshoot container that shares each node's network namespace.
set -euo pipefail
DELAY_MS="${1:-0}"
PROJECT="${COMPOSE_PROJECT_NAME:-farm-ddb}"
IMG="nicolaka/netshoot:v0.13"

region_of() { case "$1" in roach1|roach2|roach3) echo us-west;; roach4|roach5|roach6) echo us-east;; *) echo eu-west;; esac; }
cid() { docker ps -q --filter "label=com.docker.compose.project=$PROJECT" --filter "label=com.docker.compose.service=$1"; }
ip_of() { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$(cid "$1")"; }

NODES="roach1 roach2 roach3 roach4 roach5 roach6 roach7 roach8 roach9"
# (macOS ships bash 3.2: no associative arrays, so look IPs up as needed)

for n in $NODES; do
  others=""
  for m in $NODES; do
    if [ "$(region_of "$m")" != "$(region_of "$n")" ]; then others="$others $(ip_of "$m")"; fi
  done
  script="tc qdisc del dev eth0 root 2>/dev/null || true"
  if [ "$DELAY_MS" != "0" ]; then
    script="$script; tc qdisc add dev eth0 root handle 1: prio bands 4"
    script="$script; tc qdisc add dev eth0 parent 1:4 handle 40: netem delay ${DELAY_MS}ms"
    for ip in $others; do
      script="$script; tc filter add dev eth0 parent 1:0 protocol ip prio 1 u32 match ip dst ${ip}/32 flowid 1:4"
    done
  fi
  docker run --rm --net "container:$(cid "$n")" --cap-add NET_ADMIN "$IMG" sh -c "$script"
  echo "$n ($(region_of "$n")): ${DELAY_MS} ms to other regions"
done
