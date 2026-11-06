#!/usr/bin/env bash
# End-to-end: start the cluster, generate + load data, run the preliminary
# experiments, inspect the cluster, draw figures. Logs go to results/*.log.
# Usage: scripts/run_all.sh            (from the repository root)
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=farmgrid

echo "== 1. start 9 nodes, HAProxy, tools, APIs"
docker compose up -d --build
echo "== 2. initialise the cluster (first run only)"
docker compose --profile setup run --rm init || echo "(already initialised)"
until docker compose exec -T roach1 ./cockroach sql --insecure -e "SELECT 1" >/dev/null 2>&1; do sleep 2; done

T="docker compose exec -T tools"
echo "== 3. generate synthetic data";        $T python -m farmgrid.generate            | tee results/generate.log
echo "== 4. create schema + load";            $T python -m farmgrid.load --reset --presplit 4 | tee results/load.log
echo "== 5. cluster inspection";              $T python -m experiments.inspect_cluster > results/inspect.log
echo "== 6. E1 contention";                   $T python -m experiments.e1_contention --workers 4,16,48 --stock 100 --runs 3 | tee results/e1.log
echo "== 7. E2 trades";                       $T python -m experiments.e2_trades --workers 12 --seconds 30 | tee results/e2.log
echo "== 8. E4 follower reads at 0/50/100 ms inter-region RTT"
for d in 0 25 50; do
  scripts/netem.sh "$d" >/dev/null
  $T python -m experiments.e4_follower --seconds 20 --label "rtt$((2 * d))" | tee -a results/e4.log
done
scripts/netem.sh 0 >/dev/null
echo "== 9. E3 node failure";                 $T python -m experiments.e3_failover --seconds 75 --kill-at 25 --restart-at 50 --node roach2 | tee results/e3.log
echo "== 10. audit + figures"
$T python -m farmgrid.audit | tee results/audit.log
$T python -m experiments.plots
echo "done: see results/"
