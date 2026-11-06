# FarmGrid: a tiered-consistency distributed database backend for a multiplayer farming game

CSE 512 Group 11 prototype. A 9-node CockroachDB cluster in three simulated
regions (us-west, us-east, eu-west) stores a synthetic farming-game world. A
stateless FastAPI service (one per region) exposes game actions. Each kind of
game data gets the weakest consistency that is still correct for it:

| Tier | Data | Mechanism |
| --- | --- | --- |
| Critical | wallets, inventory, items, trades, shared fields | SERIALIZABLE transactions with retry; `FOR UPDATE` or optimistic version checks on hot rows; idempotency keys |
| General | players, farm plots | strong reads/writes through the range leaseholder (Raft majority) |
| Delay-tolerant | leaderboard, presence | follower reads `AS OF SYSTEM TIME follower_read_timestamp()`, row-level TTL |

## Run it

Requires Docker Desktop (about 6 GB of memory for Docker).

```bash
scripts/run_all.sh          # everything: cluster, data, experiments, figures
```

or step by step:

```bash
docker compose up -d --build
docker compose --profile setup run --rm init
docker compose exec tools python -m farmgrid.generate
docker compose exec tools python -m farmgrid.load --reset --presplit 4
docker compose exec tools python -m farmgrid.audit
```

- DB Console: http://localhost:8080 (also :8081, :8082)
- HAProxy stats: http://localhost:8404
- API docs: http://localhost:8001/docs (us-west), :8002 (us-east), :8003 (eu-west)
- Geographic delay: `scripts/netem.sh 50` adds 50 ms each way between regions; `scripts/netem.sh 0` removes it.

## Files

| Path | What it does |
| --- | --- |
| `docker-compose.yml` | 9 nodes with `--locality` region/zone tags, HAProxy, tools container, 3 regional APIs |
| `haproxy/haproxy.cfg` | per-region ports; local nodes first, other regions as backup; health checks on `/health?ready=1` |
| `sql/01_schema.sql` | multi-region database; REGIONAL BY ROW / GLOBAL / REGIONAL BY TABLE localities; CHECK constraints; TTL |
| `config/default.yaml` | every data-generation and workload parameter, plus the random seed |
| `farmgrid/generate.py` | synthetic data pipeline: generate, validate, write CSVs, report, profile figure |
| `farmgrid/load.py` | schema creation, batched load, optional pre-splitting into more ranges |
| `farmgrid/ops.py` | game operations and their consistency tier |
| `farmgrid/db.py` | connections and the 40001 retry loop |
| `farmgrid/audit.py` | invariant audit: coins and items conserved, no negatives, no oversold field, no duplicate trades |
| `api/main.py` | FastAPI endpoints |
| `experiments/e1_contention.py` | Golden Seed Drop: serializable vs pessimistic vs optimistic |
| `experiments/e2_trades.py` | atomic same-/cross-region trades and idempotent retries |
| `experiments/e3_failover.py` | kill a node under load; success rate, recovery time, lost acknowledged writes |
| `experiments/e4_follower.py` | strong vs follower read latency and staleness under injected delay |
| `experiments/inspect_cluster.py` | partitions, ranges, leaseholders, EXPLAIN ANALYZE plans |
| `experiments/plots.py` | figures from `results/*.json` |

`FARM_DIALECT=postgres` runs the logic against a plain PostgreSQL (no
multi-region features) for quick local testing only; all reported results
come from the CockroachDB cluster.
