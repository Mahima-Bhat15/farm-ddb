"""E1 (preliminary): the Golden Seed Drop - many buyers race for one field.

    python -m experiments.e1_contention --workers 4,16,48 --stock 100 --runs 3

For each concurrency-control mode and each number of concurrent buyers, the
field is restocked with --stock seeds and every buyer keeps buying until it
is sold out. We record latency per successful purchase, retries per purchase
and throughput, then check that exactly --stock seeds were sold (no overselling)
and run the invariant audit.

Modes:
  serializable  SERIALIZABLE, no explicit lock: conflicts abort (40001), client retries
  pessimistic   SERIALIZABLE + SELECT ... FOR UPDATE: buyers queue on the row lock
  optimistic    READ COMMITTED + version check: app-level conflict detection, retry
"""
import argparse
import threading
import uuid
import time

from experiments.common import env_info, farm_admin, players_by_region, region_conn, save, summarize_ms
from farmdb import audit, ops
from farmdb.db import RetryLimit, run_txn

ISOLATION = {"serializable": "SERIALIZABLE", "pessimistic": "SERIALIZABLE", "optimistic": "READ COMMITTED"}


def one_config(admin, mode, workers, stock, field_id, buyers):
    admin.execute("UPDATE shared_fields SET quantity_remaining = %s, initial_quantity = %s, "
                  "epoch = epoch + 1 WHERE field_id = %s", (stock, stock, field_id))
    audit.rebaseline(admin)
    chosen = buyers[:workers]
    conns = [region_conn(b["region"], app=f"e1-{mode}") for b in chosen]
    barrier = threading.Barrier(workers + 1)
    records = [[] for _ in range(workers)]

    def worker(i):
        b, c = chosen[i], conns[i]
        barrier.wait()
        while True:
            t0 = time.perf_counter()
            try:
                _, retries = run_txn(c, lambda cn: ops.buy_from_field(cn, b["pid"], b["region"], field_id, mode),
                                     isolation=ISOLATION[mode])
                records[i].append(("ok", time.perf_counter() - t0, retries))
            except ops.SoldOut:
                records[i].append(("sold_out", time.perf_counter() - t0, 0))
                return
            except RetryLimit:
                records[i].append(("gave_up", time.perf_counter() - t0, 50))
            except Exception as e:  # unexpected: record and stop this worker
                records[i].append(("error:" + type(e).__name__, time.perf_counter() - t0, 0))
                return

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    for t in threads:
        t.start()
    barrier.wait()
    start = time.perf_counter()
    for t in threads:
        t.join()
    wall = time.perf_counter() - start
    for c in conns:
        c.close()

    flat = [r for rec in records for r in rec]
    ok = [r for r in flat if r[0] == "ok"]
    f = admin.execute("SELECT quantity_remaining, epoch FROM shared_fields WHERE field_id = %s", (field_id,)).fetchone()
    sold = admin.execute("SELECT count(*) AS n FROM field_purchases WHERE field_id = %s AND epoch = %s",
                         (field_id, f["epoch"])).fetchone()["n"]
    aud = audit.run_audit(admin)
    return {
        "mode": mode, "workers": workers, "stock": stock,
        "successful_buys": len(ok), "seeds_sold_in_db": sold, "remaining": f["quantity_remaining"],
        "oversold": sold - stock if sold > stock else 0,
        "gave_up": sum(1 for r in flat if r[0] == "gave_up"),
        "errors": sorted({r[0] for r in flat if r[0].startswith("error")}),
        "retries_total": sum(r[2] for r in ok),
        "retries_per_buy": (sum(r[2] for r in ok) / len(ok)) if ok else None,
        "latency": summarize_ms([r[1] for r in ok]),
        "wall_s": wall, "throughput_buys_per_s": len(ok) / wall if wall else None,
        "audit_passed": aud["passed"],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--modes", default="serializable,pessimistic,optimistic")
    ap.add_argument("--workers", default="4,16,48")
    ap.add_argument("--stock", type=int, default=100)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--field", type=int, default=1)
    a = ap.parse_args()
    modes = a.modes.split(",")
    wl = [int(x) for x in a.workers.split(",")]

    admin = farm_admin()
    per_region = max(wl) // 3 + 2
    buyers = players_by_region(admin, per_region)
    # Interleave regions so every configuration has buyers from all three.
    by_r = {}
    for b in buyers:
        by_r.setdefault(b["region"], []).append(b)
    inter = [x for grp in zip(*by_r.values()) for x in grp]
    admin.execute("UPDATE wallets SET coins = coins + 1000000 WHERE player_id = ANY(%s)",
                  ([uuid.UUID(b["pid"]) for b in inter],))
    field = admin.execute("SELECT region::TEXT AS region FROM shared_fields WHERE field_id = %s",
                          (a.field,)).fetchone()

    rows = []
    for run in range(a.runs):
        for mode in modes:
            for w in wl:
                r = one_config(admin, mode, w, a.stock, a.field, inter)
                r["run"] = run + 1
                rows.append(r)
                print(f"run {run + 1} {mode:12s} w={w:3d}  buys={r['successful_buys']:4d} sold_in_db={r['seeds_sold_in_db']:4d} "
                      f"oversold={r['oversold']} p50={r['latency']['p50_ms']:.1f}ms p99={r['latency']['p99_ms']:.1f}ms "
                      f"retries/buy={r['retries_per_buy']:.2f} thr={r['throughput_buys_per_s']:.1f}/s audit={r['audit_passed']}")
    save("e1_contention", {"env": env_info(), "field_region": field["region"], "stock": a.stock, "rows": rows})


if __name__ == "__main__":
    main()
