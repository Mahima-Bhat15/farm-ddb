"""E2 (preliminary): atomic trades, same-region vs cross-region, plus idempotency.

    python -m experiments.e2_trades --workers 12 --seconds 30 --cross 0.2 --replays 50

Workers trade items between random players for --seconds. Each trade is one
SERIALIZABLE transaction touching both players' inventory and wallet rows
(5 writes); when the players live in different regions the transaction spans
ranges with leaseholders in different regions. Then --replays trades are
re-sent with the same request_id to show retries never apply twice. The
invariant audit proves no partial trade happened (coins and items conserved).
"""
import argparse
import random
import threading
import uuid
import time

from experiments.common import env_info, farm_admin, region_conn, save, summarize_ms
from farmgrid import audit, ops
from farmgrid.config import REGIONS
from farmgrid.db import RetryLimit, run_txn


def pick_traders(admin, per_region):
    sellers, buyers = [], []
    for r in REGIONS:
        rows = admin.execute(
            "SELECT i.player_id::TEXT AS pid, i.region::TEXT AS region, i.item_type_id FROM inventory i "
            "WHERE i.region = %s AND i.quantity >= 20 ORDER BY i.player_id LIMIT %s", (r, per_region)).fetchall()
        sellers += rows
        b = admin.execute("SELECT p.player_id::TEXT AS pid, p.home_region::TEXT AS region FROM players p "
                          "WHERE p.home_region = %s ORDER BY p.player_id DESC LIMIT %s", (r, per_region)).fetchall()
        buyers += b
    return sellers, buyers


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--cross", type=float, default=0.2)
    ap.add_argument("--replays", type=int, default=50)
    ap.add_argument("--per-region", type=int, default=60)
    a = ap.parse_args()

    admin = farm_admin()
    sellers, buyers = pick_traders(admin, a.per_region)
    admin.execute("UPDATE wallets SET coins = coins + 1000000 WHERE player_id = ANY(%s)",
                  ([uuid.UUID(b["pid"]) for b in buyers],))
    audit.rebaseline(admin)
    buyers_by_region = {r: [b for b in buyers if b["region"] == r] for r in REGIONS}

    records, lock = [], threading.Lock()
    deadline = time.perf_counter() + a.seconds

    def worker(i):
        rng = random.Random(i)
        conns = {r: region_conn(r, app="e2") for r in REGIONS}
        while time.perf_counter() < deadline:
            s = rng.choice(sellers)
            cross = rng.random() < a.cross
            regs = [r for r in REGIONS if r != s["region"]] if cross else [s["region"]]
            b = rng.choice(buyers_by_region[rng.choice(regs)])
            if b["pid"] == s["pid"]:
                continue
            req = ops.new_request_id()
            c = conns[b["region"]]            # the buyer's regional API/gateway issues the trade
            t0 = time.perf_counter()
            try:
                _, retries = run_txn(c, lambda cn: ops.trade(cn, req, s["pid"], s["region"], b["pid"], b["region"],
                                                             s["item_type_id"], 1, rng.randint(1, 20)),
                                     isolation="SERIALIZABLE")
                out = "ok"
            except ops.GameError as e:
                out, retries = e.code, 0
            except RetryLimit:
                out, retries = "gave_up", 50
            with lock:
                records.append((out, time.perf_counter() - t0, retries, cross))
        for c in conns.values():
            c.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(a.workers)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t_start

    ok = [r for r in records if r[0] == "ok"]
    res = {
        "env": env_info(), "workers": a.workers, "seconds": a.seconds, "cross_share": a.cross,
        "attempted": len(records), "committed": len(ok),
        "rejected": {k: sum(1 for r in records if r[0] == k) for k in {r[0] for r in records if r[0] != "ok"}},
        "throughput_trades_per_s": len(ok) / wall,
        "retries_per_trade": sum(r[2] for r in ok) / max(1, len(ok)),
        "latency_same_region": summarize_ms([r[1] for r in ok if not r[3]]),
        "latency_cross_region": summarize_ms([r[1] for r in ok if r[3]]),
    }

    # Idempotency: send each request twice; the second must be a replay.
    c = region_conn("us-west", app="e2-replay")
    # Fresh sellers that still hold plenty of items.
    rich = admin.execute(
        "SELECT player_id::TEXT AS pid, region::TEXT AS region, item_type_id FROM inventory "
        "WHERE quantity >= 5 ORDER BY quantity DESC LIMIT %s", (a.replays,)).fetchall()
    first_ok, replay_ok = 0, 0
    trades_before = admin.execute("SELECT count(*) AS n FROM trades").fetchone()["n"]
    for k, s in enumerate(rich):
        b = buyers[(k * 7) % len(buyers)]
        if s["pid"] == b["pid"]:
            continue
        req = ops.new_request_id()
        args = (req, s["pid"], s["region"], b["pid"], b["region"], s["item_type_id"], 1, 3)
        try:
            first, _ = run_txn(c, lambda cn: ops.trade(cn, *args), isolation="SERIALIZABLE")
        except ops.GameError:
            continue
        first_ok += 1
        second, _ = run_txn(c, lambda cn: ops.trade(cn, *args), isolation="SERIALIZABLE")
        replay_ok += int(second.get("replayed") is True and second["trade_id"] == first["trade_id"])
    trades_after = admin.execute("SELECT count(*) AS n FROM trades").fetchone()["n"]
    res["idempotency"] = {"requests_committed_then_resent": first_ok, "resends_returned_stored_result": replay_ok,
                          "new_trade_rows": trades_after - trades_before}
    res["audit"] = audit.run_audit(admin)
    print({k: v for k, v in res.items() if k != "env"})
    save("e2_trades", res)


if __name__ == "__main__":
    main()
