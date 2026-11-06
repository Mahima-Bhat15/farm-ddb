"""E4 (preliminary): strong reads vs follower reads for the delay-tolerant tier.

    python -m experiments.e4_follower --seconds 20 --reader eu-west --label rtt0

A writer in us-west updates a heartbeat row every 50 ms with its wall-clock
time. A reader connected to a gateway in --reader alternates:
  strong read   : SELECT ... (served by the leaseholder in us-west)
  follower read : SELECT ... AS OF SYSTEM TIME follower_read_timestamp()
                  (served by the nearest replica, a few seconds in the past)
For each read we record latency and staleness = now - heartbeat value read.
The same comparison is made for the leaderboard top-10 query.
Run once per injected inter-region delay (scripts/netem.sh) with a --label.
"""
import argparse
import threading
import time

from experiments.common import env_info, region_conn, save, summarize_ms
from farmdb import ops


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=20)
    ap.add_argument("--reader", default="eu-west")
    ap.add_argument("--label", default="rtt0")
    a = ap.parse_args()

    w = region_conn("us-west", app="e4-writer")
    w.execute("INSERT INTO heartbeat (id, client_ts) VALUES (1, %s) ON CONFLICT (id) DO UPDATE SET client_ts = excluded.client_ts",
              (time.time(),))
    time.sleep(6)          # let the row become visible to follower reads
    stop = threading.Event()

    def writer():
        while not stop.is_set():
            w.execute("UPDATE heartbeat SET client_ts = %s WHERE id = 1", (time.time(),))
            time.sleep(0.05)

    wt = threading.Thread(target=writer)
    wt.start()
    r = region_conn(a.reader, app="e4-reader")
    hb = {"strong": [], "follower": []}
    stale = {"strong": [], "follower": []}
    lb = {"strong": [], "follower": []}
    end = time.perf_counter() + a.seconds
    while time.perf_counter() < end:
        for kind in ("strong", "follower"):
            t0 = time.perf_counter()
            row = r.execute("SELECT client_ts FROM heartbeat" + ops.follower_clause(kind == "follower") +
                            " WHERE id = 1").fetchone()
            hb[kind].append(time.perf_counter() - t0)
            stale[kind].append(time.time() - row["client_ts"])
            t0 = time.perf_counter()
            ops.leaderboard(r, 10, follower=(kind == "follower"))
            lb[kind].append(time.perf_counter() - t0)
        time.sleep(0.02)
    stop.set()
    wt.join()
    # Was the follower read actually served by a local replica? Ask EXPLAIN ANALYZE.
    plan = [x[next(iter(x))] for x in r.execute(
        "EXPLAIN ANALYZE SELECT client_ts FROM heartbeat" + ops.follower_clause(True) + " WHERE id = 1").fetchall()]
    res = {
        "env": env_info(), "label": a.label, "reader_region": a.reader, "seconds": a.seconds,
        "heartbeat_latency": {k: summarize_ms(v) for k, v in hb.items()},
        "leaderboard_latency": {k: summarize_ms(v) for k, v in lb.items()},
        "staleness_s": {k: {"mean": sum(v) / len(v), "max": max(v), "min": min(v)} for k, v in stale.items()},
        "follower_read_plan": plan,
    }
    print({k: v for k, v in res.items() if k not in ("env", "follower_read_plan")})
    save(f"e4_follower_{a.label}", res)


if __name__ == "__main__":
    main()
