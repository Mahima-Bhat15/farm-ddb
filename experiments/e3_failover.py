"""E3 (preliminary): a node crashes under load.

    python -m experiments.e3_failover --seconds 75 --kill-at 25 --restart-at 50 --node roach2

Workers in all three regions continuously (a) insert a durability probe row
and (b) read a player, through their regional HAProxy endpoint. At --kill-at
the --node container is killed (SIGKILL, like a power loss); at --restart-at
it is started again. We record every request's time, outcome and latency, and
afterwards check that every write the client saw acknowledged is in the database.
"""
import argparse
import os
import random
import threading
import time
import uuid

from experiments.common import env_info, farm_admin, players_by_region, region_conn, save, summarize_ms
from farmdb.config import REGIONS


def find_container(service):
    import docker
    client = docker.from_env()
    project = os.environ.get("COMPOSE_PROJECT_NAME")
    filters = {"label": [f"com.docker.compose.service={service}"]}
    if project:
        filters["label"].append(f"com.docker.compose.project={project}")
    cs = client.containers.list(all=True, filters=filters)
    if not cs:
        raise RuntimeError(f"container for service {service} not found")
    return cs[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=75)
    ap.add_argument("--kill-at", type=float, default=25)
    ap.add_argument("--restart-at", type=float, default=50)
    ap.add_argument("--node", default="roach2")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--no-kill", action="store_true", help="dry run without stopping a node")
    a = ap.parse_args()

    admin = farm_admin()
    pool = players_by_region(admin, 50)
    by_region = {r: [p for p in pool if p["region"] == r] for r in REGIONS}
    container = None if a.no_kill else find_container(a.node)

    records, acked, lock = [], [], threading.Lock()
    t0 = time.perf_counter()
    end = t0 + a.seconds
    events = []

    def worker(i):
        region = REGIONS[i % 3]
        rng = random.Random(i)
        conn = None
        while time.perf_counter() < end:
            start = time.perf_counter()
            pid = None
            try:
                if conn is None or conn.closed:
                    conn = region_conn(region, timeout_ms=5000, app="e3")
                pid = uuid.uuid4()
                with conn.transaction():
                    conn.execute("INSERT INTO durability_probe (probe_id, region, worker) VALUES (%s, %s, %s)",
                                 (pid, region, i))
                p = rng.choice(by_region[region])
                conn.execute("SELECT level FROM players WHERE player_id = %s", (p["pid"],)).fetchone()
                ok, err = True, None
            except Exception as e:
                ok, err = False, type(e).__name__
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass
                conn = None
            done = time.perf_counter()
            with lock:
                records.append((start - t0, ok, done - start, region, err))
                if ok:
                    acked.append(pid)
            if not ok:
                time.sleep(0.05)
        if conn is not None:
            conn.close()

    def controller():
        if container is None:
            return
        time.sleep(max(0, t0 + a.kill_at - time.perf_counter()))
        container.kill()
        events.append(("kill", time.perf_counter() - t0))
        time.sleep(max(0, t0 + a.restart_at - time.perf_counter()))
        container.start()
        events.append(("restart", time.perf_counter() - t0))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(a.workers)]
    ctl = threading.Thread(target=controller)
    for t in threads:
        t.start()
    ctl.start()
    for t in threads:
        t.join()
    ctl.join()

    # Per-second timeline
    secs = int(a.seconds) + 1
    tl = [{"t": s, "ok": 0, "fail": 0} for s in range(secs)]
    for t, ok, _lat, _r, _e in records:
        s = min(int(t), secs - 1)
        tl[s]["ok" if ok else "fail"] += 1
    for row in tl:
        tot = row["ok"] + row["fail"]
        row["success_rate"] = row["ok"] / tot if tot else None

    kill_t = next((t for e, t in events if e == "kill"), None)
    restart_t = next((t for e, t in events if e == "restart"), None)
    recovery_s = None
    if kill_t is not None:
        pre = [r["ok"] for r in tl[5:int(kill_t)]]
        base = sum(pre) / max(1, len(pre))
        # first second after the kill from which 3 consecutive seconds are
        # >= 99% successful and >= 80% of pre-kill throughput
        for s in range(int(kill_t), secs - 3):
            win = tl[s:s + 3]
            if all(w["success_rate"] is not None and w["success_rate"] >= 0.99 and w["ok"] >= 0.8 * base for w in win):
                recovery_s = s - kill_t
                break

    # Durability check: every acknowledged probe must exist.
    missing = 0
    for i in range(0, len(acked), 1000):
        chunk = acked[i:i + 1000]
        found = admin.execute("SELECT count(*) AS n FROM durability_probe WHERE probe_id = ANY(%s)", (chunk,)).fetchone()["n"]
        missing += len(chunk) - found

    in_window = [r for r in records if kill_t is not None and kill_t <= r[0] < (restart_t or a.seconds)]
    errs = {}
    for r in records:
        if r[4]:
            errs[r[4]] = errs.get(r[4], 0) + 1
    res = {
        "env": env_info(), "node_killed": None if a.no_kill else a.node, "events": events,
        "requests": len(records), "succeeded": sum(1 for r in records if r[1]),
        "success_rate_overall": sum(1 for r in records if r[1]) / max(1, len(records)),
        "success_rate_while_node_down": (sum(1 for r in in_window if r[1]) / len(in_window)) if in_window else None,
        "failed_requests_total": sum(1 for r in records if not r[1]),
        "error_types": errs,
        "recovery_time_s": recovery_s,
        "acknowledged_writes": len(acked), "acknowledged_writes_missing": missing,
        "latency_before_kill": summarize_ms([r[2] for r in records if r[1] and kill_t and r[0] < kill_t]),
        "latency_while_down": summarize_ms([r[2] for r in in_window if r[1]]),
        "timeline": tl,
    }
    print({k: v for k, v in res.items() if k not in ("timeline", "env")})
    save("e3_failover", res)


if __name__ == "__main__":
    main()
