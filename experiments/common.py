"""Shared helpers for the preliminary experiments."""
import json
import random
import time
from pathlib import Path

import numpy as np

from farmgrid.config import REGIONS, ROOT, admin_dsn, dialect, region_dsn
from farmgrid.db import connect

RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)


def farm_admin(timeout_ms=120000):
    dsn = admin_dsn()
    if dialect() == "crdb":
        dsn = dsn.replace("/defaultdb", "/farm")
    return connect(dsn, statement_timeout_ms=timeout_ms, app="experiment-admin")


def region_conn(region, timeout_ms=10000, app="experiment"):
    return connect(region_dsn(region), statement_timeout_ms=timeout_ms, app=app)


def players_by_region(conn, per_region, min_coins=0, seed=1):
    out = []
    for r in REGIONS:
        rows = conn.execute(
            "SELECT p.player_id::TEXT AS pid, p.home_region::TEXT AS region FROM players p "
            "JOIN wallets w ON w.player_id = p.player_id WHERE p.home_region = %s AND w.coins >= %s "
            "ORDER BY p.player_id LIMIT %s", (r, min_coins, per_region * 5)).fetchall()
        random.Random(seed).shuffle(rows)
        out += rows[:per_region]
    return out


def pct(values, p):
    return float(np.percentile(values, p)) if len(values) else None


def summarize_ms(lat_s):
    ms = [x * 1000 for x in lat_s]
    return {"n": len(ms), "mean_ms": float(np.mean(ms)) if ms else None,
            "p50_ms": pct(ms, 50), "p95_ms": pct(ms, 95), "p99_ms": pct(ms, 99), "max_ms": max(ms) if ms else None}


def save(name, obj):
    path = RESULTS / f"{name}.json"
    with open(path, "w") as fh:
        json.dump(obj, fh, indent=2, default=str)
    print(f"saved {path}")
    return path


def env_info():
    return {"dialect": dialect(), "time": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
