"""Create the schema and load the generated CSVs.

    python -m farmgrid.load [--reset] [--presplit 4] [--data data]

--reset     drop and recreate the farm database
--presplit  split each region's partition of players/farm_plots into N ranges
            (by UUID prefix) and scatter them, so sharding is visible in
            SHOW RANGES even at laptop scale.
"""
import argparse
import csv
import gzip
import os
import re
import time
from pathlib import Path

from farmgrid import audit, ops
from farmgrid.config import REGIONS, ROOT, admin_dsn, dialect, region_dsn
from farmgrid.db import connect, insert_rows

ORDER = ["item_catalog", "players", "wallets", "inventory", "items", "farm_plots",
         "shared_fields", "harvest_log", "presence"]


def statements(path: Path) -> list[str]:
    text = "\n".join(line for line in path.read_text().splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in text.split(";") if s.strip()]


def to_postgres(stmt: str) -> str | None:
    """Local-testing only: turn CockroachDB DDL into PostgreSQL DDL."""
    if stmt.upper().startswith(("CREATE DATABASE", "USE ", "SET CLUSTER")):
        return None
    s = stmt.replace("crdb_internal_region", "TEXT").replace("STRING", "TEXT")
    s = re.sub(r",\s*INDEX \w+ \([^)]*\)", "", s)
    s = re.sub(r"\)\s*LOCALITY[\s\S]*$", ")", s)
    return s


def run_sql_file(conn, path: Path) -> None:
    for st in statements(path):
        if dialect() == "postgres":
            st = to_postgres(st)
            if not st:
                continue
        conn.execute(st)


def read_csv(path: Path):
    with gzip.open(path, "rt", newline="") as fh:
        r = csv.reader(fh)
        cols = next(r)
        rows = [tuple(None if v == "" else v for v in row) for row in r]
    return cols, rows


def presplit(conn, n: int) -> list[str]:
    """Split each region's partition into n ranges at UUID boundaries."""
    log = []
    bounds = [f"{int(i * 16 / n):x}0000000-0000-0000-0000-000000000000" for i in range(1, n)]
    for table, region_col in (("players", "home_region"), ("farm_plots", "region")):
        for reg in REGIONS:
            for b in bounds:
                try:
                    conn.execute(f"ALTER TABLE {table} SPLIT AT VALUES ('{reg}', '{b}'::UUID)")
                    log.append(f"split {table} at ({reg}, {b})")
                except Exception as e:  # keep going; report what happened
                    log.append(f"split {table} ({reg}, {b}) failed: {str(e).splitlines()[0]}")
        try:
            conn.execute(f"ALTER TABLE {table} SCATTER")
        except Exception as e:
            log.append(f"scatter {table} failed: {str(e).splitlines()[0]}")
    return log


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--presplit", type=int, default=0)
    ap.add_argument("--data", default=str(ROOT / "data"))
    a = ap.parse_args()
    t0 = time.time()

    admin = connect(admin_dsn(), statement_timeout_ms=600000, app="loader")
    if dialect() == "crdb":
        license_key = os.environ.get("CRDB_LICENSE")
        if license_key:
            admin.execute("SET CLUSTER SETTING enterprise.license = %s", (license_key,))
        run_sql_file(admin, ROOT / "sql" / "00_cluster.sql")
        if a.reset:
            admin.execute("DROP DATABASE IF EXISTS farm CASCADE")
        run_sql_file(admin, ROOT / "sql" / "01_schema.sql")   # creates + USEs farm
        conn = admin
    else:
        conn = admin
        if a.reset:
            conn.execute("DROP SCHEMA public CASCADE")
            conn.execute("CREATE SCHEMA public")
        run_sql_file(conn, ROOT / "sql" / "01_schema.sql")
    print(f"schema ready in {time.time() - t0:.1f}s")

    data = Path(a.data)
    for table in ORDER:
        t1 = time.time()
        cols, rows = read_csv(data / f"{table}.csv.gz")
        n = insert_rows(conn, table, cols, rows, batch=500)
        print(f"loaded {table:14s} {n:>8,d} rows in {time.time() - t1:6.1f}s")

    if a.presplit and dialect() == "crdb":
        for line in presplit(conn, a.presplit):
            print(line)

    n = ops.refresh_leaderboard(conn)
    print(f"leaderboard refreshed: {n} rows")
    base = audit.rebaseline(conn)
    print(f"audit baseline: total_coins={base['total_coins']:,}")
    print(f"total load time {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
