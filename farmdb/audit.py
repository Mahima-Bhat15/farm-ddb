"""Invariant audit: proves the game economy stayed correct after a test.

    python -m farmdb.audit            # check against the stored baseline
    python -m farmdb.audit --rebaseline

Conserved quantities (no transaction may create or destroy them):
  coins:  sum(wallets.coins) + sum(field_purchases.price) == baseline
  items:  for each item type, inventory + seeds left in fields
          - items created by live harvests == baseline
Never-violated rules:
  no negative inventory / coins / field stock
  each field sold exactly (initial - remaining) seeds in the current epoch
  each trade request_id produced at most one trade (idempotency)
"""
import argparse
import json
import sys

from farmdb.config import admin_dsn, dialect
from farmdb.db import connect


def _q1(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()


def snapshot(conn) -> dict:
    coins = _q1(conn, "SELECT coalesce(sum(coins), 0)::INT8 AS v FROM wallets")["v"]
    spent = _q1(conn, "SELECT coalesce(sum(price), 0)::INT8 AS v FROM field_purchases")["v"]
    items = {}
    for r in conn.execute("SELECT item_type_id, sum(quantity)::INT8 AS q FROM inventory GROUP BY 1").fetchall():
        items[r["item_type_id"]] = items.get(r["item_type_id"], 0) + r["q"]
    for r in conn.execute("SELECT item_type_id, sum(quantity_remaining)::INT8 AS q FROM shared_fields GROUP BY 1").fetchall():
        items[r["item_type_id"]] = items.get(r["item_type_id"], 0) + r["q"]
    for r in conn.execute("SELECT item_type_id, sum(quantity)::INT8 AS q FROM harvest_log WHERE kind = 'live' GROUP BY 1").fetchall():
        items[r["item_type_id"]] = items.get(r["item_type_id"], 0) - r["q"]
    return {"total_coins": coins + spent, "items": items}


def rebaseline(conn) -> dict:
    s = snapshot(conn)
    with conn.transaction():
        conn.execute("DELETE FROM audit_baseline WHERE true")
        conn.execute("INSERT INTO audit_baseline (k, v) VALUES ('total_coins', %s)", (s["total_coins"],))
        for t, q in s["items"].items():
            conn.execute("INSERT INTO audit_baseline (k, v) VALUES (%s, %s)", (f"item:{t}", q))
    return s


def run_audit(conn) -> dict:
    base = {r["k"]: r["v"] for r in conn.execute("SELECT k, v FROM audit_baseline").fetchall()}
    now = snapshot(conn)
    item_diffs = {}
    for t, q in now["items"].items():
        b = base.get(f"item:{t}", 0)
        if q != b:
            item_diffs[str(t)] = q - b
    for k, b in base.items():
        if k.startswith("item:") and int(k[5:]) not in now["items"] and b != 0:
            item_diffs[k[5:]] = -b
    oversold = _q1(conn, """
        SELECT count(*) AS n FROM shared_fields f
        WHERE f.initial_quantity - f.quantity_remaining <>
              (SELECT count(*) FROM field_purchases p WHERE p.field_id = f.field_id AND p.epoch = f.epoch)""")["n"]
    checks = {
        "coins_conserved": now["total_coins"] == base.get("total_coins"),
        "coins_difference": now["total_coins"] - base.get("total_coins", 0),
        "items_conserved": not item_diffs,
        "item_differences": item_diffs,
        "negative_inventory_rows": _q1(conn, "SELECT count(*) AS n FROM inventory WHERE quantity < 0")["n"],
        "negative_wallets": _q1(conn, "SELECT count(*) AS n FROM wallets WHERE coins < 0")["n"],
        "negative_field_stock": _q1(conn, "SELECT count(*) AS n FROM shared_fields WHERE quantity_remaining < 0")["n"],
        "fields_with_sales_mismatch": oversold,
        "duplicate_trades_per_request": _q1(conn, "SELECT count(*) AS n FROM (SELECT request_id FROM trades "
                                                  "GROUP BY request_id HAVING count(*) > 1) d")["n"],
        "wallets_without_player": _q1(conn, "SELECT count(*) AS n FROM wallets w WHERE NOT EXISTS "
                                            "(SELECT 1 FROM players p WHERE p.player_id = w.player_id)")["n"],
    }
    checks["passed"] = (checks["coins_conserved"] and checks["items_conserved"] and
                        all(checks[k] == 0 for k in ("negative_inventory_rows", "negative_wallets",
                                                     "negative_field_stock", "fields_with_sales_mismatch",
                                                     "duplicate_trades_per_request", "wallets_without_player")))
    return checks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebaseline", action="store_true")
    a = ap.parse_args()
    dsn = admin_dsn()
    if dialect() == "crdb" and dsn.endswith("/defaultdb?sslmode=disable"):
        dsn = dsn.replace("/defaultdb", "/farm")
    conn = connect(dsn, statement_timeout_ms=120000, app="audit")
    if a.rebaseline:
        rebaseline(conn)
        print("baseline stored")
    res = run_audit(conn)
    print(json.dumps(res, indent=2, default=str))
    sys.exit(0 if res["passed"] else 1)


if __name__ == "__main__":
    main()
