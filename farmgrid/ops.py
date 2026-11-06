"""Game operations: the business logic the API exposes.

Each function runs inside a transaction opened by db.run_txn(). Which
consistency tier an operation uses is decided here:

  critical  (buy_from_field, trade)    SERIALIZABLE (+ FOR UPDATE or OCC variants)
  general   (get_player, plant, ...)   strong reads/writes via the leaseholder
  delay-tolerant (leaderboard)          follower read AS OF SYSTEM TIME
"""
import json
import uuid

from farmgrid.config import dialect


class GameError(Exception):
    code = "game_error"


class NotFound(GameError):
    code = "not_found"


class SoldOut(GameError):
    code = "sold_out"


class InsufficientFunds(GameError):
    code = "insufficient_funds"


class InsufficientItems(GameError):
    code = "insufficient_items"


class NotReady(GameError):
    code = "not_ready"


# --- general tier --------------------------------------------------------------

def get_player(conn, player_id):
    row = conn.execute(
        "SELECT p.player_id::TEXT AS player_id, p.username, p.home_region::TEXT AS region, p.level, w.coins "
        "FROM players p JOIN wallets w ON w.player_id = p.player_id WHERE p.player_id = %s",
        (player_id,)).fetchone()
    if not row:
        raise NotFound(f"player {player_id}")
    return row


def get_farm(conn, player_id):
    plots = conn.execute(
        "SELECT plot_id::TEXT AS plot_id, crop_type_id, ready_at, version FROM farm_plots "
        "WHERE owner_id = %s ORDER BY plot_id", (player_id,)).fetchall()
    inv = conn.execute(
        "SELECT item_type_id, quantity FROM inventory WHERE player_id = %s AND quantity > 0 "
        "ORDER BY item_type_id", (player_id,)).fetchall()
    return {"plots": plots, "inventory": inv}


def add_inventory(conn, player_id, region, item_type_id, qty):
    cur = conn.execute(
        "UPDATE inventory SET quantity = quantity + %s WHERE player_id = %s AND item_type_id = %s",
        (qty, player_id, item_type_id))
    if cur.rowcount == 0:
        conn.execute(
            "INSERT INTO inventory (player_id, item_type_id, region, quantity) VALUES (%s, %s, %s, %s)",
            (player_id, item_type_id, region, qty))


def plant(conn, player_id, plot_id, crop_type_id):
    crop = conn.execute("SELECT growth_seconds FROM item_catalog WHERE item_type_id = %s AND kind = 'crop'",
                        (crop_type_id,)).fetchone()
    if not crop:
        raise NotFound(f"crop {crop_type_id}")
    row = conn.execute(
        "UPDATE farm_plots SET crop_type_id = %s, planted_at = now(), "
        "ready_at = now() + %s::INTERVAL, version = version + 1 "
        "WHERE plot_id = %s AND owner_id = %s AND crop_type_id IS NULL "
        "RETURNING plot_id::TEXT AS plot_id, ready_at",
        (crop_type_id, f"{crop['growth_seconds']} seconds", plot_id, player_id)).fetchone()
    if not row:
        raise NotReady("plot is not empty or not yours")
    return row


def harvest(conn, player_id, region, plot_id, yield_qty):
    row = conn.execute(
        "SELECT crop_type_id, version FROM farm_plots WHERE plot_id = %s AND owner_id = %s "
        "AND crop_type_id IS NOT NULL AND ready_at <= now()", (plot_id, player_id)).fetchone()
    if not row:
        raise NotReady("nothing ready to harvest on this plot")
    # Same transaction (serializable), so nobody can harvest this plot in between.
    conn.execute(
        "UPDATE farm_plots SET crop_type_id = NULL, planted_at = NULL, ready_at = NULL, version = version + 1 "
        "WHERE plot_id = %s", (plot_id,))
    add_inventory(conn, player_id, region, row["crop_type_id"], yield_qty)
    conn.execute(
        "INSERT INTO harvest_log (player_id, region, source, source_id, item_type_id, quantity, kind) "
        "VALUES (%s, %s, 'plot', %s, %s, %s, 'live')",
        (player_id, region, str(plot_id), row["crop_type_id"], yield_qty))
    return {"crop_type_id": row["crop_type_id"], "quantity": yield_qty}


# --- critical tier: contended purchase -------------------------------------------

def buy_from_field(conn, player_id, region, field_id, mode="serializable"):
    """Buy one Golden Seed from a shared field.

    mode = 'serializable' : plain read + write; CockroachDB aborts conflicting
                            transactions (40001) and run_txn retries them.
    mode = 'pessimistic'  : SELECT ... FOR UPDATE locks the field row first, so
                            competing buyers queue instead of restarting.
    mode = 'optimistic'   : run under READ COMMITTED; the UPDATE only succeeds if
                            the row's version is unchanged since we read it,
                            otherwise OptimisticConflict -> retry.
    """
    from farmgrid.db import OptimisticConflict

    lock = " FOR UPDATE" if mode == "pessimistic" else ""
    f = conn.execute(
        "SELECT quantity_remaining, item_type_id, version, epoch FROM shared_fields WHERE field_id = %s" + lock,
        (field_id,)).fetchone()
    if not f:
        raise NotFound(f"field {field_id}")
    if f["quantity_remaining"] <= 0:
        raise SoldOut(f"field {field_id} is sold out")
    if mode == "optimistic":
        cur = conn.execute(
            "UPDATE shared_fields SET quantity_remaining = quantity_remaining - 1, version = version + 1 "
            "WHERE field_id = %s AND version = %s", (field_id, f["version"]))
        if cur.rowcount == 0:
            raise OptimisticConflict()
    else:
        conn.execute(
            "UPDATE shared_fields SET quantity_remaining = quantity_remaining - 1, version = version + 1 "
            "WHERE field_id = %s", (field_id,))
    price = conn.execute("SELECT base_value FROM item_catalog WHERE item_type_id = %s",
                         (f["item_type_id"],)).fetchone()["base_value"]
    cur = conn.execute("UPDATE wallets SET coins = coins - %s, updated_at = now() "
                       "WHERE player_id = %s AND coins >= %s", (price, player_id, price))
    if cur.rowcount == 0:
        raise InsufficientFunds("not enough coins")
    add_inventory(conn, player_id, region, f["item_type_id"], 1)
    conn.execute("INSERT INTO field_purchases (field_id, epoch, player_id, region, price) "
                 "VALUES (%s, %s, %s, %s, %s)", (field_id, f["epoch"], player_id, region, price))
    return {"field_id": field_id, "price": price, "remaining": f["quantity_remaining"] - 1}


# --- critical tier: distributed transaction across players (and regions) --------

def trade(conn, request_id, seller_id, seller_region, buyer_id, buyer_region, item_type_id, qty, price):
    """Seller gives qty items to buyer for price coins. All five writes commit
    together or not at all, even when seller and buyer live in different
    regions (different ranges/leaseholders). request_id makes retries safe."""
    from psycopg.types.json import Jsonb

    prev = conn.execute("SELECT response FROM requests WHERE request_id = %s", (request_id,)).fetchone()
    if prev:
        r = prev["response"]
        return (json.loads(r) if isinstance(r, str) else r) | {"replayed": True}
    cur = conn.execute("UPDATE inventory SET quantity = quantity - %s WHERE player_id = %s "
                       "AND item_type_id = %s AND quantity >= %s", (qty, seller_id, item_type_id, qty))
    if cur.rowcount == 0:
        raise InsufficientItems("seller does not have enough items")
    cur = conn.execute("UPDATE wallets SET coins = coins - %s, updated_at = now() "
                       "WHERE player_id = %s AND coins >= %s", (price, buyer_id, price))
    if cur.rowcount == 0:
        raise InsufficientFunds("buyer does not have enough coins")
    conn.execute("UPDATE wallets SET coins = coins + %s, updated_at = now() WHERE player_id = %s",
                 (price, seller_id))
    add_inventory(conn, buyer_id, buyer_region, item_type_id, qty)
    t = conn.execute(
        "INSERT INTO trades (request_id, seller_id, buyer_id, item_type_id, quantity, price, region) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING trade_id::TEXT AS trade_id",
        (request_id, seller_id, buyer_id, item_type_id, qty, price, seller_region)).fetchone()
    resp = {"trade_id": t["trade_id"], "status": "committed"}
    conn.execute("INSERT INTO requests (request_id, region, endpoint, response) VALUES (%s, %s, 'trade', %s)",
                 (request_id, seller_region, Jsonb(resp)))
    return resp


# --- delay-tolerant tier -----------------------------------------------------------

def follower_clause(follower: bool) -> str:
    return " AS OF SYSTEM TIME follower_read_timestamp()" if follower and dialect() == "crdb" else ""


def leaderboard(conn, limit=10, follower=True):
    return conn.execute(
        "SELECT player_id::TEXT AS player_id, region, score FROM leaderboard" + follower_clause(follower) +
        " ORDER BY score DESC LIMIT %s", (limit,)).fetchall()


def refresh_leaderboard(conn):
    """Asynchronous derivation: recompute scores from committed harvests."""
    cur = conn.execute(
        "INSERT INTO leaderboard (player_id, region, score, updated_at) "
        "SELECT player_id, region::TEXT, sum(quantity), now() FROM harvest_log GROUP BY player_id, region "
        "ON CONFLICT (player_id) DO UPDATE SET score = excluded.score, updated_at = excluded.updated_at")
    return cur.rowcount


def touch_presence(conn, player_id, region, ttl_minutes=5):
    conn.execute(
        "INSERT INTO presence (player_id, region, status, last_seen, expires_at) "
        "VALUES (%s, %s, 'online', now(), now() + %s::INTERVAL) "
        "ON CONFLICT (player_id) DO UPDATE SET last_seen = excluded.last_seen, "
        "expires_at = excluded.expires_at, status = 'online'",
        (player_id, region, f"{ttl_minutes} minutes"))


def online_count(conn, region=None, follower=True):
    q = "SELECT count(*) AS n FROM presence" + follower_clause(follower) + " WHERE expires_at > now()"
    if region:
        return conn.execute(q + " AND region = %s", (region,)).fetchone()["n"]
    return conn.execute(q).fetchone()["n"]


def new_request_id() -> str:
    return str(uuid.uuid4())
