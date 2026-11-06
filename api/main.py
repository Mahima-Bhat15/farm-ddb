"""FarmGrid game API: a stateless FastAPI service, one instance per region.

    uvicorn api.main:app --port 8000      (env: REGION, FARM_DSN)

Every endpoint is one database transaction (with retry). The consistency tier
of each endpoint is listed in its docstring and in GET /tiers.
"""
import os
import random
import threading
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from farmgrid import ops
from farmgrid.db import RetryLimit, run_txn

REGION = os.environ.get("REGION", "us-west")
DSN = os.environ.get("FARM_DSN", "postgresql://root@localhost:26257/farm?sslmode=disable")
CROP_YIELD = {1: 4, 2: 3, 3: 5, 4: 4, 5: 3, 6: 1, 7: 6, 8: 2}

pool: ConnectionPool | None = None
stats = {"requests": 0, "retries": 0, "errors": 0}


def _configure(conn):
    conn.autocommit = True
    conn.row_factory = dict_row
    conn.execute("SET statement_timeout = '10s'")


def leaderboard_refresher(stop: threading.Event, every_s: float):
    """Asynchronous derivation of the leaderboard (delay-tolerant tier).
    Only the us-west instance refreshes, so there is one writer."""
    while not stop.wait(every_s):
        try:
            with pool.connection() as c:
                ops.refresh_leaderboard(c)
        except Exception:
            pass


@asynccontextmanager
async def lifespan(_app):
    global pool
    pool = ConnectionPool(DSN, min_size=2, max_size=20, configure=_configure, open=True)
    stop = threading.Event()
    if REGION == "us-west" and os.environ.get("LEADERBOARD_REFRESH", "1") == "1":
        threading.Thread(target=leaderboard_refresher, args=(stop, 10.0), daemon=True).start()
    yield
    stop.set()
    pool.close()


app = FastAPI(title="FarmGrid farm-game API", lifespan=lifespan)


def tx(fn, isolation="SERIALIZABLE"):
    stats["requests"] += 1
    try:
        with pool.connection() as c:
            result, retries = run_txn(c, fn, isolation=isolation)
        stats["retries"] += retries
        return result
    except ops.NotFound as e:
        raise HTTPException(404, {"code": e.code, "detail": str(e)})
    except ops.GameError as e:
        raise HTTPException(409, {"code": e.code, "detail": str(e)})
    except RetryLimit as e:
        stats["errors"] += 1
        raise HTTPException(503, {"code": "contention", "detail": str(e)})


class PlantReq(BaseModel):
    player_id: uuid.UUID
    plot_id: uuid.UUID
    crop_type_id: int


class HarvestReq(BaseModel):
    player_id: uuid.UUID
    plot_id: uuid.UUID


class BuyReq(BaseModel):
    player_id: uuid.UUID
    mode: str = Field("pessimistic", pattern="^(serializable|pessimistic|optimistic)$")


class TradeReq(BaseModel):
    request_id: uuid.UUID
    seller_id: uuid.UUID
    buyer_id: uuid.UUID
    item_type_id: int
    quantity: int = Field(1, gt=0)
    price: int = Field(..., ge=0)


@app.get("/health")
def health():
    with pool.connection() as c:
        c.execute("SELECT 1")
    return {"status": "ok", "region": REGION, **stats}


@app.get("/tiers")
def tiers():
    return {
        "critical (serializable transactions)": ["POST /fields/{id}/buy", "POST /trade"],
        "general (strong reads, majority-replicated writes)": ["GET /players/{id}", "GET /players/{id}/farm",
                                                               "POST /plant", "POST /harvest"],
        "delay-tolerant (follower reads, TTL)": ["GET /leaderboard", "POST /presence/{id}", "GET /presence/online"],
    }


@app.get("/players/{player_id}")
def get_player(player_id: uuid.UUID):
    """General tier: strong read from the leaseholder."""
    return tx(lambda c: ops.get_player(c, player_id))


@app.get("/players/{player_id}/farm")
def get_farm(player_id: uuid.UUID):
    """General tier."""
    return tx(lambda c: ops.get_farm(c, player_id))


@app.post("/plant")
def plant(req: PlantReq):
    """General tier: single-player write, usually one range in the home region."""
    return tx(lambda c: ops.plant(c, req.player_id, req.plot_id, req.crop_type_id))


@app.post("/harvest")
def harvest(req: HarvestReq):
    """General tier, plus a durable harvest_log record."""
    def fn(c):
        p = ops.get_player(c, req.player_id)
        crop = c.execute("SELECT crop_type_id FROM farm_plots WHERE plot_id = %s", (req.plot_id,)).fetchone()
        mean = CROP_YIELD.get(crop["crop_type_id"] if crop and crop["crop_type_id"] else 1, 1)
        qty = max(1, int(random.expovariate(1 / mean)) + 1)
        return ops.harvest(c, req.player_id, p["region"], req.plot_id, qty)
    return tx(fn)


@app.post("/fields/{field_id}/buy")
def buy(field_id: int, req: BuyReq):
    """Critical tier, contended: Golden Seed purchase."""
    iso = "READ COMMITTED" if req.mode == "optimistic" else "SERIALIZABLE"

    def fn(c):
        p = ops.get_player(c, req.player_id)
        return ops.buy_from_field(c, req.player_id, p["region"], field_id, req.mode)
    return tx(fn, isolation=iso)


@app.post("/trade")
def trade(req: TradeReq):
    """Critical tier: atomic multi-row (possibly cross-region) transaction,
    idempotent on request_id."""
    def fn(c):
        s = ops.get_player(c, req.seller_id)
        b = ops.get_player(c, req.buyer_id)
        return ops.trade(c, req.request_id, req.seller_id, s["region"], req.buyer_id, b["region"],
                         req.item_type_id, req.quantity, req.price)
    return tx(fn)


@app.get("/leaderboard")
def get_leaderboard(limit: int = 10, fresh: bool = False):
    """Delay-tolerant tier: follower read (a few seconds stale) unless fresh=true."""
    t0 = time.perf_counter()
    with pool.connection() as c:
        rows = ops.leaderboard(c, limit, follower=not fresh)
    return {"follower_read": not fresh, "ms": round((time.perf_counter() - t0) * 1000, 2), "rows": rows}


@app.post("/presence/{player_id}")
def presence(player_id: uuid.UUID):
    """Delay-tolerant tier: row expires automatically (row-level TTL)."""
    def fn(c):
        p = ops.get_player(c, player_id)
        ops.touch_presence(c, player_id, p["region"])
        return {"player_id": str(player_id), "status": "online"}
    return tx(fn)


@app.get("/presence/online")
def online(region: str | None = None):
    with pool.connection() as c:
        return {"region": region or "all", "online": ops.online_count(c, region)}
