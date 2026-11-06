"""Connections and the transaction retry loop.

CockroachDB runs transactions at SERIALIZABLE isolation by default. When two
transactions conflict, one of them is aborted with SQLSTATE 40001
("restart transaction") and must be retried by the client. run_txn() does
that retry with exponential backoff and reports how many retries it needed,
which is the "retry rate" metric in our evaluation.
"""
import random
import time

import psycopg
from psycopg.rows import dict_row


class OptimisticConflict(Exception):
    """Raised by application-level optimistic concurrency control when a
    version check fails (another transaction changed the row first)."""


class RetryLimit(Exception):
    pass


def connect(dsn: str, *, statement_timeout_ms: int = 10000, app: str = "farm") -> psycopg.Connection:
    conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row,
                           connect_timeout=3, application_name=app)
    conn.execute(f"SET statement_timeout = '{int(statement_timeout_ms)}ms'")
    return conn


def run_txn(conn, fn, *, isolation: str | None = None, max_retries: int = 50,
            base_backoff_s: float = 0.002, max_backoff_s: float = 0.2):
    """Run fn(conn) inside one transaction, retrying on serialization
    failures (40001) and optimistic-concurrency conflicts.

    Returns (result, retries). Game errors raised by fn (sold out, not enough
    coins, ...) abort the transaction and propagate without retry.
    """
    retries = 0
    while True:
        try:
            with conn.transaction():
                if isolation:
                    conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation}")
                result = fn(conn)
            return result, retries
        except (psycopg.errors.SerializationFailure, OptimisticConflict):
            retries += 1
            if retries > max_retries:
                raise RetryLimit(f"gave up after {max_retries} retries")
            sleep = min(max_backoff_s, base_backoff_s * (2 ** min(retries, 8)))
            time.sleep(random.uniform(0, sleep))


def insert_rows(conn, table: str, cols: list[str], rows: list[tuple], batch: int = 500) -> int:
    """Multi-row INSERT in batches (works on CockroachDB and PostgreSQL)."""
    if not rows:
        return 0
    one = "(" + ",".join(["%s"] * len(cols)) + ")"
    n = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES " + ",".join([one] * len(chunk))
        params = [v for r in chunk for v in r]
        conn.execute(sql, params)
        n += len(chunk)
    return n
