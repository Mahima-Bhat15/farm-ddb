"""Configuration and connection settings."""
import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
REGIONS = ["us-west", "us-east", "eu-west"]


def load_config(path: str | None = None) -> dict:
    path = path or os.environ.get("FARM_CONFIG", ROOT / "config" / "default.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


def region_dsn(region: str) -> str:
    """DSN of the regional HAProxy endpoint (gateway nodes in that region)."""
    key = "FARM_DSN_" + region.upper().replace("-", "_")
    dsn = os.environ.get(key) or os.environ.get("FARM_DSN")
    if not dsn:
        raise RuntimeError(f"Set {key} or FARM_DSN")
    return dsn


def admin_dsn() -> str:
    return os.environ.get("FARM_DSN_ADMIN") or region_dsn("us-west")


def dialect() -> str:
    """'crdb' normally; 'postgres' only for local logic tests without a cluster."""
    return os.environ.get("FARM_DIALECT", "crdb")
