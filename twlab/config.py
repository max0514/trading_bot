"""Runtime configuration, resolved from environment variables at call time.

Follows the repo convention of configuring everything by env var (MONGODB_URI);
values are read lazily so tests can monkeypatch the environment.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DEFAULT_MONGO_URI = "mongodb://127.0.0.1:27017"
DEFAULT_MONGO_DB = "twlab"


def store_dir() -> Path:
    """Directory holding the materialized Parquet Wide Frames.

    TWLAB_STORE_DIR overrides; default is ~/.twlab/store so notebooks anywhere
    on the machine resolve the same store.
    """
    env = os.getenv("TWLAB_STORE_DIR")
    if env:
        return Path(env)
    return Path.home() / ".twlab" / "store"


def mongo_uri() -> str:
    return os.getenv("MONGODB_URI") or DEFAULT_MONGO_URI


def mongo_db_name() -> str:
    return os.getenv("TWLAB_MONGO_DB") or DEFAULT_MONGO_DB


def server_url() -> str | None:
    """Base URL of the server's published store (research machines); None = local."""
    return os.getenv("TWLAB_SERVER_URL") or None


def remote_store_dir() -> Path | None:
    """The server store mounted as a directory (NAS/LAN); None = not configured."""
    env = os.getenv("TWLAB_REMOTE_STORE")
    return Path(env) if env else None


def cache_ttl() -> float:
    """Seconds between freshness checks against the server (default 1 hour)."""
    return float(os.getenv("TWLAB_CACHE_TTL") or 3600)


def catalog_path() -> Path:
    """Path to the FinLab catalog JSON (the coverage spec)."""
    env = os.getenv("TWLAB_CATALOG")
    if env:
        return Path(env)
    return Path(__file__).resolve().parent.parent / "docs" / "finlab_catalog.json"
