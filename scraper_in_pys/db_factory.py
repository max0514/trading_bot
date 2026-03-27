"""
Database factory — returns either Mongo or LocalDB based on configuration.

Priority:
  1. DB_BACKEND env var: "mongo" or "local"
  2. If MONGODB_USER is set and non-empty → Mongo
  3. Otherwise → LocalDB (SQLite)

Usage:
    from scraper_in_pys.db_factory import get_db
    repo = get_db(db='trading_bot', collection='stock_price')
    # repo has the same interface regardless of backend
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_backend = None


def _detect_backend():
    global _backend
    if _backend is not None:
        return _backend

    explicit = os.getenv('DB_BACKEND', '').strip().lower()
    if explicit == 'local':
        _backend = 'local'
    elif explicit == 'mongo':
        _backend = 'mongo'
    elif os.getenv('MONGODB_USER', '').strip():
        # Try connecting to Mongo; fall back to local on failure
        try:
            from scraper_in_pys.mongo import get_client
            client = get_client()
            if client is not None:
                client.admin.command('ping')
                _backend = 'mongo'
            else:
                _backend = 'local'
        except Exception as e:
            logger.warning(f"MongoDB unavailable ({e}), falling back to local SQLite")
            _backend = 'local'
    else:
        _backend = 'local'

    logger.info(f"Database backend: {_backend}")
    return _backend


def get_db(db='trading_bot', collection='stock_price'):
    """Return a DB instance (Mongo or LocalDB) with identical interface."""
    backend = _detect_backend()

    if backend == 'mongo':
        from scraper_in_pys.mongo import Mongo
        return Mongo(db=db, collection=collection)
    else:
        from scraper_in_pys.local_db import LocalDB
        return LocalDB(db=db, collection=collection)


def get_backend_name():
    """Return the current backend name ('mongo' or 'local')."""
    return _detect_backend()
