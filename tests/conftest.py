"""Shared test plumbing: recorded fixtures, fake HTTP session, in-memory Mongo.

No test in this suite touches the live network — Seam 2 fakes HTTP with
recorded responses, Seam 1 reads a store seeded through the pipeline.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import mongomock
import pytest

from twlab.store.mongo import MongoStore
from twlab.store.parquet import ParquetStore

FIXTURES = Path(__file__).parent / "fixtures"

DAY = dt.date(2026, 8, 7)
NOW = dt.datetime(2026, 8, 7, 21, 32, 0)


def load_fixture(name: str) -> dict:
    with open(FIXTURES / "price" / name, encoding="utf-8") as f:
        return json.load(f)


class FakeSession:
    """Stands in for PoliteSession at the HTTP boundary.

    Maps a substring of the URL to a recorded payload and counts calls.
    """

    def __init__(self, payload_by_host: dict[str, dict]):
        self._payload_by_host = payload_by_host
        self.calls: list[str] = []

    def get_json(self, url: str, params=None):
        self.calls.append(url)
        for fragment, payload in self._payload_by_host.items():
            if fragment in url:
                return payload
        raise AssertionError(f"unexpected URL fetched in test: {url}")


@pytest.fixture
def twse_payload() -> dict:
    return load_fixture("twse_mi_index_20260807.json")


@pytest.fixture
def tpex_payload() -> dict:
    return load_fixture("tpex_daily_quotes_20260807.json")


@pytest.fixture
def good_session(twse_payload, tpex_payload) -> FakeSession:
    return FakeSession({"twse.com.tw": twse_payload, "tpex.org.tw": tpex_payload})


@pytest.fixture
def mongo() -> MongoStore:
    return MongoStore(client=mongomock.MongoClient(), db_name="twlab_test")


@pytest.fixture
def store(tmp_path) -> ParquetStore:
    return ParquetStore(tmp_path / "store")


@pytest.fixture
def store_env(tmp_path, monkeypatch) -> Path:
    """Point the data API at this test's Parquet store."""
    root = tmp_path / "store"
    monkeypatch.setenv("TWLAB_STORE_DIR", str(root))
    return root
