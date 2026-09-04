"""Shared test plumbing: recorded fixtures, fake HTTP session, in-memory Mongo.

No test in this suite touches the live network — Seam 2 fakes HTTP with
recorded responses, Seam 1 reads a store seeded through the pipeline.
"""
from __future__ import annotations

import datetime as dt
import gzip
import json
from pathlib import Path
from urllib.parse import urlencode

import mongomock
import pytest

from twlab.store.mongo import MongoStore
from twlab.store.parquet import ParquetStore

FIXTURES = Path(__file__).parent / "fixtures"

DAY = dt.date(2026, 8, 7)
NOW = dt.datetime(2026, 8, 7, 21, 32, 0)


def load_fixture(name: str, dataset: str = "price") -> dict:
    """A recorded JSON response under tests/fixtures/<dataset>/."""
    with open(FIXTURES / dataset / name, encoding="utf-8") as f:
        return json.load(f)


def load_text_fixture(name: str, dataset: str) -> str:
    """A recorded HTML/text response under tests/fixtures/<dataset>/.

    Large recordings are stored gzipped (<name>.gz) as the decoded UTF-8 text
    the parser receives from PoliteSession.get_text().
    """
    path = FIXTURES / dataset / name
    if path.exists():
        return path.read_text(encoding="utf-8")
    with gzip.open(path.with_name(path.name + ".gz"), "rt", encoding="utf-8") as f:
        return f.read()


class FakeSession:
    """Stands in for PoliteSession at the HTTP boundary.

    Maps a substring of the request (URL plus encoded params) to a recorded
    payload — a dict/list for get_json, a str for get_text — and records calls.
    """

    def __init__(self, payload_by_fragment: dict[str, object]):
        self._payloads = payload_by_fragment
        self.calls: list[str] = []

    def _lookup(self, url: str, params: dict | None):
        request = url + ("?" + urlencode(params) if params else "")
        self.calls.append(request)
        for fragment, payload in self._payloads.items():
            if fragment in request:
                return payload
        raise AssertionError(f"unexpected request in test: {request}")

    def get_json(self, url: str, params=None):
        payload = self._lookup(url, params)
        assert not isinstance(payload, str), f"get_json got a text fixture for {url}"
        return payload

    def get_text(self, url: str, params=None, encoding=None) -> str:
        payload = self._lookup(url, params)
        assert isinstance(payload, str), f"get_text got a JSON fixture for {url}"
        return payload


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
    monkeypatch.delenv("TWLAB_REMOTE_STORE", raising=False)
    monkeypatch.delenv("TWLAB_SERVER_URL", raising=False)
    return root
