"""Client-side Parquet cache (twlab 08): FinLab-style research-machine access.

Cold read fetches and caches; warm read touches no network; a server update
refreshes only stale frames; an unreachable server serves the last-synced
frame with a staleness warning.
"""
import datetime as dt
import shutil

import pandas as pd
import pytest

from twlab import cache, config, data, pipeline
from twlab.errors import DatasetNotMaterializedError, RemoteUnavailable, StalenessWarning
from twlab.store.parquet import ParquetStore

from conftest import DAY, NOW


class FakeRemote:
    """A server store behind a flappable network: counts every request."""

    def __init__(self, server: ParquetStore):
        self.server = server
        self.online = True
        self.calls: list[str] = []

    def _guard(self, what):
        self.calls.append(what)
        if not self.online:
            raise RemoteUnavailable("connection refused")

    def manifest(self, dataset):
        self._guard(f"manifest:{dataset}")
        return self.server.read_manifest(dataset)

    def frame_bytes(self, dataset, field):
        self._guard(f"frame:{dataset}:{field}")
        return self.server.frame_path(dataset, field).read_bytes()

    def table_bytes(self, dataset):
        self._guard(f"table:{dataset}")
        return self.server.table_path(dataset).read_bytes()


class Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


@pytest.fixture
def server(tmp_path, good_session, mongo):
    store = ParquetStore(tmp_path / "server")
    assert pipeline.run("price", DAY, session=good_session, mongo=mongo, store=store, now=NOW).status == "ok"
    return store


@pytest.fixture
def client(tmp_path, server):
    remote = FakeRemote(server)
    clock = Clock()
    cached = cache.CachedStore(ParquetStore(tmp_path / "client"), remote, ttl=3600, clock=clock)
    return cached, remote, clock


def test_cold_read_fetches_and_caches(client, tmp_path):
    cached, remote, _ = client
    close = cached.read_frame("price", "收盤價")
    assert close.loc["2026-08-07", "2330"] == 2370.0
    assert remote.calls == ["manifest:price", "frame:price:收盤價"]
    assert (tmp_path / "client" / "price" / "收盤價.parquet").exists()


def test_warm_read_touches_no_network(client):
    cached, remote, _ = client
    cached.read_frame("price", "收盤價")
    remote.calls.clear()
    assert cached.read_frame("price", "收盤價").loc["2026-08-07", "2330"] == 2370.0
    assert remote.calls == []


def test_after_ttl_only_freshness_is_checked_when_nothing_changed(client):
    cached, remote, clock = client
    cached.read_frame("price", "收盤價")
    clock.t += 3601
    remote.calls.clear()
    cached.read_frame("price", "收盤價")
    assert remote.calls == ["manifest:price"]        # no frame re-download


def test_server_update_refreshes_the_frame(client, server, good_session, mongo):
    cached, remote, clock = client
    cached.read_frame("price", "收盤價")

    # Next night the server publishes a new version with a new trading day.
    from conftest import FakeSession, load_fixture
    twse = load_fixture("twse_mi_index_20260807.json")
    twse_next = {**twse, "date": "20260810"}
    for t in twse_next["tables"]:
        t["date"] = "20260810"
    later = FakeSession({"twse.com.tw": twse_next,
                         "tpex.org.tw": load_fixture("tpex_daily_quotes_20260807.json")})
    assert pipeline.run("price", DAY + dt.timedelta(days=3), session=later, mongo=mongo,
                        store=server, now=NOW + dt.timedelta(days=3)).status == "ok"

    clock.t += 3601
    remote.calls.clear()
    close = cached.read_frame("price", "收盤價")
    assert remote.calls == ["manifest:price", "frame:price:收盤價"]
    assert pd.Timestamp("2026-08-10") in close.index


def test_unreachable_server_serves_cache_with_a_warning(client):
    cached, remote, clock = client
    cached.read_frame("price", "收盤價")
    remote.online = False
    clock.t += 3601
    with pytest.warns(StalenessWarning, match="price"):
        close = cached.read_frame("price", "收盤價")
    assert close.loc["2026-08-07", "2330"] == 2370.0


def test_unreachable_server_and_no_cache_is_an_error(client):
    cached, remote, _ = client
    remote.online = False
    with pytest.raises(DatasetNotMaterializedError, match="unreachable"):
        cached.read_frame("price", "收盤價")


def test_manifest_and_frequency_are_cached_too(client):
    cached, remote, clock = client
    assert cached.read_manifest("price")["frequency"] == "daily"
    remote.online = False
    clock.t += 3601
    with pytest.warns(StalenessWarning):
        assert cached.read_manifest("price")["frequency"] == "daily"


def test_data_get_resolves_through_the_cache_when_a_server_is_configured(
    tmp_path, server, monkeypatch
):
    from urllib.parse import quote
    served: dict[str, bytes] = {}
    for path in server.root.rglob("*"):
        if path.is_file():
            rel = "/".join(quote(part) for part in path.relative_to(server.root).parts)
            served["http://nas.local:8787/" + rel] = path.read_bytes()
    calls = []

    def fake_get(url):
        calls.append(url)
        if url not in served:
            raise DatasetNotMaterializedError(url)
        return served[url]

    monkeypatch.setattr(cache, "_http_get_bytes", fake_get)
    monkeypatch.setenv("TWLAB_STORE_DIR", str(tmp_path / "laptop"))
    monkeypatch.setenv("TWLAB_SERVER_URL", "http://nas.local:8787")

    close = data.get("price:收盤價")
    assert close.loc["2026-08-07", "2330"] == 2370.0
    assert close._freq == "daily"
    assert calls == ["http://nas.local:8787/price/manifest.json",
                     "http://nas.local:8787/price/%E6%94%B6%E7%9B%A4%E5%83%B9.parquet"]
    calls.clear()
    data.get("price:收盤價")
    assert calls == []                                   # warm read: offline-capable


def test_directory_remote_serves_a_lan_mount(tmp_path, server):
    remote = cache.DirectoryRemote(server.root)
    cached = cache.CachedStore(ParquetStore(tmp_path / "client"), remote, ttl=0, clock=Clock())
    assert cached.read_frame("price", "開盤價").loc["2026-08-07", "2330"] == 2390.0
    shutil.move(server.root, tmp_path / "unmounted")   # the NAS goes away
    with pytest.warns(StalenessWarning):
        assert cached.read_frame("price", "開盤價").loc["2026-08-07", "2330"] == 2390.0
