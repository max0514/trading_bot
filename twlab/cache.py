"""Client-side Parquet cache: FinLab-style access from a research machine.

The server (Docker Compose stack) materializes Wide Frames into its store;
research machines point `data.get()` at that store through one adapter —
HTTP (`TWLAB_SERVER_URL`, e.g. the stack's static file server) or a LAN
mount (`TWLAB_REMOTE_STORE`) — and keep a local Parquet cache with freshness
metadata. Reads check the server's manifest at most once per TTL, fetch only
frames whose version changed, and, when the server is unreachable, serve the
last-synced frame with a StalenessWarning. Without a remote configured,
`data.get()` reads the local store directly (the server's own notebooks).
"""
from __future__ import annotations

import io
import json
import time
import warnings
from pathlib import Path
from typing import Any, Callable, Protocol
from urllib.parse import quote

import pandas as pd
import requests

from twlab import config
from twlab.errors import DatasetNotMaterializedError, RemoteUnavailable, StalenessWarning
from twlab.store.parquet import MANIFEST_FILE, TABLE_FILE, ParquetStore, _atomic_write

CACHE_FILE = "cache.json"


class RemoteStore(Protocol):
    def manifest(self, dataset: str) -> dict: ...
    def frame_bytes(self, dataset: str, field: str) -> bytes: ...
    def table_bytes(self, dataset: str) -> bytes: ...


def _http_get_bytes(url: str, timeout: float = 20.0) -> bytes:
    try:
        resp = requests.get(url, timeout=timeout)
    except requests.RequestException as exc:
        raise RemoteUnavailable(f"{url}: {exc}") from exc
    if resp.status_code == 404:
        raise DatasetNotMaterializedError(f"server has not materialized {url}")
    if resp.status_code >= 500:
        raise RemoteUnavailable(f"{url}: HTTP {resp.status_code}")
    resp.raise_for_status()
    return resp.content


class HttpRemote:
    """The server store published over HTTP (any static file server: nginx,
    `python -m twlab.serve`). Fetching is injectable for tests."""

    def __init__(self, base_url: str, get_bytes: Callable[[str], bytes] | None = None):
        self._base = base_url.rstrip("/")
        self._get = get_bytes

    def _fetch(self, dataset: str, name: str) -> bytes:
        get = self._get or _http_get_bytes
        return get(f"{self._base}/{quote(dataset)}/{quote(name)}")

    def manifest(self, dataset: str) -> dict:
        return json.loads(self._fetch(dataset, MANIFEST_FILE).decode("utf-8"))

    def frame_bytes(self, dataset: str, field: str) -> bytes:
        return self._fetch(dataset, f"{field}.parquet")

    def table_bytes(self, dataset: str) -> bytes:
        return self._fetch(dataset, TABLE_FILE)


class DirectoryRemote:
    """The server store reachable as a directory (NAS share, LAN mount)."""

    def __init__(self, root: Path | str):
        self._root = Path(root)

    def _read(self, dataset: str, name: str) -> bytes:
        path = self._root / dataset / name
        if not self._root.exists():
            raise RemoteUnavailable(f"{self._root} is not mounted")
        if not path.exists():
            raise DatasetNotMaterializedError(f"server has not materialized {path}")
        try:
            return path.read_bytes()
        except OSError as exc:
            raise RemoteUnavailable(f"{path}: {exc}") from exc

    def manifest(self, dataset: str) -> dict:
        return json.loads(self._read(dataset, MANIFEST_FILE).decode("utf-8"))

    def frame_bytes(self, dataset: str, field: str) -> bytes:
        return self._read(dataset, f"{field}.parquet")

    def table_bytes(self, dataset: str) -> bytes:
        return self._read(dataset, TABLE_FILE)


class CachedStore:
    """A read-through cache over a remote store with the ParquetStore read API."""

    def __init__(self, local: ParquetStore, remote: RemoteStore, *,
                 ttl: float = 3600.0, clock: Callable[[], float] = time.time):
        self._local = local
        self._remote = remote
        self._ttl = ttl
        self._clock = clock

    # ── freshness metadata ─────────────────────────────────────────────

    def _meta_path(self, dataset: str) -> Path:
        return self._local.root / dataset / CACHE_FILE

    def _load_meta(self, dataset: str) -> dict[str, Any]:
        path = self._meta_path(dataset)
        if not path.exists():
            return {"checked_at": None, "manifest": None, "versions": {}}
        return json.loads(path.read_text(encoding="utf-8"))

    def _save_meta(self, dataset: str, meta: dict[str, Any]) -> None:
        path = self._meta_path(dataset)
        path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(path, lambda tmp: tmp.write_text(
            json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8"))

    def _current_manifest(self, dataset: str, meta: dict[str, Any]) -> dict | None:
        """The server's manifest, re-checked at most once per TTL. None = unreachable."""
        checked = meta.get("checked_at")
        if meta.get("manifest") is not None and checked is not None \
                and self._clock() - checked < self._ttl:
            return meta["manifest"]
        try:
            manifest = self._remote.manifest(dataset)
        except RemoteUnavailable:
            return None
        meta["manifest"] = manifest
        meta["checked_at"] = self._clock()
        self._save_meta(dataset, meta)
        _atomic_write(self._local.manifest_path(dataset), lambda tmp: tmp.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8"))
        return manifest

    def _stale_warning(self, dataset: str, meta: dict[str, Any]) -> None:
        synced = meta.get("manifest", {}).get("materialized_at") if meta.get("manifest") else None
        warnings.warn(
            f"twlab server unreachable; serving the last-synced {dataset} frames "
            f"(server version {synced or 'unknown'})",
            StalenessWarning, stacklevel=4,
        )

    # ── reads ──────────────────────────────────────────────────────────

    def _sync(self, dataset: str, name: str, fetch: Callable[[], bytes],
              local_path: Path) -> None:
        meta = self._load_meta(dataset)
        manifest = self._current_manifest(dataset, meta)
        if manifest is None:                                   # offline
            if local_path.exists():
                self._stale_warning(dataset, meta)
                return
            raise DatasetNotMaterializedError(
                f"{dataset}/{name}: server unreachable and never synced to this machine")
        version = manifest.get("materialized_at")
        if local_path.exists() and meta["versions"].get(name) == version:
            return
        try:
            payload = fetch()
        except RemoteUnavailable:
            if local_path.exists():
                self._stale_warning(dataset, meta)
                return
            raise DatasetNotMaterializedError(
                f"{dataset}/{name}: server unreachable and never synced to this machine")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(local_path, lambda tmp: tmp.write_bytes(payload))
        meta["versions"][name] = version
        self._save_meta(dataset, meta)

    def read_frame(self, dataset: str, field: str) -> pd.DataFrame:
        name = f"{field}.parquet"
        self._sync(dataset, name, lambda: self._remote.frame_bytes(dataset, field),
                   self._local.root / dataset / name)
        return self._local.read_frame(dataset, field)

    def read_table(self, dataset: str) -> pd.DataFrame:
        self._sync(dataset, TABLE_FILE, lambda: self._remote.table_bytes(dataset),
                   self._local.root / dataset / TABLE_FILE)
        return self._local.read_table(dataset)

    def read_manifest(self, dataset: str) -> dict:
        meta = self._load_meta(dataset)
        manifest = self._current_manifest(dataset, meta)
        if manifest is not None:
            return manifest
        if meta.get("manifest") is not None:
            self._stale_warning(dataset, meta)
            return meta["manifest"]
        raise DatasetNotMaterializedError(
            f"{dataset}: server unreachable and never synced to this machine")


def client_store() -> ParquetStore | CachedStore:
    """The store `data.get()` reads: cached over a configured remote, else local."""
    local = ParquetStore(config.store_dir())
    server_url = config.server_url()
    if server_url:
        return CachedStore(local, HttpRemote(server_url), ttl=config.cache_ttl())
    remote_dir = config.remote_store_dir()
    if remote_dir:
        return CachedStore(local, DirectoryRemote(remote_dir), ttl=config.cache_ttl())
    return local
