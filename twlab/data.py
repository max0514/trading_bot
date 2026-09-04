"""The twlab data API — FinLab's contract, self-hosted.

    from twlab import data
    close = data.get("price:收盤價")
    data.search("收盤")

`get()` accepts FinLab's exact Data Keys (per docs/finlab_catalog.json) and
returns a FinlabDataFrame read from the materialized Parquet store: a Wide
Frame for `dataset:field` keys, the whole table for bare static keys such as
`security_categories`. Monthly and quarterly frames arrive indexed by their
Statutory Deadline and tagged with their frequency, so they auto-align when
combined with daily frames. `get()` never touches MongoDB; on a research
machine it syncs frames from the server through a local cache and keeps
working offline on the last-synced data.
"""
from __future__ import annotations

from twlab import cache, catalog
from twlab.dataframe import FinlabDataFrame


def _store():
    """Local store on the server; a read-through cache on research machines
    (TWLAB_SERVER_URL / TWLAB_REMOTE_STORE) — see twlab.cache."""
    return cache.client_store()


def get(key: str) -> FinlabDataFrame:
    """Resolve a Data Key to its frame (index=date, columns=Stock ID)."""
    entry = catalog.resolve(key)
    store = _store()
    if entry.field == "":
        table = FinlabDataFrame(store.read_table(entry.dataset))
        table._freq = "static"
        return table
    frame = FinlabDataFrame(store.read_frame(entry.dataset, entry.field))
    frame._freq = store.read_manifest(entry.dataset).get("frequency", "daily")
    return frame


def search(keyword: str) -> list[dict[str, str]]:
    """Find Data Keys in the Catalog matching a keyword.

    Returns [{"key", "dataset", "field", "dtype", "description"}, ...].
    """
    return [
        {
            "key": f.key,
            "dataset": f.dataset,
            "field": f.field,
            "dtype": f.dtype,
            "description": f.description,
        }
        for f in catalog.search(keyword)
    ]
