"""The twlab data API — FinLab's contract, self-hosted.

    from twlab import data
    close = data.get("price:收盤價")
    data.search("收盤")

`get()` accepts FinLab's exact Data Keys (per docs/finlab_catalog.json) and
returns a FinlabDataFrame Wide Frame read from the materialized Parquet store.
It never touches MongoDB or the network.
"""
from __future__ import annotations

from twlab import catalog, config
from twlab.dataframe import FinlabDataFrame
from twlab.store.parquet import ParquetStore


def get(key: str) -> FinlabDataFrame:
    """Resolve a Data Key to its Wide Frame (index=date, columns=Stock ID)."""
    entry = catalog.resolve(key)
    store = ParquetStore(config.store_dir())
    frame = store.read_frame(entry.dataset, entry.field)
    return FinlabDataFrame(frame)


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
