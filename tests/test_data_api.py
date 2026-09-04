"""Seam 1 — the twlab data API, against a store seeded through the pipeline.

This is the contract FinLab strategies rely on: every Catalog key resolves,
frames are date × Stock ID Wide Frames of type FinlabDataFrame, and
search() discovers keys.
"""
import pandas as pd
import pytest

from twlab import catalog, data
from twlab.dataframe import FinlabDataFrame
from twlab.errors import DatasetNotMaterializedError, UnknownDataKeyError

from conftest import DAY, NOW


PRICE_KEYS = [
    "price:成交股數", "price:成交筆數", "price:成交金額",
    "price:收盤價", "price:開盤價", "price:最低價", "price:最高價",
    "price:最後揭示買價", "price:最後揭示賣價", "price:最後揭示買量", "price:最後揭示賣量",
]


@pytest.fixture
def seeded(good_session, mongo, store_env):
    """Run the real pipeline once so the API reads pipeline-produced frames."""
    from twlab import pipeline
    from twlab.store.parquet import ParquetStore

    result = pipeline.run(
        "price", DAY,
        session=good_session, mongo=mongo,
        store=ParquetStore(store_env), now=NOW,
    )
    assert result.status == "ok"
    return result


def test_price_keys_in_catalog_match_registry():
    from twlab import registry
    catalog_keys = {f.key for f in catalog.dataset_fields("price")}
    registry_keys = {f"price:{f}" for f in registry.get_spec("price").fields}
    assert catalog_keys == registry_keys  # the Catalog is the coverage spec


@pytest.mark.parametrize("key", PRICE_KEYS)
def test_every_price_field_resolves_to_a_wide_frame(seeded, key):
    frame = data.get(key)

    assert isinstance(frame, FinlabDataFrame)
    assert isinstance(frame.index, pd.DatetimeIndex)
    assert frame.index.name == "date"
    assert "2330" in frame.columns   # TWSE security
    assert "5483" in frame.columns   # TPEx security
    assert all(isinstance(c, str) for c in frame.columns)  # Stock IDs stay strings


def test_golden_values_round_trip(seeded):
    assert data.get("price:收盤價").loc["2026-08-07", "2330"] == 2370.0
    assert data.get("price:成交股數").loc["2026-08-07", "2330"] == 24414025
    assert data.get("price:最後揭示賣量").loc["2026-08-07", "2330"] == 563
    assert data.get("price:收盤價").loc["2026-08-07", "5483"] == 168.5


def test_frames_are_independent_copies(seeded):
    a = data.get("price:收盤價")
    a.iloc[0, 0] = -1.0
    assert data.get("price:收盤價").iloc[0, 0] != -1.0


def test_search_finds_the_close_price_key():
    hits = data.search("收盤")
    keys = [h["key"] for h in hits]
    assert "price:收盤價" in keys
    hit = next(h for h in hits if h["key"] == "price:收盤價")
    assert hit["dataset"] == "price"
    assert hit["field"] == "收盤價"
    assert hit["dtype"] == "float"


def test_unknown_key_rejected_with_suggestions():
    with pytest.raises(UnknownDataKeyError, match="price:收盤價"):
        data.get("price:收盤")   # near-miss should suggest the real key


def test_unmaterialized_dataset_reports_clearly(store_env):
    with pytest.raises(DatasetNotMaterializedError, match="pipeline"):
        data.get("price:收盤價")
