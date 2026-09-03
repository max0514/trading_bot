"""benchmark_return (twlab 04) across the three seams.

Seam 3: parse(raw) → rows against recorded MFI94U month pages (ROC-year dates,
a 日　期 header spelled with a full-width space).
Seam 2: the pipeline with HTTP faked — a month page is re-fetched every day, so
re-runs and month-to-month appends must never duplicate rows.
Seam 1: the single Catalog key resolves to a one-column Wide Frame named like
FinLab's, carrying the Witness's real index values.
"""
import datetime as dt

import pandas as pd
import pytest

from twlab import catalog, data, pipeline, registry
from twlab.dataframe import FinlabDataFrame
from twlab.datasets import benchmark_return
from twlab.errors import ParseError
from twlab.store.parquet import ParquetStore

from conftest import FakeSession, load_fixture

DS = "benchmark_return"
FIELD = "發行量加權股價報酬指數"
KEY = f"{DS}:{FIELD}"
CATALOG_KEYS = [f.key for f in catalog.dataset_fields(DS)]

AUGUST_DAY = dt.date(2026, 8, 7)
AUGUST_NOW = dt.datetime(2026, 8, 7, 21, 32)
SEPTEMBER_DAY = dt.date(2026, 9, 3)
SEPTEMBER_NOW = dt.datetime(2026, 9, 3, 21, 32)
AUGUST_TRADING_DAYS = 21
SEPTEMBER_TRADING_DAYS = 3   # the page as of 2026-09-03

# Golden values from the real MFI94U recordings; the Witness (FinMind
# TaiwanStockTotalReturnIndex, TAIEX) agrees on every day of both pages.
AUG_03, AUG_07, AUG_31, SEP_01 = 100_027.02, 101_989.71, 106_498.23, 108_395.72


def raw(name):
    return {"source": "twse", "payload": load_fixture(name, DS)}


def session_for(name) -> FakeSession:
    return FakeSession({"MFI94U": load_fixture(name, DS)})


def run(session, mongo, store, day=AUGUST_DAY, now=AUGUST_NOW):
    return pipeline.run(DS, day, session=session, mongo=mongo, store=store, now=now)


# ── Seam 3: parser ─────────────────────────────────────────────────────────

def test_parse_shape_and_golden_values():
    rows = benchmark_return.parse(raw("twse_mfi94u_202608.json"))

    assert list(rows.columns) == ["stock_id", "date", "market", FIELD]
    assert len(rows) == AUGUST_TRADING_DAYS
    assert (rows["stock_id"] == benchmark_return.STOCK_ID).all()
    assert (rows["market"] == "TWSE").all()
    assert rows["date"].is_unique and rows["date"].is_monotonic_increasing
    assert rows["date"].iloc[0] == pd.Timestamp("2026-08-03")     # Aug 1 2026 is a Saturday
    assert rows["date"].iloc[-1] == pd.Timestamp("2026-08-31")

    by_date = rows.set_index("date")[FIELD]
    assert by_date[pd.Timestamp("2026-08-03")] == AUG_03
    assert by_date[pd.Timestamp("2026-08-07")] == AUG_07
    assert by_date[pd.Timestamp("2026-08-31")] == AUG_31


def test_date_header_whitespace_is_normalised():
    payload = load_fixture("twse_mfi94u_202608.json", DS)
    assert payload["fields"][0] == "日\u3000期"          # TWSE spells it with U+3000
    for header in ("日\u3000期", "日 期", "日期"):
        rows = benchmark_return.parse({"source": "twse", "payload": {**payload, "fields": [header, FIELD]}})
        assert len(rows) == AUGUST_TRADING_DAYS


def test_roc_dates_convert_to_gregorian():
    assert benchmark_return._parse_roc_date("115/08/03") == pd.Timestamp("2026-08-03")
    assert benchmark_return._parse_roc_date("92/01/02") == pd.Timestamp("2003-01-02")
    with pytest.raises(ParseError):
        benchmark_return._parse_roc_date("2026-08-03")
    with pytest.raises(ParseError):
        benchmark_return._parse_roc_date("115/13/01")


def test_renamed_column_fails_loudly():
    with pytest.raises(ParseError, match=FIELD):
        benchmark_return.parse(raw("twse_mfi94u_202608_malformed.json"))


def test_no_data_month_yields_empty_batch():
    rows = benchmark_return.parse({"source": "twse", "payload": {"stat": "很抱歉，沒有符合條件的資料!"}})
    assert rows.empty
    assert list(rows.columns) == ["stock_id", "date", "market", FIELD]


def test_unexpected_stat_rejected():
    with pytest.raises(ParseError, match="stat"):
        benchmark_return.parse({"source": "twse", "payload": {"stat": "ERROR"}})


def test_unknown_source_rejected():
    with pytest.raises(ParseError):
        benchmark_return.parse({"source": "tpex", "payload": {}})


def test_unparseable_number_rejected():
    payload = load_fixture("twse_mfi94u_202608.json", DS)
    payload["data"][0][1] = "1O0,027.02"
    with pytest.raises(ParseError):
        benchmark_return.parse({"source": "twse", "payload": payload})


# ── Seam 2: pipeline ───────────────────────────────────────────────────────

def test_fetch_asks_twse_for_the_month_page_of_the_day():
    session = session_for("twse_mfi94u_202608.json")
    raws = benchmark_return.fetch(session, AUGUST_DAY)
    assert [r["source"] for r in raws] == ["twse"]
    assert any("/TAIEX/MFI94U?date=20260807&response=json" in c for c in session.calls)


def test_full_run_materializes_single_column_frame(mongo, store_env):
    result = run(session_for("twse_mfi94u_202608.json"), mongo, ParquetStore(store_env))

    assert result.status == "ok"
    assert result.rows == AUGUST_TRADING_DAYS
    frame = data.get(KEY)
    assert list(frame.columns) == [FIELD]                  # FinLab's single-column frame
    assert len(frame) == AUGUST_TRADING_DAYS
    assert frame.loc["2026-08-07", FIELD] == AUG_07
    assert frame._freq == "daily"


@pytest.mark.parametrize("key", CATALOG_KEYS)
def test_every_catalog_key_resolves_to_a_wide_frame(mongo, store_env, key):
    run(session_for("twse_mfi94u_202608.json"), mongo, ParquetStore(store_env))
    frame = data.get(key)

    assert isinstance(frame, FinlabDataFrame)
    assert isinstance(frame.index, pd.DatetimeIndex)
    assert frame.index.name == "date"
    assert all(isinstance(c, str) for c in frame.columns)
    assert frame.loc["2026-08-31", FIELD] == AUG_31


def test_rerun_is_idempotent(mongo, store):
    first = run(session_for("twse_mfi94u_202608.json"), mongo, store)
    second = run(session_for("twse_mfi94u_202608.json"), mongo, store)
    assert first.status == second.status == "ok"
    assert mongo.collection(DS).count_documents({}) == AUGUST_TRADING_DAYS


def test_next_month_appends_without_duplicating(mongo, store_env):
    api_store = ParquetStore(store_env)
    run(session_for("twse_mfi94u_202608.json"), mongo, api_store)
    result = run(session_for("twse_mfi94u_202609.json"), mongo, api_store,
                 day=SEPTEMBER_DAY, now=SEPTEMBER_NOW)

    assert result.status == "ok"
    assert mongo.collection(DS).count_documents({}) == AUGUST_TRADING_DAYS + SEPTEMBER_TRADING_DAYS
    frame = data.get(KEY)
    assert len(frame) == AUGUST_TRADING_DAYS + SEPTEMBER_TRADING_DAYS
    assert frame.index.is_monotonic_increasing
    assert frame.loc["2026-08-31", FIELD] == AUG_31
    assert frame.loc["2026-09-01", FIELD] == SEP_01


def test_poisoned_month_is_quarantined_and_last_good_frames_survive(mongo, store_env):
    api_store = ParquetStore(store_env)
    good = run(session_for("twse_mfi94u_202608.json"), mongo, api_store)
    assert good.status == "ok"

    # A zero total-return index is impossible; the Invariant must hold the batch.
    bad = run(session_for("twse_mfi94u_202609_poison.json"), mongo, api_store,
              day=SEPTEMBER_DAY, now=SEPTEMBER_NOW)

    assert bad.status == "quarantined"
    assert any("positive" in f for f in bad.failures)
    frame = data.get(KEY)
    assert frame.index.max() == pd.Timestamp("2026-08-31")   # September never published
    assert frame.loc["2026-08-07", FIELD] == AUG_07


def test_parse_failure_quarantines_run(mongo, store):
    result = run(session_for("twse_mfi94u_202608_malformed.json"), mongo, store)

    assert result.status == "quarantined"
    assert any("parse" in f for f in result.failures)
    assert mongo.collection(DS).count_documents({}) == 0
    assert not store.manifest_path(DS).exists()


# ── Seam 1: Registry contract ──────────────────────────────────────────────

def test_registry_entry_matches_catalog_history():
    spec = registry.get_spec(DS)
    assert spec.fields == (FIELD,)
    assert spec.frequency == "daily" and spec.cadence.kind == "daily"
    assert spec.backfill_start == dt.date(2003, 1, 2)   # Catalog: 2003/01/02 至 …
