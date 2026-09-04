"""etl:adj_* (twlab 05) — back-adjusted OHLC derived from price × Corporate Actions.

Unit: back_adjust / combined_ratio on tiny frames.
Pipeline: derive() through pipeline.run, against a store seeded by the REAL
price pipeline for dates around a real TSMC ex-dividend (2330, 2026-06-11)
and a real 上市 capital reduction (2380 虹光, 2026-06-29), with the events
seeded through the Corporate Action pipelines from real TWSE recordings.
Golden values are the FinMind Witness's closes and reference prices
(tests/fixtures/corporate_actions/README.md).
Data API: the four etl Catalog keys resolve to daily Wide Frames.
"""
import copy
import datetime as dt
import shutil

import mongomock
import pandas as pd
import pytest

from twlab import data, pipeline, registry
from twlab.dataframe import FinlabDataFrame
from twlab.datasets import etl
from twlab.errors import DeriveError
from twlab.store.mongo import MongoStore
from twlab.store.parquet import ParquetStore

from conftest import FakeSession, load_fixture

CA = "corporate_actions"
NOW = dt.datetime(2026, 6, 30, 22, 0)

# Witness OHLC (FinMind TaiwanStockPrice) as (open, high, low, close).
TSMC_OHLC = {
    dt.date(2026, 6, 9): (2305.0, 2320.0, 2295.0, 2305.0),
    dt.date(2026, 6, 10): (2285.0, 2300.0, 2255.0, 2255.0),   # last cum-dividend close
    dt.date(2026, 6, 11): (2240.0, 2260.0, 2210.0, 2250.0),   # ex-dividend day
    dt.date(2026, 6, 12): (2325.0, 2325.0, 2290.0, 2310.0),
}
HL_OHLC = {
    dt.date(2026, 6, 9): (5.78, 5.8, 5.65, 5.66),
    dt.date(2026, 6, 10): (5.7, 5.7, 5.6, 5.6),
    dt.date(2026, 6, 11): (5.61, 5.85, 5.61, 5.85),
    dt.date(2026, 6, 12): (5.79, 6.43, 5.79, 6.43),
    dt.date(2026, 6, 16): (6.3, 6.84, 6.26, 6.6),             # last day before the reduction halt
    dt.date(2026, 6, 29): (23.85, 23.85, 21.5, 21.5),         # trading resumes at reference 23.86
    dt.date(2026, 6, 30): (20.3, 20.3, 19.35, 19.35),
}
TSMC_REFERENCE, TSMC_BEFORE = 2248.99, 2255.0                 # Witness after_price / before_price
TSMC_RATIO = TSMC_REFERENCE / TSMC_BEFORE
HL_REFERENCE, HL_LAST_CLOSE = 23.86, 6.6                       # Witness PostReductionReferencePrice
HL_RATIO = HL_REFERENCE / HL_LAST_CLOSE

PRICE_DAYS = {d: {"2330": TSMC_OHLC[d], "2380": HL_OHLC[d]} if d in TSMC_OHLC else {"2380": HL_OHLC[d]}
              for d in sorted(HL_OHLC)}
OHLC_COLUMNS = ("開盤價", "最高價", "最低價", "收盤價")
CA_DAY = dt.date(2026, 9, 2)                                   # the Corporate Action recordings' end


# ── seeding through the real pipelines ─────────────────────────────────────

def price_session(day: dt.date, ohlc_by_id: dict[str, tuple]) -> FakeSession:
    """The recorded 2026-08-07 TWSE/TPEx quotes re-dated to `day`, trimmed to
    the row-count floor, with the golden Stock IDs' OHLC set to Witness values.
    Every other security keeps the recording's quotes."""
    twse = copy.deepcopy(load_fixture("twse_mi_index_20260807.json"))
    twse["date"] = day.strftime("%Y%m%d")
    quotes = next(t for t in twse["tables"] if "證券代號" in (t.get("fields") or []))
    fields = quotes["fields"]
    golden = {row[0]: row for row in quotes["data"] if row[0] in ("2330", "2380")}
    for stock_id, row in golden.items():
        if stock_id in ohlc_by_id:
            for column, value in zip(OHLC_COLUMNS, ohlc_by_id[stock_id]):
                row[fields.index(column)] = f"{value:,.2f}"
    quotes["data"] = [r for r in quotes["data"][:500] if r[0] not in golden] + list(golden.values())

    tpex = copy.deepcopy(load_fixture("tpex_daily_quotes_20260807.json"))
    tpex["date"] = tpex["tables"][0]["date"] = day.strftime("%Y/%m/%d")
    return FakeSession({"twse.com.tw": twse, "tpex.org.tw": tpex})


@pytest.fixture(scope="module")
def price_store_dir(tmp_path_factory):
    """price materialized for the seven Witness days through the real price
    pipeline — once per module; each test gets its own copy."""
    root = tmp_path_factory.mktemp("price_store")
    store = ParquetStore(root)
    mongo = MongoStore(client=mongomock.MongoClient(), db_name="twlab_price_seed")
    for day, ohlc in sorted(PRICE_DAYS.items()):
        result = pipeline.run("price", day, session=price_session(day, ohlc), mongo=mongo,
                              store=store, now=dt.datetime.combine(day, dt.time(21, 32)))
        assert result.status == "ok"
    return root


@pytest.fixture
def seeded(price_store_dir, mongo, store_env):
    """A fresh store holding the seeded price frames, wired to the data API."""
    shutil.copytree(price_store_dir, store_env)
    return mongo, ParquetStore(store_env)


def run_dividend_tse(mongo, store):
    session = FakeSession({"TWT49U": load_fixture("twse_twt49u_20260601_20260902.json", CA)})
    return pipeline.run("dividend_tse", CA_DAY, session=session, mongo=mongo, store=store, now=NOW)


def run_capital_reduction_tse(mongo, store):
    session = FakeSession({"TWTAUU": load_fixture("twse_twtauu_20260101_20260902.json", CA)})
    return pipeline.run("capital_reduction_tse", CA_DAY, session=session, mongo=mongo, store=store, now=NOW)


def run_etl(mongo, store, now=NOW):
    return pipeline.run("etl", now.date(), mongo=mongo, store=store, now=now)


def frame(values, dates, columns=("A",)) -> pd.DataFrame:
    return pd.DataFrame(values, index=pd.DatetimeIndex(dates, name="date"), columns=list(columns), dtype=float)


# ── unit: the back-adjustment ──────────────────────────────────────────────

def test_back_adjust_scales_only_prices_before_the_event():
    raw = frame([[100.0], [110.0], [105.0], [120.0]], ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"])
    ratio = frame([[0.5]], ["2026-01-07"])

    adj = etl.back_adjust(raw, ratio)

    assert adj["A"].tolist() == [50.0, 55.0, 105.0, 120.0]   # ex-date itself is already post-event
    assert adj.index.equals(raw.index) and list(adj.columns) == ["A"]


def test_back_adjust_compounds_events_in_date_order():
    dates = ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"]
    raw = frame([[100.0]] * 5, dates)
    ratio = frame([[0.5], [2.0]], ["2026-01-06", "2026-01-08"])

    adj = etl.back_adjust(raw, ratio)

    assert adj["A"].tolist() == [100.0, 200.0, 200.0, 100.0, 100.0]


def test_back_adjust_event_after_the_last_price_scales_everything():
    raw = frame([[100.0], [110.0]], ["2026-01-05", "2026-01-06"])
    ratio = frame([[0.9]], ["2026-01-12"])          # announced ahead, not yet a price date
    adj = etl.back_adjust(raw, ratio)
    assert adj["A"].tolist() == pytest.approx([90.0, 99.0])


def test_back_adjust_leaves_eventless_stocks_missing_ratios_and_nan_prices_alone():
    raw = frame([[100.0, 50.0], [float("nan"), 60.0]], ["2026-01-05", "2026-01-06"], columns=("A", "B"))
    ratio = pd.DataFrame({"B": [float("nan")], "Z": [0.5]}, index=pd.DatetimeIndex(["2026-01-06"]))

    adj = etl.back_adjust(raw, ratio)

    assert adj["B"].tolist() == [50.0, 60.0]          # NaN ratio = no usable event
    assert adj.loc["2026-01-05", "A"] == 100.0        # no events at all
    assert pd.isna(adj.loc["2026-01-06", "A"])        # missing price stays missing
    assert list(adj.columns) == ["A", "B"]            # Z is not a priced security


def test_back_adjust_without_events_is_the_raw_series():
    raw = frame([[100.0], [110.0]], ["2026-01-05", "2026-01-06"])
    empty = pd.DataFrame(index=pd.DatetimeIndex([], name="date"))
    pd.testing.assert_frame_equal(etl.back_adjust(raw, empty), raw)


def test_combined_ratio_multiplies_same_day_events_across_tables(store):
    store.write_frames("dividend_tse", {"twse_divide_ratio": frame([[0.9]], ["2026-01-07"])}, now=NOW)
    nan = float("nan")
    store.write_frames("capital_reduction_tse",
                       {"twse_cap_divide_ratio": frame([[2.0, nan], [nan, 3.0]],
                                                       ["2026-01-07", "2026-02-01"], ("A", "B"))}, now=NOW)

    combined = etl.combined_ratio(store)

    assert combined.loc["2026-01-07", "A"] == pytest.approx(1.8)
    assert combined.loc["2026-02-01", "B"] == pytest.approx(3.0)
    assert combined.loc["2026-02-01", "A"] == 1.0                 # no event → neutral


def test_combined_ratio_with_no_event_tables_is_empty(store):
    assert etl.combined_ratio(store).empty


def test_derive_rejects_non_positive_ratios(store):
    store.write_frames("price", {f: frame([[100.0]], ["2026-01-05"]) for f in etl.PRICE_SOURCE.values()}, now=NOW)
    store.write_frames("dividend_tse", {"twse_divide_ratio": frame([[0.0]], ["2026-01-06"])}, now=NOW)
    with pytest.raises(DeriveError):
        etl.derive(store)


# ── Seam 2: the derived pipeline ───────────────────────────────────────────

def test_registry_entry_is_a_partial_coverage_derived_dataset():
    spec = registry.get_spec("etl")
    assert spec.is_derived and spec.coverage == "partial"
    assert spec.fields == ("adj_close", "adj_open", "adj_high", "adj_low")
    assert set(spec.depends_on) == {"price", "dividend_tse", "dividend_otc", "capital_reduction_tse",
                                    "capital_reduction_otc", "par_value_change_tse", "par_value_change_otc"}
    assert spec.frequency == "daily"
    assert spec.cadence.kind == "daily" and spec.cadence.at == "22:00"    # after price (21:32)
    assert spec.backfill_start == dt.date(2007, 4, 23)


def test_price_missing_quarantines_the_run(mongo, store):
    result = run_etl(mongo, store)
    assert result.status == "quarantined"
    assert any("derive" in f and "price" in f for f in result.failures)
    assert not store.manifest_path("etl").exists()
    assert mongo.runs("etl")[0]["status"] == "quarantined"


def test_unmaterialized_event_tables_mean_no_events(seeded):
    mongo, store = seeded
    result = run_etl(mongo, store)

    assert result.status == "ok"
    for field, source in etl.PRICE_SOURCE.items():
        adj, raw = data.get(f"etl:{field}"), data.get(f"price:{source}")
        pd.testing.assert_frame_equal(pd.DataFrame(adj), pd.DataFrame(raw))


def test_golden_tsmc_ex_dividend_back_adjustment(seeded):
    mongo, store = seeded
    assert run_dividend_tse(mongo, store).status == "ok"
    assert run_etl(mongo, store).status == "ok"
    adj, raw = data.get("etl:adj_close"), data.get("price:收盤價")

    # (i) on and after the ex-date the adjusted close IS the raw close
    assert adj.loc["2026-06-11", "2330"] == raw.loc["2026-06-11", "2330"] == 2250.0
    assert adj.loc["2026-06-12", "2330"] == raw.loc["2026-06-12", "2330"] == 2310.0
    # (ii) the day before is raw × ratio — i.e. the Witness's after_price
    assert adj.loc["2026-06-10", "2330"] == pytest.approx(TSMC_BEFORE * TSMC_RATIO, abs=1e-6)
    assert adj.loc["2026-06-10", "2330"] == pytest.approx(TSMC_REFERENCE, abs=1e-6)
    assert adj.loc["2026-06-09", "2330"] == pytest.approx(2305.0 * TSMC_RATIO, abs=1e-6)
    # (iii) the adjusted day-over-day return across the ex-date is what the
    #       Witness's before/after prices imply: close on the ex-date vs. the
    #       restated (after_price) close of the day before.
    adjusted_return = adj.loc["2026-06-11", "2330"] / adj.loc["2026-06-10", "2330"] - 1
    witness_return = 2250.0 / TSMC_REFERENCE - 1
    assert adjusted_return == pytest.approx(witness_return, abs=1e-9)
    assert adjusted_return > 2250.0 / TSMC_BEFORE - 1           # the raw series shows a spurious drop
    # open/high/low are adjusted by the same factor
    for field, source in (("adj_open", "開盤價"), ("adj_high", "最高價"), ("adj_low", "最低價")):
        f, r = data.get(f"etl:{field}"), data.get(f"price:{source}")
        assert f.loc["2026-06-10", "2330"] == pytest.approx(r.loc["2026-06-10", "2330"] * TSMC_RATIO, abs=1e-6)
        assert f.loc["2026-06-11", "2330"] == r.loc["2026-06-11", "2330"]
    # a security without a dividend event is untouched
    assert adj.loc["2026-06-10", "2380"] == raw.loc["2026-06-10", "2380"] == 5.6


def test_capital_reduction_scales_earlier_prices_up(seeded):
    mongo, store = seeded
    assert run_capital_reduction_tse(mongo, store).status == "ok"
    assert run_etl(mongo, store).status == "ok"
    adj, raw = data.get("etl:adj_close"), data.get("price:收盤價")

    assert adj.loc["2026-06-16", "2380"] == pytest.approx(HL_LAST_CLOSE * HL_RATIO, abs=1e-6)
    assert adj.loc["2026-06-16", "2380"] == pytest.approx(HL_REFERENCE, abs=1e-6)
    assert adj.loc["2026-06-16", "2380"] > raw.loc["2026-06-16", "2380"] == 6.6
    assert adj.loc["2026-06-09", "2380"] == pytest.approx(5.66 * HL_RATIO, abs=1e-6)     # all of pre-halt history
    assert adj.loc["2026-06-29", "2380"] == raw.loc["2026-06-29", "2380"] == 21.5
    assert adj.loc["2026-06-30", "2380"] == 19.35
    assert data.get("etl:adj_low").loc["2026-06-16", "2380"] == pytest.approx(6.26 * HL_RATIO, abs=1e-6)
    # TSMC has no known event in this store: unchanged
    assert adj.loc["2026-06-10", "2330"] == raw.loc["2026-06-10", "2330"]


def test_rerun_after_a_new_event_readjusts_history(seeded):
    mongo, store = seeded
    assert run_dividend_tse(mongo, store).status == "ok"
    assert run_etl(mongo, store).status == "ok"
    first = data.get("etl:adj_close")
    assert first.loc["2026-06-16", "2380"] == 6.6                  # reduction not yet collected
    tsmc_before = first.loc["2026-06-10", "2330"]

    assert run_capital_reduction_tse(mongo, store).status == "ok"
    later = NOW + dt.timedelta(days=1)
    assert run_etl(mongo, store, now=later).status == "ok"
    second = data.get("etl:adj_close")

    assert second.loc["2026-06-16", "2380"] == pytest.approx(HL_REFERENCE, abs=1e-6)
    assert second.loc["2026-06-10", "2330"] == tsmc_before          # the other event still applied
    assert store.read_manifest("etl")["materialized_at"] == later.isoformat()


# ── Seam 1: data API ───────────────────────────────────────────────────────

@pytest.mark.parametrize("key", ["etl:adj_close", "etl:adj_open", "etl:adj_high", "etl:adj_low"])
def test_every_etl_key_resolves_to_a_daily_wide_frame(seeded, key):
    mongo, store = seeded
    assert run_etl(mongo, store).status == "ok"

    frame = data.get(key)

    assert isinstance(frame, FinlabDataFrame)
    assert frame._freq == "daily"
    assert isinstance(frame.index, pd.DatetimeIndex) and frame.index.name == "date"
    assert frame.index.equals(data.get("price:收盤價").index)
    assert "2330" in frame.columns and "2380" in frame.columns
    assert all(isinstance(c, str) for c in frame.columns)
