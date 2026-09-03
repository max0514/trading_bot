"""Corporate Actions (twlab 05) across the three seams.

Six Event Tables — dividend_tse/otc, capital_reduction_tse/otc,
par_value_change_tse/otc — share one reference-price parser parameterized by
a table spec, and are served as Wide Frames per Field (index = event date).

Seam 3: parse(raw) → rows against REAL recorded responses (golden values
        cross-checked with the FinMind Witness — see
        tests/fixtures/corporate_actions/README.md).
Seam 2: the pipeline with HTTP faked, asserted through run outcomes and the API.
Seam 1: every Catalog key of the six Datasets resolves through data.get().
"""
import copy
import datetime as dt
import math

import pandas as pd
import pytest

from twlab import catalog, data, pipeline, registry
from twlab.dataframe import FinlabDataFrame
from twlab.datasets import corporate_actions as ca
from twlab.errors import ParseError
from twlab.store.parquet import ParquetStore

from conftest import FakeSession, load_fixture

DS = "corporate_actions"
DAY = dt.date(2026, 9, 2)                   # every recording ends on 2026-09-02
NOW = dt.datetime(2026, 9, 2, 20, 0)

# Witness (FinMind TaiwanStockDividendResult / TaiwanStockCapitalReductionReferencePrice)
TSMC_BEFORE, TSMC_REFERENCE = 2255.0, 2248.99          # 2330, ex-dividend 2026-06-11
SMC_BEFORE, SMC_REFERENCE = 234.0, 231.5               # 5483, ex-dividend 2026-07-23
GW_BEFORE, GW_REFERENCE = 1480.0, 1474.3               # 6488, ex-dividend 2026-07-16
HL_LAST_CLOSE, HL_REFERENCE = 6.6, 23.86               # 2380, capital reduction 2026-06-29

# Per Dataset: (URL fragment the fetch hits, recorded fixture, golden Stock ID, event date)
SEEDS = {
    "dividend_tse": ("TWT49U", "twse_twt49u_20260601_20260902.json", "2330", "2026-06-11"),
    "dividend_otc": ("bulletin/exDailyQ", "tpex_exdailyq_20260601_20260902.json", "5483", "2026-07-23"),
    "capital_reduction_tse": ("TWTAUU", "twse_twtauu_20260101_20260902.json", "2380", "2026-06-29"),
    "capital_reduction_otc": ("bulletin/revivt", "tpex_revivt_20260101_20260902.json", "8093", "2026-01-12"),
    "par_value_change_tse": ("change/TWTB8U", "twse_twtb8u_20200101_20260902.json", "8070", "2020-08-17"),
    "par_value_change_otc": ("bulletin/pvChgRslt", "tpex_pvchgrslt_20190101_20260902.json", "6548", "2019-09-09"),
}
EMPTY = {
    "dividend_tse": "twse_twt49u_20260829_20260830_empty.json",
    "dividend_otc": "tpex_exdailyq_20260829_20260830_empty.json",
    "capital_reduction_tse": "twse_twtauu_20260829_20260830_empty.json",
    "par_value_change_tse": "twse_twtb8u_20260829_20260830_empty.json",
}


def raw(dataset: str, name: str | None = None) -> dict:
    return {"dataset": dataset, "payload": load_fixture(name or SEEDS[dataset][1], DS)}


def session_for(dataset: str, payload=None) -> FakeSession:
    fragment, name, *_ = SEEDS[dataset]
    return FakeSession({fragment: payload if payload is not None else load_fixture(name, DS)})


def run(dataset: str, mongo, store, session=None, day=DAY, now=NOW):
    return pipeline.run(dataset, day, session=session or session_for(dataset),
                        mongo=mongo, store=store, now=now)


def columns_of(dataset: str) -> list[str]:
    return ["stock_id", "date", "market", *registry.get_spec(dataset).fields]


def row_of(rows: pd.DataFrame, stock_id: str, date: str) -> pd.Series:
    hit = rows[(rows["stock_id"] == stock_id) & (rows["date"] == pd.Timestamp(date))]
    assert len(hit) == 1, f"{stock_id} on {date}: {len(hit)} rows"
    return hit.iloc[0]


# ── Seam 3: parser ─────────────────────────────────────────────────────────

def test_dividend_tse_parse_shape_and_golden_values():
    rows = ca.parse(raw("dividend_tse"))

    assert list(rows.columns) == columns_of("dividend_tse")
    assert len(rows) == 916                                      # every event in the recording
    assert (rows["market"] == "TWSE").all()
    assert rows["權息"].value_counts().to_dict() == {"息": 831, "權息": 52, "權": 33}

    tsmc = row_of(rows, "2330", "2026-06-11")                    # ROC 115年06月11日, the ex-date
    assert tsmc["除權息前收盤價"] == TSMC_BEFORE
    assert tsmc["除權息參考價"] == TSMC_REFERENCE
    assert tsmc["權值+息值"] == 6.000035
    assert tsmc["權息"] == "息"                                   # source column 權/息
    assert tsmc["漲停價格"] == 2470.0
    assert tsmc["跌停價格"] == 2025.0
    assert tsmc["開盤競價基準"] == 2250.0
    assert tsmc["減除股利參考價"] == TSMC_REFERENCE
    assert tsmc["詳細資料"] == "2330,20260611"
    assert tsmc["最近一次申報資料 季別日期"] == "115年第2季"       # MOPS link suffix stripped
    assert tsmc["最近一次申報每股 (單位)淨值"] == 248.05
    assert tsmc["最近一次申報每股 (單位)盈餘"] == 49.33
    assert tsmc["twse_divide_ratio"] == pytest.approx(TSMC_REFERENCE / TSMC_BEFORE)

    wiwynn = row_of(rows, "6669", "2026-09-02")                  # 200% stock dividend, comma-grouped
    assert wiwynn["除權息前收盤價"] == 7800.0
    assert wiwynn["權息"] == "權"
    assert wiwynn["twse_divide_ratio"] == pytest.approx(2614.99 / 7800.0)


def test_dividend_tse_blank_filing_cells_become_missing():
    rows = ca.parse(raw("dividend_tse"))
    etf = row_of(rows, "00953B", "2026-06-02")                    # bond ETF: no MOPS filing
    assert math.isnan(etf["最近一次申報每股 (單位)淨值"])
    assert math.isnan(etf["最近一次申報每股 (單位)盈餘"])
    assert etf["最近一次申報資料 季別日期"] is None
    assert etf["twse_divide_ratio"] == pytest.approx(9.51 / 9.58)


def test_dividend_otc_parse_maps_source_columns_to_catalog_fields():
    rows = ca.parse(raw("dividend_otc"))

    assert list(rows.columns) == columns_of("dividend_otc")
    assert len(rows) == 687
    assert (rows["market"] == "TPEx").all()
    assert rows["權息"].value_counts().to_dict() == {"除息": 613, "除權息": 42, "除權": 32}

    smc = row_of(rows, "5483", "2026-07-23")                     # ROC 115/07/23
    assert smc["除權息前收盤價"] == SMC_BEFORE
    assert smc["除權息參考價"] == SMC_REFERENCE
    assert smc["權值"] == 0.0
    assert smc["息值"] == 2.5
    assert smc["權+息值"] == 2.5                                  # source column 權值+息值
    assert smc["權息"] == "除息"                                  # TPEx prints text; Catalog types it float
    assert smc["漲停價格"] == 254.5                               # source column 漲停價
    assert smc["跌停價格"] == 208.5
    assert smc["開盤競價基準"] == 231.5                           # source column 開始交易基準價
    assert smc["現金股利"] == 2.5
    assert smc["每千股無償配股"] == 0.0                           # source column 每仟股無償配股
    assert smc["otc_divide_ratio"] == pytest.approx(SMC_REFERENCE / SMC_BEFORE)

    gw = row_of(rows, "6488", "2026-07-16")
    assert gw["otc_divide_ratio"] == pytest.approx(GW_REFERENCE / GW_BEFORE)

    rights = row_of(rows, "3709", "2026-06-01")                  # rights issue alongside 除權
    assert rights["權息"] == "除權"
    assert rights["現金增資股數"] == 20_000_000
    assert rights["現金增資認購價"] == 71.0
    assert rights["原股東認購數"] == 16_000_000                  # source column 原股東認購股數
    assert rights["按持股比例千股認購"] == 127.4195276            # source column 按持股比例仟股認購


def test_capital_reduction_tse_golden_values_and_ratio_above_one():
    rows = ca.parse(raw("capital_reduction_tse"))

    assert list(rows.columns) == columns_of("capital_reduction_tse")
    assert len(rows) == 4
    hl = row_of(rows, "2380", "2026-06-29")                      # ROC 115/06/29
    assert hl["恢復買賣日期"] == pd.Timestamp("2026-06-29")       # datetime Field, also the row date
    assert hl["停止買賣前收盤價格"] == HL_LAST_CLOSE
    assert hl["恢復買賣參考價"] == HL_REFERENCE
    assert hl["漲停價格"] == 26.2
    assert hl["跌停價格"] == 21.5
    assert hl["開盤競價基準"] == 23.85
    assert math.isnan(hl["除權參考價"])                           # "--" at the source
    assert hl["減資原因"] == "彌補虧損"
    assert hl["twse_cap_divide_ratio"] == pytest.approx(HL_REFERENCE / HL_LAST_CLOSE)
    assert hl["twse_cap_divide_ratio"] > 1

    tunghwa = row_of(rows, "1414", "2026-01-12")                 # Witness: 17.45 → 18.27, cash refund
    assert tunghwa["減資原因"] == "退還股款"
    assert tunghwa["twse_cap_divide_ratio"] == pytest.approx(18.27 / 17.45)


def test_capital_reduction_otc_keeps_catalog_string_date():
    rows = ca.parse(raw("capital_reduction_otc"))

    assert list(rows.columns) == columns_of("capital_reduction_otc")
    assert len(rows) == 5
    row = row_of(rows, "8093", "2026-01-12")                     # ROC 1150112
    assert row["恢復買賣日期"] == "1150112"                       # the Catalog types this Field str
    assert row["最後交易之收盤價格"] == 13.0                       # source column 最後交易日之收盤價格
    assert row["減資恢復買賣開始日參考價格"] == 21.67
    assert row["開始交易基準價"] == 21.65
    assert row["除權參考價"] == 0.0
    assert row["減資原因"] == "彌補虧損"
    assert row["otc_cap_divide_ratio"] == pytest.approx(21.67 / 13.0)
    assert "詳細資料" not in rows.columns                         # TPEx's HTML fragment is not a Catalog Field


def test_par_value_change_tables():
    tse = ca.parse(raw("par_value_change_tse"))
    assert list(tse.columns) == columns_of("par_value_change_tse")
    assert len(tse) == 9
    row = row_of(tse, "8070", "2020-08-17")                      # 面額 10 → 1
    assert row["恢復買賣日期"] == pd.Timestamp("2020-08-17")
    assert row["停止買賣前收盤價格"] == 190.0
    assert row["恢復買賣參考價"] == 19.0
    assert row["開盤競價基準"] == 19.0
    assert row["twse_par_value_change_divide_ratio"] == pytest.approx(0.1)
    assert row_of(tse, "6415", "2022-07-13")["停止買賣前收盤價格"] == 2485.0   # "2,485.00"

    otc = ca.parse(raw("par_value_change_otc"))
    assert list(otc.columns) == columns_of("par_value_change_otc")
    assert len(otc) == 14
    row = row_of(otc, "6548", "2019-09-09")                      # ROC 1080909, 面額 10 → 1
    assert row["恢復買賣日期"] == pd.Timestamp("2019-09-09")
    assert row["最後交易日之收盤價格"] == 312.0
    assert row["恢復買賣開始日參考價"] == 31.2                    # source column 恢復買賣開始參考價
    assert row["開始交易基準價"] == 31.2
    assert row["otc_par_value_change_divide_ratio"] == pytest.approx(0.1)
    assert row_of(otc, "5314", "2025-03-31")["otc_par_value_change_divide_ratio"] == pytest.approx(0.05)


def test_renamed_column_fails_loudly():
    with pytest.raises(ParseError, match="除權息參考價"):
        ca.parse(raw("dividend_tse", "twse_twt49u_20260601_20260902_malformed.json"))


@pytest.mark.parametrize("dataset", sorted(EMPTY))
def test_event_free_window_yields_empty_batch(dataset):
    rows = ca.parse(raw(dataset, EMPTY[dataset]))
    assert rows.empty
    assert list(rows.columns) == columns_of(dataset)


def test_unexpected_stat_and_unknown_dataset_rejected():
    with pytest.raises(ParseError):
        ca.parse({"dataset": "dividend_tse", "payload": {"stat": "系統忙碌中"}})
    with pytest.raises(ParseError):
        ca.parse({"dataset": "dividend_otc", "payload": {"stat": "error", "tables": []}})
    with pytest.raises(ParseError):
        ca.parse({"dataset": "nasdaq_dividends", "payload": {}})


def test_ratio_is_missing_when_a_price_is_missing():
    payload = copy.deepcopy(load_fixture(SEEDS["dividend_tse"][1], DS))
    payload["data"][0][3] = "--"                                 # first row's 除權息前收盤價
    rows = ca.parse({"dataset": "dividend_tse", "payload": payload})
    assert math.isnan(rows.iloc[0]["除權息前收盤價"])
    assert math.isnan(rows.iloc[0]["twse_divide_ratio"])
    assert rows["twse_divide_ratio"].notna().sum() == 915


@pytest.mark.parametrize("text, expected", [
    ("115年06月11日", "2026-06-11"),   # TWSE TWT49U
    ("115/01/12", "2026-01-12"),       # TWSE TWTAUU / TWTB8U
    ("1150112", "2026-01-12"),         # TPEx
    ("2026/06/11", "2026-06-11"),      # Gregorian, just in case
])
def test_source_dates_are_parsed(text, expected):
    assert ca._parse_date(text) == pd.Timestamp(expected)


def test_unparseable_date_and_number_rejected():
    with pytest.raises(ParseError):
        ca._parse_date("十五年六月")
    assert ca._parse_number("2,248.99") == 2248.99
    assert ca._parse_number("--") is None
    assert ca._parse_number("") is None
    with pytest.raises(ParseError):
        ca._parse_number("12a4")
    assert ca._parse_text("115年第2季(https://mops.twse.com.tw/mops/web/t163sb01)") == "115年第2季"
    assert ca._parse_text("  鑫聯大投控           ") == "鑫聯大投控"
    assert ca._parse_text("--") is None


# ── Seam 2: pipeline ───────────────────────────────────────────────────────

def test_fetch_asks_for_the_trailing_31_day_window():
    session = session_for("dividend_tse")
    raws = ca.fetch(session, DAY, dataset="dividend_tse")
    assert [r["dataset"] for r in raws] == ["dividend_tse"]
    assert any("TWT49U" in c and "startDate=20260802&endDate=20260902" in c and "response=json" in c
               for c in session.calls)

    session = session_for("dividend_otc")
    ca.fetch(session, DAY, dataset="dividend_otc")
    assert any("bulletin/exDailyQ" in c and "startDate=2026%2F08%2F02&endDate=2026%2F09%2F02" in c
               for c in session.calls)


def test_full_run_materializes_one_frame_per_field(mongo, store_env):
    result = run("dividend_tse", mongo, ParquetStore(store_env))

    assert result.status == "ok"
    assert result.rows == 916
    ratio = data.get("dividend_tse:twse_divide_ratio")
    assert isinstance(ratio, FinlabDataFrame)
    assert ratio._freq == "daily"
    assert ratio.index.min() == pd.Timestamp("2026-06-01") and ratio.index.max() == pd.Timestamp("2026-09-02")
    assert ratio.loc["2026-06-11", "2330"] == pytest.approx(TSMC_REFERENCE / TSMC_BEFORE)
    assert pd.isna(ratio.loc["2026-06-11", "2612"])              # 中航's event was 06-01, not 06-11
    assert data.get("dividend_tse:權息").loc["2026-06-11", "2330"] == "息"
    assert data.get("dividend_tse:除權息前收盤價").loc["2026-06-11", "2330"] == TSMC_BEFORE


@pytest.mark.parametrize("dataset", sorted(EMPTY))
def test_empty_window_is_no_data_and_publishes_nothing(mongo, store, dataset):
    result = run(dataset, mongo, store, session=session_for(dataset, load_fixture(EMPTY[dataset], DS)))
    assert result.status == "no_data"
    assert not store.manifest_path(dataset).exists()


def test_overlapping_windows_are_idempotent(mongo, store):
    first = run("dividend_tse", mongo, store)
    second = run("dividend_tse", mongo, store, day=DAY + dt.timedelta(days=5), now=NOW + dt.timedelta(days=5))
    assert first.status == second.status == "ok"
    assert mongo.collection("dividend_tse").count_documents({}) == first.rows


def test_parse_failure_quarantines_and_keeps_last_good_frames(mongo, store_env):
    api_store = ParquetStore(store_env)
    assert run("dividend_tse", mongo, api_store).status == "ok"

    malformed = session_for("dividend_tse", load_fixture("twse_twt49u_20260601_20260902_malformed.json", DS))
    bad = run("dividend_tse", mongo, api_store, session=malformed,
              day=DAY + dt.timedelta(days=1), now=NOW + dt.timedelta(days=1))

    assert bad.status == "quarantined"
    assert any("parse" in f for f in bad.failures)
    ratio = data.get("dividend_tse:twse_divide_ratio")
    assert ratio.loc["2026-06-11", "2330"] == pytest.approx(TSMC_REFERENCE / TSMC_BEFORE)


def test_implausible_ratio_is_quarantined(mongo, store_env):
    api_store = ParquetStore(store_env)
    assert run("dividend_tse", mongo, api_store).status == "ok"

    payload = copy.deepcopy(load_fixture(SEEDS["dividend_tse"][1], DS))
    payload["data"][0][4] = "577.00"                             # 中航's reference price 10× its close
    bad = run("dividend_tse", mongo, api_store, session=session_for("dividend_tse", payload),
              day=DAY + dt.timedelta(days=1), now=NOW + dt.timedelta(days=1))

    assert bad.status == "quarantined"
    assert any("ratio_range" in f for f in bad.failures)
    ratio = data.get("dividend_tse:twse_divide_ratio")
    assert ratio.loc["2026-06-11", "2330"] == pytest.approx(TSMC_REFERENCE / TSMC_BEFORE)


def test_non_positive_reference_price_is_quarantined(mongo, store):
    payload = copy.deepcopy(load_fixture(SEEDS["capital_reduction_tse"][1], DS))
    payload["data"][0][4] = "0.00"                               # 東和's 恢復買賣參考價
    bad = run("capital_reduction_tse", mongo, store, session=session_for("capital_reduction_tse", payload))
    assert bad.status == "quarantined"
    assert any("reference_price_positive" in f for f in bad.failures)
    assert not store.manifest_path("capital_reduction_tse").exists()


def test_run_outcomes_are_logged(mongo, store):
    run("capital_reduction_otc", mongo, store)
    runs = mongo.runs("capital_reduction_otc")
    assert len(runs) == 1
    assert runs[0]["status"] == "ok"
    assert runs[0]["rows"] == 5
    assert runs[0]["at"] == NOW


# ── Seam 1: data API — every Catalog key of the six Datasets ──────────────

def catalog_keys() -> list[str]:
    return [f.key for name in SEEDS for f in catalog.dataset_fields(name)]


def test_six_datasets_are_registered_with_full_catalog_coverage():
    for name in SEEDS:
        spec = registry.get_spec(name)
        assert spec.coverage == "full"
        assert spec.cadence.kind == "daily" and spec.cadence.at == "20:00"
        assert {f"{name}:{f}" for f in spec.fields} == {f.key for f in catalog.dataset_fields(name)}
    assert registry.get_spec("dividend_tse").backfill_start == dt.date(2003, 5, 6)
    assert registry.get_spec("dividend_otc").backfill_start == dt.date(2008, 1, 10)
    assert registry.get_spec("capital_reduction_tse").backfill_start == dt.date(2011, 1, 25)
    assert registry.get_spec("capital_reduction_otc").backfill_start == dt.date(2013, 1, 16)
    assert registry.get_spec("par_value_change_tse").backfill_start == dt.date(2020, 8, 17)
    assert registry.get_spec("par_value_change_otc").backfill_start == dt.date(2019, 9, 9)


@pytest.mark.parametrize("key", catalog_keys())
def test_every_catalog_key_resolves_to_an_event_wide_frame(mongo, store_env, key):
    dataset, field = catalog.split_key(key)
    _, _, golden_id, event_date = SEEDS[dataset]
    assert run(dataset, mongo, ParquetStore(store_env)).status == "ok"   # real recordings pass every Invariant

    frame = data.get(key)

    assert isinstance(frame, FinlabDataFrame)
    assert isinstance(frame.index, pd.DatetimeIndex) and frame.index.name == "date"
    assert all(isinstance(c, str) for c in frame.columns)
    assert pd.Timestamp(event_date) in frame.index
    assert golden_id in frame.columns
    # Round trip: the published cell is exactly what the parser emitted.
    expected = row_of(ca.parse(raw(dataset)), golden_id, event_date)[field]
    served = frame.loc[event_date, golden_id]
    if expected is None or (isinstance(expected, float) and math.isnan(expected)):
        assert pd.isna(served)
    else:
        assert served == expected
