"""price_earning_ratio (twlab 04) across the three seams.

Seam 3: parse(raw) → rows against the recorded TWSE BWIBBU_d response and a
format-accurate TPEx peQryDate fixture.
Seam 2: the pipeline with HTTP faked, asserted through run outcomes and the API.
Seam 1: every Catalog key resolves to a Wide Frame carrying the golden values.
"""
import datetime as dt

import pandas as pd
import pytest

from twlab import catalog, data, pipeline
from twlab.dataframe import FinlabDataFrame
from twlab.datasets import price_earning_ratio
from twlab.errors import ParseError
from twlab.store.parquet import ParquetStore

from conftest import DAY, NOW, FakeSession, load_fixture

DS = "price_earning_ratio"
CATALOG_KEYS = [f.key for f in catalog.dataset_fields(DS)]
TWSE_ROWS = 1082   # every security in the recorded BWIBBU_d response
TPEX_ROWS = 12

# Golden values from the real TWSE recording for 2026-08-07; the FinMind
# Witness (TaiwanStockPER) reports the same three figures for 2330.
TSMC = {"殖利率(%)": 0.93, "本益比": 31.86, "股價淨值比": 10.43}
# TPEx golden values are the Witness's figures embedded in the synthesized
# fixture — see tests/fixtures/price_earning_ratio/README.md.
SAS = {"殖利率(%)": 2.08, "本益比": 23.93, "股價淨值比": 2.09}   # 5483 中美晶


def raw(source, name):
    return {"source": source, "payload": load_fixture(name, DS)}


def session_for(twse="twse_bwibbu_d_20260807.json",
                tpex="tpex_pe_qry_date_20260807.json") -> FakeSession:
    return FakeSession({
        "BWIBBU_d": load_fixture(twse, DS),
        "peQryDate": load_fixture(tpex, DS),
    })


def run(session, mongo, store, day=DAY, now=NOW):
    return pipeline.run(DS, day, session=session, mongo=mongo, store=store, now=now)


# ── Seam 3: parser ─────────────────────────────────────────────────────────

def test_twse_parse_shape_and_golden_values():
    rows = price_earning_ratio.parse(raw("twse", "twse_bwibbu_d_20260807.json"))

    assert list(rows.columns) == ["stock_id", "date", "market", *price_earning_ratio.FIELDS]
    assert len(rows) == TWSE_ROWS
    assert (rows["market"] == "TWSE").all()
    assert rows["date"].nunique() == 1
    assert rows["date"].iloc[0] == pd.Timestamp("2026-08-07")

    tsmc = rows[rows["stock_id"] == "2330"].iloc[0]
    for field, value in TSMC.items():
        assert tsmc[field] == value, field


def test_twse_dash_means_missing():
    rows = price_earning_ratio.parse(raw("twse", "twse_bwibbu_d_20260807.json"))
    # 台泥 posted a loss, so TWSE prints its 本益比 as "-".
    taiwan_cement = rows[rows["stock_id"] == "1101"].iloc[0]
    assert pd.isna(taiwan_cement["本益比"])
    assert taiwan_cement["殖利率(%)"] == 3.29
    assert taiwan_cement["股價淨值比"] == 0.78


def test_tpex_parse_maps_source_columns_to_catalog_fields():
    rows = price_earning_ratio.parse(raw("tpex", "tpex_pe_qry_date_20260807.json"))

    assert list(rows.columns) == ["stock_id", "date", "market", *price_earning_ratio.FIELDS]
    assert len(rows) == TPEX_ROWS
    assert (rows["market"] == "TPEx").all()
    assert rows["date"].iloc[0] == pd.Timestamp("2026-08-07")

    sas = rows[rows["stock_id"] == "5483"].iloc[0]
    for field, value in SAS.items():
        assert sas[field] == value, field
    # TPEx prints "N/A" for the 本益比 of a loss-making company.
    medigen = rows[rows["stock_id"] == "6547"].iloc[0]
    assert pd.isna(medigen["本益比"])
    assert medigen["殖利率(%)"] == 0.0
    assert medigen["股價淨值比"] == 4.82


def test_renamed_column_fails_loudly():
    with pytest.raises(ParseError, match="本益比"):
        price_earning_ratio.parse(raw("twse", "twse_bwibbu_d_20260807_malformed.json"))


def test_twse_no_data_day_yields_empty_batch():
    holiday = {"stat": "很抱歉，沒有符合條件的資料!"}
    rows = price_earning_ratio.parse({"source": "twse", "payload": holiday})
    assert rows.empty
    assert list(rows.columns) == ["stock_id", "date", "market", *price_earning_ratio.FIELDS]


def test_tpex_empty_table_yields_empty_batch():
    payload = load_fixture("tpex_pe_qry_date_20260807.json", DS)
    table = {**payload["tables"][0], "data": [], "totalCount": 0}
    rows = price_earning_ratio.parse({"source": "tpex", "payload": {**payload, "tables": [table]}})
    assert rows.empty


def test_unexpected_stat_rejected():
    with pytest.raises(ParseError, match="stat"):
        price_earning_ratio.parse({"source": "twse", "payload": {"stat": "ERROR"}})
    with pytest.raises(ParseError, match="stat"):
        price_earning_ratio.parse({"source": "tpex", "payload": {"stat": "fail", "tables": []}})


def test_unknown_source_rejected():
    with pytest.raises(ParseError):
        price_earning_ratio.parse({"source": "nasdaq", "payload": {}})


def test_unparseable_number_rejected():
    assert price_earning_ratio._parse_number("2,370.00") == 2370.0
    assert price_earning_ratio._parse_number("-") is None
    assert price_earning_ratio._parse_number("N/A") is None
    with pytest.raises(ParseError):
        price_earning_ratio._parse_number("31.8x")


# ── Seam 2: pipeline ───────────────────────────────────────────────────────

def test_fetch_asks_both_sources_for_the_day():
    session = session_for()
    raws = price_earning_ratio.fetch(session, DAY)
    assert [r["source"] for r in raws] == ["twse", "tpex"]
    assert any("BWIBBU_d?date=20260807&selectType=ALL&response=json" in c for c in session.calls)
    assert any("peQryDate?date=2026%2F08%2F07&response=json" in c for c in session.calls)


def test_full_run_materializes_wide_frames(mongo, store_env):
    result = run(session_for(), mongo, ParquetStore(store_env))

    assert result.status == "ok"
    assert result.rows == TWSE_ROWS + TPEX_ROWS
    pe = data.get("price_earning_ratio:本益比")
    assert list(pe.index) == [pd.Timestamp("2026-08-07")]
    assert pe.loc["2026-08-07", "2330"] == TSMC["本益比"]   # TWSE row
    assert pe.loc["2026-08-07", "5483"] == SAS["本益比"]    # TPEx row
    assert pe._freq == "daily"


@pytest.mark.parametrize("key", CATALOG_KEYS)
def test_every_catalog_key_resolves_to_a_wide_frame(mongo, store_env, key):
    run(session_for(), mongo, ParquetStore(store_env))
    frame = data.get(key)

    assert isinstance(frame, FinlabDataFrame)
    assert isinstance(frame.index, pd.DatetimeIndex)
    assert frame.index.name == "date"
    assert all(isinstance(c, str) for c in frame.columns)   # Stock IDs stay strings
    field = key.partition(":")[2]
    assert frame.loc["2026-08-07", "2330"] == TSMC[field]
    assert frame.loc["2026-08-07", "5483"] == SAS[field]


def test_rerun_is_idempotent(mongo, store):
    first = run(session_for(), mongo, store)
    second = run(session_for(), mongo, store)
    assert first.status == second.status == "ok"
    assert mongo.collection(DS).count_documents({}) == first.rows


def test_truncated_batch_is_quarantined_and_last_good_frames_survive(mongo, store_env):
    api_store = ParquetStore(store_env)
    good = run(session_for(), mongo, api_store)
    assert good.status == "ok"

    # Next night TWSE answers truncated: parses cleanly, but far too few rows.
    tiny = session_for(twse="twse_bwibbu_d_20260807_tiny.json")
    bad = run(tiny, mongo, api_store, day=DAY + dt.timedelta(days=1),
              now=NOW + dt.timedelta(days=1))

    assert bad.status == "quarantined"
    assert any("min_rows" in f for f in bad.failures)
    pe = data.get("price_earning_ratio:本益比")
    assert list(pe.index) == [pd.Timestamp("2026-08-07")]
    assert pe.loc["2026-08-07", "2330"] == TSMC["本益比"]
    assert "9998" not in pe.columns   # the bad batch's poison Stock ID


def test_missing_market_is_quarantined_even_above_the_row_floor(mongo, store_env):
    """The TWSE recording alone clears the row floor; an empty TPEx table on a
    trading day still means half the universe is missing."""
    payload = load_fixture("tpex_pe_qry_date_20260807.json", DS)
    empty = {**payload, "tables": [{**payload["tables"][0], "data": []}]}
    session = FakeSession({
        "BWIBBU_d": load_fixture("twse_bwibbu_d_20260807.json", DS),
        "peQryDate": empty,
    })
    result = run(session, mongo, ParquetStore(store_env))

    assert result.status == "quarantined"
    assert result.rows == TWSE_ROWS
    assert any("both_markets" in f for f in result.failures)
    assert not ParquetStore(store_env).has_dataset(DS)


def test_parse_failure_quarantines_run(mongo, store):
    malformed = session_for(twse="twse_bwibbu_d_20260807_malformed.json")
    result = run(malformed, mongo, store)

    assert result.status == "quarantined"
    assert any("parse" in f for f in result.failures)
    assert mongo.collection(DS).count_documents({}) == 0
    assert not store.manifest_path(DS).exists()
