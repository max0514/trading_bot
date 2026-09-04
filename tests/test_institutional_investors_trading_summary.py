"""institutional_investors_trading_summary (twlab 06) across the three seams.

Seam 3: parse(raw) → rows against the recorded T86 (TWSE) and insti/dailyTrade
(TPEx) responses — a renamed T86 column must fail loudly; TPEx's flat header
(seven unnamed 買進/賣出/買賣超 triples) is pinned by position and guarded by
the page's own arithmetic.
Seam 2: the pipeline with HTTP faked, asserted through run outcomes and the API,
including the 買賣超 == 買進 − 賣出 Invariant that catches a column shift.
Seam 1: every Catalog key resolves to a daily Wide Frame carrying the golden values.
"""
import datetime as dt

import pandas as pd
import pytest

from twlab import catalog, data, pipeline, sources
from twlab.dataframe import FinlabDataFrame
from twlab.datasets import institutional_investors_trading_summary as insti
from twlab.errors import ParseError
from twlab.store.parquet import ParquetStore

from conftest import FakeSession, load_fixture

DS = "institutional_investors_trading_summary"
DAY = dt.date(2026, 9, 2)
NOW = dt.datetime(2026, 9, 2, 21, 32)
TWSE_ROWS, TPEX_ROWS = 1338, 892        # securities in the recorded responses
KEYS = sorted(f.key for f in catalog.dataset_fields(DS))

# Golden values (股) from the real recordings for 2026-09-02 — see
# tests/fixtures/institutional_investors_trading_summary/README.md. The TSMC
# row also matches the FinMind Witness for that day.
TSMC = {
    "外陸資買進股數(不含外資自營商)": 9_328_102, "外陸資賣出股數(不含外資自營商)": 21_315_085,
    "外陸資買賣超股數(不含外資自營商)": -11_986_983,
    "外資自營商買進股數": 0, "外資自營商賣出股數": 0, "外資自營商買賣超股數": 0,
    "投信買進股數": 121_786, "投信賣出股數": 958_531, "投信買賣超股數": -836_745,
    "自營商買進股數(自行買賣)": 150_315, "自營商賣出股數(自行買賣)": 77_500, "自營商買賣超股數(自行買賣)": 72_815,
    "自營商買進股數(避險)": 94_571, "自營商賣出股數(避險)": 94_902, "自營商買賣超股數(避險)": -331,
}
SAS = {  # 5483 中美晶, TPEx
    "外陸資買進股數(不含外資自營商)": 2_747_050, "外陸資賣出股數(不含外資自營商)": 4_592_000,
    "外陸資買賣超股數(不含外資自營商)": -1_844_950,
    "外資自營商買進股數": 0, "外資自營商賣出股數": 0, "外資自營商買賣超股數": 0,
    "投信買進股數": 0, "投信賣出股數": 44_000, "投信買賣超股數": -44_000,
    "自營商買進股數(自行買賣)": 175_000, "自營商賣出股數(自行買賣)": 63_960, "自營商買賣超股數(自行買賣)": 111_040,
    "自營商買進股數(避險)": 87_952, "自營商賣出股數(避險)": 147_009, "自營商買賣超股數(避險)": -59_057,
}


@pytest.fixture
def t86_payload() -> dict:
    return load_fixture("twse_t86_20260902.json", DS)


@pytest.fixture
def tpex_insti_payload() -> dict:
    return load_fixture("tpex_insti_daily_trade_20260902.json", DS)


@pytest.fixture
def insti_session(t86_payload, tpex_insti_payload) -> FakeSession:
    return FakeSession({"twse.com.tw": t86_payload, "tpex.org.tw": tpex_insti_payload})


def run_insti(session, mongo, store, day=DAY, now=NOW):
    return pipeline.run(DS, day, session=session, mongo=mongo, store=store, now=now)


def with_tpex_table(payload: dict, **changes) -> dict:
    table = payload["tables"][0]
    return {**payload, "tables": [{**table, **changes}, *payload["tables"][1:]]}


# ── Seam 3: parser ─────────────────────────────────────────────────────────

def test_twse_parse_shape_and_golden_values(t86_payload):
    rows = insti.parse({"source": "twse", "payload": t86_payload})

    assert list(rows.columns) == ["stock_id", "date", "market", *insti.FIELDS]
    assert len(rows) == TWSE_ROWS
    assert (rows["market"] == "TWSE").all()
    assert rows["date"].nunique() == 1
    assert rows["date"].iloc[0] == pd.Timestamp("2026-09-02")
    # T86 carries totals the Catalog does not: they must not leak in as Fields.
    assert "三大法人買賣超股數" not in rows.columns
    assert "自營商買賣超股數" not in rows.columns

    tsmc = rows[rows["stock_id"] == "2330"].iloc[0]
    for field, value in TSMC.items():
        assert tsmc[field] == value, field


def test_twse_names_are_not_part_of_the_key(t86_payload):
    rows = insti.parse({"source": "twse", "payload": t86_payload})
    assert rows["stock_id"].is_unique
    assert not rows["stock_id"].str.contains(r"\s").any()   # padded 證券名稱 never bleeds into the ID


def test_tpex_parse_locates_groups_by_position(tpex_insti_payload):
    rows = insti.parse({"source": "tpex", "payload": tpex_insti_payload})

    assert list(rows.columns) == ["stock_id", "date", "market", *insti.FIELDS]
    assert len(rows) == TPEX_ROWS
    assert (rows["market"] == "TPEx").all()
    assert rows["date"].iloc[0] == pd.Timestamp("2026-09-02")   # from the ROC table date 115/09/02

    sas = rows[rows["stock_id"] == "5483"].iloc[0]
    for field, value in SAS.items():
        assert sas[field] == value, field


def test_renamed_column_fails_loudly():
    payload = load_fixture("twse_t86_20260902_malformed.json", DS)
    with pytest.raises(ParseError, match="外陸資買進股數"):
        insti.parse({"source": "twse", "payload": payload})


def test_tpex_header_pattern_drift_fails_loudly(tpex_insti_payload):
    fields = tpex_insti_payload["tables"][0]["fields"]
    one_triple_short = fields[:-4] + fields[-1:]           # six groups instead of seven
    with pytest.raises(ParseError, match="header is not"):
        insti.parse({"source": "tpex", "payload": with_tpex_table(tpex_insti_payload, fields=one_triple_short)})
    renamed = [("淨買超股數" if f == "買賣超股數" else f) for f in fields]
    with pytest.raises(ParseError, match="header is not"):
        insti.parse({"source": "tpex", "payload": with_tpex_table(tpex_insti_payload, fields=renamed)})


def test_tpex_reordered_data_fails_arithmetic_guard(tpex_insti_payload):
    # The header stays intact but the data's 外陸資買進 and 外資自營商買進
    # columns trade places: only the page's own arithmetic can tell.
    def swapped(row):
        row = list(row)
        row[2], row[5] = row[5], row[2]
        return row

    data = [swapped(r) for r in tpex_insti_payload["tables"][0]["data"]]
    with pytest.raises(ParseError, match="do not add up"):
        insti.parse({"source": "tpex", "payload": with_tpex_table(tpex_insti_payload, data=data)})


def test_twse_no_data_day_yields_empty_batch():
    holiday = {"stat": "很抱歉，沒有符合條件的資料!", "date": "20260906"}
    assert insti.parse({"source": "twse", "payload": holiday}).empty


def test_tpex_no_data_day_yields_empty_batch(tpex_insti_payload):
    holiday = with_tpex_table(tpex_insti_payload, data=[], totalCount=0)
    assert insti.parse({"source": "tpex", "payload": holiday}).empty


def test_unexpected_stat_rejected(t86_payload):
    with pytest.raises(ParseError, match="stat"):
        insti.parse({"source": "twse", "payload": {**t86_payload, "stat": "系統維護中"}})


def test_unknown_source_rejected():
    with pytest.raises(ParseError):
        insti.parse({"source": "nasdaq", "payload": {}})


def test_share_counts_parse_as_signed_integers():
    assert sources.parse_int("26,004,465") == 26_004_465
    assert sources.parse_int("-58,090") == -58_090
    assert sources.parse_int("0") == 0
    assert sources.parse_int("--") is None
    assert sources.parse_int("") is None
    with pytest.raises(ParseError):
        sources.parse_int("12a4")
    with pytest.raises(ParseError):
        sources.parse_int("1,234.5")   # a fractional share count is drift, not data


def test_dates_parse_in_twse_and_roc_forms():
    assert sources.parse_date("20260902", "TWSE") == pd.Timestamp("2026-09-02")
    assert sources.parse_date("2026/09/02", "TPEx") == pd.Timestamp("2026-09-02")
    assert sources.parse_date("115/09/02", "TPEx") == pd.Timestamp("2026-09-02")
    with pytest.raises(ParseError):
        sources.parse_date("115/13/02", "TPEx")


# ── Seam 2: pipeline ───────────────────────────────────────────────────────

def test_fetch_asks_both_sources_for_the_day(insti_session):
    raws = insti.fetch(insti_session, DAY)
    assert [r["source"] for r in raws] == ["twse", "tpex"]
    twse_call, tpex_call = insti_session.calls
    assert "fund/T86" in twse_call and "date=20260902" in twse_call and "selectType=ALLBUT0999" in twse_call
    assert "insti/dailyTrade" in tpex_call and "date=2026%2F09%2F02" in tpex_call and "sect=EW" in tpex_call


def test_full_run_materializes_wide_frames(insti_session, mongo, store_env):
    result = run_insti(insti_session, mongo, ParquetStore(store_env))

    assert result.status == "ok"
    assert result.rows == TWSE_ROWS + TPEX_ROWS
    net = data.get(f"{DS}:外陸資買賣超股數(不含外資自營商)")
    assert net.loc["2026-09-02", "2330"] == TSMC["外陸資買賣超股數(不含外資自營商)"]
    assert net.loc["2026-09-02", "5483"] == SAS["外陸資買賣超股數(不含外資自營商)"]
    assert net._freq == "daily"


def test_rerun_same_day_is_idempotent(insti_session, mongo, store):
    first = run_insti(insti_session, mongo, store)
    second = run_insti(insti_session, mongo, store)
    assert first.status == second.status == "ok"
    assert mongo.collection(DS).count_documents({}) == first.rows


def test_truncated_response_is_quarantined_and_last_good_frames_survive(
    insti_session, mongo, store_env, tpex_insti_payload
):
    api_store = ParquetStore(store_env)
    assert run_insti(insti_session, mongo, api_store).status == "ok"

    tiny = FakeSession({
        "twse.com.tw": load_fixture("twse_t86_20260902_tiny.json", DS),
        "tpex.org.tw": tpex_insti_payload,
    })
    bad = run_insti(tiny, mongo, api_store, day=DAY + dt.timedelta(days=1),
                    now=NOW + dt.timedelta(days=1))

    assert bad.status == "quarantined"
    assert any("min_rows" in f for f in bad.failures)
    buy = data.get(f"{DS}:投信買進股數")
    assert list(buy.index) == [pd.Timestamp("2026-09-02")]      # day 2 never published
    assert buy.loc["2026-09-02", "2330"] == TSMC["投信買進股數"]
    assert "9998" not in buy.columns                           # the bad batch's poison Stock ID


def test_column_shift_is_caught_by_net_consistency_invariant(
    t86_payload, tpex_insti_payload, mongo, store
):
    # The 買進 and 賣出 headers swap while the data stays put: every expected
    # name is still present, so parsing succeeds — but 買賣超 ≠ 買進 − 賣出.
    fields = list(t86_payload["fields"])
    i, j = fields.index("外陸資買進股數(不含外資自營商)"), fields.index("外陸資賣出股數(不含外資自營商)")
    fields[i], fields[j] = fields[j], fields[i]
    shifted = FakeSession({
        "twse.com.tw": {**t86_payload, "fields": fields},
        "tpex.org.tw": tpex_insti_payload,
    })
    result = run_insti(shifted, mongo, store)

    assert result.status == "quarantined"
    assert any("net_equals_buy_minus_sell" in f for f in result.failures)
    assert not store.manifest_path(DS).exists()


def test_parse_failure_quarantines_run(mongo, store, tpex_insti_payload):
    malformed = FakeSession({
        "twse.com.tw": load_fixture("twse_t86_20260902_malformed.json", DS),
        "tpex.org.tw": tpex_insti_payload,
    })
    result = run_insti(malformed, mongo, store)

    assert result.status == "quarantined"
    assert any("parse" in f for f in result.failures)
    assert mongo.collection(DS).count_documents({}) == 0
    assert not store.manifest_path(DS).exists()


def test_holiday_run_reports_no_data(mongo, store, tpex_insti_payload):
    closed = FakeSession({
        "twse.com.tw": {"stat": "很抱歉，沒有符合條件的資料!", "date": "20260906"},
        "tpex.org.tw": with_tpex_table(tpex_insti_payload, data=[], totalCount=0),
    })
    result = run_insti(closed, mongo, store, day=dt.date(2026, 9, 6))
    assert result.status == "no_data"
    assert mongo.collection(DS).count_documents({}) == 0


# ── Seam 1: data API ───────────────────────────────────────────────────────

@pytest.fixture
def seeded(insti_session, mongo, store_env):
    result = run_insti(insti_session, mongo, ParquetStore(store_env))
    assert result.status == "ok"
    return result


def test_catalog_promises_fifteen_share_count_fields():
    assert len(KEYS) == 15
    assert all(catalog.resolve(k).dtype == "int" for k in KEYS)


@pytest.mark.parametrize("key", KEYS)
def test_every_catalog_key_resolves_with_golden_values(seeded, key):
    frame = data.get(key)
    field = key.split(":", 1)[1]

    assert isinstance(frame, FinlabDataFrame)
    assert isinstance(frame.index, pd.DatetimeIndex)
    assert frame.index.name == "date"
    assert all(isinstance(c, str) for c in frame.columns)
    assert frame.loc["2026-09-02", "2330"] == TSMC[field]   # TWSE security
    assert frame.loc["2026-09-02", "5483"] == SAS[field]    # TPEx security
