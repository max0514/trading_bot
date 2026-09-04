"""monthly_revenue (twlab 03) across the three seams.

Seam 3: parse(raw) → rows against MOPS-format fixtures.
Seam 2: the pipeline with HTTP faked, asserted through run outcomes and the API.
Seam 1: Statutory-Deadline alignment — May revenue is first visible on June 10,
while the raw store keeps the revenue-month period.
"""
import datetime as dt

import pandas as pd
import pytest

from twlab import data, pipeline, registry
from twlab.dataframe import FinlabDataFrame
from twlab.datasets import monthly_revenue
from twlab.errors import ParseError
from twlab.store.parquet import ParquetStore

from conftest import FakeSession, load_text_fixture

DS = "monthly_revenue"
JULY_DEADLINE = dt.date(2026, 8, 10)    # July 2026 revenue is due by Aug 10
MAY_DEADLINE = dt.date(2026, 6, 10)
NOW = dt.datetime(2026, 8, 10, 22, 0)

# Golden values (千元) from the real MOPS recordings (cross-checked against the
# FinMind Witness) — see tests/fixtures/monthly_revenue/README.md.
TSMC_JULY = {"當月營收": 467_580_548, "上月營收": 442_679_969, "去年當月營收": 323_165_707,
             "上月比較增減(%)": 5.62, "去年同月增減(%)": 44.68,
             "當月累計營收": 2_872_064_238, "去年累計營收": 2_096_211_240, "前期比較增減(%)": 37.01}
SII_FILERS, OTC_FILERS = 992, 860          # companies in the July 2026 recordings
TSMC_MAY_REVENUE = 416_975_163


def raw(market, name, period="2026-07"):
    return {"source": market, "period": period, "payload": load_text_fixture(name, DS)}


def session_for(month: int) -> FakeSession:
    return FakeSession({
        f"/sii/t21sc03_115_{month}_0.html": load_text_fixture(f"mops_sii_t21sc03_115_{month}_0.html", DS),
        f"/otc/t21sc03_115_{month}_0.html": load_text_fixture(f"mops_otc_t21sc03_115_{month}_0.html", DS),
    })


# ── Seam 3: parser ─────────────────────────────────────────────────────────

def test_sii_parse_shape_and_golden_values():
    rows = monthly_revenue.parse(raw("sii", "mops_sii_t21sc03_115_7_0.html"))

    assert list(rows.columns) == ["stock_id", "date", "market", *monthly_revenue.FIELDS]
    assert (rows["market"] == "TWSE").all()
    assert rows["date"].nunique() == 1
    assert rows["date"].iloc[0] == pd.Timestamp("2026-07-01")   # the revenue month, not the deadline
    assert len(rows) == SII_FILERS
    assert "合計" not in rows["stock_id"].values                 # industry subtotal rows dropped

    tsmc = rows[rows["stock_id"] == "2330"].iloc[0]
    for field, value in TSMC_JULY.items():
        assert tsmc[field] == value, field


def test_otc_parse_maps_market():
    rows = monthly_revenue.parse(raw("otc", "mops_otc_t21sc03_115_7_0.html"))
    assert (rows["market"] == "TPEx").all()
    row = rows[rows["stock_id"] == "5483"].iloc[0]
    assert row["當月營收"] == 6_997_671
    assert row["上月比較增減(%)"] == -9.19


def test_renamed_column_fails_loudly():
    with pytest.raises(ParseError, match="當月營收"):
        monthly_revenue.parse(raw("sii", "mops_sii_t21sc03_115_7_0_malformed.html"))


def test_wrong_month_served_fails_loudly():
    with pytest.raises(ParseError, match="115年6月份"):
        monthly_revenue.parse(raw("sii", "mops_sii_t21sc03_115_7_0.html", period="2026-06"))


def test_unknown_source_rejected():
    with pytest.raises(ParseError):
        monthly_revenue.parse({"source": "nasdaq", "period": "2026-07", "payload": ""})


# ── Seam 2: pipeline ───────────────────────────────────────────────────────

def test_fetch_asks_mops_for_the_month_before_the_deadline():
    session = session_for(7)
    raws = monthly_revenue.fetch(session, JULY_DEADLINE)
    assert [r["source"] for r in raws] == ["sii", "otc"]
    assert all(r["period"] == "2026-07" for r in raws)
    assert any("/sii/t21sc03_115_7_0.html" in c for c in session.calls)
    assert any("/otc/t21sc03_115_7_0.html" in c for c in session.calls)


def test_full_run_materializes_deadline_indexed_frames(mongo, store_env):
    result = pipeline.run(DS, JULY_DEADLINE, session=session_for(7), mongo=mongo,
                          store=ParquetStore(store_env), now=NOW)

    assert result.status == "ok"
    assert result.rows == SII_FILERS + OTC_FILERS
    rev = data.get("monthly_revenue:當月營收")
    assert list(rev.index) == [pd.Timestamp("2026-08-10")]   # Statutory Deadline, not July 1
    assert rev.loc["2026-08-10", "2330"] == TSMC_JULY["當月營收"]
    assert rev.loc["2026-08-10", "5483"] == 6_997_671
    assert rev._freq == "monthly"


@pytest.mark.parametrize("field", monthly_revenue.FIELDS)
def test_every_catalog_key_resolves(mongo, store_env, field):
    pipeline.run(DS, JULY_DEADLINE, session=session_for(7), mongo=mongo,
                 store=ParquetStore(store_env), now=NOW)
    frame = data.get(f"monthly_revenue:{field}")
    assert frame.loc["2026-08-10", "2330"] == TSMC_JULY[field]


def test_rerun_is_idempotent(mongo, store):
    first = pipeline.run(DS, JULY_DEADLINE, session=session_for(7), mongo=mongo, store=store, now=NOW)
    second = pipeline.run(DS, JULY_DEADLINE, session=session_for(7), mongo=mongo, store=store, now=NOW)
    assert first.status == second.status == "ok"
    assert mongo.collection(DS).count_documents({}) == first.rows


def test_truncated_page_is_quarantined_and_last_good_frames_survive(mongo, store_env):
    api_store = ParquetStore(store_env)
    good = pipeline.run(DS, MAY_DEADLINE, session=session_for(5), mongo=mongo, store=api_store,
                        now=dt.datetime(2026, 6, 10, 22, 0))
    assert good.status == "ok"

    truncated = FakeSession({
        "/sii/": load_text_fixture("mops_sii_t21sc03_115_7_0_tiny.html", DS),
        "/otc/": load_text_fixture("mops_otc_t21sc03_115_7_0.html", DS),
    })
    bad = pipeline.run(DS, JULY_DEADLINE, session=truncated, mongo=mongo, store=api_store, now=NOW)

    assert bad.status == "quarantined"
    assert any("min_rows" in f for f in bad.failures)
    rev = data.get("monthly_revenue:當月營收")
    assert list(rev.index) == [pd.Timestamp("2026-06-10")]     # July never published
    assert rev.loc["2026-06-10", "2330"] == TSMC_MAY_REVENUE


# ── Seam 1: Point-in-Time Alignment ────────────────────────────────────────

def test_may_revenue_first_visible_on_june_10_and_period_kept_in_raw_store(mongo, store_env):
    api_store = ParquetStore(store_env)
    pipeline.run(DS, MAY_DEADLINE, session=session_for(5), mongo=mongo, store=api_store,
                 now=dt.datetime(2026, 6, 10, 22, 0))
    pipeline.run(DS, JULY_DEADLINE, session=session_for(7), mongo=mongo, store=api_store, now=NOW)

    rev = data.get("monthly_revenue:當月營收")
    assert list(rev.index) == [pd.Timestamp("2026-06-10"), pd.Timestamp("2026-08-10")]
    assert rev.loc["2026-06-10", "2330"] == TSMC_MAY_REVENUE
    assert rev.loc["2026-08-10", "2330"] == TSMC_JULY["當月營收"]

    # A daily condition never sees May revenue before June 10.
    daily = pd.bdate_range("2026-06-01", "2026-06-15", name="date")
    ones = FinlabDataFrame(1.0, index=daily, columns=["2330"])
    ones._freq = "daily"
    visible = (rev > 0) & (ones > 0)
    assert visible.index.min() == pd.Timestamp("2026-06-10")
    assert visible.loc["2026-06-10", "2330"]

    # The system of record keeps the revenue-month period, not the deadline.
    stored = mongo.load_long(registry.get_spec(DS))
    tsmc = stored[stored["stock_id"] == "2330"].sort_values("date")
    assert list(tsmc["date"]) == [pd.Timestamp("2026-05-01"), pd.Timestamp("2026-07-01")]
