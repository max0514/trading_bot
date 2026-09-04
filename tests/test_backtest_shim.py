"""twlab 12 — the backtest shim and the verbatim-FinLab acceptance bar.

`backtest/data.py` keeps its legacy API over twlab so the repo's existing
strategy runs unchanged, and FinLab's PEG example runs with only its two
import lines changed, through twlab.data and the existing backtest engine.

The store is seeded through the real pipelines with the recorded fixtures
(prices, valuation ratios, monthly revenue, statements); because the
recordings cover one trading day, price and valuation history is then
extended to 80 trading days with a deterministic random walk seeded from the
recorded cross-section — documented here, so the backtest has something to
trade.
"""
import math
import runpy
import shutil
from pathlib import Path

import mongomock
import numpy as np
import pandas as pd
import pytest

from backtest import data as legacy
from backtest.engine import Report
from twlab import data, pipeline
from twlab.backtest import sim
from twlab.dataframe import FinlabDataFrame
from twlab.store.mongo import MongoStore
from twlab.store.parquet import ParquetStore

from conftest import DAY, NOW, FakeSession, load_fixture
from test_financial_statement import (
    ANNUAL_DEADLINE, ANNUAL_NOW, Q1_DEADLINE, annual_session, q1_session, seed_universe,
)
from test_financial_statement import NOW as FS_NOW
from test_monthly_revenue import JULY_DEADLINE, MAY_DEADLINE
from test_monthly_revenue import session_for as revenue_session
from test_price_earning_ratio import session_for as pe_session

REPO = Path(__file__).resolve().parent.parent
HISTORY_DAYS = 80
LAST_DAY = pd.Timestamp("2026-08-18")


def extend_history(store: ParquetStore, dataset: str, walk_fields: tuple[str, ...],
                   rng: np.random.Generator) -> None:
    """Grow a one-day Dataset into HISTORY_DAYS trading days ending on LAST_DAY.

    Fields in `walk_fields` follow a mild random walk anchored on the recorded
    values (the recorded day keeps its exact values); every other Field is
    carried forward unchanged.
    """
    manifest = store.read_manifest(dataset)
    dates = pd.bdate_range(end=LAST_DAY, periods=HISTORY_DAYS, name="date")
    frames = {}
    for field in manifest["fields"]:
        recorded = store.read_frame(dataset, field)
        base = recorded.iloc[-1]
        if field in walk_fields:
            steps = 1 + rng.normal(0.001, 0.01, size=(HISTORY_DAYS, len(base)))
            anchor = list(dates).index(pd.Timestamp(recorded.index[-1]))
            path = np.cumprod(steps, axis=0)
            path = path / path[anchor]                    # recorded day == recorded values
            values = base.to_numpy(dtype=float)[None, :] * path
        else:
            values = np.repeat(base.to_numpy()[None, :], HISTORY_DAYS, axis=0)
        frames[field] = pd.DataFrame(values, index=dates, columns=recorded.columns)
    store.write_frames(dataset, frames, now=NOW, frequency=manifest["frequency"])


@pytest.fixture(scope="module")
def acceptance_store(tmp_path_factory):
    root = tmp_path_factory.mktemp("store")
    store = ParquetStore(root)
    mongo = MongoStore(client=mongomock.MongoClient(), db_name="twlab_shim_module")

    # Statements need a small universe first (their fetch reads price columns).
    seed_universe(store)
    assert pipeline.run("financial_statement", ANNUAL_DEADLINE, session=annual_session(),
                        mongo=mongo, store=store, now=ANNUAL_NOW).status == "ok"
    assert pipeline.run("financial_statement", Q1_DEADLINE, session=q1_session(),
                        mongo=mongo, store=store, now=FS_NOW).status == "ok"

    prices = FakeSession({"twse.com.tw": load_fixture("twse_mi_index_20260807.json"),
                          "tpex.org.tw": load_fixture("tpex_daily_quotes_20260807.json")})
    assert pipeline.run("price", DAY, session=prices, mongo=mongo, store=store, now=NOW).status == "ok"
    assert pipeline.run("price_earning_ratio", DAY, session=pe_session(), mongo=mongo,
                        store=store, now=NOW).status == "ok"
    assert pipeline.run("monthly_revenue", MAY_DEADLINE, session=revenue_session(5), mongo=mongo,
                        store=store, now=NOW).status == "ok"
    assert pipeline.run("monthly_revenue", JULY_DEADLINE, session=revenue_session(7), mongo=mongo,
                        store=store, now=NOW).status == "ok"

    rng = np.random.default_rng(20260807)
    extend_history(store, "price", ("收盤價", "開盤價", "最高價", "最低價"), rng)
    extend_history(store, "price_earning_ratio", ("本益比",), rng)
    return root


@pytest.fixture
def acceptance_api(acceptance_store, monkeypatch):
    monkeypatch.setenv("TWLAB_STORE_DIR", str(acceptance_store))
    monkeypatch.delenv("TWLAB_REMOTE_STORE", raising=False)
    monkeypatch.delenv("TWLAB_SERVER_URL", raising=False)


# ── the shim ───────────────────────────────────────────────────────────────

class TestShim:
    @pytest.mark.parametrize("field,key", sorted(legacy.PRICE_FIELD_MAP.items()))
    def test_price_fields_map_to_twlab_keys(self, acceptance_api, field, key):
        frame = legacy.get_price(field)
        assert isinstance(frame, FinlabDataFrame)
        assert frame.equals(pd.DataFrame(data.get(f"price:{key}")))
        assert frame.loc["2026-08-07", "2330"] == data.get(f"price:{key}").loc["2026-08-07", "2330"]

    def test_monthly_revenue_keeps_legacy_units_and_gains_deadline_dating(self, acceptance_api):
        revenue = legacy.get_monthly_revenue("revenue")
        assert revenue.loc["2026-08-10", "2330"] == 467_580_548 * 1000       # 元, as before
        assert list(revenue.index) == [pd.Timestamp("2026-06-10"), pd.Timestamp("2026-08-10")]
        assert legacy.get_monthly_revenue("yoy").loc["2026-08-10", "2330"] == 44.68
        with pytest.raises(ValueError, match="yoy"):
            legacy.get_monthly_revenue("qoq")

    def test_financial_accepts_finmind_names_and_catalog_fields(self, acceptance_api):
        revenue = legacy.get_financial("Revenue")
        assert revenue.loc["2026-05-15", "2330"] == 1_134_103_440
        assert revenue._freq == "quarterly"
        assert legacy.get_financial("OperatingExpenses").equals(legacy.get_financial("營業費用"))
        with pytest.raises(ValueError, match="FinMind names"):
            legacy.get_financial("Ebitda")

    def test_list_stocks_and_unknown_price_field(self, acceptance_api):
        stocks = legacy.list_stocks()
        assert "2330" in stocks and "5483" in stocks
        with pytest.raises(ValueError, match="Choices"):
            legacy.get_price("vwap")


def test_existing_strategy_runs_unchanged_through_the_shim(acceptance_api, tmp_path, capsys):
    """stock_strategies/rd_strong_short_term.py, byte for byte, on twlab data."""
    source = REPO / "stock_strategies" / "rd_strong_short_term.py"
    target_dir = tmp_path / "stock_strategies"           # it saves CSVs next to its parent
    target_dir.mkdir()
    shutil.copy(source, target_dir / source.name)

    runpy.run_path(str(target_dir / source.name), run_name="__main__")

    out = capsys.readouterr().out
    assert "=== RD_Strong_ShortTerm ===" in out
    assert (tmp_path / "rd_strong_equity.csv").exists()
    assert (tmp_path / "rd_strong_trades.csv").exists()


# ── the acceptance bar: FinLab's PEG example, verbatim ─────────────────────

def test_finlab_peg_example_runs_verbatim(acceptance_api):
    namespace = runpy.run_path(str(REPO / "examples" / "finlab_peg_strategy.py"))

    position, report = namespace["position"], namespace["report"]
    assert isinstance(position, FinlabDataFrame)
    assert position._freq == "daily"                      # monthly YoY auto-aligned to daily
    # Same-frequency operands keep pandas semantics (as in FinLab): Stock IDs
    # present on only one side come back as missing, which sim() treats as not
    # held. Where both sides exist the result is boolean.
    held = position.astype(float).fillna(0.0) > 0
    assert held.to_numpy().any()                          # the screen selects something
    assert not held.loc[:"2026-06-09"].to_numpy().any()   # nothing knowable before the first deadline
    assert position.loc[:, position.dtypes.eq(bool)].shape[1] > 100

    assert isinstance(report, Report)
    assert report.metrics["n_days"] == HISTORY_DAYS
    assert report.equity.notna().all() and (report.equity > 0).all()
    assert math.isfinite(report.metrics["total_return"])
    assert report.metrics["n_trades"] >= 1
    assert set(report.trades["stock_id"]) <= set(position.columns)

    # Only the two import lines differ from the finlab original.
    text = (REPO / "examples" / "finlab_peg_strategy.py").read_text(encoding="utf-8")
    assert "from twlab import data" in text and "from twlab.backtest import sim" in text
    assert 'sim(position, resample="M")' in text


def test_sim_adapter_maps_a_finlab_position_onto_the_engine(acceptance_api):
    close = data.get("price:收盤價")
    # Hold TSMC from the July revenue deadline (a Monday here) onwards, nothing else.
    position = FinlabDataFrame(False, index=pd.DatetimeIndex(["2026-06-10", "2026-08-10"], name="date"),
                               columns=close.columns)
    position._freq = "monthly"
    position.loc["2026-08-10", "2330"] = True

    daily = sim(position, resample="D")            # rebalance every day: the signal applies at once
    assert daily.positions.loc["2026-08-10":, "2330"].all()
    assert not daily.positions.loc[:"2026-08-07", "2330"].any()
    assert daily.positions.drop(columns="2330").to_numpy().sum() == 0
    assert list(daily.trades["stock_id"]) == ["2330"]
    assert daily.trades["entry_date"].iloc[0] == pd.Timestamp("2026-08-11")   # next day's open

    monthly = sim(position, resample="M")          # FinLab's default cadence: the month's rebalance day
    assert monthly.positions.loc["2026-08-18", "2330"]
    assert not monthly.positions.loc["2026-08-10", "2330"]


def test_sim_simulates_on_adjusted_prices_when_they_are_materialized(acceptance_api, acceptance_store):
    """Story 6 / #15: no phantom ex-date crashes — sim() reads etl:adj_* like FinLab."""
    store = ParquetStore(acceptance_store)
    raw_open = store.read_frame("price", "開盤價")
    adjusted = {
        "adj_close": store.read_frame("price", "收盤價") * 0.5,
        "adj_open": raw_open * 0.5,
        "adj_high": store.read_frame("price", "最高價") * 0.5,
        "adj_low": store.read_frame("price", "最低價") * 0.5,
    }
    store.write_frames("etl", adjusted, now=NOW, frequency="daily")
    try:
        position = FinlabDataFrame(False, index=raw_open.index, columns=raw_open.columns)
        position._freq = "daily"
        position.loc["2026-08-10":, "2330"] = True
        report = sim(position, resample="D")
        entry = report.trades.iloc[0]
        assert entry["entry_price"] == pytest.approx(raw_open.loc[entry["entry_date"], "2330"] * 0.5)
    finally:
        shutil.rmtree(store.root / "etl")
