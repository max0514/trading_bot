"""fundamental_features (twlab 10): the 53 derived 財務指標.

Seam 1 on a synthetic statement store — every formula checked against hand
arithmetic, including averages, year-over-year growth, and division by zero —
and on the real MOPS recordings through the financial_statement pipeline, so
the golden values are TSMC's actual Q1 2026 filing.
"""
import datetime as dt
import math
import random
import shutil

import mongomock
import numpy as np
import pandas as pd
import pytest

from twlab import catalog, data, pipeline, registry
from twlab.datasets import fundamental_features as ff
from twlab.store.mongo import MongoStore
from twlab.store.parquet import ParquetStore

from twlab import witness
from conftest import FakeSession, load_fixture
from test_financial_statement import (
    ANNUAL_DEADLINE, ANNUAL_NOW, NOW, Q1_DEADLINE, TSMC_Q1_2026,
    annual_session, q1_session, seed_universe,
)

DS = "fundamental_features"
DEADLINES = pd.DatetimeIndex(
    ["2025-03-31", "2025-05-15", "2025-08-14", "2025-11-14", "2026-03-31", "2026-05-15"],
    name="date",
)


def synthetic_statements() -> dict[str, pd.DataFrame]:
    """Six quarters for two companies with simple, hand-checkable numbers."""
    n = len(DEADLINES)
    rev_a = np.array([1000, 1100, 1200, 1300, 1400, 1500], dtype=float)
    rev_b = np.array([500, 0, 500, 500, 500, 600], dtype=float)     # a zero-revenue quarter
    cost_a, cost_b = 600.0, 300.0
    gross_a, gross_b = rev_a - cost_a, rev_b - cost_b
    opex = 100.0
    op_a, op_b = gross_a - opex, gross_b - opex
    pretax_a, pretax_b = op_a + 10, op_b + 10
    tax_a, tax_b = pretax_a * 0.2, pretax_b * 0.2
    net_a, net_b = pretax_a - tax_a, pretax_b - tax_b
    assets_a = 4000 + 100 * np.arange(n)

    def frame(a, b):
        a = np.broadcast_to(np.asarray(a, dtype=float), (n,))
        b = np.broadcast_to(np.asarray(b, dtype=float), (n,))
        return pd.DataFrame({"A": a, "B": b}, index=DEADLINES)

    return {
        "營業收入淨額": frame(rev_a, rev_b), "營業成本": frame(cost_a, cost_b),
        "營業毛利": frame(gross_a, gross_b), "營業費用": frame(opex, opex),
        "研究發展費": frame(20, 20), "推銷費用": frame(30, 30), "管理費用": frame(50, 50),
        "營業利益": frame(op_a, op_b), "營業外收入及支出": frame(10, 10),
        "稅前淨利": frame(pretax_a, pretax_b), "所得稅費用": frame(tax_a, tax_b),
        "繼續營業單位損益": frame(net_a, net_b), "合併總損益": frame(net_a, net_b),
        "本期綜合損益總額": frame(net_a + 5, net_b + 5),
        "歸屬母公司淨利_損": frame(net_a - 1, net_b - 1), "綜合損益歸屬母公司": frame(net_a + 4, net_b + 4),
        "每股盈餘": frame(2.0, 1.0),
        "折舊費用": frame(50, 50), "攤銷費用": frame(10, 10), "利息費用": frame(5, np.nan),
        "應收帳款及票據": frame(200, 200), "存貨": frame(300, np.nan),
        "流動資產": frame(1000, 1000), "不動產廠房及設備": frame(2000, 2000),
        "資產總額": frame(assets_a, 4000), "流動負債": frame(500, 500), "負債總額": frame(1500, 1500),
        "普通股股本": frame(1000, 1000), "母公司股東權益合計": frame(2400, 2400),
        "股東權益總額": frame(2500, 2500),
        "營業活動之淨現金流入_流出": frame(300, 300), "取得不動產_廠房及設備": frame(-80, 80),
    }


@pytest.fixture
def synthetic(mongo, store_env):
    store = ParquetStore(store_env)
    store.write_frames("financial_statement", synthetic_statements(), now=NOW, frequency="quarterly")
    result = pipeline.run(DS, Q1_DEADLINE, mongo=mongo, store=store, now=NOW)
    assert result.status == "ok"
    return store


LAST = "2026-05-15"


def test_registry_declares_the_53_catalog_fields():
    spec = registry.get_spec(DS)
    assert spec.is_derived and spec.depends_on == ("financial_statement",)
    assert len(spec.fields) == 53
    assert {f"{DS}:{f}" for f in spec.fields} == {f.key for f in catalog.dataset_fields(DS)}


class TestFormulas:
    def test_passthroughs_and_amounts(self, synthetic):
        assert data.get("fundamental_features:營業利益").loc[LAST, "A"] == 800.0
        assert data.get("fundamental_features:EBITDA").loc[LAST, "A"] == 800 + 50 + 10
        assert data.get("fundamental_features:營運現金流").loc[LAST, "A"] == 300.0
        assert data.get("fundamental_features:歸屬母公司淨利").loc[LAST, "A"] == 648 - 1
        assert data.get("fundamental_features:營運資金").loc[LAST, "A"] == 1000 - 500
        assert data.get("fundamental_features:自由現金流量").loc[LAST, "A"] == 300 - 80
        assert data.get("fundamental_features:自由現金流量").loc[LAST, "B"] == 300 - 80   # capex sign-agnostic
        assert data.get("fundamental_features:經常稅後淨利").loc[LAST, "A"] == 648.0

    def test_margins_and_rates_are_percentages(self, synthetic):
        assert data.get("fundamental_features:營業毛利率").loc[LAST, "A"] == pytest.approx(60.0)
        assert data.get("fundamental_features:營業利益率").loc[LAST, "A"] == pytest.approx(800 / 1500 * 100)
        assert data.get("fundamental_features:稅率").loc[LAST, "A"] == pytest.approx(20.0)
        assert data.get("fundamental_features:貝里比率").loc[LAST, "A"] == pytest.approx(900.0)
        assert data.get("fundamental_features:研究發展費用率").loc[LAST, "A"] == pytest.approx(20 / 1500 * 100)
        assert data.get("fundamental_features:負債比率").loc[LAST, "A"] == pytest.approx(1500 / 4500 * 100)
        assert data.get("fundamental_features:淨值除資產").loc[LAST, "A"] == pytest.approx(2500 / 4500 * 100)
        assert data.get("fundamental_features:流動比率").loc[LAST, "A"] == pytest.approx(200.0)
        assert data.get("fundamental_features:速動比率").loc[LAST, "A"] == pytest.approx(140.0)
        assert data.get("fundamental_features:利息支出率").loc[LAST, "A"] == pytest.approx(5 / 1500 * 100)
        assert data.get("fundamental_features:現金流量比率").loc[LAST, "A"] == pytest.approx(60.0)

    def test_per_share_values_use_the_10_dollar_par_share_count(self, synthetic):
        # 股本 1000 仟元 at 10 元 par = 100 仟股
        assert data.get("fundamental_features:每股營業額").loc[LAST, "A"] == pytest.approx(15.0)
        assert data.get("fundamental_features:每股營業利益").loc[LAST, "A"] == pytest.approx(8.0)
        assert data.get("fundamental_features:每股現金流量").loc[LAST, "A"] == pytest.approx(3.0)
        assert data.get("fundamental_features:每股稅前淨利").loc[LAST, "A"] == pytest.approx(8.1)
        assert data.get("fundamental_features:每股稅後淨利").loc[LAST, "A"] == 2.0     # reported EPS

    def test_returns_use_average_balances(self, synthetic):
        # A's total assets grow 100 a quarter: avg of 4400 and 4500 on the last row
        roa = data.get("fundamental_features:ROA稅後息前")
        assert roa.loc[LAST, "A"] == pytest.approx((648 + 5 * 0.8) / 4450 * 100)
        # first row (revenue 1000 → net 248): its own balance, no earlier quarter to average
        assert roa.loc["2025-03-31", "A"] == pytest.approx((248 + 5 * 0.8) / 4000 * 100)
        assert data.get("fundamental_features:ROE稅後").loc[LAST, "A"] == pytest.approx(647 / 2400 * 100)
        assert data.get("fundamental_features:ROE綜合損益").loc[LAST, "A"] == pytest.approx(652 / 2400 * 100)
        assert data.get("fundamental_features:總資產週轉次數").loc[LAST, "A"] == pytest.approx(1500 / 4450)
        assert data.get("fundamental_features:存貨週轉率").loc[LAST, "A"] == pytest.approx(600 / 300)
        # missing interest expense counts as none, not as a missing ratio
        assert not math.isnan(roa.loc[LAST, "B"])

    def test_growth_compares_with_the_same_quarter_a_year_earlier(self, synthetic):
        growth = data.get("fundamental_features:營收成長率")
        assert growth.loc[LAST, "A"] == pytest.approx((1500 / 1100 - 1) * 100)
        assert growth.loc["2025-11-14", "A"].item() != growth.loc["2025-11-14", "A"].item()   # NaN: no year-ago row
        assets_growth = data.get("fundamental_features:資產總額成長率")
        assert assets_growth.loc[LAST, "A"] == pytest.approx((4500 / 4100 - 1) * 100)
        assert data.get("fundamental_features:淨值成長率").loc[LAST, "A"] == pytest.approx(0.0)

    def test_division_by_zero_is_missing_not_infinite(self, synthetic):
        margin = data.get("fundamental_features:營業毛利率")
        assert math.isnan(margin.loc["2025-05-15", "B"])          # zero revenue quarter
        assert not np.isinf(margin.to_numpy()).any()
        quick = data.get("fundamental_features:速動比率")
        assert quick.loc[LAST, "B"] == pytest.approx(200.0)       # no inventory line: nothing subtracted

    def test_frames_keep_the_quarterly_deadline_index_and_tag(self, synthetic):
        frame = data.get("fundamental_features:營業毛利率")
        assert frame._freq == "quarterly"
        assert list(frame.index) == list(DEADLINES)
        assert list(frame.columns) == ["A", "B"]


def test_missing_statements_quarantine_the_run(mongo, store):
    result = pipeline.run(DS, Q1_DEADLINE, mongo=mongo, store=store, now=NOW)
    assert result.status == "quarantined"
    assert "financial_statement" in result.failures[0]
    assert not store.has_dataset(DS)


# ── real MOPS recordings through the financial_statement pipeline ──────────

@pytest.fixture(scope="module")
def real_store(tmp_path_factory):
    root = tmp_path_factory.mktemp("store")
    store = ParquetStore(root)
    seed_universe(store)
    mongo = MongoStore(client=mongomock.MongoClient(), db_name="twlab_ff_module")
    assert pipeline.run("financial_statement", ANNUAL_DEADLINE, session=annual_session(),
                        mongo=mongo, store=store, now=ANNUAL_NOW).status == "ok"
    assert pipeline.run("financial_statement", Q1_DEADLINE, session=q1_session(),
                        mongo=mongo, store=store, now=NOW).status == "ok"
    assert pipeline.run(DS, Q1_DEADLINE, mongo=mongo, store=store,
                        now=dt.datetime(2026, 5, 15, 23, 0)).status == "ok"
    return root


@pytest.fixture
def real_api(real_store, monkeypatch):
    monkeypatch.setenv("TWLAB_STORE_DIR", str(real_store))
    monkeypatch.delenv("TWLAB_REMOTE_STORE", raising=False)
    monkeypatch.delenv("TWLAB_SERVER_URL", raising=False)


@pytest.mark.parametrize("key", sorted(f.key for f in catalog.dataset_fields(DS)))
def test_every_catalog_key_resolves(real_api, key):
    frame = data.get(key)
    assert frame._freq == "quarterly"
    assert list(frame.index) == [pd.Timestamp("2026-03-31"), pd.Timestamp("2026-05-15")]
    assert "2330" in frame.columns


def test_tsmc_q1_2026_ratios_match_the_filing(real_api):
    """Golden values recomputed by hand from TSMC's real Q1 2026 statement."""
    g = TSMC_Q1_2026
    at = lambda key: data.get(f"fundamental_features:{key}").loc["2026-05-15", "2330"]
    assert at("營業毛利率") == pytest.approx(g["營業毛利"] / g["營業收入淨額"] * 100)          # 66.25 %
    assert at("營業利益率") == pytest.approx(g["營業利益"] / g["營業收入淨額"] * 100)          # 58.10 %
    assert at("稅後淨利率") == pytest.approx(g["合併總損益"] / g["營業收入淨額"] * 100)
    assert at("稅率") == pytest.approx(g["所得稅費用"] / g["稅前淨利"] * 100)                  # 16.72 %
    assert at("負債比率") == pytest.approx(g["負債總額"] / g["資產總額"] * 100)               # 31.50 %
    assert at("EBITDA") == g["營業利益"] + g["折舊費用"] + g["攤銷費用"]
    assert at("每股稅後淨利") == g["每股盈餘"]                                                 # 22.08
    assert at("每股營業額") == pytest.approx(g["營業收入淨額"] / (g["普通股股本"] / 10))      # ≈ 43.7 元
    assert at("自由現金流量") == g["營業活動之淨現金流入_流出"] - abs(g["取得不動產_廠房及設備"])
    assert at("營運資金") == g["流動資產"] - g["流動負債"]
    assert at("流動比率") == pytest.approx(g["流動資產"] / g["流動負債"] * 100)
    assert at("歸屬母公司淨利") == g["歸屬母公司淨利_損"]
    # ROE uses the average of the Q4-2025 and Q1-2026 parent equity as published.
    equity = data.get("financial_statement:母公司股東權益合計").loc[:, "2330"]
    assert at("ROE稅後") == pytest.approx(g["歸屬母公司淨利_損"] / equity.mean() * 100)
    # A year-ago quarter is not in the store, so growth is unknown — not zero.
    assert math.isnan(at("營收成長率"))


# ── the external check: agreement with the Witness, not with our own inputs ──

def finmind_session() -> FakeSession:
    """FinMind's recorded answers for the same three companies and quarters,
    keyed the way `FinMindClient` asks for them (one call per Stock ID)."""
    recorded = load_fixture("finmind_financial_statements_20251201_20260430.json", "witness")
    return FakeSession({f"data_id={sid}": payload for sid, payload in recorded.items()})


def test_derived_ratios_agree_with_the_witness(real_store):
    """twlab 13's acceptance criterion: the ratios must agree with an outside
    calculation, not just with the inputs twlab itself parsed.

    Every `fundamental_features` Probe is checked against FinMind's recording
    of the same filings — EPS and 營業利益 read off their line items, 營業毛利率
    and 營業利益率 recomputed from their 營業毛利/營業收入 and 營業利益/營業收入.
    """
    store = ParquetStore(real_store)
    mongo = MongoStore(client=mongomock.MongoClient(), db_name="twlab_ff_witness")
    client = witness.FinMindClient(finmind_session())

    reports = witness.run_witness(store, mongo, client, now=dt.datetime(2026, 5, 16, 1, 0),
                                  only=DS, samples=20, seed=7)

    assert {r.field for r in reports} == {"每股稅後淨利", "營業毛利率", "營業利益率", "營業利益"}
    for report in reports:
        assert report.mismatches == [], report.summary()
        assert report.checked - report.missing >= 3, report.summary()
    assert mongo.runs(DS)[0]["status"] == "witness_ok"


def test_the_witness_catches_a_ratio_that_drifts(real_store, tmp_path):
    """The check has teeth: halving one published ratio must be reported.

    On a copy of the store — the real one is shared by the whole module.
    """
    shutil.copytree(real_store, tmp_path / "store")
    store = ParquetStore(tmp_path / "store")
    client = witness.FinMindClient(finmind_session())
    probe = next(p for p in witness.PROBES if p.dataset == DS and p.field == "營業毛利率")

    assert witness.check(probe, store, client, samples=20, rng=random.Random(7)).mismatches == []

    store.write_frames(DS, {"營業毛利率": store.read_frame(DS, "營業毛利率") / 2},
                       now=dt.datetime(2026, 5, 16, 1, 0), frequency="quarterly")
    drifted = witness.check(probe, store, client, samples=20, rng=random.Random(7))
    assert drifted.mismatches, drifted.summary()
    assert all(m.ours == pytest.approx(m.witness / 2) for m in drifted.mismatches)
