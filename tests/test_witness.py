"""The Witness (twlab 07): FinMind samples cross-check published frames and
alert — never collect. A systematically wrong parse (千元 vs 元) must be caught.
"""
import datetime as dt

import pytest

from twlab import pipeline, witness
from twlab.store.parquet import ParquetStore

from conftest import DAY, NOW, FakeSession

LATER = NOW + dt.timedelta(hours=1)          # the Witness runs after the pipeline


def witness_runs(mongo):
    return [r for r in mongo.runs("price") if r["status"].startswith("witness")]


def finmind_rows(stock_id, date, close, volume):
    return {"msg": "success", "status": 200, "data": [
        {"date": date, "stock_id": stock_id, "Trading_Volume": volume, "Trading_money": 1,
         "open": close, "max": close, "min": close, "close": close, "spread": 0, "Trading_turnover": 1}
    ]}


def witness_answering(store, close_scale=1.0):
    """A FinMind stand-in that echoes what we published (optionally mis-scaled)."""
    close = store.read_frame("price", "收盤價")
    volume = store.read_frame("price", "成交股數")
    return FakeSession({
        f"data_id={sid}": finmind_rows(
            sid, "2026-08-07",
            float(close.loc["2026-08-07", sid]) * close_scale,
            float(volume.loc["2026-08-07", sid]),
        )
        for sid in close.columns
    })


@pytest.fixture
def published(good_session, mongo, store):
    assert pipeline.run("price", DAY, session=good_session, mongo=mongo, store=store, now=NOW).status == "ok"
    return store


def test_matching_samples_report_ok(published, mongo):
    # The Witness answers every sampled Stock ID with the value we published.
    fake = witness_answering(published)
    client = witness.FinMindClient(fake)

    reports = witness.run_witness(published, mongo, client, now=LATER, samples=5, seed=1)

    assert {r.dataset for r in reports} == {"price"}
    assert {r.field for r in reports} == {"收盤價", "成交股數"}
    assert all(r.checked == 5 and r.mismatches == [] for r in reports)
    runs = witness_runs(mongo)
    assert len(runs) == 1 and runs[0]["status"] == "witness_ok"   # one record per Dataset
    assert len(fake.calls) <= 10                     # one FinMind call per sampled Stock ID


def test_unit_error_is_flagged_as_an_alert(published, mongo):
    # The Witness reports closes in a unit 1000× ours — a 千元-vs-元 class bug.
    fake = witness_answering(published, close_scale=1000)
    reports = witness.run_witness(published, mongo, witness.FinMindClient(fake), now=LATER, samples=3, seed=1)

    close_report = next(r for r in reports if r.field == "收盤價")
    volume_report = next(r for r in reports if r.field == "成交股數")
    assert len(close_report.mismatches) == 3
    assert volume_report.mismatches == []
    assert close_report.mismatches[0].dataset == "price"
    run = witness_runs(mongo)[0]
    assert run["status"] == "witness_alert"
    assert "收盤價" in run["detail"][0]
    assert "witness=" in run["detail"][0]


def test_witness_gaps_are_reported_not_treated_as_mismatches(published, mongo):
    fake = FakeSession({"finmindtrade.com": {"msg": "success", "status": 200, "data": []}})
    reports = witness.run_witness(published, mongo, witness.FinMindClient(fake), now=LATER, samples=2, seed=1)
    assert all(r.mismatches == [] and r.missing == 2 for r in reports)
    run = witness_runs(mongo)[0]
    assert run["status"] == "witness_ok"
    assert "unanswered" in run["detail"][-1]


def test_unmaterialized_datasets_are_skipped(mongo, store):
    fake = FakeSession({})
    assert witness.run_witness(store, mongo, witness.FinMindClient(fake), now=NOW) == []


def test_monthly_revenue_probe_maps_deadline_to_finmind_month():
    probe = next(p for p in witness.PROBES if p.dataset == "monthly_revenue")
    import pandas as pd
    # Our index is the Statutory Deadline (Aug 10 for July revenue); FinMind
    # dates July revenue 2026-08-01, in 元 — the probe must bridge both.
    assert probe.finmind_date(pd.Timestamp("2026-08-10")) == "2026-08-01"
    assert probe.scale == 1 / 1000
