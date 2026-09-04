"""Seam 2 — the price pipeline with HTTP faked at the boundary.

fetch → parse → upsert → QA gate → materialize, asserted through the pipeline's
observable outcomes (run status, Mongo row counts, what data.get() serves) —
never through Mongo document layouts or Parquet file internals.
"""
import datetime as dt

import pandas as pd

from twlab import data, pipeline
from twlab.store.parquet import ParquetStore

from conftest import DAY, NOW, FakeSession, load_fixture


def run_price(session, mongo, store):
    return pipeline.run(
        "price", DAY, session=session, mongo=mongo, store=store, now=NOW
    )


def test_full_run_materializes_wide_frames(good_session, mongo, store_env):
    result = run_price(good_session, mongo, ParquetStore(store_env))

    assert result.status == "ok"
    assert result.rows == 1377 + 1012  # TWSE + TPEx securities in the fixtures
    close = data.get("price:收盤價")
    assert close.loc["2026-08-07", "2330"] == 2370.0   # TWSE row
    assert close.loc["2026-08-07", "5483"] == 168.5    # TPEx row


def test_rerun_same_day_is_idempotent(good_session, mongo, store):
    first = run_price(good_session, mongo, store)
    count_after_first = mongo.collection("price").count_documents({})

    second = run_price(good_session, mongo, store)
    count_after_second = mongo.collection("price").count_documents({})

    assert first.status == second.status == "ok"
    assert count_after_first == count_after_second == first.rows


def test_invariant_failure_blocks_materialization(good_session, mongo, store_env, tpex_payload):
    api_store = ParquetStore(store_env)
    good = run_price(good_session, mongo, api_store)
    assert good.status == "ok"

    # Next night the TWSE response comes back truncated: parses cleanly,
    # but the row-count Invariant must Quarantine it.
    tiny = FakeSession({
        "twse.com.tw": load_fixture("twse_mi_index_20260807_tiny.json"),
        "tpex.org.tw": tpex_payload,
    })
    bad = pipeline.run(
        "price", DAY + dt.timedelta(days=1),
        session=tiny, mongo=mongo, store=api_store,
        now=NOW + dt.timedelta(days=1),
    )

    assert bad.status == "quarantined"
    assert any("min_rows" in f for f in bad.failures)
    # The API still serves the last good version, untouched: same dates,
    # same values, and nothing from the bad batch.
    close = data.get("price:收盤價")
    assert list(close.index.astype(str)) == ["2026-08-07"]
    assert close.loc["2026-08-07", "2330"] == 2370.0
    assert "9998" not in close.columns  # the bad batch's poison Stock ID


def test_quarantined_rows_never_leak_into_later_materializations(
    good_session, mongo, store_env, tpex_payload
):
    """A Quarantined batch's rows must not ride along when a LATER good run
    re-materializes the Dataset from the system of record."""
    api_store = ParquetStore(store_env)

    tiny = FakeSession({
        "twse.com.tw": load_fixture("twse_mi_index_20260807_tiny.json"),
        "tpex.org.tw": tpex_payload,
    })
    bad = run_price(tiny, mongo, api_store)
    assert bad.status == "quarantined"

    good = run_price(good_session, mongo, api_store)
    assert good.status == "ok"

    close = data.get("price:收盤價")
    assert "9998" not in close.columns       # quarantined-only row stays out
    assert close.loc["2026-08-07", "2330"] == 2370.0
    # A key the bad batch had flagged is trusted again once re-scraped.
    assert pd.notna(close.loc["2026-08-07", "00401A"])


def test_bad_rescrape_of_a_published_day_never_overwrites_it(good_session, mongo, store_env, tpex_payload):
    """QA gates the upsert: a truncated re-scrape of a day already published
    leaves the system of record untouched, keeps the evidence aside, and the
    next good day's materialization still carries the original rows."""
    api_store = ParquetStore(store_env)
    assert run_price(good_session, mongo, api_store).status == "ok"

    truncated = FakeSession({
        "twse.com.tw": load_fixture("twse_mi_index_20260807_tiny.json"),
        "tpex.org.tw": tpex_payload,
    })
    bad = run_price(truncated, mongo, api_store)             # same day, bad source
    assert bad.status == "quarantined"

    next_day = load_fixture("twse_mi_index_20260807.json")
    next_day = {**next_day, "date": "20260810"}
    good_later = pipeline.run(
        "price", DAY + dt.timedelta(days=3),
        session=FakeSession({"twse.com.tw": next_day, "tpex.org.tw": tpex_payload}),
        mongo=mongo, store=api_store, now=NOW + dt.timedelta(days=3),
    )
    assert good_later.status == "ok"

    close = data.get("price:收盤價")
    assert close.loc["2026-08-07", "2330"] == 2370.0           # the published day survived intact
    assert close.loc["2026-08-07"].notna().sum() >= 1377        # every TWSE row still there
    assert "9998" not in close.columns
    evidence = mongo.quarantined_rows("price")
    assert "9998" in set(evidence["stock_id"])                  # the bad batch is kept for inspection


def test_parse_failure_quarantines_run(mongo, store, tpex_payload):
    malformed = FakeSession({
        "twse.com.tw": load_fixture("twse_mi_index_20260807_malformed.json"),
        "tpex.org.tw": tpex_payload,
    })
    result = run_price(malformed, mongo, store)

    assert result.status == "quarantined"
    assert any("parse" in f for f in result.failures)
    assert mongo.collection("price").count_documents({}) == 0
    assert not store.manifest_path("price").exists()


def test_run_outcomes_are_logged(good_session, mongo, store):
    run_price(good_session, mongo, store)
    runs = list(mongo.collection("runs").find({}))
    assert len(runs) == 1
    assert runs[0]["dataset"] == "price"
    assert runs[0]["status"] == "ok"
    assert runs[0]["rows"] == 1377 + 1012
    assert runs[0]["at"] == NOW
