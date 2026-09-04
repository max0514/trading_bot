"""`benchmark_return` Dataset: TWSE's 發行量加權股價報酬指數 (TAIEX total return).

Official Source: TWSE 報酬指數 report MFI94U (under /rwd/zh/TAIEX/; the
indicesReport path 404s), which answers with the WHOLE month containing the
query date — one row per trading day, ROC-year dates, comma-grouped values,
and a date header spelled 日　期 with a full-width space, so header names go
through `sources.squeeze` before they are located — as do the envelope, the
ROC dates and the number spellings. fetch(session, day) pulls that month's
page and parse emits one row per listed day; upserts keyed on (stock_id, date) keep the daily
re-fetch of a growing month idempotent.

The Wide Frame has a single column: stock_id is the literal Field name, so
`data.get("benchmark_return:發行量加權股價報酬指數")` yields a one-column frame
named like FinLab's. The index is based at 2003-01-02 (民國 92/01/02), which is
also the archive's first month.
"""
from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd

from twlab import qa, sources
from twlab.errors import ParseError
from twlab.http import PoliteSession
from twlab.spec import Cadence, DatasetSpec

TWSE_URL = "https://www.twse.com.tw/rwd/zh/TAIEX/MFI94U"

FIELD = "發行量加權股價報酬指數"   # the Dataset's only Field, named as the Catalog does
FIELDS = [FIELD]
STOCK_ID = FIELD                  # the Wide Frame's single column
DATE_COLUMN = "日期"
MARKET = "TWSE"
COLUMNS = sources.batch_columns(FIELDS)


def fetch(session: PoliteSession, day: dt.date) -> list[dict[str, Any]]:
    """Fetch the report page for the month containing `day`."""
    payload = session.get_json(
        TWSE_URL, params={"date": day.strftime("%Y%m%d"), "response": "json"},
    )
    return [{"source": "twse", "payload": payload}]


def parse(raw: dict[str, Any]) -> pd.DataFrame:
    """parse(raw) → rows: one long-form row per trading day listed on the page.

    Empty DataFrame means TWSE reported no data for the month; malformed
    structure raises ParseError.
    """
    source = raw.get("source")
    if source != "twse":
        raise ParseError(f"unknown raw payload source {source!r}")
    payload = raw["payload"]
    stat = str(payload.get("stat", ""))
    if stat != "OK":
        if sources.is_no_data(stat):        # a month TWSE has no page for
            return sources.empty_batch(FIELDS)
        raise ParseError(f"TWSE: unexpected stat {stat!r}")

    # The date header is spelled 日　期 with a full-width space, so squeeze it.
    fields = [sources.squeeze(f) for f in (payload.get("fields") or [])]
    missing = [c for c in (DATE_COLUMN, FIELD) if c not in fields]
    if missing:
        raise ParseError(
            f"TWSE: expected columns missing from MFI94U table: {missing} "
            f"(got {fields}) — source format changed?"
        )
    date_at, value_at = fields.index(DATE_COLUMN), fields.index(FIELD)
    records = [
        {
            "stock_id": STOCK_ID,
            "date": sources.parse_date(row[date_at], "TWSE"),
            "market": MARKET,
            FIELD: sources.parse_number(row[value_at]),
        }
        for row in payload.get("data") or []
    ]
    return pd.DataFrame(records, columns=COLUMNS)


# ── Dataset-specific Invariants ────────────────────────────────────────────

def _single_series(batch: pd.DataFrame) -> str | None:
    if "stock_id" not in batch.columns:
        return None  # required_columns reports absence
    others = sorted(set(batch["stock_id"]) - {STOCK_ID})
    return f"unexpected stock_id values {others}" if others else None


def _positive_values(batch: pd.DataFrame) -> str | None:
    """A total-return index is strictly positive; 0 means a mis-parsed cell."""
    if FIELD not in batch.columns:
        return None
    bad = batch[FIELD].dropna() <= 0
    return f"{int(bad.sum())} rows with a non-positive {FIELD}" if bad.any() else None


SINGLE_SERIES = qa.Invariant("single_series", _single_series)
POSITIVE_VALUES = qa.Invariant("positive_values", _positive_values)


SPECS = [
    DatasetSpec(
        name="benchmark_return",
        official_source="TWSE MFI94U 發行量加權股價報酬指數 (month page)",
        # The month page carries the day's close by the evening; align with price.
        cadence=Cadence(kind="daily", at="21:32"),
        frequency="daily",
        fields=tuple(FIELDS),
        int_fields=frozenset(),
        key_fields=("stock_id", "date"),
        invariants=(
            qa.required_columns(COLUMNS),
            qa.unique_key(["stock_id", "date"]),
            qa.min_rows(1),   # a month page lists at least the day it was fetched for
            SINGLE_SERIES,
            POSITIVE_VALUES,
        ),
        backfill_start=dt.date(2003, 1, 2),  # index base date; Catalog history start
        fetch=fetch,
        parse=parse,
    ),
]
