"""`price` Dataset: daily quotes for every listed/OTC security.

Official Sources: TWSE MI_INDEX (上市) and TPEx daily quotes (上櫃), merged into
one long-form batch with FinLab's field names. The rwd envelope, the number
and date spellings, and the column-by-name location all come from
`twlab.sources`; only the two column maps below are specific to this Dataset.
"""
from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd

from twlab import qa, sources
from twlab.errors import ParseError
from twlab.http import PoliteSession
from twlab.sources import SourceTable
from twlab.spec import Cadence, DatasetSpec

TWSE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"

# The 11 value Fields of the `price` Dataset, named exactly as the Catalog does.
FIELDS = [
    "成交股數", "成交筆數", "成交金額",
    "開盤價", "最高價", "最低價", "收盤價",
    "最後揭示買價", "最後揭示買量", "最後揭示賣價", "最後揭示賣量",
]
INT_FIELDS = {"成交股數", "成交筆數", "成交金額"}

_TWSE_TABLE = SourceTable(
    market="TWSE",
    id_column="證券代號",
    column_map={f: f for f in FIELDS},  # TWSE already matches FinLab's names
    normalize=sources.squeeze,
    what="quotes table",
)

# TPEx quote sizes are in 張 (the payload says so: "flagField": "張數"), the same
# unit as TWSE's 揭示量 — no scaling. Column names are matched after stripping
# the whitespace and <br> tags TPEx sprinkles into its headers.
_TPEX_TABLE = SourceTable(
    market="TPEx",
    id_column="代號",
    column_map={
        "成交股數": "成交股數",
        "成交筆數": "成交筆數",
        "成交金額(元)": "成交金額",
        "開盤": "開盤價",
        "最高": "最高價",
        "最低": "最低價",
        "收盤": "收盤價",
        "最後買價": "最後揭示買價",
        "最後買量(張數)": "最後揭示買量",
        "最後賣價": "最後揭示賣價",
        "最後賣量(張數)": "最後揭示賣量",
    },
    normalize=sources.squeeze,
    what="quotes table",
)


def fetch(session: PoliteSession, day: dt.date) -> list[dict[str, Any]]:
    """Fetch both markets' daily quotes; returns raw payloads for parse()."""
    twse = session.get_json(
        TWSE_URL,
        params={
            "date": day.strftime("%Y%m%d"),
            "type": "ALLBUT0999",
            "response": "json",
        },
    )
    tpex = session.get_json(
        TPEX_URL,
        params={
            "date": day.strftime("%Y/%m/%d"),
            "type": "EW",
            "response": "json",
        },
    )
    return [
        {"source": "twse", "payload": twse},
        {"source": "tpex", "payload": tpex},
    ]


def parse(raw: dict[str, Any]) -> pd.DataFrame:
    """parse(raw) → rows: long-form DataFrame with stock_id, date, market + FIELDS.

    Empty DataFrame (no rows) means the source reported no data for the day
    (holiday); malformed structure raises ParseError.
    """
    source = raw.get("source")
    if source == "twse":
        return _parse_twse(raw["payload"])
    if source == "tpex":
        return _parse_tpex(raw["payload"])
    raise ParseError(f"unknown raw payload source {source!r}")


def _parse_twse(payload: dict) -> pd.DataFrame:
    stat = str(payload.get("stat", ""))
    if stat != "OK":
        if sources.is_no_data(stat):        # a non-trading day, politely put
            return sources.empty_batch(FIELDS)
        raise ParseError(f"TWSE: unexpected stat {stat!r}")
    date = sources.parse_date(payload.get("date", ""), "TWSE")
    table = sources.find_table(payload, _TWSE_TABLE)
    return sources.rows_from_table(table, _TWSE_TABLE, date)


def _parse_tpex(payload: dict) -> pd.DataFrame:
    stat = str(payload.get("stat", ""))
    if stat.lower() != "ok":
        raise ParseError(f"TPEx: unexpected stat {stat!r}")
    table = sources.find_table(payload, _TPEX_TABLE)
    if not table.get("data"):
        return sources.empty_batch(FIELDS)  # non-trading day
    date = sources.parse_date(payload.get("date") or table.get("date", ""), "TPEx")
    return sources.rows_from_table(table, _TPEX_TABLE, date)


SPECS = [
    DatasetSpec(
        name="price",
        official_source="TWSE MI_INDEX + TPEx dailyQuotes",
        # FinLab publishes price at 21:32 — after both exchanges' final after-market files.
        cadence=Cadence(kind="daily", at="21:32"),
        frequency="daily",
        fields=tuple(FIELDS),
        int_fields=frozenset(INT_FIELDS),
        key_fields=("stock_id", "date"),
        invariants=(
            qa.required_columns(sources.batch_columns(FIELDS)),
            qa.unique_key(["stock_id", "date"]),
            # ~2,400 securities trade across both markets on a normal day (1,377 上市
            # + 1,012 上櫃 in the recordings); below 1,500 means a market's file is
            # missing or truncated.
            qa.min_rows(1500),
            qa.non_negative(FIELDS),
            qa.high_not_below_low(),
        ),
        backfill_start=dt.date(2007, 4, 23),  # Catalog history start for `price`
        fetch=fetch,
        parse=parse,
    ),
]
