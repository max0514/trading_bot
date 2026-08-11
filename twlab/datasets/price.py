"""`price` Dataset: daily quotes for every listed/OTC security.

Official Sources: TWSE MI_INDEX (上市) and TPEx daily quotes (上櫃), merged into
one long-form batch with FinLab's field names. Columns are located BY NAME in
each source table, so a renamed or vanished column raises ParseError instead of
silently shifting values.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

import pandas as pd

from twlab.errors import ParseError
from twlab.http import PoliteSession

TWSE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"

# The 11 value Fields of the `price` Dataset, named exactly as the Catalog does.
FIELDS = [
    "成交股數", "成交筆數", "成交金額",
    "開盤價", "最高價", "最低價", "收盤價",
    "最後揭示買價", "最後揭示買量", "最後揭示賣價", "最後揭示賣量",
]
INT_FIELDS = {"成交股數", "成交筆數", "成交金額"}

@dataclass(frozen=True)
class SourceTable:
    """How one Official Source's quotes table maps onto the Dataset's Fields."""
    market: str                    # value for the batch's market column
    id_column: str                 # source column holding the Stock ID
    column_map: dict[str, str]     # source column name -> Field name


_TWSE_TABLE = SourceTable(
    market="TWSE",
    id_column="證券代號",
    column_map={f: f for f in FIELDS},  # TWSE already matches FinLab's names
)

# TPEx quote sizes are already in 千股, same unit as TWSE's 揭示量 — no scaling.
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
        "最後買量(千股)": "最後揭示買量",
        "最後賣價": "最後揭示賣價",
        "最後賣量(千股)": "最後揭示賣量",
    },
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


def _empty_batch() -> pd.DataFrame:
    return pd.DataFrame(columns=["stock_id", "date", "market", *FIELDS])


def _parse_number(value: Any) -> float | None:
    """TWSE/TPEx numbers are strings with comma grouping; '--'-style means N/A."""
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in ("", "--", "---", "----", "-----", "N/A"):
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise ParseError(f"unparseable number {value!r}") from exc


def _find_table(payload: dict, spec: SourceTable) -> dict:
    for table in payload.get("tables", []):
        fields = [str(f).strip() for f in (table.get("fields") or [])]
        if spec.id_column in fields:
            return {**table, "fields": fields}
    raise ParseError(
        f"{spec.market}: no table with an {spec.id_column!r} column — "
        f"source format changed? tables: "
        f"{[str(t.get('title'))[:30] for t in payload.get('tables', [])]}"
    )


def _parse_date(text: str, source: str) -> pd.Timestamp:
    try:
        return pd.Timestamp(dt.datetime.strptime(text.replace("/", ""), "%Y%m%d").date())
    except ValueError as exc:
        raise ParseError(f"{source}: unparseable payload date {text!r}") from exc


def _rows_from_table(
    table: dict, spec: SourceTable, date: pd.Timestamp
) -> pd.DataFrame:
    fields = table["fields"]
    missing = [c for c in (spec.id_column, *spec.column_map) if c not in fields]
    if missing:
        raise ParseError(
            f"{spec.market}: expected columns missing from quotes table: {missing} "
            f"(got {fields}) — source format changed?"
        )
    index_of = {
        name: fields.index(name) for name in (spec.id_column, *spec.column_map)
    }
    records = []
    for row in table.get("data") or []:
        record: dict[str, Any] = {
            "stock_id": str(row[index_of[spec.id_column]]).strip(),
            "date": date,
            "market": spec.market,
        }
        for src_name, field in spec.column_map.items():
            record[field] = _parse_number(row[index_of[src_name]])
        records.append(record)
    return pd.DataFrame(records, columns=["stock_id", "date", "market", *FIELDS])


def _parse_twse(payload: dict) -> pd.DataFrame:
    stat = str(payload.get("stat", ""))
    if stat != "OK":
        # TWSE answers a polite "no data" stat on non-trading days.
        if "沒有符合條件" in stat or "查無資料" in stat:
            return _empty_batch()
        raise ParseError(f"TWSE: unexpected stat {stat!r}")
    date = _parse_date(str(payload.get("date", "")), "TWSE")
    return _rows_from_table(_find_table(payload, _TWSE_TABLE), _TWSE_TABLE, date)


def _parse_tpex(payload: dict) -> pd.DataFrame:
    stat = str(payload.get("stat", ""))
    if stat.lower() != "ok":
        raise ParseError(f"TPEx: unexpected stat {stat!r}")
    table = _find_table(payload, _TPEX_TABLE)
    if not table.get("data"):
        return _empty_batch()  # non-trading day
    date = _parse_date(str(payload.get("date") or table.get("date", "")), "TPEx")
    return _rows_from_table(table, _TPEX_TABLE, date)
