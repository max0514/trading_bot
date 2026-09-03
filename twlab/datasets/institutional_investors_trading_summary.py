"""`institutional_investors_trading_summary` Dataset: 三大法人 daily share counts per security.

Official Sources: TWSE T86 (三大法人買賣超日報, 上市) and TPEx insti/dailyTrade
(上櫃), merged into one long-form batch with FinLab's field names — the five
investor groups' 買進 / 賣出 / 買賣超 share counts, in 股. Columns are located
BY NAME in each source table, so a renamed or vanished column raises
ParseError instead of silently shifting values. T86 also publishes two totals
(自營商買賣超股數, 三大法人買賣超股數) that are not Catalog Fields; they are
deliberately left out.

A dataset-specific Invariant checks 買賣超 == 買進 − 賣出 for every investor
group on every row — exactly what a misaligned source column breaks.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

import pandas as pd

from twlab import qa
from twlab.errors import ParseError
from twlab.http import PoliteSession
from twlab.spec import Cadence, DatasetSpec

TWSE_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade"

# Investor group -> its (買進, 賣出, 買賣超) Fields, named exactly as the Catalog does.
GROUPS: dict[str, tuple[str, str, str]] = {
    "外陸資(不含外資自營商)": (
        "外陸資買進股數(不含外資自營商)", "外陸資賣出股數(不含外資自營商)", "外陸資買賣超股數(不含外資自營商)",
    ),
    "外資自營商": ("外資自營商買進股數", "外資自營商賣出股數", "外資自營商買賣超股數"),
    "投信": ("投信買進股數", "投信賣出股數", "投信買賣超股數"),
    "自營商(自行買賣)": ("自營商買進股數(自行買賣)", "自營商賣出股數(自行買賣)", "自營商買賣超股數(自行買賣)"),
    "自營商(避險)": ("自營商買進股數(避險)", "自營商賣出股數(避險)", "自營商買賣超股數(避險)"),
}
# The 15 value Fields of the Dataset, in Catalog order.
FIELDS = [field for triple in GROUPS.values() for field in triple]
INT_FIELDS = set(FIELDS)
# 買進 / 賣出 are counts; 買賣超 is signed and may legitimately be negative.
BUY_SELL_FIELDS = [field for buy, sell, _net in GROUPS.values() for field in (buy, sell)]


@dataclass(frozen=True)
class SourceTable:
    """How one Official Source's 三大法人 table maps onto the Dataset's Fields."""
    market: str                    # value for the batch's market column
    id_column: str                 # source column holding the Stock ID
    column_map: dict[str, str]     # source column name -> Field name


_TWSE_TABLE = SourceTable(
    market="TWSE",
    id_column="證券代號",
    column_map={f: f for f in FIELDS},  # T86 already uses FinLab's names
)

_TPEX_TABLE = SourceTable(
    market="TPEx",
    id_column="代號",
    column_map={
        "外資及陸資(不含外資自營商)-買進股數": "外陸資買進股數(不含外資自營商)",
        "外資及陸資(不含外資自營商)-賣出股數": "外陸資賣出股數(不含外資自營商)",
        "外資及陸資(不含外資自營商)-買賣超股數": "外陸資買賣超股數(不含外資自營商)",
        "外資自營商-買進股數": "外資自營商買進股數",
        "外資自營商-賣出股數": "外資自營商賣出股數",
        "外資自營商-買賣超股數": "外資自營商買賣超股數",
        "投信-買進股數": "投信買進股數",
        "投信-賣出股數": "投信賣出股數",
        "投信-買賣超股數": "投信買賣超股數",
        "自營商(自行買賣)-買進股數": "自營商買進股數(自行買賣)",
        "自營商(自行買賣)-賣出股數": "自營商賣出股數(自行買賣)",
        "自營商(自行買賣)-買賣超股數": "自營商買賣超股數(自行買賣)",
        "自營商(避險)-買進股數": "自營商買進股數(避險)",
        "自營商(避險)-賣出股數": "自營商賣出股數(避險)",
        "自營商(避險)-買賣超股數": "自營商買賣超股數(避險)",
    },
)


def fetch(session: PoliteSession, day: dt.date) -> list[dict[str, Any]]:
    """Fetch both markets' 三大法人 daily tables; returns raw payloads for parse()."""
    twse = session.get_json(
        TWSE_URL,
        params={
            "date": day.strftime("%Y%m%d"),
            "selectType": "ALLBUT0999",
            "response": "json",
        },
    )
    tpex = session.get_json(
        TPEX_URL,
        params={
            "type": "Daily",
            "sect": "EW",
            "date": day.strftime("%Y/%m/%d"),
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


_NA_TEXTS = {"", "-", "--", "---", "----", "N/A"}


def _parse_int(value: Any) -> int | None:
    """Share counts are comma-grouped and signed (買賣超); dashes mean N/A.

    Anything else — including a fractional count — is source drift, not data.
    """
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in _NA_TEXTS:
        return None
    try:
        return int(text)
    except ValueError as exc:
        raise ParseError(f"unparseable share count {value!r}") from exc


def _parse_date(text: str, source: str) -> pd.Timestamp:
    try:
        return pd.Timestamp(dt.datetime.strptime(text.replace("/", ""), "%Y%m%d").date())
    except ValueError as exc:
        raise ParseError(f"{source}: unparseable payload date {text!r}") from exc


def _rows_from_table(
    fields: list[str], data: list[list[Any]], spec: SourceTable, date: pd.Timestamp
) -> pd.DataFrame:
    fields = [str(f).strip() for f in fields]
    missing = [c for c in (spec.id_column, *spec.column_map) if c not in fields]
    if missing:
        raise ParseError(
            f"{spec.market}: expected columns missing from 三大法人 table: {missing} "
            f"(got {fields}) — source format changed?"
        )
    index_of = {
        name: fields.index(name) for name in (spec.id_column, *spec.column_map)
    }
    records = []
    for row in data:
        if len(row) < len(fields):
            raise ParseError(
                f"{spec.market}: row has {len(row)} cells under a {len(fields)}-column "
                f"header: {row[:2]} — source format changed?"
            )
        record: dict[str, Any] = {
            "stock_id": str(row[index_of[spec.id_column]]).strip(),
            "date": date,
            "market": spec.market,
        }
        for src_name, field in spec.column_map.items():
            record[field] = _parse_int(row[index_of[src_name]])
        records.append(record)
    return pd.DataFrame(records, columns=["stock_id", "date", "market", *FIELDS])


def _parse_twse(payload: dict) -> pd.DataFrame:
    """T86 is a single rwd table: stat / date / fields / data at the top level."""
    stat = str(payload.get("stat", ""))
    if stat != "OK":
        # TWSE answers a polite "no data" stat on non-trading days.
        if "沒有符合條件" in stat or "查無資料" in stat:
            return _empty_batch()
        raise ParseError(f"TWSE: unexpected stat {stat!r}")
    date = _parse_date(str(payload.get("date", "")), "TWSE")
    return _rows_from_table(
        payload.get("fields") or [], payload.get("data") or [], _TWSE_TABLE, date
    )


def _find_tpex_table(payload: dict) -> dict:
    for table in payload.get("tables", []):
        fields = [str(f).strip() for f in (table.get("fields") or [])]
        if _TPEX_TABLE.id_column in fields:
            return {**table, "fields": fields}
    raise ParseError(
        f"TPEx: no table with a {_TPEX_TABLE.id_column!r} column — source format "
        f"changed? tables: {[str(t.get('title'))[:30] for t in payload.get('tables', [])]}"
    )


def _parse_tpex(payload: dict) -> pd.DataFrame:
    stat = str(payload.get("stat", ""))
    if stat.lower() != "ok":
        raise ParseError(f"TPEx: unexpected stat {stat!r}")
    table = _find_tpex_table(payload)
    if not table.get("data"):
        return _empty_batch()  # non-trading day
    date = _parse_date(str(payload.get("date") or table.get("date", "")), "TPEx")
    return _rows_from_table(table["fields"], table["data"], _TPEX_TABLE, date)


def net_equals_buy_minus_sell() -> qa.Invariant:
    """買賣超 must equal 買進 − 賣出 for every investor group on every row.

    Every Field is located by name, so a renamed column fails in the parser;
    this catches the quieter failure where the names survive but the data
    underneath them is shifted or reordered.
    """

    def check(batch: pd.DataFrame) -> str | None:
        for buy, sell, net in GROUPS.values():
            if not {buy, sell, net} <= set(batch.columns):
                return None  # required_columns reports absence
            complete = batch[[buy, sell, net]].dropna()
            bad = complete[net] != complete[buy] - complete[sell]
            if bad.any():
                sample = batch.loc[bad[bad].index[:3], ["stock_id", buy, sell, net]]
                return (
                    f"{int(bad.sum())} rows where {net} != {buy} - {sell}, "
                    f"e.g. {sample.to_dict('records')}"
                )
        return None

    return qa.Invariant("net_equals_buy_minus_sell", check)


SPECS = [
    DatasetSpec(
        name="institutional_investors_trading_summary",
        official_source="TWSE T86 + TPEx insti/dailyTrade",
        # Both exchanges publish 三大法人 by ~17:00; FinLab serves it in the same
        # 21:32 nightly window as price.
        cadence=Cadence(kind="daily", at="21:32"),
        frequency="daily",
        fields=tuple(FIELDS),
        int_fields=frozenset(INT_FIELDS),
        key_fields=("stock_id", "date"),
        invariants=(
            qa.required_columns(["stock_id", "date", "market", *FIELDS]),
            qa.unique_key(["stock_id", "date"]),
            # ~1,100+ 上市 and ~700 上櫃 securities see institutional flow on a
            # normal day; far fewer means a truncated scrape or a missing market.
            qa.min_rows(1000),
            qa.non_negative(BUY_SELL_FIELDS),
            net_equals_buy_minus_sell(),
        ),
        backfill_start=dt.date(2012, 5, 2),  # Catalog history start
        fetch=fetch,
        parse=parse,
    ),
]
