"""`institutional_investors_trading_summary` Dataset: 三大法人 daily share counts per security.

Official Sources: TWSE T86 (三大法人買賣超日報, 上市) and TPEx insti/dailyTrade
(上櫃), merged into one long-form batch with FinLab's field names — the five
investor groups' 買進 / 賣出 / 買賣超 share counts, in 股.

T86 names every column (FinLab copied its names), so Fields are located BY
NAME through `twlab.sources` — which also owns the envelope, the placeholder
set and the date spellings — and a renamed or vanished column raises
ParseError. T86 also publishes
two totals (自營商買賣超股數, 三大法人買賣超股數) that are not Catalog Fields;
they are left out. TPEx publishes a flat header — 代號, 名稱, then seven
unnamed (買進股數, 賣出股數, 買賣超股數) triples and a 合計 — where only the
POSITION says which investor group a triple belongs to; the parser pins the
header to that exact pattern and checks the page's own arithmetic on every
row (subtotal triples and the 合計), so a reordered column fails loudly.

A dataset-specific Invariant checks 買賣超 == 買進 − 賣出 for every investor
group on every row — exactly what a misaligned source column breaks.
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


_TWSE_TABLE = SourceTable(
    market="TWSE",
    id_column="證券代號",
    column_map={f: f for f in FIELDS},  # T86 already uses FinLab's names
    parse_value=sources.parse_int,
    what="三大法人 table",
)

# TPEx's flat header: 代號, 名稱, seven (買進股數, 賣出股數, 買賣超股數) triples, 合計.
TPEX_ID_COLUMN = "代號"
TPEX_NAME_COLUMN = "名稱"
TPEX_TRIPLE = ("買進股數", "賣出股數", "買賣超股數")
TPEX_TOTAL_COLUMN = "三大法人買賣超股數合計"
# Investor group of each triple, by position. Verified by the identities the
# page obeys: triple 3 = 1 + 2, triple 7 = 5 + 6, 合計 = nets of 3 + 4 + 7.
TPEX_GROUP_ORDER = (
    "外陸資(不含外資自營商)", "外資自營商", "外資及陸資合計",
    "投信", "自營商(自行買賣)", "自營商(避險)", "自營商合計",
)
TPEX_HEADER = [TPEX_ID_COLUMN, TPEX_NAME_COLUMN, *(TPEX_TRIPLE * len(TPEX_GROUP_ORDER)), TPEX_TOTAL_COLUMN]
# Only used to LOCATE the table: TPEx repeats 買進股數 seven times, so the
# Fields are read by position below rather than through `column_map`.
_TPEX_TABLE = SourceTable(
    market="TPEx", id_column=TPEX_ID_COLUMN, column_map={},
    parse_value=sources.parse_int, what="三大法人 table",
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


def _parse_twse(payload: dict) -> pd.DataFrame:
    """T86 is a single rwd table: stat / date / fields / data at the top level."""
    stat = str(payload.get("stat", ""))
    if stat != "OK":
        if sources.is_no_data(stat):        # a non-trading day, politely put
            return sources.empty_batch(FIELDS)
        raise ParseError(f"TWSE: unexpected stat {stat!r}")
    date = sources.parse_date(payload.get("date", ""), "TWSE")
    table = sources.find_table(payload, _TWSE_TABLE)
    return sources.rows_from_table(table, _TWSE_TABLE, date)


def _check_tpex_arithmetic(stock_id: str, triples: list[tuple], total: int | None) -> None:
    """The page's own identities, checked per row: any column reorder breaks them."""
    if total is None or any(v is None for triple in triples for v in triple):
        return
    foreign, foreign_dealer, foreign_all, _trust, dealer_own, dealer_hedge, dealer_all = triples
    consistent = (
        all(buy - sell == net for buy, sell, net in triples)
        and tuple(a + b for a, b in zip(foreign, foreign_dealer)) == foreign_all
        and tuple(a + b for a, b in zip(dealer_own, dealer_hedge)) == dealer_all
        and total == foreign_all[2] + _trust[2] + dealer_all[2]
    )
    if not consistent:
        raise ParseError(
            f"TPEx: {stock_id}: 三大法人 columns do not add up (triples {triples}, "
            f"合計 {total}) — column order changed?"
        )


def _parse_tpex(payload: dict) -> pd.DataFrame:
    stat = str(payload.get("stat", ""))
    if stat.lower() != "ok":
        raise ParseError(f"TPEx: unexpected stat {stat!r}")
    table = sources.find_table(payload, _TPEX_TABLE)
    if table["fields"] != TPEX_HEADER:
        raise ParseError(
            f"TPEx: header is not {TPEX_ID_COLUMN}/{TPEX_NAME_COLUMN} + "
            f"{len(TPEX_GROUP_ORDER)} × {TPEX_TRIPLE} + {TPEX_TOTAL_COLUMN}: got "
            f"{table['fields']} — source format changed?"
        )
    if not table.get("data"):
        return sources.empty_batch(FIELDS)  # non-trading day
    date = sources.parse_date(table.get("date") or payload.get("date", ""), "TPEx")
    first_value = len((TPEX_ID_COLUMN, TPEX_NAME_COLUMN))
    records = []
    for row in table["data"]:
        if len(row) < len(TPEX_HEADER):
            raise ParseError(
                f"TPEx: row has {len(row)} cells under a {len(TPEX_HEADER)}-column "
                f"header: {row[:2]} — source format changed?"
            )
        stock_id = str(row[0]).strip()
        triples = [
            tuple(sources.parse_int(row[first_value + 3 * k + j]) for j in range(3))
            for k in range(len(TPEX_GROUP_ORDER))
        ]
        total = sources.parse_int(row[first_value + 3 * len(TPEX_GROUP_ORDER)])
        _check_tpex_arithmetic(stock_id, triples, total)
        record: dict[str, Any] = {"stock_id": stock_id, "date": date, "market": "TPEx"}
        for group, triple in zip(TPEX_GROUP_ORDER, triples):
            if group in GROUPS:                     # subtotal triples are not Fields
                record.update(zip(GROUPS[group], triple))
        records.append(record)
    return pd.DataFrame(records, columns=sources.batch_columns(FIELDS))


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
            qa.required_columns(sources.batch_columns(FIELDS)),
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
