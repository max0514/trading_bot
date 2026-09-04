"""`price_earning_ratio` Dataset: daily valuation ratios for every listed/OTC stock.

Official Sources: TWSE BWIBBU_d (上市 個股日本益比、殖利率及股價淨值比) and TPEx
peQryDate (上櫃), merged into one long-form batch with FinLab's field names.
Both sources already name the three ratios the way FinLab does; the envelope,
the number and date spellings, and the column-by-name location come from
`twlab.sources`. BWIBBU_d is a flat fields/data payload keyed by 證券代號;
peQryDate wraps an untitled table (never locate it by title) in tables[] keyed
by 股票代號, pads 公司名稱 with trailing spaces and serves 股利年度 as an int —
`sources.find_table` searches both shapes. A loss-maker's 本益比 prints as "-"
on TWSE and "N/A" on TPEx; both become missing.
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

TWSE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate"

# The 3 value Fields of the Dataset, named exactly as the Catalog does.
FIELDS = ["殖利率(%)", "本益比", "股價淨值比"]
COLUMNS = sources.batch_columns(FIELDS)
MARKETS = ("TWSE", "TPEx")

_TWSE_TABLE = SourceTable(
    market="TWSE", id_column="證券代號", column_map={f: f for f in FIELDS},
    what="ratios table",
)
_TPEX_TABLE = SourceTable(
    market="TPEx", id_column="股票代號", column_map={f: f for f in FIELDS},
    what="ratios table",
)


def fetch(session: PoliteSession, day: dt.date) -> list[dict[str, Any]]:
    """Fetch both markets' valuation ratios; returns raw payloads for parse()."""
    twse = session.get_json(
        TWSE_URL,
        params={"date": day.strftime("%Y%m%d"), "selectType": "ALL", "response": "json"},
    )
    tpex = session.get_json(
        TPEX_URL,
        params={"date": day.strftime("%Y/%m/%d"), "response": "json"},
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


# ── Dataset-specific Invariants ────────────────────────────────────────────

def _both_markets_present(batch: pd.DataFrame) -> str | None:
    """A trading-day batch must carry both exchanges: the 上市 half alone clears
    the row floor, so a vanished TPEx table would otherwise publish silently."""
    if "market" not in batch.columns:
        return None  # required_columns reports absence
    missing = [m for m in MARKETS if m not in set(batch["market"])]
    return f"no rows from {missing}" if missing else None


BOTH_MARKETS = qa.Invariant("both_markets", _both_markets_present)


SPECS = [
    DatasetSpec(
        name="price_earning_ratio",
        official_source="TWSE BWIBBU_d + TPEx peQryDate",
        # Both after-market files are final well before FinLab's 21:32 price publish.
        cadence=Cadence(kind="daily", at="21:32"),
        frequency="daily",
        fields=tuple(FIELDS),
        int_fields=frozenset(),
        key_fields=("stock_id", "date"),
        invariants=(
            qa.required_columns(COLUMNS),
            qa.unique_key(["stock_id", "date"]),
            # 1,082 上市 + 888 上櫃 rows on 2026-08-07; the 上市 half alone
            # clears this floor, which is what BOTH_MARKETS is for.
            qa.min_rows(1000),
            qa.non_negative(FIELDS),
            BOTH_MARKETS,
        ),
        backfill_start=dt.date(2010, 1, 4),  # Catalog history start for the Dataset
        fetch=fetch,
        parse=parse,
    ),
]
