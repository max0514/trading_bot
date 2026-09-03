"""`price_earning_ratio` Dataset: daily valuation ratios for every listed/OTC stock.

Official Sources: TWSE BWIBBU_d (上市 個股日本益比、殖利率及股價淨值比) and TPEx
peQryDate (上櫃), merged into one long-form batch with FinLab's field names.
Both sources already name the three ratios the way FinLab does; columns are
still located BY NAME in each table, so a renamed or vanished column raises
ParseError instead of silently shifting values. BWIBBU_d is a flat fields/data
payload keyed by 證券代號; peQryDate wraps an untitled table (never locate it by
title) in tables[] keyed by 股票代號, pads 公司名稱 with trailing spaces and
serves 股利年度 as an int. A loss-maker's 本益比 prints as "-" on TWSE and "N/A"
on TPEx; both become missing.
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

TWSE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate"

# The 3 value Fields of the Dataset, named exactly as the Catalog does.
FIELDS = ["殖利率(%)", "本益比", "股價淨值比"]
COLUMNS = ["stock_id", "date", "market", *FIELDS]
MARKETS = ("TWSE", "TPEx")

_MISSING = {"", "-", "--", "---", "N/A", "n/a"}


@dataclass(frozen=True)
class SourceTable:
    """How one Official Source's ratios table maps onto the Dataset's Fields."""
    market: str                    # value for the batch's market column
    id_column: str                 # source column holding the Stock ID
    column_map: dict[str, str]     # source column name -> Field name


_TWSE_TABLE = SourceTable(
    market="TWSE", id_column="證券代號", column_map={f: f for f in FIELDS},
)
_TPEX_TABLE = SourceTable(
    market="TPEx", id_column="股票代號", column_map={f: f for f in FIELDS},
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


def _empty_batch() -> pd.DataFrame:
    """Typed like a parsed batch, so concatenating a holiday's empty half onto
    the other market's rows neither warns nor widens dtypes."""
    return pd.DataFrame({
        "stock_id": pd.Series(dtype="object"),
        "date": pd.Series(dtype="datetime64[ns]"),
        "market": pd.Series(dtype="object"),
        **{f: pd.Series(dtype="float64") for f in FIELDS},
    })


def _parse_number(value: Any) -> float | None:
    """Source numbers are strings with comma grouping; '-' / 'N/A' mean missing."""
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in _MISSING:
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise ParseError(f"unparseable number {value!r}") from exc


def _find_table(payload: dict, spec: SourceTable) -> dict:
    """BWIBBU_d is a flat fields/data payload; TPEx wraps its table in tables[]."""
    for table in (payload, *(payload.get("tables") or [])):
        fields = [str(f).strip() for f in (table.get("fields") or [])]
        if spec.id_column in fields:
            return {**table, "fields": fields}
    raise ParseError(
        f"{spec.market}: no table with a {spec.id_column!r} column — "
        f"source format changed? fields: {payload.get('fields')!r}, tables: "
        f"{[str(t.get('title'))[:30] for t in payload.get('tables') or []]}"
    )


def _parse_date(text: str, source: str) -> pd.Timestamp:
    """'20260807' / '2026/08/07' (payload date) or '115/08/07' (ROC, TPEx table date)."""
    parts = text.strip().split("/")
    try:
        if len(parts) == 3 and len(parts[0]) <= 3:
            return pd.Timestamp(dt.date(int(parts[0]) + 1911, int(parts[1]), int(parts[2])))
        return pd.Timestamp(dt.datetime.strptime(text.replace("/", ""), "%Y%m%d").date())
    except ValueError as exc:
        raise ParseError(f"{source}: unparseable payload date {text!r}") from exc


def _rows_from_table(table: dict, spec: SourceTable, date: pd.Timestamp) -> pd.DataFrame:
    fields = table["fields"]
    missing = [c for c in (spec.id_column, *spec.column_map) if c not in fields]
    if missing:
        raise ParseError(
            f"{spec.market}: expected columns missing from ratios table: {missing} "
            f"(got {fields}) — source format changed?"
        )
    index_of = {name: fields.index(name) for name in (spec.id_column, *spec.column_map)}
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
    return pd.DataFrame(records, columns=COLUMNS)


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
