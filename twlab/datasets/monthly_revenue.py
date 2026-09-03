"""`monthly_revenue` Dataset: every listed/OTC company's monthly revenue filing.

Official Source: MOPS monthly revenue summaries (公開資訊觀測站 t21sc03), one
HTML page per market per month — `nas/t21/sii/…` for 上市 and `nas/t21/otc/…`
for 上櫃. Values are in 千元, exactly as MOPS publishes and FinLab serves them.

Point-in-Time Alignment: rows are stored under their revenue-month period
(first day of the month); materialization re-dates them to the Statutory
Deadline — the 10th of the following month — so a backtest can never see May
revenue before June 10. The batch `day` handed to fetch() is that deadline.

Archive frontier: the IFRS-era `_0` pages exist from 2013; earlier months use
a legacy layout and are a backfill sub-task (see the Phase 1 epic).
"""
from __future__ import annotations

import datetime as dt
import io
import re
from typing import Any

import pandas as pd

from twlab import qa
from twlab.errors import ParseError
from twlab.http import PoliteSession
from twlab.spec import Cadence, DatasetSpec, align_monthly, previous_month

MOPS_URL = "https://mops.twse.com.tw/nas/t21/{market}/t21sc03_{roc_year}_{month}_0.html"

# The 8 value Fields of the Dataset, named exactly as the Catalog does.
FIELDS = [
    "當月營收", "上月營收", "去年當月營收",
    "上月比較增減(%)", "去年同月增減(%)",
    "當月累計營收", "去年累計營收", "前期比較增減(%)",
]
INT_FIELDS = {"當月營收", "上月營收", "去年當月營收", "當月累計營收", "去年累計營收"}
ID_COLUMN = "公司代號"
NAME_COLUMN = "公司名稱"

MARKETS = {"sii": "TWSE", "otc": "TPEx"}
_CODE_RE = re.compile(r"^\d{4,6}$")


def fetch(session: PoliteSession, day: dt.date) -> list[dict[str, Any]]:
    """Fetch both markets' summaries for the month before `day`."""
    year, month = previous_month(day)
    raws = []
    for market in MARKETS:
        url = MOPS_URL.format(market=market, roc_year=year - 1911, month=month)
        raws.append({
            "source": market,
            "period": f"{year:04d}-{month:02d}",
            "payload": session.get_text(url),
        })
    return raws


def _flatten(columns: pd.Index) -> list[str]:
    """MOPS uses a two-row header (營業收入 / 當月營收 …); keep the leaf names."""
    out = []
    for col in columns:
        leaf = col[-1] if isinstance(col, tuple) else col
        out.append(re.sub(r"\s+", "", str(leaf)))
    return out


def _to_number(series: pd.Series, field: str) -> pd.Series:
    text = series.astype(str).str.replace(",", "", regex=False).str.strip()
    text = text.replace({"": None, "nan": None, "-": None, "--": None})
    try:
        return pd.to_numeric(text)
    except (ValueError, TypeError) as exc:
        raise ParseError(f"unparseable number in {field!r}: {exc}") from exc


def parse(raw: dict[str, Any]) -> pd.DataFrame:
    """parse(raw) → rows: long-form DataFrame with stock_id, date, market + FIELDS.

    The period comes from the fetch (raw["period"]) and is verified against
    the page title so MOPS cannot silently serve a different month.
    """
    source = raw.get("source")
    if source not in MARKETS:
        raise ParseError(f"unknown raw payload source {source!r}")
    period = str(raw.get("period", ""))
    try:
        year, month = (int(p) for p in period.split("-"))
    except ValueError as exc:
        raise ParseError(f"{source}: bad period {period!r}") from exc

    html = raw["payload"]
    expected_title = f"{year - 1911}年{month}月份"
    if expected_title not in html:
        raise ParseError(
            f"{source}: page does not announce {expected_title} — "
            f"MOPS served a different month or changed layout"
        )

    try:
        tables = pd.read_html(io.StringIO(html), thousands=",")
    except ValueError as exc:  # "No tables found"
        raise ParseError(f"{source}: no tables in MOPS page: {exc}") from exc

    required = [ID_COLUMN, NAME_COLUMN, *FIELDS]
    parts = []
    seen_columns: list[list[str]] = []
    for table in tables:
        columns = _flatten(table.columns)
        seen_columns.append(columns)
        if not all(c in columns for c in required):
            continue
        table = table.copy()
        table.columns = columns
        table = table.loc[:, ~table.columns.duplicated()]
        parts.append(table[required])
    if not parts:
        raise ParseError(
            f"{source}: no revenue table with columns {required} — source format "
            f"changed? saw: {[c[:6] for c in seen_columns][:5]}"
        )

    df = pd.concat(parts, ignore_index=True)
    codes = df[ID_COLUMN].astype(str).str.strip()
    df = df[codes.str.match(_CODE_RE)].copy()          # drops 合計 / header echoes
    df["stock_id"] = df[ID_COLUMN].astype(str).str.strip()
    df["date"] = pd.Timestamp(dt.date(year, month, 1))
    df["market"] = MARKETS[source]
    for f in FIELDS:
        df[f] = _to_number(df[f], f)
    return df[["stock_id", "date", "market", *FIELDS]].reset_index(drop=True)


SPECS = [
    DatasetSpec(
        name="monthly_revenue",
        official_source="MOPS t21sc03 monthly revenue summaries (sii + otc)",
        # Filings are due by the 10th; collect that evening, after the last filers.
        cadence=Cadence(kind="monthly", at="22:00"),
        frequency="monthly",
        fields=tuple(FIELDS),
        int_fields=frozenset(INT_FIELDS),
        key_fields=("stock_id", "date"),
        invariants=(
            qa.required_columns(["stock_id", "date", "market", *FIELDS]),
            qa.unique_key(["stock_id", "date"]),
            # ~1,000 上市 + ~800 上櫃 filers a month; far fewer means a market is missing.
            qa.min_rows(1000),
            qa.non_negative(sorted(INT_FIELDS)),
        ),
        backfill_start=dt.date(2013, 1, 1),   # IFRS-era MOPS archive; legacy layout before
        fetch=fetch,
        parse=parse,
        align=align_monthly,
    ),
]
