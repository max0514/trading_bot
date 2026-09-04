"""`security_categories` Dataset: the static Stock ID → name / category / market table.

Official Source: the TWSE ISIN site (isin.twse.com.tw C_public.jsp) — strMode=2
lists 上市 securities and strMode=4 上櫃. Each is an MS950 HTML page with one
table whose rows are grouped by section-header rows spanning the table
(股票, 上市認購(售)權證, 特別股, 創新板, ETF, ETN, 臺灣存託憑證(TDR), 受益證券-…).
Equities — 股票, 特別股 and 創新板 — carry their 產業別 as the category, ETFs
the constant "ETF", TDRs the constant "存託憑證" (4-digit TDRs trade in the
same universe as stocks); warrants, ETNs and 受益證券 are skipped. Markets
follow FinLab's convention: "sii" for 上市, "otc" for 上櫃. 興櫃 securities are
on neither page and are out of scope — 2,373 rows against FinLab's 3,445; see
docs/catalog-deviations.md.

A static table: no date column, keyed on stock_id, cheap to re-scrape nightly
and upserted idempotently. The batch `day` handed to fetch() is just the run
day — the page has no archive.
"""
from __future__ import annotations

import datetime as dt
import re
from typing import Any

import lxml.etree
import lxml.html
import pandas as pd

from twlab import qa
from twlab.errors import ParseError
from twlab.http import PoliteSession
from twlab.spec import Cadence, DatasetSpec

ISIN_URL = "https://isin.twse.com.tw/isin/C_public.jsp"
ENCODING = "cp950"   # the ISIN site serves Big5/MS950 without declaring it usefully

# raw source -> the page to fetch and the 市場別 it must announce on every row.
PAGES = {
    "sii": {"strMode": "2", "market_label": "上市"},
    "otc": {"strMode": "4", "market_label": "上櫃"},
}
MARKET_OF = {"上市": "sii", "上櫃": "otc"}   # FinLab's market codes

FIELDS = ["name", "category", "market"]
ID_NAME_COLUMN = "有價證券代號及名稱"
MARKET_COLUMN = "市場別"
INDUSTRY_COLUMN = "產業別"

# Section header -> how its rows are categorized. INDUSTRY means "the row's
# 產業別 cell"; a section absent here (warrants, 受益證券, ETN, …) is skipped.
INDUSTRY = None
SECTION_CATEGORY: dict[str, str | None] = {
    "股票": INDUSTRY,
    "特別股": INDUSTRY,
    "創新板": INDUSTRY,
    "ETF": "ETF",
    "臺灣存託憑證(TDR)": "存託憑證",
}
_STOCK_ID_RE = re.compile(r"^[0-9A-Z]{4,6}$")


def fetch(session: PoliteSession, day: dt.date) -> list[dict[str, Any]]:
    """Fetch both markets' ISIN listings. `day` is the run day; the pages are undated."""
    return [
        {
            "source": source,
            "payload": session.get_text(
                ISIN_URL, params={"strMode": page["strMode"]}, encoding=ENCODING
            ),
        }
        for source, page in PAGES.items()
    ]


def _split_id_name(text: str, source: str) -> tuple[str, str]:
    """'2330　台積電' → ('2330', '台積電'); the separator is a full-width space."""
    parts = text.split(None, 1)
    if len(parts) != 2:
        raise ParseError(f"{source}: cannot split Stock ID and name in {text!r}")
    return parts[0], parts[1].strip()


def parse(raw: dict[str, Any]) -> pd.DataFrame:
    """parse(raw) → rows: DataFrame with stock_id, name, category, market.

    The page's 市場別 must match the source the fetch asked for, so a swapped
    strMode page fails loudly rather than mislabeling a whole market.
    """
    source = raw.get("source")
    if source not in PAGES:
        raise ParseError(f"unknown raw payload source {source!r}")
    expected_market = PAGES[source]["market_label"]
    try:
        tree = lxml.html.fromstring(raw["payload"])
    except (lxml.etree.ParserError, ValueError) as exc:
        raise ParseError(f"{source}: unparseable ISIN page: {exc}") from exc

    header: dict[str, int] | None = None
    section: str | None = None
    records = []
    for tr in tree.iter("tr"):
        cells = [td.text_content().strip() for td in tr.findall("td")]
        if not cells:
            continue
        if header is None:
            if ID_NAME_COLUMN in cells:
                missing = [c for c in (ID_NAME_COLUMN, MARKET_COLUMN, INDUSTRY_COLUMN)
                           if c not in cells]
                if missing:
                    raise ParseError(
                        f"{source}: expected columns missing from ISIN table: {missing} "
                        f"(got {cells}) — source format changed?"
                    )
                header = {c: i for i, c in enumerate(cells)}
            continue
        if len(cells) == 1:                       # a section header spanning the table
            section = cells[0]
            continue
        if len(cells) != len(header):
            raise ParseError(
                f"{source}: row with {len(cells)} cells under a {len(header)}-column "
                f"header: {cells[:2]} — source format changed?"
            )
        if section is None:
            raise ParseError(f"{source}: data row before any section header: {cells[0]!r}")
        if section not in SECTION_CATEGORY:
            continue
        stock_id, name = _split_id_name(cells[header[ID_NAME_COLUMN]], source)
        market = cells[header[MARKET_COLUMN]]
        # 創新板 rows say 上市臺灣創新板; the market is the leading 上市 / 上櫃.
        if not market.startswith(expected_market):
            raise ParseError(
                f"{source}: expected 市場別 {expected_market!r} but the page says "
                f"{market!r} for {stock_id} — wrong strMode page?"
            )
        category = SECTION_CATEGORY[section]
        if category is INDUSTRY:
            category = cells[header[INDUSTRY_COLUMN]]
        records.append({
            "stock_id": stock_id,
            "name": name,
            "category": category,
            "market": MARKET_OF[expected_market],
        })
    if header is None:
        raise ParseError(
            f"{source}: no ISIN table with an {ID_NAME_COLUMN!r} column — "
            f"a block page, or the source format changed?"
        )
    return pd.DataFrame(records, columns=["stock_id", *FIELDS])


def both_markets_present() -> qa.Invariant:
    """A batch is the whole universe: 上市 and 上櫃 both, and nothing else."""

    def check(batch: pd.DataFrame) -> str | None:
        if "market" not in batch.columns:
            return None  # required_columns reports absence
        markets = set(batch["market"].dropna())
        if markets != set(MARKET_OF.values()):
            return f"markets {sorted(markets)}, expected {sorted(MARKET_OF.values())}"
        return None

    return qa.Invariant("both_markets_present", check)


def stock_ids_are_codes() -> qa.Invariant:
    """Every key is a 4–6 character exchange code — not a name fragment or blank."""

    def check(batch: pd.DataFrame) -> str | None:
        if "stock_id" not in batch.columns:
            return None
        bad = ~batch["stock_id"].astype(str).str.match(_STOCK_ID_RE)
        if bad.any():
            return f"{int(bad.sum())} malformed Stock IDs, e.g. {batch.loc[bad, 'stock_id'].head(3).tolist()}"
        return None

    return qa.Invariant("stock_ids_are_codes", check)


SPECS = [
    DatasetSpec(
        name="security_categories",
        official_source="TWSE ISIN C_public.jsp (strMode=2 上市, strMode=4 上櫃)",
        # The listing changes on IPO/delisting days only, but the pages are cheap:
        # re-scrape every evening once the day's listing changes are posted.
        cadence=Cadence(kind="daily", at="20:00"),
        frequency="static",
        fields=tuple(FIELDS),
        int_fields=frozenset(),
        key_fields=("stock_id",),
        invariants=(
            qa.required_columns(["stock_id", *FIELDS]),
            qa.unique_key(["stock_id"]),
            # ~1,000 上市 + ~800 上櫃 equities plus ~400 ETFs; far fewer means a
            # market page is missing or truncated.
            qa.min_rows(1500),
            both_markets_present(),
            stock_ids_are_codes(),
        ),
        # The ISIN site keeps no archive: a static table's history starts the
        # day it is first scraped.
        backfill_start=dt.date(2026, 9, 3),
        fetch=fetch,
        parse=parse,
        shape="table",
    ),
]
