"""Shared plumbing for the TWSE/TPEx after-market payloads.

Most Official Sources answer with the same envelope: a `stat` line that says
OK or politely reports no data, a header list naming each column, and rows of
comma-grouped number strings with dashes where a value is missing. Every
Dataset reading one of those envelopes parses it through this module, so the
Registry's promise holds — a new Dataset is an entry plus a parser, and the
plumbing underneath is written once. It also keeps the missing-value
placeholders in ONE set: five copies of `_parse_number` had already drifted
into four different ideas of what a dash means.

What genuinely differs between sources stays a parameter. Headers are spelled
three ways (TPEx sprinkles `<br>` into its quote headers, TWSE's 報酬指數 page
uses a full-width space inside 日　期, and the bulletin tables carry
meaningful internal spaces such as `最近一次申報資料 季別/日期`), so each
`SourceTable` names its own normalizer; dates are spelled three ways too, so
`parse_date` and `parse_announcement_date` are separate contracts rather than
one permissive parser that would accept a form its source never prints.

Fields are always located BY NAME. A renamed or vanished column raises
ParseError instead of silently shifting every value one place along.
"""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import pandas as pd

from twlab.errors import ParseError

# Every placeholder the exchanges print for a missing cell, in one set: the
# union of what the individual parsers accepted before they shared this
# module. "N/A" is matched case-insensitively (TPEx prints both spellings).
MISSING = frozenset({"", "-", "--", "---", "----", "-----", "—"})

# TWSE answers a polite "no data" stat rather than an error on a non-trading
# day or an event-free window. (MOPS spells its own version 查無所需資料 and
# handles it in `financial_statement`, per company rather than per batch.)
NO_DATA_PHRASES = ("沒有符合條件", "查無資料")

_TRAILING_LINK_RE = re.compile(r"\s*\(https?://[^)]*\)\s*$")
_ANNOUNCEMENT_DATE_RE = re.compile(
    r"^(\d{2,4})\s*[年/.\-]\s*(\d{1,2})\s*[月/.\-]\s*(\d{1,2})\s*日?$"
)


def is_no_data(stat: Any) -> bool:
    """Does this `stat` mean "nothing for that day", as opposed to an error?"""
    text = str(stat)
    return any(phrase in text for phrase in NO_DATA_PHRASES)


def _is_missing(text: str) -> bool:
    return text in MISSING or text.upper() == "N/A"


def parse_number(value: Any) -> float | None:
    """A comma-grouped number string; a dash/N-A placeholder means missing."""
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if _is_missing(text):
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise ParseError(f"unparseable number {value!r}") from exc


def parse_int(value: Any) -> int | None:
    """A comma-grouped, possibly signed count (買賣超 is negative half the time).

    Anything else — including a fractional share count — is source drift, not
    data, so it raises rather than rounding it away.
    """
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if _is_missing(text):
        return None
    try:
        return int(text)
    except ValueError as exc:
        raise ParseError(f"unparseable share count {value!r}") from exc


def parse_text(value: Any) -> str | None:
    """Text Fields stay text; placeholders mean missing; a trailing static link
    such as TWT49U's `(https://mops.twse.com.tw/…)` is presentation, not data."""
    if value is None:
        return None
    text = _TRAILING_LINK_RE.sub("", str(value)).strip()
    return None if _is_missing(text) else text


def _timestamp(year: int, month: int, day: int, source: str, raw: Any) -> pd.Timestamp:
    if year < 1911:                                    # a ROC calendar year
        year += 1911
    try:
        return pd.Timestamp(dt.date(year, month, day))
    except ValueError as exc:
        raise ParseError(f"{source}: unparseable date {raw!r}") from exc


def parse_date(value: Any, source: str) -> pd.Timestamp:
    """The after-market report convention: '20260807', '2026/08/07', and the
    ROC form '115/08/07'. An ISO '2026-08-07' is not a form these endpoints
    print, so it raises rather than being quietly accepted."""
    text = str(value).strip()
    parts = text.split("/")
    if len(parts) == 3 and len(parts[0]) <= 3:         # ROC: 115/08/07
        try:
            year, month, day = (int(p) for p in parts)
        except ValueError as exc:
            raise ParseError(f"{source}: unparseable date {value!r}") from exc
        return _timestamp(year, month, day, source, value)
    try:
        return pd.Timestamp(dt.datetime.strptime(text.replace("/", ""), "%Y%m%d").date())
    except ValueError as exc:
        raise ParseError(f"{source}: unparseable date {value!r}") from exc


def parse_announcement_date(value: Any, source: str) -> pd.Timestamp:
    """The bulletin convention, which the exchanges spell three ways: TWSE
    '115年06月11日' and '115/01/12', TPEx '1150112'. Gregorian forms parse too;
    a year below 1911 is read as a ROC year."""
    text = str(value).strip()
    match = _ANNOUNCEMENT_DATE_RE.match(text)
    if match:
        year, month, day = (int(g) for g in match.groups())
    elif text.isdigit() and len(text) in (7, 8):
        year, month, day = int(text[:-4]), int(text[-4:-2]), int(text[-2:])
    else:
        raise ParseError(f"{source}: unparseable date {value!r}")
    return _timestamp(year, month, day, source, value)


# ── Header spellings ───────────────────────────────────────────────────────

def squeeze(name: Any) -> str:
    """Every space removed and TPEx's `<br>` tags dropped: '最後賣量(張數)'
    survives, and TWSE's '日　期' (U+3000) becomes '日期'."""
    return re.sub(r"\s+", "", re.sub(r"<br\s*/?>", "", str(name)))


def collapse(name: Any) -> str:
    """Whitespace collapsed to one space: the bulletin tables name columns
    with meaningful internal spaces ('最近一次申報資料 季別/日期')."""
    return re.sub(r"\s+", " ", str(name)).strip()


def strip_name(name: Any) -> str:
    """Only the outer whitespace removed — TPEx pads its header names."""
    return str(name).strip()


# ── Tables ─────────────────────────────────────────────────────────────────

def batch_columns(fields: Iterable[str]) -> list[str]:
    """The long-form batch layout every scraped Dataset produces."""
    return ["stock_id", "date", "market", *fields]


def empty_batch(
    fields: Iterable[str],
    *,
    str_fields: Iterable[str] = (),
    datetime_fields: Iterable[str] = (),
) -> pd.DataFrame:
    """A no-rows batch typed like a parsed one, so concatenating a holiday's
    empty half onto the other market's rows neither warns nor widens dtypes."""
    str_fields, datetime_fields = set(str_fields), set(datetime_fields)
    columns: dict[str, pd.Series] = {
        "stock_id": pd.Series(dtype="object"),
        "date": pd.Series(dtype="datetime64[ns]"),
        "market": pd.Series(dtype="object"),
    }
    for name in fields:
        if name in str_fields:
            dtype = "object"
        elif name in datetime_fields:
            dtype = "datetime64[ns]"
        else:
            dtype = "float64"
        columns[name] = pd.Series(dtype=dtype)
    return pd.DataFrame(columns)


@dataclass(frozen=True)
class SourceTable:
    """How one Official Source's table maps onto a Dataset's Fields."""
    market: str                                   # value for the batch's market column
    id_column: str                                # source column holding the Stock ID
    column_map: dict[str, str]                    # source column name -> Field name
    normalize: Callable[[Any], str] = strip_name  # how this source spells headers
    parse_value: Callable[[Any], Any] = parse_number
    label: str = ""                               # what to call the source in errors
    what: str = "source table"                    # ditto, for the table itself

    @property
    def where(self) -> str:
        return self.label or self.market

    @property
    def fields(self) -> tuple[str, ...]:
        return tuple(self.column_map.values())


def find_table(payload: dict, spec: SourceTable) -> dict:
    """Locate the table carrying `spec.id_column` — never by title, which the
    sources leave blank or rewrite freely.

    Some endpoints wrap their table in `tables[]` (TWSE MI_INDEX, every TPEx
    page) and some put `fields`/`data` at the top level (TWSE BWIBBU_d, T86),
    so both shapes are searched. The returned copy carries normalized headers.
    """
    for table in (payload, *(payload.get("tables") or [])):
        if not isinstance(table, dict):
            continue
        fields = [spec.normalize(f) for f in (table.get("fields") or [])]
        if spec.id_column in fields:
            return {**table, "fields": fields}
    raise ParseError(
        f"{spec.where}: no table with a {spec.id_column!r} column — source "
        f"format changed? fields: {payload.get('fields')!r}, tables: "
        f"{[str(t.get('title'))[:30] for t in payload.get('tables') or []]}"
    )


def rows_from_table(table: dict, spec: SourceTable, date: pd.Timestamp) -> pd.DataFrame:
    """One long-form row per source row, all Fields located by name."""
    fields = table["fields"]
    wanted = (spec.id_column, *spec.column_map)
    missing = [c for c in wanted if c not in fields]
    if missing:
        raise ParseError(
            f"{spec.where}: expected columns missing from {spec.what}: {missing} "
            f"(got {fields}) — source format changed?"
        )
    index_of = {name: fields.index(name) for name in wanted}
    width = max(index_of.values()) + 1
    records = []
    for row in table.get("data") or []:
        if len(row) < width:
            raise ParseError(
                f"{spec.where}: row has {len(row)} cells under a {len(fields)}-column "
                f"header: {row[:2]} — source format changed?"
            )
        record: dict[str, Any] = {
            "stock_id": str(row[index_of[spec.id_column]]).strip(),
            "date": date,
            "market": spec.market,
        }
        for source_name, field_name in spec.column_map.items():
            record[field_name] = spec.parse_value(row[index_of[source_name]])
        records.append(record)
    if not records:
        return empty_batch(spec.fields)
    return pd.DataFrame(records, columns=batch_columns(spec.fields))
