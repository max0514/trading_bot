"""Corporate Action Datasets: six reference-price Event Tables.

    dividend_tse           TWSE 除權除息計算結果表            rwd/zh/exRight/TWT49U
    dividend_otc           TPEx 除權除息計算結果表            www/zh-tw/bulletin/exDailyQ
    capital_reduction_tse  TWSE 減資恢復買賣參考價            rwd/zh/reducation/TWTAUU
    capital_reduction_otc  TPEx 減資恢復買賣參考價格          www/zh-tw/bulletin/revivt
    par_value_change_tse   TWSE 變更股票面額恢復買賣參考價    rwd/zh/change/TWTB8U
    par_value_change_otc   TPEx 變更股票面額恢復買賣參考價格  www/zh-tw/bulletin/pvChgRslt

All six endpoints were verified live on 2026-09-03 (the TPEx names come from
each announcement page's `tables.init({action: …})`; the TWSE par-value
table lives under the `change` controller, not `reducation`).

Every table is the same thing — dated rows keyed by Stock ID carrying a
pre-event close and a post-event reference price — so one parser serves all
six, parameterized by an `EventTable`: which source columns map onto which
Catalog Fields (located BY NAME; drift raises ParseError), which column dates
the event, and which two Fields form the back-adjustment ratio
(reference price ÷ pre-event close) that the `etl` Dataset consumes.

Rows are dated by the event date — the ex-date, or the day trading resumes —
and materialized as one Wide Frame per Field (index = event date, columns =
Stock ID), exactly as FinLab serves them. A month holds 0–300 events, so the
Invariants check structure (positive prices, a plausible ratio) rather than a
row floor; an empty window is a `no_data` run.

Source quirks the parser absorbs: the two exchanges print ROC dates three
ways (TWT49U `115年06月11日`, TWTAUU/TWTB8U `115/01/12`, TPEx `1150112`);
TPEx pads names with spaces and names several columns differently from the
Catalog (漲停價 / 開始交易基準價 / 每仟股無償配股 …, mapped by name below);
placeholders (`--`, blank) mean missing for text as well as numbers; and
TWT49U appends the static MOPS link to the 季別 cell
(`115年第2季(https://mops.twse.com.tw/…)`), which is stripped.

Each fetch asks for the trailing 31-day window ending on the batch day:
events are announced ahead, windows overlap, and upserts are idempotent.
"""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass, field
from functools import partial
from typing import Any

import pandas as pd

from twlab import qa
from twlab.errors import ParseError
from twlab.http import PoliteSession
from twlab.spec import Cadence, DatasetSpec

WINDOW_DAYS = 31

# Plausible back-adjustment ratios per event kind. A dividend can only lower
# the reference price (a rights issue priced above market nudges it up); a
# capital reduction raises it, by a lot when most of the capital is written
# off; a par-value change can go either way (面額 10 → 1 is a 0.1 ratio).
DIVIDEND_RATIO_RANGE = (0.2, 3.0)            # 緯穎's 2026 200% stock dividend is 0.335
CAPITAL_REDUCTION_RATIO_RANGE = (0.5, 30.0)
PAR_VALUE_RATIO_RANGE = (0.005, 200.0)       # 面額 10 → 0.5 is exactly 0.05

_MISSING = {"", "-", "--", "---", "N/A", "—"}
_TRAILING_LINK_RE = re.compile(r"\s*\(https?://[^)]*\)\s*$")
_DATE_RE = re.compile(r"^(\d{2,4})\s*[年/.\-]\s*(\d{1,2})\s*[月/.\-]\s*(\d{1,2})\s*日?$")


@dataclass(frozen=True)
class EventTable:
    """How one Official Source's reference-price table maps onto a Dataset."""
    name: str
    market: str                          # value for the batch's market column
    style: str                           # payload envelope: "twse" or "tpex"
    official_source: str
    url: str
    id_column: str                       # source column holding the Stock ID
    date_column: str                     # source column holding the event date
    column_map: dict[str, str]           # source column -> Field, in Catalog order
    ratio_field: str                     # derived Field: numerator / denominator
    ratio_numerator: str
    ratio_denominator: str
    ratio_range: tuple[float, float]
    backfill_start: dt.date              # Catalog history start
    str_fields: frozenset[str] = field(default_factory=frozenset)
    datetime_fields: frozenset[str] = field(default_factory=frozenset)

    @property
    def fields(self) -> tuple[str, ...]:
        return (*self.column_map.values(), self.ratio_field)


def _same(*names: str) -> dict[str, str]:
    return {n: n for n in names}


TABLES: dict[str, EventTable] = {t.name: t for t in (
    EventTable(
        name="dividend_tse", market="TWSE", style="twse",
        official_source="TWSE 除權除息計算結果表 (exRight/TWT49U)",
        url="https://www.twse.com.tw/rwd/zh/exRight/TWT49U",
        id_column="股票代號", date_column="資料日期",
        column_map={
            **_same("除權息前收盤價", "除權息參考價", "權值+息值"),
            "權/息": "權息",
            **_same("漲停價格", "跌停價格", "開盤競價基準", "減除股利參考價", "詳細資料"),
            "最近一次申報資料 季別/日期": "最近一次申報資料 季別日期",
            **_same("最近一次申報每股 (單位)淨值", "最近一次申報每股 (單位)盈餘"),
        },
        ratio_field="twse_divide_ratio",
        ratio_numerator="除權息參考價", ratio_denominator="除權息前收盤價",
        ratio_range=DIVIDEND_RATIO_RANGE,
        backfill_start=dt.date(2003, 5, 6),
        str_fields=frozenset({"權息", "詳細資料", "最近一次申報資料 季別日期"}),
    ),
    EventTable(
        name="dividend_otc", market="TPEx", style="tpex",
        official_source="TPEx 除權除息計算結果表 (bulletin/exDailyQ)",
        url="https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ",
        id_column="代號", date_column="除權息日期",
        column_map={
            **_same("除權息前收盤價", "除權息參考價", "權值", "息值"),
            "權值+息值": "權+息值",
            "權/息": "權息",   # TPEx prints text (除息/除權/除權息); the Catalog types it float
            "漲停價": "漲停價格",
            "跌停價": "跌停價格",
            "開始交易基準價": "開盤競價基準",
            **_same("減除股利參考價", "現金股利"),
            "每仟股無償配股": "每千股無償配股",
            **_same("現金增資股數", "現金增資認購價", "公開承銷股數", "員工認購股數"),
            "原股東認購股數": "原股東認購數",
            "按持股比例仟股認購": "按持股比例千股認購",
        },
        ratio_field="otc_divide_ratio",
        ratio_numerator="除權息參考價", ratio_denominator="除權息前收盤價",
        ratio_range=DIVIDEND_RATIO_RANGE,
        backfill_start=dt.date(2008, 1, 10),
        str_fields=frozenset({"權息"}),
    ),
    EventTable(
        name="capital_reduction_tse", market="TWSE", style="twse",
        official_source="TWSE 減資恢復買賣參考價 (reducation/TWTAUU)",
        url="https://www.twse.com.tw/rwd/zh/reducation/TWTAUU",
        id_column="股票代號", date_column="恢復買賣日期",
        column_map=_same("恢復買賣日期", "減資原因", "恢復買賣參考價", "停止買賣前收盤價格",
                         "漲停價格", "跌停價格", "開盤競價基準", "除權參考價"),
        ratio_field="twse_cap_divide_ratio",
        ratio_numerator="恢復買賣參考價", ratio_denominator="停止買賣前收盤價格",
        ratio_range=CAPITAL_REDUCTION_RATIO_RANGE,
        backfill_start=dt.date(2011, 1, 25),
        str_fields=frozenset({"減資原因"}),
        datetime_fields=frozenset({"恢復買賣日期"}),
    ),
    EventTable(
        name="capital_reduction_otc", market="TPEx", style="tpex",
        official_source="TPEx 減資恢復買賣參考價格 (bulletin/revivt)",
        url="https://www.tpex.org.tw/www/zh-tw/bulletin/revivt",
        id_column="股票代號", date_column="恢復買賣日期",
        column_map={
            **_same("恢復買賣日期", "減資原因", "開始交易基準價"),
            "最後交易日之收盤價格": "最後交易之收盤價格",
            **_same("減資恢復買賣開始日參考價格", "漲停價格", "跌停價格", "除權參考價"),
        },
        ratio_field="otc_cap_divide_ratio",
        ratio_numerator="減資恢復買賣開始日參考價格", ratio_denominator="最後交易之收盤價格",
        ratio_range=CAPITAL_REDUCTION_RATIO_RANGE,
        backfill_start=dt.date(2013, 1, 16),
        # The Catalog types this table's 恢復買賣日期 as str: the source text is kept.
        str_fields=frozenset({"恢復買賣日期", "減資原因"}),
    ),
    EventTable(
        name="par_value_change_tse", market="TWSE", style="twse",
        official_source="TWSE 變更股票面額恢復買賣參考價 (change/TWTB8U)",
        url="https://www.twse.com.tw/rwd/zh/change/TWTB8U",
        id_column="股票代號", date_column="恢復買賣日期",
        column_map=_same("恢復買賣日期", "停止買賣前收盤價格", "恢復買賣參考價",
                         "漲停價格", "跌停價格", "開盤競價基準"),
        ratio_field="twse_par_value_change_divide_ratio",
        ratio_numerator="恢復買賣參考價", ratio_denominator="停止買賣前收盤價格",
        ratio_range=PAR_VALUE_RATIO_RANGE,
        backfill_start=dt.date(2020, 8, 17),
        datetime_fields=frozenset({"恢復買賣日期"}),
    ),
    EventTable(
        name="par_value_change_otc", market="TPEx", style="tpex",
        official_source="TPEx 變更股票面額恢復買賣參考價格 (bulletin/pvChgRslt)",
        url="https://www.tpex.org.tw/www/zh-tw/bulletin/pvChgRslt",
        id_column="證券代號", date_column="恢復買賣日期",
        column_map={
            **_same("恢復買賣日期", "最後交易日之收盤價格"),
            "恢復買賣開始參考價": "恢復買賣開始日參考價",
            **_same("漲停價格", "跌停價格", "開始交易基準價"),
        },
        ratio_field="otc_par_value_change_divide_ratio",
        ratio_numerator="恢復買賣開始日參考價", ratio_denominator="最後交易日之收盤價格",
        ratio_range=PAR_VALUE_RATIO_RANGE,
        backfill_start=dt.date(2019, 9, 9),
        datetime_fields=frozenset({"恢復買賣日期"}),
    ),
)}


# ── fetch ──────────────────────────────────────────────────────────────────

def fetch(session: PoliteSession, day: dt.date, dataset: str) -> list[dict[str, Any]]:
    """Fetch one Event Table for the trailing 31-day window ending on `day`."""
    table = TABLES[dataset]
    start = day - dt.timedelta(days=WINDOW_DAYS)
    if table.style == "twse":
        params = {"startDate": start.strftime("%Y%m%d"),
                  "endDate": day.strftime("%Y%m%d"), "response": "json"}
    else:
        params = {"startDate": start.strftime("%Y/%m/%d"),
                  "endDate": day.strftime("%Y/%m/%d"), "response": "json"}
    return [{"dataset": dataset, "payload": session.get_json(table.url, params=params)}]


# ── parse ──────────────────────────────────────────────────────────────────

def parse(raw: dict[str, Any]) -> pd.DataFrame:
    """parse(raw) → rows: long-form DataFrame with stock_id, date, market + the
    table's Fields. An empty range yields an empty batch; malformed structure
    raises ParseError."""
    table = TABLES.get(str(raw.get("dataset")))
    if table is None:
        raise ParseError(f"unknown raw payload dataset {raw.get('dataset')!r}")
    payload = raw["payload"]
    header, rows = (_twse_table if table.style == "twse" else _tpex_table)(payload, table)
    if header is None:
        return _empty_batch(table)
    return _rows_from_table(header, rows, table)


def _empty_batch(table: EventTable) -> pd.DataFrame:
    return pd.DataFrame(columns=["stock_id", "date", "market", *table.fields])


def _normalize(name: Any) -> str:
    return re.sub(r"\s+", " ", str(name)).strip()


def _parse_number(value: Any) -> float | None:
    """Comma-grouped number strings; '--'-style placeholders mean N/A."""
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in _MISSING:
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise ParseError(f"unparseable number {value!r}") from exc


def _parse_date(value: Any) -> pd.Timestamp:
    """ROC dates as TWSE (115年06月11日) and TPEx (115/06/11, 1150611) print
    them; Gregorian forms are accepted too."""
    text = str(value).strip()
    m = _DATE_RE.match(text)
    if m:
        year, month, day = (int(g) for g in m.groups())
    elif text.isdigit() and len(text) in (7, 8):
        year, month, day = int(text[:-4]), int(text[-4:-2]), int(text[-2:])
    else:
        raise ParseError(f"unparseable date {value!r}")
    if year < 1911:
        year += 1911
    try:
        return pd.Timestamp(dt.date(year, month, day))
    except ValueError as exc:
        raise ParseError(f"unparseable date {value!r}") from exc


def _parse_text(value: Any) -> str | None:
    """Text Fields stay text; placeholders mean missing; a trailing static link
    such as TWT49U's `(https://mops.twse.com.tw/…)` is presentation, not data."""
    if value is None:
        return None
    text = _TRAILING_LINK_RE.sub("", str(value)).strip()
    return None if text in _MISSING else text


def _twse_table(payload: dict, table: EventTable) -> tuple[list[str] | None, list]:
    stat = str(payload.get("stat", ""))
    if stat != "OK":
        # TWSE answers a polite "no data" stat for an event-free window.
        if "沒有符合條件" in stat or "查無資料" in stat:
            return None, []
        raise ParseError(f"{table.name}: unexpected TWSE stat {stat!r}")
    fields = [_normalize(f) for f in (payload.get("fields") or [])]
    data = payload.get("data") or []
    if not fields and not data:
        return None, []
    return fields, data


def _tpex_table(payload: dict, table: EventTable) -> tuple[list[str] | None, list]:
    stat = str(payload.get("stat", ""))
    if stat.lower() != "ok":
        raise ParseError(f"{table.name}: unexpected TPEx stat {stat!r}")
    tables = payload.get("tables") or []
    for t in tables:
        fields = [_normalize(f) for f in (t.get("fields") or [])]
        if table.id_column in fields:
            return fields, t.get("data") or []
    if not any(t.get("data") for t in tables):
        return None, []          # event-free window: no table, or an empty one
    raise ParseError(
        f"{table.name}: no TPEx table with a {table.id_column!r} column — source "
        f"format changed? tables: {[str(t.get('title'))[:30] for t in tables]}"
    )


def _rows_from_table(fields: list[str], data: list, table: EventTable) -> pd.DataFrame:
    wanted = [table.id_column, table.date_column, *table.column_map]
    missing = [_normalize(c) for c in wanted if _normalize(c) not in fields]
    if missing:
        raise ParseError(
            f"{table.name}: expected columns missing from source table: {missing} "
            f"(got {fields}) — source format changed?"
        )
    index_of = {c: fields.index(_normalize(c)) for c in wanted}
    width = max(index_of.values()) + 1

    records = []
    for row in data:
        if len(row) < width:
            raise ParseError(f"{table.name}: row has {len(row)} cells, expected ≥ {width}: {row!r}")
        record: dict[str, Any] = {
            "stock_id": str(row[index_of[table.id_column]]).strip(),
            "date": _parse_date(row[index_of[table.date_column]]),
            "market": table.market,
        }
        for source, name in table.column_map.items():
            value = row[index_of[source]]
            if name in table.str_fields:
                record[name] = _parse_text(value)
            elif name in table.datetime_fields:
                record[name] = _parse_date(value)
            else:
                record[name] = _parse_number(value)
        numerator = record[table.ratio_numerator]
        denominator = record[table.ratio_denominator]
        record[table.ratio_field] = (
            numerator / denominator if numerator is not None and denominator else None
        )
        records.append(record)

    df = pd.DataFrame(records, columns=["stock_id", "date", "market", *table.fields])
    for name in table.fields:
        if name in table.str_fields:
            continue
        if name in table.datetime_fields:
            df[name] = pd.to_datetime(df[name])
        else:
            df[name] = df[name].astype(float)
    return df


# ── Invariants ─────────────────────────────────────────────────────────────

def _reference_price_positive(fields: list[str]) -> qa.Invariant:
    def check(batch: pd.DataFrame) -> str | None:
        for f in fields:
            if f not in batch.columns:
                continue
            bad = batch[f].dropna() <= 0
            if bad.any():
                return f"{int(bad.sum())} rows with {f!r} <= 0"
        return None

    return qa.Invariant("reference_price_positive", check)


def _ratio_range(field_name: str, low: float, high: float) -> qa.Invariant:
    def check(batch: pd.DataFrame) -> str | None:
        if field_name not in batch.columns:
            return None
        values = batch[field_name].dropna()
        bad = (values < low) | (values > high)
        if bad.any():
            return (f"{int(bad.sum())} rows with {field_name!r} outside [{low}, {high}], "
                    f"e.g. {values[bad].head(3).round(4).tolist()}")
        return None

    return qa.Invariant("ratio_range", check)


def _spec(table: EventTable) -> DatasetSpec:
    return DatasetSpec(
        name=table.name,
        official_source=table.official_source,
        # Both exchanges publish the next day's events by the evening.
        cadence=Cadence(kind="daily", at="20:00"),
        frequency="daily",
        fields=table.fields,
        int_fields=frozenset(),
        key_fields=("stock_id", "date"),
        invariants=(
            qa.required_columns(["stock_id", "date", "market", *table.fields]),
            qa.unique_key(["stock_id", "date"]),
            _reference_price_positive([table.ratio_numerator, table.ratio_denominator]),
            _ratio_range(table.ratio_field, *table.ratio_range),
        ),
        backfill_start=table.backfill_start,
        fetch=partial(fetch, dataset=table.name),
        parse=parse,
    )


SPECS = [_spec(table) for table in TABLES.values()]
