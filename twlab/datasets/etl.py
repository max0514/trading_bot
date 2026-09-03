"""`etl` derived Dataset: back-adjusted OHLC (etl:adj_close / adj_open / adj_high / adj_low).

No Official Source — computed locally from `price` and the six Corporate
Action ratio Fields. FinLab's adjusted prices are 向後調整 (back-adjusted):
quotes on and after the latest event are the raw quotes, and every earlier
quote is scaled by the product of the ratios of all events after it, so the
series is continuous across ex-dates, capital reductions, and par-value changes:

    adj[t] = raw[t] × ∏ R[d]   over event dates d > t
    R[d]   = ∏ (reference price ÷ pre-event close) of that day's events

Implemented as a reversed cumulative product over the union of price and
event dates, aligned back onto the price index. A Corporate Action table that
is not materialized yet contributes no events — the platform stays usable
while event tables backfill — but a missing `price` frame is a DeriveError
(the run is Quarantined and the last good adj_* frames keep being served).

Coverage is partial: the Catalog's other etl:* keys (market_value, the
disposal/notice filters, …) belong to later tickets.
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

from twlab.errors import DatasetNotMaterializedError, DeriveError
from twlab.spec import Cadence, DatasetSpec
from twlab.store.parquet import ParquetStore

FIELDS = ("adj_close", "adj_open", "adj_high", "adj_low")
PRICE_SOURCE = {
    "adj_close": "收盤價", "adj_open": "開盤價", "adj_high": "最高價", "adj_low": "最低價",
}
# (Dataset, ratio Field) — each ratio is reference price ÷ pre-event close.
RATIO_SOURCES = (
    ("dividend_tse", "twse_divide_ratio"),
    ("dividend_otc", "otc_divide_ratio"),
    ("capital_reduction_tse", "twse_cap_divide_ratio"),
    ("capital_reduction_otc", "otc_cap_divide_ratio"),
    ("par_value_change_tse", "twse_par_value_change_divide_ratio"),
    ("par_value_change_otc", "otc_par_value_change_divide_ratio"),
)


def _empty_ratio() -> pd.DataFrame:
    return pd.DataFrame(index=pd.DatetimeIndex([], name="date"))


def combined_ratio(store: ParquetStore) -> pd.DataFrame:
    """One ratio frame R over every materialized Corporate Action table:
    index = event dates, columns = Stock IDs, the product where several
    events share a date and 1.0 elsewhere. Unmaterialized tables are skipped."""
    frames = []
    for dataset, field in RATIO_SOURCES:
        try:
            frame = store.read_frame(dataset, field)
        except DatasetNotMaterializedError:
            continue
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return _empty_ratio()
    index, columns = frames[0].index, frames[0].columns
    for frame in frames[1:]:
        index, columns = index.union(frame.index), columns.union(frame.columns)
    combined = pd.DataFrame(1.0, index=pd.DatetimeIndex(index, name="date"), columns=columns)
    for frame in frames:
        combined = combined * frame.reindex(index=index, columns=columns).astype(float).fillna(1.0)
    return combined.sort_index()


def back_adjust(raw: pd.DataFrame, ratio: pd.DataFrame) -> pd.DataFrame:
    """adj[t] = raw[t] × ∏ ratio[d] for event dates d > t, per Stock ID.

    Events for securities not in `raw` are ignored; a missing ratio counts as
    no event; NaN prices stay NaN."""
    if ratio.empty or raw.empty:
        return raw.copy()
    events = ratio.reindex(columns=raw.columns).astype(float).fillna(1.0)
    events.index = pd.DatetimeIndex(events.index)
    union = raw.index.union(events.index)
    full = events.reindex(union).fillna(1.0)
    # Reversed cumulative product = ∏ over d ≥ t; shift up one row = ∏ over d > t.
    factor = full.iloc[::-1].cumprod().iloc[::-1].shift(-1).fillna(1.0)
    return raw * factor.reindex(raw.index)


def derive(store: ParquetStore) -> dict[str, pd.DataFrame]:
    """Compute the four adj_* Wide Frames from materialized price + event tables."""
    try:
        raws = {f: store.read_frame("price", source) for f, source in PRICE_SOURCE.items()}
    except DatasetNotMaterializedError as exc:
        raise DeriveError(f"price is not materialized: {exc}") from exc

    ratio = combined_ratio(store)
    if not ratio.empty:
        values = ratio.to_numpy(dtype=float)
        if not np.isfinite(values).all() or (values <= 0).any():
            raise DeriveError(
                "Corporate Action ratio frames hold non-positive or non-finite values"
            )

    frames = {}
    for name, raw in raws.items():
        adjusted = back_adjust(raw.astype(float), ratio)
        adjusted.index = pd.DatetimeIndex(adjusted.index, name="date")
        frames[name] = adjusted
    return frames


SPECS = [
    DatasetSpec(
        name="etl",
        official_source="derived locally: price × Corporate Action ratios (no Official Source)",
        # After price (21:32) and the Corporate Action tables (20:00) have run.
        cadence=Cadence(kind="daily", at="22:00"),
        frequency="daily",
        fields=FIELDS,
        int_fields=frozenset(),
        key_fields=("stock_id", "date"),
        invariants=(),
        backfill_start=dt.date(2007, 4, 23),   # Catalog history start for etl:adj_* (= price)
        derive=derive,
        depends_on=("price", *(dataset for dataset, _ in RATIO_SOURCES)),
        coverage="partial",
    ),
]
