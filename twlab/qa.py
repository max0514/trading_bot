"""QA Invariants: per-Dataset structural expectations checked on every batch.

Invariants run between the Mongo upsert and Parquet materialization; any
failure Quarantines the batch — materialization is skipped and the API keeps
serving the last good Wide Frames.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

Check = Callable[[pd.DataFrame], str | None]  # None = pass, str = failure detail

# `batch.attrs` key: how many Stock IDs the fetch asked the Official Source
# about, set by the pipeline for per-company sources (DatasetSpec.universe_from).
# Market-wide sources answer one file for everybody, so they never set it.
REQUESTED = "requested"


@dataclass(frozen=True)
class Invariant:
    name: str
    check: Check


def required_columns(columns: list[str]) -> Invariant:
    def check(batch: pd.DataFrame) -> str | None:
        missing = [c for c in columns if c not in batch.columns]
        return f"missing columns: {missing}" if missing else None

    return Invariant("required_columns", check)


def unique_key(key_fields: list[str]) -> Invariant:
    def check(batch: pd.DataFrame) -> str | None:
        dupes = batch.duplicated(subset=key_fields)
        if dupes.any():
            sample = batch.loc[dupes, key_fields].head(5).to_dict("records")
            return f"{int(dupes.sum())} duplicate keys, e.g. {sample}"
        return None

    return Invariant("unique_key", check)


def min_rows(n: int) -> Invariant:
    """Absolute row-count floor for a market-wide source: a daily all-market
    batch far below the typical security count means a truncated or partial
    scrape.

    This is a stand-in for the Trading Calendar row-count Invariant story 16
    asks for — a fixed number, not the exchange's own count of what should
    have traded that day. It stays until the calendar Dataset lands.
    """

    def check(batch: pd.DataFrame) -> str | None:
        if len(batch) < n:
            return f"only {len(batch)} rows, expected at least {n}"
        return None

    return Invariant("min_rows", check)


def min_coverage(fraction: float) -> Invariant:
    """Row floor for a per-company source, as a share of the universe asked for.

    `min_rows` cannot serve here: the batch is one row per company that
    answered, so its size follows a universe that grows with the market and
    shrinks as you backfill into the past. Instead the pipeline records how
    many companies the fetch asked about in `batch.attrs[REQUESTED]`, and the
    floor is relative to that — so a MOPS outage answering 「查無所需資料」 for
    all but a handful of companies Quarantines instead of publishing as `ok`.

    Batches from market-wide sources carry no such count and are left to
    `min_rows`.
    """

    def check(batch: pd.DataFrame) -> str | None:
        requested = batch.attrs.get(REQUESTED)
        if not requested:
            return None       # not a per-company source: min_rows applies
        if len(batch) < fraction * requested:
            return (f"only {len(batch)} rows from {requested} companies asked "
                    f"({len(batch) / requested:.0%}), expected at least {fraction:.0%}")
        return None

    return Invariant("min_coverage", check)


def non_negative(fields: list[str]) -> Invariant:
    def check(batch: pd.DataFrame) -> str | None:
        for f in fields:
            if f in batch.columns and (batch[f].dropna() < 0).any():
                return f"negative values in {f!r}"
        return None

    return Invariant("non_negative", check)


def high_not_below_low(high: str = "最高價", low: str = "最低價") -> Invariant:
    def check(batch: pd.DataFrame) -> str | None:
        if high not in batch.columns or low not in batch.columns:
            return None  # required_columns reports absence
        bad = (batch[high] < batch[low]).fillna(False)
        if bad.any():
            return f"{int(bad.sum())} rows with {high} < {low}"
        return None

    return Invariant("high_not_below_low", check)


def run_invariants(invariants: list[Invariant], batch: pd.DataFrame) -> list[str]:
    """Run every Invariant; return failure messages (empty list = batch is good)."""
    failures = []
    for inv in invariants:
        detail = inv.check(batch)
        if detail is not None:
            failures.append(f"{inv.name}: {detail}")
    return failures
