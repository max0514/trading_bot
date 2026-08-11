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
    """Row-count floor standing in for the Trading Calendar check until the
    calendar Dataset lands: a daily all-market batch far below the typical
    security count means a truncated or partial scrape."""

    def check(batch: pd.DataFrame) -> str | None:
        if len(batch) < n:
            return f"only {len(batch)} rows, expected at least {n}"
        return None

    return Invariant("min_rows", check)


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
