"""The Registry: declarative index of every Dataset the platform collects.

Each entry declares its Official Source, Cadence, Invariants, backfill depth,
and fetch/parse implementation. Adding dataset #2..#123 means declaring an
entry plus a parser — the pipeline, QA gate, and stores are shared.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Callable, Literal

import pandas as pd

from twlab import qa
from twlab.datasets import price


@dataclass(frozen=True)
class Cadence:
    """When a Dataset is due, mirroring its Official Source's publication window."""
    kind: Literal["daily", "monthly", "quarterly"]
    at: str              # local wall-clock time the source has published by
    tz: str = "Asia/Taipei"


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    official_source: str
    cadence: Cadence
    fields: tuple[str, ...]                 # value Fields, Catalog names
    int_fields: frozenset[str]
    key_fields: tuple[str, ...]             # uniqueness key of a long-form row
    invariants: tuple[qa.Invariant, ...]
    backfill_start: dt.date                 # deepest archive date at the source
    fetch: Callable[..., list[dict[str, Any]]]
    parse: Callable[[dict[str, Any]], pd.DataFrame]


PRICE = DatasetSpec(
    name="price",
    official_source="TWSE MI_INDEX + TPEx dailyQuotes",
    # FinLab publishes price at 21:32 — after both exchanges' final after-market files.
    cadence=Cadence(kind="daily", at="21:32"),
    fields=tuple(price.FIELDS),
    int_fields=frozenset(price.INT_FIELDS),
    key_fields=("stock_id", "date"),
    invariants=(
        qa.required_columns(["stock_id", "date", "market", *price.FIELDS]),
        qa.unique_key(["stock_id", "date"]),
        # ~1,900+ securities trade across both markets on a normal day.
        qa.min_rows(500),
        qa.non_negative(price.FIELDS),
        qa.high_not_below_low(),
    ),
    backfill_start=dt.date(2007, 4, 23),  # Catalog history start for `price`
    fetch=price.fetch,
    parse=price.parse,
)

REGISTRY: dict[str, DatasetSpec] = {spec.name: spec for spec in [PRICE]}


def get_spec(name: str) -> DatasetSpec:
    if name not in REGISTRY:
        raise KeyError(
            f"No Registry entry for Dataset {name!r}. Known: {sorted(REGISTRY)}"
        )
    return REGISTRY[name]
