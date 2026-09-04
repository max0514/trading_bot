"""Dataset specification types shared by the Registry and every Dataset module.

A Dataset module (twlab/datasets/<name>.py) declares one or more `DatasetSpec`
entries in a module-level `SPECS` list; the Registry discovers them. The
Statutory Deadline arithmetic a Cadence and a Dataset's `align` are built on
is `twlab/deadlines.py`.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Callable, Literal

import pandas as pd

from twlab import qa
from twlab.deadlines import MONTHLY_DEADLINE_DAY, QUARTERLY_DEADLINES

Frequency = Literal["daily", "monthly", "quarterly", "static"]
Shape = Literal["wide", "table"]


@dataclass(frozen=True)
class Cadence:
    """When a Dataset is due, mirroring its Official Source's publication window.

    daily      → every calendar day at `at` (non-trading days yield no_data)
    monthly    → the monthly Statutory Deadline (10th) at `at`, for the prior month
    quarterly  → each quarterly Statutory Deadline at `at`, for the quarter it closes
    """
    kind: Literal["daily", "monthly", "quarterly"]
    at: str                      # wall-clock "HH:MM" (server time, Asia/Taipei) the source has published by

    def is_due_day(self, day: dt.date) -> bool:
        if self.kind == "daily":
            return True
        if self.kind == "monthly":
            return day.day == MONTHLY_DEADLINE_DAY
        return (day.month, day.day) in QUARTERLY_DEADLINES.values()

    def due_at(self, day: dt.date) -> dt.datetime:
        hour, minute = (int(p) for p in self.at.split(":"))
        return dt.datetime.combine(day, dt.time(hour, minute))


# ── The specification ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class DatasetSpec:
    """Everything the pipeline needs to collect and publish one Dataset.

    Scraped Datasets provide `fetch` + `parse`; derived (ETL) Datasets provide
    `derive`, which computes Wide Frames from already-materialized Datasets.
    """
    name: str
    official_source: str
    cadence: Cadence
    frequency: Frequency
    fields: tuple[str, ...]                 # value Fields, Catalog names
    int_fields: frozenset[str]
    key_fields: tuple[str, ...]             # uniqueness key of a long-form row
    invariants: tuple[qa.Invariant, ...]
    backfill_start: dt.date                 # deepest archive date at the source
    fetch: Callable[[Any, dt.date], list[dict[str, Any]]] | None = None
    parse: Callable[[dict[str, Any]], pd.DataFrame] | None = None
    derive: Callable[[Any], dict[str, pd.DataFrame]] | None = None
    align: Callable[[pd.Series], pd.Series] | None = None   # period → availability
    shape: Shape = "wide"
    depends_on: tuple[str, ...] = ()        # Datasets a derived Dataset reads
    # "partial": only some of the Catalog's Fields are implemented yet (e.g. the
    # `etl` Dataset's adj_* keys); the Registry coverage test then checks ⊆.
    coverage: Literal["full", "partial"] = "full"
    # A Data Key whose Wide Frame columns define the Stock ID universe handed
    # to fetch(session, day, universe=[...]) — for per-company sources (MOPS).
    universe_from: str | None = None

    @property
    def is_derived(self) -> bool:
        return self.derive is not None
