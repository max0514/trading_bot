"""Dataset specification types shared by the Registry and every Dataset module.

A Dataset module (twlab/datasets/<name>.py) declares one or more `DatasetSpec`
entries in a module-level `SPECS` list; the Registry discovers them. Statutory
Deadline arithmetic for Point-in-Time Alignment also lives here so monthly and
quarterly Datasets share one implementation.
"""
from __future__ import annotations

import calendar
import datetime as dt
from dataclasses import dataclass
from typing import Any, Callable, Literal

import pandas as pd

from twlab import qa

Frequency = Literal["daily", "monthly", "quarterly", "static"]
Shape = Literal["wide", "table"]

MONTHLY_DEADLINE_DAY = 10
# Quarter -> (month, day) of the Statutory Deadline; Q4 falls in the next year.
QUARTERLY_DEADLINES: dict[int, tuple[int, int]] = {
    1: (5, 15), 2: (8, 14), 3: (11, 14), 4: (3, 31),
}


@dataclass(frozen=True)
class Cadence:
    """When a Dataset is due, mirroring its Official Source's publication window.

    daily      → every calendar day at `at` (non-trading days yield no_data)
    monthly    → the monthly Statutory Deadline (10th) at `at`, for the prior month
    quarterly  → each quarterly Statutory Deadline at `at`, for the quarter it closes
    """
    kind: Literal["daily", "monthly", "quarterly"]
    at: str                      # local wall-clock "HH:MM" the source has published by
    tz: str = "Asia/Taipei"

    def is_due_day(self, day: dt.date) -> bool:
        if self.kind == "daily":
            return True
        if self.kind == "monthly":
            return day.day == MONTHLY_DEADLINE_DAY
        return (day.month, day.day) in QUARTERLY_DEADLINES.values()

    def due_at(self, day: dt.date) -> dt.datetime:
        hour, minute = (int(p) for p in self.at.split(":"))
        return dt.datetime.combine(day, dt.time(hour, minute))


# ── Statutory Deadline arithmetic ──────────────────────────────────────────

def previous_month(day: dt.date) -> tuple[int, int]:
    """(year, month) of the month before `day`'s month."""
    if day.month == 1:
        return day.year - 1, 12
    return day.year, day.month - 1


def month_start(year: int, month: int) -> dt.date:
    return dt.date(year, month, 1)


def monthly_deadline(year: int, month: int) -> dt.date:
    """Statutory Deadline for `year`-`month` revenue: the 10th of the next month."""
    if month == 12:
        return dt.date(year + 1, 1, MONTHLY_DEADLINE_DAY)
    return dt.date(year, month + 1, MONTHLY_DEADLINE_DAY)


def quarter_end(year: int, quarter: int) -> dt.date:
    month = quarter * 3
    return dt.date(year, month, calendar.monthrange(year, month)[1])


def quarterly_deadline(year: int, quarter: int) -> dt.date:
    month, day = QUARTERLY_DEADLINES[quarter]
    return dt.date(year + 1 if quarter == 4 else year, month, day)


def quarter_due_on(day: dt.date) -> tuple[int, int] | None:
    """(year, quarter) whose Statutory Deadline is exactly `day`, else None."""
    for quarter, (month, dday) in QUARTERLY_DEADLINES.items():
        if (day.month, day.day) == (month, dday):
            return (day.year - 1 if quarter == 4 else day.year), quarter
    return None


def latest_quarter_due(day: dt.date) -> tuple[int, int]:
    """The most recent (year, quarter) whose Statutory Deadline is on or before `day`."""
    candidates = []
    for year in (day.year - 1, day.year):
        for quarter in QUARTERLY_DEADLINES:
            deadline = quarterly_deadline(year, quarter)
            if deadline <= day:
                candidates.append((deadline, year, quarter))
    _, year, quarter = max(candidates)
    return year, quarter


def align_monthly(periods: pd.Series) -> pd.Series:
    """Map revenue-month period dates (any day in the month) to their Statutory Deadline."""
    ts = pd.to_datetime(periods)
    return (ts + pd.offsets.MonthBegin(1)) + pd.Timedelta(days=MONTHLY_DEADLINE_DAY - 1)


def align_quarterly(periods: pd.Series) -> pd.Series:
    """Map quarter-end period dates to their Statutory Deadline."""
    ts = pd.to_datetime(periods)
    out = []
    for value in ts:
        quarter = (value.month - 1) // 3 + 1
        out.append(pd.Timestamp(quarterly_deadline(value.year, quarter)))
    return pd.Series(out, index=periods.index)


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

    @property
    def is_derived(self) -> bool:
        return self.derive is not None
