"""Statutory Deadline arithmetic: when the market could first know a figure.

A monthly or quarterly Dataset is collected on the deadline its Official
Source is legally bound by — 月營收 on the 10th of the following month, 季報
on 5/15, 8/14, 11/14 and 3/31 — and its Wide Frames are indexed by that date
rather than the period they describe. That is Point-in-Time Alignment, and
every Dataset that needs it uses the one implementation here.

Kept apart from `spec.py`, which is the Registry's own types: a Cadence needs
the two deadline constants, but nothing else here is about specifying a
Dataset, and the Witness reaches for `quarter_end` without wanting a
`DatasetSpec` at all.
"""
from __future__ import annotations

import calendar
import datetime as dt

import pandas as pd

MONTHLY_DEADLINE_DAY = 10
# Quarter -> (month, day) of the Statutory Deadline; Q4 falls in the next year.
QUARTERLY_DEADLINES: dict[int, tuple[int, int]] = {
    1: (5, 15), 2: (8, 14), 3: (11, 14), 4: (3, 31),
}


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
