"""FinlabDataFrame: the Wide Frame type returned by `data.get()`.

A pandas DataFrame subclass carrying FinLab's strategy-helper set and its
cross-frequency auto-alignment: when a monthly or quarterly frame (indexed by
Statutory Deadline) meets a daily frame in a comparison, arithmetic, or boolean
operation, both are aligned onto the union of their dates with forward fill
and the intersection of their Stock IDs — so `rev_yoy > 20` composes with a
daily price condition without manual reindexing.

Frequency travels with the frame as `_freq` ("daily" / "monthly" /
"quarterly" / "static"); `data.get()` sets it from the Dataset's manifest.
Untagged frames infer it from their index spacing.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

_FREQ_RANK = {"daily": 0, "monthly": 1, "quarterly": 2}

_ALIGNED_OPS = [
    "__lt__", "__le__", "__gt__", "__ge__", "__eq__", "__ne__",
    "__add__", "__sub__", "__mul__", "__truediv__", "__floordiv__", "__mod__", "__pow__",
    "__radd__", "__rsub__", "__rmul__", "__rtruediv__", "__rfloordiv__", "__rmod__", "__rpow__",
    "__and__", "__or__", "__xor__", "__rand__", "__ror__", "__rxor__",
]


def infer_frequency(index: pd.Index) -> str | None:
    """Guess a frame's frequency from its DatetimeIndex spacing."""
    if not isinstance(index, pd.DatetimeIndex) or len(index) < 2:
        return None
    gap = np.median(np.diff(index.values).astype("timedelta64[D]").astype(int))
    if gap <= 7:
        return "daily"
    if gap <= 45:
        return "monthly"
    if gap <= 200:
        return "quarterly"
    return None


class _Retagging:
    """Wraps a rolling/expanding/ewm object so its results keep the frequency tag."""

    def __init__(self, inner: Any, freq: str | None):
        self._inner = inner
        self._freq = freq

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if not callable(attr):
            return attr

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            out = attr(*args, **kwargs)
            if isinstance(out, FinlabDataFrame):
                out._freq = self._freq
            return out

        return wrapped


class FinlabDataFrame(pd.DataFrame):
    _metadata = ["_freq"]
    _freq: str | None = None

    @property
    def _constructor(self):
        return FinlabDataFrame

    @property
    def _constructor_sliced(self):
        return pd.Series

    # ── frequency ──────────────────────────────────────────────────────

    @property
    def frequency(self) -> str | None:
        return self._freq or infer_frequency(self.index)

    def rolling(self, *args: Any, **kwargs: Any):
        return _Retagging(super().rolling(*args, **kwargs), self._freq)

    def expanding(self, *args: Any, **kwargs: Any):
        return _Retagging(super().expanding(*args, **kwargs), self._freq)

    def ewm(self, *args: Any, **kwargs: Any):
        return _Retagging(super().ewm(*args, **kwargs), self._freq)

    # ── FinLab strategy helpers ────────────────────────────────────────

    def average(self, n: int) -> "FinlabDataFrame":
        """Rolling mean over `n` bars (FinLab: min_periods = n // 2)."""
        return self.rolling(n, min_periods=int(n / 2)).mean()

    def rise(self, n: int = 1) -> "FinlabDataFrame":
        """True where the value is higher than `n` bars ago."""
        return self > self.shift(n)

    def fall(self, n: int = 1) -> "FinlabDataFrame":
        """True where the value is lower than `n` bars ago."""
        return self < self.shift(n)

    def sustain(self, nwindow: int, nsatisfy: int | None = None) -> "FinlabDataFrame":
        """True where at least `nsatisfy` of the last `nwindow` bars are True
        (default: all of them)."""
        nsatisfy = nsatisfy or nwindow
        return self.rolling(nwindow).sum() >= nsatisfy

    def is_largest(self, n: int) -> "FinlabDataFrame":
        """True for the `n` largest values across Stock IDs on each date."""
        ranked = self.rank(axis=1, ascending=False, method="first")
        return ranked <= n

    def is_smallest(self, n: int) -> "FinlabDataFrame":
        """True for the `n` smallest values across Stock IDs on each date."""
        ranked = self.rank(axis=1, ascending=True, method="first")
        return ranked <= n

    def quantile_row(self, c: float) -> pd.Series:
        """Per-date quantile across Stock IDs."""
        return self.quantile(c, axis=1)


# ── cross-frequency auto-alignment ─────────────────────────────────────────

def _freq_of(obj: Any) -> str | None:
    if isinstance(obj, FinlabDataFrame):
        return obj.frequency
    if isinstance(obj, pd.DataFrame):
        return infer_frequency(obj.index)
    return None


def _finer(a: str | None, b: str | None) -> str | None:
    ra = _FREQ_RANK.get(a or "daily", 0)
    rb = _FREQ_RANK.get(b or "daily", 0)
    return (a or "daily") if ra <= rb else (b or "daily")


def align(left: Any, right: Any) -> tuple[Any, Any, str | None]:
    """FinLab-style reshape of two operands.

    Two DataFrames of different frequency are reindexed onto the union of
    their dates (from the later of the two starts) with forward fill, keeping
    only the Stock IDs both share. Same-frequency operands, scalars, and
    Series keep plain pandas semantics.
    """
    if not (isinstance(left, pd.DataFrame) and isinstance(right, pd.DataFrame)):
        freq = _freq_of(left) if isinstance(left, pd.DataFrame) else _freq_of(right)
        return left, right, freq

    lf, rf = _freq_of(left), _freq_of(right)
    if lf in (None, "static") or rf in (None, "static") or lf == rf:
        return left, right, lf or rf
    if not (isinstance(left.index, pd.DatetimeIndex) and isinstance(right.index, pd.DatetimeIndex)):
        return left, right, lf

    index = left.index.union(right.index)
    if len(left.index) and len(right.index):
        start = max(left.index.min(), right.index.min())
        index = index[index >= start]
    columns = left.columns.intersection(right.columns)
    aligned_left = left.reindex(index=index, method="ffill")[columns]
    aligned_right = right.reindex(index=index, method="ffill")[columns]
    return aligned_left, aligned_right, _finer(lf, rf)


def _aligned_op(name: str):
    base = getattr(pd.DataFrame, name)

    def op(self: FinlabDataFrame, other: Any):
        left, right, freq = align(self, other)
        result = base(left, right)
        if isinstance(result, FinlabDataFrame):
            result._freq = freq
        return result

    op.__name__ = name
    op.__qualname__ = f"FinlabDataFrame.{name}"
    return op


for _name in _ALIGNED_OPS:
    setattr(FinlabDataFrame, _name, _aligned_op(_name))
