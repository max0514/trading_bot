"""Seam 1 — FinlabDataFrame: strategy helpers and cross-frequency auto-alignment.

FinLab idioms (`.average`, `.rise`, `.sustain`, `.is_largest`, ...) must work
unchanged, and combining a monthly/quarterly frame with a daily one must align
to daily without manual reindexing — the way FinLab's own DataFrame behaves.
"""
import numpy as np
import pandas as pd
import pytest

from twlab.dataframe import FinlabDataFrame


def daily(values, start="2026-06-01", columns=("2330", "2317")):
    idx = pd.bdate_range(start, periods=len(values), name="date")
    df = FinlabDataFrame(values, index=idx, columns=list(columns))
    df._freq = "daily"
    return df


def monthly(values, deadlines, columns=("2330", "2317")):
    df = FinlabDataFrame(
        values, index=pd.DatetimeIndex(deadlines, name="date"), columns=list(columns)
    )
    df._freq = "monthly"
    return df


class TestHelpers:
    def test_average_is_rolling_mean_with_half_window_min_periods(self):
        df = daily([[1, 10], [2, 20], [3, 30], [4, 40]])
        avg = df.average(4)
        assert np.isnan(avg.iloc[0, 0])          # < n/2 observations
        assert avg.iloc[1, 0] == 1.5             # FinLab: min_periods = n // 2
        assert avg.iloc[3, 1] == 25.0
        assert isinstance(avg, FinlabDataFrame)
        assert avg._freq == "daily"

    def test_rise_and_fall_compare_to_n_bars_ago(self):
        df = daily([[1, 5], [2, 4], [2, 6], [1, 6]])
        assert df.rise().iloc[:, 0].tolist() == [False, True, False, False]
        assert df.fall().iloc[:, 1].tolist() == [False, True, False, False]
        assert df.rise(2).iloc[:, 0].tolist() == [False, False, True, False]

    def test_sustain_counts_satisfied_days_in_window(self):
        df = daily([[True, False], [True, False], [False, True], [True, True]])
        two_of_three = df.sustain(3, 2)
        assert two_of_three.iloc[:, 0].tolist() == [False, False, True, True]
        assert two_of_three.iloc[:, 1].tolist() == [False, False, False, True]
        # nsatisfy defaults to the whole window
        assert df.sustain(2).iloc[:, 1].tolist() == [False, False, False, True]

    def test_is_largest_and_is_smallest_rank_across_stocks_per_day(self):
        df = daily([[3.0, 1.0, 2.0], [np.nan, 1.0, 2.0]], columns=("a", "b", "c"))
        assert df.is_largest(2).iloc[0].tolist() == [True, False, True]
        assert df.is_smallest(1).iloc[0].tolist() == [False, True, False]
        # NaN never ranks
        assert df.is_largest(2).iloc[1].tolist() == [False, True, True]
        assert df.is_smallest(1).iloc[1].tolist() == [False, True, False]

    def test_helpers_preserve_frequency_tag(self):
        rev = monthly([[1, 2], [3, 4], [5, 6]], ["2026-05-10", "2026-06-10", "2026-07-10"])
        assert rev.average(2)._freq == "monthly"
        assert rev.rise()._freq == "monthly"
        assert rev.sustain(2)._freq == "monthly"
        assert rev.is_largest(1)._freq == "monthly"
        assert rev.rolling(2).sum()._freq == "monthly"
        assert rev.shift(1)._freq == "monthly"


class TestAlignment:
    def test_monthly_condition_combined_with_daily_condition_aligns_to_daily(self):
        # May revenue YoY becomes visible on the June-10 Statutory Deadline,
        # June revenue on July 10.
        yoy = monthly([[25.0, 5.0], [15.0, 30.0]], ["2026-06-10", "2026-07-10"])
        close = daily([[100.0, 50.0]] * 40)          # 2026-06-01 .. 2026-07-24

        cond = (yoy > 20) & (close > 0)

        assert isinstance(cond, FinlabDataFrame)
        assert cond._freq == "daily"
        assert cond.dtypes.eq(bool).all()
        # Dates before the first deadline are dropped (FinLab: the aligned index
        # starts at the later of the two series) — nothing is knowable there.
        assert cond.index.min() == pd.Timestamp("2026-06-10")
        # From June 10 until the next deadline, May's YoY applies.
        assert cond.loc["2026-06-10", "2330"] and not cond.loc["2026-06-10", "2317"]
        assert cond.loc["2026-07-09", "2330"] and not cond.loc["2026-07-09", "2317"]
        # From July 10, June's YoY applies.
        assert not cond.loc["2026-07-10", "2330"] and cond.loc["2026-07-10", "2317"]
        assert not cond.loc["2026-07-24", "2330"] and cond.loc["2026-07-24", "2317"]
        # Daily dates are all present (the frame is usable as a daily position).
        assert close.index[close.index >= "2026-06-10"].isin(cond.index).all()

    def test_arithmetic_between_quarterly_and_daily_frames_forward_fills(self):
        eps = FinlabDataFrame(
            [[10.0, 2.0], [12.0, 3.0]],
            index=pd.DatetimeIndex(["2026-05-15", "2026-08-14"], name="date"),
            columns=["2330", "2317"],
        )
        eps._freq = "quarterly"
        close = daily([[240.0, 60.0]] * 80, start="2026-05-01")   # through mid-August

        pe = close / eps

        assert pe._freq == "daily"
        assert pd.Timestamp("2026-05-14") not in pe.index   # before EPS is knowable
        assert pe.loc["2026-05-15", "2330"] == 24.0
        assert pe.loc["2026-08-13", "2330"] == 24.0
        assert pe.loc["2026-08-14", "2330"] == 20.0

    def test_same_frequency_frames_keep_plain_pandas_semantics(self):
        a = daily([[1.0, 1.0], [2.0, 2.0]])
        b = daily([[10.0, 10.0]], start="2026-06-02")   # only overlaps on the 2nd
        s = a + b
        assert np.isnan(s.iloc[0, 0])      # no forward fill between daily frames
        assert s.loc["2026-06-02", "2330"] == 12.0

    def test_columns_intersect_and_index_starts_at_the_later_series(self):
        yoy = monthly([[25.0, 5.0, 1.0]], ["2026-06-10"], columns=("2330", "2317", "9999"))
        close = daily([[1.0, 1.0]] * 10, start="2026-06-08")
        out = yoy + close
        assert list(out.columns) == ["2330", "2317"]
        assert out.index.min() == pd.Timestamp("2026-06-10")   # later of the two starts

    def test_plain_dataframe_on_the_left_still_aligns(self):
        yoy = monthly([[25.0, 5.0]], ["2026-06-10"])
        close = pd.DataFrame(daily([[1.0, 1.0]] * 10, start="2026-06-08"))
        out = close * yoy
        assert isinstance(out, FinlabDataFrame)
        assert out.loc["2026-06-10", "2330"] == 25.0
        assert out.loc["2026-06-17", "2330"] == 25.0

    def test_scalar_and_series_operations_are_untouched(self):
        rev = monthly([[1.0, 2.0], [3.0, 4.0]], ["2026-06-10", "2026-07-10"])
        assert (rev * 2).loc["2026-07-10", "2317"] == 8.0
        assert (rev > 2).loc["2026-07-10", "2330"]
        assert (rev * 2)._freq == "monthly"
        row_mean = rev.mean(axis=1)
        assert isinstance(row_mean, pd.Series)

    def test_frequency_inferred_from_index_when_untagged(self):
        untagged = FinlabDataFrame(
            [[1.0], [2.0], [3.0]],
            index=pd.DatetimeIndex(["2026-05-10", "2026-06-10", "2026-07-10"]),
            columns=["2330"],
        )
        close = daily([[1.0]] * 45, start="2026-05-01", columns=("2330",))
        out = untagged + close
        assert out._freq == "daily"
        assert out.loc["2026-06-12", "2330"] == 3.0     # 2 (June-10 value) + 1

    def test_position_frame_composes_helpers_and_alignment(self):
        """A FinLab-style position: revenue momentum AND price above its average."""
        rev = monthly([[100.0, 100.0], [130.0, 90.0], [150.0, 80.0]],
                      ["2026-05-10", "2026-06-10", "2026-07-10"])
        # 2330 trends up (price above its average), 2317 trends down.
        close = daily([[10.0 + i * 0.1, 10.0 - i * 0.1] for i in range(60)], start="2026-05-01")

        position = (rev > rev.average(2)) & (close > close.average(5))

        assert position._freq == "daily"
        assert position.loc["2026-07-20", "2330"]
        assert not position.loc["2026-07-20", "2317"]
