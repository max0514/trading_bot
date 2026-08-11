"""FinlabDataFrame: the Wide Frame type returned by `data.get()`.

A pandas DataFrame subclass so FinLab-style strategy code receives the type it
expects. The strategy helper set (.average, .rise, .sustain, .is_largest, ...)
and cross-frequency auto-alignment land with the FinlabDataFrame-helpers ticket;
this skeleton establishes the type and constructor plumbing.
"""
from __future__ import annotations

import pandas as pd


class FinlabDataFrame(pd.DataFrame):
    @property
    def _constructor(self):
        return FinlabDataFrame

    @property
    def _constructor_sliced(self):
        return pd.Series
