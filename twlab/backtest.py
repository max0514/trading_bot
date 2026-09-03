"""`twlab.backtest.sim`: FinLab's `sim(position, ...)` entry point over the repo's engine.

FinLab strategies end with

    from finlab.backtest import sim
    sim(position, resample="M")

where `position` is a boolean (or weight) Wide Frame: True where a stock is
held after the next rebalance. This adapter maps that call onto
`backtest.engine.run_backtest` — equal-weight holdings rebalanced at
`resample`, filled at the next day's open, TW retail costs — reading prices
through `twlab.data`, so a FinLab strategy runs with its imports as the only
change. It returns the engine's Report (equity curve, positions, trades,
metrics).
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from backtest.engine import Report, run_backtest
from twlab import data

# FinLab resample aliases → pandas offsets accepted by the engine.
_RESAMPLE = {"M": "ME", "W": "W-FRI", "Q": "QE", "D": "D"}


def _as_bool(position: pd.DataFrame) -> pd.DataFrame:
    """True where held: booleans pass through; weights/NaN → held if positive."""
    frame = pd.DataFrame(position)
    if frame.dtypes.eq(bool).all():
        return frame
    return frame.astype(float).fillna(0.0) > 0


def sim(position: pd.DataFrame, resample: str = "M", *,
        fee_ratio: float = 0.001425, tax_ratio: float = 0.003,
        initial_capital: float = 1_000_000.0, name: str = "strategy",
        open_price: pd.DataFrame | None = None,
        close_price: pd.DataFrame | None = None, **_: Any) -> Report:
    """Simulate a FinLab-style position frame; extra FinLab keyword arguments
    (stop_loss, trade_at_price, …) are accepted and ignored."""
    held = _as_bool(position)
    close = pd.DataFrame(close_price if close_price is not None else data.get("price:收盤價"))
    open_ = pd.DataFrame(open_price if open_price is not None else data.get("price:開盤價"))
    # Only trading days the price frames know about; a position dated on a
    # Statutory Deadline that is not a trading day takes effect the next day.
    held = held.reindex(index=close.index, method="ffill")
    held = (held.astype(float).fillna(0.0) > 0)
    return run_backtest(
        entry=held, exit_signal=~held, open_price=open_, close_price=close,
        resample=_RESAMPLE.get(resample, resample), fee_ratio=fee_ratio,
        tax_ratio=tax_ratio, initial_capital=initial_capital, name=name,
    )
