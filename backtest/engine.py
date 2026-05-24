"""Vectorized event-driven backtest engine for Taiwan equities.

Inputs:
- entry: bool DataFrame (date × stock) — True where entry condition is met
- exit_signal: bool DataFrame (date × stock) — True triggers immediate exit
- prices: numeric DataFrame (date × stock) — used for valuation and trading

Output: Report with equity curve, trades, and headline metrics.

Mechanics:
- Equal-weight portfolio of held stocks.
- Rebalance on a schedule (`resample`); between rebalances, holdings update only on `exit_signal`.
- Trades fill at next-day open by default (avoids look-ahead).
- Costs: TW retail defaults — 0.001425 commission both sides + 0.003 sell tax.
"""
from __future__ import annotations
from dataclasses import dataclass, field
import numpy as np
import pandas as pd


@dataclass
class Report:
    equity: pd.Series
    positions: pd.DataFrame
    trades: pd.DataFrame
    name: str = "strategy"
    fee_ratio: float = 0.001425
    tax_ratio: float = 0.003
    metrics: dict = field(default_factory=dict)

    def __post_init__(self):
        self.metrics = self._compute_metrics()

    def _compute_metrics(self) -> dict:
        eq = self.equity.dropna()
        if eq.empty or len(eq) < 2:
            return {}
        ret = eq.pct_change().dropna()
        days = (eq.index[-1] - eq.index[0]).days
        years = max(days / 365.25, 1e-9)
        total_return = eq.iloc[-1] / eq.iloc[0] - 1
        cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1 / years) - 1
        peak = eq.cummax()
        dd = (eq / peak - 1)
        max_dd = dd.min()
        sharpe = ret.mean() / ret.std() * np.sqrt(252) if ret.std() > 0 else float("nan")
        downside = ret[ret < 0]
        sortino = ret.mean() / downside.std() * np.sqrt(252) if len(downside) and downside.std() > 0 else float("nan")
        wins = (self.trades["return"] > 0).sum() if not self.trades.empty else 0
        win_rate = wins / len(self.trades) if len(self.trades) else float("nan")
        return {
            "start": str(eq.index[0].date()),
            "end": str(eq.index[-1].date()),
            "n_days": len(eq),
            "n_trades": len(self.trades),
            "total_return": round(total_return, 4),
            "cagr": round(cagr, 4),
            "max_drawdown": round(max_dd, 4),
            "calmar": round(cagr / abs(max_dd), 4) if max_dd < 0 else float("nan"),
            "sharpe": round(sharpe, 4) if pd.notna(sharpe) else float("nan"),
            "sortino": round(sortino, 4) if pd.notna(sortino) else float("nan"),
            "win_rate": round(win_rate, 4) if pd.notna(win_rate) else float("nan"),
            "avg_trade_return": round(self.trades["return"].mean(), 4) if not self.trades.empty else float("nan"),
        }

    def summary(self) -> str:
        lines = [f"=== {self.name} ==="]
        for k, v in self.metrics.items():
            lines.append(f"{k:>18}: {v}")
        return "\n".join(lines)


def _resample_dates(index: pd.DatetimeIndex, freq: str) -> pd.DatetimeIndex:
    """Pick the last index date in each resample bucket — these are the rebalance days."""
    s = pd.Series(index, index=index)
    return pd.DatetimeIndex(s.resample(freq).last().dropna().values)


def run_backtest(
    entry: pd.DataFrame,
    exit_signal: pd.DataFrame,
    open_price: pd.DataFrame,
    close_price: pd.DataFrame,
    *,
    resample: str = "2W",
    fee_ratio: float = 0.001425,
    tax_ratio: float = 0.003,
    initial_capital: float = 1_000_000.0,
    name: str = "strategy",
) -> Report:
    """Run an entry/exit backtest with periodic rebalancing.

    Trades execute at the *next day's open* after a signal (entry on rebalance day,
    exit on the day exit_signal first becomes True).
    """
    # Align everything to the close-price universe + dates
    dates = close_price.index
    cols = close_price.columns
    entry = entry.reindex(index=dates, columns=cols).fillna(False).astype(bool)
    exit_signal = exit_signal.reindex(index=dates, columns=cols).fillna(False).astype(bool)
    open_price = open_price.reindex(index=dates, columns=cols)
    close_price = close_price.reindex(index=dates, columns=cols)

    rebalance_days = _resample_dates(dates, resample)
    # Drop the very first rebalance if it's the first day (need data to evaluate signals)
    rebalance_days = rebalance_days[rebalance_days >= dates[5]]

    # Per-day position mask: which stocks are held at end of day
    n_days = len(dates)
    n_stocks = len(cols)
    holding = np.zeros((n_days, n_stocks), dtype=bool)
    rebalance_mask = dates.isin(rebalance_days)

    cur = np.zeros(n_stocks, dtype=bool)
    for i, dt in enumerate(dates):
        # Apply exits first (effective same day)
        if exit_signal.iloc[i].any():
            cur = cur & ~exit_signal.iloc[i].values
        # On rebalance day, replace selection with today's entry signal
        if rebalance_mask[i]:
            cur = entry.iloc[i].values.copy()
        holding[i] = cur

    holding = pd.DataFrame(holding, index=dates, columns=cols)

    # Determine trade events: position transitions from False→True (buy) and True→False (sell)
    # Trades fill at next-day open.
    prev = holding.shift(1, fill_value=False).astype(bool)
    buys = holding & ~prev
    sells = ~holding & prev

    # Build trades record by walking per-stock state
    trade_records = []
    for stock in cols:
        b_dates = buys.index[buys[stock].values]
        s_dates = sells.index[sells[stock].values]
        # Pair up: each buy paired to next sell after it
        s_iter = iter(s_dates)
        next_sell = next(s_iter, None)
        for bd in b_dates:
            while next_sell is not None and next_sell <= bd:
                next_sell = next(s_iter, None)
            sd = next_sell if next_sell is not None else dates[-1]
            # Entry at next-day open after signal day
            try:
                entry_idx = dates.get_loc(bd) + 1
                exit_idx = dates.get_loc(sd) + 1 if sd != dates[-1] else len(dates) - 1
                if entry_idx >= len(dates):
                    continue
                exit_idx = min(exit_idx, len(dates) - 1)
                entry_px = open_price[stock].iloc[entry_idx]
                exit_px = open_price[stock].iloc[exit_idx] if exit_idx > entry_idx else close_price[stock].iloc[-1]
                if pd.isna(entry_px) or pd.isna(exit_px):
                    continue
                gross = exit_px / entry_px - 1
                net = (exit_px * (1 - fee_ratio - tax_ratio)) / (entry_px * (1 + fee_ratio)) - 1
                trade_records.append({
                    "stock_id": stock,
                    "entry_date": dates[entry_idx],
                    "exit_date": dates[exit_idx],
                    "entry_price": entry_px,
                    "exit_price": exit_px,
                    "gross_return": gross,
                    "return": net,
                    "holding_days": exit_idx - entry_idx,
                })
                if next_sell is not None:
                    next_sell = next(s_iter, None)
            except KeyError:
                continue
    trades = pd.DataFrame(trade_records)

    # Compute equity curve via daily portfolio returns.
    # On day t, the portfolio is the holdings set at end of day t-1 (we enter at day t open).
    # Daily return = mean of selected stocks' close-to-close returns (equal weight at rebalance).
    daily_ret_close = close_price.pct_change(fill_method=None)
    # When an exit fires intraday for a stock, approximate by realizing close return for that day still.
    held_yesterday = holding.shift(1, fill_value=False).astype(bool)
    n_held = held_yesterday.sum(axis=1).replace(0, np.nan)
    port_ret = (daily_ret_close.where(held_yesterday) ).sum(axis=1) / n_held
    port_ret = port_ret.fillna(0.0)

    # Apply round-trip transaction cost on rebalance days, scaled by turnover fraction.
    turnover = (holding.astype(int) - prev.astype(int)).abs().sum(axis=1) / n_stocks
    cost = turnover * (fee_ratio * 2 + tax_ratio)
    port_ret = port_ret - cost

    equity = (1 + port_ret).cumprod() * initial_capital
    equity.name = "equity"

    return Report(
        equity=equity,
        positions=holding,
        trades=trades,
        name=name,
        fee_ratio=fee_ratio,
        tax_ratio=tax_ratio,
    )
