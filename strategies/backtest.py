"""
Backtesting engine for strategy evaluation.

Takes a StrategyResult and calculates performance metrics:
- Total return, annualized return, Sharpe ratio
- Max drawdown, win rate
- Trade log
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List
from strategies.base import StrategyResult


@dataclass
class Trade:
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    return_pct: float
    holding_days: int


@dataclass
class BacktestResult:
    strategy_name: str
    total_return_pct: float
    annualized_return_pct: float
    sharpe_ratio: float
    max_drawdown_pct: float
    win_rate_pct: float
    total_trades: int
    initial_capital: float
    final_capital: float
    trades: List[Trade]
    equity_curve: pd.Series  # daily portfolio value
    daily_returns: pd.Series
    benchmark_return_pct: float  # buy-and-hold return


class Backtester:
    """Run backtests on StrategyResult objects."""

    def __init__(self, initial_capital=1_000_000, fee_rate=0.001425, tax_rate=0.003):
        self.initial_capital = initial_capital
        self.fee_rate = fee_rate  # broker commission
        self.tax_rate = tax_rate  # securities transaction tax (sell only)

    def run(self, result: StrategyResult) -> BacktestResult:
        """Execute backtest on a StrategyResult."""
        price = result.price
        buys = result.buy_signals
        sells = result.sell_signals

        capital = self.initial_capital
        shares = 0
        trades = []
        entry_price = 0
        entry_date = None

        # Track equity curve
        equity = pd.Series(0.0, index=price.index, dtype=float)

        for i in range(len(price)):
            current_price = price.iloc[i]
            date = price.index[i]

            if buys.iloc[i] and shares == 0:
                # Buy
                buy_cost = current_price * (1 + self.fee_rate)
                shares = int(capital / buy_cost)
                if shares > 0:
                    capital -= shares * buy_cost
                    entry_price = current_price
                    entry_date = date

            elif sells.iloc[i] and shares > 0:
                # Sell
                sell_revenue = shares * current_price * (1 - self.fee_rate - self.tax_rate)
                capital += sell_revenue

                ret_pct = (current_price - entry_price) / entry_price * 100
                holding = (date - entry_date).days if entry_date else 0

                trades.append(Trade(
                    entry_date=str(entry_date)[:10],
                    exit_date=str(date)[:10],
                    entry_price=round(entry_price, 2),
                    exit_price=round(current_price, 2),
                    return_pct=round(ret_pct, 2),
                    holding_days=holding,
                ))

                shares = 0
                entry_price = 0
                entry_date = None

            # Portfolio value
            equity.iloc[i] = capital + shares * current_price

        # If still holding at end, mark-to-market
        if shares > 0:
            final_price = price.iloc[-1]
            ret_pct = (final_price - entry_price) / entry_price * 100
            holding = (price.index[-1] - entry_date).days if entry_date else 0
            trades.append(Trade(
                entry_date=str(entry_date)[:10],
                exit_date=str(price.index[-1])[:10] + ' (open)',
                entry_price=round(entry_price, 2),
                exit_price=round(final_price, 2),
                return_pct=round(ret_pct, 2),
                holding_days=holding,
            ))

        final_capital = equity.iloc[-1] if len(equity) > 0 else self.initial_capital

        # Metrics
        total_return = (final_capital - self.initial_capital) / self.initial_capital * 100

        # Annualized return
        n_days = len(price)
        n_years = n_days / 252 if n_days > 0 else 1
        annualized = ((final_capital / self.initial_capital) ** (1 / max(n_years, 0.01)) - 1) * 100

        # Daily returns for Sharpe
        daily_returns = equity.pct_change().fillna(0)
        sharpe = 0.0
        if daily_returns.std() > 0:
            sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)

        # Max drawdown
        cummax = equity.cummax()
        drawdown = (equity - cummax) / cummax.replace(0, np.nan)
        max_dd = drawdown.min() * 100 if len(drawdown) > 0 else 0

        # Win rate
        winning = sum(1 for t in trades if t.return_pct > 0)
        win_rate = (winning / len(trades) * 100) if trades else 0

        # Benchmark (buy and hold)
        if len(price) > 1:
            benchmark = (price.iloc[-1] - price.iloc[0]) / price.iloc[0] * 100
        else:
            benchmark = 0

        return BacktestResult(
            strategy_name=result.name,
            total_return_pct=round(total_return, 2),
            annualized_return_pct=round(annualized, 2),
            sharpe_ratio=round(sharpe, 2),
            max_drawdown_pct=round(max_dd, 2),
            win_rate_pct=round(win_rate, 2),
            total_trades=len(trades),
            initial_capital=self.initial_capital,
            final_capital=round(final_capital, 2),
            trades=trades,
            equity_curve=equity,
            daily_returns=daily_returns,
            benchmark_return_pct=round(benchmark, 2),
        )
