"""R&D-focused tech leaders, short-term momentum on new highs.

Fundamental gates:
  - R&D expense ratio top 20% (proxied — see NOTE below)
  - Monthly revenue YoY > 0 for 2 consecutive months
Technical gates:
  - Close == 10-day high
  - Bullish MA alignment: MA5 > MA10 > MA20 > MA60
  - Close < 200
  - 3-day average volume > 200 lots
Schedule:
  - Rebalance every 2 weeks
  - Exit: close below MA20 for 2 consecutive days

NOTE — R&D proxy: FinMind's free `taiwan_stock_financial_statement` endpoint
exposes only aggregate line items; R&D is not broken out. This strategy
uses `OperatingExpenses / Revenue` rank as a stand-in. Tech firms' OPEX is
heavily R&D-weighted so the rank correlates, but it is not the literal
condition. Swap in true R&D once a paid data source is wired up.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from backtest.data import get_price, get_monthly_revenue, get_financial
from backtest.engine import run_backtest


def main():
    close = get_price("close")
    open_ = get_price("open")
    volume_shares = get_price("volume_shares")
    volume_lots = volume_shares / 1000  # 1 lot = 1000 shares

    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()

    # === Fundamentals (proxied) ===
    opex = get_financial("OperatingExpenses")
    rev_q = get_financial("Revenue")
    opex_ratio_q = opex / rev_q  # proxy for R&D ratio

    # Top-20% rank per quarter, then propagate to daily index by announce-date
    n_top = max(1, int(opex_ratio_q.shape[1] * 0.20))
    rd_top20_q = opex_ratio_q.rank(axis=1, ascending=False) <= n_top
    rd_top20 = rd_top20_q.reindex(close.index, method="ffill").reindex(columns=close.columns).fillna(False)

    # Monthly revenue YoY > 0 for 2 consecutive months
    yoy = get_monthly_revenue("yoy")
    rev_growth_2m_m = (yoy > 0) & (yoy.shift(1) > 0)
    rev_growth_2m = rev_growth_2m_m.reindex(close.index, method="ffill").reindex(columns=close.columns).fillna(False)

    # === Technical conditions ===
    cond_high = close == close.rolling(10).max()
    cond_ma_align = (ma5 > ma10) & (ma10 > ma20) & (ma20 > ma60)
    cond_price = close < 200
    cond_volume = volume_lots.rolling(3).mean() > 200

    entry = cond_high & cond_ma_align & cond_price & cond_volume & rd_top20 & rev_growth_2m

    # === Custom exit: close below MA20 for 2 consecutive days ===
    below_ma20 = close < ma20
    exit_signal = below_ma20 & below_ma20.shift(1).fillna(False)

    report = run_backtest(
        entry=entry,
        exit_signal=exit_signal,
        open_price=open_,
        close_price=close,
        resample="2W",
        name="RD_Strong_ShortTerm",
    )

    print(report.summary())
    print(f"\nEntry-signal totals: {entry.sum().sum()} signals across {entry.shape[1]} stocks")
    if not report.trades.empty:
        print("\n=== First 5 trades ===")
        print(report.trades.head().to_string(index=False))
        print("\n=== Last 5 trades ===")
        print(report.trades.tail().to_string(index=False))

    # Save artifacts
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
    report.equity.to_csv(os.path.join(out_dir, "rd_strong_equity.csv"))
    report.trades.to_csv(os.path.join(out_dir, "rd_strong_trades.csv"), index=False)
    print("\nSaved equity curve and trades to repo root.")


if __name__ == "__main__":
    main()
