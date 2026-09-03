"""本益成長比 (PEG) — a FinLab example strategy running verbatim on twlab.

This is the strategy exactly as it is written against FinLab's API — the
Data Keys (`price_earning_ratio:本益比`, `monthly_revenue:去年同月增減(%)`,
`price:收盤價`), the `.average()` helper, monthly-with-daily boolean
composition, and `sim(position, resample="M")`. The ONLY edits are the two
import lines, which point at twlab instead of the finlab package
(ADR-0003: the package is not named `finlab`, so it cannot shadow the real one).

Run it on a materialized store:

    python examples/finlab_peg_strategy.py
"""
from twlab import data              # FinLab: from finlab import data
from twlab.backtest import sim      # FinLab: from finlab.backtest import sim

# 本益比與營收年增率
pe = data.get("price_earning_ratio:本益比")
rev_yoy = data.get("monthly_revenue:去年同月增減(%)")
close = data.get("price:收盤價")

# 本益成長比 = 本益比 / 近三個月平均營收年增率
peg = pe / rev_yoy.average(3)

# 便宜的成長股，且股價站上季線
cond_cheap_growth = peg < 0.8
cond_growth = rev_yoy > 20
cond_trend = close > close.average(60)

position = cond_cheap_growth & cond_growth & cond_trend

report = sim(position, resample="M")

if __name__ == "__main__":
    print(report.summary())
