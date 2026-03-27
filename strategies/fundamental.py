"""
Fundamental Analysis Strategies for Taiwan Stock Market.

Modernized from legacy notebooks (ROE_filter, find_low_price_stock)
and inspired by FinLab strategies (PER value, revenue growth).
"""

import pandas as pd
import numpy as np
from strategies.base import Strategy, StrategyResult


class ROEFilter(Strategy):
    """
    ROE (Return on Equity) filter strategy.
    Modernized from ROE_filter.ipynb.

    Buy stocks with high ROE that is improving, sell when ROE deteriorates.
    """

    name = 'ROE Filter'
    description = 'Buy high-ROE stocks with improving profitability'
    category = 'fundamental'

    def __init__(self, roe_threshold=10, roe_growth_min=0):
        super().__init__(roe_threshold=roe_threshold, roe_growth_min=roe_growth_min)
        self.roe_threshold = roe_threshold
        self.roe_growth_min = roe_growth_min

    @classmethod
    def get_param_schema(cls):
        return {
            'roe_threshold': {'type': 'float', 'default': 10, 'min': 0, 'max': 50, 'label': 'Min ROE (%)'},
            'roe_growth_min': {'type': 'float', 'default': 0, 'min': -20, 'max': 30, 'label': 'Min ROE Growth (%)'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']

        financial_df = extra_data.get('financial_df')

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)

        if financial_df is None or financial_df.empty:
            return StrategyResult(
                name=self.name, buy_signals=buy_signals, sell_signals=sell_signals,
                price=close, params=self.params,
            )

        fin = financial_df.copy()
        if 'Timestamp' in fin.columns:
            fin['Timestamp'] = pd.to_datetime(fin['Timestamp'])
            fin = fin.sort_values('Timestamp')

        # Look for ROE column (could be named differently)
        roe_col = None
        for col in fin.columns:
            if 'roe' in col.lower() or '權益報酬率' in col:
                roe_col = col
                break

        if roe_col is None:
            return StrategyResult(
                name=self.name, buy_signals=buy_signals, sell_signals=sell_signals,
                price=close, params=self.params,
            )

        # Forward-fill quarterly ROE onto daily price index
        fin_indexed = fin.set_index('Timestamp')[[roe_col]].rename(columns={roe_col: 'ROE'})
        fin_indexed = fin_indexed[~fin_indexed.index.duplicated(keep='last')]
        roe_daily = fin_indexed.reindex(df.index, method='ffill')
        roe_daily['ROE_prev'] = roe_daily['ROE'].shift(60)  # ~1 quarter lag

        in_position = False
        for i in range(60, len(df)):
            roe_val = roe_daily['ROE'].iloc[i]
            roe_prev = roe_daily['ROE_prev'].iloc[i]
            if pd.isna(roe_val):
                continue

            roe_growth = roe_val - roe_prev if not pd.isna(roe_prev) else 0

            if not in_position and roe_val >= self.roe_threshold and roe_growth >= self.roe_growth_min:
                buy_signals.iloc[i] = True
                in_position = True
            elif in_position and (roe_val < self.roe_threshold * 0.7 or roe_growth < -5):
                sell_signals.iloc[i] = True
                in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'ROE': roe_daily.get('ROE', pd.Series(dtype=float))},
        )


class RevenueGrowthMomentum(Strategy):
    """
    Monthly Revenue Growth Momentum strategy.
    Modernized from clustering_monthly_revenue.ipynb.

    Buy when YoY revenue growth accelerates, sell when it decelerates.
    """

    name = 'Revenue Growth'
    description = 'Buy on accelerating YoY revenue growth, sell on deceleration'
    category = 'fundamental'

    def __init__(self, growth_threshold=10, consecutive_months=2):
        super().__init__(growth_threshold=growth_threshold, consecutive_months=consecutive_months)
        self.growth_threshold = growth_threshold
        self.consecutive_months = consecutive_months

    @classmethod
    def get_param_schema(cls):
        return {
            'growth_threshold': {'type': 'float', 'default': 10, 'min': -10, 'max': 100, 'label': 'Min YoY Growth (%)'},
            'consecutive_months': {'type': 'int', 'default': 2, 'min': 1, 'max': 6, 'label': 'Consecutive Months'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']
        revenue_df = extra_data.get('revenue_df')

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)

        if revenue_df is None or revenue_df.empty:
            return StrategyResult(
                name=self.name, buy_signals=buy_signals, sell_signals=sell_signals,
                price=close, params=self.params,
            )

        rev = revenue_df.copy()
        if 'Timestamp' in rev.columns:
            rev['Timestamp'] = pd.to_datetime(rev['Timestamp'])
            rev = rev.sort_values('Timestamp')

        # Find YoY growth column
        yoy_col = None
        for col in rev.columns:
            if '去年同月增減' in col or 'yoy' in col.lower():
                yoy_col = col
                break

        if yoy_col is None and '當月營收' in rev.columns:
            # Calculate YoY manually
            rev['yoy_growth'] = rev['當月營收'].pct_change(12) * 100
            yoy_col = 'yoy_growth'

        if yoy_col is None:
            return StrategyResult(
                name=self.name, buy_signals=buy_signals, sell_signals=sell_signals,
                price=close, params=self.params,
            )

        # Forward-fill monthly revenue data onto daily index
        rev_indexed = rev.set_index('Timestamp')[[yoy_col]].rename(columns={yoy_col: 'YoY'})
        rev_indexed = rev_indexed[~rev_indexed.index.duplicated(keep='last')]
        yoy_daily = rev_indexed.reindex(df.index, method='ffill')

        # Track consecutive months of growth
        in_position = False
        consec_count = 0
        last_yoy = None

        for i in range(len(df)):
            yoy = yoy_daily['YoY'].iloc[i]
            if pd.isna(yoy):
                continue

            if yoy != last_yoy:
                if yoy >= self.growth_threshold:
                    consec_count += 1
                else:
                    consec_count = 0
                last_yoy = yoy

            if not in_position and consec_count >= self.consecutive_months:
                buy_signals.iloc[i] = True
                in_position = True
                consec_count = 0
            elif in_position and yoy < 0:
                sell_signals.iloc[i] = True
                in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'YoY Growth %': yoy_daily.get('YoY', pd.Series(dtype=float))},
        )


class PERValueStrategy(Strategy):
    """
    PER (Price-to-Earnings Ratio) value strategy.
    Inspired by FinLab PER/PBR strategies.

    Buy when PER is low (undervalued), sell when PER becomes expensive.
    Uses a rolling estimate: PER ≈ Price / (EPS × 4 for quarterly).
    """

    name = 'PER Value'
    description = 'Buy undervalued stocks (low PER), sell when overvalued'
    category = 'fundamental'

    def __init__(self, per_buy=12, per_sell=25):
        super().__init__(per_buy=per_buy, per_sell=per_sell)
        self.per_buy = per_buy
        self.per_sell = per_sell

    @classmethod
    def get_param_schema(cls):
        return {
            'per_buy': {'type': 'float', 'default': 12, 'min': 3, 'max': 25, 'label': 'Buy Below PER'},
            'per_sell': {'type': 'float', 'default': 25, 'min': 15, 'max': 60, 'label': 'Sell Above PER'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']
        financial_df = extra_data.get('financial_df')

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)

        if financial_df is None or financial_df.empty:
            return StrategyResult(
                name=self.name, buy_signals=buy_signals, sell_signals=sell_signals,
                price=close, params=self.params,
            )

        fin = financial_df.copy()
        if 'Timestamp' in fin.columns:
            fin['Timestamp'] = pd.to_datetime(fin['Timestamp'])
            fin = fin.sort_values('Timestamp')

        # Find EPS column
        eps_col = None
        for col in fin.columns:
            if 'eps' in col.lower() or '每股盈餘' in col:
                eps_col = col
                break

        if eps_col is None:
            return StrategyResult(
                name=self.name, buy_signals=buy_signals, sell_signals=sell_signals,
                price=close, params=self.params,
            )

        fin_indexed = fin.set_index('Timestamp')[[eps_col]].rename(columns={eps_col: 'EPS'})
        fin_indexed = fin_indexed[~fin_indexed.index.duplicated(keep='last')]
        eps_daily = fin_indexed.reindex(df.index, method='ffill')

        # Annualized EPS (quarterly × 4)
        annual_eps = eps_daily['EPS'] * 4
        per = close / annual_eps.replace(0, np.nan)

        in_position = False
        for i in range(len(df)):
            if pd.isna(per.iloc[i]) or per.iloc[i] <= 0:
                continue
            if not in_position and per.iloc[i] < self.per_buy:
                buy_signals.iloc[i] = True
                in_position = True
            elif in_position and per.iloc[i] > self.per_sell:
                sell_signals.iloc[i] = True
                in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'PER': per},
        )


class LowPriceValue(Strategy):
    """
    Low Price Value strategy.
    Modernized from find_low_price_stock.ipynb and FinLab's price < 6 strategy.

    Buy cheap stocks (bottom quantile), sell when they appreciate significantly.
    """

    name = 'Low Price Value'
    description = 'Buy low-priced stocks with low volatility, sell on appreciation'
    category = 'fundamental'

    def __init__(self, max_price=15, volatility_max=0.3, target_gain=0.3):
        super().__init__(max_price=max_price, volatility_max=volatility_max, target_gain=target_gain)
        self.max_price = max_price
        self.volatility_max = volatility_max
        self.target_gain = target_gain

    @classmethod
    def get_param_schema(cls):
        return {
            'max_price': {'type': 'float', 'default': 15, 'min': 3, 'max': 50, 'label': 'Max Price (TWD)'},
            'volatility_max': {'type': 'float', 'default': 0.3, 'min': 0.1, 'max': 0.8, 'label': 'Max 60d Volatility'},
            'target_gain': {'type': 'float', 'default': 0.3, 'min': 0.1, 'max': 1.0, 'label': 'Target Gain (%)'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']

        # 60-day volatility: (max - min) / max
        rolling_max = close.rolling(60).max()
        rolling_min = close.rolling(60).min()
        volatility = (rolling_max - rolling_min) / rolling_max.replace(0, np.nan)

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)
        in_position = False
        buy_price = 0

        for i in range(60, len(df)):
            price = close.iloc[i]
            vol = volatility.iloc[i]

            if pd.isna(vol):
                continue

            if not in_position and price <= self.max_price and vol < self.volatility_max:
                buy_signals.iloc[i] = True
                in_position = True
                buy_price = price
            elif in_position and buy_price > 0:
                gain = (price - buy_price) / buy_price
                if gain >= self.target_gain or gain <= -0.15:  # stop loss at -15%
                    sell_signals.iloc[i] = True
                    in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'60d Volatility': volatility},
        )
