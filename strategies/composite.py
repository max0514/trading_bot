"""
Composite / Advanced Strategies for Taiwan Stock Market.

These combine multiple indicators or use novel approaches inspired by
FinLab (Intent Factor, institutional tracking) and legacy notebooks
(小蝦米跟大鯨魚, price_bias_ratio, find_120days_high).
"""

import pandas as pd
import numpy as np
from strategies.base import Strategy, StrategyResult


class IntentFactorStrategy(Strategy):
    """
    Intent Factor strategy from FinLab.

    Measures whether price movement follows a direct path (intentional support)
    vs meandering (random). Lower volatility with same returns = stronger intent.

    Intent Factor = 60d Return / Sum of |daily returns| over 60 days
    """

    name = 'Intent Factor'
    description = 'Detect intentional price support via return/volatility ratio (FinLab style)'
    category = 'composite'

    def __init__(self, lookback=60, return_cap=0.20, min_volume=200000):
        super().__init__(lookback=lookback, return_cap=return_cap, min_volume=min_volume)
        self.lookback = lookback
        self.return_cap = return_cap
        self.min_volume = min_volume

    @classmethod
    def get_param_schema(cls):
        return {
            'lookback': {'type': 'int', 'default': 60, 'min': 20, 'max': 120, 'label': 'Lookback Days'},
            'return_cap': {'type': 'float', 'default': 0.20, 'min': 0.05, 'max': 0.5, 'label': 'Max Return Cap'},
            'min_volume': {'type': 'int', 'default': 200000, 'min': 50000, 'max': 1000000, 'label': 'Min Volume'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']
        volume = df.get('Trading_Volume', pd.Series(self.min_volume + 1, index=df.index))

        # Daily returns
        daily_ret = close.pct_change()

        # 60-day cumulative return
        cum_return = close / close.shift(self.lookback) - 1

        # Sum of absolute daily returns (volatility measure)
        abs_sum = daily_ret.abs().rolling(self.lookback).sum()

        # Intent factor
        intent = cum_return / abs_sum.replace(0, np.nan)

        # Score = intent / volume (find undervalued low-attention stocks)
        score = intent / volume.replace(0, np.nan)

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)
        in_position = False

        for i in range(self.lookback, len(df)):
            ret = cum_return.iloc[i]
            vol = volume.iloc[i]
            intent_val = intent.iloc[i]

            if pd.isna(intent_val):
                continue

            # Buy: positive intent, return below cap, sufficient volume
            if (not in_position and intent_val > 0.3
                    and 0 < ret < self.return_cap
                    and vol >= self.min_volume):
                buy_signals.iloc[i] = True
                in_position = True
            # Sell: intent turns negative or return exceeds cap
            elif in_position and (intent_val < 0 or ret > self.return_cap * 1.5):
                sell_signals.iloc[i] = True
                in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'Intent Factor': intent, 'Cum Return': cum_return},
        )


class InstitutionalTracking(Strategy):
    """
    Institutional Tracking strategy ("小蝦米跟大鯨魚").
    Modernized from legacy notebook.

    Tracks large shareholder (>1M shares) custody ratio changes.
    Buy when institutional accumulation detected, sell on distribution.

    Falls back to volume-based proxy if custody data unavailable.
    """

    name = 'Institutional Tracking'
    description = 'Follow big-money accumulation patterns (大鯨魚追蹤)'
    category = 'composite'

    def __init__(self, accumulation_threshold=2.0, distribution_threshold=-2.0, smoothing=10):
        super().__init__(
            accumulation_threshold=accumulation_threshold,
            distribution_threshold=distribution_threshold,
            smoothing=smoothing,
        )
        self.accumulation_threshold = accumulation_threshold
        self.distribution_threshold = distribution_threshold
        self.smoothing = smoothing

    @classmethod
    def get_param_schema(cls):
        return {
            'accumulation_threshold': {'type': 'float', 'default': 2.0, 'min': 0.5, 'max': 10.0, 'label': 'Accumulation Threshold (%)'},
            'distribution_threshold': {'type': 'float', 'default': -2.0, 'min': -10.0, 'max': -0.5, 'label': 'Distribution Threshold (%)'},
            'smoothing': {'type': 'int', 'default': 10, 'min': 3, 'max': 30, 'label': 'Smoothing Window'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']

        # Use volume-based proxy: large volume with price increase = accumulation
        volume = df.get('Trading_Volume', pd.Series(0, index=df.index))
        avg_volume = volume.rolling(20).mean()
        volume_ratio = volume / avg_volume.replace(0, np.nan)
        price_change = close.pct_change(5) * 100  # 5-day % change

        # Accumulation signal: volume spike + price rising
        accum_score = volume_ratio * price_change
        smoothed = accum_score.rolling(self.smoothing).mean()

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)
        in_position = False

        for i in range(20, len(df)):
            score = smoothed.iloc[i]
            if pd.isna(score):
                continue

            if not in_position and score > self.accumulation_threshold:
                buy_signals.iloc[i] = True
                in_position = True
            elif in_position and score < self.distribution_threshold:
                sell_signals.iloc[i] = True
                in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'Accum Score': smoothed, 'Volume Ratio': volume_ratio},
        )


class MomentumValueCombo(Strategy):
    """
    Momentum + Value composite strategy.
    Combines price momentum with fundamental value screening.

    Buy when RSI momentum is positive AND price is near 120-day low (value zone).
    Sell when momentum exhausts (near 120-day high with weakening RSI).
    """

    name = 'Momentum + Value'
    description = 'Combine RSI momentum with value zone detection'
    category = 'composite'

    def __init__(self, rsi_period=14, lookback=120, value_pct=0.3, exhaust_pct=0.9):
        super().__init__(rsi_period=rsi_period, lookback=lookback, value_pct=value_pct, exhaust_pct=exhaust_pct)
        self.rsi_period = rsi_period
        self.lookback = lookback
        self.value_pct = value_pct
        self.exhaust_pct = exhaust_pct

    @classmethod
    def get_param_schema(cls):
        return {
            'rsi_period': {'type': 'int', 'default': 14, 'min': 5, 'max': 30, 'label': 'RSI Period'},
            'lookback': {'type': 'int', 'default': 120, 'min': 30, 'max': 250, 'label': 'Lookback Days'},
            'value_pct': {'type': 'float', 'default': 0.3, 'min': 0.1, 'max': 0.5, 'label': 'Value Zone (% from low)'},
            'exhaust_pct': {'type': 'float', 'default': 0.9, 'min': 0.7, 'max': 1.0, 'label': 'Exhaust Zone (% of high)'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']

        # RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        avg_gain = gain.rolling(self.rsi_period).mean()
        avg_loss = loss.rolling(self.rsi_period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))

        # Position within N-day range
        rolling_high = close.rolling(self.lookback).max()
        rolling_low = close.rolling(self.lookback).min()
        range_pct = (close - rolling_low) / (rolling_high - rolling_low).replace(0, np.nan)

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)
        in_position = False

        for i in range(self.lookback, len(df)):
            r = rsi.iloc[i]
            pct = range_pct.iloc[i]
            if pd.isna(r) or pd.isna(pct):
                continue

            # Buy: in value zone + RSI turning up from oversold
            if not in_position and pct < self.value_pct and 30 < r < 50:
                buy_signals.iloc[i] = True
                in_position = True
            # Sell: near high + RSI overbought
            elif in_position and pct > self.exhaust_pct and r > 70:
                sell_signals.iloc[i] = True
                in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'RSI': rsi, 'Range %': range_pct},
        )


class NewHighBreakout(Strategy):
    """
    New High Breakout strategy.
    Modernized from find_120days_high.ipynb and FinLab's 250-day high strategy.

    Buy when stock breaks to N-day high (momentum breakout), sell on pullback.
    """

    name = 'New High Breakout'
    description = 'Buy on N-day high breakout, sell on pullback below MA'
    category = 'composite'

    def __init__(self, high_period=120, exit_ma=20):
        super().__init__(high_period=high_period, exit_ma=exit_ma)
        self.high_period = high_period
        self.exit_ma = exit_ma

    @classmethod
    def get_param_schema(cls):
        return {
            'high_period': {'type': 'int', 'default': 120, 'min': 20, 'max': 250, 'label': 'High Period (days)'},
            'exit_ma': {'type': 'int', 'default': 20, 'min': 5, 'max': 60, 'label': 'Exit MA Period'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']

        rolling_high = close.rolling(self.high_period).max()
        exit_line = close.rolling(self.exit_ma).mean()

        is_new_high = close >= rolling_high
        # Only trigger on the first day of a new high streak
        was_new_high = is_new_high.shift(1).fillna(False)

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)
        in_position = False

        for i in range(self.high_period, len(df)):
            if not in_position and is_new_high.iloc[i] and not was_new_high.iloc[i]:
                buy_signals.iloc[i] = True
                in_position = True
            elif in_position and close.iloc[i] < exit_line.iloc[i]:
                sell_signals.iloc[i] = True
                in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={f'{self.high_period}d High': rolling_high, f'MA{self.exit_ma}': exit_line},
        )


class PriceBiasReversal(Strategy):
    """
    Price Bias Ratio (乖離率) reversal strategy.
    Modernized from price_bias_ratio.ipynb.

    When price deviates far from MA, it tends to revert.
    Buy on extreme negative bias, sell on extreme positive bias.
    """

    name = 'Price Bias Reversal'
    description = 'Mean-reversion based on price deviation from moving average (乖離率)'
    category = 'composite'

    def __init__(self, ma_period=30, buy_bias=-0.05, sell_bias=0.05):
        super().__init__(ma_period=ma_period, buy_bias=buy_bias, sell_bias=sell_bias)
        self.ma_period = ma_period
        self.buy_bias = buy_bias
        self.sell_bias = sell_bias

    @classmethod
    def get_param_schema(cls):
        return {
            'ma_period': {'type': 'int', 'default': 30, 'min': 10, 'max': 120, 'label': 'MA Period'},
            'buy_bias': {'type': 'float', 'default': -0.05, 'min': -0.20, 'max': 0.0, 'label': 'Buy Bias Threshold'},
            'sell_bias': {'type': 'float', 'default': 0.05, 'min': 0.0, 'max': 0.20, 'label': 'Sell Bias Threshold'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']

        ma = close.rolling(self.ma_period).mean()
        bias = (close - ma) / ma

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)
        in_position = False

        for i in range(self.ma_period, len(df)):
            b = bias.iloc[i]
            if pd.isna(b):
                continue

            if not in_position and b < self.buy_bias:
                buy_signals.iloc[i] = True
                in_position = True
            elif in_position and b > self.sell_bias:
                sell_signals.iloc[i] = True
                in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'Bias Ratio': bias, f'MA{self.ma_period}': ma},
        )
