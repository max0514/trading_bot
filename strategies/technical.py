"""
Technical Analysis Strategies for Taiwan Stock Market.

Modernized from legacy notebooks (TSMC_bottom_fishing, price_bias_ratio)
and inspired by FinLab strategy patterns.
"""

import pandas as pd
import numpy as np
from strategies.base import Strategy, StrategyResult


class BollingerBandStrategy(Strategy):
    """
    Bollinger Band mean-reversion strategy.
    Modernized from TSMC_bottom_fishing.ipynb.

    Buy when price drops below lower band, sell when it rises above upper band.
    """

    name = 'Bollinger Band'
    description = 'Mean-reversion using Bollinger Bands (SMA ± N×STD)'
    category = 'technical'

    def __init__(self, sma_period=60, num_std=3):
        super().__init__(sma_period=sma_period, num_std=num_std)
        self.sma_period = sma_period
        self.num_std = num_std

    @classmethod
    def get_param_schema(cls):
        return {
            'sma_period': {'type': 'int', 'default': 60, 'min': 10, 'max': 200, 'label': 'SMA Period'},
            'num_std': {'type': 'float', 'default': 3.0, 'min': 0.5, 'max': 4.0, 'label': 'Std Deviations'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']

        sma = close.rolling(self.sma_period).mean()
        std = close.rolling(self.sma_period).std()
        upper = sma + self.num_std * std
        lower = sma - self.num_std * std

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)
        in_position = False

        for i in range(self.sma_period, len(df)):
            if not in_position and close.iloc[i] < lower.iloc[i]:
                buy_signals.iloc[i] = True
                in_position = True
            elif in_position and close.iloc[i] > upper.iloc[i]:
                sell_signals.iloc[i] = True
                in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'SMA': sma, 'Upper Band': upper, 'Lower Band': lower},
        )


class MACDStrategy(Strategy):
    """
    MACD (Moving Average Convergence Divergence) strategy.

    Buy when MACD crosses above signal line, sell when it crosses below.
    """

    name = 'MACD'
    description = 'Trend-following using MACD crossover signals'
    category = 'technical'

    def __init__(self, fast=12, slow=26, signal=9):
        super().__init__(fast=fast, slow=slow, signal=signal)
        self.fast = fast
        self.slow = slow
        self.signal_period = signal

    @classmethod
    def get_param_schema(cls):
        return {
            'fast': {'type': 'int', 'default': 12, 'min': 5, 'max': 50, 'label': 'Fast EMA'},
            'slow': {'type': 'int', 'default': 26, 'min': 10, 'max': 100, 'label': 'Slow EMA'},
            'signal': {'type': 'int', 'default': 9, 'min': 3, 'max': 30, 'label': 'Signal Line'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']

        ema_fast = close.ewm(span=self.fast, adjust=False).mean()
        ema_slow = close.ewm(span=self.slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=self.signal_period, adjust=False).mean()
        histogram = macd_line - signal_line

        # Crossover signals
        prev_macd = macd_line.shift(1)
        prev_signal = signal_line.shift(1)

        buy_signals = (prev_macd <= prev_signal) & (macd_line > signal_line)
        sell_signals = (prev_macd >= prev_signal) & (macd_line < signal_line)

        # Only keep signals where we alternate buy/sell
        buy_signals, sell_signals = self._filter_alternating(buy_signals, sell_signals)

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'MACD': macd_line, 'Signal': signal_line, 'Histogram': histogram},
        )

    @staticmethod
    def _filter_alternating(buys, sells):
        """Ensure signals alternate: buy, sell, buy, sell..."""
        filtered_buys = pd.Series(False, index=buys.index)
        filtered_sells = pd.Series(False, index=sells.index)
        in_position = False

        for i in range(len(buys)):
            if not in_position and buys.iloc[i]:
                filtered_buys.iloc[i] = True
                in_position = True
            elif in_position and sells.iloc[i]:
                filtered_sells.iloc[i] = True
                in_position = False

        return filtered_buys, filtered_sells


class RSIStrategy(Strategy):
    """
    RSI (Relative Strength Index) strategy.

    Buy when RSI drops below oversold threshold, sell when it rises above overbought.
    Inspired by FinLab's RSI ranking strategy.
    """

    name = 'RSI'
    description = 'Momentum oscillator — buy oversold, sell overbought'
    category = 'technical'

    def __init__(self, period=14, oversold=30, overbought=70):
        super().__init__(period=period, oversold=oversold, overbought=overbought)
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    @classmethod
    def get_param_schema(cls):
        return {
            'period': {'type': 'int', 'default': 14, 'min': 5, 'max': 50, 'label': 'RSI Period'},
            'oversold': {'type': 'int', 'default': 30, 'min': 10, 'max': 50, 'label': 'Oversold Threshold'},
            'overbought': {'type': 'int', 'default': 70, 'min': 50, 'max': 90, 'label': 'Overbought Threshold'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']

        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.rolling(self.period).mean()
        avg_loss = loss.rolling(self.period).mean()

        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)
        in_position = False

        for i in range(self.period, len(df)):
            if pd.isna(rsi.iloc[i]):
                continue
            if not in_position and rsi.iloc[i] < self.oversold:
                buy_signals.iloc[i] = True
                in_position = True
            elif in_position and rsi.iloc[i] > self.overbought:
                sell_signals.iloc[i] = True
                in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'RSI': rsi},
        )


class KDStrategy(Strategy):
    """
    KD Stochastic Oscillator strategy (popular in Taiwan market).

    Buy when K crosses above D in oversold zone, sell when K crosses below D in overbought.
    """

    name = 'KD Stochastic'
    description = 'KD oscillator — buy golden cross in oversold, sell death cross in overbought'
    category = 'technical'

    def __init__(self, k_period=9, d_period=3, oversold=20, overbought=80):
        super().__init__(k_period=k_period, d_period=d_period, oversold=oversold, overbought=overbought)
        self.k_period = k_period
        self.d_period = d_period
        self.oversold = oversold
        self.overbought = overbought

    @classmethod
    def get_param_schema(cls):
        return {
            'k_period': {'type': 'int', 'default': 9, 'min': 5, 'max': 30, 'label': 'K Period'},
            'd_period': {'type': 'int', 'default': 3, 'min': 2, 'max': 10, 'label': 'D Period'},
            'oversold': {'type': 'int', 'default': 20, 'min': 10, 'max': 40, 'label': 'Oversold'},
            'overbought': {'type': 'int', 'default': 80, 'min': 60, 'max': 95, 'label': 'Overbought'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        high = df['max'] if 'max' in df.columns else df['close']
        low = df['min'] if 'min' in df.columns else df['close']
        close = df['close']

        lowest_low = low.rolling(self.k_period).min()
        highest_high = high.rolling(self.k_period).max()

        rsv = 100 * (close - lowest_low) / (highest_high - lowest_low).replace(0, np.nan)

        # Exponential smoothing for K and D (Taiwan style: 2/3 weight)
        k_values = pd.Series(50.0, index=df.index, dtype=float)
        d_values = pd.Series(50.0, index=df.index, dtype=float)

        for i in range(1, len(df)):
            if pd.isna(rsv.iloc[i]):
                k_values.iloc[i] = k_values.iloc[i - 1]
            else:
                k_values.iloc[i] = (2 / 3) * k_values.iloc[i - 1] + (1 / 3) * rsv.iloc[i]
            d_values.iloc[i] = (2 / 3) * d_values.iloc[i - 1] + (1 / 3) * k_values.iloc[i]

        buy_signals = pd.Series(False, index=df.index)
        sell_signals = pd.Series(False, index=df.index)
        in_position = False

        for i in range(1, len(df)):
            k_cross_up = k_values.iloc[i] > d_values.iloc[i] and k_values.iloc[i - 1] <= d_values.iloc[i - 1]
            k_cross_down = k_values.iloc[i] < d_values.iloc[i] and k_values.iloc[i - 1] >= d_values.iloc[i - 1]

            if not in_position and k_cross_up and k_values.iloc[i] < self.oversold:
                buy_signals.iloc[i] = True
                in_position = True
            elif in_position and k_cross_down and k_values.iloc[i] > self.overbought:
                sell_signals.iloc[i] = True
                in_position = False

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={'K': k_values, 'D': d_values},
        )


class MovingAverageCrossover(Strategy):
    """
    Dual Moving Average Crossover (golden cross / death cross).

    Buy when fast MA crosses above slow MA, sell on cross below.
    Inspired by FinLab's MA > 20-day MA strategy.
    """

    name = 'MA Crossover'
    description = 'Golden cross / death cross between two moving averages'
    category = 'technical'

    def __init__(self, fast_period=20, slow_period=60):
        super().__init__(fast_period=fast_period, slow_period=slow_period)
        self.fast_period = fast_period
        self.slow_period = slow_period

    @classmethod
    def get_param_schema(cls):
        return {
            'fast_period': {'type': 'int', 'default': 20, 'min': 5, 'max': 60, 'label': 'Fast MA Period'},
            'slow_period': {'type': 'int', 'default': 60, 'min': 20, 'max': 240, 'label': 'Slow MA Period'},
        }

    def generate_signals(self, price_df, **extra_data):
        df = self._ensure_sorted(price_df)
        close = df['close']

        fast_ma = close.rolling(self.fast_period).mean()
        slow_ma = close.rolling(self.slow_period).mean()

        prev_fast = fast_ma.shift(1)
        prev_slow = slow_ma.shift(1)

        raw_buys = (prev_fast <= prev_slow) & (fast_ma > slow_ma)
        raw_sells = (prev_fast >= prev_slow) & (fast_ma < slow_ma)

        buy_signals, sell_signals = MACDStrategy._filter_alternating(raw_buys, raw_sells)

        return StrategyResult(
            name=self.name,
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            price=close,
            params=self.params,
            indicators={f'MA{self.fast_period}': fast_ma, f'MA{self.slow_period}': slow_ma},
        )
