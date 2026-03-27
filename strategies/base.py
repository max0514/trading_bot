"""
Base strategy class and result container.
All strategies inherit from Strategy and implement generate_signals().
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from typing import Optional


@dataclass
class StrategyResult:
    """Container for strategy output."""
    name: str
    buy_signals: pd.Series  # boolean Series indexed by date
    sell_signals: pd.Series  # boolean Series indexed by date
    price: pd.Series  # closing price series
    params: dict = field(default_factory=dict)
    indicators: dict = field(default_factory=dict)  # additional indicator series for plotting

    @property
    def signal_count(self):
        return {'buys': int(self.buy_signals.sum()), 'sells': int(self.sell_signals.sum())}


class Strategy(ABC):
    """Base class for all trading strategies."""

    name: str = 'BaseStrategy'
    description: str = ''
    category: str = 'general'  # 'technical', 'fundamental', 'composite'

    def __init__(self, **params):
        self.params = params

    @abstractmethod
    def generate_signals(self, price_df: pd.DataFrame, **extra_data) -> StrategyResult:
        """
        Generate buy/sell signals.

        Args:
            price_df: DataFrame with columns: Timestamp, open, max, min, close, Trading_Volume
            **extra_data: Additional data (revenue_df, financial_df, etc.)

        Returns:
            StrategyResult with buy/sell boolean Series.
        """
        pass

    def _ensure_sorted(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure df is sorted by Timestamp and has datetime index."""
        df = df.copy()
        if 'Timestamp' in df.columns:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            df = df.sort_values('Timestamp').reset_index(drop=True)
            df = df.set_index('Timestamp')
        df = df[df['close'] > 0]
        return df

    @classmethod
    def get_param_schema(cls) -> dict:
        """Return parameter definitions for UI rendering. Override in subclass."""
        return {}
