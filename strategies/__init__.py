"""
Trading Bot Strategy Library

Modular, backtestable strategies for Taiwan stock market.
Each strategy produces buy/sell signals as a pandas DataFrame of booleans.
"""

from strategies.base import Strategy, StrategyResult
from strategies.technical import (
    BollingerBandStrategy,
    MACDStrategy,
    RSIStrategy,
    KDStrategy,
    MovingAverageCrossover,
)
from strategies.fundamental import (
    ROEFilter,
    RevenueGrowthMomentum,
    PERValueStrategy,
    LowPriceValue,
)
from strategies.composite import (
    IntentFactorStrategy,
    InstitutionalTracking,
    MomentumValueCombo,
    NewHighBreakout,
    PriceBiasReversal,
)
from strategies.backtest import Backtester

STRATEGY_REGISTRY = {
    # Technical
    'bollinger_band': BollingerBandStrategy,
    'macd': MACDStrategy,
    'rsi': RSIStrategy,
    'kd': KDStrategy,
    'ma_crossover': MovingAverageCrossover,
    # Fundamental
    'roe_filter': ROEFilter,
    'revenue_growth': RevenueGrowthMomentum,
    'per_value': PERValueStrategy,
    'low_price_value': LowPriceValue,
    # Composite
    'intent_factor': IntentFactorStrategy,
    'institutional_tracking': InstitutionalTracking,
    'momentum_value': MomentumValueCombo,
    'new_high_breakout': NewHighBreakout,
    'price_bias_reversal': PriceBiasReversal,
}
