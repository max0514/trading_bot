"""Data loaders that pivot MongoDB collections into wide DataFrames.

Wide DataFrame convention: index = pd.DatetimeIndex, columns = stock_id (str), values = the field.
Output mirrors finlab's `data.get(...)` return shape so strategies port cleanly.
"""
from __future__ import annotations
import os
import sys
import pandas as pd
from functools import lru_cache

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraper_in_pys.mongo import Mongo


PRICE_FIELD_MAP = {
    "open": "open",
    "high": "max",
    "low": "min",
    "close": "close",
    "volume_shares": "Trading_Volume",
    "turnover": "Trading_money",
    "trades": "Trading_turnover",
}


def _to_wide(df: pd.DataFrame, value_col: str, date_col: str = "Timestamp") -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["stock_id"] = df["stock_id"].astype(str)
    wide = df.pivot_table(index=date_col, columns="stock_id", values=value_col, aggfunc="last")
    wide = wide.sort_index()
    return wide


@lru_cache(maxsize=None)
def get_price(field: str = "close") -> pd.DataFrame:
    """Return wide price DataFrame for one of: open/high/low/close/volume_shares/turnover/trades."""
    if field not in PRICE_FIELD_MAP:
        raise ValueError(f"Unknown price field {field!r}. Choices: {list(PRICE_FIELD_MAP)}")
    col = PRICE_FIELD_MAP[field]
    repo = Mongo(collection="stock_price")
    cursor = repo.collection.find({}, {"_id": 0, "stock_id": 1, "Timestamp": 1, col: 1})
    df = pd.DataFrame(list(cursor))
    if df.empty:
        return pd.DataFrame()
    return _to_wide(df, value_col=col)


@lru_cache(maxsize=None)
def get_monthly_revenue(field: str = "revenue") -> pd.DataFrame:
    """Wide DataFrame of monthly revenue, indexed by month-end date.

    field: 'revenue' (NTD) or 'yoy' (% change vs same month last year, computed on the fly).
    """
    repo = Mongo(collection="month_revenue")
    cursor = repo.collection.find({}, {"_id": 0, "stock_id": 1, "Timestamp": 1, "revenue": 1})
    df = pd.DataFrame(list(cursor))
    if df.empty:
        return pd.DataFrame()
    rev_wide = _to_wide(df, value_col="revenue")
    if field == "revenue":
        return rev_wide
    if field == "yoy":
        # FinMind month_revenue dates are 1st-of-month-after; compare to value 12 rows back
        return (rev_wide / rev_wide.shift(12) - 1) * 100
    raise ValueError(f"Unknown monthly_revenue field {field!r}")


@lru_cache(maxsize=None)
def get_financial(field: str) -> pd.DataFrame:
    """Wide DataFrame of a quarterly financial statement field, indexed by quarter-end date.

    `field` is FinMind's `type` value, e.g. 'Revenue', 'OperatingExpenses', 'GrossProfit'.
    """
    repo = Mongo(collection="financial_statement")
    cursor = repo.collection.find(
        {"type": field},
        {"_id": 0, "stock_id": 1, "Timestamp": 1, "value": 1},
    )
    df = pd.DataFrame(list(cursor))
    if df.empty:
        return pd.DataFrame()
    return _to_wide(df, value_col="value")


def list_stocks() -> list[str]:
    """Stocks present in the price collection."""
    repo = Mongo(collection="stock_price")
    return sorted(repo.collection.distinct("stock_id"))
