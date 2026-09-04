"""Legacy data loaders, now a thin shim over twlab (ADR-0003).

The functions keep their names, arguments, and Wide Frame shape (index =
dates, columns = stock_id strings) so existing strategies run unchanged, but
they read twlab's materialized Parquet store instead of the FinMind-fed Mongo
collections:

    get_price("close")            → data.get("price:收盤價")
    get_monthly_revenue("yoy")    → data.get("monthly_revenue:去年同月增減(%)")
    get_financial("Revenue")      → data.get("financial_statement:營業收入淨額")

Two deliberate differences from the FinMind era, both point-in-time
corrections: monthly and quarterly frames are indexed by their Statutory
Deadline (10th of the next month; 5/15, 8/14, 11/14, 3/31) rather than the
period, and quarterly flows are single quarters. Monthly revenue is still
returned in 元 (twlab stores MOPS's 千元; the shim scales it) so absolute
thresholds in old strategies keep their meaning. Returned frames are
FinlabDataFrames, so FinLab helpers and auto-alignment are available too.
"""
from __future__ import annotations

import pandas as pd

from twlab import catalog, data
from twlab.dataframe import FinlabDataFrame

PRICE_FIELD_MAP = {
    "open": "開盤價",
    "high": "最高價",
    "low": "最低價",
    "close": "收盤價",
    "volume_shares": "成交股數",
    "turnover": "成交金額",
    "trades": "成交筆數",
}

# FinMind `type` names (what the old loader accepted) → financial_statement Fields.
FINANCIAL_FIELD_MAP = {
    "Revenue": "營業收入淨額",
    "CostOfGoodsSold": "營業成本",
    "GrossProfit": "營業毛利",
    "OperatingExpenses": "營業費用",
    "SellingExpenses": "推銷費用",
    "AdministrativeExpenses": "管理費用",
    "ResearchAndDevelopmentExpenses": "研究發展費",
    "OperatingIncome": "營業利益",
    "TotalNonoperatingIncomeAndExpense": "營業外收入及支出",
    "PreTaxIncome": "稅前淨利",
    "IncomeTax": "所得稅費用",
    "IncomeFromContinuingOperations": "繼續營業單位損益",
    "IncomeAfterTaxes": "合併總損益",
    "TotalConsolidatedProfitForThePeriod": "本期綜合損益總額",
    "EquityAttributableToOwnersOfParent": "歸屬母公司淨利_損",
    "NoncontrollingInterests": "歸屬非控制權益淨利_損",
    "EPS": "每股盈餘",
    "InterestExpense": "利息費用",
    "CashAndCashEquivalents": "現金及約當現金",
    "Inventories": "存貨",
    "CurrentAssets": "流動資產",
    "TotalAssets": "資產總額",
    "CurrentLiabilities": "流動負債",
    "Liabilities": "負債總額",
    "Equity": "股東權益總額",
    "OrdinaryShare": "普通股股本",
    "CashFlowsFromOperatingActivities": "營業活動之淨現金流入_流出",
}

MONTHLY_REVENUE_SCALE = 1000   # twlab serves MOPS's 千元; the old loader returned 元


def get_price(field: str = "close") -> FinlabDataFrame:
    """Wide price frame for one of: open/high/low/close/volume_shares/turnover/trades."""
    if field not in PRICE_FIELD_MAP:
        raise ValueError(f"Unknown price field {field!r}. Choices: {list(PRICE_FIELD_MAP)}")
    return data.get(f"price:{PRICE_FIELD_MAP[field]}")


def get_monthly_revenue(field: str = "revenue") -> FinlabDataFrame:
    """Monthly revenue indexed by Statutory Deadline.

    field: 'revenue' (元, as the FinMind-era loader returned) or 'yoy'
    (% change vs the same month last year, as MOPS reports it).
    """
    if field == "revenue":
        return data.get("monthly_revenue:當月營收") * MONTHLY_REVENUE_SCALE
    if field == "yoy":
        return data.get("monthly_revenue:去年同月增減(%)")
    raise ValueError(f"Unknown monthly_revenue field {field!r}. Choices: ['revenue', 'yoy']")


def get_financial(field: str) -> FinlabDataFrame:
    """Quarterly statement frame indexed by Statutory Deadline.

    `field` is either a FinMind `type` name the old loader accepted (e.g.
    'Revenue', 'OperatingExpenses') or a financial_statement Field name
    (e.g. '營業收入淨額').
    """
    name = FINANCIAL_FIELD_MAP.get(field, field)
    key = f"financial_statement:{name}"
    try:
        catalog.resolve(key)
    except KeyError:
        raise ValueError(
            f"Unknown financial field {field!r}. FinMind names: {sorted(FINANCIAL_FIELD_MAP)}; "
            f"or any financial_statement Field name from the Catalog."
        ) from None
    return data.get(key)


def list_stocks() -> list[str]:
    """Stock IDs present in the price Dataset."""
    return sorted(str(c) for c in data.get("price:收盤價").columns)
