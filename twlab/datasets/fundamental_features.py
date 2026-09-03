"""`fundamental_features` Dataset: FinLab's 53 財務指標, derived from `financial_statement`.

A derived (ETL) Dataset: no scrape. derive(store) reads the materialized
`financial_statement` Wide Frames (single-quarter flows, point-in-time balance
sheet, indexed by Statutory Deadline) and publishes one Wide Frame per ratio
under the exact FinLab keys, with the same quarterly Point-in-Time Alignment
as its inputs — a ratio is knowable exactly when the statement it comes from is.

Conventions (matching the Catalog's descriptions: amounts in 仟元, rates in %,
per-share values in 元, turnover in times per quarter):

* Every flow (revenue, profit, cash flow) is the SINGLE QUARTER as served by
  `financial_statement`; nothing is annualized. Strategies wanting TTM do
  `.rolling(4).sum()` as they would with FinLab.
* "Average" balances (ROA, ROE, turnover) are the mean of this quarter's and the
  previous quarter's balance-sheet value; the first observed quarter uses its
  own balance.
* Growth rates compare with the same quarter one year earlier (four rows back
  on the quarterly index), in %.
* Share count = 普通股股本 / 10 (仟股 at the 10 元 par value); per-share values
  divide by it. 每股稅後淨利 is the reported 每股盈餘.
* 稅率 = 所得稅費用 / 稅前淨利; 經常稅後淨利 = 繼續營業單位損益.
* 自由現金流量 = 營業活動之淨現金流入_流出 − |取得不動產_廠房及設備| (capex is an
  outflow whichever sign MOPS prints).
* Divisions by zero yield missing values, never infinities.

Formulas (per Field) are in `_compute`; FinLab does not publish its exact
definitions, so these follow the standard TEJ/FinLab 財務指標 conventions and
are documented here as the platform's own.
"""
from __future__ import annotations

import datetime as dt
from typing import Any

import numpy as np
import pandas as pd

from twlab.errors import DatasetNotMaterializedError, DeriveError
from twlab.spec import Cadence, DatasetSpec

SOURCE = "financial_statement"
PAR_VALUE = 10.0     # 元 per share; 股本 (仟元) / 10 = 仟股

# The 53 Fields, in Catalog order.
FIELDS = [
    "營業利益", "EBITDA", "營運現金流", "歸屬母公司淨利", "折舊", "流動資產", "流動負債",
    "取得不動產廠房及設備", "經常稅後淨利", "ROA稅後息前", "ROA綜合損益", "ROE稅後",
    "ROE綜合損益", "稅前息前折舊前淨利率", "營業毛利率", "營業利益率", "稅前淨利率",
    "稅後淨利率", "業外收支營收率", "貝里比率", "營業費用率", "推銷費用率", "管理費用率",
    "研究發展費用率", "現金流量比率", "稅率", "每股營業額", "每股營業利益", "每股現金流量",
    "每股稅前淨利", "每股綜合損益", "每股稅後淨利", "總負債除總淨值", "負債比率", "淨值除資產",
    "營收成長率", "營業毛利成長率", "營業利益成長率", "稅前淨利成長率", "稅後淨利成長率",
    "經常利益成長率", "資產總額成長率", "淨值成長率", "流動比率", "速動比率", "利息支出率",
    "營運資金", "總資產週轉次數", "應收帳款週轉率", "存貨週轉率", "固定資產週轉次數",
    "淨值週轉率次數", "自由現金流量",
]

# `financial_statement` Fields the formulas read.
INPUTS = [
    "營業收入淨額", "營業成本", "營業毛利", "營業費用", "研究發展費", "推銷費用", "管理費用",
    "營業利益", "營業外收入及支出", "稅前淨利", "所得稅費用", "繼續營業單位損益", "合併總損益",
    "本期綜合損益總額", "歸屬母公司淨利_損", "綜合損益歸屬母公司", "每股盈餘",
    "折舊費用", "攤銷費用", "利息費用",
    "應收帳款及票據", "存貨", "流動資產", "不動產廠房及設備", "資產總額",
    "流動負債", "負債總額", "普通股股本", "母公司股東權益合計", "股東權益總額",
    "營業活動之淨現金流入_流出", "取得不動產_廠房及設備",
]


def _load_inputs(store: Any) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for name in INPUTS:
        try:
            frames[name] = store.read_frame(SOURCE, name)
        except DatasetNotMaterializedError as exc:
            raise DeriveError(
                f"fundamental_features needs {SOURCE}:{name} materialized first: {exc}"
            ) from exc
    base = frames["營業收入淨額"]
    index = base.index
    columns = base.columns
    for name, frame in frames.items():
        if not frame.index.equals(index) or not frame.columns.equals(columns):
            frames[name] = frame.reindex(index=index, columns=columns)
    return {name: frame.astype(float) for name, frame in frames.items()}


def _ratio(num: pd.DataFrame, den: pd.DataFrame) -> pd.DataFrame:
    out = num / den.where(den != 0)
    return out.replace([np.inf, -np.inf], np.nan)


def _pct(num: pd.DataFrame, den: pd.DataFrame) -> pd.DataFrame:
    return _ratio(num, den) * 100


def _avg(balance: pd.DataFrame) -> pd.DataFrame:
    """Mean of this and the previous quarter's balance (own value on the first row)."""
    return balance.rolling(2, min_periods=1).mean()


def _yoy(flow: pd.DataFrame) -> pd.DataFrame:
    """Growth vs the same quarter a year earlier, in %."""
    return _pct(flow - flow.shift(4), flow.shift(4).abs())


def _compute(f: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    revenue = f["營業收入淨額"]
    shares = f["普通股股本"] / PAR_VALUE                       # 仟股
    tax_rate = _ratio(f["所得稅費用"], f["稅前淨利"]).clip(lower=0, upper=1).fillna(0)
    interest = f["利息費用"].fillna(0)
    ebitda = f["營業利益"] + f["折舊費用"].fillna(0) + f["攤銷費用"].fillna(0)
    ebitda_margin_num = f["稅前淨利"] + interest + f["折舊費用"].fillna(0) + f["攤銷費用"].fillna(0)
    capex = f["取得不動產_廠房及設備"].abs()
    avg_assets = _avg(f["資產總額"])
    avg_parent_equity = _avg(f["母公司股東權益合計"])
    avg_equity = _avg(f["股東權益總額"])

    return {
        "營業利益": f["營業利益"],
        "EBITDA": ebitda,
        "營運現金流": f["營業活動之淨現金流入_流出"],
        "歸屬母公司淨利": f["歸屬母公司淨利_損"],
        "折舊": f["折舊費用"],
        "流動資產": f["流動資產"],
        "流動負債": f["流動負債"],
        "取得不動產廠房及設備": f["取得不動產_廠房及設備"],
        "經常稅後淨利": f["繼續營業單位損益"],
        "ROA稅後息前": _pct(f["合併總損益"] + interest * (1 - tax_rate), avg_assets),
        "ROA綜合損益": _pct(f["本期綜合損益總額"], avg_assets),
        "ROE稅後": _pct(f["歸屬母公司淨利_損"], avg_parent_equity),
        "ROE綜合損益": _pct(f["綜合損益歸屬母公司"], avg_parent_equity),
        "稅前息前折舊前淨利率": _pct(ebitda_margin_num, revenue),
        "營業毛利率": _pct(f["營業毛利"], revenue),
        "營業利益率": _pct(f["營業利益"], revenue),
        "稅前淨利率": _pct(f["稅前淨利"], revenue),
        "稅後淨利率": _pct(f["合併總損益"], revenue),
        "業外收支營收率": _pct(f["營業外收入及支出"], revenue),
        "貝里比率": _pct(f["營業毛利"], f["營業費用"]),
        "營業費用率": _pct(f["營業費用"], revenue),
        "推銷費用率": _pct(f["推銷費用"], revenue),
        "管理費用率": _pct(f["管理費用"], revenue),
        "研究發展費用率": _pct(f["研究發展費"], revenue),
        "現金流量比率": _pct(f["營業活動之淨現金流入_流出"], f["流動負債"]),
        "稅率": _pct(f["所得稅費用"], f["稅前淨利"]),
        "每股營業額": _ratio(revenue, shares),
        "每股營業利益": _ratio(f["營業利益"], shares),
        "每股現金流量": _ratio(f["營業活動之淨現金流入_流出"], shares),
        "每股稅前淨利": _ratio(f["稅前淨利"], shares),
        "每股綜合損益": _ratio(f["綜合損益歸屬母公司"], shares),
        "每股稅後淨利": f["每股盈餘"],
        "總負債除總淨值": _pct(f["負債總額"], f["股東權益總額"]),
        "負債比率": _pct(f["負債總額"], f["資產總額"]),
        "淨值除資產": _pct(f["股東權益總額"], f["資產總額"]),
        "營收成長率": _yoy(revenue),
        "營業毛利成長率": _yoy(f["營業毛利"]),
        "營業利益成長率": _yoy(f["營業利益"]),
        "稅前淨利成長率": _yoy(f["稅前淨利"]),
        "稅後淨利成長率": _yoy(f["合併總損益"]),
        "經常利益成長率": _yoy(f["繼續營業單位損益"]),
        "資產總額成長率": _yoy(f["資產總額"]),
        "淨值成長率": _yoy(f["股東權益總額"]),
        "流動比率": _pct(f["流動資產"], f["流動負債"]),
        "速動比率": _pct(f["流動資產"] - f["存貨"].fillna(0), f["流動負債"]),
        "利息支出率": _pct(interest, revenue),
        "營運資金": f["流動資產"] - f["流動負債"],
        "總資產週轉次數": _ratio(revenue, avg_assets),
        "應收帳款週轉率": _ratio(revenue, _avg(f["應收帳款及票據"])),
        "存貨週轉率": _ratio(f["營業成本"], _avg(f["存貨"])),
        "固定資產週轉次數": _ratio(revenue, _avg(f["不動產廠房及設備"])),
        "淨值週轉率次數": _ratio(revenue, avg_equity),
        "自由現金流量": f["營業活動之淨現金流入_流出"] - capex,
    }


def derive(store: Any) -> dict[str, pd.DataFrame]:
    """Compute every Field's Wide Frame from the materialized statements."""
    inputs = _load_inputs(store)
    if inputs["營業收入淨額"].empty:
        raise DeriveError("financial_statement is materialized but empty")
    frames = _compute(inputs)
    missing = [name for name in FIELDS if name not in frames]
    if missing:
        raise DeriveError(f"formulas missing for {missing}")
    out = {}
    for name in FIELDS:
        frame = frames[name].copy()
        frame.index = pd.DatetimeIndex(frame.index, name="date")
        frame.columns.name = None
        out[name] = frame
    return out


SPECS = [
    DatasetSpec(
        name="fundamental_features",
        official_source="derived from financial_statement (MOPS IFRS statements)",
        # Recomputed the evening of each quarterly Statutory Deadline, after the
        # statements themselves (22:00).
        cadence=Cadence(kind="quarterly", at="23:00"),
        frequency="quarterly",
        fields=tuple(FIELDS),
        int_fields=frozenset(),
        key_fields=("stock_id", "date"),
        invariants=(),
        backfill_start=dt.date(2013, 3, 31),   # follows financial_statement
        derive=derive,
        depends_on=("financial_statement",),
    ),
]
