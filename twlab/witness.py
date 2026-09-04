"""The Witness: FinMind cross-checks of published data — alert only, never collect.

A systematically wrong parse (a 千元 column read as 元, a shifted column)
passes structural Invariants but not a comparison with an independent source.
Each Probe maps one of our Data Keys onto a FinMind dataset/column, including
the unit scale and the date convention gap (our Statutory-Deadline index vs
FinMind's period dating). A Probe on a derived ratio has no single column to
read, so it recomputes the ratio from FinMind's own line items (`combine`) —
which is what makes it evidence rather than a restatement of our own parse. Samples are drawn from recent rows of the published
Wide Frame; outcomes are logged as `RunStatus.WITNESS_OK` / `WITNESS_ALERT` runs.
"""
from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd

from twlab import registry
from twlab.errors import DatasetNotMaterializedError
from twlab.spec import quarter_due_on, quarter_end
from twlab.status import RunStatus
from twlab.store.mongo import MongoStore
from twlab.store.parquet import ParquetStore

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"


@dataclass(frozen=True)
class Probe:
    dataset: str
    field: str
    finmind_dataset: str
    finmind_value: str                             # column holding the value in a FinMind row
    scale: float                                   # FinMind value × scale == our unit
    finmind_date: Callable[[pd.Timestamp], str]    # our index date → FinMind row date
    tolerance: float = 1e-6                        # relative
    row_filter: dict[str, Any] | None = None       # only rows matching these fields count
    value: Callable[[dict[str, Any]], float | None] | None = None   # custom extraction
    # Our derived ratios have no single FinMind row to read: the Witness has to
    # recompute them from the line items FinMind publishes for that date. When
    # set, this is handed {line item name: value} for one date instead.
    combine: Callable[[dict[str, float]], float | None] | None = None
    finmind_type: str = "type"                     # column naming the line item


def _same_day(d: pd.Timestamp) -> str:
    return d.strftime("%Y-%m-%d")


def _month_start(d: pd.Timestamp) -> str:
    # FinMind dates month-M revenue on the 1st of month M+1; our index is the
    # 10th of month M+1 (the Statutory Deadline) — same month, day 1.
    return d.strftime("%Y-%m-01")


def _quarter_end_of_deadline(d: pd.Timestamp) -> str:
    # Our quarterly index is the Statutory Deadline; FinMind dates statements
    # by the quarter end they describe.
    due = quarter_due_on(d.date())
    if due is None:                                # not a deadline: previous quarter end
        month = ((d.month - 1) // 3) * 3
        return quarter_end(d.year if month else d.year - 1, month // 3 or 4).strftime("%Y-%m-%d")
    return quarter_end(*due).strftime("%Y-%m-%d")


def _net(row: dict[str, Any]) -> float:
    return float(row.get("buy", 0)) - float(row.get("sell", 0))


def _ratio(numerator: str, denominator: str) -> Callable[[dict[str, float]], float | None]:
    """A percentage rebuilt from two of FinMind's own statement line items.

    This is what makes a `fundamental_features` probe worth having: our ratio
    is computed from our parse of MOPS, theirs from their parse of the same
    filing, and the units cancel — so agreement is evidence about the whole
    chain (MOPS page → Fields → derived ratio), not a restatement of it.
    """

    def combine(items: dict[str, float]) -> float | None:
        top, bottom = items.get(numerator), items.get(denominator)
        if top is None or not bottom:
            return None
        return 100.0 * top / bottom

    return combine


FINMIND_STATEMENTS = "TaiwanStockFinancialStatements"


PROBES: list[Probe] = [
    Probe("price", "收盤價", "TaiwanStockPrice", "close", 1.0, _same_day),
    Probe("price", "成交股數", "TaiwanStockPrice", "Trading_Volume", 1.0, _same_day),
    Probe("monthly_revenue", "當月營收", "TaiwanStockMonthRevenue", "revenue", 1 / 1000, _month_start),
    Probe("price_earning_ratio", "本益比", "TaiwanStockPER", "PER", 1.0, _same_day, tolerance=0.01),
    Probe("institutional_investors_trading_summary", "投信買賣超股數",
          "TaiwanStockInstitutionalInvestorsBuySell", "buy", 1.0, _same_day,
          row_filter={"name": "Investment_Trust"}, value=_net),
    # The 千元-vs-元 class of error on statements: a point-in-time balance.
    Probe("financial_statement", "資產總額", "TaiwanStockBalanceSheet", "value", 1 / 1000,
          _quarter_end_of_deadline, tolerance=1e-4, row_filter={"type": "TotalAssets"}),
    # Derived ratios (twlab 13) against FinMind's independent calculation from
    # the same filing. FinMind serves single-quarter statement values, which is
    # the convention `financial_statement` de-cumulates to, so the periods line
    # up without adjustment.
    Probe("fundamental_features", "每股稅後淨利", FINMIND_STATEMENTS, "value", 1.0,
          _quarter_end_of_deadline, tolerance=1e-4, row_filter={"type": "EPS"}),
    Probe("fundamental_features", "營業毛利率", FINMIND_STATEMENTS, "value", 1.0,
          _quarter_end_of_deadline, tolerance=1e-4,
          combine=_ratio("GrossProfit", "Revenue")),
    Probe("fundamental_features", "營業利益率", FINMIND_STATEMENTS, "value", 1.0,
          _quarter_end_of_deadline, tolerance=1e-4,
          combine=_ratio("OperatingIncome", "Revenue")),
    Probe("fundamental_features", "營業利益", FINMIND_STATEMENTS, "value", 1 / 1000,
          _quarter_end_of_deadline, tolerance=1e-4, row_filter={"type": "OperatingIncome"}),
]


class FinMindClient:
    """Thin FinMind v4 client over a (polite) session; token optional."""

    def __init__(self, session: Any, token: str | None = None):
        self._session = session
        self._token = token

    def rows(self, dataset: str, stock_id: str, start: str, end: str) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "dataset": dataset, "data_id": stock_id, "start_date": start, "end_date": end,
        }
        if self._token:
            params["token"] = self._token
        payload = self._session.get_json(FINMIND_URL, params=params)
        if not isinstance(payload, dict) or payload.get("status") != 200:
            return []
        return list(payload.get("data") or [])


@dataclass(frozen=True)
class Mismatch:
    dataset: str
    field: str
    stock_id: str
    date: str
    ours: float
    witness: float


@dataclass
class WitnessReport:
    dataset: str
    field: str
    checked: int = 0
    missing: int = 0                    # cells the Witness had no value for
    mismatches: list[Mismatch] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.mismatches

    def summary(self) -> str:
        head = (f"{self.dataset}:{self.field}: {self.checked} checked, "
                f"{self.missing} unanswered, {len(self.mismatches)} mismatches")
        lines = [f"  {m.stock_id} {m.date}: ours={m.ours:g} witness={m.witness:g}"
                 for m in self.mismatches[:5]]
        return "\n".join([head, *lines])


def _witness_value(row: dict[str, Any], probe: Probe) -> float | None:
    if probe.row_filter and any(row.get(k) != v for k, v in probe.row_filter.items()):
        return None                                # long-form dataset: another line item
    if probe.value is not None:
        return probe.value(row)
    value = row.get(probe.finmind_value)
    return None if value is None else float(value)


def _answers(rows: list[dict[str, Any]], probe: Probe,
             wanted: dict[str, pd.Timestamp]) -> dict[str, float]:
    """The Witness's value for each wanted date — read off one row, or, for a
    ratio probe, recomputed from every line item FinMind has for that date."""
    if probe.combine is None:
        answers = {}
        for row in rows:
            value = _witness_value(row, probe)
            if value is not None and str(row.get("date")) in wanted:
                answers[str(row.get("date"))] = value
        return answers

    items: dict[str, dict[str, float]] = {}
    for row in rows:
        date, value = str(row.get("date")), row.get(probe.finmind_value)
        if date in wanted and value is not None:
            items.setdefault(date, {})[str(row.get(probe.finmind_type))] = float(value)
    combined = {date: probe.combine(by_item) for date, by_item in items.items()}
    return {date: value for date, value in combined.items() if value is not None}


def _sample_cells(frame: pd.DataFrame, n: int, rng: random.Random,
                  recent_rows: int = 60) -> list[tuple[pd.Timestamp, str]]:
    recent = frame.tail(recent_rows)
    stacked = recent.stack(future_stack=True).dropna()
    if stacked.empty:
        return []
    cells = list(stacked.index)
    rng.shuffle(cells)
    return [(pd.Timestamp(d), str(s)) for d, s in cells[:n]]


def check(probe: Probe, store: ParquetStore, client: FinMindClient, *,
          samples: int = 20, rng: random.Random | None = None) -> WitnessReport | None:
    """Cross-check `samples` recent cells of one Data Key; None if unmaterialized."""
    try:
        frame = store.read_frame(probe.dataset, probe.field)
    except DatasetNotMaterializedError:
        return None
    rng = rng or random.Random()
    report = WitnessReport(probe.dataset, probe.field)
    cells = _sample_cells(frame, samples, rng)
    by_stock: dict[str, list[pd.Timestamp]] = {}
    for date, stock_id in cells:
        by_stock.setdefault(stock_id, []).append(date)
    for stock_id, dates in by_stock.items():
        wanted = {probe.finmind_date(d): d for d in dates}
        start, end = min(wanted), max(wanted)
        answers = _answers(client.rows(probe.finmind_dataset, stock_id, start, end),
                           probe, wanted)
        for fm_date, our_date in wanted.items():
            report.checked += 1
            if fm_date not in answers:
                report.missing += 1
                continue
            ours = float(frame.loc[our_date, stock_id])
            theirs = answers[fm_date] * probe.scale
            if abs(ours - theirs) > probe.tolerance * max(1.0, abs(ours)):
                report.mismatches.append(Mismatch(
                    probe.dataset, probe.field, stock_id, _same_day(our_date), ours, theirs))
    return report


def run_witness(store: ParquetStore, mongo: MongoStore, client: FinMindClient, *,
                now: dt.datetime, probes: list[Probe] | None = None,
                only: str | None = None, samples: int = 20,
                seed: int | None = None) -> list[WitnessReport]:
    """Run every Probe whose Dataset is materialized; log one run per Dataset."""
    rng = random.Random(seed)
    reports: list[WitnessReport] = []
    for probe in (probes if probes is not None else PROBES):
        if only is not None and probe.dataset != only:
            continue
        report = check(probe, store, client, samples=samples, rng=rng)
        if report is not None:
            reports.append(report)

    # One run record per Dataset, combining every probe on it.
    by_dataset: dict[str, list[WitnessReport]] = {}
    for report in reports:
        by_dataset.setdefault(report.dataset, []).append(report)
    for dataset, group in by_dataset.items():
        try:
            spec = registry.get_spec(dataset)
        except KeyError:
            continue
        mismatches = [m for r in group for m in r.mismatches]
        status = RunStatus.WITNESS_OK if not mismatches else RunStatus.WITNESS_ALERT
        detail = [f"{m.dataset}:{m.field} {m.stock_id} {m.date}: "
                  f"ours={m.ours:g} witness={m.witness:g}" for m in mismatches[:10]]
        missing = sum(r.missing for r in group)
        checked = sum(r.checked for r in group)
        if missing:
            detail.append(f"{missing} of {checked} samples unanswered by the Witness")
        mongo.record_run(spec, now.date(), status, now, rows=checked, detail=detail)
    return reports
