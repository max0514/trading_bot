"""twlab Pipelines panel for the Scraper Monitor tab.

One row per Registry Dataset with its latest Orchestrator outcome (from the
run log) and a Run button that triggers a single-Dataset run through the same
entry point the nightly job uses; "Run all due" runs the Orchestrator plan.
The legacy market-data buttons (Stock Price, Monthly Revenue, Quarterly
Report) are re-pointed at the twlab Datasets that supersede their FinMind
scrapers — see LEGACY_TO_TWLAB — and show the Orchestrator's outcome too;
News and PTT keep their legacy scrapers. Runs happen in background threads so
the dashboard stays responsive; the outcome shows up on the next status
refresh. The Orchestrator entry points are injectable so the logic is testable
without Dash or MongoDB.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Callable

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, html

from twlab import orchestrator, registry
from twlab.pipeline import PUBLISHED

logger = logging.getLogger(__name__)

RUN_ALL = "__all_due__"

# Legacy scraper-monitor buttons → the twlab Dataset that replaced the scraper.
LEGACY_TO_TWLAB = {
    "stock_price": "price",
    "monthly_revenue": "monthly_revenue",
    "quarterly_report": "financial_statement",
}

# Orchestrator status → (badge text, badge css class)
_BADGES = {
    "ok": ("Published", "badge badge-success"),
    "witness_ok": ("Witness OK", "badge badge-success"),
    "no_data": ("No data", "badge badge-idle"),
    "never": ("Never run", "badge badge-idle"),
    "quarantined": ("Quarantined", "badge badge-error"),
    "failed": ("Failed", "badge badge-error"),
    "witness_alert": ("Witness alert", "badge badge-error"),
    "unavailable": ("Mongo unavailable", "badge badge-error"),
    "running": ("Running", "badge badge-running"),
}


class PipelineTriggers:
    """Background runs + status for the dashboard; dependencies injectable."""

    def __init__(
        self,
        *,
        run_dataset: Callable[[str], Any] = orchestrator.run_dataset,
        run_due: Callable[[], list] = orchestrator.run_due,
        status: Callable[[], list[dict[str, Any]]] = orchestrator.status,
        names: Callable[[], list[str]] = registry.names,
    ):
        self._run_dataset = run_dataset
        self._run_due = run_due
        self._status = status
        self._names = names
        self._threads: dict[str, threading.Thread] = {}
        self._log: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def names(self) -> list[str]:
        return self._names()

    def is_running(self, name: str) -> bool:
        thread = self._threads.get(name)
        return thread is not None and thread.is_alive()

    def _add_log(self, dataset: str, message: str, level: str = "INFO") -> None:
        with self._lock:
            self._log.append({
                "timestamp": datetime.now().isoformat(),
                "dataset": dataset,
                "message": message,
                "level": level,
            })
            if len(self._log) > 500:
                self._log = self._log[-300:]

    def log(self, limit: int = 30) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._log[-limit:])

    def _describe(self, result: Any) -> str:
        text = f"{result.day}: {result.status}"
        if getattr(result, "rows", 0):
            text += f" ({result.rows} rows)"
        if getattr(result, "failures", None):
            text += " — " + "; ".join(result.failures)[:300]
        return text

    def run(self, name: str) -> bool:
        """Trigger one Dataset's run (the Orchestrator's manual path)."""
        if self.is_running(name) or self.is_running(RUN_ALL):
            self._add_log(name, "already running", level="WARN")
            return False

        def target() -> None:
            self._add_log(name, "run started")
            try:
                result = self._run_dataset(name)
                level = "INFO" if result.status in PUBLISHED else "ERROR"
                self._add_log(name, self._describe(result), level=level)
            except Exception as exc:  # noqa: BLE001 — surface, never crash the UI
                logger.exception("twlab run %s failed", name)
                self._add_log(name, f"failed: {type(exc).__name__}: {exc}", level="ERROR")

        thread = threading.Thread(target=target, daemon=True, name=f"twlab-{name}")
        self._threads[name] = thread
        thread.start()
        return True

    def run_due(self) -> bool:
        """Trigger the Orchestrator plan (everything due, catch-up included)."""
        if self.is_running(RUN_ALL):
            return False

        def target() -> None:
            self._add_log("orchestrator", "run-due started")
            try:
                results = self._run_due()
                if not results:
                    self._add_log("orchestrator", "nothing was due")
                for result in results:
                    level = "INFO" if result.status in PUBLISHED else "ERROR"
                    self._add_log(result.dataset, self._describe(result), level=level)
            except Exception as exc:  # noqa: BLE001
                logger.exception("twlab run-due failed")
                self._add_log("orchestrator", f"failed: {type(exc).__name__}: {exc}", level="ERROR")

        thread = threading.Thread(target=target, daemon=True, name="twlab-all")
        self._threads[RUN_ALL] = thread
        thread.start()
        return True

    def status(self) -> list[dict[str, Any]]:
        """Latest Orchestrator outcome per Dataset, with the running flag."""
        try:
            rows = self._status()
        except Exception as exc:  # noqa: BLE001 — Mongo down: show it, don't crash
            rows = [{"dataset": n, "cadence": "", "status": "unavailable", "day": None,
                     "rows": 0, "detail": [str(exc)[:200]], "last_ok_day": None}
                    for n in self.names()]
        for row in rows:
            row["running"] = self.is_running(row["dataset"]) or self.is_running(RUN_ALL)
        return rows


# ── layout ────────────────────────────────────────────────────────────────

def _row(name: str) -> dbc.Row:
    return dbc.Row([
        dbc.Col([
            html.Span(name, className="fw-semibold"),
            html.Small(id=f"twlab-cadence-{name}", className="text-muted ms-2"),
        ], width=5),
        dbc.Col([
            html.Span(id=f"twlab-badge-{name}", className="badge badge-idle"),
            html.Small(id=f"twlab-day-{name}", className="text-muted ms-2"),
        ], width=5),
        dbc.Col(
            dbc.Button("Run", id=f"btn-twlab-run-{name}", color="primary", size="sm"),
            width=2, className="text-end",
        ),
    ], align="center", className="mb-2")


def twlab_card(runner: PipelineTriggers) -> dbc.Card:
    return dbc.Card([
        dbc.CardHeader([
            "twlab Pipelines",
            html.Small(" — official-source Datasets, Orchestrator run log",
                       className="text-muted"),
            dbc.Button("Run all due", id="btn-twlab-run-due", color="success",
                       size="sm", className="float-end"),
        ]),
        dbc.CardBody(
            [_row(name) for name in runner.names()]
            + [html.Hr(), html.Div(id="twlab-log", className="log-panel")]
        ),
    ])


def badge_for(row: dict[str, Any]) -> tuple[str, str]:
    if row.get("running"):
        return _BADGES["running"]
    return _BADGES.get(row["status"], (row["status"], "badge badge-idle"))


def legacy_row_status(rows_by_dataset: dict[str, dict[str, Any]], legacy_name: str) -> tuple[str, str, str]:
    """(badge text, badge class, progress text) for a re-pointed legacy button."""
    row = rows_by_dataset.get(LEGACY_TO_TWLAB[legacy_name])
    if row is None:
        return _BADGES["never"] + ("",)
    text, css = badge_for(row)
    return text, css, day_text(row)


def day_text(row: dict[str, Any]) -> str:
    parts = []
    if row.get("day"):
        parts.append(str(row["day"]))
    if row.get("rows"):
        parts.append(f"{row['rows']} rows")
    if row.get("detail"):
        parts.append(str(row["detail"][0])[:80])
    return " · ".join(parts)


def register_callbacks(runner: PipelineTriggers) -> None:
    names = runner.names()

    @callback(
        [Output(f"twlab-badge-{n}", "children") for n in names]
        + [Output(f"twlab-badge-{n}", "className") for n in names]
        + [Output(f"twlab-day-{n}", "children") for n in names]
        + [Output(f"twlab-cadence-{n}", "children") for n in names]
        + [Output("twlab-log", "children")],
        Input("interval-refresh", "n_intervals"),
    )
    def refresh(_n):
        rows = {r["dataset"]: r for r in runner.status()}
        badges = [badge_for(rows[n]) for n in names]
        log_children = [
            html.Div([
                html.Span(e["timestamp"][:19], className="log-time"),
                html.Span(f"[{e['dataset']}]", className="log-scraper"),
                html.Span(e["message"], className="log-message"),
            ], className="log-entry log-error" if e["level"] == "ERROR" else "log-entry")
            for e in reversed(runner.log())
        ]
        return (
            [b[0] for b in badges] + [b[1] for b in badges]
            + [day_text(rows[n]) for n in names]
            + [rows[n].get("cadence", "") for n in names]
            + [log_children]
        )

    for _name in names:
        @callback(
            Output("store-dummy", "data", allow_duplicate=True),
            Input(f"btn-twlab-run-{_name}", "n_clicks"),
            prevent_initial_call=True,
        )
        def run_one(n_clicks, dataset=_name):
            if n_clicks:
                runner.run(dataset)
            return dash.no_update

    @callback(
        Output("store-dummy", "data", allow_duplicate=True),
        Input("btn-twlab-run-due", "n_clicks"),
        prevent_initial_call=True,
    )
    def run_all(n_clicks):
        if n_clicks:
            runner.run_due()
        return dash.no_update
