"""Dashboard triggers (twlab 11): a button runs one Dataset through the
Orchestrator's manual path in the background and the panel reflects the
outcome from the run log — without Dash or MongoDB in the loop."""
import threading
import time

import pytest

from dashboard.twlab_panel import TwlabRunner, badge_for, day_text
from twlab.pipeline import RunResult


def wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def make_runner(outcomes: dict, status_rows=None, names=("price", "monthly_revenue")):
    started = threading.Event()
    release = threading.Event()

    def run_dataset(name):
        started.set()
        release.wait(timeout=5)
        outcome = outcomes[name]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def run_due():
        release.wait(timeout=5)
        return list(outcomes.values())

    def status():
        if isinstance(status_rows, Exception):
            raise status_rows
        return [dict(r) for r in (status_rows or [])]

    runner = TwlabRunner(run_dataset=run_dataset, run_due=run_due, status=status,
                         names=lambda: list(names))
    return runner, started, release


BASE_ROWS = [
    {"dataset": "price", "cadence": "daily", "status": "ok", "day": "2026-08-07",
     "rows": 1389, "detail": [], "last_ok_day": "2026-08-07"},
    {"dataset": "monthly_revenue", "cadence": "monthly", "status": "never", "day": None,
     "rows": 0, "detail": [], "last_ok_day": None},
]


def test_button_triggers_a_single_dataset_run_and_logs_its_outcome():
    runner, started, release = make_runner(
        {"price": RunResult("price", None, "ok", rows=1389)}, BASE_ROWS)

    assert runner.run("price") is True
    assert started.wait(timeout=5)
    assert runner.status()[0]["running"] is True          # reflected while in flight
    assert runner.run("price") is False                    # no double-runs
    release.set()

    assert wait_until(lambda: not runner.is_running("price"))
    messages = [e["message"] for e in runner.log()]
    assert messages[0] == "run started"
    assert "ok (1389 rows)" in messages[-1]
    assert runner.status()[0]["running"] is False


def test_quarantined_outcome_is_logged_as_an_error():
    runner, started, release = make_runner({
        "price": RunResult("price", None, "quarantined", rows=3,
                           failures=["min_rows: only 3 rows, expected at least 500"]),
    }, BASE_ROWS)
    release.set()
    runner.run("price")
    assert wait_until(lambda: not runner.is_running("price"))
    last = runner.log()[-1]
    assert last["level"] == "ERROR"
    assert "min_rows" in last["message"]


def test_exceptions_are_surfaced_not_raised_into_the_ui():
    runner, _, release = make_runner({"price": ConnectionError("TWSE unreachable")}, BASE_ROWS)
    release.set()
    runner.run("price")
    assert wait_until(lambda: not runner.is_running("price"))
    last = runner.log()[-1]
    assert last["level"] == "ERROR" and "TWSE unreachable" in last["message"]


def test_run_all_due_runs_the_orchestrator_plan():
    runner, _, release = make_runner({
        "price": RunResult("price", None, "ok", rows=10),
        "monthly_revenue": RunResult("monthly_revenue", None, "no_data"),
    }, BASE_ROWS)
    release.set()
    assert runner.run_due() is True
    assert wait_until(lambda: not runner.is_running("__all_due__"))
    datasets = [e["dataset"] for e in runner.log()]
    assert datasets == ["orchestrator", "price", "monthly_revenue"]


def test_status_reflects_the_orchestrator_log_and_mongo_outage():
    runner, *_ = make_runner({}, BASE_ROWS)
    rows = {r["dataset"]: r for r in runner.status()}
    assert badge_for(rows["price"]) == ("Published", "badge badge-success")
    assert day_text(rows["price"]) == "2026-08-07 · 1389 rows"
    assert badge_for(rows["monthly_revenue"]) == ("Never run", "badge badge-idle")

    down, *_ = make_runner({}, RuntimeError("ServerSelectionTimeoutError: mongo:27017"))
    rows = {r["dataset"]: r for r in down.status()}
    assert badge_for(rows["price"])[0] == "Mongo unavailable"
    assert "mongo:27017" in day_text(rows["price"])


def test_quarantine_detail_is_visible_in_the_panel():
    row = {"dataset": "price", "cadence": "daily", "status": "quarantined", "day": "2026-08-08",
           "rows": 3, "detail": ["min_rows: only 3 rows, expected at least 500"], "running": False}
    assert badge_for(row) == ("Quarantined", "badge badge-error")
    assert "min_rows" in day_text(row)
