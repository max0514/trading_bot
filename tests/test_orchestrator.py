"""The Orchestrator (twlab 07): due-ness from Cadence + a frozen clock,
catch-up of missed windows, per-dataset run status, and failure isolation.

Planning is asserted directly (pure, given the run log); execution is asserted
through the run log and through what the pipeline publishes.
"""
import datetime as dt

import pytest

from twlab import orchestrator, qa
from twlab.pipeline import RunResult
from twlab.spec import Cadence, DatasetSpec


def fake_spec(name, kind="daily", at="21:32", derived=False, depends_on=()):
    return DatasetSpec(
        name=name, official_source="test", cadence=Cadence(kind=kind, at=at),
        frequency={"daily": "daily", "monthly": "monthly", "quarterly": "quarterly"}[kind],
        fields=("x",), int_fields=frozenset(), key_fields=("stock_id", "date"),
        invariants=(qa.required_columns(["stock_id"]),), backfill_start=dt.date(2020, 1, 1),
        fetch=None if derived else (lambda session, day: []),
        parse=None if derived else (lambda raw: None),
        derive=(lambda store: {}) if derived else None,
        depends_on=depends_on,
    )


DAILY = fake_spec("daily_ds")
MONTHLY = fake_spec("monthly_ds", kind="monthly", at="22:00")
QUARTERLY = fake_spec("quarterly_ds", kind="quarterly", at="22:00")
DERIVED = fake_spec("derived_ds", at="22:30", derived=True, depends_on=("daily_ds",))


def at(day: str, hm: str = "23:00") -> dt.datetime:
    return dt.datetime.fromisoformat(f"{day}T{hm}")


def plan(now, specs, mongo, **kw):
    return [(d.dataset, d.day) for d in orchestrator.plan(now, mongo, specs=specs, **kw)]


class TestDailyDueness:
    def test_not_due_before_the_publication_window(self, mongo):
        mongo.record_run(DAILY, dt.date(2026, 8, 6), "ok", at("2026-08-06"))
        assert plan(at("2026-08-07", "21:00"), {"daily_ds": DAILY}, mongo) == []

    def test_due_once_the_window_has_passed(self, mongo):
        mongo.record_run(DAILY, dt.date(2026, 8, 6), "ok", at("2026-08-06"))
        assert plan(at("2026-08-07", "21:32"), {"daily_ds": DAILY}, mongo) == [
            ("daily_ds", dt.date(2026, 8, 7))
        ]

    def test_catches_up_every_missed_day_since_the_last_good_run(self, mongo):
        mongo.record_run(DAILY, dt.date(2026, 8, 4), "ok", at("2026-08-04"))
        assert plan(at("2026-08-07"), {"daily_ds": DAILY}, mongo) == [
            ("daily_ds", dt.date(2026, 8, 5)),
            ("daily_ds", dt.date(2026, 8, 6)),
            ("daily_ds", dt.date(2026, 8, 7)),
        ]

    def test_days_already_run_are_skipped_but_quarantined_days_are_retried(self, mongo):
        mongo.record_run(DAILY, dt.date(2026, 8, 4), "ok", at("2026-08-04"))
        mongo.record_run(DAILY, dt.date(2026, 8, 5), "no_data", at("2026-08-05"))   # holiday
        mongo.record_run(DAILY, dt.date(2026, 8, 6), "quarantined", at("2026-08-06"))
        assert plan(at("2026-08-07"), {"daily_ds": DAILY}, mongo) == [
            ("daily_ds", dt.date(2026, 8, 6)),
            ("daily_ds", dt.date(2026, 8, 7)),
        ]

    def test_catch_up_is_capped_to_the_most_recent_due_days(self, mongo):
        mongo.record_run(DAILY, dt.date(2026, 5, 1), "ok", at("2026-05-01"))
        days = [d for _, d in plan(at("2026-08-07"), {"daily_ds": DAILY}, mongo, max_catchup=3)]
        assert days == [dt.date(2026, 8, 5), dt.date(2026, 8, 6), dt.date(2026, 8, 7)]

    def test_fresh_install_only_collects_the_latest_due_day(self, mongo):
        assert plan(at("2026-08-07"), {"daily_ds": DAILY}, mongo) == [
            ("daily_ds", dt.date(2026, 8, 7))
        ]


class TestMonthlyAndQuarterlyDueness:
    def test_monthly_is_due_on_the_statutory_deadline_only(self, mongo):
        mongo.record_run(MONTHLY, dt.date(2026, 7, 10), "ok", at("2026-07-10"))
        assert plan(at("2026-08-09"), {"monthly_ds": MONTHLY}, mongo) == []
        assert plan(at("2026-08-10", "21:00"), {"monthly_ds": MONTHLY}, mongo) == []
        assert plan(at("2026-08-10", "22:00"), {"monthly_ds": MONTHLY}, mongo) == [
            ("monthly_ds", dt.date(2026, 8, 10))
        ]

    def test_monthly_catches_up_a_missed_deadline(self, mongo):
        mongo.record_run(MONTHLY, dt.date(2026, 6, 10), "ok", at("2026-06-10"))
        assert plan(at("2026-08-10"), {"monthly_ds": MONTHLY}, mongo) == [
            ("monthly_ds", dt.date(2026, 7, 10)),
            ("monthly_ds", dt.date(2026, 8, 10)),
        ]

    def test_quarterly_is_due_on_each_deadline(self, mongo):
        mongo.record_run(QUARTERLY, dt.date(2026, 3, 31), "ok", at("2026-03-31"))
        assert plan(at("2026-05-14"), {"quarterly_ds": QUARTERLY}, mongo) == []
        assert plan(at("2026-08-14"), {"quarterly_ds": QUARTERLY}, mongo) == [
            ("quarterly_ds", dt.date(2026, 5, 15)),      # missed Q1 deadline caught up
            ("quarterly_ds", dt.date(2026, 8, 14)),
        ]

    def test_fresh_install_finds_the_latest_deadline_within_the_cadence(self, mongo):
        assert plan(at("2026-09-03"), {"monthly_ds": MONTHLY}, mongo) == [
            ("monthly_ds", dt.date(2026, 8, 10))
        ]
        assert plan(at("2026-09-03"), {"quarterly_ds": QUARTERLY}, mongo) == [
            ("quarterly_ds", dt.date(2026, 8, 14))
        ]


class TestDerivedAndOrdering:
    def test_derived_datasets_run_after_their_dependencies_for_today_only(self, mongo):
        mongo.record_run(DAILY, dt.date(2026, 8, 4), "ok", at("2026-08-04"))
        specs = {"derived_ds": DERIVED, "daily_ds": DAILY}
        p = plan(at("2026-08-07"), specs, mongo)
        assert p[-1] == ("derived_ds", dt.date(2026, 8, 7))
        assert p[:-1] == [("daily_ds", dt.date(2026, 8, d)) for d in (5, 6, 7)]

    def test_derived_not_due_before_its_window_or_when_already_derived(self, mongo):
        specs = {"derived_ds": DERIVED}
        assert plan(at("2026-08-07", "22:00"), specs, mongo) == []
        mongo.record_run(DERIVED, dt.date(2026, 8, 7), "ok", at("2026-08-07", "22:31"))
        assert plan(at("2026-08-07", "23:00"), specs, mongo) == []

    def test_single_dataset_filter(self, mongo):
        specs = {"daily_ds": DAILY, "monthly_ds": MONTHLY}
        assert plan(at("2026-08-10"), specs, mongo, only="daily_ds") == [
            ("daily_ds", dt.date(2026, 8, 10))
        ]


class TestExecution:
    def test_runs_the_plan_and_isolates_failures(self, mongo):
        specs = {"daily_ds": DAILY, "monthly_ds": MONTHLY}
        calls = []

        def runner(dataset, day):
            calls.append((dataset, day))
            if dataset == "daily_ds":
                raise ConnectionError("TWSE unreachable")
            mongo.record_run(specs[dataset], day, "ok", at("2026-08-10"), rows=10)  # as the pipeline does
            return RunResult(dataset, day, "ok", rows=10)

        results = orchestrator.run_due(at("2026-08-10"), mongo=mongo, specs=specs, runner=runner)

        assert calls == [("daily_ds", dt.date(2026, 8, 10)), ("monthly_ds", dt.date(2026, 8, 10))]
        assert [r.status for r in results] == ["failed", "ok"]
        assert "TWSE unreachable" in results[0].failures[0]
        # the failed run is visible in the log and will be retried next night
        assert mongo.runs("daily_ds")[0]["status"] == "failed"
        assert plan(at("2026-08-11", "12:00"), specs, mongo, max_catchup=5) == [
            ("daily_ds", dt.date(2026, 8, 10)),
        ]

    def test_status_shows_the_latest_outcome_per_dataset(self, mongo):
        specs = {"daily_ds": DAILY, "monthly_ds": MONTHLY, "quarterly_ds": QUARTERLY}
        mongo.record_run(DAILY, dt.date(2026, 8, 6), "ok", at("2026-08-06"), rows=1400)
        mongo.record_run(DAILY, dt.date(2026, 8, 7), "quarantined", at("2026-08-07"),
                         rows=3, detail=["min_rows: only 3 rows, expected at least 500"])
        mongo.record_run(MONTHLY, dt.date(2026, 8, 10), "ok", at("2026-08-10"), rows=1800)

        status = {s["dataset"]: s for s in orchestrator.status(mongo, specs=specs)}

        assert status["daily_ds"]["status"] == "quarantined"
        assert status["daily_ds"]["day"] == dt.date(2026, 8, 7)
        assert status["daily_ds"]["last_ok_day"] == dt.date(2026, 8, 6)
        assert "min_rows" in status["daily_ds"]["detail"][0]
        assert status["monthly_ds"]["status"] == "ok"
        assert status["quarterly_ds"]["status"] == "never"

    def test_backfill_walks_every_due_day_in_range_and_skips_done_days(self, mongo):
        mongo.record_run(MONTHLY, dt.date(2026, 6, 10), "ok", at("2026-06-10"))
        calls = []
        results = orchestrator.backfill(
            "monthly_ds", dt.date(2026, 4, 1), dt.date(2026, 8, 31), mongo=mongo,
            specs={"monthly_ds": MONTHLY},
            runner=lambda d, day: calls.append(day) or RunResult(d, day, "ok", rows=1),
        )
        assert calls == [dt.date(2026, 4, 10), dt.date(2026, 5, 10),
                         dt.date(2026, 7, 10), dt.date(2026, 8, 10)]
        assert all(r.status == "ok" for r in results)


class TestRealPipelineRunner:
    def test_default_runner_publishes_through_the_pipeline(self, good_session, mongo, store_env):
        from twlab import data, registry
        from twlab.store.parquet import ParquetStore

        results = orchestrator.run_due(
            at("2026-08-07", "21:32"), mongo=mongo, specs={"price": registry.get_spec("price")},
            session=good_session, store=ParquetStore(store_env),
        )
        assert [(r.dataset, r.status) for r in results] == [("price", "ok")]
        assert data.get("price:收盤價").loc["2026-08-07", "2330"] == 2370.0
