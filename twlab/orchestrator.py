"""The Orchestrator: walks the Registry and runs every Dataset that is due.

One command (`python -m twlab.orchestrator run`) computes due-ness from each
Registry entry's Cadence and an injected clock, catches up windows missed
inside a bounded window (including Quarantined or failed days that a later
good day would otherwise hide), runs each due batch through the pipeline in
isolation (one failure never stops the others), and records the outcome in
the run log that `status` and the dashboard read. Derived Datasets are
planned after the scraped ones have run and are due whenever one of their
inputs has published since their last derivation. Manual re-runs
(`run --dataset X`, or `run_dataset()` from the dashboard) and `backfill`
use the same entry point. Scheduled nightly by cron inside Docker Compose.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from twlab import config, pipeline, registry
from twlab.http import PoliteSession
from twlab.pipeline import PUBLISHED, RunResult
from twlab.spec import DatasetSpec
from twlab.store.mongo import MongoStore
from twlab.store.parquet import ParquetStore

logger = logging.getLogger(__name__)

DEFAULT_MAX_CATCHUP = 14          # due days per Dataset per run; older gaps are `backfill`
# How far back a fresh install looks for the latest due day of each Cadence.
_FRESH_LOOKBACK_DAYS = {"daily": 7, "monthly": 45, "quarterly": 400}
# How far back an installed Dataset re-checks for unpublished due days
# (missed, Quarantined, or failed) on every run; older gaps are `backfill`.
_CATCHUP_WINDOW_DAYS = {"daily": 45, "monthly": 130, "quarterly": 400}

RunFn = Callable[[str, dt.date], RunResult]


@dataclass(frozen=True)
class Due:
    dataset: str
    day: dt.date


def _days(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    day = start
    while day <= end:
        yield day
        day += dt.timedelta(days=1)


def _inputs_published_since_last_derive(spec: DatasetSpec, mongo: MongoStore) -> bool:
    """A derived Dataset is due when any input published after its last derivation."""
    inputs = [t for d in spec.depends_on if (t := mongo.last_ok_at(d)) is not None]
    if not inputs:
        return False
    mine = mongo.last_ok_at(spec.name)
    return mine is None or max(inputs) > mine


def _due_days(spec: DatasetSpec, now: dt.datetime, mongo: MongoStore,
              max_catchup: int) -> list[dt.date]:
    today = now.date()
    if spec.is_derived:
        # Recomputed from the whole store, so only "now" is a meaningful batch
        # day; due whenever an input has published since the last derivation.
        return [today] if _inputs_published_since_last_derive(spec, mongo) else []
    last_ok = mongo.last_ok_day(spec.name)
    if last_ok is None:
        start = today - dt.timedelta(days=_FRESH_LOOKBACK_DAYS[spec.cadence.kind])
    else:
        # Re-check the whole window, not just days after the last good run:
        # a Quarantined or failed day behind a later good day must be retried.
        start = today - dt.timedelta(days=_CATCHUP_WINDOW_DAYS[spec.cadence.kind])
        first = mongo.first_run_day(spec.name)
        if first is not None:
            start = max(start, first)
    due = [
        day for day in _days(start, today)
        if spec.cadence.is_due_day(day)
        and spec.cadence.due_at(day) <= now
        and not mongo.has_run(spec.name, day, statuses=PUBLISHED)   # others retried
    ]
    if last_ok is None:
        due = due[-1:]                                  # never backfill by accident
    return due[-max_catchup:]


def _ordered(specs: dict[str, DatasetSpec]) -> list[DatasetSpec]:
    """Scraped Datasets first (Registry order), then derived ones after their inputs."""
    scraped = [s for s in specs.values() if not s.is_derived]
    derived = [s for s in specs.values() if s.is_derived]
    ordered: list[DatasetSpec] = list(scraped)
    placed = {s.name for s in scraped}
    remaining = list(derived)
    while remaining:
        progress = False
        for spec in list(remaining):
            deps_in_scope = [d for d in spec.depends_on if d in specs and specs[d].is_derived]
            if all(d in placed for d in deps_in_scope):
                ordered.append(spec)
                placed.add(spec.name)
                remaining.remove(spec)
                progress = True
        if not progress:                                # cycle: append the rest as-is
            ordered.extend(remaining)
            break
    return ordered


def plan(now: dt.datetime, mongo: MongoStore, *,
         specs: dict[str, DatasetSpec] | None = None,
         only: str | None = None,
         max_catchup: int = DEFAULT_MAX_CATCHUP,
         derived: bool | None = None) -> list[Due]:
    """Every (Dataset, batch day) due at `now` that has not been published yet.

    `derived=False` plans only scraped Datasets, `True` only derived ones
    (evaluated against the run log as it stands), `None` both.
    """
    specs = specs if specs is not None else registry.all_specs()
    if only is not None:
        specs = {only: specs[only]}
    if derived is not None:
        specs = {n: s for n, s in specs.items() if s.is_derived == derived}
    return [
        Due(spec.name, day)
        for spec in _ordered(specs)
        for day in _due_days(spec, now, mongo, max_catchup)
    ]


def default_runner(*, session: PoliteSession | None, mongo: MongoStore,
                   store: ParquetStore, now: dt.datetime) -> RunFn:
    session = session or PoliteSession()

    def run(dataset: str, day: dt.date) -> RunResult:
        return pipeline.run(dataset, day, session=session, mongo=mongo, store=store, now=now)

    return run


def execute(due: list[Due], runner: RunFn, mongo: MongoStore,
            specs: dict[str, DatasetSpec], now: dt.datetime) -> list[RunResult]:
    """Run each due batch; a raised exception becomes a recorded `failed` run."""
    results: list[RunResult] = []
    for item in due:
        try:
            result = runner(item.dataset, item.day)
        except Exception as exc:  # noqa: BLE001 — isolate every Dataset from the others
            logger.exception("%s %s: failed", item.dataset, item.day)
            detail = f"{type(exc).__name__}: {exc}"
            mongo.record_run(specs[item.dataset], item.day, "failed", now, detail=[detail])
            result = RunResult(item.dataset, item.day, "failed", failures=[detail])
        results.append(result)
        logger.info("%s %s: %s%s", result.dataset, result.day, result.status,
                    f" ({result.rows} rows)" if result.rows else "")
    return results


def run_due(now: dt.datetime | None = None, *,
            mongo: MongoStore | None = None,
            store: ParquetStore | None = None,
            session: PoliteSession | None = None,
            specs: dict[str, DatasetSpec] | None = None,
            only: str | None = None,
            max_catchup: int = DEFAULT_MAX_CATCHUP,
            runner: RunFn | None = None) -> list[RunResult]:
    """The nightly entry point: scraped Datasets first, then the derived
    ones whose inputs just published."""
    now = now or dt.datetime.now()
    mongo = mongo or MongoStore()
    store = store or ParquetStore(config.store_dir())
    specs = specs if specs is not None else registry.all_specs()
    runner = runner or default_runner(session=session, mongo=mongo, store=store, now=now)
    results = execute(plan(now, mongo, specs=specs, only=only, max_catchup=max_catchup,
                           derived=False), runner, mongo, specs, now)
    results += execute(plan(now, mongo, specs=specs, only=only, max_catchup=max_catchup,
                            derived=True), runner, mongo, specs, now)
    if not results:
        logger.info("nothing due at %s", now)
    return results


def run_dataset(dataset: str, day: dt.date | None = None, *,
                mongo: MongoStore | None = None, store: ParquetStore | None = None,
                session: PoliteSession | None = None,
                now: dt.datetime | None = None) -> RunResult:
    """Manual single-Dataset run (dashboard button / `run --dataset`), same path
    as the nightly job. Without `day`, the latest due day is used."""
    now = now or dt.datetime.now()
    mongo = mongo or MongoStore()
    store = store or ParquetStore(config.store_dir())
    spec = registry.get_spec(dataset)
    if day is None:
        lookback = _FRESH_LOOKBACK_DAYS[spec.cadence.kind]
        candidates = [d for d in _days(now.date() - dt.timedelta(days=lookback), now.date())
                      if spec.cadence.is_due_day(d) and spec.cadence.due_at(d) <= now]
        day = candidates[-1] if candidates else now.date()
    runner = default_runner(session=session, mongo=mongo, store=store, now=now)
    return execute([Due(dataset, day)], runner, mongo, {dataset: spec}, now)[0]


def backfill(dataset: str, start: dt.date | None = None, end: dt.date | None = None, *,
             mongo: MongoStore | None = None, store: ParquetStore | None = None,
             session: PoliteSession | None = None,
             specs: dict[str, DatasetSpec] | None = None,
             runner: RunFn | None = None,
             now: dt.datetime | None = None) -> list[RunResult]:
    """Walk every due day of a Dataset's Cadence in [start, end], oldest first,
    skipping days already published. Failures are recorded and skipped.
    `start` defaults to the Registry's backfill_start (the archive's depth)."""
    now = now or dt.datetime.now()
    mongo = mongo or MongoStore()
    store = store or ParquetStore(config.store_dir())
    specs = specs if specs is not None else registry.all_specs()
    spec = specs[dataset]
    start = start or spec.backfill_start
    end = end or now.date()
    runner = runner or default_runner(session=session, mongo=mongo, store=store, now=now)
    due = [Due(dataset, d) for d in _days(start, end)
           if spec.cadence.is_due_day(d) and not mongo.has_run(dataset, d, statuses=PUBLISHED)]
    logger.info("%s: backfilling %d due days from %s to %s", dataset, len(due), start, end)
    return execute(due, runner, mongo, specs, now)


def status(mongo: MongoStore | None = None, *,
           specs: dict[str, DatasetSpec] | None = None) -> list[dict[str, Any]]:
    """Latest outcome per Dataset for operators and the dashboard."""
    mongo = mongo or MongoStore()
    specs = specs if specs is not None else registry.all_specs()
    rows = []
    for name in sorted(specs):
        runs = mongo.runs(name, limit=1)
        latest = runs[0] if runs else None
        rows.append({
            "dataset": name,
            "cadence": specs[name].cadence.kind,
            "status": latest["status"] if latest else "never",
            "day": latest["day"].date() if latest else None,
            "at": latest["at"] if latest else None,
            "rows": latest["rows"] if latest else 0,
            "detail": latest.get("detail", []) if latest else [],
            "last_ok_day": mongo.last_ok_day(name),
        })
    return rows


# ── CLI ──────────────────────────────────────────────────────────────────

def _print_status(rows: list[dict[str, Any]]) -> None:
    print(f"{'dataset':42s} {'cadence':9s} {'status':14s} {'day':10s} {'last ok':10s} rows")
    for r in rows:
        print(f"{r['dataset']:42s} {r['cadence']:9s} {r['status']:14s} "
              f"{str(r['day'] or '-'):10s} {str(r['last_ok_day'] or '-'):10s} {r['rows']}")
        for line in r["detail"][:3]:
            print(f"{'':42s}   ↳ {line}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="twlab Orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="run everything due (the nightly job)")
    run_p.add_argument("--now", type=dt.datetime.fromisoformat, default=None)
    run_p.add_argument("--dataset", default=None, help="only this Dataset")
    run_p.add_argument("--max-catchup", type=int, default=DEFAULT_MAX_CATCHUP)
    run_p.add_argument("--witness", action="store_true",
                       help="also cross-check samples against the FinMind Witness")

    sub.add_parser("status", help="latest outcome per Dataset")

    bf = sub.add_parser("backfill", help="load history for one Dataset")
    bf.add_argument("dataset")
    bf.add_argument("--from", dest="start", type=dt.date.fromisoformat, default=None,
                    help="first day (default: the Registry's backfill_start for the Dataset)")
    bf.add_argument("--to", dest="end", type=dt.date.fromisoformat, default=dt.date.today())

    wit = sub.add_parser("witness", help="cross-check samples against FinMind")
    wit.add_argument("--dataset", default=None)
    wit.add_argument("--samples", type=int, default=20)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.command == "status":
        _print_status(status())
        return 0
    if args.command == "backfill":
        results = backfill(args.dataset, args.start, args.end)
        bad = [r for r in results if r.status not in PUBLISHED]
        print(f"{args.dataset}: {len(results)} batches, {len(bad)} not published")
        return 1 if bad else 0
    if args.command == "witness":
        from twlab import witness
        reports = witness.run_witness(ParquetStore(config.store_dir()), MongoStore(),
                                      witness.FinMindClient(PoliteSession(), config.finmind_token()),
                                      now=dt.datetime.now(), only=args.dataset,
                                      samples=args.samples)
        for r in reports:
            print(r.summary())
        return 1 if any(r.mismatches for r in reports) else 0

    results = run_due(args.now, only=args.dataset, max_catchup=args.max_catchup)
    for r in results:
        print(f"{r.dataset} {r.day}: {r.status}" + (f" — {r.failures}" if r.failures else ""))
    if args.witness:
        from twlab import witness
        for r in witness.run_witness(ParquetStore(config.store_dir()), MongoStore(),
                                     witness.FinMindClient(PoliteSession(), config.finmind_token()),
                                     now=args.now or dt.datetime.now()):
            print(r.summary())
    return 1 if any(r.status in ("failed", "quarantined") for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
