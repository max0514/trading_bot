"""The per-Dataset pipeline: fetch → parse → QA gate → upsert → materialize.

Invariant failures (and parse failures) Quarantine the batch: the run is
recorded, the batch's rows are kept as evidence in a side collection and
never touch the system of record, Parquet materialization is skipped, and
the API keeps serving the last good Wide Frames — so a bad re-scrape of an
already-published day cannot overwrite it. Derived (ETL) Datasets skip the
scrape and compute their frames from other materialized Datasets. Time and
I/O dependencies are injectable.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
from dataclasses import dataclass, field

import pandas as pd

from twlab import config, qa, registry
from twlab.errors import TwlabError
from twlab.http import PoliteSession
from twlab.spec import DatasetSpec
from twlab.status import PUBLISHED, RunStatus
from twlab.store.mongo import MongoStore
from twlab.store.parquet import ParquetStore

logger = logging.getLogger(__name__)

__all__ = ["PUBLISHED", "RunResult", "RunStatus", "defaults", "materialize", "run",
           "wide_frames"]


def defaults(
    mongo: MongoStore | None,
    store: ParquetStore | None,
    now: dt.datetime | None,
) -> tuple[MongoStore, ParquetStore, dt.datetime]:
    """Resolve the three dependencies every entry point takes.

    The system of record, the published store and the clock are optional
    kwargs on `run`, `run_due`, `run_dataset` and `backfill` so tests can
    inject a mongomock client, a tmp_path store and a frozen `now`. This is
    that defaulting, written once instead of at each entry point.
    """
    return (mongo or MongoStore(),
            store or ParquetStore(config.store_dir()),
            now or dt.datetime.now())


@dataclass(frozen=True)
class RunResult:
    dataset: str
    day: dt.date
    status: RunStatus
    rows: int = 0
    failures: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        # Callers (and tests) may pass the plain string a run log holds.
        object.__setattr__(self, "status", RunStatus(self.status))


def wide_frames(spec: DatasetSpec, long_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Pivot a long-form history into one Wide Frame per Field.

    Monthly/quarterly Datasets are re-dated here from their period to their
    Statutory Deadline (Point-in-Time Alignment); the period stays in Mongo.
    """
    if long_df.empty:
        empty_index = pd.DatetimeIndex([], name="date")
        return {f: pd.DataFrame(index=empty_index) for f in spec.fields}
    if spec.align is not None:
        long_df = long_df.assign(date=spec.align(long_df["date"]))
    grouped = long_df.groupby(["date", "stock_id"], sort=True)
    frames = {}
    for f in spec.fields:
        wide = grouped[f].last().unstack("stock_id").sort_index()
        wide.index = pd.DatetimeIndex(wide.index, name="date")
        wide.columns.name = None
        frames[f] = wide
    return frames


def materialize(spec: DatasetSpec, mongo: MongoStore,
                store: ParquetStore, now: dt.datetime) -> None:
    """Publish the Dataset's full (non-Quarantined) Mongo history to Parquet."""
    long_df = mongo.load_long(spec)
    if spec.shape == "table":
        columns = [*spec.key_fields, *spec.fields]
        table = long_df.reindex(columns=columns).sort_values(list(spec.key_fields))
        store.write_table(spec.name, table.reset_index(drop=True), now=now)
        return
    store.write_frames(spec.name, wide_frames(spec, long_df), now=now,
                       frequency=spec.frequency)


def _fetch_kwargs(spec: DatasetSpec, store: ParquetStore) -> dict:
    """Per-company sources receive the Stock ID universe from a materialized frame."""
    if spec.universe_from is None:
        return {}
    dataset, _, field = spec.universe_from.partition(":")
    return {"universe": [str(c) for c in store.read_frame(dataset, field).columns]}


def _run_derived(spec: DatasetSpec, day: dt.date, mongo: MongoStore,
                 store: ParquetStore, now: dt.datetime) -> RunResult:
    try:
        frames = spec.derive(store)
    except TwlabError as exc:
        logger.error("%s %s: derive failed: %s", spec.name, day, exc)
        mongo.record_run(spec, day, RunStatus.QUARANTINED, now, detail=[f"derive: {exc}"])
        return RunResult(spec.name, day, RunStatus.QUARANTINED, failures=[f"derive: {exc}"])
    rows = sum(len(f) for f in frames.values())
    store.write_frames(spec.name, frames, now=now, frequency=spec.frequency)
    mongo.record_run(spec, day, RunStatus.OK, now, rows=rows)
    logger.info("%s %s: ok — derived %d fields", spec.name, day, len(frames))
    return RunResult(spec.name, day, RunStatus.OK, rows=rows)


def run(
    dataset: str,
    day: dt.date,
    *,
    session: PoliteSession | None = None,
    mongo: MongoStore | None = None,
    store: ParquetStore | None = None,
    now: dt.datetime | None = None,
) -> RunResult:
    """Run one Dataset's pipeline for one batch day.

    `day` is the Cadence due day: the trading day for daily Datasets, the
    Statutory Deadline for monthly/quarterly ones (the Dataset's fetch
    derives its period from it).
    """
    spec = registry.get_spec(dataset)
    mongo, store, now = defaults(mongo, store, now)

    if spec.is_derived:
        return _run_derived(spec, day, mongo, store, now)

    session = session or PoliteSession()
    kwargs = _fetch_kwargs(spec, store)
    raws = spec.fetch(session, day, **kwargs)
    try:
        parts = [spec.parse(raw) for raw in raws]
    except TwlabError as exc:
        logger.error("%s %s: parse failed: %s", dataset, day, exc)
        mongo.record_run(spec, day, RunStatus.QUARANTINED, now, detail=[f"parse: {exc}"])
        return RunResult(dataset, day, RunStatus.QUARANTINED, failures=[f"parse: {exc}"])

    # Drop empty halves (a market's holiday) so concat keeps the typed dtypes.
    parts = [p for p in parts if not p.empty]
    batch = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if batch.empty:
        mongo.record_run(spec, day, RunStatus.NO_DATA, now)
        return RunResult(dataset, day, RunStatus.NO_DATA)

    if spec.universe_from is not None:
        # A per-company source: one raw per company the fetch asked about, so
        # the QA gate can judge coverage rather than an absolute row count.
        batch.attrs[qa.REQUESTED] = len(raws)

    rows = len(batch)
    failures = qa.run_invariants(list(spec.invariants), batch)
    if failures:
        logger.error("%s %s: quarantined: %s", dataset, day, failures)
        mongo.quarantine_batch(spec, batch, day, now)
        mongo.record_run(spec, day, RunStatus.QUARANTINED, now, rows=rows, detail=failures)
        return RunResult(dataset, day, RunStatus.QUARANTINED, rows=rows, failures=failures)

    upsert = mongo.upsert_batch(spec, batch)
    materialize(spec, mongo, store, now)
    mongo.record_run(spec, day, RunStatus.OK, now, rows=rows)
    logger.info(
        "%s %s: ok — %d rows (%d new)", dataset, day, rows, upsert.upserted
    )
    return RunResult(dataset, day, RunStatus.OK, rows=rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one twlab Dataset pipeline")
    parser.add_argument("dataset", help="Registry name, e.g. price")
    parser.add_argument(
        "--date",
        type=dt.date.fromisoformat,
        default=dt.date.today(),
        help="Batch day (YYYY-MM-DD; default today). Daily: the trading day; "
             "monthly/quarterly: the Statutory Deadline whose period to fetch.",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = run(args.dataset, args.date)
    print(f"{result.dataset} {result.day}: {result.status}"
          + (f" ({result.rows} rows)" if result.rows else "")
          + (f" — {result.failures}" if result.failures else ""))
    return 0 if result.status.published else 1


if __name__ == "__main__":
    raise SystemExit(main())
