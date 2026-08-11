"""The per-Dataset pipeline: fetch → parse → upsert → QA gate → materialize.

Invariant failures (and parse failures) Quarantine the batch: the run is
recorded, the batch's rows are flagged in the system of record, Parquet
materialization is skipped, and the API keeps serving the last good Wide
Frames. Time and I/O dependencies are injectable for tests.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

from twlab import config, qa, registry
from twlab.errors import ParseError
from twlab.http import PoliteSession
from twlab.store.mongo import MongoStore
from twlab.store.parquet import ParquetStore

logger = logging.getLogger(__name__)


RunStatus = Literal["ok", "quarantined", "no_data"]


@dataclass(frozen=True)
class RunResult:
    dataset: str
    day: dt.date
    status: RunStatus
    rows: int = 0
    failures: list[str] = field(default_factory=list)


def materialize(spec: registry.DatasetSpec, mongo: MongoStore,
                store: ParquetStore, now: dt.datetime) -> None:
    """Pivot the Dataset's full Mongo history into one Wide Frame per Field."""
    long_df = mongo.load_long(spec)
    frames = {}
    for f in spec.fields:
        wide = long_df.pivot_table(
            index="date", columns="stock_id", values=f, aggfunc="last"
        ).sort_index()
        wide.index.name = "date"
        wide.columns.name = None
        frames[f] = wide
    store.write_frames(spec.name, frames, now=now)


def run(
    dataset: str,
    day: dt.date,
    *,
    session: PoliteSession | None = None,
    mongo: MongoStore | None = None,
    store: ParquetStore | None = None,
    now: dt.datetime | None = None,
) -> RunResult:
    """Run one Dataset's pipeline for one day."""
    spec = registry.get_spec(dataset)
    session = session or PoliteSession()
    mongo = mongo or MongoStore()
    store = store or ParquetStore(config.store_dir())
    now = now or dt.datetime.now()

    raws = spec.fetch(session, day)
    try:
        parts = [spec.parse(raw) for raw in raws]
    except ParseError as exc:
        logger.error("%s %s: parse failed: %s", dataset, day, exc)
        mongo.record_run(spec, day, "quarantined", now, detail=[f"parse: {exc}"])
        return RunResult(dataset, day, "quarantined", failures=[f"parse: {exc}"])

    batch = pd.concat(parts, ignore_index=True)
    if batch.empty:
        mongo.record_run(spec, day, "no_data", now)
        return RunResult(dataset, day, "no_data")

    upsert = mongo.upsert_batch(spec, batch)
    rows = len(batch)

    failures = qa.run_invariants(list(spec.invariants), batch)
    if failures:
        logger.error("%s %s: quarantined: %s", dataset, day, failures)
        mongo.quarantine_batch(spec, batch)
        mongo.record_run(spec, day, "quarantined", now, rows=rows, detail=failures)
        return RunResult(dataset, day, "quarantined", rows=rows, failures=failures)

    materialize(spec, mongo, store, now)
    mongo.record_run(spec, day, "ok", now, rows=rows)
    logger.info(
        "%s %s: ok — %d rows (%d new)", dataset, day, rows, upsert.upserted
    )
    return RunResult(dataset, day, "ok", rows=rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one twlab Dataset pipeline")
    parser.add_argument("dataset", help="Registry name, e.g. price")
    parser.add_argument(
        "--date",
        type=dt.date.fromisoformat,
        default=dt.date.today(),
        help="Trading day to scrape (YYYY-MM-DD; default today)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = run(args.dataset, args.date)
    print(f"{result.dataset} {result.day}: {result.status}"
          + (f" ({result.rows} rows)" if result.rows else "")
          + (f" — {result.failures}" if result.failures else ""))
    return 0 if result.status in ("ok", "no_data") else 1


if __name__ == "__main__":
    raise SystemExit(main())
