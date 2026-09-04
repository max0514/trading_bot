"""MongoDB system of record: one collection per Dataset, long-form rows.

Rows are keyed by the Dataset's key fields and written with idempotent upserts,
so re-runs and overlapping backfills never duplicate rows. Only batches that
passed the QA gate reach a Dataset's collection; a Quarantined batch is kept
as evidence in the `quarantine` collection and never overwrites published
rows, so it cannot leak into Wide Frames through a later materialization.
Run outcomes (ok / quarantined / no_data / failed, witness_*) are logged to
the `runs` collection, which the Orchestrator and dashboard query.
"""
from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass
from typing import Any

import pandas as pd
from pymongo import DESCENDING, MongoClient, UpdateOne

from twlab import config
from twlab.spec import DatasetSpec

RUNS = "runs"
QUARANTINE = "quarantine"


@dataclass(frozen=True)
class UpsertResult:
    matched: int
    upserted: int


def _to_python(value: Any, is_int: bool) -> Any:
    """Convert pandas/numpy scalars to Mongo-encodable python values."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if value is pd.NaT:
        return None
    if is_int:
        return int(value)
    if isinstance(value, (int, float)):
        return float(value) if not isinstance(value, bool) else value
    if hasattr(value, "item"):          # numpy scalar
        return value.item()
    return value


class MongoStore:
    def __init__(self, client: MongoClient | None = None, db_name: str | None = None):
        self._client = client or MongoClient(
            config.mongo_uri(), serverSelectionTimeoutMS=config.mongo_timeout_ms()
        )
        self._db = self._client[db_name or config.mongo_db_name()]

    def collection(self, dataset: str):
        return self._db[dataset]

    def upsert_batch(self, spec: DatasetSpec, batch: pd.DataFrame) -> UpsertResult:
        col = self.collection(spec.name)
        col.create_index(
            [(k, 1) for k in spec.key_fields], unique=True, name="dataset_key"
        )
        ops = []
        for record in batch.to_dict("records"):
            doc = {
                k: _to_python(v, is_int=k in spec.int_fields)
                for k, v in record.items()
            }
            key = {k: doc[k] for k in spec.key_fields}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        if not ops:
            return UpsertResult(matched=0, upserted=0)
        result = col.bulk_write(ops, ordered=False)
        return UpsertResult(
            matched=result.matched_count, upserted=result.upserted_count
        )

    def quarantine_batch(self, spec: DatasetSpec, batch: pd.DataFrame,
                         day: dt.date, now: dt.datetime) -> None:
        """Keep a failed batch as evidence, outside the Dataset's collection."""
        stamp = {"dataset": spec.name, "day": dt.datetime.combine(day, dt.time()), "at": now}
        docs = [
            {**stamp, **{k: _to_python(v, is_int=k in spec.int_fields) for k, v in record.items()}}
            for record in batch.to_dict("records")
        ]
        if docs:
            self._db[QUARANTINE].insert_many(docs)

    def quarantined_rows(self, dataset: str) -> pd.DataFrame:
        """Evidence rows of Quarantined batches (operators inspect these; the
        API never sees them)."""
        return pd.DataFrame(list(self._db[QUARANTINE].find({"dataset": dataset}, {"_id": 0})))

    def load_long(self, spec: DatasetSpec) -> pd.DataFrame:
        """Long-form published history of a Dataset, for Wide Frame
        materialization. Period dates are returned as stored."""
        cursor = self.collection(spec.name).find({}, {"_id": 0})
        df = pd.DataFrame(list(cursor))
        if df.empty:
            return pd.DataFrame(columns=[*spec.key_fields, *spec.fields])
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df

    # ── run log ──────────────────────────────────────────────────────────

    def record_run(
        self,
        spec: DatasetSpec,
        day: dt.date,
        status: str,
        now: dt.datetime,
        rows: int = 0,
        detail: list[str] | None = None,
    ) -> None:
        self._db[RUNS].insert_one(
            {
                "dataset": spec.name,
                "day": dt.datetime.combine(day, dt.time()),
                "status": status,
                "rows": rows,
                "detail": detail or [],
                "at": now,
            }
        )

    def runs(self, dataset: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """Most recent run records, newest first."""
        query = {"dataset": dataset} if dataset else {}
        cursor = (
            self._db[RUNS].find(query, {"_id": 0}).sort("at", DESCENDING).limit(limit)
        )
        return list(cursor)

    def last_ok_day(self, dataset: str) -> dt.date | None:
        """The latest batch day this Dataset was published for (status ok)."""
        doc = self._db[RUNS].find_one(
            {"dataset": dataset, "status": "ok"}, sort=[("day", DESCENDING)]
        )
        return doc["day"].date() if doc else None

    def last_ok_at(self, dataset: str) -> dt.datetime | None:
        """When this Dataset last published (status ok), by run time."""
        doc = self._db[RUNS].find_one(
            {"dataset": dataset, "status": "ok"}, sort=[("at", DESCENDING)]
        )
        return doc["at"] if doc else None

    def first_run_day(self, dataset: str) -> dt.date | None:
        """The earliest batch day ever attempted — the catch-up horizon."""
        doc = self._db[RUNS].find_one({"dataset": dataset}, sort=[("day", 1)])
        return doc["day"].date() if doc else None

    def has_run(self, dataset: str, day: dt.date, statuses: tuple[str, ...] = ("ok", "no_data")) -> bool:
        return self._db[RUNS].count_documents({
            "dataset": dataset,
            "day": dt.datetime.combine(day, dt.time()),
            "status": {"$in": list(statuses)},
        }) > 0
