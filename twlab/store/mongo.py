"""MongoDB system of record: one collection per Dataset, long-form rows.

Rows are keyed by the Dataset's key fields and written with idempotent upserts,
so re-runs and overlapping backfills never duplicate rows. Rows from a batch
that failed QA stay in the system of record (raw evidence) but carry a
Quarantine flag, and materialization excludes them — so they can never leak
into published Wide Frames through a later run. A subsequent good scrape of
the same keys clears the flag. Run outcomes (ok / quarantined / no_data) are
logged to the `runs` collection, which the Orchestrator and dashboard query.
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
        self._client = client or MongoClient(config.mongo_uri())
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
            # A fresh scrape supersedes any earlier Quarantine of these keys.
            doc["quarantined"] = False
            key = {k: doc[k] for k in spec.key_fields}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        if not ops:
            return UpsertResult(matched=0, upserted=0)
        result = col.bulk_write(ops, ordered=False)
        return UpsertResult(
            matched=result.matched_count, upserted=result.upserted_count
        )

    def quarantine_batch(self, spec: DatasetSpec, batch: pd.DataFrame) -> None:
        """Flag exactly this batch's rows so materialization never serves them."""
        ops = [
            UpdateOne(
                {
                    k: _to_python(record[k], is_int=k in spec.int_fields)
                    for k in spec.key_fields
                },
                {"$set": {"quarantined": True}},
            )
            for record in batch.to_dict("records")
        ]
        if ops:
            self.collection(spec.name).bulk_write(ops, ordered=False)

    def load_long(self, spec: DatasetSpec) -> pd.DataFrame:
        """Long-form history of a Dataset (Quarantined rows excluded), for
        Wide Frame materialization. Period dates are returned as stored."""
        cursor = self.collection(spec.name).find(
            {"quarantined": {"$ne": True}}, {"_id": 0, "quarantined": 0}
        )
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

    def has_run(self, dataset: str, day: dt.date, statuses: tuple[str, ...] = ("ok", "no_data")) -> bool:
        return self._db[RUNS].count_documents({
            "dataset": dataset,
            "day": dt.datetime.combine(day, dt.time()),
            "status": {"$in": list(statuses)},
        }) > 0
