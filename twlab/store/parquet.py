"""Parquet Wide Frame store: what `data.get()` reads.

Layout: <store>/<dataset>/<field>.parquet plus a per-Dataset manifest.json
carrying freshness/version metadata. Frames are written atomically (temp file +
rename) so a reader never observes a half-written frame.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Callable

import pandas as pd

from twlab.errors import DatasetNotMaterializedError


def _atomic_write(path: Path, write: Callable[[Path], object]) -> None:
    """Write to a temp file next to `path`, then rename into place."""
    tmp = path.with_name(path.name + ".tmp")
    write(tmp)
    tmp.replace(path)


class ParquetStore:
    def __init__(self, root: Path):
        self._root = Path(root)

    def _dataset_dir(self, dataset: str) -> Path:
        return self._root / dataset

    def _frame_path(self, dataset: str, field: str) -> Path:
        return self._dataset_dir(dataset) / f"{field}.parquet"

    def manifest_path(self, dataset: str) -> Path:
        return self._dataset_dir(dataset) / "manifest.json"

    def write_frames(
        self,
        dataset: str,
        frames: dict[str, pd.DataFrame],
        now: dt.datetime,
    ) -> None:
        """Materialize one Wide Frame per Field, then publish the manifest last."""
        directory = self._dataset_dir(dataset)
        directory.mkdir(parents=True, exist_ok=True)
        for field, frame in frames.items():
            _atomic_write(self._frame_path(dataset, field), frame.to_parquet)
        manifest = {
            "dataset": dataset,
            "materialized_at": now.isoformat(),
            "fields": {
                field: {
                    "rows": int(len(frame)),
                    "columns": int(frame.shape[1]),
                    "first_date": frame.index.min().isoformat() if len(frame) else None,
                    "last_date": frame.index.max().isoformat() if len(frame) else None,
                }
                for field, frame in frames.items()
            },
        }
        _atomic_write(
            self.manifest_path(dataset),
            lambda tmp: tmp.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8"
            ),
        )

    def read_frame(self, dataset: str, field: str) -> pd.DataFrame:
        path = self._frame_path(dataset, field)
        if not path.exists():
            raise DatasetNotMaterializedError(
                f"No materialized Wide Frame for {dataset}:{field} in {self._root} — "
                f"run `python -m twlab.pipeline {dataset}` first."
            )
        return pd.read_parquet(path)

    def read_manifest(self, dataset: str) -> dict:
        path = self.manifest_path(dataset)
        if not path.exists():
            raise DatasetNotMaterializedError(
                f"No manifest for Dataset {dataset!r} in {self._root}"
            )
        return json.loads(path.read_text(encoding="utf-8"))
