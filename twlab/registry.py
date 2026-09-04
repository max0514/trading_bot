"""The Registry: declarative index of every Dataset the platform collects.

Each Dataset module under twlab/datasets/ declares its entries in a
module-level `SPECS` list (Official Source, Cadence, Invariants, backfill
depth, fetch/parse or derive). The Registry discovers them, so adding
Dataset #9..#123 means adding a module — the pipeline, QA gate, and stores
are shared and never edited.
"""
from __future__ import annotations

import importlib
import pkgutil
from functools import lru_cache

from twlab import datasets as _datasets
from twlab.spec import Cadence, DatasetSpec

__all__ = ["Cadence", "DatasetSpec", "all_specs", "get_spec", "names"]


@lru_cache(maxsize=1)
def all_specs() -> dict[str, DatasetSpec]:
    specs: dict[str, DatasetSpec] = {}
    modules = sorted(pkgutil.iter_modules(_datasets.__path__), key=lambda m: m.name)
    for info in modules:
        module = importlib.import_module(f"{_datasets.__name__}.{info.name}")
        for spec in getattr(module, "SPECS", ()):
            if spec.name in specs:
                raise RuntimeError(
                    f"Dataset {spec.name!r} declared twice (twlab.datasets.{info.name})"
                )
            specs[spec.name] = spec
    return specs


def get_spec(name: str) -> DatasetSpec:
    specs = all_specs()
    if name not in specs:
        raise KeyError(
            f"No Registry entry for Dataset {name!r}. Known: {sorted(specs)}"
        )
    return specs[name]


def names() -> list[str]:
    return sorted(all_specs())
