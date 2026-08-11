"""The Catalog: FinLab's field specification, loaded from docs/finlab_catalog.json.

The Catalog defines every valid Data Key (`dataset:field`, or bare `dataset` for
Event Tables) and is the source of truth for key resolution and `data.search()`.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache

from twlab import config
from twlab.errors import UnknownDataKeyError

_KEY_RE = re.compile(r'data\.get\("([^"]+)"\)')


@dataclass(frozen=True)
class CatalogField:
    key: str          # full Data Key, e.g. "price:收盤價"
    dataset: str      # e.g. "price"
    field: str        # e.g. "收盤價" ("" for bare Event Table keys)
    dtype: str        # "int" / "float" / "str" / ...
    description: str


def split_key(key: str) -> tuple[str, str]:
    """Split a Data Key into (dataset, field). Bare keys yield field ""."""
    dataset, _, field = key.partition(":")
    return dataset, field


@lru_cache(maxsize=1)
def _fields_by_key() -> dict[str, CatalogField]:
    with open(config.catalog_path(), encoding="utf-8") as f:
        raw = json.load(f)
    out: dict[str, CatalogField] = {}
    for entry in raw:
        for row in entry.get("fields", []):
            # row: [field_name, 'data.get("key")', dtype, size, range, description]
            m = _KEY_RE.search(row[1])
            if not m:
                continue
            key = m.group(1)
            dataset, field = split_key(key)
            out[key] = CatalogField(
                key=key,
                dataset=dataset,
                field=field,
                dtype=row[2],
                description=row[5] if len(row) > 5 else "",
            )
    return out


def resolve(key: str) -> CatalogField:
    """Resolve a Data Key against the Catalog, or raise UnknownDataKeyError."""
    fields = _fields_by_key()
    if key in fields:
        return fields[key]
    suggestions = [k for k in fields if key in k][:5]
    hint = f" Did you mean: {suggestions}?" if suggestions else ""
    raise UnknownDataKeyError(f"Unknown Data Key {key!r}.{hint}")


def dataset_fields(dataset: str) -> list[CatalogField]:
    """All Catalog fields belonging to one Dataset."""
    return [f for f in _fields_by_key().values() if f.dataset == dataset]


def search(keyword: str) -> list[CatalogField]:
    """Catalog fields whose key or description mentions the keyword."""
    kw = keyword.strip()
    return [
        f for f in _fields_by_key().values()
        if kw in f.key or kw in f.description
    ]
