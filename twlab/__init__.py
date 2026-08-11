"""twlab — self-hosted, FinLab-parity Taiwan market data platform.

Usage:
    from twlab import data
    close = data.get("price:收盤價")   # Wide Frame: index=date, columns=Stock ID
    data.search("收盤")                # discover Data Keys in the Catalog

Pipeline (server side):
    python -m twlab.pipeline price --date 2026-08-07

Vocabulary (Dataset, Field, Data Key, Wide Frame, Registry, Invariant,
Quarantine, ...) is defined in CONTEXT.md; architecture decisions in docs/adr/.
"""
from twlab import data

__all__ = ["data"]
