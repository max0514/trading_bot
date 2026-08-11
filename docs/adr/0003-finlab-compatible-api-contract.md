# The data API replicates FinLab's contract exactly

`from twlab import data` exposes `data.get("dataset:欄位")` using FinLab's exact data keys
(Chinese field names included, per `docs/finlab_catalog.json`), returning a
FinlabDataFrame-equivalent subclass — strategy helpers (`.average`, `.rise`, `.sustain`,
`.is_largest`, …) plus automatic cross-frequency alignment when monthly/quarterly frames
are combined with daily ones. Monthly and quarterly datasets are point-in-time aligned to
statutory disclosure deadlines by default (月營收 → 10th of next month; 季報 → 5/15, 8/14,
11/14, 3/31), with actual MOPS filing dates planned as a later upgrade. The acceptance bar
is that a published FinLab example strategy runs verbatim on twlab. We rejected
English-only keys and plain-pandas returns (both break copy-paste compatibility, which is
the point of the system) and naming the package `finlab` (would shadow the real package).
`backtest/data.py` stays as a thin shim over twlab so existing strategies keep working.
