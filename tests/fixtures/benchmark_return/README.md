# benchmark_return fixtures

Recorded MFI94U month pages used by the parser (Seam 3) and pipeline (Seam 2) tests. No test
hits the live network.

The 報酬指數 report lives at `https://www.twse.com.tw/rwd/zh/TAIEX/MFI94U?date=YYYYMMDD&response=json`
(the `indicesReport/MFI94U` path returns TWSE's 404 page). It answers with the whole month
containing the query date — `{"stat": "OK", "date": <query date>, "title": "115年08月
發行量加權股價報酬指數", "fields": ["日　期", "發行量加權股價報酬指數"], "data": [[ROC date,
comma-grouped value], …], "total": n}` — and spells the date header `日　期` with a full-width
space (U+3000), which the parser normalises away before locating columns. Both pages were
recorded 2026-09-03 with Python `requests` and the twlab User-Agent.

| File | Provenance |
| --- | --- |
| `twse_mfi94u_202608.json` | **Real recording.** `?date=20260801`: August 2026, 21 trading days. Unmodified. |
| `twse_mfi94u_202609.json` | **Real recording.** `?date=20260903`: September 2026 as of that day, 3 trading days. Unmodified. |
| `twse_mfi94u_202608_malformed.json` | Derived from the real August page: `發行量加權股價報酬指數` renamed to `報酬指數` — a silent source format change the parser must reject. |
| `twse_mfi94u_202609_poison.json` | Derived from the real September page: 115/09/02 set to `0.00`. Parses cleanly but must trip the strictly-positive Invariant. |

Golden values asserted in tests (e.g. 2026-08-07 → 101,989.71; 2026-09-01 → 108,395.72) come
from the recordings; the FinMind Witness (`TaiwanStockTotalReturnIndex`, `data_id=TAIEX`)
reports the same value for every one of the 24 days across both pages.
