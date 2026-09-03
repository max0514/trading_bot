# benchmark_return fixtures

MFI94U-format month pages used by the parser (Seam 3) and pipeline (Seam 2) tests. No test
hits the live network.

**All files are synthesized, format-accurate.** TWSE's WAF answers every request for
`https://www.twse.com.tw/rwd/zh/indicesReport/MFI94U?date=YYYYMMDD&response=json` from the
development network with its block page (「因為安全性考量，您所執行的頁面無法呈現」, keyed on
the client IP), so the 報酬指數 report could not be recorded. The files follow that
endpoint's documented shape: `{"stat": "OK", "date": <query date>, "title": "115年08月
發行量加權股價報酬指數", "fields": ["日期", "發行量加權股價報酬指數"], "data": [[ROC date,
comma-grouped value], …]}` — the whole month containing the query date, one row per trading
day.

| File | Content |
| --- | --- |
| `twse_mfi94u_202608.json` | August 2026 (query date 20260807): 21 trading days whose values are the FinMind Witness's `TaiwanStockTotalReturnIndex` (`data_id=TAIEX`, field `price`) closes, recorded 2026-09-03. |
| `twse_mfi94u_202609.json` | September 2026 as of the query date 20260903: 3 trading days, same construction. |
| `twse_mfi94u_202608_malformed.json` | `發行量加權股價報酬指數` column renamed to `報酬指數` — a silent source format change the parser must reject. |
| `twse_mfi94u_202609_poison.json` | September page with 115/09/02 set to `0.00`: parses cleanly but must trip the strictly-positive Invariant. |

Golden values asserted in tests (e.g. 2026-08-07 → 101,989.71; 2026-09-01 → 108,395.72) are
the Witness figures. **Re-record from the real MFI94U endpoint when network access allows,
and delete this note.**
