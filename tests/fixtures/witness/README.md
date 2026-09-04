# witness fixtures

Recorded Witness (FinMind) responses. The Witness is never a collection source
(ADR-0001) — these exist so the cross-check itself runs offline, like every
other test in this suite.

| File | Provenance |
| --- | --- |
| `finmind_financial_statements_20251201_20260430.json` | **Real recording.** Three `GET https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockFinancialStatements&data_id=<2330\|2317\|1101>&start_date=2025-12-01&end_date=2026-04-30` calls (no token — the free tier answers small pulls), recorded 2026-09-04. Unmodified, keyed by Stock ID: each value is that call's whole response envelope. Covers the two quarters `test_fundamental_features`'s `real_store` materializes, 2025 Q4 and 2026 Q1. |

## What the recording establishes

FinMind serves **single-quarter** statement values, not year-to-date ones —
the same convention `financial_statement` de-cumulates MOPS into. TSMC's
2025-12-31 `Revenue` is 1,046,090,421,000 元, which is the annual figure minus
the first three quarters, and matches `TSMC_Q4_2025["營業收入淨額"]` (仟元)
exactly. So the two sides' periods line up with no adjustment, and the only
scale difference is 元 vs 仟元.
