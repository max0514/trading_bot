# price fixtures

Recorded source responses used by the parser (Seam 3) and pipeline (Seam 2) tests.
No test hits the live network.

| File | Provenance |
| --- | --- |
| `twse_mi_index_20260807.json` | **Real recording.** `GET https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date=20260807&type=ALLBUT0999&response=json`, recorded 2026-08-11. Unmodified. |
| `twse_mi_index_20260807_malformed.json` | Derived from the real recording: the quotes table's `收盤價` column renamed to `收盤`, simulating a silent source format change. Parsers must fail loudly on it. |
| `twse_mi_index_20260807_tiny.json` | Derived from the real recording: quotes table truncated to 3 rows, the first renamed to poison Stock ID `9998`. Parses cleanly but must trip the row-count Invariant; `9998` lets tests prove Quarantined rows never reach published frames. |
| `tpex_daily_quotes_20260807.json` | **Synthesized, format-accurate.** tpex.org.tw sits behind Cloudflare and blocked every recording attempt from the development network (curl, requests, and a real Chrome all got 403 / connection resets). The file follows the documented shape of `GET https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date=2026/08/07&type=EW&response=json` (`tables[].fields/data`, comma-grouped number strings, `最後買量/賣量` in 千股). Stock IDs are genuine TPEx codes; values are plausible but invented. **Re-record from the real endpoint when network access allows, and delete this note.** |

Golden values asserted in tests (e.g. TSMC `2330` close `2370.00` on 2026-08-07) come
from the real TWSE recording; TPEx assertions are format/plumbing checks only until the
fixture is re-recorded.
