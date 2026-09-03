# price_earning_ratio fixtures

Recorded source responses used by the parser (Seam 3) and pipeline (Seam 2) tests.
No test hits the live network.

| File | Provenance |
| --- | --- |
| `twse_bwibbu_d_20260807.json` | **Real recording.** `GET https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?date=20260807&selectType=ALL&response=json`, recorded 2026-09-03. Unmodified: a flat `fields`/`data` payload of 1,082 securities (`證券代號, 證券名稱, 收盤價, 殖利率(%), 股利年度, 本益比, 股價淨值比, 財報年/季`); a loss-maker's `本益比` prints as `-`. |
| `twse_bwibbu_d_20260807_malformed.json` | Derived from the real recording: the `本益比` column renamed to `本益比(倍)`, simulating a silent source format change. Parsers must fail loudly on it. |
| `twse_bwibbu_d_20260807_tiny.json` | Derived from the real recording: truncated to 3 rows, the first renamed to poison Stock ID `9998`. Parses cleanly but must trip the row-count Invariant; `9998` lets tests prove Quarantined rows never reach published frames. |
| `tpex_pe_qry_date_20260807.json` | **Real recording.** `GET https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate?date=2026/08/07&response=json`, recorded 2026-09-03 with Python `requests` and the twlab User-Agent (tpex.org.tw's Cloudflare front answers curl and browsers from the development network with 403). Unmodified: `tables[0]` has an **empty `title`** (never locate it by title), fields `股票代號, 公司名稱, 本益比, 每股股利, 股利年度, 殖利率(%), 股價淨值比, 財報年/季`, 888 rows; `公司名稱` is space-padded, `股利年度` is an int, a loss-maker's `本益比` is `N/A`; the top-level `date` is `20260807` while the table's own `date` is ROC `115/08/07`. |

Golden values asserted in tests come from the two real recordings — TSMC `2330` on
2026-08-07: 殖利率 0.93 / 本益比 31.86 / 股價淨值比 10.43; 中美晶 `5483`: 2.08 / 23.93 / 2.09 —
and the FinMind Witness (`TaiwanStockPER`) reports the same figures for both, and for every
other TPEx code cross-checked (3105, 3293, 3529, 4123, 4966, 5347, 6488, 6510, 6547, 8069, 8299).
