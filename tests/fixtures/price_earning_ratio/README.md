# price_earning_ratio fixtures

Recorded and synthesized source responses used by the parser (Seam 3) and pipeline
(Seam 2) tests. No test hits the live network.

| File | Provenance |
| --- | --- |
| `twse_bwibbu_d_20260807.json` | **Real recording.** `GET https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?date=20260807&selectType=ALL&response=json`, recorded 2026-09-03. Unmodified: a flat `fields`/`data` payload of 1,082 securities (`證券代號, 證券名稱, 收盤價, 殖利率(%), 股利年度, 本益比, 股價淨值比, 財報年/季`); loss-making companies print `本益比` as `-`. |
| `twse_bwibbu_d_20260807_malformed.json` | Derived from the real recording: the `本益比` column renamed to `本益比(倍)`, simulating a silent source format change. Parsers must fail loudly on it. |
| `twse_bwibbu_d_20260807_tiny.json` | Derived from the real recording: truncated to 3 rows, the first renamed to poison Stock ID `9998`. Parses cleanly but must trip the row-count Invariant; `9998` lets tests prove Quarantined rows never reach published frames. |
| `tpex_pe_qry_date_20260807.json` | **Synthesized, format-accurate.** tpex.org.tw sits behind Cloudflare and answers every request from the development network with 403, so `GET https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate?date=2026/08/07&response=json` could not be recorded. The file follows that endpoint's documented shape (the same `tables[].fields/data` envelope as the `price` TPEx fixture; columns `代號, 名稱, 本益比, 每股股利, 股利年度, 殖利率(%), 股價淨值比`; `N/A` for a loss-maker's 本益比). Its 12 Stock IDs are genuine TPEx codes and their `本益比 / 殖利率(%) / 股價淨值比` are the FinMind Witness's `TaiwanStockPER` figures for 2026-08-07, recorded 2026-09-03; `每股股利` and `股利年度` are not Fields of the Dataset and are plausible fillers. **Re-record from the real endpoint when network access allows, and delete this note.** |

Golden values asserted in tests come from the real TWSE recording (TSMC `2330` on
2026-08-07: 殖利率 0.93 / 本益比 31.86 / 股價淨值比 10.43 — the Witness reports the same three
figures) and, for TPEx (中美晶 `5483`: 2.08 / 23.93 / 2.09), from the Witness values embedded
in the synthesized fixture.
