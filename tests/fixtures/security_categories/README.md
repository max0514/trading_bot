# security_categories fixtures

Recorded ISIN pages used by the parser (Seam 3) and pipeline (Seam 2) tests. No test hits
the live network. The files are the raw MS950 bytes the site serves — decode them as
`PoliteSession.get_text(encoding="cp950")` does.

| File | Provenance |
| --- | --- |
| `isin_c_public_strmode2.html` | **Real recording, warrant section reduced.** `GET https://isin.twse.com.tw/isin/C_public.jsp?strMode=2` (上市), recorded 2026-09-03 with Python `requests` and the twlab User-Agent (curl and a browser get TWSE's WAF block page). The 8.4 MB page is 96% warrants, so all but the first 3 rows of the `上市認購(售)權證` section were removed (33,712 rows); everything else — the header, all 8 section rows (股票, 上市認購(售)權證, 特別股, 創新板, ETF, ETN, 臺灣存託憑證(TDR), 受益證券-不動產投資信託), every other row and the footer — is verbatim. 1,054 股票, 28 特別股, 30 創新板, 240 ETF, 15 ETN, 10 TDR, 6 受益證券. 創新板 rows carry 市場別 `上市臺灣創新板`. |
| `isin_c_public_strmode4.html` | Same for `strMode=4` (上櫃): all but the first 3 rows of `上櫃認購(售)權證` removed (10,252 rows). 120 ETF, 6 ETN, 890 股票, 1 特別股, 4 受益證券-資產基礎證券 — this page lists warrants and ETFs *before* 股票, and its header still says 上市日. |
| `isin_c_public_strmode2_malformed.html` | Derived from the reduced 上市 page: `產業別` header renamed to `行業別` — a silent source format change the parser must reject. |
| `isin_c_public_strmode2_tiny.html` | Derived from the reduced 上市 page: the header, the 股票 section row and its first 5 stocks. Parses cleanly but must trip the row-count Invariant. |

The parser was also run against the full, unreduced recordings and produced exactly the
rows the reduced fixtures produce (the kept warrant rows are skipped either way).
Golden values asserted in tests (2330 → 台積電 / 半導體業 / sii; 2881A → 金融保險業 from the
特別股 section; 2237 → 汽車工業 from 創新板; 9103 → 存託憑證; 5483 → 中美晶 / 半導體業 / otc)
are the pages' own. One 4-digit Stock ID of the real 2026-08-07 price recording, `2867`
三商壽, is absent from the pages: it last traded 2026-08-19 (FinMind Witness price history)
and is likewise missing from the 2026-09-01/02 T86 recordings — a delisting between the two
recordings, listed as the coverage test's sole exception.
