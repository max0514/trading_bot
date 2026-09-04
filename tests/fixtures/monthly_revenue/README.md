# monthly_revenue fixtures

Recorded MOPS pages used by the parser (Seam 3) and pipeline (Seam 2) tests. No test hits
the live network.

Each `*.html.gz` holds the page exactly as the parser receives it from
`PoliteSession.get_text()`: the Big5 (cp950) body decoded to UTF-8 text, gzipped because
a page is ~450 KB. `tests/conftest.py::load_text_fixture` transparently reads the `.gz`.

| File | Provenance |
| --- | --- |
| `mops_sii_t21sc03_115_7_0.html.gz` | **Real recording.** `GET https://mopsov.twse.com.tw/nas/t21/sii/t21sc03_115_7_0.html` (上市, July 2026 revenue), recorded 2026-09-03. Unmodified: 992 filers across 35 industry tables, 出表日期 115/09/03. |
| `mops_otc_t21sc03_115_7_0.html.gz` | **Real recording.** Same path under `nas/t21/otc/` (上櫃, July 2026), 860 filers. |
| `mops_sii_t21sc03_115_5_0.html.gz`, `mops_otc_t21sc03_115_5_0.html.gz` | **Real recordings** for May 2026 (Statutory Deadline June 10), used by the Point-in-Time Alignment tests. |
| `mops_sii_t21sc03_115_7_0_malformed.html.gz` | Derived from the real 上市 July page: the first `當月營收` header cell renamed to `本月營收`, simulating a silent source format change. Parsers must fail loudly on it. |
| `mops_sii_t21sc03_115_7_0_tiny.html.gz` | Derived from the real 上市 July page: only the first 8 filer rows kept. Parses cleanly but must trip the row-count Invariant. |

Golden values asserted in tests (e.g. TSMC `2330` July 2026 revenue 467,580,548 千元, YoY
+44.68 %) come from the real recordings and agree with the FinMind Witness
(`TaiwanStockMonthRevenue`, 467,580,548,000 元).

Note on the host: the redesigned `mops.twse.com.tw` returns 404 for the `nas/t21` summaries
and WAF-blocks scripted clients; the legacy `mopsov.twse.com.tw` serves them to plain
`requests` with the twlab User-Agent (curl and browsers from this network were blocked).
