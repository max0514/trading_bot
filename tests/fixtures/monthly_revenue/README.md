# monthly_revenue fixtures

MOPS-format pages used by the parser (Seam 3) and pipeline (Seam 2) tests. No test hits
the live network.

**All files are synthesized, format-accurate.** MOPS (`mops.twse.com.tw`) answers every
request from the development network — curl, requests, and a real Chromium — with its
WAF block page (「因為安全性考量，您所執行的頁面無法呈現」, keyed on the client IP), so the
`nas/t21/{sii,otc}/t21sc03_<roc year>_<month>_0.html` pages could not be recorded.
The files follow that page's documented layout: UTF-8, a `民國115年7月份上市公司每月營業收入
彙總表` heading, one nested table per 產業別 with the two-row header (`營業收入` /
`當月營收 上月營收 …`), comma-grouped 千元 values, and a `合計` subtotal row per industry.

| File | Content |
| --- | --- |
| `mops_sii_t21sc03_115_7_0.html` | 上市, July 2026. 5 real filers (1101, 2330, 2454, 2317, 2412) whose values are the FinMind Witness's `TaiwanStockMonthRevenue` figures (元 ÷ 1000) recorded 2026-09-03, plus 600 synthetic filers (codes 90000–90599, 模擬公司) so the batch clears the row-count Invariant. |
| `mops_otc_t21sc03_115_7_0.html` | 上櫃, July 2026. Real filers 5483, 6488, 3105, 8069, 4966; synthetic codes 95000–95599. |
| `mops_sii_t21sc03_115_5_0.html`, `mops_otc_t21sc03_115_5_0.html` | Same construction for May 2026 (Statutory Deadline June 10). |
| `mops_sii_t21sc03_115_7_0_malformed.html` | `當月營收` header renamed to `本月營收` — a silent source format change the parser must reject. |
| `mops_sii_t21sc03_115_7_0_tiny.html` | Only 8 filers: parses cleanly but must trip the row-count Invariant. |

Golden values asserted in tests (e.g. TSMC `2330` July 2026 revenue 467,580,548 千元) are the
Witness figures. **Re-record from the real MOPS pages when network access allows, and delete
this note.**
