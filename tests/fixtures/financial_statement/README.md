# financial_statement fixtures

Recorded MOPS per-company statement pages used by the parser (Seam 3) and pipeline (Seam 2)
tests. No test hits the live network.

**All `mops_t164sb0{3,4,5}_<id>_<roc year>_<season>.html` files are real recordings**, made
2026-09-03 with Python `requests` (User-Agent `twlab/0.1 (self-hosted research data pipeline)`,
one request every 3.5 s) from MOPS's legacy host — the new `mops.twse.com.tw` WAF-blocks or
404s these endpoints, the old application lives on at `mopsov.twse.com.tw`:

`GET https://mopsov.twse.com.tw/mops/web/ajax_t164sb<03|04|05>?encodeURIComponent=1&step=1&firstin=1&off=1&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=<id>&year=<roc year>&season=<season>`

Responses were `200 text/html; charset=UTF-8` (23–56 KB) and are stored unmodified.

| File | Provenance |
| --- | --- |
| `mops_t164sb0{3,4,5}_{2330,2317,1101}_115_1.html` | **Real recording.** Q1 2026 (Statutory Deadline 2026-05-15): 資產負債表 (`03`), 綜合損益表 (`04`), 現金流量表 (`05`) for 台積電, 鴻海, 台泥. |
| `mops_t164sb0{3,4,5}_{2330,2317,1101}_114_4.html` | **Real recording.** The 2025 annual report (season 4, deadline 2026-03-31): the income statement carries only `114年度` / `113年度`, the cash flow `114年度` / `113年度`. |
| `mops_t164sb0{4,5}_{2330,2317,1101}_114_3.html` | **Real recording.** Q3 2025 income (`114年第3季` + `114年01月01日至114年09月30日`) and cash flow (`114年01月01日至114年09月30日`) — the "previous season" pages the Q4 de-cumulation subtracts. |
| `mops_t164sb0{3,4,5}_2330_115_2.html` | **Real recording.** Q2 2026 for 2330: income with both `115年第2季` and `115年01月01日至115年06月30日`, cash flow year-to-date to 06-30 (de-cumulated against the `115_1` page). |
| `mops_t164sb04_2330_115_1_malformed.html` | Derived from the real 2330 Q1 income page: the `會計項目` header cell renamed to `科目`, simulating a silent source format change. Parsers must fail loudly on it. |
| `mops_t164sb03_2330_115_1_wrong_company.html` | Derived from the real 2330 Q1 balance sheet: the XBRL / e-book link ids (`CO_ID=2330`, `co_id=2330`) and the `本資料由台積電公司提供` heading changed to 2317 鴻海 — MOPS answering for another company. |
| `mops_t164sb03_2330_115_1_unbalanced.html` | Derived from the real 2330 Q1 balance sheet: `資產總額` / `負債及權益總計` changed from 8,660,949,685 to 8,060,949,685. Parses cleanly but must trip the balance-sheet identity Invariant. |

## What the real layout looks like

Each page is a `hasBorder` table captioned by two `<th colspan>` rows (`民國115年第1季`,
`單位：新台幣仟元`), then the `會計項目` header row with the period labels (colspan 2 over a
`金額` / `%` sub-header; the cash-flow statement has a single `金額` column per period),
then one row per account. Sub-items are indented with full-width spaces, values are
space-padded, comma-grouped 千元 with a leading minus for negatives, section headings are rows
with blank cells, and some labels repeat as a heading followed by an indented value row
(`其他收益及費損淨額`, `基本每股盈餘`). Q1–Q3 income pages carry both the three-month column
(`115年第1季`) and the year-to-date one (`115年01月01日至115年03月31日`); the annual page carries
only `114年度`; cash-flow pages are always year-to-date. There are no hidden form inputs — the
page identifies itself through the XBRL link (`CO_ID=2330&SYEAR=2026&SSEASON=1`), the e-book
link (`co_id=2330&year=115`) and the caption.

## Golden values

Golden values asserted in tests (e.g. TSMC `2330` Q1 2026 營業收入淨額 1,134,103,440 千元,
每股盈餘 22.08, 資產總額 8,660,949,685; single-quarter Q2 2026 營業活動之淨現金流入_流出
783,364,977 = YTD 1,482,341,242 − 698,976,265; single-quarter Q4 2025 營業收入淨額 1,046,090,421
= `114年度` − `114年01月01日至114年09月30日`) were taken from the FinMind Witness
(`TaiwanStockFinancialStatements`, `TaiwanStockBalanceSheet`, `TaiwanStockCashFlowsStatement`,
元 ÷ 1000) before the pages were recorded, and match the recordings exactly. 台泥 `1101` values
in the derived-sum test (e.g. 應收帳款及票據 = 4,085,885 + 23,524,066 + 428,294) are read off
its real balance sheet.
