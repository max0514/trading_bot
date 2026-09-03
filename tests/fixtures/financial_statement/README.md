# financial_statement fixtures

MOPS-format per-company statement pages used by the parser (Seam 3) and pipeline (Seam 2)
tests. No test hits the live network.

**All files are synthesized, format-accurate.** MOPS (`mops.twse.com.tw`) answers every
request from the development network — curl, requests, and a real Chromium — with its
WAF block page (「因為安全性考量，您所執行的頁面無法呈現」, keyed on the client IP), so the
`mops/web/ajax_t164sb03` (資產負債表), `ajax_t164sb04` (綜合損益表) and `ajax_t164sb05`
(現金流量表) pages could not be recorded. The files follow those pages' documented layout:
UTF-8; the query echoed as hidden form inputs (`co_id`, `year`, `season`, …); the company
name, a `合併資產負債表` / `合併綜合損益表` / `合併現金流量表` heading and `單位：新台幣仟元`;
then one `hasBorder` table whose first column is `會計項目` and whose value columns carry
the period in the header — a two-row header (`115年第1季` / `金額` `%`; balance sheet
`115年03月31日`) for the balance sheet and income statement, a single-row header of
year-to-date ranges (`115年01月01日至115年06月30日`, no `%` column) for the cash-flow
statement. Sub-items are indented with `&nbsp;`, numbers are comma-grouped 千元 with a
leading minus for negatives (每股盈餘 in 元 with two decimals), and section headings are rows
with blank cells. Income-statement Q2/Q3 pages carry both the three-month and the cumulative
column, Q4 (`season=4`) is the annual report with only `114年度` / `113年度`, and cash-flow
pages are always cumulative — the semantics the parser's de-cumulation is built on.

| File | Content |
| --- | --- |
| `mops_t164sb0{3,4,5}_<id>_115_1.html` | Q1 2026 (Statutory Deadline 2026-05-15) for 2330, 2317 and 1101: one file per statement. |
| `mops_t164sb0{3,4,5}_<id>_114_4.html` | The 2025 annual report (season 4, deadline 2026-03-31) for the same three companies: the income statement shows only `114年度` / `113年度`, the cash flow `114年01月01日至114年12月31日`. |
| `mops_t164sb0{4,5}_<id>_114_3.html` | Q3 2025 income (`114年第3季` + `114年前三季`) and cash-flow pages — the "previous season" pages the Q4 de-cumulation subtracts. |
| `mops_t164sb0{3,4,5}_2330_115_2.html` | Q2 2026 for 2330 only: income with both `115年第2季` and `115年上半年度`, cash flow year-to-date to 06-30 (de-cumulated against the `115_1` page). |
| `mops_t164sb04_2330_115_1_malformed.html` | The `會計項目` header renamed to `科目` — a silent source format change the parser must reject. |
| `mops_t164sb03_2330_115_1_wrong_company.html` | Hidden `co_id` and the heading announce 2317 鴻海 while 2330 was requested. |
| `mops_t164sb03_2330_115_1_unbalanced.html` | `資產總額` / `負債及權益總計` changed from 8,660,949,685 to 8,060,949,685: parses cleanly but must trip the balance-sheet identity Invariant. |

## Provenance of the numbers

**2330 (台積電) and 2317 (鴻海): real Witness values.** Every line whose MOPS label has a
counterpart in the FinMind Witness — `TaiwanStockBalanceSheet`, `TaiwanStockFinancialStatements`
(single-quarter) and `TaiwanStockCashFlowsStatement` (cumulative), recorded 2026-09-03 for
2024 Q1 – 2026 Q2 — carries the Witness figure (元 ÷ 1000; EPS as reported). Cumulative
income columns (`上半年度`, `前三季`, `年度`) are sums of the single-quarter Witness values.
The real lines are:

- Balance sheet: 現金及約當現金, 透過損益按公允價值衡量之金融資產－流動 / －非流動, 透過其他綜合
  損益按公允價值衡量之金融資產－流動 / －非流動, 按攤銷後成本衡量之金融資產－流動 / －非流動,
  避險之金融資產－流動, 應收帳款淨額, 應收帳款－關係人淨額, 其他應收款淨額 (2317), 其他應收款－
  關係人淨額, 存貨, 預付款項 (2317), 其他流動資產, 流動資產合計, 採用權益法之投資, 不動產、廠房
  及設備, 使用權資產, 無形資產, 遞延所得稅資產, 其他非流動資產, 非流動資產合計, 資產總額, 短期
  借款 (2317), 透過損益按公允價值衡量之金融負債－流動, 避險之金融負債－流動 (2330), 應付帳款,
  應付帳款－關係人, 其他應付款, 本期所得稅負債, 負債準備－流動 (2317), 其他流動負債, 流動負債
  合計, 應付公司債, 長期借款, 其他非流動負債, 非流動負債合計, 負債總額, 普通股股本, 股本合計,
  資本公積合計, 法定盈餘公積, 未分配盈餘（或待彌補虧損）, 保留盈餘合計, 其他權益合計, 歸屬於母
  公司業主之權益合計, 非控制權益, 權益總額, 負債及權益總計, 母公司暨子公司所持有之母公司庫藏股
  股數.
- Income statement: 營業收入合計, 營業成本合計, 營業毛利（毛損）(and 淨額), 營業費用合計, 其他
  收益及費損淨額 (2330), 營業利益（損失）, 營業外收入及支出合計, 稅前淨利（淨損）, 所得稅費用
  （利益）合計, 繼續營業單位本期淨利（淨損）, 本期淨利（淨損）, 其他綜合損益（淨額）合計, 本期綜合
  損益總額, 淨利（淨損）歸屬於母公司業主 / 非控制權益, 綜合損益總額歸屬於非控制權益 (母公司 =
  total − that), 基本每股盈餘合計 (稀釋 copies it).
- Cash flow (year-to-date): 繼續營業單位稅前淨利（淨損）, 本期稅前淨利（淨損）, 折舊費用, 攤銷
  費用, 利息費用, 利息收入, 收益費損項目合計, 應收帳款（增加）減少, 存貨（增加）減少, 應付帳款
  增加（減少）, 營運產生之現金流入（流出）, 營業活動之淨現金流入（流出）, 取得不動產、廠房及設備,
  存出保證金減少 (2330), 其他投資活動, 投資活動之淨現金流入（流出）, 短期借款減少 (2317), 償還
  公司債, 舉借長期借款, 償還長期借款, 租賃本金償還, 支付之利息, 除列避險之金融負債 (2330), 其他
  非流動負債增加 / 減少 (2317), 籌資活動之淨現金流入（流出）, 本期現金及約當現金增加（減少）數,
  期初現金及約當現金餘額, 期末現金及約當現金餘額 (資產負債表帳列之現金及約當現金 copies it).

**Synthetic lines** (plausible, invented) fill the rest of each statement, and one line per
section is a residual "plug" so every section sums exactly to its real subtotal: balance
sheet 其他金融資產－流動 / －非流動, 一年或一營業週期內到期長期負債, 租賃負債－非流動, 預收股款,
特別盈餘公積, 庫藏股票 (2317: −15,194, the Witness reports only its 1,483,078-share count);
income statement 推銷費用 / 管理費用 (fixed shares of 營業費用合計), 研究發展費用 (residual),
預期信用減損損失（利益）(2317 only), 利息收入 / 其他收入 / 財務成本淨額 / 採用權益法認列之關聯企業
及合資損益之份額淨額 (fixed shares), 其他利益及損失淨額 (residual), the 其他綜合損益 sub-lines;
cash flow 透過損益按公允價值衡量金融資產及負債之淨損失（利益）, 股利收入, 採用權益法認列之關聯企業
及合資損失（利益）之份額, 處分及報廢不動產、廠房及設備損失（利益）, 未實現外幣兌換損失（利益）
(residual), 其他應收款 / 預付款項 / 其他流動資產（增加）減少, the 資產 / 負債之淨變動合計 lines,
其他應付款增加（減少）(residual), 退還（支付）之所得稅 (residual), 取得 / 處分透過其他綜合損益按
公允價值衡量之金融資產, 取得 / 處分按攤銷後成本衡量之金融資產 (residual), 處分不動產、廠房及設備,
取得無形資產, 收取之利息, 收取之股利, 短期借款增加 or 發放現金股利 (residual, by sign),
匯率變動對現金及約當現金之影響 (residual). Where the Witness's 普通股股本 exceeded its own
股本合計 (2330 at 2025-03-31 and 2026-03-31, by ~1.5 million) 普通股股本 was clipped to the
subtotal.

**1101 (模擬 台泥): fully synthetic** — a self-consistent model company generated from a
deterministic formula (revenue 30,000,000 千元 growing 2% a quarter), whose balance sheet
carries the lines the real two lack (應收票據淨額, 應付票據, 應付短期票券, 商譽, 投資性不動產淨額,
庫藏股票) so the derived-sum and alias Fields are exercised.

Golden values asserted in tests (e.g. TSMC `2330` Q1 2026 營業收入淨額 1,134,103,440 千元,
每股盈餘 22.08, 資產總額 8,660,949,685; single-quarter Q2 2026 營業活動之淨現金流入_流出
783,364,977 = YTD 1,482,341,242 − 698,976,265) are the Witness figures. **Re-record from the
real MOPS pages when network access allows, and delete this note.**
