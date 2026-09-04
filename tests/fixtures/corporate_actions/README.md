# corporate_actions fixtures

Source responses for the six Corporate Action Event Tables (`dividend_tse`, `dividend_otc`,
`capital_reduction_tse`, `capital_reduction_otc`, `par_value_change_tse`, `par_value_change_otc`),
used by the parser (Seam 3), pipeline (Seam 2), data API (Seam 1), and `etl:adj_*` golden tests.
No test hits the live network.

**Every source response here is a real recording**, made 2026-09-03 with Python `requests` and
the twlab User-Agent (curl and browsers are blocked by TWSE's WAF and TPEx's Cloudflare; `requests`
is not). Files are byte-for-byte as served, except the one `_malformed` derivative noted below.

| File | Provenance |
| --- | --- |
| `twse_twt49u_20260601_20260902.json` | **Real recording.** `GET https://www.twse.com.tw/rwd/zh/exRight/TWT49U?startDate=20260601&endDate=20260902&response=json` — 916 除權息 events. Dates print as `115年06月11日`; `詳細資料` is `code,yyyymmdd`; the 季別 cell carries the static MOPS link in parentheses (the parser strips it); ETF rows leave the filing cells blank. |
| `twse_twt49u_20260601_20260902_malformed.json` | Derived from the recording above: `除權息參考價` renamed to `除權息參考價格`, simulating a silent source format change. Parsers must fail loudly on it. |
| `twse_twt49u_20260829_20260830_empty.json` | **Real recording.** Same endpoint for a weekend window: `{"stat":"很抱歉，沒有符合條件的資料!"}` → empty batch (`no_data`). |
| `tpex_exdailyq_20260601_20260902.json` | **Real recording.** `GET https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ?startDate=2026/06/01&endDate=2026/09/02&response=json` — 687 上櫃 除權息 events. Dates print as `115/06/01`; names are space-padded; several columns are named differently from the Catalog (`權值+息值`, `漲停價`, `跌停價`, `開始交易基準價`, `每仟股無償配股`, `原股東認購股數`, `按持股比例仟股認購`), mapped by name in the parser. |
| `tpex_exdailyq_20260829_20260830_empty.json` | **Real recording.** Same endpoint for a weekend window: `tables[0].data` is `[]`. |
| `twse_twtauu_20260101_20260902.json` | **Real recording.** `GET https://www.twse.com.tw/rwd/zh/reducation/TWTAUU?startDate=20260101&endDate=20260902&response=json` — 4 減資 events. Dates print as `115/01/12` (unlike TWT49U); the name column is `名稱`; `除權參考價` is `--`. |
| `twse_twtauu_20260829_20260830_empty.json` | **Real recording.** Same endpoint, weekend window → `沒有符合條件的資料` stat. |
| `tpex_revivt_20260101_20260902.json` | **Real recording.** `GET https://www.tpex.org.tw/www/zh-tw/bulletin/revivt?startDate=2026/01/01&endDate=2026/09/02&response=json` — 5 上櫃 減資 events. Dates print as the 7-digit ROC `1150112`; the source column is `最後交易日之收盤價格` (Catalog: `最後交易之收盤價格`); `詳細資料` is an HTML fragment (not a Catalog Field, not emitted). |
| `twse_twtb8u_20200101_20260902.json` | **Real recording.** `GET https://www.twse.com.tw/rwd/zh/change/TWTB8U?startDate=20200101&endDate=20260902&response=json` — all 9 上市 面額變更 events since 2020. Note the `change` controller: `reducation/TWTB8U` is a TWSE 404 page. |
| `twse_twtb8u_20260829_20260830_empty.json` | **Real recording.** Same endpoint, weekend window: `stat` `OK` with `data: []` (this table does not use the 沒有符合條件 stat). |
| `tpex_pvchgrslt_20190101_20260902.json` | **Real recording.** `GET https://www.tpex.org.tw/www/zh-tw/bulletin/pvChgRslt?startDate=2019/01/01&endDate=2026/09/02&response=json` — all 14 上櫃 面額變更 events since 2019. Columns are `證券代號`/`證券名稱` and `恢復買賣開始參考價` (Catalog: `恢復買賣開始日參考價`). |

## How the TPEx endpoint names were found

TPEx's JSON endpoints are not documented. These guessed paths all return TPEx's own
"404 - 證券櫃檯買賣中心" page (or a transient Cloudflare 520): `bulletin/exRight`, `exRights`,
`exDaily`, `exRightDaily`, `exRightQuery`, `exRgt`, `exdaily`, `exRightsDaily`, `exright`,
`exRightInfo`, `exRightList`, `exDividend`, `exRightResult`, `dividend`, `exrights`,
`preRemuneration`, `dailyquo`; and for 面額: `parValue`, `parValueChange`, `parValueChg`,
`parvalue`, `pvChange`, `pvchg`, `parChange`, `changePar`, `chgParValue`, `faceValue`,
`faceValueChange`, `parValueDaily`, `parChg`, `stockPar`, `denomination`, `revivtPar`,
`parvalueRevivt`, `pvDailyQ`, `chgPar`, `facevalue` (and more). The real names come from the
site itself: `/data/menu/zh-tw/menu.json` lists the announcement pages
(`/zh-tw/announce/market/ex/cal.html`, `…/reduction/reference.html`,
`…/change/reference.html`), and each page initializes its table with
`tables.init({action: "bulletin/exDailyQ" | "bulletin/revivt" | "bulletin/pvChgRslt"})`.
(`bulletin/decap` and `bulletin/prePost` are the 減資/除權息 *預告* tables, not used here.)

## Witness cross-checks

Golden values asserted in tests are read from these recordings and agree exactly with the
FinMind Witness: 2330 台積電 ex-dividend 2026-06-11 (2,255.00 → 2,248.99, 息值 6.000035,
limits 2,470/2,025, basis 2,250; `TaiwanStockDividendResult`), 5483 中美晶 2026-07-23
(234.00 → 231.50) and 6488 環球晶 2026-07-16 (1,480.00 → 1,474.30), 2380 虹光 capital
reduction resumed 2026-06-29 (6.60 → 23.86, limits 26.20/21.50, basis 23.85, 彌補虧損) and
1414 東和 2026-01-12 (17.45 → 18.27, 退還股款; `TaiwanStockCapitalReductionReferencePrice`).

The `etl:adj_*` golden tests seed `price` through the real price pipeline by re-dating the
recorded `tests/fixtures/price/twse_mi_index_20260807.json` and setting 2330's and 2380's OHLC
for 2026-06-09..06-12, 06-16, 06-29 and 06-30 to the Witness's `TaiwanStockPrice` quotes (all
other securities keep the recording's 2026-08-07 values). They assert that TSMC's adjusted close
the day before the ex-date equals 2,255.00 × 2,248.99/2,255.00 = 2,248.99 (the Witness's
`after_price`) and that 2380's pre-halt close 6.60 scales up to 23.86.
