# institutional_investors_trading_summary fixtures

Source-format responses used by the parser (Seam 3) and pipeline (Seam 2) tests. No test
hits the live network.

**All files are synthesized, format-accurate.** TWSE's WAF blocks `fund/T86` for the
development network's IP: every variant tried on 2026-09-03 — curl with the twlab UA, a
browser UA with referer, the legacy non-rwd URL, and a real Chromium — got the
「因為安全性考量，您所執行的頁面無法呈現」 page (HTTP 307 with a `rule: T86` header), while
`afterTrading/MI_INDEX` and `BWIBBU_d` on the same host answered normally. tpex.org.tw sits
behind Cloudflare (403). The files follow each endpoint's shape; the 三大法人 figures come
from the FinMind Witness (`TaiwanStockInstitutionalInvestorsBuySell`, 2026-09-01, recorded
2026-09-03) wherever a per-stock pull was made, and are deterministic inventions elsewhere.

| File | Content |
| --- | --- |
| `twse_t86_20260901.json` | Shape of `GET https://www.twse.com.tw/rwd/zh/fund/T86?date=20260901&selectType=ALLBUT0999&response=json`: the rwd single-table envelope (`stat`/`date`/`title`/`fields`/`data`/`notes`, comma-grouped 股 counts, space-padded `證券名稱`) with T86's 19 columns — the Catalog's 15 Fields plus `證券代號`, `證券名稱` and the two totals `自營商買賣超股數` and `三大法人買賣超股數` that are *not* Fields. The 1,157 rows are traded securities of the real `price/twse_mi_index_20260807.json` recording (real Stock IDs and names). 59 rows carry Witness values: 0050, 0056, 00631L, 00878, 00919, 00929, 006208, 1101, 1216, 1301, 1303, 1326, 1402, 1605, 1795, 2002, 2105, 2207, 2303, 2308, 2317, 2327, 2330, 2345, 2357, 2379, 2382, 2383, 2395, 2408, 2409, 2412, 2454, 2603, 2609, 2615, 2801, 2880, 2881, 2882, 2883, 2884, 2885, 2886, 2887, 2890, 2891, 2892, 2912, 3008, 3034, 3037, 3231, 3481, 3711, 5880, 6446, 6669, 9910. The rest are PRNG values scaled off each security's real 2026-08-07 volume, with 買賣超 = 買進 − 賣出 and the totals consistent. |
| `twse_t86_20260901_malformed.json` | `外陸資買進股數(不含外資自營商)` header renamed to `外資買進股數(不含外資自營商)`, simulating a silent source format change. Parsers must fail loudly on it. |
| `twse_t86_20260901_tiny.json` | Truncated to 3 rows, the first renamed to poison Stock ID `9998`. Parses cleanly but must trip the row-count Invariant; `9998` lets tests prove Quarantined rows never reach published frames. |
| `tpex_insti_daily_trade_20260901.json` | Shape of `GET https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade?type=Daily&sect=EW&date=2026/09/01&response=json` (`tables[].fields/data`, TPEx's own hyphenated column names such as `外資及陸資(不含外資自營商)-買進股數`; 24 columns including the `外資及陸資` / `自營商` subtotals and `三大法人買賣超股數合計`). 20 real 上櫃 codes (3081, 3105, 3227, 3260, 3293, 3529, 4123, 4966, 4979, 5274, 5347, 5483, 6180, 6182, 6488, 6510, 6547, 8069, 8086, 8299) with names from the Witness's `TaiwanStockInfo`; every value is the Witness's for 2026-09-01. |

Golden values asserted in tests (e.g. TSMC `2330` 外陸資買賣超 5,730,863 股 and 中美晶 `5483`
4,974,118 股 on 2026-09-01) are the Witness figures. **Re-record from the real endpoints when
network access allows, and delete this note.**
