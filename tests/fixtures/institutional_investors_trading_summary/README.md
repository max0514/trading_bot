# institutional_investors_trading_summary fixtures

Recorded source responses used by the parser (Seam 3) and pipeline (Seam 2) tests.
No test hits the live network.

| File | Provenance |
| --- | --- |
| `twse_t86_20260902.json` | **Real recording.** `GET https://www.twse.com.tw/rwd/zh/fund/T86?date=20260902&selectType=ALLBUT0999&response=json`, recorded 2026-09-03 through `twlab.http.PoliteSession.get_json` (Python `requests` with the twlab User-Agent — curl and a browser from the same IP get TWSE's WAF block page) and re-serialized as compact JSON; content unmodified. 1,338 securities, 19 columns (the Catalog's 15 Fields plus `證券代號`, `證券名稱` and the two totals `自營商買賣超股數` / `三大法人買賣超股數`). Cross-check: the `2330` row equals the FinMind Witness (`TaiwanStockInstitutionalInvestorsBuySell`, 2026-09-02) in all five investor groups. |
| `twse_t86_20260902_malformed.json` | Derived from the recording: `外陸資買進股數(不含外資自營商)` header renamed to `外資買進股數(不含外資自營商)`, simulating a silent source format change. Parsers must fail loudly on it. |
| `twse_t86_20260902_tiny.json` | Derived from the recording: truncated to 3 rows, the first renamed to poison Stock ID `9998`. Parses cleanly but must trip the row-count Invariant; `9998` lets tests prove Quarantined rows never reach published frames. |
| `tpex_insti_daily_trade_20260902.json` | **Real recording.** `GET https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade?type=Daily&sect=EW&date=2026/09/02&response=json`, recorded 2026-09-03 with Python `requests` and the twlab User-Agent (curl and a browser get Cloudflare 403). Unmodified: 892 securities in `tables[0]` (`tables[1]` is empty); a flat 24-column header — `代號`, `名稱`, seven *unnamed* `買進股數` / `賣出股數` / `買賣超股數` triples, `三大法人買賣超股數合計` — whose investor-group order (外資及陸資(不含外資自營商), 外資自營商, 外資及陸資合計, 投信, 自營商(自行買賣), 自營商(避險), 自營商合計) is fixed by position and holds arithmetically on every row (triple 3 = 1 + 2, triple 7 = 5 + 6, 合計 = nets of 3 + 4 + 7). The table date is in ROC form (`115/09/02`). |

Golden values asserted in tests (e.g. TSMC `2330` 外陸資買賣超 −11,986,983 股 and 中美晶 `5483`
−1,844,950 股 on 2026-09-02) come from these recordings.
