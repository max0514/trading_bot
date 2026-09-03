# security_categories fixtures

ISIN-page fixtures used by the parser (Seam 3) and pipeline (Seam 2) tests. No test hits
the live network.

**All files are synthesized, format-accurate.** `isin.twse.com.tw` answers every request
from the development network with TWSE's WAF block page (「因為安全性考量，您所執行的頁面無法
呈現」, keyed on the client IP), so `C_public.jsp?strMode=2` (上市) and `strMode=4` (上櫃)
could not be recorded. The files follow that page's layout: MS950 bytes (decode them as
`PoliteSession.get_text(encoding="cp950")` does), a `charset=big5` meta, the
本國上市/上櫃證券國際證券辨識號碼一覽表 headings, and one `<table class='h4'>` whose header row
is 有價證券代號及名稱 / 國際證券辨識號碼(ISIN Code) / 上市日 (上櫃日) / 市場別 / 產業別 /
CFICode / 備註, with `colspan=7` section rows (股票, 上市認購(售)權證, ETF, 臺灣存託憑證,
受益證券, ETN, 創新板股票) and the code and name separated by a full-width space
(`2330　台積電`).

Real content: Stock IDs, names and 產業別 are the FinMind Witness's `TaiwanStockInfo`
(recorded 2026-09-03), so every 4-digit Stock ID of the real `price/twse_mi_index_20260807.json`
recording is on the 上市 page; ISIN codes follow the real TW scheme (Luhn check digit,
verified against TW0002330008 / TW0000050004 / TW0001101004). Where the Witness lists several
categories for one stock (e.g. 2330: 半導體業 and the umbrella 電子工業) the specific one is
used; the Witness's stale 上櫃 entries for the 11 stocks that transferred to 上市 are dropped
from the 上櫃 page; category spelling is the Witness's and may differ from the ISIN page's own
(e.g. 金融保險 vs 金融保險業). Placeholders: 上市日/上櫃日 (deterministic pseudo-dates),
CFICode (per-section constants), and the five 上市 warrant rows (030001, 030002, 031506,
03500P, 086001).

| File | Content |
| --- | --- |
| `isin_c_public_strmode2.html` | 上市: 1,215 股票 (preferred shares such as 2881A included), 5 warrants, 271 ETF, 36 臺灣存託憑證, 8 受益證券, 28 ETN, 36 創新板股票. |
| `isin_c_public_strmode4.html` | 上櫃: 927 股票, 36 warrants, 151 ETF, 20 ETN. |
| `isin_c_public_strmode2_malformed.html` | `產業別` header renamed to `行業別` — a silent source format change the parser must reject. |
| `isin_c_public_strmode2_tiny.html` | Header, the 股票 section row and 5 stocks: parses cleanly but must trip the row-count Invariant. |

Golden values asserted in tests (2330 → 台積電 / 半導體業 / sii; 5483 → 中美晶 / 半導體業 / otc;
0050 → ETF; 9103 → 存託憑證) are the Witness's. **Re-record from the real pages when network
access allows, and delete this note.**
