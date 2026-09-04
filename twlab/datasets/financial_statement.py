"""`financial_statement` Dataset: every listed/OTC company's quarterly IFRS statements.

Official Source: MOPS per-company IFRS statement pages (公開資訊觀測站) — one GET
per statement per company per quarter, fetched with `session.get_text`:

    資產負債表  https://mopsov.twse.com.tw/mops/web/ajax_t164sb03
    綜合損益表  https://mopsov.twse.com.tw/mops/web/ajax_t164sb04
    現金流量表  https://mopsov.twse.com.tw/mops/web/ajax_t164sb05

each with `...&co_id=<Stock ID>&year=<ROC year>&season=<1-4>` (see
`query_params`). The host is MOPS's legacy site: when the new
mops.twse.com.tw launched, the old application moved to mopsov.twse.com.tw
and the new host WAF-blocks or 404s these ajax endpoints; mopsov serves them
as `text/html; charset=UTF-8` to a plain identifiable client.

Page layout (as recorded in tests/fixtures/financial_statement/): a `hasBorder`
table captioned by two `<th colspan>` rows (`民國115年第1季`, `單位：新台幣仟元`),
then the `會計項目` header row whose other cells carry the period labels
(colspan 2 over a `金額` / `%` sub-header; the cash-flow statement has `金額`
only), then one row per account, sub-items indented with full-width spaces,
values space-padded and comma-grouped, negatives with a leading minus. Values
are 千元 exactly as MOPS publishes and FinLab serves them; 每股盈餘 is 元. Section
headings are rows with blank cells, and MOPS repeats some labels as a heading
followed by an indented value row (其他收益及費損淨額, 基本每股盈餘), so labels
are merged position-wise with the first non-empty value winning. There are no
hidden form inputs; the page identifies itself through its XBRL link
(`CO_ID=2330&SYEAR=2026&SSEASON=1`), its e-book link (`co_id=2330&year=115`)
and the caption, all of which parse() checks so MOPS can never silently serve
another company or period. 財務成本淨額 is printed as a positive amount inside
營業外收入及支出 and is passed through as printed.

Universe
--------
fetch(session, day, universe) receives the Stock IDs of the materialized
`price:收盤價` frame (DatasetSpec.universe_from) and keeps only company codes —
four digits with a non-zero first digit. ETFs (00xx, 0050…), ETNs, warrants and
TDRs never file statements and are dropped. `day` is the batch day; the
(year, quarter) fetched is `latest_quarter_due(day)`, i.e. the most recent
quarter whose Statutory Deadline is on or before `day`. On the deadline itself
that is the quarter the deadline closes; on any other day the fallback makes a
late or manual run collect the right quarter instead of failing.

Flow-item semantics: single quarters
-------------------------------------
FinLab serves SINGLE-QUARTER values for income-statement and cash-flow Fields
(strategies do `.rolling(4).sum()` for TTM) and point-in-time values for
balance-sheet Fields. MOPS, however, prints:

* income statement — Q1–Q3: the three-month column (`115年第2季`) AND the
  year-to-date one (`115年01月01日至115年06月30日`); Q4 (`season=4`): the annual
  report with only the full year (`114年度`);
* cash flow — ALWAYS year-to-date columns (`115年01月01日至115年06月30日`, and
  `114年度` on the annual report).

parse() therefore takes the three-month income column when the page has one
(Q1–Q3), and otherwise de-cumulates: single = YTD(season) − YTD(season − 1).
The previous season's page is bundled by fetch() into the same raw dict —
`prev_income` for Q4, `prev_cash_flow` for Q2–Q4 — so a raw dict is
self-contained: {"stock_id", "year", "season", "balance_sheet", "income",
"cash_flow", "prev_income"?, "prev_cash_flow"?}. A line item present in one
year-to-date page but absent from the other counts as 0 there. Cash balances
are not flows: 期末現金及約當現金餘額 and 資產負債表帳列之現金及約當現金 are the
quarter-end balances as printed, and 期初現金及約當現金餘額 is the previous
quarter's closing balance (the page's own opening balance for Q1), so
期末 − 期初 = 本期現金及約當現金增加_減少_數 holds per quarter. If MOPS has no
page for the previous season (a company that first filed mid-year), the
year-to-date value stands in and is, in fact, the whole reported history.

Point-in-Time Alignment
-----------------------
Rows are stored under the quarter-end period date (e.g. 2026-03-31 for Q1);
materialization re-dates them to the Statutory Deadline — 5/15, 8/14, 11/14,
3/31 — via `align_quarterly`, so Q1 2026 is first visible at the 2026-05-15
index and the 2025 annual report at 2026-03-31, while the system of record
keeps the period. parse() never re-dates.

Run size
--------
A full-market run is ~1,900 companies × 3–5 pages ≈ 8k polite requests — about
seven hours at PoliteSession's 3-second interval — acceptable for a quarterly
Cadence. Companies MOPS reports no data for (「查無所需資料」) contribute no row;
any other deviation raises ParseError and Quarantines the batch.

MOPS name → Catalog Field mapping
---------------------------------
Every one of the Catalog's 158 Fields is mapped explicitly below, grouped by
statement, from the MOPS display label(s) that carry it. Labels are compared
after `normalize_account`, which folds MOPS punctuation (full-width dashes and
parentheses, 、 and ／, whitespace) into `_` — the Catalog's own convention
(MOPS '營業活動之淨現金流入（流出）' ↔ Catalog '營業活動之淨現金流入_流出'). Where
the Catalog keeps an older or FinLab-specific name the mapping is a deliberate
alias: 合併總損益 ← 本期淨利（淨損）; 股東權益總額 ← 權益總額; 母公司股東權益合計 ←
歸屬於母公司業主之權益合計; 歸屬母公司淨利_損 ← the attribution row 母公司業主
（淨利／損）(likewise 非控制權益 / 共同控制下前手權益, and the （綜合損益） rows);
採權益法之長期股權投資 ← 採用權益法之投資; 應付商業本票∕承兌匯票 ← 應付短期票券;
銀行借款_非流動 ← 長期借款; 應計退休金負債 ← 淨確定福利負債－非流動; 遞延所得稅
(liability side) ← 遞延所得稅負債; 一年內到期長期負債 ← 一年或一營業週期內到期長期
負債; 呆帳費用提列_轉列收入_數 ← MOPS's combined 預期信用減損損失（利益）數／呆帳費用
提列（轉列收入）數 line. Three Fields are sums of MOPS lines (`Sum`): 應收帳款及
票據, 應付帳款及票據 (notes + accounts, incl. related parties) and 商譽及無形資產
合計 (無形資產 + 商譽). Two Fields are pre-IFRS concepts with no IFRS line —
遞延資產合計 and 遞延貸項 — and map only to a same-named line, so they are None
for IFRS-era filings. A company that does not report a line (no 短期借款, no
停業單位損益) gets None, never an error.
"""
from __future__ import annotations

import datetime as dt
import math
import re
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd
from lxml import etree as lxml_etree
from lxml import html as lxml_html

from twlab import qa
from twlab.errors import ParseError
from twlab.http import PoliteSession
from twlab.spec import Cadence, DatasetSpec, align_quarterly, latest_quarter_due, quarter_end

# The legacy host: the ajax_t164sb* endpoints live on mopsov.twse.com.tw (the old
# site moved there when the new mops.twse.com.tw launched; the new host WAF-blocks
# or 404s these paths). Pages are served as text/html; charset=UTF-8.
MOPS_HOST = "https://mopsov.twse.com.tw"
MOPS_URL = MOPS_HOST + "/mops/web/ajax_t164sb{page}"
STATEMENT_URLS = {
    "balance_sheet": MOPS_URL.format(page="03"),
    "income": MOPS_URL.format(page="04"),
    "cash_flow": MOPS_URL.format(page="05"),
}
# The per-company pages carry no exchange; join price / security_categories for it.
MARKET = "MOPS"   # the Official Source, not an exchange — docs/catalog-deviations.md
ACCOUNT_COLUMN = "會計項目"
AMOUNT_COLUMN = "金額"
# MOPS's explicit "nothing filed for this company/period" answers.
NO_DATA_MARKERS = ("查無所需資料", "查無資料", "資料庫中查無需求資料", "無應編製財務報告")
BALANCE_TOLERANCE = 0.005        # 資產總額 vs 負債總額 + 股東權益總額
GROSS_PROFIT_TOLERANCE = 0.005   # 營業毛利 vs 營業收入淨額 − 營業成本, relative to revenue

_COMPANY_RE = re.compile(r"[1-9]\d{3}")


@dataclass(frozen=True)
class Sum:
    """A Field that is the sum of several MOPS lines (absent lines count as 0;
    None when none of them is present)."""
    components: tuple[str, ...]


# ── MOPS 會計項目 → Catalog Field, in Catalog order ─────────────────────────
# Field: tuple of MOPS labels tried in order (first non-empty value wins), or Sum.

_BALANCE_SHEET: dict[str, tuple[str, ...] | Sum] = {
    "現金及約當現金": ("現金及約當現金",),
    "透過損益按公允價值衡量之金融資產_流動": ("透過損益按公允價值衡量之金融資產－流動",),
    "透過其他綜合損益按公允價值衡量之金融資產_流動": ("透過其他綜合損益按公允價值衡量之金融資產－流動",),
    "按攤銷後成本衡量之金融資產_流動": ("按攤銷後成本衡量之金融資產－流動",),
    "避險之金融資產_流動": ("避險之金融資產－流動",),
    "合約資產_流動": ("合約資產－流動",),
    "應收帳款及票據": Sum(("應收票據淨額", "應收票據－關係人淨額", "應收帳款淨額", "應收帳款－關係人淨額")),
    "其他應收款": ("其他應收款淨額", "其他應收款"),
    "存貨": ("存貨",),
    "待出售非流動資產": ("待出售非流動資產（或處分群組）淨額", "待出售非流動資產"),
    "當期所得稅資產_流動": ("本期所得稅資產", "當期所得稅資產－流動", "當期所得稅資產"),
    "其他流動資產": ("其他流動資產",),
    "流動資產": ("流動資產合計", "流動資產"),
    "透過損益按公允價值衡量之金融資產_非流動": ("透過損益按公允價值衡量之金融資產－非流動",),
    "透過其他綜合損益按公允價值衡量之金融資產_非流動": ("透過其他綜合損益按公允價值衡量之金融資產－非流動",),
    "按攤銷後成本衡量之金融資產_非流動": ("按攤銷後成本衡量之金融資產－非流動",),
    "避險之金融資產_非流動": ("避險之金融資產－非流動",),
    "合約資產_非流動": ("合約資產－非流動",),
    "採權益法之長期股權投資": ("採用權益法之投資", "採用權益法之投資淨額"),
    "預付投資款": ("預付投資款",),
    "不動產廠房及設備": ("不動產、廠房及設備",),
    "商譽及無形資產合計": Sum(("無形資產", "商譽")),
    "遞延所得稅資產": ("遞延所得稅資產",),
    "遞延資產合計": ("遞延資產合計", "遞延資產"),                      # pre-IFRS; None under IFRS
    "使用權資產": ("使用權資產",),
    "投資性不動產淨額": ("投資性不動產淨額", "投資性不動產"),
    "其他非流動資產": ("其他非流動資產",),
    "非流動資產": ("非流動資產合計", "非流動資產"),
    "資產總額": ("資產總額", "資產總計"),
    "短期借款": ("短期借款",),
    "應付商業本票∕承兌匯票": ("應付短期票券", "應付商業本票／承兌匯票"),
    "透過損益按公允價值衡量之金融負債_流動": ("透過損益按公允價值衡量之金融負債－流動",),
    "避險之金融負債_流動": ("避險之金融負債－流動",),
    "按攤銷後成本衡量之金融負債_流動": ("按攤銷後成本衡量之金融負債－流動",),
    "合約負債_流動": ("合約負債－流動",),
    "應付帳款及票據": Sum(("應付票據", "應付票據－關係人", "應付帳款", "應付帳款－關係人")),
    "其他應付款": ("其他應付款",),
    "當期所得稅負債": ("本期所得稅負債", "當期所得稅負債"),
    "負債準備_流動": ("負債準備－流動",),
    "與待出售非流動資產直接相關之負債": ("與待出售非流動資產直接相關之負債",
                                        "與待出售非流動資產（或處分群組）直接相關之負債"),
    "租賃負債─流動": ("租賃負債－流動",),
    "一年內到期長期負債": ("一年或一營業週期內到期長期負債", "一年內到期長期負債"),
    "特別股負債_流動": ("特別股負債－流動",),
    "流動負債": ("流動負債合計", "流動負債"),
    "透過損益按公允價值衡量之金融負債_非流動": ("透過損益按公允價值衡量之金融負債－非流動",),
    "避險之金融負債_非流動": ("避險之金融負債－非流動",),
    "按攤銷後成本衡量之金融負債_非流動": ("按攤銷後成本衡量之金融負債－非流動",),
    "合約負債_非流動": ("合約負債－非流動",),
    "特別股負債_非流動": ("特別股負債－非流動",),
    "應付公司債_非流動": ("應付公司債", "應付公司債－非流動"),
    "銀行借款_非流動": ("長期借款", "銀行借款－非流動", "長期銀行借款"),
    "租賃負債_非流動": ("租賃負債－非流動",),
    "負債準備_非流動": ("負債準備－非流動",),
    "遞延貸項": ("遞延貸項", "遞延貸項－非流動"),                       # pre-IFRS; None under IFRS
    "應計退休金負債": ("淨確定福利負債－非流動", "應計退休金負債"),
    "遞延所得稅": ("遞延所得稅負債",),
    "非流動負債": ("非流動負債合計", "非流動負債"),
    "負債總額": ("負債總額", "負債總計"),
    "普通股股本": ("普通股股本",),
    "特別股股本": ("特別股股本",),
    "預收股款": ("預收股款",),
    "待分配股票股利": ("待分配股票股利",),
    "換股權利證書": ("換股權利證書",),
    "股本": ("股本合計", "股本"),
    "資本公積合計": ("資本公積合計", "資本公積"),
    "法定盈餘公積": ("法定盈餘公積",),
    "未分配盈餘": ("未分配盈餘（或待彌補虧損）", "未分配盈餘"),
    "保留盈餘": ("保留盈餘合計", "保留盈餘"),
    "其他權益": ("其他權益合計", "其他權益"),
    "庫藏股票帳面值": ("庫藏股票",),
    "母公司股東權益合計": ("歸屬於母公司業主之權益合計", "歸屬於母公司業主之權益"),
    "共同控制下前手權益": ("共同控制下前手權益",),
    "合併前非屬共同控制股權": ("合併前非屬共同控制股權",),
    "非控制權益": ("非控制權益",),
    "股東權益總額": ("權益總額", "權益總計"),
    "負債及股東權益總額": ("負債及權益總計", "負債及權益總額"),
}

_INCOME: dict[str, tuple[str, ...] | Sum] = {
    "營業收入淨額": ("營業收入合計", "營業收入", "營業收入淨額"),
    "營業成本": ("營業成本合計", "營業成本"),
    "營業毛利": ("營業毛利（毛損）", "營業毛利"),
    "營業費用": ("營業費用合計", "營業費用"),
    "研究發展費": ("研究發展費用", "研究發展費"),
    "推銷費用": ("推銷費用",),
    "管理費用": ("管理費用",),
    "預期信用減損_損失_利益_營業費用": ("預期信用減損損失（利益）", "預期信用減損（損失）利益－營業費用"),
    "其他收益及費損淨額": ("其他收益及費損淨額",),
    "營業利益": ("營業利益（損失）", "營業利益"),
    "財務成本": ("財務成本淨額", "財務成本"),
    "採權益法之關聯企業及合資損益之份額": ("採用權益法認列之關聯企業及合資損益之份額淨額",
                                        "採用權益法認列之關聯企業及合資損益之份額"),
    "營業外收入及支出": ("營業外收入及支出合計", "營業外收入及支出"),
    "稅前淨利": ("稅前淨利（淨損）",),
    "所得稅費用": ("所得稅費用（利益）合計", "所得稅費用（利益）"),
    "繼續營業單位損益": ("繼續營業單位本期淨利（淨損）",),
    "停業單位損益": ("停業單位損益",),
    "合併前非屬共同控制股權損益": ("合併前非屬共同控制股權損益",),
    "合併總損益": ("本期淨利（淨損）",),
    "本期綜合損益總額": ("本期綜合損益總額",),
    "歸屬母公司淨利_損": ("母公司業主（淨利／損）", "淨利（淨損）歸屬於母公司業主"),
    "歸屬非控制權益淨利_損": ("非控制權益（淨利／損）", "淨利（淨損）歸屬於非控制權益"),
    "歸屬共同控制下前手權益淨利_損": ("共同控制下前手權益（淨利／損）", "淨利（淨損）歸屬於共同控制下前手權益"),
    "綜合損益歸屬母公司": ("母公司業主（綜合損益）", "綜合損益總額歸屬於母公司業主"),
    "綜合損益歸屬非控制權益": ("非控制權益（綜合損益）", "綜合損益總額歸屬於非控制權益"),
    "綜合損益歸屬共同控制下前手權益": ("共同控制下前手權益（綜合損益）", "綜合損益總額歸屬於共同控制下前手權益"),
    "每股盈餘": ("基本每股盈餘", "基本每股盈餘合計"),
}

_CASH_FLOW: dict[str, tuple[str, ...] | Sum] = {
    "繼續營業單位稅前淨利_淨損": ("繼續營業單位稅前淨利（淨損）",),
    "本期稅前淨利_淨損": ("本期稅前淨利（淨損）",),
    "折舊費用": ("折舊費用",),
    "攤銷費用": ("攤銷費用",),
    "呆帳費用提列_轉列收入_數": ("預期信用減損損失（利益）數／呆帳費用提列（轉列收入）數",
                          "呆帳費用提列（轉列收入）數", "預期信用減損損失（利益）數"),
    "透過損益按公允價值衡量金融資產及負債之淨損失_利益": ("透過損益按公允價值衡量金融資產及負債之淨損失（利益）",),
    "利息費用": ("利息費用",),
    "利息收入": ("利息收入",),
    "股利收入": ("股利收入",),
    "採用權益法認列之關聯企業及合資損失_利益_之份額": ("採用權益法認列之關聯企業及合資損失（利益）之份額",),
    "處分及報廢不動產_廠房及設備損失_利益": ("處分及報廢不動產、廠房及設備損失（利益）",),
    "處分無形資產損失_利益": ("處分無形資產損失（利益）",),
    "處分投資損失_利益": ("處分投資損失（利益）",),
    "非金融資產減損迴轉利益": ("非金融資產減損迴轉利益",),
    "未實現銷貨利益_損失": ("未實現銷貨利益（損失）",),
    "已實現銷貨損失_利益": ("已實現銷貨損失（利益）",),
    "未實現外幣兌換損失_利益": ("未實現外幣兌換損失（利益）",),
    "收益費損項目合計": ("收益費損項目合計",),
    "應收帳款_增加_減少": ("應收帳款（增加）減少",),
    "應收帳款_關係人_增加_減少": ("應收帳款－關係人（增加）減少",),
    "存貨_增加_減少": ("存貨（增加）減少",),
    "與營業活動相關之資產之淨變動合計": ("與營業活動相關之資產之淨變動合計",),
    "應付帳款增加_減少": ("應付帳款增加（減少）",),
    "應付帳款_關係人增加_減少": ("應付帳款－關係人增加（減少）",),
    "與營業活動相關之負債之淨變動合計": ("與營業活動相關之負債之淨變動合計",),
    "營運產生之現金流入_流出": ("營運產生之現金流入（流出）",),
    "退還_支付_之所得稅": ("退還（支付）之所得稅",),
    "營業活動之淨現金流入_流出": ("營業活動之淨現金流入（流出）",),
    "取得透過其他綜合損益按公允價值衡量之金融資產": ("取得透過其他綜合損益按公允價值衡量之金融資產",),
    "處分透過其他綜合損益按公允價值衡量之金融資產": ("處分透過其他綜合損益按公允價值衡量之金融資產",),
    "取得不動產_廠房及設備": ("取得不動產、廠房及設備",),
    "處分不動產_廠房及設備": ("處分不動產、廠房及設備",),
    "取得無形資產": ("取得無形資產",),
    "處分無形資產": ("處分無形資產",),
    "收取之利息": ("收取之利息",),
    "收取之股利": ("收取之股利",),
    "其他投資活動": ("其他投資活動",),
    "投資活動之淨現金流入_流出": ("投資活動之淨現金流入（流出）",),
    "短期借款增加": ("短期借款增加",),
    "短期借款減少": ("短期借款減少",),
    "應付短期票券增加": ("應付短期票券增加",),
    "應付短期票券減少": ("應付短期票券減少",),
    "發行公司債": ("發行公司債",),
    "償還公司債": ("償還公司債",),
    "舉借長期借款": ("舉借長期借款",),
    "償還長期借款": ("償還長期借款",),
    "存入保證金增加": ("存入保證金增加",),
    "存入保證金減少": ("存入保證金減少",),
    "發放現金股利": ("發放現金股利",),
    "支付之利息": ("支付之利息",),
    "籌資活動之淨現金流入_流出": ("籌資活動之淨現金流入（流出）",),
    "本期現金及約當現金增加_減少_數": ("本期現金及約當現金增加（減少）數",),
    "期初現金及約當現金餘額": ("期初現金及約當現金餘額",),
    "期末現金及約當現金餘額": ("期末現金及約當現金餘額",),
    "資產負債表帳列之現金及約當現金": ("資產負債表帳列之現金及約當現金",),
}

# Cash-flow Fields that are balances, not flows (never de-cumulated).
_CLOSING_BALANCE_FIELDS = frozenset({"期末現金及約當現金餘額", "資產負債表帳列之現金及約當現金"})
_OPENING_BALANCE_FIELD = "期初現金及約當現金餘額"

BALANCE_SHEET_FIELDS = list(_BALANCE_SHEET)
INCOME_FIELDS = list(_INCOME)
CASH_FLOW_FIELDS = list(_CASH_FLOW)
# The 158 value Fields of the Dataset, named exactly as the Catalog does.
FIELDS = [*BALANCE_SHEET_FIELDS, *INCOME_FIELDS, *CASH_FLOW_FIELDS]
COLUMNS = ["stock_id", "date", "market", *FIELDS]

_PUNCT_RE = re.compile(r"[\s　\xa0()（）、，,\-－—–─／/∕：:]+")


def normalize_account(name: Any) -> str:
    """Fold MOPS punctuation into the Catalog's `_` convention:
    '營業活動之淨現金流入（流出）' → '營業活動之淨現金流入_流出'."""
    return _PUNCT_RE.sub("_", str(name)).strip("_")


def _lookup(table: dict[str, tuple[str, ...] | Sum]) -> dict[str, tuple[str, ...] | Sum]:
    out: dict[str, tuple[str, ...] | Sum] = {}
    for field, source in table.items():
        if isinstance(source, Sum):
            out[field] = Sum(tuple(normalize_account(c) for c in source.components))
        else:
            out[field] = tuple(normalize_account(a) for a in source)
    return out


_BALANCE_SHEET_LOOKUP = _lookup(_BALANCE_SHEET)
_INCOME_LOOKUP = _lookup(_INCOME)
_CASH_FLOW_LOOKUP = _lookup(_CASH_FLOW)


# ── fetch ──────────────────────────────────────────────────────────────────

def query_params(stock_id: str, roc_year: int, season: int) -> dict[str, str]:
    """MOPS's query for one company/period, in the order the site's own form sends it."""
    return {
        "encodeURIComponent": "1", "step": "1", "firstin": "1", "off": "1",
        "queryName": "co_id", "inpuType": "co_id", "TYPEK": "all", "isnew": "false",
        "co_id": stock_id, "year": str(roc_year), "season": str(season),
    }


def fetch(session: PoliteSession, day: dt.date, universe: Iterable[str] = ()) -> list[dict[str, Any]]:
    """Fetch every company's statements for the latest quarter due by `day`.

    One raw dict per company bundles its pages, plus the previous season's
    income and cash-flow pages for Q4 (the annual report is cumulative-only)
    and the previous cash-flow page for Q2/Q3 (always cumulative).
    """
    year, season = latest_quarter_due(day)
    roc_year = year - 1911
    raws = []
    for stock_id in dict.fromkeys(str(s).strip() for s in universe):
        if not _COMPANY_RE.fullmatch(stock_id):
            continue   # ETFs, ETNs, warrants, TDRs: no statements
        raw: dict[str, Any] = {"stock_id": stock_id, "year": year, "season": season}
        for key, url in STATEMENT_URLS.items():
            raw[key] = session.get_text(url, params=query_params(stock_id, roc_year, season))
        if season == 4:
            raw["prev_income"] = session.get_text(
                STATEMENT_URLS["income"], params=query_params(stock_id, roc_year, 3))
        if season >= 2:
            raw["prev_cash_flow"] = session.get_text(
                STATEMENT_URLS["cash_flow"], params=query_params(stock_id, roc_year, season - 1))
        raws.append(raw)
    return raws


# ── parse ──────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class _Statement:
    """One MOPS statement table: period labels and, per normalized account, its values."""
    periods: tuple[str, ...]
    rows: dict[str, tuple[float | None, ...]]

    def column(self, label: str) -> dict[str, float | None]:
        index = self.periods.index(label)
        return {account: values[index] for account, values in self.rows.items()}


def _empty_batch() -> pd.DataFrame:
    return pd.DataFrame(columns=COLUMNS)


def _parse_number(value: Any, where: str, account: str) -> float | None:
    """MOPS prints space-padded, comma-grouped 千元 with a leading minus (or
    parentheses) for negatives; blank cells are section headings or unreported items."""
    if value is None:
        return None
    if isinstance(value, (float, np.floating)):
        return None if math.isnan(value) else float(value)
    if isinstance(value, (int, np.integer)) and not isinstance(value, bool):
        return float(value)
    text = str(value).replace(",", "").replace("\xa0", "").replace("　", "").strip()
    if text in ("", "-", "--", "nan", "NaN", "None"):
        return None
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1].strip()
    text = text.replace("－", "-").replace("−", "-")
    try:
        number = float(text)
    except ValueError as exc:
        raise ParseError(f"{where}: unparseable number {value!r} in {account!r}") from exc
    return -number if negative else number


# MOPS identifies the page through its XBRL link (CO_ID / SYEAR (西元) / SSEASON),
# its e-book link (co_id / year (民國)) and the `民國115年第1季` table caption.
_XBRL_LINK_RE = re.compile(r"CO_ID=(?P<co_id>\d+)&SYEAR=(?P<year>\d{4})&SSEASON=(?P<season>\d)")
_EBOOK_LINK_RE = re.compile(r"co_id=(?P<co_id>\d+)&year=(?P<roc_year>\d{2,3})")
_CAPTION_RE = re.compile(r"民國(?P<roc_year>\d{2,3})年第(?P<season>\d)季")


def _check_identity(html: str, where: str, stock_id: str, roc_year: int, season: int) -> None:
    """The page must name the company and period asked for, wherever it says so."""
    announced: dict[str, str] = {}
    if (m := _XBRL_LINK_RE.search(html)):
        announced.update({"co_id": m["co_id"], "year": str(int(m["year"]) - 1911), "season": m["season"]})
    if (m := _EBOOK_LINK_RE.search(html)):
        announced.setdefault("co_id", m["co_id"])
        announced.setdefault("year", str(int(m["roc_year"])))
    if (m := _CAPTION_RE.search(html)):
        announced.setdefault("year", str(int(m["roc_year"])))
        announced.setdefault("season", m["season"])
    expected = {"co_id": stock_id, "year": str(roc_year), "season": str(season)}
    for key, value in expected.items():
        if key in announced and announced[key] != value:
            raise ParseError(
                f"{where}: page announces {key}={announced[key]!r}, expected {value!r} — "
                f"MOPS served another company or period"
            )


_UNIT_RE = re.compile(r"單位\s*[：:]\s*新台幣\s*(?P<unit>[^\s<]*?)元")


def _check_unit(html: str, where: str) -> None:
    match = _UNIT_RE.search(html)
    if match and match.group("unit") not in ("仟", "千"):
        raise ParseError(
            f"{where}: page declares 單位：新台幣{match.group('unit')}元, expected 仟元"
        )


def _cell_text(cell: Any) -> str:
    return cell.text_content().replace("\xa0", " ").strip()


def _table_rows(table: Any) -> list[Any]:
    return table.xpath("./tr | ./thead/tr | ./tbody/tr")


def _find_statement_table(doc: Any) -> tuple[Any, int] | None:
    """The table (and the index of its 會計項目 header row) holding the statement."""
    for table in doc.iter("table"):
        for index, row in enumerate(_table_rows(table)):
            cells = row.xpath("./th")
            if cells and normalize_account(_cell_text(cells[0])) == ACCOUNT_COLUMN:
                return table, index
    return None


def _statement_from_table(table: Any, header_index: int, where: str) -> _Statement:
    """Read the period header (colspans expanded), the 金額/% sub-header, then the
    account rows; a label that appears twice (a section heading followed by its
    indented value row) is merged position-wise, first non-empty value wins."""
    rows = _table_rows(table)
    header = rows[header_index].xpath("./th")
    expanded: list[str] = []
    for cell in header:
        expanded += [_cell_text(cell)] * int(cell.get("colspan") or 1)
    positions: list[int] = []
    periods: list[str] = []
    sub_header = rows[header_index + 1].xpath("./th") if header_index + 1 < len(rows) else []
    body_start = header_index + 1
    if sub_header and not rows[header_index + 1].xpath("./td"):
        body_start += 1
        for position, cell in enumerate(sub_header):
            if _cell_text(cell) == AMOUNT_COLUMN and position < len(expanded):
                positions.append(position)
    else:
        positions = list(range(1, len(expanded)))
    for position in positions:
        label = expanded[position]
        if label and label not in periods:
            periods.append(label)
    positions = positions[: len(periods)]
    if not periods:
        raise ParseError(f"{where}: statement table has no period columns — source format changed?")
    parsed: dict[str, list[float | None]] = {}
    for row in rows[body_start:]:
        cells = row.xpath("./td")
        if not cells:
            continue
        account = normalize_account(_cell_text(cells[0]))
        if not account:
            continue
        values = [
            _parse_number(_cell_text(cells[p]), where, account) if p < len(cells) else None
            for p in positions
        ]
        if account in parsed:
            parsed[account] = [
                old if old is not None else new for old, new in zip(parsed[account], values)
            ]
        else:
            parsed[account] = values
    return _Statement(tuple(periods), {k: tuple(v) for k, v in parsed.items()})


def _read_statement(html: Any, statement: str, stock_id: str, roc_year: int,
                    season: int) -> _Statement | None:
    """Parse one page; None when MOPS explicitly reports no data for it."""
    where = f"{stock_id} {roc_year}/{season} {statement}"
    if not isinstance(html, str) or not html.strip():
        raise ParseError(f"{where}: empty page")
    _check_identity(html, where, stock_id, roc_year, season)
    try:
        doc = lxml_html.fromstring(html)
    except (ValueError, lxml_etree.ParserError) as exc:
        raise ParseError(f"{where}: not an HTML page: {exc}") from exc
    found = _find_statement_table(doc)
    if found is None:
        if any(marker in html for marker in NO_DATA_MARKERS):
            return None
        raise ParseError(
            f"{where}: no statement table with a {ACCOUNT_COLUMN!r} column — source format changed?"
        )
    _check_unit(html, where)
    return _statement_from_table(*found, where)


def _map_fields(lookup: dict[str, tuple[str, ...] | Sum],
                column: dict[str, float | None]) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    for field, source in lookup.items():
        if isinstance(source, Sum):
            present = [column[c] for c in source.components if column.get(c) is not None]
            out[field] = float(sum(present)) if present else None
        else:
            out[field] = next((column[a] for a in source if column.get(a) is not None), None)
    return out


def _minus(current: float | None, previous: float | None) -> float | None:
    """single = YTD − previous YTD; a line absent from one page counts as 0 there."""
    if current is None and previous is None:
        return None
    return round((current or 0.0) - (previous or 0.0), 2)


def _period_column(statement: _Statement, labels: Iterable[str], where: str) -> dict[str, float | None]:
    for label in labels:
        if label in statement.periods:
            return statement.column(label)
    raise ParseError(
        f"{where}: page has none of the {list(labels)!r} columns (columns: {list(statement.periods)}) — "
        f"MOPS served another period or changed layout"
    )


def _previous(raw: dict[str, Any], key: str, statement: str, stock_id: str,
              roc_year: int, season: int) -> _Statement | None:
    if raw.get(key) is None:
        raise ParseError(
            f"{stock_id} {roc_year}/{season} {statement}: cumulative-only page needs "
            f"raw[{key!r}] (season {season - 1}) to de-cumulate; fetch() bundles it"
        )
    return _read_statement(raw[key], statement, stock_id, roc_year, season - 1)


def _quarter_labels(roc_year: int, season: int) -> tuple[str, ...]:
    """Three-month column: `115年第1季`."""
    return (f"{roc_year}年第{season}季",)


def _cumulative_labels(roc_year: int, season: int) -> tuple[str, ...]:
    """Year-to-date column: `115年01月01日至115年06月30日`; the annual report prints `114年度`."""
    end = quarter_end(roc_year + 1911, season)
    ytd = f"{roc_year}年01月01日至{roc_year}年{end.month:02d}月{end.day:02d}日"
    return (f"{roc_year}年度", ytd) if season == 4 else (ytd,)


def _balance_sheet_fields(statement: _Statement, roc_year: int, season: int,
                          where: str) -> dict[str, float | None]:
    end = quarter_end(roc_year + 1911, season)
    label = f"{roc_year}年{end.month:02d}月{end.day:02d}日"
    return _map_fields(_BALANCE_SHEET_LOOKUP, _period_column(statement, (label,), where))


def _income_fields(statement: _Statement, raw: dict[str, Any], stock_id: str,
                   roc_year: int, season: int, where: str) -> dict[str, float | None]:
    quarter = _quarter_labels(roc_year, season)
    if any(label in statement.periods for label in quarter):
        return _map_fields(_INCOME_LOOKUP, _period_column(statement, quarter, where))
    current = _map_fields(_INCOME_LOOKUP, _period_column(statement, _cumulative_labels(roc_year, season), where))
    if season == 1:
        return current
    previous = _previous(raw, "prev_income", "income", stock_id, roc_year, season)
    if previous is None:
        return current   # nothing filed earlier in the year: YTD is the whole history
    prior = _map_fields(_INCOME_LOOKUP, _period_column(
        previous, _cumulative_labels(roc_year, season - 1), where + " (previous)"))
    return {field: _minus(current[field], prior[field]) for field in current}


def _cash_flow_fields(statement: _Statement, raw: dict[str, Any], stock_id: str,
                      roc_year: int, season: int, where: str) -> dict[str, float | None]:
    current = _map_fields(_CASH_FLOW_LOOKUP, _period_column(statement, _cumulative_labels(roc_year, season), where))
    if season == 1:
        return current
    previous = _previous(raw, "prev_cash_flow", "cash_flow", stock_id, roc_year, season)
    if previous is None:
        return current
    prior = _map_fields(_CASH_FLOW_LOOKUP, _period_column(
        previous, _cumulative_labels(roc_year, season - 1), where + " (previous)"))
    single: dict[str, float | None] = {}
    for field, value in current.items():
        if field in _CLOSING_BALANCE_FIELDS:
            single[field] = value
        elif field == _OPENING_BALANCE_FIELD:
            prior_close = prior["期末現金及約當現金餘額"]
            single[field] = prior_close if prior_close is not None else value
        else:
            single[field] = _minus(value, prior[field])
    return single


def parse(raw: dict[str, Any]) -> pd.DataFrame:
    """parse(raw) → one long-form row (stock_id, date, market + FIELDS) for one company.

    `date` is the quarter-end period; flows are single-quarter (see module
    docstring). An empty DataFrame means MOPS reports no statements for the
    company/period; layout drift, another company/period, or an unparseable
    number raises ParseError.
    """
    stock_id = str(raw.get("stock_id", "")).strip()
    try:
        year, season = int(raw["year"]), int(raw["season"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"{stock_id!r}: raw payload lacks year/season: {exc}") from exc
    if season not in (1, 2, 3, 4):
        raise ParseError(f"{stock_id}: season must be 1–4, got {season!r}")
    roc_year = year - 1911

    statements = {
        key: _read_statement(raw.get(key), key, stock_id, roc_year, season)
        for key in STATEMENT_URLS
    }
    missing = [key for key, statement in statements.items() if statement is None]
    if len(missing) == len(statements):
        return _empty_batch()
    if missing:
        raise ParseError(
            f"{stock_id} {roc_year}/{season}: MOPS reports no data for {missing} "
            f"but serves the other statements"
        )

    where = f"{stock_id} {roc_year}/{season}"
    values: dict[str, float | None] = {}
    values.update(_balance_sheet_fields(statements["balance_sheet"], roc_year, season, where + " balance_sheet"))
    values.update(_income_fields(statements["income"], raw, stock_id, roc_year, season, where + " income"))
    values.update(_cash_flow_fields(statements["cash_flow"], raw, stock_id, roc_year, season, where + " cash_flow"))

    record = {"stock_id": stock_id, "date": pd.Timestamp(quarter_end(year, season)), "market": MARKET}
    record.update({field: values[field] for field in FIELDS})
    df = pd.DataFrame([record], columns=COLUMNS)
    df[FIELDS] = df[FIELDS].astype("float64")
    return df


# ── Invariants: accounting identities that catch silent corruption ────────

def _numeric(batch: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(batch[column], errors="coerce")


def _report(batch: pd.DataFrame, bad: pd.Series, columns: list[str], message: str) -> str | None:
    if not bad.any():
        return None
    sample = batch.loc[bad, ["stock_id", "date", *columns]].head(3).to_dict("records")
    return f"{int(bad.sum())} rows where {message}, e.g. {sample}"


def balance_sheet_balances() -> qa.Invariant:
    """資產總額 ≈ 負債總額 + 股東權益總額 (within 0.5%), where all three are present."""
    columns = ["資產總額", "負債總額", "股東權益總額"]

    def check(batch: pd.DataFrame) -> str | None:
        if any(c not in batch.columns for c in columns):
            return None   # required_columns reports absence
        assets, liabilities, equity = (_numeric(batch, c) for c in columns)
        gap = (assets - (liabilities + equity)).abs()
        present = assets.notna() & liabilities.notna() & equity.notna()
        bad = present & (gap > BALANCE_TOLERANCE * assets.abs())
        return _report(batch, bad, columns, "資產總額 ≠ 負債總額 + 股東權益總額 (beyond 0.5%)")

    return qa.Invariant("balance_sheet_balances", check)


def current_assets_within_total_assets() -> qa.Invariant:
    """流動資產 ≤ 資產總額, where both are present."""
    columns = ["流動資產", "資產總額"]

    def check(batch: pd.DataFrame) -> str | None:
        if any(c not in batch.columns for c in columns):
            return None
        current, total = (_numeric(batch, c) for c in columns)
        bad = current.notna() & total.notna() & (current > total)
        return _report(batch, bad, columns, "流動資產 > 資產總額")

    return qa.Invariant("current_assets_within_total_assets", check)


def gross_profit_identity() -> qa.Invariant:
    """營業毛利 ≈ 營業收入淨額 − 營業成本 (within 0.5% of revenue), where all three are present."""
    columns = ["營業毛利", "營業收入淨額", "營業成本"]

    def check(batch: pd.DataFrame) -> str | None:
        if any(c not in batch.columns for c in columns):
            return None
        gross, revenue, cost = (_numeric(batch, c) for c in columns)
        gap = (gross - (revenue - cost)).abs()
        present = gross.notna() & revenue.notna() & cost.notna()
        bad = present & (gap > GROSS_PROFIT_TOLERANCE * revenue.abs() + 1)
        return _report(batch, bad, columns, "營業毛利 ≠ 營業收入淨額 − 營業成本 (beyond 0.5% of revenue)")

    return qa.Invariant("gross_profit_identity", check)


SPECS = [
    DatasetSpec(
        name="financial_statement",
        official_source="MOPS per-company IFRS statements (ajax_t164sb03/04/05)",
        # Filings are due by each quarterly Statutory Deadline; collect that evening.
        cadence=Cadence(kind="quarterly", at="22:00"),
        frequency="quarterly",
        fields=tuple(FIELDS),
        int_fields=frozenset(),          # the Catalog types every Field as float
        key_fields=("stock_id", "date"),
        invariants=(
            qa.required_columns(COLUMNS),
            qa.unique_key(["stock_id", "date"]),
            # The batch size follows the universe, so the floor is relative:
            # half the companies asked about must answer. Real quarters land
            # near 100%, and a backfill into 2013 — today's universe against a
            # market a third smaller — still clears it, while a MOPS outage
            # answering 「查無所需資料」 for all but a handful does not.
            qa.min_coverage(0.5),
            balance_sheet_balances(),
            current_assets_within_total_assets(),
            gross_profit_identity(),
        ),
        backfill_start=dt.date(2013, 3, 31),   # IFRS adoption: Q1 2013, due 2013-05-15 (Catalog start)
        fetch=fetch,
        parse=parse,
        align=align_quarterly,
        universe_from="price:收盤價",
    ),
]
