"""financial_statement (twlab 09) across the three seams.

Seam 3: parse(raw) → one row per company against MOPS-format statement pages,
including single-quarter de-cumulation of cumulative income (Q4) and cash-flow
(Q2–Q4) columns.
Seam 2: the pipeline with HTTP faked — the Stock ID universe comes from a
seeded price frame; accounting-identity Invariants Quarantine corrupt batches.
Seam 1: Statutory-Deadline alignment — Q1 2026 is first visible at 2026-05-15,
the 2025 annual report at 2026-03-31, while the raw store keeps quarter ends;
every one of the Catalog's 158 Data Keys resolves through data.get().
"""
import datetime as dt
from urllib.parse import urlencode

import mongomock
import pandas as pd
import pytest

from twlab import catalog, data, pipeline, registry
from twlab.dataframe import FinlabDataFrame
from twlab.datasets import financial_statement as fs
from twlab.errors import ParseError
from twlab.spec import align_quarterly
from twlab.store.mongo import MongoStore
from twlab.store.parquet import ParquetStore

from conftest import FakeSession, load_text_fixture

DS = "financial_statement"
Q1_DEADLINE = dt.date(2026, 5, 15)       # Q1 2026 is due by May 15
ANNUAL_DEADLINE = dt.date(2026, 3, 31)   # the 2025 annual report (Q4) by March 31
NOW = dt.datetime(2026, 5, 15, 22, 0)
ANNUAL_NOW = dt.datetime(2026, 3, 31, 22, 0)
COMPANIES = ["2330", "2317", "1101"]

# Golden values (千元; 每股盈餘 in 元) from the FinMind Witness, embedded in the
# synthesized MOPS-format fixtures — see tests/fixtures/financial_statement/README.md.
TSMC_Q1_2026 = {
    "營業收入淨額": 1_134_103_440, "營業成本": 382_808_019, "營業毛利": 751_295_421,
    "營業費用": 94_005_657, "營業利益": 658_966_142, "稅前淨利": 687_799_687,
    "所得稅費用": 114_998_383, "合併總損益": 572_801_304, "歸屬母公司淨利_損": 572_479_752,
    "歸屬非控制權益淨利_損": 321_552, "本期綜合損益總額": 626_796_955, "每股盈餘": 22.08,
    "現金及約當現金": 3_035_637_228, "存貨": 311_453_459, "流動資產": 4_265_512_176,
    "不動產廠房及設備": 3_954_679_396, "資產總額": 8_660_949_685,
    "流動負債": 1_714_253_448, "負債總額": 2_728_560_764, "普通股股本": 259_325_245, "股本": 259_323_701,
    "母公司股東權益合計": 5_890_960_252, "股東權益總額": 5_932_388_921,
    "負債及股東權益總額": 8_660_949_685,
    "本期稅前淨利_淨損": 687_799_687, "折舊費用": 163_312_605, "攤銷費用": 2_137_832,
    "營業活動之淨現金流入_流出": 698_976_265, "取得不動產_廠房及設備": -350_762_799,
    "投資活動之淨現金流入_流出": -356_853_756, "籌資活動之淨現金流入_流出": -119_910_612,
    "期初現金及約當現金餘額": 2_767_856_402, "期末現金及約當現金餘額": 3_035_637_228,
}
TSMC_Q2_2026 = {   # single-quarter: the 3-month income column; cash flow YTD(Q2) − YTD(Q1)
    "營業收入淨額": 1_270_380_250, "歸屬母公司淨利_損": 706_561_938, "每股盈餘": 27.25,
    "營業活動之淨現金流入_流出": 1_482_341_242 - 698_976_265,
    "取得不動產_廠房及設備": -846_764_746 - (-350_762_799),
    "折舊費用": 359_541_458 - 163_312_605,
    "本期現金及約當現金增加_減少_數": 366_361_811 - 267_780_826,
    "期初現金及約當現金餘額": 3_035_637_228, "期末現金及約當現金餘額": 3_134_218_213,
    "資產總額": 9_375_654_727,
}
TSMC_Q4_2025 = {   # single-quarter: annual − 前三季 for income; YTD(Q4) − YTD(Q3) for cash flow
    "營業收入淨額": 1_046_090_421, "歸屬母公司淨利_損": 505_743_990, "每股盈餘": 19.51,
    "營業活動之淨現金流入_流出": 2_274_975_625 - 1_549_466_838,
    "資產總額": 7_933_023_878,
}
HONHAI_Q1_2026 = {"營業收入淨額": 2_119_533_391, "每股盈餘": 3.56, "資產總額": 5_230_479_067,
                  "歸屬母公司淨利_損": 49_919_449}


# ── fixture plumbing ───────────────────────────────────────────────────────

def page(stmt: str, stock_id: str, roc_year: int, season: int, suffix: str = "") -> str:
    return load_text_fixture(f"mops_t164sb{stmt}_{stock_id}_{roc_year}_{season}{suffix}.html", DS)


def request_for(statement: str, stock_id: str, roc_year: int, season: int) -> str:
    """The exact request fetch() issues, as FakeSession sees it."""
    params = fs.query_params(stock_id, roc_year, season)
    return f"{fs.STATEMENT_URLS[statement]}?{urlencode(params)}"


STMT = {"balance_sheet": "03", "income": "04", "cash_flow": "05"}


def pages_for(stock_id: str, roc_year: int, season: int, statements=STMT, **overrides) -> dict[str, str]:
    """{request: page} for one company/season; overrides swap a statement's page."""
    return {
        request_for(name, stock_id, roc_year, season): overrides.get(name, page(code, stock_id, roc_year, season))
        for name, code in statements.items()
    }


def q1_session(**overrides) -> FakeSession:
    payloads = {}
    for sid in COMPANIES:
        payloads.update(pages_for(sid, 115, 1, **(overrides if sid == "2330" else {})))
    return FakeSession(payloads)


def annual_session() -> FakeSession:
    payloads = {}
    for sid in COMPANIES:
        payloads.update(pages_for(sid, 114, 4))
        payloads.update(pages_for(sid, 114, 3, statements={"income": "04", "cash_flow": "05"}))
    return FakeSession(payloads)


def raw_q1(stock_id: str = "2330", year: int = 2026, season: int = 1, **overrides) -> dict:
    raw = {"stock_id": stock_id, "year": year, "season": season,
           "balance_sheet": page("03", stock_id, 115, 1),
           "income": page("04", stock_id, 115, 1),
           "cash_flow": page("05", stock_id, 115, 1)}
    raw.update(overrides)
    return raw


def raw_q2_tsmc(**overrides) -> dict:
    raw = {"stock_id": "2330", "year": 2026, "season": 2,
           "balance_sheet": page("03", "2330", 115, 2),
           "income": page("04", "2330", 115, 2),
           "cash_flow": page("05", "2330", 115, 2),
           "prev_cash_flow": page("05", "2330", 115, 1)}
    raw.update(overrides)
    return raw


def raw_q4_2025(stock_id: str = "2330") -> dict:
    return {"stock_id": stock_id, "year": 2025, "season": 4,
            "balance_sheet": page("03", stock_id, 114, 4),
            "income": page("04", stock_id, 114, 4),
            "cash_flow": page("05", stock_id, 114, 4),
            "prev_income": page("04", stock_id, 114, 3),
            "prev_cash_flow": page("05", stock_id, 114, 3)}


def seed_universe(store: ParquetStore, ids=("2330", "0050", "2317", "00631L", "1101")) -> None:
    """A tiny price frame whose columns define the Stock ID universe (ETF/ETN codes included)."""
    frame = pd.DataFrame([[100.0] * len(ids)], columns=list(ids),
                         index=pd.DatetimeIndex([pd.Timestamp("2026-05-14")], name="date"))
    store.write_frames("price", {"收盤價": frame}, now=NOW, frequency="daily")


def one_row(rows: pd.DataFrame, stock_id: str) -> pd.Series:
    assert list(rows["stock_id"]) == [stock_id]
    return rows.iloc[0]


# ── Seam 3: parser ─────────────────────────────────────────────────────────

def test_q1_parse_shape_and_golden_values():
    rows = fs.parse(raw_q1("2330"))

    assert list(rows.columns) == ["stock_id", "date", "market", *fs.FIELDS]
    assert len(fs.FIELDS) == 158
    tsmc = one_row(rows, "2330")
    assert tsmc["date"] == pd.Timestamp("2026-03-31")     # the quarter end, not the deadline
    assert isinstance(tsmc["market"], str)
    for field, value in TSMC_Q1_2026.items():
        assert tsmc[field] == pytest.approx(value), field


def test_q1_parse_second_company():
    hon_hai = one_row(fs.parse(raw_q1("2317")), "2317")
    for field, value in HONHAI_Q1_2026.items():
        assert hon_hai[field] == pytest.approx(value), field


def test_missing_optional_line_items_are_none_not_errors():
    tsmc = one_row(fs.parse(raw_q1("2330")), "2330")
    # TSMC reports no short-term borrowings, preferred shares or discontinued operations.
    for field in ("短期借款", "特別股股本", "停業單位損益", "遞延資產合計", "應付商業本票∕承兌匯票"):
        assert pd.isna(tsmc[field]), field


def test_derived_sums_and_aliases_from_the_real_1101_page():
    cement = one_row(fs.parse(raw_q1("1101")), "1101")
    assert cement["應收帳款及票據"] == 4_085_885 + 23_524_066 + 428_294   # 應收票據淨額 + 應收帳款淨額 + 關係人
    assert cement["應付帳款及票據"] == 13_359_245 + 666_929               # 應付帳款 + 應付帳款－關係人 (no 應付票據 line)
    assert cement["商譽及無形資產合計"] == 63_933_806                       # 無形資產 alone: no separate 商譽 line
    assert cement["其他應收款"] == 4_176_862                                # MOPS spells it 其他應收款淨額
    assert cement["應付商業本票∕承兌匯票"] == 1_208_818                      # MOPS: 應付短期票券
    assert cement["庫藏股票帳面值"] == -979_439
    assert cement["特別股股本"] == 2_000_000
    assert cement["合約負債_流動"] == 1_950_240
    assert cement["租賃負債─流動"] == 907_585                               # Catalog's ─ (U+2500) spelling
    assert cement["呆帳費用提列_轉列收入_數"] == -11_694                    # MOPS's combined 預期信用減損…／呆帳費用提列… label
    assert cement["非金融資產減損迴轉利益"] == 56_728
    assert cement["應付短期票券減少"] == -1_297_968
    assert cement["每股盈餘"] == 0.10


def test_q2_decumulates_cash_flow_and_takes_the_three_month_income_column():
    tsmc = one_row(fs.parse(raw_q2_tsmc()), "2330")
    assert tsmc["date"] == pd.Timestamp("2026-06-30")
    for field, value in TSMC_Q2_2026.items():
        assert tsmc[field] == pytest.approx(value), field
    # Internal consistency of the single-quarter cash flow.
    assert tsmc["期末現金及約當現金餘額"] - tsmc["期初現金及約當現金餘額"] == pytest.approx(
        tsmc["本期現金及約當現金增加_減少_數"])


def test_q4_decumulates_annual_income_and_cash_flow():
    tsmc = one_row(fs.parse(raw_q4_2025("2330")), "2330")
    assert tsmc["date"] == pd.Timestamp("2025-12-31")
    for field, value in TSMC_Q4_2025.items():
        assert tsmc[field] == pytest.approx(value, abs=0.011), field
    # The identity survives de-cumulation.
    assert tsmc["營業毛利"] == pytest.approx(tsmc["營業收入淨額"] - tsmc["營業成本"])


def test_cumulative_page_without_previous_season_fails_loudly():
    with pytest.raises(ParseError, match="prev_cash_flow"):
        fs.parse(raw_q2_tsmc(prev_cash_flow=None))
    with pytest.raises(ParseError, match="prev_income"):
        raw = raw_q4_2025("2330")
        raw.pop("prev_income")
        fs.parse(raw)


def test_renamed_account_header_fails_loudly():
    with pytest.raises(ParseError, match="會計項目"):
        fs.parse(raw_q1("2330", income=page("04", "2330", 115, 1, "_malformed")))


def test_page_for_another_company_fails_loudly():
    with pytest.raises(ParseError, match="2317"):
        fs.parse(raw_q1("2330", balance_sheet=page("03", "2330", 115, 1, "_wrong_company")))


def test_page_for_another_period_fails_loudly():
    with pytest.raises(ParseError, match="115"):
        fs.parse(raw_q1("2330", year=2025, season=1))      # 114/1 requested, 115/1 served
    with pytest.raises(ParseError, match="season"):
        fs.parse(raw_q1("2330", year=2026, season=2))      # 115/2 requested, 115/1 served


def test_unparseable_number_fails_loudly():
    corrupt = page("04", "2330", 115, 1).replace("1,134,103,440", "1,134,1O3,440")
    with pytest.raises(ParseError, match="1O3"):
        fs.parse(raw_q1("2330", income=corrupt))


def test_no_data_page_yields_no_rows():
    nothing = "<html><body><div id='table01'><h4>查無所需資料！</h4></div></body></html>"
    rows = fs.parse(raw_q1("2330", balance_sheet=nothing, income=nothing, cash_flow=nothing))
    assert rows.empty
    assert list(rows.columns) == ["stock_id", "date", "market", *fs.FIELDS]


def test_unit_other_than_thousands_is_rejected():
    corrupt = page("03", "2330", 115, 1).replace("單位：新台幣仟元", "單位：新台幣元")
    with pytest.raises(ParseError, match="仟元"):
        fs.parse(raw_q1("2330", balance_sheet=corrupt))


# ── Seam 2: fetch + pipeline ───────────────────────────────────────────────

def test_fetch_asks_mops_for_three_statements_per_company_and_skips_non_companies():
    session = q1_session()
    raws = fs.fetch(session, Q1_DEADLINE, universe=["2330", "0050", "2317", "00631L", "1101", "2330"])

    assert [r["stock_id"] for r in raws] == COMPANIES               # ETF/ETN codes and duplicates dropped
    assert all((r["year"], r["season"]) == (2026, 1) for r in raws)
    assert set(raws[0]) == {"stock_id", "year", "season", "balance_sheet", "income", "cash_flow"}
    assert len(session.calls) == 9
    assert request_for("balance_sheet", "2330", 115, 1) in session.calls
    assert all("co_id=" in c and "year=115" in c and "season=1" in c for c in session.calls)


def test_fetch_for_annual_deadline_adds_previous_season_pages():
    session = annual_session()
    raws = fs.fetch(session, ANNUAL_DEADLINE, universe=["2330"])
    raw = raws[0]
    assert (raw["year"], raw["season"]) == (2025, 4)
    assert "prev_income" in raw and "prev_cash_flow" in raw
    assert request_for("income", "2330", 114, 3) in session.calls
    assert request_for("cash_flow", "2330", 114, 3) in session.calls
    assert len(session.calls) == 5


def test_fetch_between_deadlines_falls_back_to_the_latest_quarter_due():
    session = q1_session()
    raws = fs.fetch(session, dt.date(2026, 6, 1), universe=["2330"])
    assert (raws[0]["year"], raws[0]["season"]) == (2026, 1)


def test_full_run_materializes_deadline_indexed_frames(mongo, store_env):
    store = ParquetStore(store_env)
    seed_universe(store)

    result = pipeline.run(DS, Q1_DEADLINE, session=q1_session(), mongo=mongo, store=store, now=NOW)

    assert result.status == "ok"
    assert result.rows == 3
    revenue = data.get("financial_statement:營業收入淨額")
    assert list(revenue.index) == [pd.Timestamp("2026-05-15")]      # Statutory Deadline, not March 31
    assert list(revenue.columns) == sorted(COMPANIES)
    assert revenue.loc["2026-05-15", "2330"] == TSMC_Q1_2026["營業收入淨額"]
    assert revenue.loc["2026-05-15", "2317"] == HONHAI_Q1_2026["營業收入淨額"]
    assert revenue._freq == "quarterly"
    assert data.get("financial_statement:每股盈餘").loc["2026-05-15", "2317"] == 3.56


def test_rerun_is_idempotent(mongo, store):
    seed_universe(store)
    first = pipeline.run(DS, Q1_DEADLINE, session=q1_session(), mongo=mongo, store=store, now=NOW)
    second = pipeline.run(DS, Q1_DEADLINE, session=q1_session(), mongo=mongo, store=store, now=NOW)
    assert first.status == second.status == "ok"
    assert mongo.collection(DS).count_documents({}) == first.rows == 3


def test_unbalanced_balance_sheet_is_quarantined_and_last_good_frames_survive(mongo, store_env):
    store = ParquetStore(store_env)
    seed_universe(store)
    good = pipeline.run(DS, ANNUAL_DEADLINE, session=annual_session(), mongo=mongo, store=store, now=ANNUAL_NOW)
    assert good.status == "ok"

    # Q1: TSMC's 資產總額 comes back 600 billion short of 負債 + 權益 — silent corruption.
    corrupt = q1_session(balance_sheet=page("03", "2330", 115, 1, "_unbalanced"))
    bad = pipeline.run(DS, Q1_DEADLINE, session=corrupt, mongo=mongo, store=store, now=NOW)

    assert bad.status == "quarantined"
    assert any("資產總額" in f for f in bad.failures)
    revenue = data.get("financial_statement:營業收入淨額")
    assert list(revenue.index) == [pd.Timestamp("2026-03-31")]      # Q1 never published
    assert revenue.loc["2026-03-31", "2330"] == TSMC_Q4_2025["營業收入淨額"]
    assert data.get("financial_statement:資產總額").loc["2026-03-31", "2330"] == TSMC_Q4_2025["資產總額"]


def test_parse_failure_quarantines_run(mongo, store):
    seed_universe(store)
    malformed = q1_session(income=page("04", "2330", 115, 1, "_malformed"))
    result = pipeline.run(DS, Q1_DEADLINE, session=malformed, mongo=mongo, store=store, now=NOW)

    assert result.status == "quarantined"
    assert any("parse" in f for f in result.failures)
    assert mongo.collection(DS).count_documents({}) == 0
    assert not store.manifest_path(DS).exists()


def test_invariants_catch_each_accounting_identity():
    rows = fs.parse(raw_q1("2330"))
    spec = registry.get_spec(DS)
    names = [inv.name for inv in spec.invariants]
    assert {"required_columns", "unique_key", "balance_sheet_balances",
            "current_assets_within_total_assets", "gross_profit_identity"} <= set(names)

    def failures(batch):
        return [inv.name for inv in spec.invariants if inv.check(batch) is not None]

    assert failures(rows) == []
    assert failures(rows.assign(流動資產=rows["資產總額"] + 1)) == ["current_assets_within_total_assets"]
    assert failures(rows.assign(營業成本=rows["營業成本"] * 1.5)) == ["gross_profit_identity"]
    assert failures(rows.assign(負債總額=rows["負債總額"] * 1.1)) == ["balance_sheet_balances"]
    # Identities are skipped, not failed, where a line item is absent.
    assert failures(rows.assign(流動資產=float("nan"), 營業毛利=float("nan"))) == []


# ── Seam 1: data API + Point-in-Time Alignment ─────────────────────────────

CATALOG_KEYS = [f.key for f in catalog.dataset_fields(DS)]


@pytest.fixture(scope="module")
def q1_store(tmp_path_factory):
    """One pipeline run shared by the 158 key-resolution tests."""
    root = tmp_path_factory.mktemp("store")
    store = ParquetStore(root)
    seed_universe(store)
    mongo = MongoStore(client=mongomock.MongoClient(), db_name="twlab_fs_module")
    result = pipeline.run(DS, Q1_DEADLINE, session=q1_session(), mongo=mongo, store=store, now=NOW)
    assert result.status == "ok"
    return root


@pytest.fixture
def api_store(q1_store, monkeypatch):
    monkeypatch.setenv("TWLAB_STORE_DIR", str(q1_store))
    monkeypatch.delenv("TWLAB_REMOTE_STORE", raising=False)
    monkeypatch.delenv("TWLAB_SERVER_URL", raising=False)


def test_catalog_lists_158_fields_and_the_registry_mirrors_them():
    assert len(CATALOG_KEYS) == 158
    spec = registry.get_spec(DS)
    assert [f"{DS}:{f}" for f in spec.fields] == CATALOG_KEYS
    assert spec.frequency == "quarterly"
    assert spec.cadence.kind == "quarterly"
    assert spec.align is align_quarterly
    assert spec.universe_from == "price:收盤價"
    assert spec.key_fields == ("stock_id", "date")


@pytest.mark.parametrize("key", CATALOG_KEYS)
def test_every_catalog_key_resolves(api_store, key):
    frame = data.get(key)
    assert isinstance(frame, FinlabDataFrame)
    assert frame._freq == "quarterly"
    assert list(frame.index) == [pd.Timestamp("2026-05-15")]
    assert list(frame.columns) == sorted(COMPANIES)


def test_golden_values_through_the_data_api(api_store):
    for field, value in TSMC_Q1_2026.items():
        assert data.get(f"{DS}:{field}").loc["2026-05-15", "2330"] == pytest.approx(value), field
    assert data.get(f"{DS}:每股盈餘").loc["2026-05-15", "2317"] == 3.56
    assert pd.isna(data.get(f"{DS}:短期借款").loc["2026-05-15", "2330"])


def test_q1_first_visible_on_may_15_annual_on_march_31_and_period_kept_in_raw_store(mongo, store_env):
    store = ParquetStore(store_env)
    seed_universe(store)
    pipeline.run(DS, ANNUAL_DEADLINE, session=annual_session(), mongo=mongo, store=store, now=ANNUAL_NOW)
    pipeline.run(DS, Q1_DEADLINE, session=q1_session(), mongo=mongo, store=store, now=NOW)

    eps = data.get("financial_statement:每股盈餘")
    assert list(eps.index) == [pd.Timestamp("2026-03-31"), pd.Timestamp("2026-05-15")]
    assert eps.loc["2026-03-31", "2330"] == pytest.approx(TSMC_Q4_2025["每股盈餘"], abs=0.011)
    assert eps.loc["2026-05-15", "2330"] == TSMC_Q1_2026["每股盈餘"]

    # A daily condition sees Q4 2025 EPS from March 31 and Q1 2026 EPS only from May 15.
    daily = pd.bdate_range("2026-03-01", "2026-05-29", name="date")
    close = FinlabDataFrame(1000.0, index=daily, columns=["2330"])
    close._freq = "daily"
    pe = close / eps
    assert pe._freq == "daily"
    assert pe.index.min() == pd.Timestamp("2026-03-31")
    assert pe.loc["2026-05-14", "2330"] == pytest.approx(1000.0 / 19.51, rel=1e-3)
    assert pe.loc["2026-05-15", "2330"] == pytest.approx(1000.0 / 22.08)

    # The system of record keeps quarter ends, not deadlines.
    stored = mongo.load_long(registry.get_spec(DS))
    tsmc = stored[stored["stock_id"] == "2330"].sort_values("date")
    assert list(tsmc["date"]) == [pd.Timestamp("2025-12-31"), pd.Timestamp("2026-03-31")]
    # ...and single-quarter flows (a TTM is four consecutive rows summed).
    assert list(tsmc["營業收入淨額"]) == [TSMC_Q4_2025["營業收入淨額"], TSMC_Q1_2026["營業收入淨額"]]
