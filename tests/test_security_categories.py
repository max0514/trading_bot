"""security_categories (twlab 06) across the three seams.

Seam 3: parse(raw) → rows against ISIN-page fixtures (MS950, section-header
rows): equities and ETFs are kept, warrants and other sections skipped, and a
renamed column or a page for the wrong market fails loudly.
Seam 2: the pipeline with HTTP faked — a static table that upserts idempotently
and keeps serving the last good table when a scrape is Quarantined.
Seam 1: the bare Catalog key resolves to the table, which covers every 4-digit
Stock ID in the real TWSE price recording.
"""
import datetime as dt
import re

import pytest

from twlab import catalog, data, pipeline, registry
from twlab.dataframe import FinlabDataFrame
from twlab.datasets import security_categories
from twlab.errors import ParseError
from twlab.store.parquet import ParquetStore

from conftest import FIXTURES, FakeSession, load_fixture

DS = "security_categories"
DAY = dt.date(2026, 9, 3)                 # a static table: the batch day is just the run day
NOW = dt.datetime(2026, 9, 3, 20, 0)
SII_ROWS = 1215 + 36 + 271 + 36           # 股票 + 創新板股票 + ETF + 臺灣存託憑證 sections of the 上市 page
OTC_ROWS = 927 + 151                      # 股票 + ETF sections of the 上櫃 page
KEYS = sorted({f.key for f in catalog.dataset_fields(DS)})

WAF_BLOCK_PAGE = (
    "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"></head>"
    "<body> 因為安全性考量，您所執行的頁面無法呈現。<BR>FOR SECURITY REASONS, THIS PAGE CAN NOT BE "
    "ACCESSED.<BR>錯誤代碼：7641438220195869210<BR></body></html>"
)


def load_isin(name: str) -> str:
    """The ISIN site serves MS950; decode exactly as PoliteSession.get_text(encoding="cp950") does."""
    return (FIXTURES / DS / name).read_bytes().decode("cp950")


def raw(source: str, name: str) -> dict:
    return {"source": source, "payload": load_isin(name)}


def session_for(sii="isin_c_public_strmode2.html", otc="isin_c_public_strmode4.html") -> FakeSession:
    return FakeSession({"strMode=2": load_isin(sii), "strMode=4": load_isin(otc)})


def run_categories(session, mongo, store, day=DAY, now=NOW):
    return pipeline.run(DS, day, session=session, mongo=mongo, store=store, now=now)


# ── Seam 3: parser ─────────────────────────────────────────────────────────

def test_sii_parse_shape_and_golden_values():
    rows = security_categories.parse(raw("sii", "isin_c_public_strmode2.html"))

    assert list(rows.columns) == ["stock_id", "name", "category", "market"]
    assert len(rows) == SII_ROWS
    assert (rows["market"] == "sii").all()
    assert rows["stock_id"].is_unique

    by_id = rows.set_index("stock_id")
    assert by_id.loc["2330", "name"] == "台積電"
    assert by_id.loc["2330", "category"] == "半導體業"
    assert by_id.loc["1101", "category"] == "水泥工業"
    assert by_id.loc["0050", "name"] == "元大台灣50"
    assert by_id.loc["0050", "category"] == "ETF"          # ETFs get the section name, not 產業別
    assert by_id.loc["00631L", "category"] == "ETF"
    assert by_id.loc["2881A", "category"] == "金融保險"     # preferred shares sit in the 股票 section


def test_sii_parse_keeps_tdrs_and_skips_warrants_and_other_sections():
    rows = security_categories.parse(raw("sii", "isin_c_public_strmode2.html"))
    ids = set(rows["stock_id"])
    by_id = rows.set_index("stock_id")
    # 4-digit TDRs trade in the same universe as stocks and must be covered.
    assert by_id.loc["9103", "category"] == "存託憑證"
    assert "030001" not in ids and "03500P" not in ids      # 上市認購(售)權證
    assert "020000" not in ids and not any(i.startswith("02") for i in ids)   # ETN
    assert not rows["category"].eq("").any()


def test_otc_parse_maps_market():
    rows = security_categories.parse(raw("otc", "isin_c_public_strmode4.html"))
    assert len(rows) == OTC_ROWS
    assert (rows["market"] == "otc").all()
    by_id = rows.set_index("stock_id")
    assert by_id.loc["5483", "name"] == "中美晶"
    assert by_id.loc["5483", "category"] == "半導體業"
    assert "709966" not in by_id.index                      # 上櫃認購(售)權證
    assert (by_id[by_id.index.str.startswith("00")]["category"] == "ETF").all()


def test_page_for_the_other_market_is_rejected():
    with pytest.raises(ParseError, match="上櫃"):
        security_categories.parse(raw("sii", "isin_c_public_strmode4.html"))


def test_renamed_column_fails_loudly():
    with pytest.raises(ParseError, match="產業別"):
        security_categories.parse(raw("sii", "isin_c_public_strmode2_malformed.html"))


def test_waf_block_page_fails_loudly():
    with pytest.raises(ParseError, match="有價證券代號及名稱"):
        security_categories.parse({"source": "sii", "payload": WAF_BLOCK_PAGE})


def test_unknown_source_rejected():
    with pytest.raises(ParseError):
        security_categories.parse({"source": "nasdaq", "payload": ""})


# ── Seam 2: pipeline ───────────────────────────────────────────────────────

def test_fetch_requests_both_isin_pages_as_cp950():
    class EncodingRecorder(FakeSession):
        def __init__(self, payloads):
            super().__init__(payloads)
            self.encodings = []

        def get_text(self, url, params=None, encoding=None):
            self.encodings.append(encoding)
            return super().get_text(url, params, encoding)

    session = EncodingRecorder({"strMode=2": load_isin("isin_c_public_strmode2.html"),
                                "strMode=4": load_isin("isin_c_public_strmode4.html")})
    raws = security_categories.fetch(session, DAY)
    assert [r["source"] for r in raws] == ["sii", "otc"]
    assert all("isin.twse.com.tw/isin/C_public.jsp" in c for c in session.calls)
    assert any("strMode=2" in c for c in session.calls) and any("strMode=4" in c for c in session.calls)
    assert session.encodings == ["cp950", "cp950"]


def test_full_run_materializes_the_table(mongo, store_env):
    result = run_categories(session_for(), mongo, ParquetStore(store_env))

    assert result.status == "ok"
    assert result.rows == SII_ROWS + OTC_ROWS
    table = data.get("security_categories")
    assert list(table.columns) == ["stock_id", "name", "category", "market"]
    assert len(table) == SII_ROWS + OTC_ROWS
    assert table._freq == "static"
    tsmc = table[table["stock_id"] == "2330"].iloc[0]
    assert (tsmc["name"], tsmc["category"], tsmc["market"]) == ("台積電", "半導體業", "sii")
    sas = table[table["stock_id"] == "5483"].iloc[0]
    assert (sas["name"], sas["category"], sas["market"]) == ("中美晶", "半導體業", "otc")


def test_table_covers_the_whole_listed_universe(mongo, store_env):
    run_categories(session_for(), mongo, ParquetStore(store_env))
    price_payload = load_fixture("twse_mi_index_20260807.json")   # the real TWSE recording
    quotes = next(t for t in price_payload["tables"] if "證券代號" in (t.get("fields") or []))
    listed = {row[0].strip() for row in quotes["data"] if re.fullmatch(r"\d{4}", row[0].strip())}
    assert len(listed) > 1000

    table = data.get("security_categories")
    assert listed <= set(table["stock_id"])


def test_rerun_is_idempotent(mongo, store):
    first = run_categories(session_for(), mongo, store)
    second = run_categories(session_for(), mongo, store, day=DAY + dt.timedelta(days=1),
                            now=NOW + dt.timedelta(days=1))
    assert first.status == second.status == "ok"
    assert mongo.collection(DS).count_documents({}) == first.rows


def test_truncated_page_is_quarantined_and_last_good_table_survives(mongo, store_env):
    api_store = ParquetStore(store_env)
    assert run_categories(session_for(), mongo, api_store).status == "ok"

    bad = run_categories(session_for(sii="isin_c_public_strmode2_tiny.html"), mongo, api_store,
                         day=DAY + dt.timedelta(days=1), now=NOW + dt.timedelta(days=1))

    assert bad.status == "quarantined"
    assert any("min_rows" in f for f in bad.failures)
    table = data.get("security_categories")
    assert len(table) == SII_ROWS + OTC_ROWS
    assert table.set_index("stock_id").loc["2330", "name"] == "台積電"


def test_parse_failure_quarantines_run(mongo, store):
    result = run_categories(session_for(sii="isin_c_public_strmode2_malformed.html"), mongo, store)
    assert result.status == "quarantined"
    assert any("parse" in f for f in result.failures)
    assert mongo.collection(DS).count_documents({}) == 0
    assert not store.manifest_path(DS).exists()


def test_spec_is_a_static_table():
    spec = registry.get_spec(DS)
    assert spec.shape == "table" and spec.frequency == "static"
    assert spec.key_fields == ("stock_id",)
    assert spec.fields == ("name", "category", "market")
    assert spec.cadence.kind == "daily" and spec.cadence.at == "20:00"


# ── Seam 1: data API ───────────────────────────────────────────────────────

@pytest.mark.parametrize("key", KEYS)
def test_every_catalog_key_resolves_to_the_table(mongo, store_env, key):
    run_categories(session_for(), mongo, ParquetStore(store_env))
    table = data.get(key)

    assert isinstance(table, FinlabDataFrame)
    assert list(table.columns) == ["stock_id", "name", "category", "market"]
    assert set(table["market"]) == {"sii", "otc"}
    assert all(isinstance(c, str) for c in table["stock_id"])
