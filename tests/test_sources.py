"""Seam 3 — the shared TWSE/TPEx payload plumbing every parser is built on.

The Dataset parsers each own a column map and an envelope quirk; everything
underneath — placeholders, number and date spellings, locating a table and
reading it by column name — is this module, and is pinned here once instead
of five times.
"""
import pandas as pd
import pytest

from twlab import sources
from twlab.errors import ParseError


# ── placeholders: one set, not four ────────────────────────────────────────

@pytest.mark.parametrize("text", ["", "-", "--", "---", "----", "-----", "—",
                                  "N/A", "n/a", "N/a", "  --  "])
def test_every_source_placeholder_means_missing(text):
    """The five parsers each had their own idea of what a dash meant; a value
    one of them read as missing must not be a ParseError in another."""
    assert sources.parse_number(text) is None
    assert sources.parse_int(text) is None
    assert sources.parse_text(text) is None


def test_numbers_are_comma_grouped():
    assert sources.parse_number("1,234.5") == 1234.5
    assert sources.parse_number("2,370.00") == 2370.0
    assert sources.parse_number(None) is None
    with pytest.raises(ParseError):
        sources.parse_number("31.8x")


def test_share_counts_are_signed_ints_and_never_rounded():
    assert sources.parse_int("26,004,465") == 26_004_465
    assert sources.parse_int("-58,090") == -58_090
    assert sources.parse_int("0") == 0
    with pytest.raises(ParseError):
        sources.parse_int("1,234.5")     # a fractional count is drift, not data


def test_text_keeps_padding_out_and_the_static_link_off():
    assert sources.parse_text("  鑫聯大投控           ") == "鑫聯大投控"
    assert sources.parse_text(
        "115年第2季(https://mops.twse.com.tw/mops/web/t163sb01)") == "115年第2季"


# ── dates: two contracts, because the sources print two ────────────────────

@pytest.mark.parametrize("text,expected", [
    ("20260807", "2026-08-07"),
    ("2026/08/07", "2026-08-07"),
    ("115/08/07", "2026-08-07"),
    ("92/01/02", "2003-01-02"),
])
def test_report_dates_accept_gregorian_and_roc(text, expected):
    assert sources.parse_date(text, "TWSE") == pd.Timestamp(expected)


@pytest.mark.parametrize("text", ["2026-08-07", "115/13/01", "", "日期"])
def test_report_dates_reject_forms_the_endpoints_never_print(text):
    with pytest.raises(ParseError):
        sources.parse_date(text, "TPEx")


@pytest.mark.parametrize("text,expected", [
    ("115年06月11日", "2026-06-11"),
    ("115/01/12", "2026-01-12"),
    ("1150112", "2026-01-12"),
    ("20260611", "2026-06-11"),
])
def test_announcement_dates_accept_all_three_bulletin_spellings(text, expected):
    assert sources.parse_announcement_date(text, "dividend_tse") == pd.Timestamp(expected)


def test_announcement_dates_reject_prose():
    with pytest.raises(ParseError):
        sources.parse_announcement_date("十五年六月", "dividend_tse")


# ── headers: three spellings, each named by its SourceTable ────────────────

def test_header_normalizers():
    assert sources.squeeze("日　期") == "日期"                    # TWSE MFI94U
    assert sources.squeeze("最後賣量<br>(張數)") == "最後賣量(張數)"     # TPEx quotes
    assert sources.collapse("最近一次申報資料  季別/日期") == "最近一次申報資料 季別/日期"
    assert sources.strip_name("  代號  ") == "代號"


# ── locating and reading a table ───────────────────────────────────────────

SPEC = sources.SourceTable(
    market="TWSE", id_column="證券代號", column_map={"收盤價": "收盤價", "成交量": "成交股數"},
    label="TWSE", what="quotes table",
)


def test_find_table_reads_both_envelope_shapes():
    """BWIBBU_d and T86 put fields/data at the top level; MI_INDEX and every
    TPEx page wrap theirs in tables[]."""
    flat = {"fields": ["證券代號", "收盤價", "成交量"], "data": [["2330", "1", "2"]]}
    assert sources.find_table(flat, SPEC)["data"] == [["2330", "1", "2"]]

    wrapped = {"tables": [{"title": "", "fields": ["其他"], "data": []}, flat]}
    assert sources.find_table(wrapped, SPEC)["data"] == [["2330", "1", "2"]]


def test_find_table_names_the_missing_id_column():
    with pytest.raises(ParseError, match="證券代號"):
        sources.find_table({"tables": [{"fields": ["代號"], "data": []}]}, SPEC)


def test_rows_are_read_by_name_not_position():
    reordered = {"fields": ["成交量", "證券代號", "收盤價"],
                 "data": [["1,000", "2330", "1,234.5"]]}
    rows = sources.rows_from_table(sources.find_table(reordered, SPEC), SPEC,
                                   pd.Timestamp("2026-08-07"))
    assert rows.loc[0, "收盤價"] == 1234.5
    assert rows.loc[0, "成交股數"] == 1000.0
    assert rows.loc[0, "stock_id"] == "2330"
    assert list(rows.columns) == ["stock_id", "date", "market", "收盤價", "成交股數"]


def test_a_renamed_column_fails_loudly():
    renamed = {"fields": ["證券代號", "收市價", "成交量"], "data": []}
    with pytest.raises(ParseError, match="收盤價"):
        sources.rows_from_table(sources.find_table(renamed, SPEC), SPEC,
                                pd.Timestamp("2026-08-07"))


def test_a_short_row_fails_loudly_rather_than_indexing_off_the_end():
    truncated = {"fields": ["證券代號", "收盤價", "成交量"], "data": [["2330", "1"]]}
    with pytest.raises(ParseError, match="cells"):
        sources.rows_from_table(sources.find_table(truncated, SPEC), SPEC,
                                pd.Timestamp("2026-08-07"))


def test_empty_batch_is_typed_so_a_holiday_half_concatenates_cleanly():
    empty = sources.empty_batch(["收盤價", "權息", "除權息日"],
                                str_fields={"權息"}, datetime_fields={"除權息日"})
    assert empty.empty
    assert list(empty.columns) == ["stock_id", "date", "market", "收盤價", "權息", "除權息日"]
    assert empty["收盤價"].dtype == "float64"
    assert empty["權息"].dtype == "object"
    assert empty["date"].dtype == "datetime64[ns]"

    rows = sources.rows_from_table(
        {"fields": ["證券代號", "收盤價", "成交量"], "data": [["2330", "1", "2"]]},
        SPEC, pd.Timestamp("2026-08-07"))
    joined = pd.concat([sources.empty_batch(SPEC.fields), rows], ignore_index=True)
    assert list(joined.dtypes) == list(rows.dtypes)


def test_no_rows_yields_the_typed_empty_batch():
    rows = sources.rows_from_table({"fields": ["證券代號", "收盤價", "成交量"], "data": []},
                                   SPEC, pd.Timestamp("2026-08-07"))
    assert rows.empty and rows["收盤價"].dtype == "float64"


# ── the polite "no data" stat ──────────────────────────────────────────────

@pytest.mark.parametrize("stat,expected", [
    ("很抱歉，沒有符合條件的資料!", True),
    ("查無資料", True),
    ("OK", False),
    ("系統忙碌中", False),
])
def test_no_data_stat_is_told_apart_from_an_error(stat, expected):
    assert sources.is_no_data(stat) is expected


def test_parse_value_is_per_source():
    """`institutional_investors_trading_summary` reads ints; the price-shaped
    Datasets read floats. Same plumbing, different converter."""
    ints = sources.SourceTable(market="TWSE", id_column="證券代號",
                               column_map={"買進股數": "投信買進股數"},
                               parse_value=sources.parse_int)
    rows = sources.rows_from_table({"fields": ["證券代號", "買進股數"],
                                    "data": [["2330", "26,004,465"]]},
                                   ints, pd.Timestamp("2026-08-07"))
    assert rows.loc[0, "投信買進股數"] == 26_004_465
    assert rows["投信買進股數"].dtype == "int64"   # counts, not floats
