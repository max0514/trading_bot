"""Seam 3 — parser units: parse(raw) → rows against recorded fixtures.

A changed source format must fail loudly here, not corrupt data downstream.
"""
import math

import pytest

from twlab.datasets import price
from twlab.errors import ParseError

from conftest import load_fixture


def test_twse_parse_shape_and_golden_values(twse_payload):
    rows = price.parse({"source": "twse", "payload": twse_payload})

    assert list(rows.columns) == ["stock_id", "date", "market", *price.FIELDS]
    assert len(rows) == 1377  # every security in the recorded response
    assert (rows["market"] == "TWSE").all()
    assert rows["date"].nunique() == 1
    assert str(rows["date"].iloc[0])[:10] == "2026-08-07"

    tsmc = rows[rows["stock_id"] == "2330"].iloc[0]
    # Golden values from the real recorded TWSE response for 2026-08-07.
    assert tsmc["收盤價"] == 2370.0
    assert tsmc["開盤價"] == 2390.0
    assert tsmc["最高價"] == 2395.0
    assert tsmc["最低價"] == 2355.0
    assert tsmc["成交股數"] == 24414025
    assert tsmc["成交筆數"] == 64670
    assert tsmc["成交金額"] == 57947015347
    assert tsmc["最後揭示買價"] == 2370.0
    assert tsmc["最後揭示買量"] == 306
    assert tsmc["最後揭示賣價"] == 2375.0
    assert tsmc["最後揭示賣量"] == 563


def test_twse_dashes_become_missing(twse_payload):
    rows = price.parse({"source": "twse", "payload": twse_payload})
    # The recorded day contains untraded securities whose prices print as "--".
    assert rows["收盤價"].isna().any()


def test_tpex_parse_maps_source_columns_to_catalog_fields(tpex_payload):
    rows = price.parse({"source": "tpex", "payload": tpex_payload})

    assert list(rows.columns) == ["stock_id", "date", "market", *price.FIELDS]
    assert len(rows) == 1012  # every security in the recorded response
    assert (rows["market"] == "TPEx").all()

    # Golden values from the real recorded TPEx response for 2026-08-07.
    row = rows[rows["stock_id"] == "5483"].iloc[0]
    assert row["收盤價"] == 168.5            # from TPEx column "收盤 "
    assert row["成交金額"] == 3186659500     # from TPEx column " 成交金額(元)"
    assert row["最後揭示買量"] == 247        # from TPEx column "最後買量<br>(張數)"
    assert row["最後揭示賣量"] == 5
    assert row["成交股數"] == 18584000


def test_renamed_column_fails_loudly():
    payload = load_fixture("twse_mi_index_20260807_malformed.json")
    with pytest.raises(ParseError, match="收盤價"):
        price.parse({"source": "twse", "payload": payload})


def test_twse_no_data_day_yields_empty_batch(twse_payload):
    holiday = {**twse_payload, "stat": "很抱歉，沒有符合條件的資料!", "tables": []}
    rows = price.parse({"source": "twse", "payload": holiday})
    assert rows.empty


def test_unknown_source_rejected():
    with pytest.raises(ParseError):
        price.parse({"source": "nasdaq", "payload": {}})


def test_unparseable_number_rejected():
    assert price._parse_number("1,234.5") == 1234.5
    assert price._parse_number("--") is None
    with pytest.raises(ParseError):
        price._parse_number("12a4")
