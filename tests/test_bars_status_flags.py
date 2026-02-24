"""Tests for bars stream status flag fields."""

from __future__ import annotations

from unittest.mock import PropertyMock

from tap_massive.option_streams import OptionsBars1DayStream
from tap_massive.stock_streams import StockBars1DayStream


def _make_ohlcv_row(include_otc: bool = True) -> dict:
    row = {
        "t": 1750046400000,
        "o": 10.1,
        "h": 11.2,
        "l": 9.8,
        "c": 10.7,
        "v": 1200,
        "vw": 10.6,
        "n": 42,
        "source_feed": "sip",
    }
    if include_otc:
        row["otc"] = False
    return row


def test_stock_bars_default_active_true_when_unset():
    stream = StockBars1DayStream.__new__(StockBars1DayStream)
    stream.query_params = {}

    result = stream.post_process(_make_ohlcv_row(), {"ticker": "BACpK"})

    assert result["ticker"] == "BACpK"
    assert result["active"] is True


def test_stock_bars_active_uses_query_param_when_provided():
    stream = StockBars1DayStream.__new__(StockBars1DayStream)
    stream.query_params = {"active": False}

    result = stream.post_process(_make_ohlcv_row(), {"ticker": "BACpK"})

    assert result["active"] is False


def test_stock_bars_active_uses_stock_tickers_query_param():
    stream = StockBars1DayStream.__new__(StockBars1DayStream)
    stream.query_params = {}
    type(stream).config = PropertyMock(
        return_value={"stock_tickers": {"query_params": {"active": False}}}
    )

    result = stream.post_process(_make_ohlcv_row(), {"ticker": "BACpK"})

    assert result["active"] is False


def test_stock_bars_active_both_uses_partition_context_value():
    stream = StockBars1DayStream.__new__(StockBars1DayStream)
    stream.query_params = {}
    type(stream).config = PropertyMock(
        return_value={"stock_tickers": {"query_params": {"active": "both"}}}
    )

    result = stream.post_process(
        _make_ohlcv_row(),
        {"ticker": "BACpK", "active": False},
    )

    assert result["active"] is False


def test_stock_bars_active_context_overrides_query_param_when_boolean():
    stream = StockBars1DayStream.__new__(StockBars1DayStream)
    stream.query_params = {"active": True}

    result = stream.post_process(
        _make_ohlcv_row(),
        {"ticker": "BACpK", "active": False},
    )

    assert result["active"] is False


def test_options_bars_uses_expired_field_instead_of_active():
    stream = OptionsBars1DayStream.__new__(OptionsBars1DayStream)
    stream._status_flag_query_params = lambda: {}

    result = stream.post_process(
        _make_ohlcv_row(include_otc=False),
        {"ticker": "O:BACPK260320C00025000", "expired": True},
    )

    assert result["expired"] is True
    assert "active" not in result


def test_options_bars_default_expired_false_when_unset():
    stream = OptionsBars1DayStream.__new__(OptionsBars1DayStream)
    stream._status_flag_query_params = lambda: {}

    result = stream.post_process(
        _make_ohlcv_row(include_otc=False),
        {"ticker": "O:BACPK260320C00025000"},
    )

    assert result["expired"] is False


def test_bars_schema_includes_source_feed():
    assert "source_feed" in StockBars1DayStream.schema["properties"]
    assert "source_feed" in OptionsBars1DayStream.schema["properties"]
