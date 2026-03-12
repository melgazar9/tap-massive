from tap_massive.base_streams import BaseLastQuoteStream, BaseQuoteStream
from tap_massive.earnings_subset import (
    OptionsEarningsQuotesStream,
    StockEarningsQuotesStream,
)
from tap_massive.option_streams import OptionsQuoteStream


def _sequence_number_schema(stream_schema: dict) -> dict:
    return stream_schema["properties"]["sequence_number"]


def _assert_bigint_sized_sequence_number(stream_schema: dict) -> None:
    sequence_schema = _sequence_number_schema(stream_schema)
    assert sequence_schema["minimum"] == 0
    assert sequence_schema["maximum"] == 9_223_372_036_854_775_807


def test_stock_earnings_quotes_sequence_number_is_bigint_sized() -> None:
    _assert_bigint_sized_sequence_number(StockEarningsQuotesStream.schema)


def test_options_earnings_quotes_sequence_number_is_bigint_sized() -> None:
    _assert_bigint_sized_sequence_number(OptionsEarningsQuotesStream.schema)


def test_base_quote_sequence_number_is_bigint_sized() -> None:
    _assert_bigint_sized_sequence_number(BaseQuoteStream.schema)


def test_base_last_quote_sequence_number_is_bigint_sized() -> None:
    _assert_bigint_sized_sequence_number(BaseLastQuoteStream.schema)


def test_options_quote_sequence_number_is_bigint_sized() -> None:
    _assert_bigint_sized_sequence_number(OptionsQuoteStream.schema)
