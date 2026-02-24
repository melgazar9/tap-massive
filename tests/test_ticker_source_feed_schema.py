"""Schema guards for source_feed in ticker streams."""

from __future__ import annotations

from tap_massive.base_streams import AllTickersStream
from tap_massive.crypto_streams import CryptoTickerStream
from tap_massive.forex_streams import ForexTickerStream
from tap_massive.indices_streams import IndicesTickerStream


def test_reference_ticker_schemas_include_source_feed():
    assert "source_feed" in AllTickersStream.schema["properties"]
    assert "source_feed" in IndicesTickerStream.schema["properties"]
    assert "source_feed" in ForexTickerStream.schema["properties"]
    assert "source_feed" in CryptoTickerStream.schema["properties"]
