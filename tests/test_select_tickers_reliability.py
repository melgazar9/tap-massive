"""Reliability tests for centralized select_tickers behavior."""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock

from tap_massive.crypto_streams import CryptoTickerPartitionStream
from tap_massive.forex_streams import ForexTickerPartitionStream
from tap_massive.futures_streams import FuturesTickerPartitionStream
from tap_massive.indices_streams import IndicesTickerPartitionStream
from tap_massive.option_streams import OptionsChainSnapshotStream
from tap_massive.stock_streams import StockTickerPartitionStream


def _build_partition_stream(stream_cls, config, tap_method, records):
    tap = MagicMock()
    getattr(tap, tap_method).return_value = records

    stream = stream_cls.__new__(stream_cls)
    stream._tap = tap
    type(stream).config = PropertyMock(return_value=config)
    return stream, tap


def test_stock_partition_honors_stock_tickers_select():
    stream, _ = _build_partition_stream(
        StockTickerPartitionStream,
        {"stock_tickers": {"select_tickers": ["AAPL"]}},
        "get_cached_stock_tickers",
        [{"ticker": "AAPL"}, {"ticker": "MSFT"}],
    )
    assert stream.partitions == [{"ticker": "AAPL"}]


def test_stock_partition_preserves_ticker_case_from_cache():
    stream, _ = _build_partition_stream(
        StockTickerPartitionStream,
        {"stock_tickers": {"select_tickers": ["BACpK"]}},
        "get_cached_stock_tickers",
        [{"ticker": "BACpK"}, {"ticker": "MSFT"}],
    )
    assert stream.partitions == [{"ticker": "BACpK"}]


def test_forex_partition_honors_forex_tickers_select():
    stream, _ = _build_partition_stream(
        ForexTickerPartitionStream,
        {"forex_tickers": {"select_tickers": ["C:EURUSD"]}},
        "get_cached_forex_tickers",
        [{"ticker": "C:EURUSD"}, {"ticker": "C:GBPUSD"}],
    )
    assert stream.partitions == [{"ticker": "C:EURUSD"}]


def test_forex_partition_normalizes_pair_formats():
    stream, _ = _build_partition_stream(
        ForexTickerPartitionStream,
        {"forex_tickers": {"select_tickers": ["EUR-USD"]}},
        "get_cached_forex_tickers",
        [{"ticker": "C:EURUSD"}, {"ticker": "C:GBPUSD"}],
    )
    assert stream.partitions == [{"ticker": "C:EURUSD"}]


def test_crypto_partition_honors_crypto_tickers_select():
    stream, _ = _build_partition_stream(
        CryptoTickerPartitionStream,
        {"crypto_tickers": {"select_tickers": ["X:BTCUSD"]}},
        "get_cached_crypto_tickers",
        [{"ticker": "X:BTCUSD"}, {"ticker": "X:ETHUSD"}],
    )
    assert stream.partitions == [{"ticker": "X:BTCUSD"}]


def test_indices_partition_honors_indices_tickers_select():
    stream, _ = _build_partition_stream(
        IndicesTickerPartitionStream,
        {"indices_tickers": {"select_tickers": ["I:SPX"]}},
        "get_cached_indices_tickers",
        [{"ticker": "I:SPX"}, {"ticker": "I:NDX"}],
    )
    assert stream.partitions == [{"ticker": "I:SPX"}]


def test_futures_partition_honors_futures_tickers_select():
    stream, _ = _build_partition_stream(
        FuturesTickerPartitionStream,
        {"futures_tickers": {"select_tickers": ["ESU6"]}},
        "get_cached_futures_tickers",
        [{"ticker": "ESU6"}, {"ticker": "NQU6"}],
    )
    assert stream.partitions == [{"ticker": "ESU6"}]


def test_futures_partition_falls_back_to_futures_contracts_select():
    stream, _ = _build_partition_stream(
        FuturesTickerPartitionStream,
        {"futures_contracts": {"select_tickers": ["ESU6"]}},
        "get_cached_futures_tickers",
        [{"ticker": "ESU6"}, {"ticker": "NQU6"}],
    )
    assert stream.partitions == [{"ticker": "ESU6"}]


def test_options_chain_snapshot_uses_option_tickers_select():
    tap = MagicMock()
    tap.get_cached_stock_tickers.return_value = [
        {"ticker": "AAPL"},
        {"ticker": "META"},
    ]

    stream = OptionsChainSnapshotStream.__new__(OptionsChainSnapshotStream)
    stream._tap = tap
    stream._prepare_context_and_params = MagicMock(return_value=({}, {}, {}))
    stream.paginate_records = MagicMock(return_value=iter([]))
    type(stream).config = PropertyMock(
        return_value={"option_tickers": {"select_tickers": ["META"]}}
    )

    list(stream.get_records(None))

    calls = stream.paginate_records.call_args_list
    assert len(calls) == 1
    context = calls[0].args[0]
    assert context["path_params"]["underlyingAsset"] == "META"
