"""Tests for per-underlying options partitioning and contract fetching."""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock

import pytest
import requests

from tap_massive.tap import TapMassive


@pytest.fixture()
def tap_config():
    return {
        "api_key": "test_key",
        "option_tickers": {
            "query_params": {
                "expired": True,
                "sort": "ticker",
            },
            "select_tickers": ["AAPL"],
        },
    }


@pytest.fixture()
def mock_tap(tap_config):
    """Create a TapMassive instance with mocked config."""
    tap = MagicMock(spec=TapMassive)
    tap.config = tap_config
    tap.logger = MagicMock()
    tap._mock_option_tickers_stream = MagicMock()
    tap.get_option_tickers_stream = MagicMock(
        return_value=tap._mock_option_tickers_stream
    )
    tap.get_option_contracts_for_underlying = (
        TapMassive.get_option_contracts_for_underlying.__get__(tap)
    )
    tap._paginate_option_contracts = TapMassive._paginate_option_contracts.__get__(tap)
    return tap


class TestGetOptionContractsForUnderlying:
    def test_fetches_contracts_with_query_params(self, mock_tap):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [
                {"ticker": "O:AAPL250321C00150000", "strike_price": 150},
                {"ticker": "O:AAPL250321C00250000", "strike_price": 250},
            ],
            "next_url": None,
        }
        mock_tap._mock_option_tickers_stream.get_response.return_value = mock_response
        contracts = mock_tap.get_option_contracts_for_underlying("AAPL")

        assert len(contracts) == 2
        call_args = mock_tap._mock_option_tickers_stream.get_response.call_args
        params = call_args.args[1]
        assert params["underlying_ticker"] == "AAPL"
        assert "strike_price.gte" not in params
        assert "strike_price.lte" not in params

    def test_pages_through_results(self, mock_tap):
        page1_response = MagicMock()
        page1_response.json.return_value = {
            "results": [{"ticker": "O:AAPL1"}],
            "next_url": "https://api.massive.com/v3/reference/options/contracts?cursor=abc",
        }
        page1_response.raise_for_status = MagicMock()

        page2_response = MagicMock()
        page2_response.json.return_value = {
            "results": [{"ticker": "O:AAPL2"}],
            "next_url": None,
        }
        mock_tap._mock_option_tickers_stream.get_response.side_effect = [
            page1_response,
            page2_response,
        ]
        contracts = mock_tap.get_option_contracts_for_underlying("AAPL")

        assert len(contracts) == 2
        assert contracts[0]["ticker"] == "O:AAPL1"
        assert contracts[1]["ticker"] == "O:AAPL2"

    def test_normalizes_double_underscore_params(self, mock_tap):
        mock_tap.config["option_tickers"]["query_params"][
            "expiration_date__gte"
        ] = "2025-01-01"

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [], "next_url": None}
        mock_tap._mock_option_tickers_stream.get_response.return_value = mock_response
        mock_tap.get_option_contracts_for_underlying("AAPL")

        params = mock_tap._mock_option_tickers_stream.get_response.call_args.args[1]
        assert "expiration_date.gte" in params
        assert "expiration_date__gte" not in params


class TestStateResetBetweenContracts:
    """Verify state is reset before each contract to prevent cross-contamination."""

    def test_state_reset_prevents_bookmark_leakage(self, tap_config):
        """Each contract should start with initial state, not previous contract's bookmark."""
        from tap_massive.option_streams import OptionsTickerPartitionStream

        stream = OptionsTickerPartitionStream.__new__(OptionsTickerPartitionStream)
        stream._tap = MagicMock()
        stream._tap.get_option_contracts_for_underlying.return_value = [
            {"ticker": "O:AAPL250117C00100000"},
            {"ticker": "O:AAPL250117C00200000"},
        ]
        stream.name = "test_stream"
        stream._ticker_param = "ticker"
        type(stream).config = PropertyMock(return_value=tap_config)

        # Track state values seen during paginate_records calls
        state_dict = {}
        stream.get_context_state = MagicMock(return_value=state_dict)
        stream._prepare_context_and_params = MagicMock(return_value=({}, {}, {}))

        states_seen = []

        def mock_paginate(ctx):
            # Record state before processing
            states_seen.append(dict(state_dict))
            # Simulate what paginate_records does: update state
            state_dict["replication_key_value"] = "2025-12-31T00:00:00Z"
            return iter([])

        stream.paginate_records = mock_paginate

        list(stream.get_records({"underlying": "AAPL"}))

        # Both contracts should see empty state (initial bookmark)
        assert len(states_seen) == 2
        assert states_seen[0] == {}  # First contract: fresh state
        assert states_seen[1] == {}  # Second contract: state was reset

    def test_passes_contract_expired_flag_into_context(self, tap_config):
        from tap_massive.option_streams import OptionsTickerPartitionStream

        stream = OptionsTickerPartitionStream.__new__(OptionsTickerPartitionStream)
        stream._tap = MagicMock()
        stream._tap.get_option_contracts_for_underlying.return_value = [
            {"ticker": "O:AAPL250117C00100000", "expired": True},
        ]
        stream.name = "test_stream"
        stream._ticker_param = "ticker"
        type(stream).config = PropertyMock(return_value=tap_config)

        stream.get_context_state = MagicMock(return_value={})
        stream._prepare_context_and_params = MagicMock(return_value=({}, {}, {}))

        contexts_seen = []

        def mock_paginate(ctx):
            contexts_seen.append(ctx)
            return iter([])

        stream.paginate_records = mock_paginate

        list(stream.get_records({"underlying": "AAPL"}))

        assert len(contexts_seen) == 1
        assert contexts_seen[0]["expired"] is True


class TestOptionsTickerPartitionStreamPartitions:
    def test_partitions_use_stock_tickers(self, tap_config):
        from tap_massive.option_streams import OptionsTickerPartitionStream

        # Remove select_tickers to test unfiltered
        cfg = dict(tap_config)
        cfg["option_tickers"] = dict(cfg["option_tickers"])
        cfg["option_tickers"].pop("select_tickers", None)

        mock_tap_obj = MagicMock()
        mock_tap_obj.get_cached_stock_tickers.return_value = [
            {"ticker": "AAPL"},
            {"ticker": "MSFT"},
            {"ticker": "GOOG"},
        ]

        stream = OptionsTickerPartitionStream.__new__(OptionsTickerPartitionStream)
        stream._tap = mock_tap_obj
        type(stream).config = PropertyMock(return_value=cfg)

        partitions = stream.partitions
        assert partitions == [
            {"underlying": "AAPL"},
            {"underlying": "MSFT"},
            {"underlying": "GOOG"},
        ]

    def test_partitions_filtered_by_select_tickers(self, tap_config):
        from tap_massive.option_streams import OptionsTickerPartitionStream

        mock_tap_obj = MagicMock()
        mock_tap_obj.get_cached_stock_tickers.return_value = [
            {"ticker": "AAPL"},
            {"ticker": "MSFT"},
            {"ticker": "GOOG"},
        ]

        stream = OptionsTickerPartitionStream.__new__(OptionsTickerPartitionStream)
        stream._tap = mock_tap_obj
        type(stream).config = PropertyMock(return_value=tap_config)

        partitions = stream.partitions
        # select_tickers is ["AAPL"] in config
        assert partitions == [{"underlying": "AAPL"}]

    def test_partitions_do_not_intersect_with_stock_ticker_config(self, tap_config):
        from tap_massive.option_streams import OptionsTickerPartitionStream

        cfg = dict(tap_config)
        cfg["option_tickers"] = dict(cfg["option_tickers"])
        cfg["option_tickers"]["select_tickers"] = ["META"]
        cfg["stock_tickers"] = {"select_tickers": ["AAPL"]}

        mock_tap_obj = MagicMock()
        mock_tap_obj.get_cached_stock_tickers.return_value = [{"ticker": "AAPL"}]

        stream = OptionsTickerPartitionStream.__new__(OptionsTickerPartitionStream)
        stream._tap = mock_tap_obj
        type(stream).config = PropertyMock(return_value=cfg)

        partitions = stream.partitions
        assert partitions == [{"underlying": "META"}]


class TestOptionsContractsTickerSelection:
    def test_prefers_option_tickers_select_tickers(self, tap_config):
        from tap_massive.option_streams import OptionsContractsStream

        cfg = dict(tap_config)
        cfg["option_tickers"] = dict(cfg["option_tickers"])
        cfg["option_tickers"]["select_tickers"] = ["AAPL", "META"]

        stream = OptionsContractsStream.__new__(OptionsContractsStream)
        type(stream).config = PropertyMock(return_value=cfg)

        assert stream.get_ticker_list() == ["AAPL", "META"]

    def test_falls_back_to_options_contracts_select_tickers(self, tap_config):
        from tap_massive.option_streams import OptionsContractsStream

        cfg = dict(tap_config)
        cfg["option_tickers"] = dict(cfg["option_tickers"])
        cfg["option_tickers"].pop("select_tickers", None)
        cfg["options_contracts"] = {"select_tickers": ["AAPL"]}

        stream = OptionsContractsStream.__new__(OptionsContractsStream)
        type(stream).config = PropertyMock(return_value=cfg)

        assert stream.get_ticker_list() == ["AAPL"]

    def test_returns_none_when_no_select_tickers(self, tap_config):
        from tap_massive.option_streams import OptionsContractsStream

        cfg = dict(tap_config)
        cfg["option_tickers"] = dict(cfg["option_tickers"])
        cfg["option_tickers"].pop("select_tickers", None)

        stream = OptionsContractsStream.__new__(OptionsContractsStream)
        type(stream).config = PropertyMock(return_value=cfg)

        assert stream.get_ticker_list() is None


class TestOptionsContractsBothMode:
    def test_loops_expired_true_false_per_selected_ticker(self, both_tap_config):
        from tap_massive.option_streams import OptionsContractsStream

        cfg = dict(both_tap_config)
        cfg["option_tickers"] = dict(cfg["option_tickers"])
        cfg["option_tickers"]["select_tickers"] = ["AAPL", "META"]

        stream = OptionsContractsStream.__new__(OptionsContractsStream)
        stream.query_params = {"sort": "ticker", "expired": True}
        stream.paginate_records = MagicMock(return_value=iter([]))
        type(stream).config = PropertyMock(return_value=cfg)

        list(stream.get_records(None))

        calls = stream.paginate_records.call_args_list
        assert len(calls) == 4
        params = [c.args[0]["query_params"] for c in calls]
        assert params[0]["underlying_ticker"] == "AAPL"
        assert params[0]["expired"] is True
        assert params[1]["underlying_ticker"] == "AAPL"
        assert params[1]["expired"] is False
        assert params[2]["underlying_ticker"] == "META"
        assert params[2]["expired"] is True
        assert params[3]["underlying_ticker"] == "META"
        assert params[3]["expired"] is False

    def test_loops_expired_true_false_without_ticker_filter(self, both_tap_config):
        from tap_massive.option_streams import OptionsContractsStream

        cfg = dict(both_tap_config)
        cfg["option_tickers"] = dict(cfg["option_tickers"])
        cfg["option_tickers"].pop("select_tickers", None)

        stream = OptionsContractsStream.__new__(OptionsContractsStream)
        stream.query_params = {"sort": "ticker", "expired": True}
        stream.paginate_records = MagicMock(return_value=iter([]))
        type(stream).config = PropertyMock(return_value=cfg)

        list(stream.get_records(None))

        calls = stream.paginate_records.call_args_list
        assert len(calls) == 2
        params = [c.args[0]["query_params"] for c in calls]
        assert params[0]["expired"] is True
        assert params[1]["expired"] is False
        assert "underlying_ticker" not in params[0]
        assert "underlying_ticker" not in params[1]


@pytest.fixture()
def both_tap_config():
    return {
        "api_key": "test_key",
        "option_tickers": {
            "query_params": {
                "sort": "ticker",
            },
            "other_params": {
                "expired": "both",
            },
            "select_tickers": ["AAPL"],
        },
    }


@pytest.fixture()
def mock_both_tap(both_tap_config):
    """Create a TapMassive instance configured with expired='both'."""
    tap = MagicMock(spec=TapMassive)
    tap.config = both_tap_config
    tap.logger = MagicMock()
    tap._mock_option_tickers_stream = MagicMock()
    tap.get_option_tickers_stream = MagicMock(
        return_value=tap._mock_option_tickers_stream
    )
    tap.get_option_contracts_for_underlying = (
        TapMassive.get_option_contracts_for_underlying.__get__(tap)
    )
    tap._paginate_option_contracts = TapMassive._paginate_option_contracts.__get__(tap)
    return tap


def _make_response(results, next_url=None):
    """Helper to create a mock requests response."""
    resp = MagicMock()
    resp.json.return_value = {"results": results, "next_url": next_url}
    resp.raise_for_status = MagicMock()
    return resp


class TestExpiredBothMode:
    def test_unions_expired_and_active_contracts(self, mock_both_tap):
        expired_resp = _make_response(
            [{"ticker": "O:AAPL240119C00100000"}, {"ticker": "O:AAPL240119P00100000"}]
        )
        active_resp = _make_response(
            [{"ticker": "O:AAPL260320C00200000"}, {"ticker": "O:AAPL260320P00200000"}]
        )
        mock_both_tap._mock_option_tickers_stream.get_response.side_effect = [
            expired_resp,
            active_resp,
        ]
        contracts = mock_both_tap.get_option_contracts_for_underlying("AAPL")

        assert len(contracts) == 4
        # Verify both calls were made with different expired values
        calls = mock_both_tap._mock_option_tickers_stream.get_response.call_args_list
        assert calls[0].args[1]["expired"] is True
        assert calls[1].args[1]["expired"] is False

    def test_deduplicates_by_ticker(self, mock_both_tap):
        shared = {"ticker": "O:AAPL260320C00200000", "strike_price": 200}
        expired_resp = _make_response([shared, {"ticker": "O:AAPL240119C00100000"}])
        active_resp = _make_response([shared])
        mock_both_tap._mock_option_tickers_stream.get_response.side_effect = [
            expired_resp,
            active_resp,
        ]
        contracts = mock_both_tap.get_option_contracts_for_underlying("AAPL")

        tickers = [c["ticker"] for c in contracts]
        assert len(tickers) == len(set(tickers))
        assert len(contracts) == 2

    def test_results_sorted_by_ticker(self, mock_both_tap):
        expired_resp = _make_response([{"ticker": "O:AAPL_C"}])
        active_resp = _make_response([{"ticker": "O:AAPL_A"}, {"ticker": "O:AAPL_B"}])
        mock_both_tap._mock_option_tickers_stream.get_response.side_effect = [
            expired_resp,
            active_resp,
        ]
        contracts = mock_both_tap.get_option_contracts_for_underlying("AAPL")

        assert [c["ticker"] for c in contracts] == [
            "O:AAPL_A",
            "O:AAPL_B",
            "O:AAPL_C",
        ]

    def test_strips_expired_from_query_params(self, mock_both_tap):
        """If query_params also has 'expired', it should be removed in both mode."""
        mock_both_tap.config["option_tickers"]["query_params"]["expired"] = True

        expired_resp = _make_response([])
        active_resp = _make_response([])
        mock_both_tap._mock_option_tickers_stream.get_response.side_effect = [
            expired_resp,
            active_resp,
        ]
        mock_both_tap.get_option_contracts_for_underlying("AAPL")

        # Each call should have the correct expired value, not the original True
        calls = mock_both_tap._mock_option_tickers_stream.get_response.call_args_list
        assert calls[0].args[1]["expired"] is True
        assert calls[1].args[1]["expired"] is False

    def test_adds_expired_flag_when_missing_in_api_response(self, mock_both_tap):
        expired_resp = _make_response([{"ticker": "O:AAPL240119C00100000"}])
        active_resp = _make_response([{"ticker": "O:AAPL260320C00200000"}])
        mock_both_tap._mock_option_tickers_stream.get_response.side_effect = [
            expired_resp,
            active_resp,
        ]

        contracts = mock_both_tap.get_option_contracts_for_underlying("AAPL")
        by_ticker = {contract["ticker"]: contract for contract in contracts}

        assert by_ticker["O:AAPL240119C00100000"]["expired"] is True
        assert by_ticker["O:AAPL260320C00200000"]["expired"] is False


class TestRetryLogic:
    def test_uses_stream_get_response(self, mock_tap):
        response = _make_response([{"ticker": "O:AAPL250321C00150000"}])
        mock_tap._mock_option_tickers_stream.get_response.return_value = response

        contracts = mock_tap.get_option_contracts_for_underlying("AAPL")

        assert len(contracts) == 1
        assert mock_tap._mock_option_tickers_stream.get_response.called

    def test_raises_on_403(self, mock_tap):
        mock_tap._mock_option_tickers_stream.get_response.side_effect = (
            requests.exceptions.HTTPError(response=MagicMock(status_code=403))
        )

        with pytest.raises(requests.exceptions.HTTPError):
            mock_tap.get_option_contracts_for_underlying("AAPL")

    def test_stops_on_no_data_response(self, mock_tap):
        mock_tap._mock_option_tickers_stream.get_response.return_value = None

        contracts = mock_tap.get_option_contracts_for_underlying("AAPL")

        assert contracts == []
        mock_tap._mock_option_tickers_stream.get_response.assert_called_once()
