"""Tests for per-underlying options partitioning and moneyness filtering."""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock, patch

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
            "other_params": {
                "moneyness_min": 0.5,
                "moneyness_max": 1.5,
                "max_dte": 120,
            },
            "select_tickers": ["AAPL"],
        },
    }


@pytest.fixture()
def mock_tap(tap_config):
    """Create a TapMassive instance with mocked config."""
    tap = MagicMock(spec=TapMassive)
    tap.config = tap_config
    tap._spot_price_cache = {}
    tap.logger = MagicMock()
    # Bind actual methods to the mock so they use real logic
    tap.get_spot_price = TapMassive.get_spot_price.__get__(tap)
    tap.get_option_contracts_for_underlying = (
        TapMassive.get_option_contracts_for_underlying.__get__(tap)
    )
    return tap


class TestGetSpotPrice:
    def test_returns_close_price(self, mock_tap):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {
            "results": [{"c": 230.50}],
        }
        mock_response.raise_for_status = MagicMock()

        with patch(
            "tap_massive.tap.requests.get", return_value=mock_response
        ) as mock_get:
            price = mock_tap.get_spot_price("AAPL")

        assert price == 230.50
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "AAPL" in call_args[0][0]
        assert call_args[1]["params"]["apiKey"] == "test_key"

    def test_caches_result(self, mock_tap):
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"c": 100.0}]}
        mock_response.raise_for_status = MagicMock()

        with patch(
            "tap_massive.tap.requests.get", return_value=mock_response
        ) as mock_get:
            price1 = mock_tap.get_spot_price("AAPL")
            price2 = mock_tap.get_spot_price("AAPL")

        assert price1 == 100.0
        assert price2 == 100.0
        assert mock_get.call_count == 1  # Only one API call due to caching

    def test_returns_none_on_error(self, mock_tap):
        with patch(
            "tap_massive.tap.requests.get",
            side_effect=requests.RequestException("API error"),
        ):
            price = mock_tap.get_spot_price("INVALID")

        assert price is None
        assert mock_tap._spot_price_cache["INVALID"] is None

    def test_returns_none_for_empty_results(self, mock_tap):
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": []}
        mock_response.raise_for_status = MagicMock()

        with patch("tap_massive.tap.requests.get", return_value=mock_response):
            price = mock_tap.get_spot_price("AAPL")

        assert price is None


class TestGetOptionContractsForUnderlying:
    def test_applies_moneyness_filter(self, mock_tap):
        mock_tap._spot_price_cache = {"AAPL": 200.0}

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [
                {"ticker": "O:AAPL250321C00150000", "strike_price": 150},
                {"ticker": "O:AAPL250321C00250000", "strike_price": 250},
            ],
            "next_url": None,
        }
        mock_response.raise_for_status = MagicMock()

        with patch(
            "tap_massive.tap.requests.get", return_value=mock_response
        ) as mock_get:
            contracts = mock_tap.get_option_contracts_for_underlying("AAPL")

        assert len(contracts) == 2
        call_args = mock_get.call_args
        params = call_args[1]["params"]
        # moneyness_min=0.5 * 200 = 100, moneyness_max=1.5 * 200 = 300
        assert params["strike_price.gte"] == 100.0
        assert params["strike_price.lte"] == 300.0
        assert params["underlying_ticker"] == "AAPL"

    def test_applies_max_dte_filter_when_not_expired(self, mock_tap):
        mock_tap._spot_price_cache = {"AAPL": 200.0}
        mock_tap.config["option_tickers"]["query_params"]["expired"] = False

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [], "next_url": None}
        mock_response.raise_for_status = MagicMock()

        with patch(
            "tap_massive.tap.requests.get", return_value=mock_response
        ) as mock_get:
            mock_tap.get_option_contracts_for_underlying("AAPL")

        params = mock_get.call_args[1]["params"]
        assert "expiration_date.lte" in params

    def test_pages_through_results(self, mock_tap):
        mock_tap._spot_price_cache = {"AAPL": 200.0}

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
        page2_response.raise_for_status = MagicMock()

        with patch(
            "tap_massive.tap.requests.get",
            side_effect=[page1_response, page2_response],
        ):
            contracts = mock_tap.get_option_contracts_for_underlying("AAPL")

        assert len(contracts) == 2
        assert contracts[0]["ticker"] == "O:AAPL1"
        assert contracts[1]["ticker"] == "O:AAPL2"

    def test_no_moneyness_filter_without_config(self, mock_tap):
        mock_tap.config["option_tickers"]["other_params"] = {}
        mock_tap._spot_price_cache = {}

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [], "next_url": None}
        mock_response.raise_for_status = MagicMock()

        with patch(
            "tap_massive.tap.requests.get", return_value=mock_response
        ) as mock_get:
            mock_tap.get_option_contracts_for_underlying("AAPL")

        params = mock_get.call_args[1]["params"]
        assert "strike_price.gte" not in params
        assert "strike_price.lte" not in params

    def test_skips_moneyness_without_spot_price(self, mock_tap):
        mock_tap._spot_price_cache = {"AAPL": None}

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [], "next_url": None}
        mock_response.raise_for_status = MagicMock()

        with patch(
            "tap_massive.tap.requests.get", return_value=mock_response
        ) as mock_get:
            mock_tap.get_option_contracts_for_underlying("AAPL")

        params = mock_get.call_args[1]["params"]
        assert "strike_price.gte" not in params
        assert "strike_price.lte" not in params

    def test_normalizes_double_underscore_params(self, mock_tap):
        mock_tap.config["option_tickers"]["query_params"][
            "expiration_date__gte"
        ] = "2025-01-01"
        mock_tap._spot_price_cache = {"AAPL": 200.0}

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [], "next_url": None}
        mock_response.raise_for_status = MagicMock()

        with patch(
            "tap_massive.tap.requests.get", return_value=mock_response
        ) as mock_get:
            mock_tap.get_option_contracts_for_underlying("AAPL")

        params = mock_get.call_args[1]["params"]
        assert "expiration_date.gte" in params
        assert "expiration_date__gte" not in params


class TestDteFilterGuard:
    """Verify DTE filter is skipped when expired=true."""

    def test_dte_skipped_when_expired_true(self, mock_tap):
        """When expired=true, max_dte should NOT set expiration_date.lte."""
        mock_tap.config["option_tickers"]["query_params"]["expired"] = True
        mock_tap.config["option_tickers"]["other_params"] = {"max_dte": 120}
        mock_tap._spot_price_cache = {}

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [], "next_url": None}
        mock_response.raise_for_status = MagicMock()

        with patch(
            "tap_massive.tap.requests.get", return_value=mock_response
        ) as mock_get:
            mock_tap.get_option_contracts_for_underlying("AAPL")

        params = mock_get.call_args[1]["params"]
        assert "expiration_date.lte" not in params

    def test_dte_applied_when_expired_false(self, mock_tap):
        """When expired=false, max_dte should set expiration_date.lte."""
        mock_tap.config["option_tickers"]["query_params"]["expired"] = False
        mock_tap.config["option_tickers"]["other_params"] = {"max_dte": 120}
        mock_tap._spot_price_cache = {}

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [], "next_url": None}
        mock_response.raise_for_status = MagicMock()

        with patch(
            "tap_massive.tap.requests.get", return_value=mock_response
        ) as mock_get:
            mock_tap.get_option_contracts_for_underlying("AAPL")

        params = mock_get.call_args[1]["params"]
        assert "expiration_date.lte" in params


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
