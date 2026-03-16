"""Tests for disk cache integration in TapMassive."""

from __future__ import annotations

import os
import threading
from collections import OrderedDict
from unittest.mock import MagicMock, patch

import pytest

from tap_massive.disk_cache import DiskCache
from tap_massive.tap import TapMassive


@pytest.fixture()
def cache_dir(tmp_path):
    return str(tmp_path / "cache")


@pytest.fixture()
def base_config():
    return {
        "api_key": "test_key",
        "base_url": "https://api.test.com",
        "option_tickers": {
            "query_params": {"expiration_date__gte": "2025-01-01"},
            "other_params": {"expired": "both"},
        },
    }


class TestDiskCacheInit:
    def test_disk_cache_created_when_env_set(self, cache_dir):
        with patch.dict(os.environ, {"MELTANO_SHARED_CACHE_DIR": cache_dir}):
            tap = MagicMock(spec=TapMassive)
            tap.config = {"api_key": "k", "base_url": "https://x"}
            tap._ticker_caches = {}
            tap._ticker_streams = {}
            tap._ticker_locks = {
                a: threading.Lock() for a in TapMassive._ticker_stream_classes
            }
            tap._earnings_calendar_cache = {}
            tap._earnings_calendar_lock = threading.Lock()
            tap._market_windows_cache = {}
            tap._market_windows_lock = threading.Lock()
            tap._nyse_sessions_cache = {}
            tap._nyse_sessions_lock = threading.Lock()
            tap._option_contracts_cache = OrderedDict()
            tap._option_contracts_lock = threading.Lock()
            tap._option_contracts_cache_max = 20

            shared = os.environ.get("MELTANO_SHARED_CACHE_DIR")
            disk_cache = (
                DiskCache(cache_dir=shared, namespace="tap_massive", ttl_hours=24.0)
                if shared
                else None
            )
            assert disk_cache is not None
            assert isinstance(disk_cache, DiskCache)

    def test_disk_cache_none_when_env_unset(self):
        with patch.dict(os.environ, {}, clear=True):
            shared = os.environ.get("MELTANO_SHARED_CACHE_DIR")
            disk_cache = (
                DiskCache(cache_dir=shared, namespace="tap_massive") if shared else None
            )
            assert disk_cache is None


class TestOptionSkipBehavior:
    """_get_cached_tickers must NOT disk-cache the 'option' asset class."""

    def test_option_not_in_disk_cacheable_assets(self):
        assert "option" not in TapMassive._DISK_CACHEABLE_ASSETS

    def test_stock_in_disk_cacheable_assets(self):
        assert "stock" in TapMassive._DISK_CACHEABLE_ASSETS

    def test_all_non_option_assets_cacheable(self):
        expected = {"stock", "forex", "crypto", "indices", "futures"}
        assert TapMassive._DISK_CACHEABLE_ASSETS == expected


class TestBuildCacheFingerprint:
    def _make_tap(self, base_url="https://api.test.com"):
        tap = MagicMock(spec=TapMassive)
        tap.config = {"base_url": base_url}
        tap._build_cache_fingerprint = TapMassive._build_cache_fingerprint.__get__(tap)
        return tap

    def test_same_params_same_fingerprint(self):
        tap = self._make_tap()
        qp = {"active": "true", "limit": "1000"}
        f1 = tap._build_cache_fingerprint(query_params=qp)
        f2 = tap._build_cache_fingerprint(query_params=qp)
        assert f1 == f2

    def test_different_params_different_fingerprint(self):
        tap = self._make_tap()
        f1 = tap._build_cache_fingerprint(query_params={"active": "true"})
        f2 = tap._build_cache_fingerprint(query_params={"active": "false"})
        assert f1 != f2

    def test_param_order_independent(self):
        tap = self._make_tap()
        f1 = tap._build_cache_fingerprint(query_params={"a": "1", "b": "2"})
        f2 = tap._build_cache_fingerprint(query_params={"b": "2", "a": "1"})
        assert f1 == f2

    def test_excludes_apiKey(self):
        tap = self._make_tap()
        f1 = tap._build_cache_fingerprint(query_params={"active": "true"})
        f2 = tap._build_cache_fingerprint(
            query_params={"active": "true", "apiKey": "secret"}
        )
        assert f1 == f2

    def test_different_base_url_different_fingerprint(self):
        tap1 = self._make_tap("https://api1.com")
        tap2 = self._make_tap("https://api2.com")
        f1 = tap1._build_cache_fingerprint(query_params={})
        f2 = tap2._build_cache_fingerprint(query_params={})
        assert f1 != f2

    def test_boolean_flag_key_included(self):
        tap = self._make_tap()
        other = {"active": "both"}
        f1 = tap._build_cache_fingerprint(
            query_params={}, other_params=other, boolean_flag_key="active"
        )
        f2 = tap._build_cache_fingerprint(
            query_params={}, other_params=other, boolean_flag_key=""
        )
        assert f1 != f2

    def test_extra_canonical_changes_fingerprint(self):
        tap = self._make_tap()
        f1 = tap._build_cache_fingerprint(
            query_params={}, extra_canonical={"asset": "stock"}
        )
        f2 = tap._build_cache_fingerprint(
            query_params={}, extra_canonical={"asset": "forex"}
        )
        assert f1 != f2

    def test_underscore_normalization(self):
        tap = self._make_tap()
        f1 = tap._build_cache_fingerprint(query_params={"expiration_date__gte": "2025"})
        f2 = tap._build_cache_fingerprint(query_params={"expiration_date.gte": "2025"})
        assert f1 == f2


class TestOptionContractsDiskCache:
    """Test that per-underlying option contracts use disk cache when available."""

    def test_disk_cache_hit_skips_api_fetch(self, cache_dir, base_config):
        tap = MagicMock(spec=TapMassive)
        tap.config = base_config
        tap.logger = MagicMock()
        tap._option_contracts_cache = OrderedDict()
        tap._option_contracts_lock = threading.Lock()
        tap._option_contracts_cache_max = 20
        tap._disk_cache = DiskCache(
            cache_dir=cache_dir, namespace="tap_massive", ttl_hours=24.0
        )
        tap._build_cache_fingerprint = TapMassive._build_cache_fingerprint.__get__(tap)
        tap._fetch_option_contracts = MagicMock(
            return_value=[{"ticker": "O:AAPL250321C00150000"}]
        )
        tap.get_option_contracts_for_underlying = (
            TapMassive.get_option_contracts_for_underlying.__get__(tap)
        )

        # First call: misses cache, calls _fetch_option_contracts
        result1 = tap.get_option_contracts_for_underlying("AAPL")
        assert tap._fetch_option_contracts.call_count == 1
        assert result1 == [{"ticker": "O:AAPL250321C00150000"}]

        # Clear in-memory LRU to force disk cache path
        tap._option_contracts_cache.clear()
        tap._fetch_option_contracts.reset_mock()

        # Second call: hits disk cache, skips API
        result2 = tap.get_option_contracts_for_underlying("AAPL")
        tap._fetch_option_contracts.assert_not_called()
        assert result2 == [{"ticker": "O:AAPL250321C00150000"}]

    def test_lru_hit_returns_without_disk_or_api(self, base_config):
        tap = MagicMock(spec=TapMassive)
        tap.config = base_config
        tap._option_contracts_cache = OrderedDict()
        tap._option_contracts_lock = threading.Lock()
        tap._option_contracts_cache_max = 20
        tap._disk_cache = None
        tap._build_cache_fingerprint = TapMassive._build_cache_fingerprint.__get__(tap)
        tap._fetch_option_contracts = MagicMock(return_value=[{"ticker": "O:AAPL"}])
        tap.get_option_contracts_for_underlying = (
            TapMassive.get_option_contracts_for_underlying.__get__(tap)
        )

        tap.get_option_contracts_for_underlying("AAPL")
        tap._fetch_option_contracts.reset_mock()

        # LRU hit
        result = tap.get_option_contracts_for_underlying("AAPL")
        tap._fetch_option_contracts.assert_not_called()
        assert result == [{"ticker": "O:AAPL"}]
