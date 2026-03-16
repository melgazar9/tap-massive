"""Tests for cross-process disk cache."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from tap_massive.disk_cache import (
    DiskCache,
    _sanitize_path_component,
    compute_fingerprint,
)


@pytest.fixture()
def cache_dir(tmp_path):
    return str(tmp_path / "cache")


@pytest.fixture()
def cache(cache_dir):
    return DiskCache(cache_dir=cache_dir, namespace="test", ttl_hours=1.0)


class TestComputeFingerprint:
    def test_deterministic(self):
        canonical = {"a": 1, "b": "two"}
        assert compute_fingerprint(canonical) == compute_fingerprint(canonical)

    def test_key_order_independent(self):
        assert compute_fingerprint({"a": 1, "b": 2}) == compute_fingerprint(
            {"b": 2, "a": 1}
        )

    def test_different_values_different_fingerprint(self):
        assert compute_fingerprint({"a": 1}) != compute_fingerprint({"a": 2})


class TestSanitizePath:
    def test_strips_slashes(self):
        assert "/" not in _sanitize_path_component("a/b")
        assert "\\" not in _sanitize_path_component("a\\b")

    def test_strips_dotdot(self):
        assert ".." not in _sanitize_path_component("../etc")

    def test_strips_null(self):
        assert "\x00" not in _sanitize_path_component("a\x00b")

    def test_normal_tickers_unchanged(self):
        assert _sanitize_path_component("AAPL") == "AAPL"
        assert _sanitize_path_component("BRK.A") == "BRK.A"


class TestDiskCacheInit:
    def test_rejects_zero_ttl(self):
        with pytest.raises(ValueError, match="positive"):
            DiskCache(cache_dir="/tmp", namespace="x", ttl_hours=0)

    def test_rejects_negative_ttl(self):
        with pytest.raises(ValueError, match="positive"):
            DiskCache(cache_dir="/tmp", namespace="x", ttl_hours=-1)


class TestGetOrFetch:
    def test_miss_calls_fetch_fn(self, cache):
        fetch = MagicMock(return_value=[{"ticker": "AAPL"}])
        result = cache.get_or_fetch("tickers/stock/abc123", fetch)
        fetch.assert_called_once()
        assert result == [{"ticker": "AAPL"}]

    def test_hit_skips_fetch_fn(self, cache):
        fetch = MagicMock(return_value=[{"ticker": "AAPL"}])
        cache.get_or_fetch("tickers/stock/abc123", fetch)
        fetch.reset_mock()

        result = cache.get_or_fetch("tickers/stock/abc123", fetch)
        fetch.assert_not_called()
        assert result == [{"ticker": "AAPL"}]

    def test_different_keys_are_independent(self, cache):
        cache.get_or_fetch("key/a", lambda: [1])
        cache.get_or_fetch("key/b", lambda: [2])
        assert cache.get_or_fetch("key/a", lambda: [99]) == [1]
        assert cache.get_or_fetch("key/b", lambda: [99]) == [2]

    def test_empty_list_is_valid_cache_entry(self, cache):
        fetch = MagicMock(return_value=[])
        cache.get_or_fetch("empty", fetch)
        fetch.reset_mock()

        result = cache.get_or_fetch("empty", fetch)
        fetch.assert_not_called()
        assert result == []

    def test_ttl_expiry_triggers_refetch(self, cache_dir):
        cache = DiskCache(cache_dir=cache_dir, namespace="test", ttl_hours=0.0001)
        cache.get_or_fetch("key", lambda: ["old"])
        time.sleep(0.5)

        fetch = MagicMock(return_value=["new"])
        result = cache.get_or_fetch("key", fetch)
        fetch.assert_called_once()
        assert result == ["new"]

    def test_corrupt_manifest_triggers_refetch(self, cache):
        cache.get_or_fetch("corrupt", lambda: ["good"])
        manifest_path = cache._manifest_path("corrupt")
        with open(manifest_path, "w") as f:
            f.write("{invalid json")

        fetch = MagicMock(return_value=["recovered"])
        result = cache.get_or_fetch("corrupt", fetch)
        fetch.assert_called_once()
        assert result == ["recovered"]

    def test_fetch_fn_error_propagates(self, cache):
        def failing_fetch():
            raise ConnectionError("API down")

        with pytest.raises(ConnectionError, match="API down"):
            cache.get_or_fetch("fail", failing_fetch)

    def test_creates_directories_on_demand(self, cache):
        result = cache.get_or_fetch("deep/nested/key", lambda: ["data"])
        assert result == ["data"]

    def test_non_json_payload_raises(self, cache):
        """Payloads with non-JSON-native types should fail loudly, not silently stringify."""
        from datetime import datetime

        with pytest.raises(TypeError):
            cache.get_or_fetch("bad", lambda: [{"ts": datetime.now()}])
