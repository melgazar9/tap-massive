"""Tests for QuoteUpdateBarStream interval-contained semantics and post_process."""

from __future__ import annotations

from unittest.mock import MagicMock

from tap_massive.quote_update_bar_streams import QuoteUpdateBarStream

_TICKER = "TEST"
_INTERVAL_NS = 60_000_000_000  # 1 minute


def _make_stream(api_key: str = "test", interval_seconds: int = 60) -> MagicMock:
    """Create a minimal mock stream for quote update bar tests."""
    stream = MagicMock()
    stream.config = {"api_key": api_key}
    stream._ticker_param = "ticker"
    stream._interval_seconds = interval_seconds
    stream.post_process = lambda row, ctx: row
    stream.other_params = {}
    stream._streaming_fetch_threshold = 300
    # Bind real methods so yield from self.method() works on the mock
    stream._emit_if_in_interval = (
        lambda *a, **kw: QuoteUpdateBarStream._emit_if_in_interval(stream, *a, **kw)
    )
    stream._fetch_per_boundary = (
        lambda *a, **kw: QuoteUpdateBarStream._fetch_per_boundary(stream, *a, **kw)
    )
    return stream


def _make_response(results: list[dict], next_url: str | None = None) -> MagicMock:
    resp = MagicMock()
    data = {"results": results, "status": "OK"}
    if next_url:
        data["next_url"] = next_url
    resp.json.return_value = data
    return resp


# --- Interval-contained semantics ---


class TestIntervalContainedSemantics:
    """Verify only quotes within (T-I, T] are emitted."""

    def test_quote_in_interval_is_emitted(self):
        """A quote within (window - interval, window] should be yielded."""
        stream = _make_stream()
        result = list(
            QuoteUpdateBarStream._emit_if_in_interval(
                stream,
                {"sip_timestamp": 500, "bid_price": 1.0},
                500,
                1000,
                600,
                _TICKER,
                {"ticker": _TICKER},
            )
        )
        assert len(result) == 1
        assert result[0]["window_start"] == 1000

    def test_quote_outside_interval_is_skipped(self):
        """A quote older than (window - interval) should NOT be yielded."""
        stream = _make_stream()
        result = list(
            QuoteUpdateBarStream._emit_if_in_interval(
                stream,
                {"sip_timestamp": 100, "bid_price": 1.0},
                100,
                1000,
                600,
                _TICKER,
                {"ticker": _TICKER},
            )
        )
        assert result == []

    def test_no_quote_yields_nothing(self):
        """No last_quote should yield nothing."""
        stream = _make_stream()
        result = list(
            QuoteUpdateBarStream._emit_if_in_interval(
                stream,
                None,
                None,
                1000,
                600,
                _TICKER,
                {"ticker": _TICKER},
            )
        )
        assert result == []

    def test_quote_exactly_on_boundary_is_included(self):
        """A quote at exactly T should be within (T-I, T]."""
        stream = _make_stream()
        result = list(
            QuoteUpdateBarStream._emit_if_in_interval(
                stream,
                {"sip_timestamp": 1000, "bid_price": 1.0},
                1000,
                1000,
                600,
                _TICKER,
                {"ticker": _TICKER},
            )
        )
        assert len(result) == 1


# --- Per-boundary fallback ---


class TestPerBoundaryFallback:
    """Verify per-boundary queries use interval containment."""

    def test_uses_interval_containment(self):
        """Per-boundary fallback should use timestamp.gt for interval filtering."""
        stream = _make_stream()
        window_ns = 1_000_000_000_000
        quote = {"sip_timestamp": window_ns - 30_000_000_000, "bid_price": 1.0}
        stream.get_response.return_value = _make_response([quote])

        results = list(
            QuoteUpdateBarStream._fetch_per_boundary(
                stream,
                "http://x",
                [window_ns],
                _INTERVAL_NS,
                _TICKER,
                {"ticker": _TICKER},
            )
        )

        assert len(results) == 1
        # Verify query params use interval containment
        call_args = stream.get_response.call_args
        params = call_args.args[1]
        assert params["timestamp.lte"] == window_ns
        assert params["timestamp.gt"] == window_ns - _INTERVAL_NS
        assert params["order"] == "desc"
        assert params["limit"] == 1

    def test_empty_results_skips_window(self):
        """No quotes in interval means no output for that window."""
        stream = _make_stream()
        stream.get_response.return_value = _make_response([])

        results = list(
            QuoteUpdateBarStream._fetch_per_boundary(
                stream,
                "http://x",
                [1_000_000_000],
                _INTERVAL_NS,
                _TICKER,
                {"ticker": _TICKER},
            )
        )
        assert results == []

    def test_none_response_skips_window(self):
        stream = _make_stream()
        stream.get_response.return_value = None

        results = list(
            QuoteUpdateBarStream._fetch_per_boundary(
                stream,
                "http://x",
                [1_000_000_000],
                _INTERVAL_NS,
                _TICKER,
                {"ticker": _TICKER},
            )
        )
        assert results == []


# --- Streaming merge-join ---


class TestStreamAndSample:
    """Verify streaming merge-join samples correctly at boundaries."""

    def test_single_page_multiple_windows(self):
        """Quotes spanning multiple windows should emit one bar per window."""
        stream = _make_stream()
        interval_ns = 100
        windows = [200, 300, 400]

        # Quotes: one at 150 (in window 200), one at 250 (in window 300), one at 350 (in window 400)
        quotes = [
            {"sip_timestamp": 150, "bid_price": 1.0},
            {"sip_timestamp": 250, "bid_price": 2.0},
            {"sip_timestamp": 350, "bid_price": 3.0},
        ]
        stream.get_response.return_value = _make_response(quotes)

        results = list(
            QuoteUpdateBarStream._stream_and_sample(
                stream, "http://x", windows, interval_ns, _TICKER, {"ticker": _TICKER}
            )
        )

        assert len(results) == 3
        assert results[0]["bid_price"] == 1.0
        assert results[0]["window_start"] == 200
        assert results[1]["bid_price"] == 2.0
        assert results[1]["window_start"] == 300
        assert results[2]["bid_price"] == 3.0
        assert results[2]["window_start"] == 400

    def test_gap_windows_are_skipped(self):
        """Windows with no quote update in interval should produce no output."""
        stream = _make_stream()
        interval_ns = 100
        windows = [200, 300, 400]

        # Only one quote at 150 — covers window 200 but not 300 or 400
        quotes = [{"sip_timestamp": 150, "bid_price": 1.0}]
        stream.get_response.return_value = _make_response(quotes)

        results = list(
            QuoteUpdateBarStream._stream_and_sample(
                stream, "http://x", windows, interval_ns, _TICKER, {"ticker": _TICKER}
            )
        )

        assert len(results) == 1
        assert results[0]["window_start"] == 200

    def test_no_quotes_yields_nothing(self):
        """Empty API response means no bars."""
        stream = _make_stream()
        stream.get_response.return_value = _make_response([])

        results = list(
            QuoteUpdateBarStream._stream_and_sample(
                stream, "http://x", [100, 200], 100, _TICKER, {"ticker": _TICKER}
            )
        )
        assert results == []

    def test_multiple_quotes_in_same_window_uses_latest(self):
        """Multiple quotes in one interval should use the last one."""
        stream = _make_stream()
        interval_ns = 100
        windows = [200]

        quotes = [
            {"sip_timestamp": 120, "bid_price": 1.0},
            {"sip_timestamp": 150, "bid_price": 2.0},
            {"sip_timestamp": 180, "bid_price": 3.0},
        ]
        stream.get_response.return_value = _make_response(quotes)

        results = list(
            QuoteUpdateBarStream._stream_and_sample(
                stream, "http://x", windows, interval_ns, _TICKER, {"ticker": _TICKER}
            )
        )

        assert len(results) == 1
        assert results[0]["bid_price"] == 3.0
