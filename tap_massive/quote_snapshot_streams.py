"""Quote snapshot stream classes for tap-massive.

Interval-contained bid/ask snapshots at fixed boundaries.
Only emits a row if there was a quote update in (T-interval, T].
Uses a streaming merge-join for performance: fetches quotes ascending,
samples at boundaries, falls back to per-boundary queries for dense data.
"""

from __future__ import annotations

import logging
import typing as t
from datetime import datetime, timezone

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context, Record

from tap_massive.base_streams import (
    BaseTickerPartitionStream,
    _NanosecondIncrementalMixin,
)
from tap_massive.utils import safe_int


class QuoteSnapshotStream(_NanosecondIncrementalMixin, BaseTickerPartitionStream):
    """Interval-contained quote update bar: last quote in (T-I, T] per asof_timestamp."""

    _interval_seconds: int | None = None  # Must be set by subclass
    _use_market_calendar = True  # NYSE hours only; override False for 24/7 markets
    _streaming_fetch_threshold = 300  # Use streaming for intervals <= this (seconds)

    primary_keys = ["ticker", "asof_timestamp"]
    replication_key = "asof_timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    is_sorted = True

    _use_cached_tickers_default = True
    _api_expects_unix_timestamp = True
    _unix_timestamp_unit = "ns"
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("ask_exchange", th.IntegerType),
        th.Property("ask_price", th.NumberType),
        th.Property("ask_size", th.NumberType),
        th.Property("bid_exchange", th.IntegerType),
        th.Property("bid_price", th.NumberType),
        th.Property("bid_size", th.NumberType),
        th.Property("conditions", th.ArrayType(th.IntegerType)),
        th.Property("indicators", th.ArrayType(th.IntegerType)),
        th.Property("participant_timestamp", th.IntegerType),
        th.Property(
            "sequence_number",
            th.IntegerType(
                minimum=0,
                maximum=9_223_372_036_854_775_807,
            ),
        ),
        th.Property("sip_timestamp", th.IntegerType),
        th.Property("tape", th.IntegerType),
        th.Property("trf_timestamp", th.IntegerType),
        th.Property("asof_timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v3/quotes/{ticker}"

    def get_records(self, context: dict[str, t.Any]) -> t.Iterable[dict[str, t.Any]]:
        """Fetch and yield quote update bars for a single ticker."""
        ticker = context.get(self._ticker_param)
        start_ns = self.get_starting_replication_key_value(context)
        if start_ns is None:
            return

        end_ns = self._get_end_timestamp_value()
        if end_ns is None:
            end_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)

        yield from self._yield_update_bars(ticker, start_ns, end_ns, context)

    def _yield_update_bars(
        self,
        ticker: str,
        start_ns: int,
        end_ns: int,
        context: dict[str, t.Any] | None,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Compute asof_timestamp boundaries and yield quote update bars for a time range."""
        if self._use_market_calendar:
            windows = self._tap.get_market_windows(
                self._interval_seconds, start_ns, end_ns
            )
        else:
            windows = self._compute_fixed_windows(start_ns, end_ns)

        if not windows:
            return

        url = self.get_url({self._ticker_param: ticker})
        interval_ns = self._interval_seconds * 1_000_000_000
        row_context = {**(context or {}), self._ticker_param: ticker}

        threshold = self.other_params.get(
            "streaming_fetch_threshold", self._streaming_fetch_threshold
        )
        if self._interval_seconds <= threshold:
            yield from self._stream_and_sample(
                url, windows, interval_ns, ticker, row_context
            )
        else:
            yield from self._fetch_per_boundary(
                url, windows, interval_ns, ticker, row_context
            )

    def _stream_and_sample(
        self,
        url: str,
        windows: list[int],
        interval_ns: int,
        ticker: str,
        row_context: dict,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Streaming merge-join: fetch quotes ascending, sample at asof_timestamp boundaries.

        Paginates through all quotes in the range. As quotes cross asof_timestamp
        boundaries, emits the last quote seen if it falls within the interval.
        Falls back to per-boundary queries if data is too dense.
        """
        fetch_url = url
        params: dict[str, t.Any] = {
            "timestamp.gte": windows[0] - interval_ns,
            "timestamp.lte": windows[-1],
            "sort": "timestamp",
            "order": "asc",
            "limit": self.query_params.get("limit", 50000),
            "apiKey": self.config["api_key"],
        }

        window_idx = 0
        last_quote: dict | None = None
        last_sip_ts: int | None = None
        pages_without_advance = 0

        while window_idx < len(windows):
            response = self.get_response(fetch_url, params)
            if response is None:
                break
            data = response.json()
            results = data.get("results", [])
            if not results:
                break

            start_window_idx = window_idx

            for quote in results:
                sip_ts = quote.get("sip_timestamp")
                if sip_ts is None:
                    continue

                # Emit for all windows this quote crosses past
                while window_idx < len(windows) and sip_ts > windows[window_idx]:
                    yield from self._emit_if_in_interval(
                        last_quote,
                        last_sip_ts,
                        windows[window_idx],
                        interval_ns,
                        ticker,
                        row_context,
                    )
                    window_idx += 1

                last_quote = quote
                last_sip_ts = sip_ts

            # Density fallback: if 2 consecutive pages didn't advance, too dense
            if window_idx == start_window_idx:
                pages_without_advance += 1
                if pages_without_advance >= 2:
                    logging.info(
                        f"Dense data for {ticker}, falling back to per-boundary "
                        f"queries for {len(windows) - window_idx} remaining windows."
                    )
                    yield from self._fetch_per_boundary(
                        url, windows[window_idx:], interval_ns, ticker, row_context
                    )
                    return
            else:
                pages_without_advance = 0

            next_url = data.get("next_url")
            if not next_url:
                break
            fetch_url = next_url
            params = {"apiKey": self.config["api_key"]}

        # Emit for remaining windows after pagination exhausts
        while window_idx < len(windows):
            yield from self._emit_if_in_interval(
                last_quote,
                last_sip_ts,
                windows[window_idx],
                interval_ns,
                ticker,
                row_context,
            )
            window_idx += 1

    def _fetch_per_boundary(
        self,
        url: str,
        windows: list[int],
        interval_ns: int,
        ticker: str,
        row_context: dict,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Per-boundary queries: one API call per asof_timestamp with interval containment."""
        for asof_timestamp_ns in windows:
            params = {
                "timestamp.lte": asof_timestamp_ns,
                "timestamp.gt": asof_timestamp_ns - interval_ns,
                "sort": "timestamp",
                "order": "desc",
                "limit": 1,
                "apiKey": self.config["api_key"],
            }
            response = self.get_response(url, params)
            if response is None:
                continue
            results = response.json().get("results", [])
            if not results:
                continue
            quote = results[0]
            if quote.get("sip_timestamp") is None:
                continue
            row = {**quote, "ticker": ticker, "asof_timestamp": asof_timestamp_ns}
            processed = self.post_process(row, row_context)
            if processed is not None:
                yield processed

    def _emit_if_in_interval(
        self,
        last_quote: dict | None,
        last_sip_ts: int | None,
        window_ns: int,
        interval_ns: int,
        ticker: str,
        row_context: dict,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Emit a record only if last_quote falls within (asof_timestamp - interval, asof_timestamp]."""
        if last_sip_ts is not None and last_sip_ts > window_ns - interval_ns:
            row = {**last_quote, "ticker": ticker, "asof_timestamp": window_ns}
            processed = self.post_process(row, row_context)
            if processed is not None:
                yield processed

    def _compute_fixed_windows(self, start_ns: int, end_ns: int) -> list[int]:
        """Return asof_timestamp boundaries at fixed intervals (24/7 markets)."""
        interval_ns = self._interval_seconds * 1_000_000_000
        windows: list[int] = []
        current_ns = start_ns
        while current_ns < end_ns:
            windows.append(current_ns)
            current_ns += interval_ns
        return windows

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        """Post-process quote update bar record with computed fields."""
        row["ticker"] = (
            context.get(self._ticker_param) if context else row.get("ticker")
        )
        row["sip_timestamp"] = safe_int(row.get("sip_timestamp"))
        if "participant_timestamp" in row:
            row["participant_timestamp"] = safe_int(row["participant_timestamp"])
        if "trf_timestamp" in row:
            row["trf_timestamp"] = safe_int(row["trf_timestamp"])
        return row


class QuoteSnapshot1SecondStream(QuoteSnapshotStream):
    """Quote update bars at 1-second intervals."""

    _interval_seconds = 1


class QuoteSnapshot30SecondStream(QuoteSnapshotStream):
    """Quote update bars at 30-second intervals."""

    _interval_seconds = 30


class QuoteSnapshot1MinuteStream(QuoteSnapshotStream):
    """Quote update bars at 1-minute intervals."""

    _interval_seconds = 60


class QuoteSnapshot5MinuteStream(QuoteSnapshotStream):
    """Quote update bars at 5-minute intervals."""

    _interval_seconds = 300


class QuoteSnapshot30MinuteStream(QuoteSnapshotStream):
    """Quote update bars at 30-minute intervals."""

    _interval_seconds = 1800


class QuoteSnapshot1HourStream(QuoteSnapshotStream):
    """Quote update bars at 1-hour intervals."""

    _interval_seconds = 3600
