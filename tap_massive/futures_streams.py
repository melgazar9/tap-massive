"""Futures stream classes for tap-massive."""

from __future__ import annotations

import logging
import typing as t

from singer_sdk import typing as th
from singer_sdk.helpers._typing import TypeConformanceLevel
from singer_sdk.helpers.types import Context

from tap_massive.base_streams import (
    BaseCustomBarsStream,
    BaseTickerPartitionStream,
    _NanosecondIncrementalMixin,
)
from tap_massive.client import MassiveRestStream
from tap_massive.utils import safe_decimal, safe_int

# ---------------------------------------------------------------------------
# Reference data streams
# ---------------------------------------------------------------------------


class FuturesContractsStream(MassiveRestStream):
    """Futures contracts reference data.

    Endpoint: ``GET /futures/vX/contracts``
    Max limit: 1000
    """

    name = "futures_contracts"
    primary_keys = ["ticker", "date"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("name", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("date", th.DateType),
        th.Property("type", th.StringType),
        th.Property("product_code", th.StringType),
        th.Property("group_code", th.StringType),
        th.Property("trading_venue", th.StringType),
        th.Property("first_trade_date", th.DateType),
        th.Property("last_trade_date", th.DateType),
        th.Property("settlement_date", th.DateType),
        th.Property("days_to_maturity", th.IntegerType),
        th.Property("trade_tick_size", th.NumberType),
        th.Property("spread_tick_size", th.NumberType),
        th.Property("settlement_tick_size", th.NumberType),
        th.Property("min_order_quantity", th.IntegerType),
        th.Property("max_order_quantity", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/futures/vX/contracts"


class FuturesProductsStream(MassiveRestStream):
    """Futures products reference data.

    Endpoint: ``GET /futures/vX/products``
    Max limit: 50000
    """

    name = "futures_products"
    primary_keys = ["product_code", "date"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True

    schema = th.PropertiesList(
        th.Property("product_code", th.StringType),
        th.Property("name", th.StringType),
        th.Property("date", th.DateType),
        th.Property("type", th.StringType),
        th.Property("asset_class", th.StringType),
        th.Property("asset_sub_class", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("sub_sector", th.StringType),
        th.Property("trading_venue", th.StringType),
        th.Property("settlement_currency_code", th.StringType),
        th.Property("trade_currency_code", th.StringType),
        th.Property("settlement_method", th.StringType),
        th.Property("settlement_type", th.StringType),
        th.Property("price_quotation", th.StringType),
        th.Property("unit_of_measure", th.StringType),
        th.Property("unit_of_measure_qty", th.NumberType),
        th.Property("last_updated", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/futures/vX/products"


class FuturesSchedulesStream(MassiveRestStream):
    """Futures trading schedules.

    Endpoint: ``GET /futures/vX/schedules``
    Max limit: 1000
    """

    name = "futures_schedules"
    primary_keys = ["product_code", "session_end_date", "event"]

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("product_code", th.StringType),
        th.Property("product_name", th.StringType),
        th.Property("trading_venue", th.StringType),
        th.Property("session_end_date", th.DateType),
        th.Property("event", th.StringType),
        th.Property("timestamp", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/futures/vX/schedules"


class FuturesExchangesStream(MassiveRestStream):
    """Futures exchanges reference data.

    Endpoint: ``GET /futures/vX/exchanges``
    Max limit: 999
    """

    name = "futures_exchanges"
    primary_keys = ["id"]

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("acronym", th.StringType),
        th.Property("name", th.StringType),
        th.Property("mic", th.StringType),
        th.Property("operating_mic", th.StringType),
        th.Property("locale", th.StringType),
        th.Property("type", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/futures/vX/exchanges"


class FuturesMarketStatusStream(MassiveRestStream):
    """Futures market status.

    Endpoint: ``GET /futures/vX/market-status``
    Max limit: 50000
    """

    name = "futures_market_status"
    primary_keys = ["product_code", "session_end_date", "market_event"]

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("product_code", th.StringType),
        th.Property("name", th.StringType),
        th.Property("market_event", th.StringType),
        th.Property("session_end_date", th.DateType),
        th.Property("timestamp", th.StringType),
        th.Property("trading_venue", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/futures/vX/market-status"


# ---------------------------------------------------------------------------
# Partition base (shared by bars / trades / quotes)
# ---------------------------------------------------------------------------


class FuturesTickerPartitionStream(BaseTickerPartitionStream):
    """Partition stream for futures with optional product_code / type filtering."""

    ticker_selector_keys = ("futures_tickers", "futures_contracts")
    _cached_tickers_getter = "get_cached_futures_tickers"

    def _get_futures_filter_list(self, key: str) -> list[str] | None:
        """Read a list filter from futures_tickers/futures_contracts other_params."""
        for section_key in self.ticker_selector_keys:
            section = self.config.get(section_key, {})
            if not isinstance(section, dict):
                continue
            value = section.get("other_params", {}).get(key)
            if value is None:
                continue
            if isinstance(value, str):
                return [v.strip() for v in value.split(",") if v.strip()]
            if isinstance(value, list):
                return [str(v).strip() for v in value if str(v).strip()]
        return None

    @property
    def partitions(self):
        getter = getattr(self._tap, self._cached_tickers_getter)
        tickers = self.filter_tickers_from_config(getter())

        select_product_codes = self._get_futures_filter_list("select_product_codes")
        if select_product_codes:
            codes_set = set(select_product_codes)
            tickers = [t for t in tickers if t.get("product_code") in codes_set]

        select_types = self._get_futures_filter_list("select_types")
        if select_types:
            types_set = set(select_types)
            tickers = [t for t in tickers if t.get("type") in types_set]

        logging.info(
            "%s: %d futures contracts after filtering",
            type(self).__name__,
            len(tickers),
        )

        partitions: list[dict[str, t.Any]] = []
        for rec in tickers:
            partition: dict[str, t.Any] = {"ticker": rec["ticker"]}
            if isinstance(rec.get("active"), bool):
                partition["active"] = rec["active"]
            partitions.append(partition)
        return partitions


# ---------------------------------------------------------------------------
# Aggregate bars  —  /futures/vX/aggs/{ticker}
# ---------------------------------------------------------------------------


class FuturesCustomBarsStream(FuturesTickerPartitionStream, BaseCustomBarsStream):
    """Futures aggregate bars.

    Endpoint: ``GET /futures/vX/aggs/{ticker}``
    Max limit: 50000

    Uses nanosecond ``window_start`` timestamps.  Additional fields vs the
    generic ``/v2/aggs`` endpoint: ``settlement_price``, ``session_end_date``,
    ``dollar_volume``.
    """

    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.NONE

    _api_expects_unix_timestamp = True
    _unix_timestamp_unit = "ns"
    _requires_end_timestamp_in_path_params = False
    _requires_end_timestamp_in_query_params = True
    _ticker_in_path_params = True

    _DECIMAL_KEYS = (
        "open",
        "high",
        "low",
        "close",
        "settlement_price",
        "dollar_volume",
    )

    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType),
        th.Property("ticker", th.StringType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("transactions", th.IntegerType),
        th.Property("settlement_price", th.NumberType),
        th.Property("session_end_date", th.DateType),
        th.Property("dollar_volume", th.NumberType),
        th.Property("active", th.BooleanType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        # BaseCustomBarsStream.__init__ hardcodes _cfg_ending_timestamp_key = "to".
        # Re-detect from config so window_start.gte / window_start.lte are used.
        self._cfg_starting_timestamp_key = None
        self._cfg_ending_timestamp_key = None
        self._cfg_starting_timestamp_value = None
        self._cfg_ending_timestamp_value = None
        self._set_timestamp_config_keys()

    def get_url(self, context: Context) -> str:
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/futures/vX/aggs/{ticker}"

    def post_process(self, row, context=None):
        if "window_start" not in row:
            return None
        for key in self._DECIMAL_KEYS:
            if key in row:
                row[key] = safe_decimal(row[key])
        row["timestamp"] = self.safe_parse_datetime(row.pop("window_start")).isoformat()
        row["ticker"] = context.get(self._ticker_param)
        row[self.status_flag_field] = self._resolve_status_flag(context)
        return row


class FuturesBars1SecondStream(FuturesCustomBarsStream):
    name = "futures_bars_1_second"


class FuturesBars1MinuteStream(FuturesCustomBarsStream):
    name = "futures_bars_1_minute"


class FuturesBars5MinuteStream(FuturesCustomBarsStream):
    name = "futures_bars_5_minute"


class FuturesBars1HourStream(FuturesCustomBarsStream):
    name = "futures_bars_1_hour"


class FuturesBars1DayStream(FuturesCustomBarsStream):
    name = "futures_bars_1_day"


class FuturesBars1WeekStream(FuturesCustomBarsStream):
    name = "futures_bars_1_week"


class FuturesBars1MonthStream(FuturesCustomBarsStream):
    name = "futures_bars_1_month"


# ---------------------------------------------------------------------------
# Trades  —  /futures/vX/trades/{ticker}
# ---------------------------------------------------------------------------


class FuturesTradeStream(_NanosecondIncrementalMixin, FuturesTickerPartitionStream):
    """Futures tick-level trades.

    Endpoint: ``GET /futures/vX/trades/{ticker}``
    Max limit: 50000
    """

    name = "futures_trades"
    primary_keys = ["ticker", "timestamp", "sequence_number"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    is_sorted = False

    _use_cached_tickers_default = True
    _api_expects_unix_timestamp = True
    _unix_timestamp_unit = "ns"
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("timestamp", th.IntegerType),
        th.Property("price", th.NumberType),
        th.Property("size", th.NumberType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("report_sequence", th.IntegerType),
        th.Property("session_end_date", th.DateType),
    ).to_dict()

    def get_url(self, context: Context) -> str:
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/futures/vX/trades/{ticker}"

    def post_process(self, row, context=None):
        row["ticker"] = context.get(self._ticker_param)
        row["timestamp"] = safe_int(row.get("timestamp"))
        row["price"] = safe_decimal(row.get("price"))
        return row


# ---------------------------------------------------------------------------
# Quotes  —  /futures/vX/quotes/{ticker}
# ---------------------------------------------------------------------------


class FuturesQuoteStream(_NanosecondIncrementalMixin, FuturesTickerPartitionStream):
    """Futures tick-level quotes.

    Endpoint: ``GET /futures/vX/quotes/{ticker}``
    Max limit: 50000
    """

    name = "futures_quotes"
    primary_keys = ["ticker", "timestamp", "sequence_number"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    is_sorted = False

    _use_cached_tickers_default = True
    _api_expects_unix_timestamp = True
    _unix_timestamp_unit = "ns"
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("timestamp", th.IntegerType),
        th.Property("ask_price", th.NumberType),
        th.Property("ask_size", th.NumberType),
        th.Property("ask_timestamp", th.IntegerType),
        th.Property("bid_price", th.NumberType),
        th.Property("bid_size", th.NumberType),
        th.Property("bid_timestamp", th.IntegerType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("report_sequence", th.IntegerType),
        th.Property("session_end_date", th.DateType),
    ).to_dict()

    def get_url(self, context: Context) -> str:
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/futures/vX/quotes/{ticker}"

    def post_process(self, row, context=None):
        row["ticker"] = context.get(self._ticker_param)
        row["timestamp"] = safe_int(row.get("timestamp"))
        return row


# ---------------------------------------------------------------------------
# Snapshot  —  /futures/vX/snapshot
# ---------------------------------------------------------------------------


class FuturesContractsSnapshotStream(MassiveRestStream):
    """Futures contracts snapshot.

    Endpoint: ``GET /futures/vX/snapshot``
    Max limit: 50000
    """

    name = "futures_contracts_snapshot"
    primary_keys = ["ticker"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("product_code", th.StringType),
        th.Property(
            "details",
            th.ObjectType(
                th.Property("ticker", th.StringType),
                th.Property("product_code", th.StringType),
                th.Property("open_interest", th.NumberType),
                th.Property("settlement_date", th.StringType),
            ),
        ),
        th.Property(
            "last_minute",
            th.ObjectType(
                th.Property("open", th.NumberType),
                th.Property("high", th.NumberType),
                th.Property("low", th.NumberType),
                th.Property("close", th.NumberType),
                th.Property("volume", th.NumberType),
                th.Property("last_updated", th.NumberType),
                th.Property("timeframe", th.StringType),
            ),
        ),
        th.Property(
            "last_quote",
            th.ObjectType(
                th.Property("ask", th.NumberType),
                th.Property("ask_size", th.NumberType),
                th.Property("ask_timestamp", th.NumberType),
                th.Property("bid", th.NumberType),
                th.Property("bid_size", th.NumberType),
                th.Property("bid_timestamp", th.NumberType),
                th.Property("last_updated", th.NumberType),
                th.Property("timeframe", th.StringType),
            ),
        ),
        th.Property(
            "last_trade",
            th.ObjectType(
                th.Property("price", th.NumberType),
                th.Property("size", th.NumberType),
                th.Property("last_updated", th.NumberType),
                th.Property("timeframe", th.StringType),
            ),
        ),
        th.Property(
            "session",
            th.ObjectType(
                th.Property("open", th.NumberType),
                th.Property("high", th.NumberType),
                th.Property("low", th.NumberType),
                th.Property("close", th.NumberType),
                th.Property("volume", th.NumberType),
                th.Property("change", th.NumberType),
                th.Property("change_percent", th.NumberType),
                th.Property("previous_settlement", th.NumberType),
                th.Property("settlement_price", th.NumberType),
            ),
        ),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/futures/vX/snapshot"

    def post_process(self, row, context=None):
        row = super().post_process(row, context)
        if row is None:
            return None
        details = row.get("details") or {}
        row.setdefault("ticker", details.get("ticker"))
        row.setdefault("product_code", details.get("product_code"))
        return row
