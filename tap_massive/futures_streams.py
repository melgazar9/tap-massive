"""Futures stream classes for tap-massive."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.base_streams import (
    BaseCustomBarsStream,
    BaseQuoteStream,
    BaseTickerPartitionStream,
    BaseTradeStream,
)
from tap_massive.client import MassiveRestStream


class FuturesContractsStream(MassiveRestStream):
    """Stream for retrieving all futures contracts.

    Provides complete contract specifications including settlement dates,
    tick sizes, and trading parameters.
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
        th.Property("type", th.StringType),  # 'single' or 'combo'
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

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/futures/vX/contracts"


class FuturesTickerPartitionStream(BaseTickerPartitionStream):
    @property
    def partitions(self):
        return [{"ticker": t["ticker"]} for t in self._tap.get_cached_futures_tickers()]


class FuturesProductsStream(MassiveRestStream):
    """Stream for retrieving futures products reference data.

    Provides product specifications including asset class, sector,
    settlement methods, and trading venues.
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
        th.Property("type", th.StringType),  # 'single' or 'combo'
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

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/futures/vX/products"


class FuturesSchedulesStream(MassiveRestStream):
    """Stream for retrieving futures trading schedules.

    Provides trading session schedules and events for futures products.
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

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/futures/vX/schedules"


class FuturesCustomBarsStream(FuturesTickerPartitionStream, BaseCustomBarsStream):
    """Base class for futures bars streams."""

    pass


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


class FuturesTradeStream(FuturesTickerPartitionStream, BaseTradeStream):
    """Stream for retrieving futures trade data."""

    name = "futures_trades"


class FuturesQuoteStream(FuturesTickerPartitionStream, BaseQuoteStream):
    """Stream for retrieving futures quote data."""

    name = "futures_quotes"


class FuturesContractsSnapshotStream(MassiveRestStream):
    """Stream for retrieving futures contracts snapshot data.

    Returns real-time snapshot data for futures contracts including
    current prices, volumes, and session data.
    """

    name = "futures_contracts_snapshot"
    primary_keys = ["ticker"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("market_status", th.StringType),
        th.Property("fmv", th.NumberType),
        th.Property(
            "session",
            th.ObjectType(
                th.Property("open", th.NumberType),
                th.Property("high", th.NumberType),
                th.Property("low", th.NumberType),
                th.Property("close", th.NumberType),
                th.Property("volume", th.NumberType),
                th.Property("vwap", th.NumberType),
                th.Property("change", th.NumberType),
                th.Property("change_percent", th.NumberType),
                th.Property("previous_close", th.NumberType),
            ),
        ),
        th.Property(
            "last_quote",
            th.ObjectType(
                th.Property("ask", th.NumberType),
                th.Property("ask_size", th.IntegerType),
                th.Property("bid", th.NumberType),
                th.Property("bid_size", th.IntegerType),
                th.Property("last_updated", th.IntegerType),
            ),
        ),
        th.Property(
            "last_trade",
            th.ObjectType(
                th.Property("price", th.NumberType),
                th.Property("size", th.IntegerType),
                th.Property("sip_timestamp", th.IntegerType),
                th.Property("conditions", th.ArrayType(th.IntegerType)),
            ),
        ),
        th.Property("error", th.StringType),
        th.Property("message", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/snapshot/futures"
