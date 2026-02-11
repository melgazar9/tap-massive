"""Forex stream classes for tap-massive."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.base_streams import (
    BaseCustomBarsStream,
    BaseDailyMarketSummaryStream,
    BaseIndicatorStream,
    BaseLastQuoteStream,
    BasePreviousDayBarSummaryStream,
    BaseQuoteStream,
    BaseTickerDetailsStream,
    BaseTickerPartitionStream,
    BaseTickerStream,
    BaseTickerTypesStream,
    BaseTopMarketMoversStream,
    _SnapshotNormalizationMixin,
    _TodaysChangePercentMixin,
)
from tap_massive.client import MassiveRestStream


class ForexTickerStream(BaseTickerStream):
    """Stream for retrieving all forex tickers."""

    name = "forex_tickers"
    market = "fx"
    primary_keys = ["ticker"]
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("name", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("market", th.StringType),
        th.Property("locale", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("currency_name", th.StringType),
        th.Property("base_currency_symbol", th.StringType),
        th.Property("base_currency_name", th.StringType),
        th.Property("last_updated_utc", th.StringType),
        th.Property("delisted_utc", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/reference/tickers"


class ForexTickerPartitionStream(BaseTickerPartitionStream):
    @property
    def partitions(self):
        return [{"ticker": t["ticker"]} for t in self._tap.get_cached_forex_tickers()]


class ForexTickerDetailsStream(ForexTickerPartitionStream, BaseTickerDetailsStream):
    """Stream for retrieving detailed forex ticker information."""

    name = "forex_ticker_details"


class ForexCustomBarsStream(ForexTickerPartitionStream, BaseCustomBarsStream):
    """Base class for forex bars streams."""


class ForexBars1SecondStream(ForexCustomBarsStream):
    name = "forex_bars_1_second"


class ForexBars1MinuteStream(ForexCustomBarsStream):
    name = "forex_bars_1_minute"


class ForexBars5MinuteStream(ForexCustomBarsStream):
    name = "forex_bars_5_minute"


class ForexBars1HourStream(ForexCustomBarsStream):
    name = "forex_bars_1_hour"


class ForexBars1DayStream(ForexCustomBarsStream):
    name = "forex_bars_1_day"


class ForexBars1WeekStream(ForexCustomBarsStream):
    name = "forex_bars_1_week"


class ForexBars1MonthStream(ForexCustomBarsStream):
    name = "forex_bars_1_month"


class ForexDailyMarketSummaryStream(BaseDailyMarketSummaryStream):
    """Stream for retrieving forex daily market summary."""

    name = "forex_daily_market_summary"

    def get_url(self, context: Context):
        date = context.get("path_params").get("date")
        return f"{self.url_base}/v2/aggs/grouped/locale/global/market/fx/{date}"


class ForexPreviousDayBarStream(
    ForexTickerPartitionStream, BasePreviousDayBarSummaryStream
):
    """Stream for retrieving forex previous day bar data."""

    name = "forex_previous_day_bar"


class ForexQuoteStream(ForexTickerPartitionStream, BaseQuoteStream):
    """Stream for retrieving forex quote data."""

    name = "forex_quotes"


class ForexLastQuoteStream(ForexTickerPartitionStream, BaseLastQuoteStream):
    """Stream for retrieving forex last quote data."""

    name = "forex_last_quote"


class ForexTickerTypesStream(BaseTickerTypesStream):
    """Forex ticker types."""

    name = "forex_ticker_types"
    _asset_class = "fx"


class ForexTopMarketMoversStream(BaseTopMarketMoversStream):
    """Stream for retrieving forex top market movers."""

    name = "forex_top_market_movers"

    def get_url(self, context: Context):
        direction = context.get("direction")
        return f"{self.url_base}/v2/snapshot/locale/global/markets/forex/{direction}"


class ForexTickerSnapshotStream(
    _SnapshotNormalizationMixin, _TodaysChangePercentMixin, ForexTickerPartitionStream
):
    """Stream for retrieving forex single ticker snapshot data."""

    name = "forex_ticker_snapshot"
    primary_keys = ["ticker"]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("todays_change", th.NumberType),
        th.Property("todays_change_percent", th.NumberType),
        th.Property("updated", th.IntegerType),
        th.Property("fmv", th.NumberType),
        th.Property(
            "day",
            th.ObjectType(
                th.Property("open", th.NumberType),
                th.Property("high", th.NumberType),
                th.Property("low", th.NumberType),
                th.Property("close", th.NumberType),
                th.Property("volume", th.NumberType),
                th.Property("vwap", th.NumberType),
            ),
        ),
        th.Property(
            "min",
            th.ObjectType(
                th.Property("open", th.NumberType),
                th.Property("high", th.NumberType),
                th.Property("low", th.NumberType),
                th.Property("close", th.NumberType),
                th.Property("volume", th.NumberType),
                th.Property("vwap", th.NumberType),
                th.Property("accumulated_volume", th.NumberType),
                th.Property("timestamp", th.IntegerType),
                th.Property("transactions", th.IntegerType),
            ),
        ),
        th.Property(
            "prev_day",
            th.ObjectType(
                th.Property("open", th.NumberType),
                th.Property("high", th.NumberType),
                th.Property("low", th.NumberType),
                th.Property("close", th.NumberType),
                th.Property("volume", th.NumberType),
                th.Property("vwap", th.NumberType),
            ),
        ),
        th.Property(
            "last_quote",
            th.ObjectType(
                th.Property("ask_price", th.NumberType),
                th.Property("bid_price", th.NumberType),
                th.Property("timestamp", th.IntegerType),
                th.Property("exchange", th.IntegerType),
                th.Property("ask_exchange", th.IntegerType),
                th.Property("bid_exchange", th.IntegerType),
                th.Property("indicators", th.ArrayType(th.IntegerType)),
                th.Property("conditions", th.ArrayType(th.IntegerType)),
                th.Property("sequence_number", th.IntegerType),
                th.Property("participant_timestamp", th.IntegerType),
                th.Property("trf_timestamp", th.IntegerType),
                th.Property("tape", th.IntegerType),
                th.Property("ticker", th.StringType),
            ),
        ),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return (
            f"{self.url_base}/v2/snapshot/locale/global/markets/forex/tickers/{ticker}"
        )

class ForexFullMarketSnapshotStream(
    _SnapshotNormalizationMixin, _TodaysChangePercentMixin, MassiveRestStream
):
    """Stream for retrieving forex full market snapshot data."""

    name = "forex_full_market_snapshot"
    primary_keys = ["ticker"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("todays_change", th.NumberType),
        th.Property("todays_change_percent", th.NumberType),
        th.Property("updated", th.IntegerType),
        th.Property("day", th.ObjectType()),
        th.Property("min", th.ObjectType()),
        th.Property("prev_day", th.ObjectType()),
        th.Property("last_quote", th.ObjectType()),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v2/snapshot/locale/global/markets/forex/tickers"

class ForexSmaStream(ForexTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving forex SMA indicator data."""

    name = "forex_sma"
    indicator_type = "sma"


class ForexEmaStream(ForexTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving forex EMA indicator data."""

    name = "forex_ema"
    indicator_type = "ema"


class ForexMACDStream(ForexTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving forex MACD indicator data."""

    name = "forex_macd"
    indicator_type = "macd"
    schema = BaseIndicatorStream._build_schema(True)


class ForexRSIStream(ForexTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving forex RSI indicator data."""

    name = "forex_rsi"
    indicator_type = "rsi"


class ForexCurrencyConversionStream(MassiveRestStream):
    """Stream for retrieving forex currency conversion rates.

    Converts one currency to another based on the latest market data.
    """

    name = "forex_currency_conversion"
    primary_keys = ["from", "to"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("from", th.StringType),
        th.Property("to", th.StringType),
        th.Property("converted", th.NumberType),
        th.Property("initialAmount", th.NumberType),
        th.Property(
            "last",
            th.ObjectType(
                th.Property("ask", th.NumberType),
                th.Property("bid", th.NumberType),
                th.Property("exchange", th.IntegerType),
                th.Property("timestamp", th.IntegerType),
            ),
        ),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        from_currency = self.path_params.get("from", "USD")
        to_currency = self.path_params.get("to", "EUR")
        return f"{self.url_base}/v1/conversion/{from_currency}/{to_currency}"
