"""Crypto stream classes for tap-massive."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.base_streams import (
    BaseCustomBarsStream,
    BaseDailyMarketSummaryStream,
    BaseDailyTickerSummaryStream,
    BaseIndicatorStream,
    BaseLastTradeStream,
    BasePreviousDayBarSummaryStream,
    BaseTickerDetailsStream,
    BaseTickerPartitionStream,
    BaseTickerStream,
    BaseTopMarketMoversStream,
    BaseTradeStream,
    _SnapshotNormalizationMixin,
    _TodaysChangePercentMixin,
)
from tap_massive.client import MassiveRestStream


class CryptoTickerStream(BaseTickerStream):
    """Stream for retrieving all crypto tickers."""

    name = "crypto_tickers"
    market = "crypto"
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


class CryptoTickerPartitionStream(BaseTickerPartitionStream):
    ticker_selector_keys = ("crypto_tickers",)
    _cached_tickers_getter = "get_cached_crypto_tickers"


class CryptoTickerDetailsStream(CryptoTickerPartitionStream, BaseTickerDetailsStream):
    """Stream for retrieving detailed crypto ticker information."""

    name = "crypto_ticker_details"


class CryptoCustomBarsStream(CryptoTickerPartitionStream, BaseCustomBarsStream):
    """Base class for crypto bars streams."""


class CryptoBars1SecondStream(CryptoCustomBarsStream):
    name = "crypto_bars_1_second"


class CryptoBars1MinuteStream(CryptoCustomBarsStream):
    name = "crypto_bars_1_minute"


class CryptoBars5MinuteStream(CryptoCustomBarsStream):
    name = "crypto_bars_5_minute"


class CryptoBars1HourStream(CryptoCustomBarsStream):
    name = "crypto_bars_1_hour"


class CryptoBars1DayStream(CryptoCustomBarsStream):
    name = "crypto_bars_1_day"


class CryptoBars1WeekStream(CryptoCustomBarsStream):
    name = "crypto_bars_1_week"


class CryptoBars1MonthStream(CryptoCustomBarsStream):
    name = "crypto_bars_1_month"


class CryptoDailyMarketSummaryStream(BaseDailyMarketSummaryStream):
    """Stream for retrieving crypto daily market summary."""

    name = "crypto_daily_market_summary"

    def get_url(self, context: Context):
        date = context.get("path_params").get("date")
        return f"{self.url_base}/v2/aggs/grouped/locale/global/market/crypto/{date}"


class CryptoDailyTickerSummaryStream(
    CryptoTickerPartitionStream, BaseDailyTickerSummaryStream
):
    """Stream for retrieving crypto daily ticker summary."""

    name = "crypto_daily_ticker_summary"


class CryptoPreviousDayBarStream(
    CryptoTickerPartitionStream, BasePreviousDayBarSummaryStream
):
    """Stream for retrieving crypto previous day bar data."""

    name = "crypto_previous_day_bar"


class CryptoTradeStream(CryptoTickerPartitionStream, BaseTradeStream):
    """Stream for retrieving crypto trade data."""

    name = "crypto_trades"


class CryptoLastTradeStream(CryptoTickerPartitionStream, BaseLastTradeStream):
    """Stream for retrieving crypto last trade data."""

    name = "crypto_last_trade"


class CryptoTopMarketMoversStream(BaseTopMarketMoversStream):
    """Stream for retrieving crypto top market movers."""

    name = "crypto_top_market_movers"

    def get_url(self, context: Context | None = None):
        direction = self._get_direction(context)
        return f"{self.url_base}/v2/snapshot/locale/global/markets/crypto/{direction}"


class CryptoTickerSnapshotStream(
    _SnapshotNormalizationMixin, _TodaysChangePercentMixin, CryptoTickerPartitionStream
):
    """Stream for retrieving crypto single ticker snapshot data."""

    name = "crypto_ticker_snapshot"
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
            "last_trade",
            th.ObjectType(
                th.Property("price", th.NumberType),
                th.Property("size", th.NumberType),
                th.Property("timestamp", th.IntegerType),
                th.Property("exchange", th.IntegerType),
                th.Property("conditions", th.ArrayType(th.IntegerType)),
                th.Property("id", th.StringType),
                th.Property("correction", th.IntegerType),
                th.Property("trf_timestamp", th.IntegerType),
                th.Property("sequence_number", th.IntegerType),
                th.Property("trf_id", th.IntegerType),
                th.Property("participant_timestamp", th.IntegerType),
                th.Property("tape", th.IntegerType),
                th.Property("ticker", th.StringType),
            ),
        ),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return (
            f"{self.url_base}/v2/snapshot/locale/global/markets/crypto/tickers/{ticker}"
        )


class CryptoFullMarketSnapshotStream(
    _SnapshotNormalizationMixin, _TodaysChangePercentMixin, MassiveRestStream
):
    """Stream for retrieving crypto full market snapshot data."""

    name = "crypto_full_market_snapshot"
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
        th.Property("last_trade", th.ObjectType()),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v2/snapshot/locale/global/markets/crypto/tickers"


class CryptoSmaStream(CryptoTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving crypto SMA indicator data."""

    name = "crypto_sma"
    indicator_type = "sma"


class CryptoEmaStream(CryptoTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving crypto EMA indicator data."""

    name = "crypto_ema"
    indicator_type = "ema"


class CryptoMACDStream(CryptoTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving crypto MACD indicator data."""

    name = "crypto_macd"
    indicator_type = "macd"
    schema = BaseIndicatorStream._build_schema(True)


class CryptoRSIStream(CryptoTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving crypto RSI indicator data."""

    name = "crypto_rsi"
    indicator_type = "rsi"
