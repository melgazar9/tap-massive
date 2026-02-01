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
    @property
    def partitions(self):
        return [{"ticker": t["ticker"]} for t in self._tap.get_cached_crypto_tickers()]


class CryptoTickerDetailsStream(CryptoTickerPartitionStream, BaseTickerDetailsStream):
    """Stream for retrieving detailed crypto ticker information."""

    name = "crypto_ticker_details"


class CryptoCustomBarsStream(CryptoTickerPartitionStream, BaseCustomBarsStream):
    """Base class for crypto bars streams."""

    pass


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

    def get_url(self, context: Context):
        direction = context.get("direction")
        return f"{self.url_base}/v2/snapshot/locale/global/markets/crypto/{direction}"


class CryptoTickerSnapshotStream(CryptoTickerPartitionStream):
    """Stream for retrieving crypto single ticker snapshot data."""

    name = "crypto_ticker_snapshot"
    primary_keys = ["ticker"]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("todays_change", th.NumberType),
        th.Property("todays_change_perc", th.NumberType),
        th.Property("updated", th.IntegerType),
        th.Property("fmv", th.NumberType),
        th.Property(
            "day",
            th.ObjectType(
                th.Property("o", th.NumberType),
                th.Property("h", th.NumberType),
                th.Property("l", th.NumberType),
                th.Property("c", th.NumberType),
                th.Property("v", th.NumberType),
                th.Property("vw", th.NumberType),
            ),
        ),
        th.Property(
            "min",
            th.ObjectType(
                th.Property("o", th.NumberType),
                th.Property("h", th.NumberType),
                th.Property("l", th.NumberType),
                th.Property("c", th.NumberType),
                th.Property("v", th.NumberType),
                th.Property("vw", th.NumberType),
                th.Property("av", th.NumberType),
                th.Property("t", th.IntegerType),
                th.Property("n", th.IntegerType),
            ),
        ),
        th.Property(
            "prev_day",
            th.ObjectType(
                th.Property("o", th.NumberType),
                th.Property("h", th.NumberType),
                th.Property("l", th.NumberType),
                th.Property("c", th.NumberType),
                th.Property("v", th.NumberType),
                th.Property("vw", th.NumberType),
            ),
        ),
        th.Property(
            "last_trade",
            th.ObjectType(
                th.Property("p", th.NumberType),
                th.Property("s", th.NumberType),
                th.Property("t", th.IntegerType),
                th.Property("x", th.IntegerType),
                th.Property("c", th.ArrayType(th.IntegerType)),
                th.Property("i", th.StringType),
            ),
        ),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return (
            f"{self.url_base}/v2/snapshot/locale/global/markets/crypto/tickers/{ticker}"
        )


class CryptoFullMarketSnapshotStream(MassiveRestStream):
    """Stream for retrieving crypto full market snapshot data."""

    name = "crypto_full_market_snapshot"
    primary_keys = ["ticker"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("todays_change", th.NumberType),
        th.Property("todays_change_perc", th.NumberType),
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


class CryptoRSIStream(CryptoTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving crypto RSI indicator data."""

    name = "crypto_rsi"
    indicator_type = "rsi"
