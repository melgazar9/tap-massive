"""Indices stream classes for tap-massive."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.base_streams import (
    BaseCustomBarsStream,
    BaseDailyTickerSummaryStream,
    BaseIndicatorStream,
    BasePreviousDayBarSummaryStream,
    BaseTickerDetailsStream,
    BaseTickerPartitionStream,
    BaseTickerStream,
    BaseTickerTypesStream,
    _SnapshotNormalizationMixin,
)
from tap_massive.client import MassiveRestStream


class IndicesTickerStream(BaseTickerStream):
    """Stream for retrieving all indices tickers."""

    name = "indices_tickers"
    market = "indices"
    primary_keys = ["ticker"]
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("name", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("market", th.StringType),
        th.Property("locale", th.StringType),
        th.Property("type", th.StringType),
        th.Property("primary_exchange", th.StringType),
        th.Property("currency_name", th.StringType),
        th.Property("last_updated_utc", th.StringType),
        th.Property("delisted_utc", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/reference/tickers"


class IndicesTickerPartitionStream(BaseTickerPartitionStream):
    @property
    def partitions(self):
        return [{"ticker": t["ticker"]} for t in self._tap.get_cached_indices_tickers()]


class IndicesTickerDetailsStream(IndicesTickerPartitionStream, BaseTickerDetailsStream):
    """Stream for retrieving detailed indices ticker information."""

    name = "indices_ticker_details"


class IndicesCustomBarsStream(IndicesTickerPartitionStream, BaseCustomBarsStream):
    """Base class for indices bars streams."""


class IndicesBars1SecondStream(IndicesCustomBarsStream):
    name = "indices_bars_1_second"


class IndicesBars1MinuteStream(IndicesCustomBarsStream):
    name = "indices_bars_1_minute"


class IndicesBars5MinuteStream(IndicesCustomBarsStream):
    name = "indices_bars_5_minute"


class IndicesBars1HourStream(IndicesCustomBarsStream):
    name = "indices_bars_1_hour"


class IndicesBars1DayStream(IndicesCustomBarsStream):
    name = "indices_bars_1_day"


class IndicesBars1WeekStream(IndicesCustomBarsStream):
    name = "indices_bars_1_week"


class IndicesBars1MonthStream(IndicesCustomBarsStream):
    name = "indices_bars_1_month"


class IndicesDailyTickerSummaryStream(
    IndicesTickerPartitionStream, BaseDailyTickerSummaryStream
):
    """Stream for retrieving indices daily ticker summary."""

    name = "indices_daily_ticker_summary"


class IndicesPreviousDayBarStream(
    IndicesTickerPartitionStream, BasePreviousDayBarSummaryStream
):
    """Stream for retrieving indices previous day bar data."""

    name = "indices_previous_day_bar"


class IndicesTickerTypesStream(BaseTickerTypesStream):
    """Indices ticker types."""

    name = "indices_ticker_types"
    _asset_class = "indices"


class IndicesSnapshotStream(_SnapshotNormalizationMixin, MassiveRestStream):
    """Stream for retrieving indices snapshot data.

    Returns snapshot data for multiple indices at once.
    """

    name = "indices_snapshot"
    primary_keys = ["ticker"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("market_status", th.StringType),
        th.Property("value", th.NumberType),
        th.Property("last_updated", th.IntegerType),
        th.Property("timeframe", th.StringType),
        th.Property(
            "session",
            th.ObjectType(
                th.Property("open", th.NumberType),
                th.Property("high", th.NumberType),
                th.Property("low", th.NumberType),
                th.Property("close", th.NumberType),
                th.Property("change", th.NumberType),
                th.Property("change_percent", th.NumberType),
                th.Property("previous_close", th.NumberType),
            ),
        ),
        th.Property("error", th.StringType),
        th.Property("message", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/snapshot/indices"


class IndicesSmaStream(IndicesTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving indices SMA indicator data."""

    name = "indices_sma"
    indicator_type = "sma"


class IndicesEmaStream(IndicesTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving indices EMA indicator data."""

    name = "indices_ema"
    indicator_type = "ema"


class IndicesMACDStream(IndicesTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving indices MACD indicator data."""

    name = "indices_macd"
    indicator_type = "macd"
    schema = BaseIndicatorStream._build_schema(True)


class IndicesRSIStream(IndicesTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving indices RSI indicator data."""

    name = "indices_rsi"
    indicator_type = "rsi"
