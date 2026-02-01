"""Options stream classes for tap-massive."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.base_streams import (
    BaseTickerStream,
    BaseCustomBarsStream,
    BaseDailyTickerSummaryStream,
    BaseIndicatorStream,
    BaseLastTradeStream,
    BasePreviousDayBarSummaryStream,
    BaseQuoteStream,
    BaseTickerPartitionedStream,
    BaseTradeStream,
)
from tap_massive.client import MassiveRestStream, OptionalTickerPartitionStream


class OptionsContractsStream(BaseTickerStream):
    """Stream for retrieving all options contracts."""

    name = "options_contracts"
    primary_keys = ["ticker"]
    market = "option"
    _ticker_param = "underlying_ticker"
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("contract_type", th.StringType),
        th.Property("expiration_date", th.DateType),
        th.Property("strike_price", th.NumberType),
        th.Property("underlying_ticker", th.StringType),
        th.Property("exercise_style", th.StringType),
        th.Property("shares_per_contract", th.IntegerType),
        th.Property("cfi", th.StringType),
        th.Property("primary_exchange", th.StringType),
        th.Property("additional_underlyings", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/reference/options/contracts"


class OptionsTickerPartitionedStream(BaseTickerPartitionedStream):
    @property
    def partitions(self):
        return [{"ticker": t["ticker"]} for t in self._tap.get_cached_option_tickers()]


class OptionsCustomBarsStream(OptionsTickerPartitionedStream, BaseCustomBarsStream):
    pass


class OptionsContractOverviewStream(OptionsTickerPartitionedStream):
    """Stream for retrieving detailed information about options contracts."""

    name = "options_contract_overview"
    primary_keys = ["ticker"]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("contract_type", th.StringType),
        th.Property("exercise_style", th.StringType),
        th.Property("expiration_date", th.DateType),
        th.Property("strike_price", th.NumberType),
        th.Property("shares_per_contract", th.NumberType),
        th.Property("underlying_ticker", th.StringType),
        th.Property("primary_exchange", th.StringType),
        th.Property("cfi", th.StringType),
        th.Property("correction", th.IntegerType),
        th.Property("additional_underlyings", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def get_url(self, context: Context) -> str:
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v3/reference/options/contracts/{ticker}"


class OptionsContractSnapshotStream(OptionsTickerPartitionedStream):
    """Stream for retrieving options contract snapshot data."""

    name = "options_contract_snapshot"
    primary_keys = ["ticker"]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("break_even_price", th.NumberType),
        th.Property("fmv", th.NumberType),
        th.Property("fmv_last_updated", th.IntegerType),
        th.Property("implied_volatility", th.NumberType),
        th.Property("open_interest", th.NumberType),
        th.Property("greeks", th.ObjectType(
            th.Property("delta", th.NumberType),
            th.Property("gamma", th.NumberType),
            th.Property("theta", th.NumberType),
            th.Property("vega", th.NumberType),
        )),
        th.Property("last_quote", th.ObjectType(
            th.Property("ask", th.NumberType),
            th.Property("ask_size", th.IntegerType),
            th.Property("bid", th.NumberType),
            th.Property("bid_size", th.IntegerType),
            th.Property("ask_exchange", th.IntegerType),
            th.Property("bid_exchange", th.IntegerType),
            th.Property("last_updated", th.IntegerType),
            th.Property("midpoint", th.NumberType),
            th.Property("timeframe", th.StringType),
        )),
        th.Property("last_trade", th.ObjectType(
            th.Property("price", th.NumberType),
            th.Property("size", th.IntegerType),
            th.Property("exchange", th.IntegerType),
            th.Property("sip_timestamp", th.IntegerType),
            th.Property("conditions", th.ArrayType(th.IntegerType)),
            th.Property("timeframe", th.StringType),
        )),
        th.Property("day", th.ObjectType(
            th.Property("open", th.NumberType),
            th.Property("high", th.NumberType),
            th.Property("low", th.NumberType),
            th.Property("close", th.NumberType),
            th.Property("volume", th.NumberType),
            th.Property("vwap", th.NumberType),
            th.Property("change", th.NumberType),
            th.Property("change_percent", th.NumberType),
            th.Property("previous_close", th.NumberType),
        )),
        th.Property("details", th.ObjectType(
            th.Property("contract_type", th.StringType),
            th.Property("exercise_style", th.StringType),
            th.Property("expiration_date", th.StringType),
            th.Property("shares_per_contract", th.NumberType),
            th.Property("strike_price", th.NumberType),
            th.Property("ticker", th.StringType),
        )),
        th.Property("underlying_asset", th.ObjectType(
            th.Property("ticker", th.StringType),
            th.Property("price", th.NumberType),
            th.Property("change_to_break_even", th.NumberType),
            th.Property("last_updated", th.IntegerType),
            th.Property("timeframe", th.StringType),
        )),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v3/snapshot/options/{ticker}"


class OptionsBars1SecondStream(OptionsCustomBarsStream):
    name = "options_bars_1_second"


class OptionsBars30SecondStream(OptionsCustomBarsStream):
    name = "options_bars_30_second"


class OptionsBars1MinuteStream(OptionsCustomBarsStream):
    name = "options_bars_1_minute"


class OptionsBars5MinuteStream(OptionsCustomBarsStream):
    name = "options_bars_5_minute"


class OptionsBars30MinuteStream(OptionsCustomBarsStream):
    name = "options_bars_30_minute"


class OptionsBars1HourStream(OptionsCustomBarsStream):
    name = "options_bars_1_hour"


class OptionsBars1DayStream(OptionsCustomBarsStream):
    name = "options_bars_1_day"


class OptionsBars1WeekStream(OptionsCustomBarsStream):
    name = "options_bars_1_week"


class OptionsBars1MonthStream(OptionsCustomBarsStream):
    name = "options_bars_1_month"


class OptionsDailyTickerSummaryStream(
    OptionsTickerPartitionedStream, BaseDailyTickerSummaryStream
):
    """Stream for retrieving options daily ticker summary."""

    name = "options_daily_ticker_summary"


class OptionsPreviousDayBarStream(
    OptionsTickerPartitionedStream, BasePreviousDayBarSummaryStream
):
    """Stream for retrieving options previous day bar data."""

    name = "options_previous_day_bar"


class OptionsTradeStream(OptionsTickerPartitionedStream, BaseTradeStream):
    """Stream for retrieving options trade data."""

    name = "options_trades"


class OptionsQuoteStream(OptionsTickerPartitionedStream, BaseQuoteStream):
    """Stream for retrieving options quote data."""

    name = "options_quotes"


class OptionsSmaStream(OptionsTickerPartitionedStream, BaseIndicatorStream):
    """Stream for retrieving options SMA indicator data."""

    name = "options_sma"
    indicator_type = "sma"


class OptionsEmaStream(OptionsTickerPartitionedStream, BaseIndicatorStream):
    """Stream for retrieving options EMA indicator data."""

    name = "options_ema"
    indicator_type = "ema"


class OptionsMACDStream(OptionsTickerPartitionedStream, BaseIndicatorStream):
    """Stream for retrieving options MACD indicator data."""

    name = "options_macd"
    indicator_type = "macd"


class OptionsRSIStream(OptionsTickerPartitionedStream, BaseIndicatorStream):
    """Stream for retrieving options RSI indicator data."""

    name = "options_rsi"
    indicator_type = "rsi"


class OptionsLastTradeStream(OptionsTickerPartitionedStream, BaseLastTradeStream):
    """Stream for retrieving options last trade data."""

    name = "options_last_trade"


class OptionsChainSnapshotStream(OptionalTickerPartitionStream):
    """Stream for retrieving options chain snapshot data."""

    name = "options_chain_snapshot"
    primary_keys = ["ticker"]
    _ticker_param = "underlyingAsset"
    _use_cached_tickers_default = True
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("break_even_price", th.NumberType),
        th.Property("fmv", th.NumberType),
        th.Property("fmv_last_updated", th.IntegerType),
        th.Property("implied_volatility", th.NumberType),
        th.Property("open_interest", th.NumberType),
        th.Property("greeks", th.ObjectType(
            th.Property("delta", th.NumberType),
            th.Property("gamma", th.NumberType),
            th.Property("theta", th.NumberType),
            th.Property("vega", th.NumberType),
        )),
        th.Property("last_quote", th.ObjectType(
            th.Property("ask", th.NumberType),
            th.Property("ask_size", th.IntegerType),
            th.Property("bid", th.NumberType),
            th.Property("bid_size", th.IntegerType),
            th.Property("ask_exchange", th.IntegerType),
            th.Property("bid_exchange", th.IntegerType),
            th.Property("last_updated", th.IntegerType),
            th.Property("midpoint", th.NumberType),
            th.Property("timeframe", th.StringType),
        )),
        th.Property("last_trade", th.ObjectType(
            th.Property("price", th.NumberType),
            th.Property("size", th.IntegerType),
            th.Property("exchange", th.IntegerType),
            th.Property("sip_timestamp", th.IntegerType),
            th.Property("conditions", th.ArrayType(th.IntegerType)),
            th.Property("timeframe", th.StringType),
        )),
        th.Property("day", th.ObjectType(
            th.Property("open", th.NumberType),
            th.Property("high", th.NumberType),
            th.Property("low", th.NumberType),
            th.Property("close", th.NumberType),
            th.Property("volume", th.NumberType),
            th.Property("vwap", th.NumberType),
            th.Property("change", th.NumberType),
            th.Property("change_percent", th.NumberType),
            th.Property("previous_close", th.NumberType),
        )),
        th.Property("details", th.ObjectType(
            th.Property("contract_type", th.StringType),
            th.Property("exercise_style", th.StringType),
            th.Property("expiration_date", th.StringType),
            th.Property("shares_per_contract", th.NumberType),
            th.Property("strike_price", th.NumberType),
            th.Property("ticker", th.StringType),
        )),
        th.Property("underlying_asset", th.ObjectType(
            th.Property("ticker", th.StringType),
            th.Property("price", th.NumberType),
            th.Property("change_to_break_even", th.NumberType),
            th.Property("last_updated", th.IntegerType),
            th.Property("timeframe", th.StringType),
        )),
    ).to_dict()

    @property
    def partitions(self):
        return [{self._ticker_param: t["ticker"]} for t in self._tap.get_cached_stock_tickers()]

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/snapshot/options/{context.get('path_params').get(self._ticker_param)}"


class OptionsUnifiedSnapshotStream(MassiveRestStream):
    """Stream for retrieving options unified snapshot data.

    The Unified Snapshot endpoint retrieves consolidated market data across
    multiple asset classes in a single request. This is a bulk endpoint
    that returns multiple tickers at once with pagination.

    Configure via meltano.yml query_params:
    - type: "options" to filter to options only
    - ticker.gte: Starting ticker for lexicographic search (e.g., "O:" for all options)
    - limit: Results per page (max 250)
    """

    name = "options_unified_snapshot"
    primary_keys = ["ticker"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("type", th.StringType),  # enum: crypto, fx, indices, options, stocks
        th.Property("name", th.StringType),
        th.Property("market_status", th.StringType),
        th.Property("error", th.StringType),
        th.Property("message", th.StringType),
        th.Property("break_even_price", th.NumberType),
        th.Property("open_interest", th.NumberType),
        th.Property("fmv", th.NumberType),
        th.Property("fmv_last_updated", th.IntegerType),
        th.Property("session", th.ObjectType(
            th.Property("open", th.NumberType),
            th.Property("high", th.NumberType),
            th.Property("low", th.NumberType),
            th.Property("close", th.NumberType),
            th.Property("volume", th.NumberType),
            th.Property("vwap", th.NumberType),
            th.Property("change", th.NumberType),
            th.Property("change_percent", th.NumberType),
            th.Property("previous_close", th.NumberType),
        )),
        th.Property("last_quote", th.ObjectType(
            th.Property("ask", th.NumberType),
            th.Property("ask_size", th.IntegerType),
            th.Property("bid", th.NumberType),
            th.Property("bid_size", th.IntegerType),
            th.Property("ask_exchange", th.IntegerType),
            th.Property("bid_exchange", th.IntegerType),
            th.Property("last_updated", th.IntegerType),
            th.Property("midpoint", th.NumberType),
            th.Property("timeframe", th.StringType),
        )),
        th.Property("last_trade", th.ObjectType(
            th.Property("price", th.NumberType),
            th.Property("size", th.IntegerType),
            th.Property("exchange", th.IntegerType),
            th.Property("sip_timestamp", th.IntegerType),
            th.Property("conditions", th.ArrayType(th.IntegerType)),
            th.Property("timeframe", th.StringType),
        )),
        th.Property("greeks", th.ObjectType(
            th.Property("delta", th.NumberType),
            th.Property("gamma", th.NumberType),
            th.Property("theta", th.NumberType),
            th.Property("vega", th.NumberType),
            th.Property("implied_volatility", th.NumberType),
        )),
        th.Property("details", th.ObjectType(
            th.Property("contract_type", th.StringType),
            th.Property("exercise_style", th.StringType),
            th.Property("expiration_date", th.StringType),
            th.Property("shares_per_contract", th.NumberType),
            th.Property("strike_price", th.NumberType),
            th.Property("ticker", th.StringType),
        )),
        th.Property("underlying_asset", th.ObjectType(
            th.Property("ticker", th.StringType),
            th.Property("price", th.NumberType),
            th.Property("change_to_break_even", th.NumberType),
            th.Property("last_updated", th.IntegerType),
            th.Property("timeframe", th.StringType),
        )),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/snapshot"
