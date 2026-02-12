"""Stream type classes for tap-massive."""

from __future__ import annotations

import typing as t
from dataclasses import asdict
from datetime import datetime

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context, Record

from tap_massive.base_streams import (
    BaseConditionCodesStream,
    BaseCustomBarsStream,
    BaseDailyMarketSummaryStream,
    BaseDailyTickerSummaryStream,
    BaseExchangesStream,
    BaseIndicatorStream,
    BaseLastQuoteStream,
    BaseLastTradeStream,
    BasePreviousDayBarSummaryStream,
    BaseQuoteStream,
    BaseTickerDetailsStream,
    BaseTickerPartitionStream,
    BaseTickerStream,
    BaseTickerTypesStream,
    BaseTopMarketMoversStream,
    BaseTradeStream,
    _SnapshotNormalizationMixin,
    _TodaysChangePercentMixin,
)
from tap_massive.client import MassiveRestStream, OptionalTickerPartitionStream


class StockTickerStream(BaseTickerStream):
    """Fetch all tickers from Massive."""

    name = "stock_tickers"
    market = "stock"

    def __init__(self, tap):
        super().__init__(tap)
        self.query_params.setdefault("market", "stocks")

    primary_keys = ["ticker"]

    schema = th.PropertiesList(
        th.Property("cik", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("name", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("currency_symbol", th.StringType),
        th.Property("currency_name", th.StringType),
        th.Property("base_currency_symbol", th.StringType),
        th.Property("composite_figi", th.StringType),
        th.Property("base_currency_name", th.StringType),
        th.Property("delisted_utc", th.StringType),
        th.Property("last_updated_utc", th.StringType),
        th.Property("locale", th.StringType),
        th.Property("market", th.StringType),
        th.Property("primary_exchange", th.StringType),
        th.Property("share_class_figi", th.StringType),
        th.Property("type", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/reference/tickers"


class StockTickerPartitionStream(BaseTickerPartitionStream):
    @property
    def partitions(self):
        return [{"ticker": t["ticker"]} for t in self._tap.get_cached_stock_tickers()]


class StockTickerDetailsStream(StockTickerPartitionStream, BaseTickerDetailsStream):
    name = "stock_ticker_details"


class StockDailyTickerSummaryStream(
    StockTickerPartitionStream, BaseDailyTickerSummaryStream
):
    name = "stock_daily_ticker_summary"


class StockIndicatorStream(StockTickerPartitionStream, BaseIndicatorStream):
    def get_url(self, context: Context) -> str:
        raise ValueError("URL Must be overridden by subclass.")


class StockDailyMarketSummaryStream(BaseDailyMarketSummaryStream):
    name = "stock_daily_market_summary"

    def get_url(self, context: Context):
        date = context.get("path_params").get("date")
        if date is None:
            date = datetime.today().date().isoformat()
        return f"{self.url_base}/v2/aggs/grouped/locale/us/market/stocks/{date}"


class StockPreviousDayBarSummaryStream(
    StockTickerPartitionStream, BasePreviousDayBarSummaryStream
):
    """Retrieve the previous trading day's OHLCV data for a specified stock ticker.
    Not really useful given we have the other streams.
    """

    name = "stock_previous_day_bar"


class StockTopMarketMoversStream(BaseTopMarketMoversStream):
    """Retrieve snapshot data highlighting the top 20 gainers or losers in the U.S. stock market.
    Gainers are stocks with the largest percentage increase since the previous day's close, and losers are those
    with the largest percentage decrease. Only tickers with a minimum trading volume of 10,000 are included.
    Snapshot data is cleared daily at 3:30 AM EST and begins repopulating as exchanges report new information,
    typically starting around 4:00 AM EST.
    """

    name = "stock_top_market_movers"

    def get_url(self, context: Context):
        direction = context.get("direction")
        return f"{self.url_base}/v2/snapshot/locale/us/markets/stocks/{direction}"


class StockTickerSnapshotStream(
    _SnapshotNormalizationMixin, _TodaysChangePercentMixin, StockTickerPartitionStream
):
    """Stream for retrieving stock single ticker snapshot data.

    Returns real-time snapshot data for a single stock ticker including
    current prices, volumes, and session data.
    """

    name = "stock_ticker_snapshot"
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
                th.Property("otc", th.BooleanType),
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
                th.Property("otc", th.BooleanType),
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
                th.Property("otc", th.BooleanType),
            ),
        ),
        th.Property(
            "last_quote",
            th.ObjectType(
                th.Property("ask_price", th.NumberType),
                th.Property("ask_size", th.IntegerType),
                th.Property("bid_price", th.NumberType),
                th.Property("bid_size", th.IntegerType),
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
        th.Property(
            "last_trade",
            th.ObjectType(
                th.Property("price", th.NumberType),
                th.Property("size", th.IntegerType),
                th.Property("timestamp", th.IntegerType),
                th.Property("conditions", th.ArrayType(th.IntegerType)),
                th.Property("id", th.StringType),
                th.Property("exchange", th.IntegerType),
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

    def parse_response(self, record: dict, context: dict) -> t.Iterable[dict]:
        if isinstance(record, dict) and isinstance(record.get("ticker"), dict):
            yield record["ticker"]
        else:
            yield record

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"


class StockFullMarketSnapshotStream(
    _SnapshotNormalizationMixin, _TodaysChangePercentMixin, MassiveRestStream
):
    """Stream for retrieving stock full market snapshot data."""

    name = "stock_full_market_snapshot"
    primary_keys = ["ticker"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("todays_change", th.NumberType),
        th.Property("todays_change_percent", th.NumberType),
        th.Property("updated", th.IntegerType),
        th.Property("fmv", th.NumberType),
        th.Property("day", th.ObjectType(additional_properties=True)),
        th.Property("min", th.ObjectType(additional_properties=True)),
        th.Property("prev_day", th.ObjectType(additional_properties=True)),
        th.Property("last_quote", th.ObjectType(additional_properties=True)),
        th.Property("last_trade", th.ObjectType(additional_properties=True)),
    ).to_dict()

    def parse_response(self, record: dict, context: dict) -> t.Iterable[dict]:
        if isinstance(record, dict) and isinstance(record.get("tickers"), list):
            for ticker in record["tickers"]:
                yield ticker
        else:
            yield record

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v2/snapshot/locale/us/markets/stocks/tickers"


class StockUnifiedSnapshotStream(_SnapshotNormalizationMixin, MassiveRestStream):
    """Stream for retrieving stock unified snapshot data."""

    name = "stock_unified_snapshot"
    primary_keys = ["ticker"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("type", th.StringType),
        th.Property("name", th.StringType),
        th.Property("market_status", th.StringType),
        th.Property("error", th.StringType),
        th.Property("message", th.StringType),
        th.Property("fmv", th.NumberType),
        th.Property("fmv_last_updated", th.IntegerType),
        th.Property("timeframe", th.StringType),
        th.Property("last_updated", th.IntegerType),
        th.Property(
            "last_minute",
            th.ObjectType(
                th.Property("close", th.NumberType),
                th.Property("high", th.NumberType),
                th.Property("low", th.NumberType),
                th.Property("open", th.NumberType),
                th.Property("transactions", th.IntegerType),
                th.Property("volume", th.NumberType),
                th.Property("vwap", th.NumberType),
            ),
        ),
        th.Property(
            "session",
            th.ObjectType(
                th.Property("change", th.NumberType),
                th.Property("change_percent", th.NumberType),
                th.Property("early_trading_change", th.NumberType),
                th.Property("early_trading_change_percent", th.NumberType),
                th.Property("regular_trading_change", th.NumberType),
                th.Property("regular_trading_change_percent", th.NumberType),
                th.Property("late_trading_change", th.NumberType),
                th.Property("late_trading_change_percent", th.NumberType),
                th.Property("price", th.NumberType),
                th.Property("open", th.NumberType),
                th.Property("high", th.NumberType),
                th.Property("low", th.NumberType),
                th.Property("close", th.NumberType),
                th.Property("previous_close", th.NumberType),
                th.Property("volume", th.NumberType),
            ),
        ),
        th.Property(
            "last_quote",
            th.ObjectType(
                th.Property("ask", th.NumberType),
                th.Property("ask_exchange", th.IntegerType),
                th.Property("ask_size", th.IntegerType),
                th.Property("bid", th.NumberType),
                th.Property("bid_exchange", th.IntegerType),
                th.Property("bid_size", th.IntegerType),
                th.Property("midpoint", th.NumberType),
                th.Property("timeframe", th.StringType),
                th.Property("last_updated", th.IntegerType),
            ),
        ),
        th.Property(
            "last_trade",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("price", th.NumberType),
                th.Property("size", th.IntegerType),
                th.Property("exchange", th.IntegerType),
                th.Property("conditions", th.ArrayType(th.IntegerType)),
                th.Property("timeframe", th.StringType),
                th.Property("last_updated", th.IntegerType),
                th.Property("participant_timestamp", th.IntegerType),
                th.Property("sip_timestamp", th.IntegerType),
            ),
        ),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/snapshot"


class StockTradeStream(StockTickerPartitionStream, BaseTradeStream):
    """Retrieve comprehensive, tick-level trade data for a specified stock ticker within a defined time range.
    Each record includes price, size, exchange, trade conditions, and precise timestamp information.
    This granular data is foundational for constructing aggregated bars and performing in-depth analyses, as it captures
    every eligible trade that contributes to calculations of open, high, low, and close (OHLC) values.
    By leveraging these trades, users can refine their understanding of intraday price movements, test and optimize
    algorithmic strategies, and ensure compliance by maintaining an auditable record of market activity.

    Use Cases: Intraday analysis, algorithmic trading, market microstructure research, data integrity and compliance.

    NOTE: This stream cannot stream multiple tickers at once, so if we want to stream or fetch trades for multiple
    tickers you need to send multiple parallel or sequential API requests (one for each ticker).
    Data is delayed 15 minutes for developer plan. For real-time data top the Advanced Subscription is needed.
    """

    name = "stock_trades"


class StockLastTradeStream(StockTickerPartitionStream, BaseLastTradeStream):
    name = "stock_last_trade"


class StockQuoteStream(StockTickerPartitionStream, BaseQuoteStream):
    name = "stock_quotes"


class StockLastQuoteStream(StockTickerPartitionStream, BaseLastQuoteStream):
    name = "stock_last_quote"


class StockTickerTypesStream(BaseTickerTypesStream):
    """Stock ticker types."""

    name = "stock_ticker_types"
    _asset_class = "stocks"


class StockExchangesStream(BaseExchangesStream):
    """Stock-specific exchanges."""

    name = "stock_exchanges"
    _asset_class = "stocks"


class StockConditionCodesStream(BaseConditionCodesStream):
    """Stock-specific condition codes."""

    name = "stock_condition_codes"
    _asset_class = "stocks"


class StockRelatedTickersStream(StockTickerPartitionStream):
    name = "stock_related_tickers"

    primary_keys = ["ticker"]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v1/related-companies/{ticker}"


class StockBars1SecondStream(StockTickerPartitionStream, BaseCustomBarsStream):
    name = "stock_bars_1_second"


class StockBars30SecondStream(StockTickerPartitionStream, BaseCustomBarsStream):
    name = "stock_bars_30_second"


class StockBars1MinuteStream(StockTickerPartitionStream, BaseCustomBarsStream):
    name = "stock_bars_1_minute"


class StockBars5MinuteStream(StockTickerPartitionStream, BaseCustomBarsStream):
    name = "stock_bars_5_minute"


class StockBars30MinuteStream(StockTickerPartitionStream, BaseCustomBarsStream):
    name = "stock_bars_30_minute"


class StockBars1HourStream(StockTickerPartitionStream, BaseCustomBarsStream):
    name = "stock_bars_1_hour"


class StockBars1DayStream(StockTickerPartitionStream, BaseCustomBarsStream):
    name = "stock_bars_1_day"


class StockBars1WeekStream(StockTickerPartitionStream, BaseCustomBarsStream):
    name = "stock_bars_1_week"


class StockBars1MonthStream(StockTickerPartitionStream, BaseCustomBarsStream):
    name = "stock_bars_1_month"


class SmaStream(StockIndicatorStream):
    name = "stock_sma"
    indicator_type = "sma"


class EmaStream(StockIndicatorStream):
    name = "stock_ema"
    indicator_type = "ema"


class MACDStream(StockIndicatorStream):
    name = "stock_macd"
    indicator_type = "macd"
    schema = BaseIndicatorStream._build_schema(True)


class RSIStream(StockIndicatorStream):
    name = "stock_rsi"
    indicator_type = "rsi"


class MarketHolidaysStream(MassiveRestStream):
    """Market Holidays Stream (forward-looking)"""

    name = "market_holidays"

    primary_keys = ["date", "exchange", "name"]

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("close", th.StringType),
        th.Property("date", th.DateType),
        th.Property("exchange", th.StringType),
        th.Property("name", th.StringType),
        th.Property("open", th.StringType),
        th.Property("status", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v1/marketstatus/upcoming"

    def parse_response(self, record: dict, context: dict) -> t.Iterable[dict]:
        if isinstance(record, dict) and isinstance(record.get("response"), list):
            for item in record["response"]:
                yield item
        else:
            yield record


class MarketStatusStream(MassiveRestStream):
    """Market Status Stream"""

    name = "market_status"

    primary_keys = ["server_time"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("after_hours", th.BooleanType),
        th.Property(
            "currencies",
            th.ObjectType(
                th.Property("crypto", th.StringType),
                th.Property("fx", th.StringType),
            ),
        ),
        th.Property("early_hours", th.BooleanType),
        th.Property(
            "exchanges",
            th.ObjectType(
                th.Property("nasdaq", th.StringType),
                th.Property("nyse", th.StringType),
                th.Property("otc", th.StringType),
            ),
        ),
        th.Property(
            "indices_groups",
            th.ObjectType(
                th.Property("cccy", th.StringType),
                th.Property("cgi", th.StringType),
                th.Property("dow_jones", th.StringType),
                th.Property("ftse_russell", th.StringType),
                th.Property("msci", th.StringType),
                th.Property("mstar", th.StringType),
                th.Property("mstarc", th.StringType),
                th.Property("nasdaq", th.StringType),
                th.Property("s_and_p", th.StringType),
                th.Property("societe_generale", th.StringType),
            ),
        ),
        th.Property("market", th.StringType),
        th.Property("server_time", th.DateTimeType),
    ).to_dict()

    def get_records(
        self, context: dict[str, t.Any] | None
    ) -> t.Iterable[dict[str, t.Any]]:
        market_status = self.client.get_market_status()
        yield asdict(market_status)

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row["after_hours"] = row.pop("afterHours", None)
        row["early_hours"] = row.pop("earlyHours", None)
        row["indices_groups"] = row.pop("indicesGroups", None)
        row["server_time"] = row.pop("serverTime", None)
        return row


# --- Stock-Specific Reference Streams ---


class StockIPOsStream(OptionalTickerPartitionStream):
    """IPOs Stream"""

    name = "stock_ipos"

    primary_keys = ["listing_date", "ticker"]
    replication_key = "listing_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = False

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("announced_date", th.IntegerType),
        th.Property("currency_code", th.StringType),
        th.Property("final_issue_price", th.NumberType),
        th.Property("highest_offer_price", th.NumberType),
        th.Property(
            "ipo_status",
            th.StringType,
            enum=[
                "direct_listing_process",
                "history",
                "new",
                "pending",
                "postponed",
                "rumor",
                "withdrawn",
            ],
        ),
        th.Property("isin", th.StringType),
        th.Property("issuer_name", th.StringType),
        th.Property("last_updated", th.IntegerType),
        th.Property("listing_date", th.IntegerType),
        th.Property("lot_size", th.IntegerType),
        th.Property("lowest_offer_price", th.NumberType),
        th.Property("max_shares_offered", th.IntegerType),
        th.Property("min_shares_offered", th.IntegerType),
        th.Property("primary_exchange", th.StringType),
        th.Property("security_description", th.StringType),
        th.Property("security_type", th.StringType),
        th.Property("shares_outstanding", th.IntegerType),
        th.Property("ticker", th.StringType),
        th.Property("total_offer_size", th.NumberType),
        th.Property("us_code", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/vX/reference/ipos"

    @staticmethod
    def post_process(row: Record, context: Context | None = None) -> dict | None:
        if row.get("ipo_status"):
            row["ipo_status"] = row["ipo_status"].lower()
        return row


class StockSplitsDeprecatedStream(OptionalTickerPartitionStream):
    """Splits Stream (v3 endpoint)"""

    name = "stock_splits_deprecated"

    primary_keys = ["id"]
    replication_key = "execution_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("execution_date", th.DateType),
        th.Property("split_from", th.NumberType),
        th.Property("split_to", th.NumberType),
        th.Property("ticker", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v3/reference/splits"


class StockSplitsStream(OptionalTickerPartitionStream):
    """Splits Stream (new stocks/v1 endpoint)"""

    name = "stock_splits"

    primary_keys = ["id"]
    replication_key = "execution_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("execution_date", th.DateType),
        th.Property("split_from", th.NumberType),
        th.Property("split_to", th.NumberType),
        th.Property("adjustment_type", th.StringType),
        th.Property("historical_adjustment_factor", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/v1/splits"


class StockDividendsDeprecatedStream(OptionalTickerPartitionStream):
    """Dividends Stream (v3 endpoint)"""

    name = "stock_dividends_deprecated"

    primary_keys = ["id"]
    replication_key = "ex_dividend_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("ex_dividend_date", th.DateType),
        th.Property("declaration_date", th.DateType),
        th.Property("pay_date", th.DateType),
        th.Property("record_date", th.DateType),
        th.Property("currency", th.StringType),
        th.Property("cash_amount", th.NumberType),
        th.Property("dividend_type", th.StringType, enum=["CD", "SC", "LT", "ST"]),
        th.Property("frequency", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v3/reference/dividends"


class StockDividendsStream(OptionalTickerPartitionStream):
    """Dividends Stream (new stocks/v1 endpoint)"""

    name = "stock_dividends"

    primary_keys = ["id"]
    replication_key = "ex_dividend_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("ex_dividend_date", th.DateType),
        th.Property("declaration_date", th.DateType),
        th.Property("pay_date", th.DateType),
        th.Property("record_date", th.DateType),
        th.Property("currency", th.StringType),
        th.Property("cash_amount", th.NumberType),
        th.Property("distribution_type", th.StringType),
        th.Property("frequency", th.IntegerType),
        th.Property("historical_adjustment_factor", th.NumberType),
        th.Property("split_adjusted_cash_amount", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/v1/dividends"


class StockTickerEventsStream(StockTickerPartitionStream):
    """Ticker Events Stream"""

    name = "stock_ticker_events"

    primary_keys = ["date", "event_type", "name"]

    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("date", th.DateType),
        th.Property("event_type", th.StringType),
        th.Property("ticker_change", th.ObjectType(additional_properties=True)),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param, {})
        return f"{self.url_base}/vX/reference/tickers/{ticker}/events"

    def parse_response(self, record: dict, context: dict) -> t.Iterable[dict]:
        name = record.get("name")
        events = record.get("events", [])
        for event in events:
            if isinstance(event, dict):
                yield {"name": name, **event}
