"""Stream type classes for tap-massive base streams."""

from __future__ import annotations

import logging
import re
import typing as t
from datetime import datetime
from decimal import Decimal, InvalidOperation

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context, Record

from tap_massive.client import MassiveRestStream


def safe_decimal(x):
    try:
        if x is None or x == "":
            return None
        return Decimal(str(x))
    except (ValueError, TypeError, InvalidOperation):
        return None


def safe_int(x):
    try:
        if x is None or x == "":
            return None
        return int(Decimal(str(x)))
    except (ValueError, TypeError):
        return None

class BaseTickerStream(MassiveRestStream):
    market = None
    _ticker_param = "ticker"

    def _break_loop_check(self, next_url, replication_key_value=None) -> bool:
        return not next_url

    def get_ticker_list(self) -> list[str] | None:
        assert self.market is not None
        tickers_cfg = self.config.get(f"{self.market}_tickers", {})
        tickers = tickers_cfg.get("select_tickers") if tickers_cfg else None

        if not tickers or tickers in ("*", ["*"]):
            return None

        if isinstance(tickers, str):
            try:
                return tickers.split(",")
            except AttributeError:
                raise

        if isinstance(tickers, list):
            if tickers == ["*"]:
                return None
            return tickers
        return None

    def get_child_context(self, record, context):
        return {"ticker": record.get("ticker")}

    def get_records(
        self, context: dict[str, t.Any] | None
    ) -> t.Iterable[dict[str, t.Any]]:
        context = {} if context is None else context
        ticker_list = self.get_ticker_list()
        query_params = self.query_params.copy()
        if not ticker_list:
            logging.info("Pulling all tickers...")
            context["query_params"] = query_params
            yield from self.paginate_records(context)
        else:
            logging.info(f"Pulling tickers: {ticker_list}")
            for ticker in ticker_list:
                query_params.update({self._ticker_param: ticker})
                context["query_params"] = query_params
                yield from self.paginate_records(context)

class BaseTickerPartitionedStream(MassiveRestStream):
    @property
    def partitions(self):
        raise ValueError("Method partitions must be overridden by subclass.")


class BaseTickerDetailsStream(BaseTickerPartitionedStream):
    primary_keys = ["ticker"]

    schema = th.PropertiesList(
        th.Property("active", th.BooleanType),
        th.Property(
            "address",
            th.ObjectType(
                th.Property("address1", th.StringType),
                th.Property("address2", th.StringType),
                th.Property("city", th.StringType),
                th.Property("postal_code", th.StringType),
                th.Property("state", th.StringType),
            ),
        ),
        th.Property(
            "branding",
            th.ObjectType(
                th.Property("icon_url", th.StringType),
                th.Property("logo_url", th.StringType),
            ),
        ),
        th.Property("cik", th.StringType),
        th.Property("composite_figi", th.StringType),
        th.Property("currency_name", th.StringType),
        th.Property("delisted_utc", th.StringType),
        th.Property("description", th.StringType),
        th.Property("homepage_url", th.StringType),
        th.Property("list_date", th.DateType),
        th.Property("locale", th.StringType),  # enum: "us", "global"
        th.Property(
            "market", th.StringType
        ),  # enum: "stocks", "crypto", "fx", "otc", "indices"
        th.Property("market_cap", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("phone_number", th.StringType),
        th.Property("primary_exchange", th.StringType),
        th.Property("round_lot", th.NumberType),
        th.Property("share_class_figi", th.StringType),
        th.Property("share_class_shares_outstanding", th.NumberType),
        th.Property("sic_code", th.StringType),
        th.Property("sic_description", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("ticker_root", th.StringType),
        th.Property("ticker_suffix", th.StringType),
        th.Property("total_employees", th.NumberType),
        th.Property("type", th.StringType),
        th.Property("weighted_shares_outstanding", th.NumberType),
        th.Property("base_currency_name", th.StringType),
        th.Property("base_currency_symbol", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("cusip", th.StringType),
        th.Property("last_updated_utc", th.DateTimeType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v3/reference/tickers/{ticker}"


class BaseCustomBarsStream(BaseTickerPartitionedStream):
    primary_keys = ["timestamp", "ticker"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    is_sorted = False  # Massive cannot guarantee sorted records across pages

    _use_cached_tickers_default = True
    _api_expects_unix_timestamp = True
    _unix_timestamp_unit = "ms"
    _requires_end_timestamp_in_path_params = True
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType),
        th.Property("ticker", th.StringType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("vwap", th.NumberType),
        th.Property("transactions", th.IntegerType),
        th.Property("otc", th.BooleanType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

        self._cfg_ending_timestamp_key = "to"

    def build_path_params(self, path_params: dict) -> str:
        keys = ["multiplier", "timespan", "from", self._cfg_ending_timestamp_key]
        return "/" + "/".join(str(path_params[k]) for k in keys if k in path_params)

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        path_params = self.build_path_params(context.get("path_params"))
        return f"{self.url_base}/v2/aggs/ticker/{ticker}/range{path_params}"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        rename_map = {
            "t": "timestamp",
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "vw": "vwap",
            "n": "transactions",
        }
        for old_key, new_key in rename_map.items():
            if old_key in row:
                row[new_key] = row.pop(old_key)
                if new_key in ("open", "high", "low", "close", "vwap"):
                    row[new_key] = safe_decimal(row[new_key])

        row["ticker"] = context.get(self._ticker_param)
        row["otc"] = row.get("otc", None)
        row["timestamp"] = self.safe_parse_datetime(row["timestamp"]).isoformat()
        return row


class BaseDailyMarketSummaryStream(MassiveRestStream):
    primary_keys = ["timestamp", "ticker"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("timestamp", th.DateTimeType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("vwap", th.NumberType),
        th.Property("transactions", th.IntegerType),
        th.Property("otc", th.BooleanType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        if "t" not in row:
            logging.warning(
                f"Key 't' not found in record, likely metadata field ---> Skipping row... {row}"
            )
            return None
        mapping = {
            "T": "ticker",
            "t": "timestamp",
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "vw": "vwap",
            "n": "transactions",
            "otc": "otc",
        }
        for old_key, new_key in mapping.items():
            if old_key in row:
                row[new_key] = row.pop(old_key)
        if "timestamp" in row:
            row["timestamp"] = self.safe_parse_datetime(row["timestamp"])
        return row


class BaseDailyTickerSummaryStream(BaseTickerPartitionedStream):
    primary_keys = ["from", "ticker"]
    replication_key = "from"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _incremental_timestamp_is_date = True
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("from", th.DateType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("otc", th.BooleanType),
        th.Property("pre_market", th.NumberType),
        th.Property("after_hours", th.NumberType),
        th.Property("status", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

    def get_url(self, context: Context):
        date = self.path_params.get("date")
        ticker = context.get(self._ticker_param)
        if date is None:
            date = datetime.today().date().isoformat()
        return f"{self.url_base}/v1/open-close/{ticker}/{date}"

    @staticmethod
    def post_process(row: Record, context: Context | None = None) -> dict | None:
        row["pre_market"] = row.pop("preMarket", None)
        row["after_hours"] = row.pop("afterHours", None)
        return row


class BasePreviousDayBarSummaryStream(BaseTickerPartitionedStream):
    """Retrieve the previous trading day's OHLCV data for a specified ticker.
    Not really useful given we have the other streams."""

    primary_keys = ["timestamp", "ticker"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("timestamp", th.DateTimeType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("vwap", th.NumberType),
        th.Property("transactions", th.NumberType),
        th.Property("otc", th.BooleanType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v2/aggs/ticker/{ticker}/prev"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        mapping = {
            "T": "ticker",
            "t": "timestamp",
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "vw": "vwap",
            "n": "transactions",
            "otc": "otc",
        }
        for old_key, new_key in mapping.items():
            if old_key in row:
                row[new_key] = row.pop(old_key)
                if new_key in ("open", "high", "low", "close", "vwap"):
                    row[new_key] = safe_decimal(row[new_key])
        row["timestamp"] = self.safe_parse_datetime(row["timestamp"])
        return row


class TickerSnapshotStream(MassiveRestStream):
    """Retrieve the most recent market data snapshot for a single ticker.
    Not really useful given we have the other streams."""

    name = "ticker_snapshot"
    pass


class FullMarketSnapshotStream(MassiveRestStream):
    """
    Retrieve a comprehensive snapshot of the entire U.S. stock market, covering over 10,000+ actively traded
    tickers in a single response. Not really useful given we have the other streams.
    """

    name = "full_market_snapshot"
    pass


class UnifiedSnapshotStream(MassiveRestStream):
    """
    Retrieve unified snapshots of market data for multiple asset classes including stocks, options, forex,
    and cryptocurrencies in a single request. Not really useful given we have the other streams.
    """

    name = "unified_snapshot"
    pass


class BaseTopMarketMoversStream(MassiveRestStream):
    """
    Retrieve snapshot data highlighting the top 20 gainers or losers
    Gainers are <stocks, forex, etc> with the largest percentage increase since the previous day’s close, and losers are those
    with the largest percentage decrease.
    """

    primary_keys = ["updated", "ticker"]
    replication_key = "updated"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("updated", th.DateTimeType),
        th.Property("ticker", th.StringType),
        th.Property("day", th.AnyType()),
        th.Property("last_quote", th.AnyType()),
        th.Property("last_trade", th.AnyType()),
        th.Property("min", th.AnyType()),
        th.Property("prev_day", th.AnyType()),
        th.Property("todays_change", th.NumberType),
        th.Property("todays_change_percent", th.NumberType),
        th.Property("fair_market_value", th.BooleanType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        if (
            self.path_params.get("direction") is None
            or self.path_params.get("direction") == ""
            or self.path_params.get("direction").lower() == "both"
            or "direction" not in self.path_params
        ):
            for direction in ["gainers", "losers"]:
                url = self.get_url(context={"direction": direction})
                data = self.requests_session.get(url, params=self.query_params)
                data.raise_for_status()
                response_data = data.json()
                tickers = response_data.get("tickers")
                if tickers:
                    for record in tickers:
                        record["direction"] = direction
                        self.post_process(record)
                        yield record
                elif response_data.get("status") == "NOT_AUTHORIZED":
                    self.logger.warning(
                        f"Not authorized for top market movers: {response_data.get('message')}"
                    )
                    return
        else:
            direction = self.path_params.get("direction")
            url = self.get_url(context)
            data = self.requests_session.get(url, params=self.query_params)
            data.raise_for_status()
            response_data = data.json()
            tickers = response_data.get("tickers")
            if tickers:
                for record in tickers:
                    record["direction"] = direction
                    self.post_process(record)
                    yield record
            elif response_data.get("status") == "NOT_AUTHORIZED":
                self.logger.warning(
                    f"Not authorized for top market movers: {response_data.get('message')}"
                )
                return

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        def to_snake_case(s):
            return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()

        def clean_keys(d):
            keys = list(d.keys())
            for key in keys:
                value = d.pop(key)
                new_key = to_snake_case(key)
                if isinstance(value, dict):
                    clean_keys(value)
                d[new_key] = value

        clean_keys(row)
        row["updated"] = self.safe_parse_datetime(row["updated"])
        return row


class BaseTradeStream(BaseTickerPartitionedStream):
    """
    Retrieve comprehensive, tick-level trade data for a specified stock ticker within a defined time range.
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

    primary_keys = [
        "ticker",
        "exchange",
        "id",
    ]  # if there happen to be duplicate records, then add sip_timestamp

    replication_key = "sip_timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    is_sorted = False

    _use_cached_tickers_default = True
    _api_expects_unix_timestamp = True
    _unix_timestamp_unit = "ns"
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("sip_timestamp", th.DateTimeType),
        th.Property("participant_timestamp", th.IntegerType),
        th.Property("price", th.NumberType),
        th.Property("size", th.NumberType),
        th.Property("tape", th.IntegerType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("conditions", th.ArrayType(th.AnyType())),
        th.Property("correction", th.AnyType()),
        th.Property("trf_id", th.IntegerType),
        th.Property("trf_timestamp", th.IntegerType),
        th.Property("exchange", th.IntegerType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v3/trades/{ticker}"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row[self.replication_key] = self.safe_parse_datetime(
            row[self.replication_key]
        ).isoformat()
        row["exchange"] = safe_int(row["exchange"])
        if "participant_timestamp" in row:
            row["participant_timestamp"] = safe_int(row["participant_timestamp"])
        if "trf_timestamp" in row:
            row["trf_timestamp"] = safe_int(row["trf_timestamp"])

        row["price"] = safe_decimal(row["price"])
        return row


class BaseLastTradeStream(BaseTickerPartitionedStream):
    primary_keys = ["t", "ticker", "q"]
    replication_key = "t"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("condition_codes", th.ArrayType(th.IntegerType())),
        th.Property("trade_correction_indicator", th.IntegerType),
        th.Property("trf_timestamp", th.IntegerType),
        th.Property("trade_id", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("trf_id", th.IntegerType),
        th.Property("volume", th.NumberType),
        th.Property("sip_timestamp", th.IntegerType),
        th.Property("exchange_id", th.IntegerType),
        th.Property("exchange_participant_timestamp", th.IntegerType),
        th.Property("tape_id", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v2/last/trade/{ticker}"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        key_map = {
            "T": "ticker",
            "c": "condition_codes",
            "e": "trade_correction_indicator",
            "f": "trf_timestamp",
            "i": "trade_id",
            "p": "price",
            "q": "sequence_number",
            "r": "trf_id",
            "s": "volume",
            "t": "sip_timestamp",
            "x": "exchange_id",
            "y": "exchange_participant_timestamp",
            "z": "tape_id",
        }

        mapped_row = {key_map.get(k, k): v for k, v in row.items()}
        row = mapped_row

        row[self.replication_key] = self.safe_parse_datetime(
            row[self.replication_key]
        ).isoformat()
        return row


class BaseQuoteStream(BaseTickerPartitionedStream):
    primary_keys = ["ticker", "sip_timestamp", "sequence_number"]
    replication_key = "sip_timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    is_sorted = False

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
        th.Property("participant_timestamp", th.DateTimeType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("sip_timestamp", th.DateTimeType),
        th.Property("tape", th.IntegerType),
        th.Property("trf_timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v3/quotes/{ticker}"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row["ticker"] = context.get(self._ticker_param)
        row[self.replication_key] = self.safe_parse_datetime(
            row[self.replication_key]
        ).isoformat()
        if "trf_timestamp" in row:
            row["trf_timestamp"] = safe_int(row["trf_timestamp"])
        return row


class BaseLastQuoteStream(BaseTickerPartitionedStream):
    primary_keys = ["t", "ticker", "q"]
    replication_key = "t"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("P", th.NumberType),
        th.Property("S", th.IntegerType),
        th.Property("T", th.StringType),
        th.Property("X", th.IntegerType),
        th.Property("c", th.ArrayType(th.IntegerType)),
        th.Property("f", th.IntegerType),  # TRF nanoseconds
        th.Property("i", th.ArrayType(th.IntegerType)),
        th.Property("p", th.NumberType),
        th.Property("q", th.IntegerType),
        th.Property("s", th.IntegerType),
        th.Property("t", th.DateTimeType),  # SIP nanoseconds
        th.Property("x", th.IntegerType),
        th.Property("y", th.IntegerType),  # Participant ns
        th.Property("z", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v2/last/nbbo/{ticker}"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row["ticker"] = context.get(self._ticker_param)
        row[self.replication_key] = self.safe_parse_datetime(
            row[self.replication_key]
        ).isoformat()
        return row


class BaseIndicatorStream(BaseTickerPartitionedStream):
    primary_keys = ["timestamp", "ticker", "indicator", "series_window_timespan"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = True
    _api_expects_unix_timestamp = True
    _unix_timestamp_unit = "ms"
    _ticker_in_path_params = True

    indicator_type = None  # Must be set by subclass: "sma", "ema", "macd", "rsi"

    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType),  # Indicator value timestamp
        th.Property("underlying_timestamp", th.DateTimeType),
        th.Property("ticker", th.StringType),
        th.Property("indicator", th.StringType),
        th.Property("series_window_timespan", th.StringType),
        th.Property("value", th.NumberType),
        th.Property("underlying_ticker", th.StringType),
        th.Property("underlying_open", th.NumberType),
        th.Property("underlying_high", th.NumberType),
        th.Property("underlying_low", th.NumberType),
        th.Property("underlying_close", th.NumberType),
        th.Property("underlying_volume", th.NumberType),
        th.Property("underlying_vwap", th.NumberType),
        th.Property("underlying_transactions", th.IntegerType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

    @staticmethod
    def find_closest_agg(ts, agg_ts_map):
        left, right = 0, len(agg_ts_map) - 1
        result = None
        while left <= right:
            mid = (left + right) // 2
            if agg_ts_map[mid][0] is not None and agg_ts_map[mid][0] <= ts:
                result = agg_ts_map[mid][1]
                left = mid + 1
            else:
                right = mid - 1
        return result or {}

    def base_indicator_url(self):
        return f"{self.url_base}/v1/indicators"

    def get_url(self, context: Context):
        if self.indicator_type is None:
            raise NotImplementedError("indicator_type must be set by subclass")
        ticker = context.get(self._ticker_param)
        return f"{self.base_indicator_url()}/{self.indicator_type}/{ticker}"

    def parse_response(self, record: dict, context: dict) -> t.Iterable[dict]:
        # Flatten a single API record (which may have many "values") into flat records
        agg_window = self.query_params.get("window")
        agg_timespan = self.query_params.get("timespan")
        agg_series_type = self.query_params.get("series_type")
        series_window_timespan = f"{agg_series_type}_{agg_timespan}_{agg_window}"

        aggregates = (record.get("underlying") or {}).get("aggregates", [])
        agg_ts_map = []
        for agg in aggregates:
            ts = agg.get("t")
            if isinstance(ts, (int, float, Decimal)):
                ts = self.safe_parse_datetime(ts).isoformat()
            agg_ts_map.append((ts, agg))

        agg_ts_map.sort(key=lambda x: x[0])
        for value in record.get("values", []):
            ts = value.get("timestamp")
            if isinstance(ts, (int, float, Decimal)):
                ts = self.safe_parse_datetime(ts).isoformat()

            matching_agg = self.find_closest_agg(ts, agg_ts_map)

            yield {
                "ticker": record.get("ticker") or context.get(self._ticker_param),
                "indicator": self.name,
                "series_window_timespan": series_window_timespan,
                "timestamp": ts,
                "value": safe_decimal(value.get("value")),
                "underlying_ticker": matching_agg.get("T"),
                "underlying_volume": safe_decimal(matching_agg.get("v")),
                "underlying_vwap": safe_decimal(matching_agg.get("vw")),
                "underlying_open": safe_decimal(matching_agg.get("o")),
                "underlying_close": safe_decimal(matching_agg.get("c")),
                "underlying_high": safe_decimal(matching_agg.get("h")),
                "underlying_low": safe_decimal(matching_agg.get("l")),
                "underlying_transactions": safe_int(matching_agg.get("n")),
                "underlying_timestamp": (
                    self.safe_parse_datetime(matching_agg.get("t")).isoformat()
                    if matching_agg.get("t")
                    else None
                ),
            }
