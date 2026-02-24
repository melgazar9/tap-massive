"""Stream type classes for tap-massive base streams."""

from __future__ import annotations

import logging
import typing as t
from datetime import datetime
from decimal import Decimal, InvalidOperation

import requests
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
    _boolean_flag_key = "active"
    _set_market_query_param = (
        True  # Set to False if endpoint doesn't accept market param
    )

    def __init__(self, tap, **kwargs):
        super().__init__(tap, **kwargs)
        if self.market and self._set_market_query_param:
            # Special case: stock -> stocks (API expects plural)
            api_market = "stocks" if self.market == "stock" else self.market
            self.query_params.setdefault("market", api_market)

    def _break_loop_check(self, next_url, replication_key_value=None) -> bool:
        return not next_url

    def get_ticker_list(self) -> list[str] | None:
        """Ticker streams also check their own stream name as a config section."""
        return self.resolve_select_tickers(
            selector_keys=self.ticker_selector_keys,
            market=self.market,
            include_stream_name=True,
        )

    def get_child_context(self, record, context):
        return {"ticker": record.get("ticker")}

    @staticmethod
    def boolean_query_variants(
        base_query_params: dict[str, t.Any],
        param_name: str,
        mode: str | None,
    ) -> list[dict[str, t.Any]]:
        """Return query param variants for two-pass boolean selector handling."""
        query_params = base_query_params.copy()
        if mode == "both":
            query_params.pop(param_name, None)
            return [
                {**query_params, param_name: True},
                {**query_params, param_name: False},
            ]
        return [query_params]

    def _get_boolean_flag_mode(self) -> str | None:
        return self.other_params.get(self._boolean_flag_key)

    def get_records(
        self, context: dict[str, t.Any] | None
    ) -> t.Iterable[dict[str, t.Any]]:
        context = {} if context is None else context
        ticker_list = self.get_ticker_list()
        query_variants = self.boolean_query_variants(
            self.query_params.copy(),
            self._boolean_flag_key,
            self._get_boolean_flag_mode(),
        )

        if not ticker_list:
            logging.info("Pulling all tickers...")
            for params in query_variants:
                ctx = dict(context, query_params=params.copy())
                yield from self.paginate_records(ctx)
            return

        logging.info(f"Pulling tickers: {ticker_list}")
        for ticker in ticker_list:
            for base_params in query_variants:
                params = {**base_params, self._ticker_param: ticker}
                ctx = dict(context, query_params=params.copy())
                yield from self.paginate_records(ctx)


class AllTickersStream(BaseTickerStream):
    """All tickers across all asset classes (no market filter)."""

    name = "all_tickers"
    primary_keys = ["ticker"]

    schema = th.PropertiesList(
        th.Property("cik", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("name", th.StringType),
        th.Property("source_feed", th.StringType),
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


class BaseTickerPartitionStream(MassiveRestStream):
    _cached_tickers_getter: str | None = None  # e.g. "get_cached_stock_tickers"

    def filter_tickers_from_config(
        self,
        tickers: list[dict[str, t.Any]],
        *,
        key: str = "ticker",
    ) -> list[dict[str, t.Any]]:
        return self._filter_by_tickers(
            tickers,
            allowed=self.get_ticker_list(),
            key=key,
        )

    @property
    def partitions(self):
        if self._cached_tickers_getter is None:
            raise ValueError(
                f"{type(self).__name__} must set _cached_tickers_getter or override partitions."
            )
        getter = getattr(self._tap, self._cached_tickers_getter)
        tickers = self.filter_tickers_from_config(getter())
        return [{"ticker": t["ticker"]} for t in tickers]


class BaseTickerDetailsStream(BaseTickerPartitionStream):
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

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v3/reference/tickers/{ticker}"


class _OhlcMappingMixin:
    ohlc_mapping = {
        "T": "ticker",
        "t": "timestamp",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "vw": "vwap",
        "n": "transactions",
    }
    ohlc_decimal_keys = ("open", "high", "low", "close", "vwap")
    ohlc_include_otc = False
    ohlc_require_timestamp = True
    ohlc_parse_timestamp = True
    ohlc_timestamp_to_iso = False

    def _apply_ohlc_mapping(
        self,
        row: dict,
        *,
        include_otc: bool = False,
        decimal_keys: tuple[str, ...] | None = None,
    ) -> dict:
        mapping = dict(self.ohlc_mapping)
        if include_otc:
            mapping["otc"] = "otc"
        decimal_keys = self.ohlc_decimal_keys if decimal_keys is None else decimal_keys
        for old_key, new_key in mapping.items():
            if old_key in row:
                row[new_key] = row.pop(old_key)
                if decimal_keys and new_key in decimal_keys:
                    row[new_key] = safe_decimal(row[new_key])
        return row

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        if self.ohlc_require_timestamp and "t" not in row and "timestamp" not in row:
            return None
        row = self._apply_ohlc_mapping(
            row,
            include_otc=self.ohlc_include_otc,
            decimal_keys=self.ohlc_decimal_keys,
        )
        if self.ohlc_parse_timestamp and "timestamp" in row:
            timestamp = self.safe_parse_datetime(row["timestamp"])
            row["timestamp"] = (
                timestamp.isoformat()
                if self.ohlc_timestamp_to_iso and timestamp is not None
                else timestamp
            )
        return row


class BaseCustomBarsStream(_OhlcMappingMixin, BaseTickerPartitionStream):
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
    ohlc_include_otc = True
    ohlc_timestamp_to_iso = True
    status_flag_field = "active"
    status_flag_param = "active"
    status_flag_default = True

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
        th.Property("source_feed", th.StringType),
        th.Property("otc", th.BooleanType),
        th.Property("active", th.BooleanType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self._cfg_ending_timestamp_key = "to"

    @property
    def partitions(self):
        if self._cached_tickers_getter is None:
            raise ValueError(
                f"{type(self).__name__} must set _cached_tickers_getter or override partitions."
            )
        getter = getattr(self._tap, self._cached_tickers_getter)
        tickers = self.filter_tickers_from_config(getter())
        partitions: list[dict[str, t.Any]] = []
        for ticker_record in tickers:
            partition: dict[str, t.Any] = {"ticker": ticker_record["ticker"]}
            if isinstance(ticker_record.get("active"), bool):
                partition["active"] = ticker_record["active"]
            partitions.append(partition)
        return partitions

    def build_path_params(self, path_params: dict) -> str:
        keys = ["multiplier", "timespan", "from", self._cfg_ending_timestamp_key]
        return "/" + "/".join(str(path_params[k]) for k in keys if k in path_params)

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        path_params = self.build_path_params(context.get("path_params"))
        return f"{self.url_base}/v2/aggs/ticker/{ticker}/range{path_params}"

    def _status_flag_query_params(self) -> dict[str, t.Any]:
        if self.status_flag_param in self.query_params:
            return self.query_params

        try:
            raw_config = self.config
        except AttributeError:
            raw_config = getattr(self, "_config", {})

        if not hasattr(raw_config, "get"):
            return {}

        selector_keys = getattr(self, "ticker_selector_keys", ())
        for selector_key in selector_keys:
            cfg = raw_config.get(selector_key, {})
            if not isinstance(cfg, dict):
                continue
            selector_query_params = cfg.get("query_params", {})
            if isinstance(selector_query_params, dict):
                return selector_query_params

        return {}

    @staticmethod
    def _coerce_status_flag(value: t.Any) -> bool | None:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y"}:
                return True
            if lowered in {"false", "0", "no", "n"}:
                return False
        if isinstance(value, int):
            if value == 1:
                return True
            if value == 0:
                return False
        return None

    def _resolve_status_flag(self, context: Context | None) -> bool:
        context_value = self._coerce_status_flag(
            (context or {}).get(self.status_flag_field)
        )
        if context_value is not None:
            return context_value

        query_params = self._status_flag_query_params()
        query_value = self._coerce_status_flag(query_params.get(self.status_flag_param))
        if query_value is not None:
            return query_value

        return self.status_flag_default

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        if "t" not in row and "timestamp" not in row:
            return None
        row = self._apply_ohlc_mapping(row, include_otc=self.ohlc_include_otc)
        row["ticker"] = context.get(self._ticker_param)
        if not self.ohlc_include_otc:
            row.pop("otc", None)
        row["timestamp"] = self.safe_parse_datetime(row["timestamp"]).isoformat()
        row[self.status_flag_field] = self._resolve_status_flag(context)
        return row


class BaseDailyMarketSummaryStream(_OhlcMappingMixin, MassiveRestStream):
    primary_keys = ["timestamp", "ticker"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    ohlc_include_otc = True
    ohlc_decimal_keys: tuple[str, ...] = ()

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


class BaseDailyTickerSummaryStream(BaseTickerPartitionStream):
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
    ).to_dict()

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


class BasePreviousDayBarSummaryStream(_OhlcMappingMixin, BaseTickerPartitionStream):
    """Retrieve the previous trading day's OHLCV data for a specified ticker.
    Not really useful given we have the other streams.
    """

    primary_keys = ["timestamp", "ticker"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    ohlc_include_otc = True

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

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v2/aggs/ticker/{ticker}/prev"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        if "t" not in row and "timestamp" not in row:
            return None
        row = self._apply_ohlc_mapping(row, include_otc=True)
        if "timestamp" in row:
            row["timestamp"] = self.safe_parse_datetime(row["timestamp"])
        return row


class BaseTickerTypesStream(MassiveRestStream):
    """Base class for ticker types. Set _asset_class in subclasses to filter by asset class."""

    primary_keys = ["code"]
    _asset_class = None
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("asset_class", th.StringType),
        th.Property("code", th.StringType),
        th.Property("description", th.StringType),
        th.Property("locale", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v3/reference/tickers/types"


class AllTickerTypesStream(BaseTickerTypesStream):
    """All ticker types across all asset classes (no asset_class filter)."""

    name = "ticker_types"


class BaseExchangesStream(MassiveRestStream):
    """Base class for exchanges. Set _asset_class in subclasses to filter by asset class."""

    primary_keys = ["id"]
    _asset_class = None
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("asset_class", th.StringType),
        th.Property("locale", th.StringType),
        th.Property("name", th.StringType),
        th.Property("acronym", th.StringType),
        th.Property("mic", th.StringType),
        th.Property("operating_mic", th.StringType),
        th.Property("participant_id", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v3/reference/exchanges"


class AllExchangesStream(BaseExchangesStream):
    """All exchanges across all asset classes (no asset_class filter)."""

    name = "exchanges"


class BaseConditionCodesStream(MassiveRestStream):
    """Condition Codes Stream"""

    name = "condition_codes"

    primary_keys = ["id", "asset_class", "type"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("abbreviation", th.StringType),
        th.Property(
            "asset_class", th.StringType, enum=["stocks", "options", "crypto", "fx"]
        ),
        th.Property("data_types", th.ArrayType(th.StringType)),
        th.Property("description", th.StringType),
        th.Property("exchange", th.IntegerType),
        th.Property("legacy", th.BooleanType),
        th.Property("name", th.StringType),
        th.Property(
            "sip_mapping",
            th.ObjectType(
                th.Property("CTA", th.StringType),
                th.Property("OPRA", th.StringType),
                th.Property("UTP", th.StringType),
            ),
        ),
        th.Property(
            "type",
            th.StringType,
        ),
        th.Property(
            "update_rules",
            th.ObjectType(
                th.Property(
                    "consolidated",
                    th.ObjectType(
                        th.Property("updates_high_low", th.BooleanType),
                        th.Property("updates_open_close", th.BooleanType),
                        th.Property("updates_volume", th.BooleanType),
                    ),
                ),
                th.Property(
                    "market_center",
                    th.ObjectType(
                        th.Property("updates_high_low", th.BooleanType),
                        th.Property("updates_open_close", th.BooleanType),
                        th.Property("updates_volume", th.BooleanType),
                    ),
                ),
            ),
        ),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v3/reference/conditions"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row = super().post_process(row, context)
        if row is None:
            return None

        sip_mapping = row.get("sip_mapping")
        if isinstance(sip_mapping, dict):
            sip_key_map = {
                "cta": "CTA",
                "c_t_a": "CTA",
                "opra": "OPRA",
                "o_p_r_a": "OPRA",
                "utp": "UTP",
                "u_t_p": "UTP",
            }
            normalized_sip_mapping = {}
            for key, value in sip_mapping.items():
                mapped_key = sip_key_map.get(key)
                if mapped_key and value is not None:
                    normalized_sip_mapping[mapped_key] = value
            row["sip_mapping"] = normalized_sip_mapping

        return row


class AllConditionCodesStream(BaseConditionCodesStream):
    """All condition codes across all asset classes (no asset_class filter)."""

    name = "condition_codes"


class _SnapshotNormalizationMixin:
    _last_quote_slugs = ("lastquote",)
    _last_trade_slugs = ("lasttrade",)
    _ohlc_slugs = ("day", "min", "prevday", "lastminute")

    last_quote_map = {
        "P": "ask_price",
        "S": "ask_size",
        "p": "bid_price",
        "s": "bid_size",
        "T": "ticker",
        "t": "timestamp",
        "X": "ask_exchange",
        "x": "bid_exchange",
        "a": "ask_price",
        "b": "bid_price",
        "i": "indicators",
        "c": "conditions",
        "f": "trf_timestamp",
        "q": "sequence_number",
        "y": "participant_timestamp",
        "z": "tape",
    }

    last_trade_map = {
        "p": "price",
        "s": "size",
        "T": "ticker",
        "t": "timestamp",
        "x": "exchange",
        "X": "exchange",
        "c": "conditions",
        "i": "id",
        "e": "correction",
        "f": "trf_timestamp",
        "q": "sequence_number",
        "r": "trf_id",
        "y": "participant_timestamp",
        "z": "tape",
    }

    _ohlc_map = {
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "vw": "vwap",
        "av": "accumulated_volume",
        "n": "transactions",
        "t": "timestamp",
        "otc": "otc",
    }

    @staticmethod
    def _slug(key: str) -> str:
        return key.replace("_", "").lower()

    @classmethod
    def _apply_mapping(cls, obj: dict, mapping: dict) -> None:
        for old_key, new_key in mapping.items():
            if old_key in obj:
                if new_key not in obj:
                    obj[new_key] = obj[old_key]
                obj.pop(old_key, None)

    def _normalize_last_quote(self, payload: dict) -> None:
        has_upper_x = "X" in payload
        has_lower_x = "x" in payload
        for old_key, new_key in self.last_quote_map.items():
            if old_key not in payload:
                continue
            if old_key in ("x", "X") and not (has_upper_x and has_lower_x):
                new_key = "exchange"
            if new_key not in payload:
                payload[new_key] = payload[old_key]
            payload.pop(old_key, None)

    def _normalize_last_trade(self, payload: dict) -> None:
        for old_key, new_key in self.last_trade_map.items():
            if old_key not in payload:
                continue
            if new_key not in payload:
                payload[new_key] = payload[old_key]
            payload.pop(old_key, None)

    def _normalize_snapshot_fields(self, row: Record) -> None:
        for key in list(row.keys()):
            if not isinstance(key, str):
                continue
            slug = self._slug(key)
            payload = row.get(key)
            if not isinstance(payload, dict):
                continue
            if slug in self._last_quote_slugs:
                self._normalize_last_quote(payload)
            elif slug in self._last_trade_slugs:
                self._normalize_last_trade(payload)
            elif slug in self._ohlc_slugs:
                self._apply_mapping(payload, self._ohlc_map)

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        self._normalize_snapshot_fields(row)
        return super().post_process(row, context)


class _TodaysChangePercentMixin:
    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row = super().post_process(row, context)
        self._apply_todays_change_percent(row)
        return row

    @staticmethod
    def _apply_todays_change_percent(row: Record) -> None:
        if "todays_change_perc" in row:
            row["todays_change_percent"] = row.pop("todays_change_perc")
        elif "todaysChangePerc" in row:
            row["todays_change_percent"] = row.pop("todaysChangePerc")


class UnifiedSnapshotStream(_SnapshotNormalizationMixin, MassiveRestStream):
    """Retrieve unified snapshots of market data for multiple asset classes including stocks, options, forex,
    and cryptocurrencies in a single request. Not really useful given we have the other streams.
    """

    name = "unified_snapshot"


class BaseTopMarketMoversStream(
    _SnapshotNormalizationMixin, _TodaysChangePercentMixin, MassiveRestStream
):
    """Retrieve snapshot data highlighting the top 20 gainers or losers
    Gainers are <stocks, forex, etc> with the largest percentage increase since the previous day’s close, and losers are those
    with the largest percentage decrease.
    """

    primary_keys = ["updated", "ticker"]
    replication_key = "updated"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = False

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("updated", th.IntegerType),
        th.Property("ticker", th.StringType),
        th.Property(
            "day",
            th.ObjectType(
                th.Property("o", th.NumberType),
                th.Property("h", th.NumberType),
                th.Property("l", th.NumberType),
                th.Property("c", th.NumberType),
                th.Property("v", th.IntegerType),
                th.Property("vw", th.NumberType),
            ),
        ),
        th.Property(
            "last_quote",
            th.ObjectType(
                th.Property("P", th.NumberType),
                th.Property("S", th.IntegerType),
                th.Property("p", th.NumberType),
                th.Property("s", th.IntegerType),
                th.Property("t", th.IntegerType),
            ),
        ),
        th.Property(
            "last_trade",
            th.ObjectType(
                th.Property("c", th.ArrayType(th.IntegerType)),
                th.Property("i", th.StringType),
                th.Property("p", th.NumberType),
                th.Property("s", th.IntegerType),
                th.Property("t", th.IntegerType),
                th.Property("x", th.IntegerType),
            ),
        ),
        th.Property(
            "min",
            th.ObjectType(
                th.Property("av", th.IntegerType),
                th.Property("o", th.NumberType),
                th.Property("h", th.NumberType),
                th.Property("l", th.NumberType),
                th.Property("c", th.NumberType),
                th.Property("v", th.IntegerType),
                th.Property("vw", th.NumberType),
                th.Property("n", th.IntegerType),
                th.Property("t", th.IntegerType),
            ),
        ),
        th.Property(
            "prev_day",
            th.ObjectType(
                th.Property("o", th.NumberType),
                th.Property("h", th.NumberType),
                th.Property("l", th.NumberType),
                th.Property("c", th.NumberType),
                th.Property("v", th.IntegerType),
                th.Property("vw", th.NumberType),
            ),
        ),
        th.Property("todays_change", th.NumberType),
        th.Property("todays_change_percent", th.NumberType),
        th.Property("fmv", th.NumberType),
    ).to_dict()

    def _get_direction(self, context: Context | None) -> str:
        """Get direction from context (when looping) or path_params (when configured)."""
        return (
            context.get("direction") if context else self.path_params.get("direction")
        )

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        configured_direction = self.path_params.get("direction")
        if not configured_direction or str(configured_direction).lower() == "both":
            directions = ["gainers", "losers"]
        else:
            directions = [configured_direction]

        for direction in directions:
            url = self.get_url(context={"direction": direction})
            try:
                response = self.get_response(url=url, query_params=self.query_params)
            except requests.exceptions.HTTPError as err:
                if err.response is not None and err.response.status_code == 403:
                    self.logger.warning(
                        f"Access denied (403) for top market movers direction={direction}; skipping."
                    )
                    continue
                raise

            if response is None:
                continue

            response_data = response.json()
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

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row = super().post_process(row, context)
        return row


class _NanosecondIncrementalMixin:
    """Use integer nanoseconds for incremental replication keys."""

    def get_starting_replication_key_value(
        self, context: Context | None
    ) -> t.Any | None:
        if self.replication_method != "INCREMENTAL" or not self.replication_key:
            return None

        state = self.get_context_state(context)
        state_replication_value = (
            state.get("replication_key_value", state.get("starting_replication_value"))
            if state
            else None
        )
        cfg_value = self._cfg_starting_timestamp_value
        start_date_config = self.config.get("start_date")

        def _to_unix(value):
            if value is None:
                return None
            dt = self.safe_parse_datetime(value)
            if dt and self._unix_timestamp_unit:
                return self.iso_to_unix_timestamp(
                    dt.isoformat(), self._unix_timestamp_unit
                )
            if isinstance(value, (int, float, Decimal)):
                return int(value)
            return None

        state_unix = _to_unix(state_replication_value)
        cfg_unix = _to_unix(cfg_value)
        start_date_unix = _to_unix(start_date_config)

        candidates = [
            v for v in (state_unix, cfg_unix, start_date_unix) if v is not None
        ]
        if candidates:
            return max(candidates)
        return None


class BaseTradeStream(_NanosecondIncrementalMixin, BaseTickerPartitionStream):
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
        th.Property("sip_timestamp", th.IntegerType),
        th.Property("participant_timestamp", th.IntegerType),
        th.Property("price", th.NumberType),
        th.Property("size", th.NumberType),
        th.Property("tape", th.IntegerType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("conditions", th.ArrayType(th.IntegerType())),
        th.Property("correction", th.IntegerType()),
        th.Property("trf_id", th.IntegerType),
        th.Property("trf_timestamp", th.IntegerType),
        th.Property("exchange", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v3/trades/{ticker}"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row[self.replication_key] = safe_int(row.get(self.replication_key))
        row["exchange"] = safe_int(row["exchange"])
        if "participant_timestamp" in row:
            row["participant_timestamp"] = safe_int(row["participant_timestamp"])
        if "trf_timestamp" in row:
            row["trf_timestamp"] = safe_int(row["trf_timestamp"])
        if "correction" in row:
            row["correction"] = safe_int(row["correction"])
        if "conditions" in row and isinstance(row["conditions"], list):
            row["conditions"] = [
                condition
                for condition in (
                    safe_int(condition) for condition in row["conditions"]
                )
                if condition is not None
            ]

        row["price"] = safe_decimal(row["price"])
        return row


class _LastQuoteTradeMappingMixin:
    last_trade_map = {
        "T": "ticker",
        "c": "conditions",
        "e": "correction",
        "f": "trf_timestamp",
        "i": "id",
        "p": "price",
        "q": "sequence_number",
        "r": "trf_id",
        "s": "size",
        "t": "sip_timestamp",
        "x": "exchange",
        "y": "participant_timestamp",
        "z": "tape",
    }

    last_quote_map = {
        "T": "ticker",
        "f": "trf_timestamp",
        "q": "sequence_number",
        "t": "sip_timestamp",
        "y": "participant_timestamp",
        "P": "ask_price",
        "S": "ask_size",
        "X": "ask_exchange",
        "c": "conditions",
        "i": "indicators",
        "p": "bid_price",
        "s": "bid_size",
        "x": "bid_exchange",
        "z": "tape",
    }

    @staticmethod
    def _apply_key_map(row: Record, mapping: dict) -> dict:
        mapped = {}
        for key, value in row.items():
            mapped[mapping.get(key, key)] = value
        return mapped


class BaseLastTradeStream(
    _LastQuoteTradeMappingMixin, _NanosecondIncrementalMixin, BaseTickerPartitionStream
):
    primary_keys = ["ticker", "sip_timestamp", "sequence_number"]
    replication_key = "sip_timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    _use_cached_tickers_default = True
    _unix_timestamp_unit = "ns"

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("trf_timestamp", th.IntegerType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("sip_timestamp", th.IntegerType),
        th.Property("participant_timestamp", th.IntegerType),
        th.Property("conditions", th.ArrayType(th.IntegerType)),
        th.Property("correction", th.IntegerType),
        th.Property("id", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("trf_id", th.IntegerType),
        th.Property("size", th.NumberType),
        th.Property("exchange", th.IntegerType),
        th.Property("tape", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v2/last/trade/{ticker}"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row = self._apply_key_map(row, self.last_trade_map)
        if row.get("sip_timestamp") is not None:
            row["sip_timestamp"] = safe_int(row["sip_timestamp"])
        return row


class BaseQuoteStream(_NanosecondIncrementalMixin, BaseTickerPartitionStream):
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
        th.Property("participant_timestamp", th.IntegerType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("sip_timestamp", th.IntegerType),
        th.Property("tape", th.IntegerType),
        th.Property("trf_timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v3/quotes/{ticker}"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row["ticker"] = context.get(self._ticker_param)
        row[self.replication_key] = safe_int(row.get(self.replication_key))
        return row


class BaseLastQuoteStream(
    _LastQuoteTradeMappingMixin, _NanosecondIncrementalMixin, BaseTickerPartitionStream
):
    primary_keys = ["ticker", "sip_timestamp", "sequence_number"]
    replication_key = "sip_timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = True
    _unix_timestamp_unit = "ns"

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("trf_timestamp", th.IntegerType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("sip_timestamp", th.IntegerType),
        th.Property("participant_timestamp", th.IntegerType),
        th.Property("ask_price", th.NumberType),
        th.Property("ask_size", th.IntegerType),
        th.Property("ask_exchange", th.IntegerType),
        th.Property("conditions", th.ArrayType(th.IntegerType)),
        th.Property("indicators", th.ArrayType(th.IntegerType)),
        th.Property("bid_price", th.NumberType),
        th.Property("bid_size", th.IntegerType),
        th.Property("bid_exchange", th.IntegerType),
        th.Property("tape", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v2/last/nbbo/{ticker}"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row = self._apply_key_map(row, self.last_quote_map)
        row["ticker"] = row.get("ticker") or context.get(self._ticker_param)
        if row.get("sip_timestamp") is not None:
            row["sip_timestamp"] = safe_int(row["sip_timestamp"])
        return row


class BaseIndicatorStream(BaseTickerPartitionStream):
    primary_keys = ["timestamp", "ticker", "indicator", "series_window_timespan"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = True
    _api_expects_unix_timestamp = True
    _unix_timestamp_unit = "ms"
    _ticker_in_path_params = True

    indicator_type = None  # Must be set by subclass: "sma", "ema", "macd", "rsi"

    @staticmethod
    def _build_schema(include_macd_fields: bool = False) -> dict:
        props = [
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
        ]
        if include_macd_fields:
            props.extend(
                [
                    th.Property("signal", th.NumberType),
                    th.Property("histogram", th.NumberType),
                ]
            )
        return th.PropertiesList(*props).to_dict()

    schema = _build_schema(False)

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
            record = {
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
            if self.indicator_type == "macd":
                record["signal"] = safe_decimal(value.get("signal"))
                record["histogram"] = safe_decimal(value.get("histogram"))
            yield record
