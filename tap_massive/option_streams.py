"""Options stream classes for tap-massive."""

from __future__ import annotations

import copy
import json
import logging
import typing as t

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.base_streams import (
    BaseConditionCodesStream,
    BaseCustomBarsStream,
    BaseDailyTickerSummaryStream,
    BaseExchangesStream,
    BaseIndicatorStream,
    BaseLastTradeStream,
    BasePreviousDayBarSummaryStream,
    BaseQuoteStream,
    BaseTickerPartitionStream,
    BaseTickerStream,
    BaseTradeStream,
    _SnapshotNormalizationMixin,
)
from tap_massive.client import MassiveRestStream, OptionalTickerPartitionStream
from tap_massive.quote_update_bar_streams import (
    QuoteUpdateBar1HourStream,
    QuoteUpdateBar1MinuteStream,
    QuoteUpdateBar1SecondStream,
    QuoteUpdateBar5MinuteStream,
    QuoteUpdateBar30MinuteStream,
    QuoteUpdateBar30SecondStream,
    QuoteUpdateBarStream,
)

OPTIONS_QUOTE_UPDATE_BAR_SCHEMA = th.PropertiesList(
    th.Property("option_ticker", th.StringType),
    th.Property("underlying_ticker", th.StringType),
    th.Property("ask_exchange", th.IntegerType),
    th.Property("ask_price", th.NumberType),
    th.Property("ask_size", th.NumberType),
    th.Property("bid_exchange", th.IntegerType),
    th.Property("bid_price", th.NumberType),
    th.Property("bid_size", th.NumberType),
    th.Property("conditions", th.ArrayType(th.IntegerType)),
    th.Property("indicators", th.ArrayType(th.IntegerType)),
    th.Property("participant_timestamp", th.IntegerType),
    th.Property(
        "sequence_number",
        th.IntegerType(
            minimum=0,
            maximum=9_223_372_036_854_775_807,
        ),
    ),
    th.Property("sip_timestamp", th.IntegerType),
    th.Property("tape", th.IntegerType),
    th.Property("trf_timestamp", th.IntegerType),
    th.Property("window_start", th.IntegerType),
).to_dict()


def _rename_to_option_ticker(row: dict, context: dict | None) -> dict:
    """Rename ticker -> option_ticker and add underlying_ticker from context."""
    if row:
        row["option_ticker"] = row.pop("ticker", None)
        row["underlying_ticker"] = (
            context.get("underlying") or context.get("underlying_ticker")
            if context
            else None
        )
    return row


class OptionsContractsStream(BaseTickerStream):
    """Stream for retrieving all options contracts."""

    name = "options_contracts"
    primary_keys = ["ticker"]
    market = "options"
    _ticker_param = "underlying_ticker"
    _boolean_flag_key = "expired"
    _set_market_query_param = False  # This endpoint doesn't accept market param
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("contract_type", th.StringType),
        th.Property("expiration_date", th.StringType),
        th.Property("strike_price", th.NumberType),
        th.Property("underlying_ticker", th.StringType),
        th.Property("exercise_style", th.StringType),
        th.Property("shares_per_contract", th.NumberType),
        th.Property("cfi", th.StringType),
        th.Property("correction", th.IntegerType),
        th.Property("primary_exchange", th.StringType),
        th.Property(
            "additional_underlyings",
            th.ArrayType(
                th.ObjectType(
                    th.Property("amount", th.NumberType),
                    th.Property("type", th.StringType),
                    th.Property("underlying", th.StringType),
                )
            ),
        ),
    ).to_dict()

    # Prefer shared options ticker selector, then fall back to market/name defaults.
    ticker_selector_keys = ("option_tickers",)

    def _get_boolean_flag_mode(self) -> str | None:
        return (
            self.config.get("option_tickers", {})
            .get("other_params", {})
            .get(self._boolean_flag_key)
        )

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/reference/options/contracts"

    def get_records(
        self, context: dict[str, t.Any] | None
    ) -> t.Iterable[dict[str, t.Any]]:
        """Yield contracts per underlying via the shared per-underlying cache.

        Uses get_option_contracts_for_underlying() so running this stream
        naturally warms the same L1/L2 cache that downstream streams
        (bars, snapshots, trades) read from.
        """
        underlyings = self.get_ticker_list()
        if underlyings is None:
            underlyings = [t["ticker"] for t in self._tap.get_cached_stock_tickers()]

        for underlying in underlyings:
            contracts = self._tap.get_option_contracts_for_underlying(underlying)
            for contract in contracts:
                yield contract


class ActiveOptionsContractsStream(OptionsContractsStream):
    """Stream for retrieving only active (non-expired) options contracts."""

    name = "active_options_contracts"


class OptionsTickerPartitionStream(BaseTickerPartitionStream):
    """Partitions by underlying stock ticker, fetching filtered contracts per underlying.

    Instead of caching all option contracts globally (which OOMs with expired contracts),
    this partitions by underlying stock ticker (~7000) and fetches filtered contracts
    per underlying inside get_records(). Moneyness and DTE filters are applied via
    option_tickers.other_params config.

    All subclasses automatically get option_ticker + underlying_ticker instead of ticker
    in their schemas, primary_keys, and output records via __init_subclass__ + post_process.
    """

    ticker_selector_keys = ("option_tickers",)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        schema = cls.__dict__.get("schema")
        if schema is None:
            parent_schema = getattr(cls, "schema", None)
            if parent_schema and isinstance(parent_schema, dict):
                schema = copy.deepcopy(parent_schema)
                cls.schema = schema
        if schema and isinstance(schema, dict):
            props = schema.get("properties", {})
            if "ticker" in props and "option_ticker" not in props:
                props["option_ticker"] = props.pop("ticker")
                if "underlying_ticker" not in props:
                    props["underlying_ticker"] = {"type": ["string", "null"]}
        pks = getattr(cls, "primary_keys", None)
        if pks and "ticker" in pks:
            cls.primary_keys = ["option_ticker" if k == "ticker" else k for k in pks]

    def post_process(self, row, context=None):
        row = super().post_process(row, context)
        return _rename_to_option_ticker(row, context)

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        selected_underlyings = self.get_ticker_list()
        if selected_underlyings is not None:
            return [{"underlying": ticker} for ticker in selected_underlyings]
        return [
            {"underlying": t["ticker"]} for t in self._tap.get_cached_stock_tickers()
        ]

    def get_records(
        self, context: dict[str, t.Any] | None
    ) -> t.Iterable[dict[str, t.Any]]:
        underlying = (context or {}).get("underlying")
        if not underlying:
            return

        contracts = self._tap.get_option_contracts_for_underlying(underlying)
        if not contracts:
            logging.info(
                f"No filtered contracts for {underlying} in {self.name}, skipping."
            )
            return

        # SDK can't partition ~12.5M contracts, so state is per-underlying.
        # Example: Without reset, AAPL 150C bars to 2025-12-31 would cause AAPL 200C
        # to skip its entire history since the shared bookmark is already at 2025 for ticker AAPL.
        state = self.get_context_state(context)
        initial_bookmark = state.copy() if state else {}

        _, base_query_params, base_path_params = self._prepare_context_and_params(
            dict(context)
        )

        for contract in contracts:
            # Reset state to initial bookmark before each contract
            if state is not None:
                state.clear()
                state.update(initial_bookmark)

            contract_ctx: dict[str, t.Any] = {
                self._ticker_param: contract["ticker"],
                "underlying": underlying,
                "expired": contract.get("expired"),
                "query_params": base_query_params.copy(),
                "path_params": base_path_params.copy(),
            }
            yield from self.paginate_records(contract_ctx)


class OptionsCustomBarsStream(OptionsTickerPartitionStream, BaseCustomBarsStream):
    ohlc_include_otc = False
    status_flag_field = "expired"
    status_flag_param = "expired"
    status_flag_default = False

    def _status_flag_query_params(self) -> dict[str, t.Any]:
        return self.config.get("option_tickers", {}).get("query_params", {})

    def _resolve_status_flag(self, context: Context | None) -> bool:
        context_value = self._coerce_status_flag((context or {}).get("expired"))
        if context_value is not None:
            return context_value
        return super()._resolve_status_flag(context)

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
        th.Property("expired", th.BooleanType),
    ).to_dict()


class OptionsContractOverviewStream(OptionsTickerPartitionStream):
    """Stream for retrieving detailed information about options contracts."""

    name = "options_contract_overview"
    primary_keys = ["ticker"]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("contract_type", th.StringType),
        th.Property("exercise_style", th.StringType),
        th.Property("expiration_date", th.StringType),
        th.Property("strike_price", th.NumberType),
        th.Property("shares_per_contract", th.NumberType),
        th.Property("underlying_ticker", th.StringType),
        th.Property("primary_exchange", th.StringType),
        th.Property("cfi", th.StringType),
        th.Property("correction", th.IntegerType),
        th.Property(
            "additional_underlyings",
            th.ArrayType(
                th.ObjectType(
                    th.Property("amount", th.NumberType),
                    th.Property("type", th.StringType),
                    th.Property("underlying", th.StringType),
                )
            ),
        ),
    ).to_dict()

    def get_url(self, context: Context) -> str:
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v3/reference/options/contracts/{ticker}"


class OptionsContractSnapshotStream(
    _SnapshotNormalizationMixin, OptionsTickerPartitionStream
):
    """Stream for retrieving options contract snapshot data."""

    name = "options_contract_snapshot"
    primary_keys = ["ticker"]
    _use_cached_tickers_default = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("break_even_price", th.NumberType),
        th.Property("fmv", th.NumberType),
        th.Property("fmv_last_updated", th.IntegerType),
        th.Property("implied_volatility", th.NumberType),
        th.Property("open_interest", th.NumberType),
        th.Property(
            "greeks",
            th.ObjectType(
                th.Property("delta", th.NumberType),
                th.Property("gamma", th.NumberType),
                th.Property("theta", th.NumberType),
                th.Property("vega", th.NumberType),
            ),
        ),
        th.Property(
            "last_quote",
            th.ObjectType(
                th.Property("ask", th.NumberType),
                th.Property("ask_size", th.NumberType),
                th.Property("bid", th.NumberType),
                th.Property("bid_size", th.NumberType),
                th.Property("ask_exchange", th.IntegerType),
                th.Property("bid_exchange", th.IntegerType),
                th.Property("last_updated", th.IntegerType),
                th.Property("midpoint", th.NumberType),
                th.Property("timeframe", th.StringType),
            ),
        ),
        th.Property(
            "last_trade",
            th.ObjectType(
                th.Property("price", th.NumberType),
                th.Property("size", th.IntegerType),
                th.Property("exchange", th.IntegerType),
                th.Property("sip_timestamp", th.IntegerType),
                th.Property("conditions", th.ArrayType(th.IntegerType)),
                th.Property("timeframe", th.StringType),
            ),
        ),
        th.Property(
            "day",
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
                th.Property("last_updated", th.IntegerType),
            ),
        ),
        th.Property(
            "details",
            th.ObjectType(
                th.Property("contract_type", th.StringType),
                th.Property("exercise_style", th.StringType),
                th.Property("expiration_date", th.StringType),
                th.Property("shares_per_contract", th.NumberType),
                th.Property("strike_price", th.NumberType),
                th.Property("ticker", th.StringType),
            ),
        ),
        th.Property(
            "underlying_asset",
            th.ObjectType(
                th.Property("ticker", th.StringType),
                th.Property("price", th.NumberType),
                th.Property("change_to_break_even", th.NumberType),
                th.Property("last_updated", th.IntegerType),
                th.Property("timeframe", th.StringType),
                th.Property("value", th.NumberType),
            ),
        ),
    ).to_dict()

    def get_url(self, context: Context):
        path_params = context.get("path_params", {})
        underlying = path_params.get("underlying")
        option_contract = context.get(self._ticker_param)
        return f"{self.url_base}/v3/snapshot/options/{underlying}/{option_contract}"

    def get_records(
        self, context: dict[str, t.Any] | None
    ) -> t.Iterable[dict[str, t.Any]]:
        """Override to pass underlying through path_params for URL construction."""
        underlying = (context or {}).get("underlying")
        if not underlying:
            return

        contracts = self._tap.get_option_contracts_for_underlying(underlying)
        if not contracts:
            return

        _, base_query_params, base_path_params = self._prepare_context_and_params(
            dict(context)
        )

        for contract in contracts:
            contract_path_params = base_path_params.copy()
            contract_path_params["underlying"] = underlying
            contract_ctx: dict[str, t.Any] = {
                self._ticker_param: contract["ticker"],
                "underlying": underlying,
                "query_params": base_query_params.copy(),
                "path_params": contract_path_params,
            }
            yield from self.paginate_records(contract_ctx)

    def post_process(self, row, context: Context | None = None):
        row = super().post_process(row, context)
        if not row.get("ticker"):
            row["ticker"] = (row.get("details") or {}).get("ticker")
        return row


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
    OptionsTickerPartitionStream, BaseDailyTickerSummaryStream
):
    """Stream for retrieving options daily ticker summary."""

    name = "options_daily_ticker_summary"


class OptionsPreviousDayBarStream(
    OptionsTickerPartitionStream, BasePreviousDayBarSummaryStream
):
    """Stream for retrieving options previous day bar data."""

    name = "options_previous_day_bar"


class OptionsTradeStream(OptionsTickerPartitionStream, BaseTradeStream):
    """Stream for retrieving options trade data."""

    name = "options_trades"
    primary_keys = ["option_ticker", "exchange", "id"]

    schema = th.PropertiesList(
        th.Property("option_ticker", th.StringType),
        th.Property("underlying_ticker", th.StringType),
        th.Property("conditions", th.ArrayType(th.IntegerType)),
        th.Property("correction", th.IntegerType),
        th.Property("exchange", th.IntegerType),
        th.Property("participant_timestamp", th.IntegerType),
        th.Property("price", th.NumberType),
        th.Property("sip_timestamp", th.IntegerType),
        th.Property("size", th.NumberType),
    ).to_dict()

    def post_process(self, row, context: Context | None = None):
        row = super().post_process(row, context)
        allowed = {
            "option_ticker",
            "underlying_ticker",
            "conditions",
            "correction",
            "exchange",
            "participant_timestamp",
            "price",
            "sip_timestamp",
            "size",
        }
        return {k: row.get(k) for k in allowed if k in row}


class OptionsQuoteStream(OptionsTickerPartitionStream, BaseQuoteStream):
    """Stream for retrieving options quote data."""

    name = "options_quotes"
    primary_keys = ["option_ticker", "sip_timestamp", "sequence_number"]

    schema = th.PropertiesList(
        th.Property("option_ticker", th.StringType),
        th.Property("underlying_ticker", th.StringType),
        th.Property("ask_exchange", th.IntegerType),
        th.Property("ask_price", th.NumberType),
        th.Property("ask_size", th.NumberType),
        th.Property("bid_exchange", th.IntegerType),
        th.Property("bid_price", th.NumberType),
        th.Property("bid_size", th.NumberType),
        th.Property(
            "sequence_number",
            th.IntegerType(
                minimum=0,
                maximum=9_223_372_036_854_775_807,
            ),
        ),
        th.Property("sip_timestamp", th.IntegerType),
    ).to_dict()

    def post_process(self, row, context: Context | None = None):
        row = super().post_process(row, context)
        allowed = {
            "option_ticker",
            "underlying_ticker",
            "ask_exchange",
            "ask_price",
            "ask_size",
            "bid_exchange",
            "bid_price",
            "bid_size",
            "sequence_number",
            "sip_timestamp",
        }
        return {k: row.get(k) for k in allowed if k in row}


class OptionsExchangesStream(BaseExchangesStream):
    """Options-specific exchanges."""

    name = "options_exchanges"
    _asset_class = "options"


class OptionsConditionCodesStream(BaseConditionCodesStream):
    """Options-specific condition codes."""

    name = "options_condition_codes"
    _asset_class = "options"


class OptionsSmaStream(OptionsTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving options SMA indicator data."""

    name = "options_sma"
    indicator_type = "sma"


class OptionsEmaStream(OptionsTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving options EMA indicator data."""

    name = "options_ema"
    indicator_type = "ema"


class OptionsMACDStream(OptionsTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving options MACD indicator data."""

    name = "options_macd"
    indicator_type = "macd"
    schema = BaseIndicatorStream._build_schema(True)


class OptionsRSIStream(OptionsTickerPartitionStream, BaseIndicatorStream):
    """Stream for retrieving options RSI indicator data."""

    name = "options_rsi"
    indicator_type = "rsi"


class OptionsLastTradeStream(OptionsTickerPartitionStream, BaseLastTradeStream):
    """Stream for retrieving options last trade data."""

    name = "options_last_trade"


class OptionsChainSnapshotStream(
    _SnapshotNormalizationMixin, OptionalTickerPartitionStream
):
    """Stream for retrieving options chain snapshot data."""

    name = "options_chain_snapshot"
    primary_keys = ["ticker"]
    _ticker_param = "underlyingAsset"
    _use_cached_tickers_default = True
    _ticker_in_path_params = True
    ticker_selector_keys = ("option_tickers",)

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("break_even_price", th.NumberType),
        th.Property("fmv", th.NumberType),
        th.Property("fmv_last_updated", th.IntegerType),
        th.Property("implied_volatility", th.NumberType),
        th.Property("open_interest", th.NumberType),
        th.Property(
            "greeks",
            th.ObjectType(
                th.Property("delta", th.NumberType),
                th.Property("gamma", th.NumberType),
                th.Property("theta", th.NumberType),
                th.Property("vega", th.NumberType),
            ),
        ),
        th.Property(
            "last_quote",
            th.ObjectType(
                th.Property("ask", th.NumberType),
                th.Property("ask_size", th.NumberType),
                th.Property("bid", th.NumberType),
                th.Property("bid_size", th.NumberType),
                th.Property("ask_exchange", th.IntegerType),
                th.Property("bid_exchange", th.IntegerType),
                th.Property("last_updated", th.IntegerType),
                th.Property("midpoint", th.NumberType),
                th.Property("timeframe", th.StringType),
            ),
        ),
        th.Property(
            "last_trade",
            th.ObjectType(
                th.Property("price", th.NumberType),
                th.Property("size", th.IntegerType),
                th.Property("exchange", th.IntegerType),
                th.Property("sip_timestamp", th.IntegerType),
                th.Property("conditions", th.ArrayType(th.IntegerType)),
                th.Property("timeframe", th.StringType),
            ),
        ),
        th.Property(
            "day",
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
                th.Property("last_updated", th.IntegerType),
            ),
        ),
        th.Property(
            "details",
            th.ObjectType(
                th.Property("contract_type", th.StringType),
                th.Property("exercise_style", th.StringType),
                th.Property("expiration_date", th.StringType),
                th.Property("shares_per_contract", th.NumberType),
                th.Property("strike_price", th.NumberType),
                th.Property("ticker", th.StringType),
            ),
        ),
        th.Property(
            "underlying_asset",
            th.ObjectType(
                th.Property("ticker", th.StringType),
                th.Property("price", th.NumberType),
                th.Property("change_to_break_even", th.NumberType),
                th.Property("last_updated", th.IntegerType),
                th.Property("timeframe", th.StringType),
                th.Property("value", th.NumberType),
            ),
        ),
    ).to_dict()

    @property
    def partitions(self):
        selected_underlyings = self.get_ticker_list()
        if selected_underlyings is not None:
            return [{self._ticker_param: ticker} for ticker in selected_underlyings]
        return [
            {self._ticker_param: t["ticker"]}
            for t in self._tap.get_cached_stock_tickers()
        ]

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/snapshot/options/{context.get('path_params').get(self._ticker_param)}"

    @staticmethod
    def _context_dedupe_key(context: t.Any) -> str:
        try:
            return json.dumps(context, sort_keys=True, default=str)
        except Exception:
            return repr(context)

    def _dedupe_duplicate_partition_entries(self) -> int:
        bookmarks = self.tap_state.get("bookmarks", {})
        stream_state = bookmarks.get(self.name)
        if not isinstance(stream_state, dict):
            return 0

        partitions = stream_state.get("partitions")
        if not isinstance(partitions, list) or len(partitions) < 2:
            return 0

        deduped_by_context: dict[str, dict] = {}
        for partition_state in partitions:
            if not isinstance(partition_state, dict):
                continue
            key = self._context_dedupe_key(partition_state.get("context"))
            deduped_by_context[key] = partition_state

        removed = len(partitions) - len(deduped_by_context)
        if removed > 0:
            stream_state["partitions"] = list(deduped_by_context.values())
        return removed

    def get_context_state(self, context: Context | None) -> dict:
        removed = self._dedupe_duplicate_partition_entries()
        if removed:
            logging.warning(
                "Removed %s duplicate options_chain_snapshot partition state entr%s before processing.",
                removed,
                "y" if removed == 1 else "ies",
            )
        try:
            return super().get_context_state(context)
        except ValueError as exc:
            if "duplicate entries for partition" not in str(exc):
                raise
            removed = self._dedupe_duplicate_partition_entries()
            if removed:
                logging.warning(
                    "Removed %s duplicate options_chain_snapshot partition state entr%s after lookup failure. Retrying.",
                    removed,
                    "y" if removed == 1 else "ies",
                )
                return super().get_context_state(context)
            raise

    def post_process(self, row, context: Context | None = None):
        row = super().post_process(row, context)
        if not row.get("ticker"):
            row["ticker"] = (row.get("details") or {}).get("ticker")
        return row


class OptionsUnifiedSnapshotStream(_SnapshotNormalizationMixin, MassiveRestStream):
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
        th.Property(
            "type", th.StringType
        ),  # enum: crypto, fx, indices, options, stocks
        th.Property("name", th.StringType),
        th.Property("market_status", th.StringType),
        th.Property("error", th.StringType),
        th.Property("message", th.StringType),
        th.Property("break_even_price", th.NumberType),
        th.Property("open_interest", th.NumberType),
        th.Property("fmv", th.NumberType),
        th.Property("fmv_last_updated", th.IntegerType),
        th.Property("implied_volatility", th.NumberType),
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
                th.Property("open", th.NumberType),
                th.Property("high", th.NumberType),
                th.Property("low", th.NumberType),
                th.Property("close", th.NumberType),
                th.Property("late_trading_change", th.NumberType),
                th.Property("late_trading_change_percent", th.NumberType),
                th.Property("price", th.NumberType),
                th.Property("regular_trading_change", th.NumberType),
                th.Property("regular_trading_change_percent", th.NumberType),
                th.Property("volume", th.NumberType),
                th.Property("previous_close", th.NumberType),
            ),
        ),
        th.Property(
            "last_quote",
            th.ObjectType(
                th.Property("ask", th.NumberType),
                th.Property("ask_size", th.NumberType),
                th.Property("bid", th.NumberType),
                th.Property("bid_size", th.NumberType),
                th.Property("ask_exchange", th.IntegerType),
                th.Property("bid_exchange", th.IntegerType),
                th.Property("last_updated", th.IntegerType),
                th.Property("midpoint", th.NumberType),
                th.Property("timeframe", th.StringType),
            ),
        ),
        th.Property(
            "last_trade",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("price", th.NumberType),
                th.Property("size", th.IntegerType),
                th.Property("exchange", th.IntegerType),
                th.Property("sip_timestamp", th.IntegerType),
                th.Property("participant_timestamp", th.IntegerType),
                th.Property("last_updated", th.IntegerType),
                th.Property("conditions", th.ArrayType(th.IntegerType)),
                th.Property("timeframe", th.StringType),
            ),
        ),
        th.Property(
            "greeks",
            th.ObjectType(
                th.Property("delta", th.NumberType),
                th.Property("gamma", th.NumberType),
                th.Property("theta", th.NumberType),
                th.Property("vega", th.NumberType),
            ),
        ),
        th.Property(
            "details",
            th.ObjectType(
                th.Property("contract_type", th.StringType),
                th.Property("exercise_style", th.StringType),
                th.Property("expiration_date", th.StringType),
                th.Property("shares_per_contract", th.NumberType),
                th.Property("strike_price", th.NumberType),
            ),
        ),
        th.Property(
            "underlying_asset",
            th.ObjectType(
                th.Property("ticker", th.StringType),
                th.Property("price", th.NumberType),
                th.Property("change_to_break_even", th.NumberType),
                th.Property("last_updated", th.IntegerType),
                th.Property("timeframe", th.StringType),
                th.Property("value", th.NumberType),
            ),
        ),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/snapshot"

    def post_process(self, row, context: Context | None = None):
        row = super().post_process(row, context)
        details = row.get("details")
        if isinstance(details, dict):
            details.pop("ticker", None)
        return row


# --- Quote Update Bars Streams ---


class BaseOptionsQuoteUpdateBar(QuoteUpdateBarStream):
    """Options quote update bars partitioned by underlying stock ticker.

    Partitions by underlying (~7K tickers), loops over contracts per
    underlying inside get_records with state reset per contract.
    """

    primary_keys = ["option_ticker", "window_start"]
    ticker_selector_keys = ("option_tickers",)
    schema = OPTIONS_QUOTE_UPDATE_BAR_SCHEMA

    @property
    def partitions(self) -> list[dict]:
        """Build underlying-level partitions."""
        selected = self.resolve_select_tickers(selector_keys=self.ticker_selector_keys)
        underlyings = selected or [
            t["ticker"] for t in self._tap.get_cached_stock_tickers()
        ]
        return [{"underlying": u} for u in underlyings]

    def get_records(
        self, context: dict[str, t.Any] | None
    ) -> t.Iterable[dict[str, t.Any]]:
        """Fetch update bars for all contracts under an underlying."""
        underlying = (context or {}).get("underlying")
        if not underlying:
            return

        contracts = self._tap.get_option_contracts_for_underlying(underlying)
        if not contracts:
            return

        state = self.get_context_state(context)
        initial_bookmark = state.copy() if state else {}

        for contract in contracts:
            if state is not None:
                state.clear()
                state.update(initial_bookmark)

            contract_ctx = {**context, self._ticker_param: contract["ticker"]}
            yield from super().get_records(contract_ctx)

    def post_process(self, row, context=None):
        row = super().post_process(row, context)
        return _rename_to_option_ticker(row, context)


class OptionsQuoteUpdateBar1SecondStream(
    BaseOptionsQuoteUpdateBar, QuoteUpdateBar1SecondStream
):
    name = "options_quote_update_bars_1_second"


class OptionsQuoteUpdateBar30SecondStream(
    BaseOptionsQuoteUpdateBar, QuoteUpdateBar30SecondStream
):
    name = "options_quote_update_bars_30_second"


class OptionsQuoteUpdateBar1MinuteStream(
    BaseOptionsQuoteUpdateBar, QuoteUpdateBar1MinuteStream
):
    name = "options_quote_update_bars_1_minute"


class OptionsQuoteUpdateBar5MinuteStream(
    BaseOptionsQuoteUpdateBar, QuoteUpdateBar5MinuteStream
):
    name = "options_quote_update_bars_5_minute"


class OptionsQuoteUpdateBar30MinuteStream(
    BaseOptionsQuoteUpdateBar, QuoteUpdateBar30MinuteStream
):
    name = "options_quote_update_bars_30_minute"


class OptionsQuoteUpdateBar1HourStream(
    BaseOptionsQuoteUpdateBar, QuoteUpdateBar1HourStream
):
    name = "options_quote_update_bars_1_hour"
