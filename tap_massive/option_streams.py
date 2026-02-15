"""Options stream classes for tap-massive."""

from __future__ import annotations

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
    BaseTickerTypesStream,
    BaseTradeStream,
    _SnapshotNormalizationMixin,
)
from tap_massive.client import MassiveRestStream, OptionalTickerPartitionStream


class OptionsContractsStream(BaseTickerStream):
    """Stream for retrieving all options contracts."""

    name = "options_contracts"
    primary_keys = ["ticker"]
    market = "options"
    _ticker_param = "underlying_ticker"
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

    def get_ticker_list(self) -> list[str] | None:
        """Return None to fetch ALL contracts regardless of option_tickers.select_tickers.

        OptionsContractsStream needs ALL contracts (including expired) for complete
        historical coverage. The select_tickers filter in option_tickers config should
        only apply to bars/snapshot streams, not to the contracts table itself.
        """
        return None

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/reference/options/contracts"


class OptionsTickerPartitionStream(BaseTickerPartitionStream):
    """Partitions by underlying stock ticker, fetching filtered contracts per underlying.

    Instead of caching all option contracts globally (which OOMs with expired contracts),
    this partitions by underlying stock ticker (~7000) and fetches filtered contracts
    per underlying inside get_records(). Moneyness and DTE filters are applied via
    option_tickers.other_params config.
    """

    state_partitioning_keys = ["underlying"]

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        option_cfg = self.config.get("option_tickers", {})
        stock_tickers = self._apply_select_tickers_filter(
            self._tap.get_cached_stock_tickers(),
            option_cfg.get("select_tickers"),
        )
        return [{"underlying": t["ticker"]} for t in stock_tickers]

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
                "query_params": base_query_params.copy(),
                "path_params": base_path_params.copy(),
            }
            yield from self.paginate_records(contract_ctx)


class OptionsCustomBarsStream(OptionsTickerPartitionStream, BaseCustomBarsStream):
    ohlc_include_otc = False

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

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
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
            "ticker",
            "conditions",
            "correction",
            "exchange",
            "participant_timestamp",
            "price",
            "sip_timestamp",
            "size",
        }
        row["ticker"] = row.get("ticker") or context.get(self._ticker_param)
        return {k: row.get(k) for k in allowed if k in row}


class OptionsQuoteStream(OptionsTickerPartitionStream, BaseQuoteStream):
    """Stream for retrieving options quote data."""

    name = "options_quotes"

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("ask_exchange", th.IntegerType),
        th.Property("ask_price", th.NumberType),
        th.Property("ask_size", th.NumberType),
        th.Property("bid_exchange", th.IntegerType),
        th.Property("bid_price", th.NumberType),
        th.Property("bid_size", th.NumberType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("sip_timestamp", th.IntegerType),
    ).to_dict()

    def post_process(self, row, context: Context | None = None):
        row = super().post_process(row, context)
        allowed = {
            "ticker",
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


class OptionsTickerTypesStream(BaseTickerTypesStream):
    """Options ticker types."""

    name = "options_ticker_types"
    _asset_class = "options"


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
        return [
            {self._ticker_param: t["ticker"]}
            for t in self._tap.get_cached_stock_tickers()
        ]

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/snapshot/options/{context.get('path_params').get(self._ticker_param)}"

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
