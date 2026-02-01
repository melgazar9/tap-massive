"""Stream type classes for tap-massive."""

from __future__ import annotations

import typing as t
from dataclasses import asdict
from datetime import datetime

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context, Record

from tap_massive.base_streams import (
    BaseTickerStream,
    BaseCustomBarsStream,
    BaseDailyMarketSummaryStream,
    BaseDailyTickerSummaryStream,
    BaseIndicatorStream,
    BaseLastQuoteStream,
    BaseLastTradeStream,
    BasePreviousDayBarSummaryStream,
    BaseQuoteStream,
    BaseTickerDetailsStream,
    BaseTickerPartitionedStream,
    BaseTopMarketMoversStream,
    BaseTradeStream,
)
from tap_massive.client import OptionalTickerPartitionStream, MassiveRestStream


class StockTickerStream(BaseTickerStream):
    """Fetch all tickers from Massive."""

    name = "stock_tickers"
    market = "stock"

    primary_keys = ["ticker"]
    _ticker_in_path_params = True

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
        th.Property("source_feed", th.StringType),
    ).to_dict()


    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/reference/tickers"


class StockTickerPartitionedStream(BaseTickerPartitionedStream):
    @property
    def partitions(self):
        return [{"ticker": t["ticker"]} for t in self._tap.get_cached_stock_tickers()]


class StockTickerDetailsStream(StockTickerPartitionedStream, BaseTickerDetailsStream):
    name = "stock_ticker_details"


class StockDailyTickerSummaryStream(BaseDailyTickerSummaryStream):
    name = "stock_daily_ticker_summary"

class StockIndicatorStream(StockTickerPartitionedStream, BaseIndicatorStream):
    def get_url(self, context: Context) -> str:
        raise ValueError("URL Must be overridden by subclass.")

class StockDailyMarketSummaryStream(BaseDailyMarketSummaryStream):
    name = "stock_daily_market_summary"

    def get_url(self, context: Context):
        date = context.get("path_params").get("date")
        if date is None:
            date = datetime.today().date().isoformat()
        return f"{self.url_base}/v2/aggs/grouped/locale/us/market/stocks/{date}"


class StockPreviousDayBarSummaryStream(BasePreviousDayBarSummaryStream):
    """Retrieve the previous trading day's OHLCV data for a specified stock ticker.
    Not really useful given we have the other streams."""

    name = "stock_previous_day_bar"


class StockTopMarketMoversStream(BaseTopMarketMoversStream):
    """
    Retrieve snapshot data highlighting the top 20 gainers or losers in the U.S. stock market.
    Gainers are stocks with the largest percentage increase since the previous day’s close, and losers are those
    with the largest percentage decrease. Only tickers with a minimum trading volume of 10,000 are included.
    Snapshot data is cleared daily at 3:30 AM EST and begins repopulating as exchanges report new information,
    typically starting around 4:00 AM EST.
    """

    name = "stock_top_market_movers"

    def get_url(self, context: Context):
        direction = context.get("direction")
        return f"{self.url_base}/v2/snapshot/locale/us/markets/stocks/{direction}"


class StockTradeStream(BaseTradeStream):
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

    name = "stock_trades"


class StockLastTradeStream(BaseLastTradeStream):
    name = "stock_last_trade"


class StockQuoteStream(BaseQuoteStream):
    name = "stock_quotes"


class StockLastQuoteStream(BaseLastQuoteStream):
    name = "stock_last_quote"


class TickerTypesStream(MassiveRestStream):
    """TickerTypesStream - does not require pagination so use Massive's RESTClient() to fetch the data."""

    name = "ticker_types"

    primary_keys = ["code"]

    schema = th.PropertiesList(
        th.Property(
            "asset_class", th.StringType
        ),  # enum: stocks, options, crypto, fx, indices
        th.Property("code", th.StringType),
        th.Property("description", th.StringType),
        th.Property("locale", th.StringType),  # enum: us, global
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        ticker_types = self.client.get_ticker_types()
        for ticker_type in ticker_types:
            tt = asdict(ticker_type)
            yield tt


class RelatedCompaniesStream(StockTickerPartitionedStream):
    name = "related_companies"

    primary_keys = ["ticker"]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("related_company", th.StringType),
        th.Property("request_id", th.StringType),
        th.Property("status", th.StringType),
    ).to_dict()


    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param)
        return f"{self.url_base}/v1/related-companies/{ticker}"

    def post_process(self, row: Record, context: Context | None = None) -> dict | None:
        row["related_company"] = row["ticker"]
        row["ticker"] = context.get(self._ticker_param)
        return row


class StockBars1SecondStream(StockTickerPartitionedStream, BaseCustomBarsStream):
    name = "stock_bars_1_second"


class StockBars30SecondStream(StockTickerPartitionedStream, BaseCustomBarsStream):
    name = "stock_bars_30_second"


class StockBars1MinuteStream(StockTickerPartitionedStream, BaseCustomBarsStream):
    name = "stock_bars_1_minute"


class StockBars5MinuteStream(StockTickerPartitionedStream, BaseCustomBarsStream):
    name = "stock_bars_5_minute"


class StockBars30MinuteStream(StockTickerPartitionedStream, BaseCustomBarsStream):
    name = "stock_bars_30_minute"


class StockBars1HourStream(StockTickerPartitionedStream, BaseCustomBarsStream):
    name = "stock_bars_1_hour"


class StockBars1DayStream(StockTickerPartitionedStream, BaseCustomBarsStream):
    name = "stock_bars_1_day"


class StockBars1WeekStream(StockTickerPartitionedStream, BaseCustomBarsStream):
    name = "stock_bars_1_week"


class StockBars1MonthStream(StockTickerPartitionedStream, BaseCustomBarsStream):
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


class RSIStream(StockIndicatorStream):
    name = "stock_rsi"
    indicator_type = "rsi"

class ExchangesStream(MassiveRestStream):
    """Fetch Exchanges"""

    name = "exchanges"

    primary_keys = ["id"]

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


class ConditionCodesStream(MassiveRestStream):
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
            enum=[
                "sale_condition",
                "quote_condition",
                "sip_generated_flag",
                "financial_status_indicator",
                "short_sale_restriction_indicator",
                "settlement_condition",
                "market_condition",
                "trade_thru_exempt",
                "regular",
                "buy_or_sell_side",
            ],
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


class IPOsStream(OptionalTickerPartitionStream):
    """IPOs Stream"""

    name = "ipos"

    primary_keys = ["listing_date", "ticker"]
    replication_key = "listing_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("announced_date", th.DateType),
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
        th.Property("last_updated", th.DateType),
        th.Property("listing_date", th.DateType),
        th.Property("lot_size", th.NumberType),
        th.Property("lowest_offer_price", th.NumberType),
        th.Property("max_shares_offered", th.NumberType),
        th.Property("min_shares_offered", th.NumberType),
        th.Property("primary_exchange", th.StringType),
        th.Property("security_description", th.StringType),
        th.Property("security_type", th.StringType),
        th.Property("shares_outstanding", th.NumberType),
        th.Property("ticker", th.StringType),
        th.Property("total_offer_size", th.NumberType),
        th.Property("us_code", th.StringType),
    ).to_dict()


    def get_url(self, context: Context = None):
        return f"{self.url_base}/vX/reference/ipos"

    @staticmethod
    def post_process(row: Record, context: Context | None = None) -> dict | None:
        row["ipo_status"] = row["ipo_status"].lower()
        return row


class SplitsStream(OptionalTickerPartitionStream):
    """Splits Stream"""

    name = "splits"

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


class DividendsStream(OptionalTickerPartitionStream):
    """
    Dividends Stream

    NOTE: The Massive API does not supply a true updated_at field, which would be better for incremental syncs.
      Given this, the stream needs to perform periodic full-table syncs in order to catch any missing
      records that may have been revised with a new ex_dividend_date. Either that or just set it to full-refresh
      each time it runs. For now, I'll set the replication_method to FULL_REFRESH in order to capture all changes.
      For analytical purposes, it's important discard ex-dividend dates that have been changed (need to avoid double counting)
    """

    name = "dividends"

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


class TickerEventsStream(StockTickerPartitionedStream):
    """Ticker Events Stream"""

    name = "ticker_events"

    primary_keys = ["date", "cik", "name", "type"]

    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("composite_figi", th.StringType),
        th.Property("date", th.DateType),
        th.Property("type", th.StringType),
        th.Property("ticker", th.StringType),
    ).to_dict()


    def get_url(self, context: Context):
        ticker = context.get(self._ticker_param, {})
        return f"{self.url_base}/vX/reference/tickers/{ticker}/events"

    def parse_response(self, record: dict, context: dict) -> t.Iterable[dict]:
        parent_fields = ["name", "composite_figi", "cik"]
        events = record.get("events", [])
        for event in events:
            flat_event = {pf: record.get(pf) for pf in parent_fields}
            flat_event["date"] = event.get("date")
            flat_event["type"] = event.get("type")
            flat_event["ticker"] = (
                event.get("ticker_change", {}).get("ticker")
                if isinstance(event.get("ticker_change"), dict)
                else None
            )
            yield flat_event


def _financial_metric_property(name: str):
    return th.Property(
        name,
        th.ObjectType(
            th.Property("label", th.StringType),
            th.Property("order", th.IntegerType),
            th.Property("unit", th.StringType),
            th.Property("value", th.NumberType),
            th.Property("source", th.StringType),
            th.Property("derived_from", th.ArrayType(th.StringType)),
            th.Property("formula", th.StringType),
            th.Property("xpath", th.StringType),
        ),
    )


class FinancialsStream(MassiveRestStream):
    """Financials Stream"""

    name = "financials"

    primary_keys = ["cik", "end_date", "timeframe"]
    replication_key = "filing_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("acceptance_datetime", th.DateTimeType),
        th.Property("cik", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("end_date", th.DateType),
        th.Property("filing_date", th.DateType),
        th.Property(
            "financials",
            th.ObjectType(
                th.Property(
                    "comprehensive_income",
                    th.ObjectType(
                        _financial_metric_property("comprehensive_income_loss"),
                        _financial_metric_property("other_comprehensive_income_loss"),
                        _financial_metric_property(
                            "comprehensive_income_loss_attributable_to_parent"
                        ),
                        _financial_metric_property(
                            "comprehensive_income_loss_attributable_to_noncontrolling_interest"
                        ),
                        _financial_metric_property(
                            "other_comprehensive_income_loss_attributable_to_parent"
                        ),
                        _financial_metric_property(
                            "other_comprehensive_income_loss_attributable_to_noncontrolling_interest"
                        ),
                        additional_properties=True,
                    ),
                ),
                th.Property(
                    "income_statement",
                    th.ObjectType(
                        _financial_metric_property("revenues"),
                        _financial_metric_property("cost_of_revenue"),
                        _financial_metric_property("gross_profit"),
                        _financial_metric_property("operating_expenses"),
                        _financial_metric_property("operating_income_loss"),
                        _financial_metric_property(
                            "income_loss_from_continuing_operations_before_tax"
                        ),
                        _financial_metric_property(
                            "income_loss_from_continuing_operations_after_tax"
                        ),
                        _financial_metric_property("net_income_loss"),
                        _financial_metric_property(
                            "net_income_loss_attributable_to_parent"
                        ),
                        _financial_metric_property(
                            "net_income_loss_attributable_to_noncontrolling_interest"
                        ),
                        _financial_metric_property(
                            "net_income_loss_available_to_common_stockholders_basic"
                        ),
                        _financial_metric_property("basic_earnings_per_share"),
                        _financial_metric_property("diluted_earnings_per_share"),
                        _financial_metric_property("basic_average_shares"),
                        _financial_metric_property("diluted_average_shares"),
                        _financial_metric_property(
                            "preferred_stock_dividends_and_other_adjustments"
                        ),
                        _financial_metric_property(
                            "participating_securities_distributed_and_undistributed_earnings_loss_basic"
                        ),
                        _financial_metric_property("benefits_costs_expenses"),
                        _financial_metric_property("depreciation_and_amortization"),
                        _financial_metric_property(
                            "income_tax_expense_benefit_current"
                        ),
                        _financial_metric_property("research_and_development"),
                        _financial_metric_property("costs_and_expenses"),
                        _financial_metric_property(
                            "income_loss_from_equity_method_investments"
                        ),
                        _financial_metric_property(
                            "income_tax_expense_benefit_deferred"
                        ),
                        _financial_metric_property("income_tax_expense_benefit"),
                        _financial_metric_property("other_operating_expenses"),
                        _financial_metric_property("interest_expense_operating"),
                        _financial_metric_property(
                            "income_loss_before_equity_method_investments"
                        ),
                        _financial_metric_property(
                            "selling_general_and_administrative_expenses"
                        ),
                        _financial_metric_property(
                            "income_loss_from_discontinued_operations_net_of_tax"
                        ),
                        _financial_metric_property("nonoperating_income_loss"),
                        _financial_metric_property(
                            "provision_for_loan_lease_and_other_losses"
                        ),
                        _financial_metric_property(
                            "interest_income_expense_after_provision_for_losses"
                        ),
                        _financial_metric_property(
                            "interest_income_expense_operating_net"
                        ),
                        _financial_metric_property("noninterest_income"),
                        _financial_metric_property(
                            "undistributed_earnings_loss_allocated_to_participating_securities_basic"
                        ),
                        _financial_metric_property(
                            "net_income_loss_attributable_to_nonredeemable_noncontrolling_interest"
                        ),
                        _financial_metric_property("interest_and_debt_expense"),
                        _financial_metric_property("other_operating_income_expenses"),
                        _financial_metric_property(
                            "net_income_loss_attributable_to_redeemable_noncontrolling_interest"
                        ),
                        _financial_metric_property(
                            "income_loss_from_discontinued_operations_net_of_tax_during_phase_out"
                        ),
                        _financial_metric_property(
                            "income_loss_from_discontinued_operations_net_of_tax_gain_loss_on_disposal"
                        ),
                        _financial_metric_property("common_stock_dividends"),
                        _financial_metric_property(
                            "interest_and_dividend_income_operating"
                        ),
                        _financial_metric_property("noninterest_expense"),
                        _financial_metric_property(
                            "income_loss_from_discontinued_operations_net_of_tax_provision_for_gain_loss_on_disposal"
                        ),
                        _financial_metric_property(
                            "income_loss_from_discontinued_operations_net_of_tax_adjustment_to_prior_year_gain_loss_on_disposal"
                        ),
                    ),
                ),
                th.Property(
                    "balance_sheet",
                    th.ObjectType(
                        _financial_metric_property("assets"),
                        _financial_metric_property("current_assets"),
                        _financial_metric_property("noncurrent_assets"),
                        _financial_metric_property("liabilities"),
                        _financial_metric_property("current_liabilities"),
                        _financial_metric_property("noncurrent_liabilities"),
                        _financial_metric_property("equity"),
                        _financial_metric_property("equity_attributable_to_parent"),
                        _financial_metric_property(
                            "equity_attributable_to_noncontrolling_interest"
                        ),
                        _financial_metric_property("liabilities_and_equity"),
                        _financial_metric_property("other_current_liabilities"),
                        _financial_metric_property("wages"),
                        _financial_metric_property("intangible_assets"),
                        _financial_metric_property("prepaid_expenses"),
                        _financial_metric_property("noncurrent_prepaid_expenses"),
                        _financial_metric_property("fixed_assets"),
                        _financial_metric_property("other_noncurrent_assets"),
                        _financial_metric_property("other_current_assets"),
                        _financial_metric_property("accounts_payable"),
                        _financial_metric_property("long_term_debt"),
                        _financial_metric_property("inventory"),
                        _financial_metric_property("cash"),
                        _financial_metric_property("commitments_and_contingencies"),
                        _financial_metric_property("temporary_equity"),
                        _financial_metric_property(
                            "temporary_equity_attributable_to_parent"
                        ),
                        _financial_metric_property("accounts_receivable"),
                        _financial_metric_property(
                            "redeemable_noncontrolling_interest"
                        ),
                        _financial_metric_property(
                            "redeemable_noncontrolling_interest_preferred"
                        ),
                        _financial_metric_property("interest_payable"),
                        _financial_metric_property(
                            "redeemable_noncontrolling_interest_other"
                        ),
                        _financial_metric_property(
                            "redeemable_noncontrolling_interest_common"
                        ),
                        _financial_metric_property("long_term_investments"),
                        _financial_metric_property("other_noncurrent_liabilities"),
                    ),
                ),
                th.Property(
                    "cash_flow_statement",
                    th.ObjectType(
                        _financial_metric_property("net_cash_flow"),
                        _financial_metric_property("net_cash_flow_continuing"),
                        _financial_metric_property(
                            "net_cash_flow_from_operating_activities"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_operating_activities_continuing"
                        ),
                        _financial_metric_property("exchange_gains_losses"),
                        _financial_metric_property(
                            "net_cash_flow_from_financing_activities"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_operating_activities_discontinued"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_investing_activities_discontinued"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_investing_activities"
                        ),
                        _financial_metric_property("net_cash_flow_discontinued"),
                        _financial_metric_property(
                            "net_cash_flow_from_investing_activities_continuing"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_financing_activities_continuing"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_financing_activities_discontinued"
                        ),
                    ),
                ),
            ),
        ),
        th.Property("fiscal_period", th.StringType),
        th.Property("fiscal_year", th.StringType),
        th.Property("sic", th.StringType),
        th.Property("source_filing_file_url", th.StringType),
        th.Property("source_filing_url", th.StringType),
        th.Property("start_date", th.DateType),
        th.Property("tickers", th.ArrayType(th.StringType)),
        th.Property("timeframe", th.StringType),
    ).to_dict()


    def get_url(self, context: Context = None):
        return f"{self.url_base}/vX/reference/financials"

    @property
    def partitions(self) -> list[dict]:
        if self.use_cached_tickers:
            return [
                {"cik": t["cik"]}
                for t in self.tap.get_cached_stock_tickers()
                if "cik" in t
            ]
        return []


class ShortInterestStream(OptionalTickerPartitionStream):
    """Short Interest Stream"""

    name = "short_interest"

    primary_keys = ["settlement_date", "ticker"]
    replication_key = "settlement_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("settlement_date", th.DateType),
        th.Property("ticker", th.StringType),
        th.Property("short_interest", th.IntegerType),
        th.Property("avg_daily_volume", th.IntegerType),
        th.Property("days_to_cover", th.NumberType),
    ).to_dict()


    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/v1/short-interest"


class ShortVolumeStream(OptionalTickerPartitionStream):
    """Short Volume Stream"""

    name = "short_volume"

    primary_keys = ["date", "ticker"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("ticker", th.StringType),
        th.Property("short_volume", th.IntegerType),
        th.Property("short_volume_ratio", th.NumberType),
        th.Property("total_volume", th.IntegerType),
        th.Property("adf_short_volume", th.IntegerType),
        th.Property("adf_short_volume_exempt", th.IntegerType),
        th.Property("exempt_volume", th.IntegerType),
        th.Property("nasdaq_carteret_short_volume", th.IntegerType),
        th.Property("nasdaq_carteret_short_volume_exempt", th.IntegerType),
        th.Property("nasdaq_chicago_short_volume", th.IntegerType),
        th.Property("nasdaq_chicago_short_volume_exempt", th.IntegerType),
        th.Property("non_exempt_volume", th.IntegerType),
        th.Property("nyse_short_volume", th.IntegerType),
        th.Property("nyse_short_volume_exempt", th.IntegerType),
    ).to_dict()


    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/v1/short-volume"


class NewsStream(OptionalTickerPartitionStream):
    """News Stream"""

    name = "news"

    primary_keys = ["id"]
    replication_key = "published_utc"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _ticker_in_query_params = True

    publisher_schema = th.ObjectType(
        th.Property("homepage_url", th.StringType),
        th.Property("logo_url", th.StringType),
        th.Property("name", th.StringType),
        th.Property("favicon_url", th.StringType),
    )

    insight_schema = th.ObjectType(
        th.Property("ticker", th.StringType),
        th.Property("sentiment", th.StringType),
        th.Property("sentiment_reasoning", th.StringType),
        additional_properties=True,
    )

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("published_utc", th.DateTimeType),
        th.Property("publisher", publisher_schema),
        th.Property("tickers", th.ArrayType(th.StringType)),
        th.Property("title", th.StringType),
        th.Property("insights", th.ArrayType(insight_schema)),
        th.Property("keywords", th.ArrayType(th.StringType)),
        th.Property("amp_url", th.StringType),
        th.Property("article_url", th.StringType),
        th.Property("author", th.StringType),
        th.Property("description", th.StringType),
        th.Property("image_url", th.StringType),
    ).to_dict()


    def get_url(self, context: Context = None):
        return f"{self.url_base}/v2/reference/news"
