"""Fundamental, filings, and news stream classes for tap-massive."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.client import MassiveRestStream, OptionalTickerPartitionStream


# --- Financial Statement Streams ---


def _financial_metric_property(name: str):
    """Helper for the deprecated /vX/reference/financials endpoint's nested schema."""
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


class StockFinancialStatementStream(OptionalTickerPartitionStream):
    """Base class for new stock financial statement endpoints (flat response format)."""

    primary_keys = ["cik", "period_end", "timeframe"]
    replication_key = "filing_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True
    _ticker_param = "tickers"

    def _check_missing_fields(self, schema: dict, record: dict):
        # Financial statement endpoints can be high-dimensional and evolve.
        # Skip strict schema diffing to avoid false-positive critical logs.
        return


class StockFinancialsDeprecatedStream(MassiveRestStream):
    """Financials Stream (deprecated vX/reference/financials endpoint)"""

    name = "stock_financials_deprecated"

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
        th.Property("fiscal_period", th.StringType),
        th.Property("fiscal_year", th.StringType),
        th.Property("sic", th.StringType),
        th.Property("source_filing_file_url", th.StringType),
        th.Property("source_filing_url", th.StringType),
        th.Property("start_date", th.StringType),
        th.Property("tickers", th.ArrayType(th.StringType)),
        th.Property("timeframe", th.StringType),
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


class StockBalanceSheetsStream(StockFinancialStatementStream):
    """Balance Sheets Stream (new flat endpoint)"""

    name = "stock_balance_sheets"

    schema = th.PropertiesList(
        th.Property("cik", th.StringType),
        th.Property("tickers", th.ArrayType(th.StringType)),
        th.Property("filing_date", th.DateType),
        th.Property("period_end", th.DateType),
        th.Property("fiscal_year", th.NumberType),
        th.Property("fiscal_quarter", th.NumberType),
        th.Property("timeframe", th.StringType),
        th.Property("accounts_payable", th.NumberType),
        th.Property("accrued_and_other_current_liabilities", th.NumberType),
        th.Property("accumulated_other_comprehensive_income", th.NumberType),
        th.Property("additional_paid_in_capital", th.NumberType),
        th.Property("cash_and_equivalents", th.NumberType),
        th.Property("commitments_and_contingencies", th.NumberType),
        th.Property("common_stock", th.NumberType),
        th.Property("debt_current", th.NumberType),
        th.Property("deferred_revenue_current", th.NumberType),
        th.Property("goodwill", th.NumberType),
        th.Property("intangible_assets_net", th.NumberType),
        th.Property("inventories", th.NumberType),
        th.Property("long_term_debt_and_capital_lease_obligations", th.NumberType),
        th.Property("noncontrolling_interest", th.NumberType),
        th.Property("other_assets", th.NumberType),
        th.Property("other_current_assets", th.NumberType),
        th.Property("other_equity", th.NumberType),
        th.Property("other_noncurrent_liabilities", th.NumberType),
        th.Property("preferred_stock", th.NumberType),
        th.Property("property_plant_equipment_net", th.NumberType),
        th.Property("receivables", th.NumberType),
        th.Property("retained_earnings_deficit", th.NumberType),
        th.Property("short_term_investments", th.NumberType),
        th.Property("total_assets", th.NumberType),
        th.Property("total_current_assets", th.NumberType),
        th.Property("total_current_liabilities", th.NumberType),
        th.Property("total_equity", th.NumberType),
        th.Property("total_equity_attributable_to_parent", th.NumberType),
        th.Property("total_liabilities", th.NumberType),
        th.Property("total_liabilities_and_equity", th.NumberType),
        th.Property("treasury_stock", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/financials/v1/balance-sheets"


class StockCashFlowStatementsStream(StockFinancialStatementStream):
    """Cash Flow Statements Stream (new flat endpoint)"""

    name = "stock_cash_flow_statements"

    schema = th.PropertiesList(
        th.Property("cik", th.StringType),
        th.Property("tickers", th.ArrayType(th.StringType)),
        th.Property("filing_date", th.DateType),
        th.Property("period_end", th.DateType),
        th.Property("fiscal_year", th.NumberType),
        th.Property("fiscal_quarter", th.NumberType),
        th.Property("timeframe", th.StringType),
        th.Property(
            "cash_from_operating_activities_continuing_operations", th.NumberType
        ),
        th.Property("change_in_cash_and_equivalents", th.NumberType),
        th.Property(
            "change_in_other_operating_assets_and_liabilities_net", th.NumberType
        ),
        th.Property("depreciation_depletion_and_amortization", th.NumberType),
        th.Property("dividends", th.NumberType),
        th.Property("effect_of_currency_exchange_rate", th.NumberType),
        th.Property("income_loss_from_discontinued_operations", th.NumberType),
        th.Property("long_term_debt_issuances_repayments", th.NumberType),
        th.Property("net_cash_from_financing_activities", th.NumberType),
        th.Property(
            "net_cash_from_financing_activities_continuing_operations", th.NumberType
        ),
        th.Property(
            "net_cash_from_financing_activities_discontinued_operations",
            th.NumberType,
        ),
        th.Property("net_cash_from_investing_activities", th.NumberType),
        th.Property(
            "net_cash_from_investing_activities_continuing_operations", th.NumberType
        ),
        th.Property(
            "net_cash_from_investing_activities_discontinued_operations",
            th.NumberType,
        ),
        th.Property("net_cash_from_operating_activities", th.NumberType),
        th.Property(
            "net_cash_from_operating_activities_discontinued_operations",
            th.NumberType,
        ),
        th.Property("net_income", th.NumberType),
        th.Property("noncontrolling_interests", th.NumberType),
        th.Property("other_cash_adjustments", th.NumberType),
        th.Property("other_financing_activities", th.NumberType),
        th.Property("other_investing_activities", th.NumberType),
        th.Property("other_operating_activities", th.NumberType),
        th.Property("purchase_of_property_plant_and_equipment", th.NumberType),
        th.Property("sale_of_property_plant_and_equipment", th.NumberType),
        th.Property("short_term_debt_issuances_repayments", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/financials/v1/cash-flow-statements"


class StockIncomeStatementsStream(StockFinancialStatementStream):
    """Income Statements Stream (new flat endpoint)"""

    name = "stock_income_statements"

    schema = th.PropertiesList(
        th.Property("cik", th.StringType),
        th.Property("tickers", th.ArrayType(th.StringType)),
        th.Property("filing_date", th.DateType),
        th.Property("period_end", th.DateType),
        th.Property("fiscal_year", th.NumberType),
        th.Property("fiscal_quarter", th.NumberType),
        th.Property("timeframe", th.StringType),
        th.Property("revenue", th.NumberType),
        th.Property("cost_of_revenue", th.NumberType),
        th.Property("gross_profit", th.NumberType),
        th.Property("depreciation_depletion_amortization", th.NumberType),
        th.Property("selling_general_administrative", th.NumberType),
        th.Property("research_development", th.NumberType),
        th.Property("other_operating_expenses", th.NumberType),
        th.Property("total_operating_expenses", th.NumberType),
        th.Property("operating_income", th.NumberType),
        th.Property("other_income_expense", th.NumberType),
        th.Property("total_other_income_expense", th.NumberType),
        th.Property("interest_income", th.NumberType),
        th.Property("interest_expense", th.NumberType),
        th.Property("equity_in_affiliates", th.NumberType),
        th.Property("extraordinary_items", th.NumberType),
        th.Property("income_before_income_taxes", th.NumberType),
        th.Property("income_taxes", th.NumberType),
        th.Property("consolidated_net_income_loss", th.NumberType),
        th.Property("net_income_loss_attributable_common_shareholders", th.NumberType),
        th.Property("preferred_stock_dividends_declared", th.NumberType),
        th.Property("noncontrolling_interest", th.NumberType),
        th.Property("discontinued_operations", th.NumberType),
        th.Property("basic_earnings_per_share", th.NumberType),
        th.Property("diluted_earnings_per_share", th.NumberType),
        th.Property("basic_shares_outstanding", th.NumberType),
        th.Property("diluted_shares_outstanding", th.NumberType),
        th.Property("ebitda", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/financials/v1/income-statements"


class StockRatiosStream(OptionalTickerPartitionStream):
    """Financial Ratios Stream (flat endpoint, different PKs from other financial streams)"""

    name = "stock_ratios"

    primary_keys = ["ticker", "date"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("date", th.DateType),
        # Valuation ratios
        th.Property("price", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("enterprise_value", th.NumberType),
        th.Property("earnings_per_share", th.NumberType),
        th.Property("price_to_earnings", th.NumberType),
        th.Property("price_to_book", th.NumberType),
        th.Property("price_to_sales", th.NumberType),
        th.Property("price_to_cash_flow", th.NumberType),
        th.Property("price_to_free_cash_flow", th.NumberType),
        th.Property("ev_to_sales", th.NumberType),
        th.Property("ev_to_ebitda", th.NumberType),
        # Profitability
        th.Property("dividend_yield", th.NumberType),
        th.Property("return_on_assets", th.NumberType),
        th.Property("return_on_equity", th.NumberType),
        # Liquidity / Leverage
        th.Property("debt_to_equity", th.NumberType),
        th.Property("current", th.NumberType),
        th.Property("quick", th.NumberType),
        th.Property("cash", th.NumberType),
        # Other
        th.Property("free_cash_flow", th.NumberType),
        th.Property("average_volume", th.NumberType),
    ).to_dict()

    def _check_missing_fields(self, schema: dict, record: dict):
        # Ratios endpoint can evolve with new fields.
        return

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/financials/v1/ratios"


# --- Other Fundamental Streams ---


class StockFloatStream(OptionalTickerPartitionStream):
    """Float Stream"""

    name = "stock_float"

    primary_keys = ["effective_date", "ticker"]
    replication_key = "effective_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("effective_date", th.DateType),
        th.Property("free_float", th.IntegerType),
        th.Property("free_float_percent", th.NumberType),
        th.Property("ticker", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/vX/float"


class StockShortInterestStream(OptionalTickerPartitionStream):
    """Short Interest Stream"""

    name = "stock_short_interest"

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


class StockShortVolumeStream(OptionalTickerPartitionStream):
    """Short Volume Stream"""

    name = "stock_short_volume"

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


# --- Filings & Disclosures Streams ---


class Stock10KSectionsStream(OptionalTickerPartitionStream):
    """10-K Sections Stream"""

    name = "stock_10k_sections"

    primary_keys = ["ticker", "period_end", "section"]
    replication_key = "filing_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("cik", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("period_end", th.DateType),
        th.Property("section", th.StringType),
        th.Property("text", th.StringType),
        th.Property("filing_url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/filings/10-K/vX/sections"


class StockRiskFactorsStream(OptionalTickerPartitionStream):
    """Risk Factors Stream"""

    name = "stock_risk_factors"

    primary_keys = ["ticker", "filing_date", "primary_category"]
    replication_key = "filing_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True
    _ticker_in_query_params = True

    schema = th.PropertiesList(
        th.Property("cik", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("primary_category", th.StringType),
        th.Property("secondary_category", th.StringType),
        th.Property("tertiary_category", th.StringType),
        th.Property("supporting_text", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/filings/vX/risk-factors"


class StockRiskCategoriesStream(MassiveRestStream):
    """Risk Categories Stream"""

    name = "stock_risk_categories"

    primary_keys = ["taxonomy", "primary_category", "secondary_category", "tertiary_category"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("taxonomy", th.NumberType),
        th.Property("primary_category", th.StringType),
        th.Property("secondary_category", th.StringType),
        th.Property("tertiary_category", th.StringType),
        th.Property("description", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/taxonomies/vX/risk-factors"


# --- News Stream ---


class StockNewsStream(OptionalTickerPartitionStream):
    """News Stream"""

    name = "stock_news"

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
