"""ETF Global stream type classes for tap-massive."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.client import MassiveRestStream

# Reusable schema for array-of-object exposure fields (e.g. sector_exposure, geographic_exposure).
_EXPOSURE_ITEM_TYPE = th.ObjectType(
    th.Property("key", th.StringType),
    th.Property("value", th.NumberType),
)


class EtfGlobalConstituentsStream(MassiveRestStream):
    """ETF Global Constituents Stream."""

    name = "etf_global_constituents"

    # PK uses constituent_name (always present) instead of constituent_ticker (NULL for
    # physical commodity holdings like "GOLD OZ." in AAAU, IAU, etc.).
    primary_keys = ["composite_ticker", "constituent_name", "effective_date"]
    replication_key = "processed_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    _incremental_timestamp_is_date = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("asset_class", th.StringType),
        th.Property("composite_ticker", th.StringType),
        th.Property("constituent_name", th.StringType),
        th.Property("constituent_rank", th.IntegerType),
        th.Property("constituent_ticker", th.StringType),
        th.Property("country_of_exchange", th.StringType),
        th.Property("currency_traded", th.StringType),
        th.Property("effective_date", th.DateType),
        th.Property("exchange", th.StringType),
        th.Property("figi", th.StringType),
        th.Property("isin", th.StringType),
        th.Property("market_value", th.NumberType),
        th.Property("processed_date", th.DateType),
        th.Property("security_type", th.StringType),
        th.Property("sedol", th.StringType),
        th.Property("shares_held", th.NumberType),
        th.Property("us_code", th.StringType),
        th.Property("weight", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/etf-global/v1/constituents"


class EtfGlobalFundFlowsStream(MassiveRestStream):
    """ETF Global Fund Flows Stream."""

    name = "etf_global_fund_flows"

    primary_keys = ["composite_ticker", "effective_date"]
    replication_key = "processed_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    _incremental_timestamp_is_date = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("composite_ticker", th.StringType),
        th.Property("effective_date", th.DateType),
        th.Property("fund_flow", th.NumberType),
        th.Property("nav", th.NumberType),
        th.Property("processed_date", th.DateType),
        th.Property("shares_outstanding", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/etf-global/v1/fund-flows"


class EtfGlobalAnalyticsStream(MassiveRestStream):
    """ETF Global Analytics Stream."""

    name = "etf_global_analytics"

    primary_keys = ["composite_ticker", "effective_date"]
    replication_key = "processed_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    _incremental_timestamp_is_date = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("composite_ticker", th.StringType),
        th.Property("effective_date", th.DateType),
        th.Property("processed_date", th.DateType),
        th.Property("quant_composite_behavioral", th.NumberType),
        th.Property("quant_composite_fundamental", th.NumberType),
        th.Property("quant_composite_global", th.NumberType),
        th.Property("quant_composite_quality", th.NumberType),
        th.Property("quant_composite_sentiment", th.NumberType),
        th.Property("quant_composite_technical", th.NumberType),
        th.Property("quant_fundamental_div", th.NumberType),
        th.Property("quant_fundamental_pb", th.NumberType),
        th.Property("quant_fundamental_pcf", th.NumberType),
        th.Property("quant_fundamental_pe", th.NumberType),
        th.Property("quant_global_country", th.NumberType),
        th.Property("quant_global_sector", th.NumberType),
        th.Property("quant_grade", th.StringType),
        th.Property("quant_quality_diversification", th.NumberType),
        th.Property("quant_quality_firm", th.NumberType),
        th.Property("quant_quality_liquidity", th.NumberType),
        th.Property("quant_sentiment_iv", th.NumberType),
        th.Property("quant_sentiment_pc", th.NumberType),
        th.Property("quant_sentiment_si", th.NumberType),
        th.Property("quant_technical_it", th.NumberType),
        th.Property("quant_technical_lt", th.NumberType),
        th.Property("quant_technical_st", th.NumberType),
        th.Property("quant_total_score", th.NumberType),
        th.Property("reward_score", th.NumberType),
        th.Property("risk_country", th.NumberType),
        th.Property("risk_deviation", th.NumberType),
        th.Property("risk_efficiency", th.NumberType),
        th.Property("risk_liquidity", th.NumberType),
        th.Property("risk_structure", th.NumberType),
        th.Property("risk_total_score", th.NumberType),
        th.Property("risk_volatility", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/etf-global/v1/analytics"


class EtfGlobalProfilesStream(MassiveRestStream):
    """ETF Global Profiles & Exposure Stream."""

    name = "etf_global_profiles"

    primary_keys = ["composite_ticker", "effective_date"]
    replication_key = "processed_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    _incremental_timestamp_is_date = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("administrator", th.StringType),
        th.Property("advisor", th.StringType),
        th.Property("asset_class", th.StringType),
        th.Property("aum", th.NumberType),
        th.Property("avg_daily_trading_volume", th.NumberType),
        th.Property("bid_ask_spread", th.NumberType),
        th.Property("call_volume", th.NumberType),
        th.Property("category", th.StringType),
        th.Property("composite_ticker", th.StringType),
        th.Property("coupon_exposure", th.ArrayType(_EXPOSURE_ITEM_TYPE)),
        th.Property("creation_fee", th.NumberType),
        th.Property("creation_unit_size", th.NumberType),
        th.Property("currency_exposure", th.ArrayType(_EXPOSURE_ITEM_TYPE)),
        th.Property("custodian", th.StringType),
        th.Property("description", th.StringType),
        th.Property("development_class", th.StringType),
        th.Property("discount_premium", th.NumberType),
        th.Property("distribution_frequency", th.StringType),
        th.Property("distributor", th.StringType),
        th.Property("effective_date", th.DateType),
        th.Property("fee_waivers", th.NumberType),
        th.Property("fiscal_year_end", th.StringType),
        th.Property("focus", th.StringType),
        th.Property("futures_commission_merchant", th.StringType),
        th.Property("geographic_exposure", th.ArrayType(_EXPOSURE_ITEM_TYPE)),
        th.Property("inception_date", th.DateType),
        th.Property("industry_exposure", th.ArrayType(_EXPOSURE_ITEM_TYPE)),
        th.Property("industry_group_exposure", th.ArrayType(_EXPOSURE_ITEM_TYPE)),
        th.Property("issuer", th.StringType),
        th.Property("lead_market_maker", th.StringType),
        th.Property("leverage_style", th.StringType),
        th.Property("levered_amount", th.NumberType),
        th.Property("listing_exchange", th.StringType),
        th.Property("management_classification", th.StringType),
        th.Property("management_fee", th.NumberType),
        th.Property("maturity_exposure", th.ArrayType(_EXPOSURE_ITEM_TYPE)),
        th.Property("net_expenses", th.NumberType),
        th.Property("num_holdings", th.NumberType),
        th.Property("options_available", th.IntegerType),
        th.Property("options_volume", th.NumberType),
        th.Property("other_expenses", th.NumberType),
        th.Property("portfolio_manager", th.StringType),
        th.Property("primary_benchmark", th.StringType),
        th.Property("processed_date", th.DateType),
        th.Property("product_type", th.StringType),
        th.Property("put_call_ratio", th.NumberType),
        th.Property("put_volume", th.NumberType),
        th.Property("region", th.StringType),
        th.Property("sector_exposure", th.ArrayType(_EXPOSURE_ITEM_TYPE)),
        th.Property("short_interest", th.NumberType),
        th.Property("subadvisor", th.StringType),
        th.Property("subindustry_exposure", th.ArrayType(_EXPOSURE_ITEM_TYPE)),
        th.Property("tax_classification", th.StringType),
        th.Property("total_expenses", th.NumberType),
        th.Property("transfer_agent", th.StringType),
        th.Property("trustee", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/etf-global/v1/profiles"


class EtfGlobalTaxonomiesStream(MassiveRestStream):
    """ETF Global Taxonomies Stream."""

    name = "etf_global_taxonomies"

    primary_keys = ["composite_ticker", "effective_date"]
    replication_key = "processed_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    _incremental_timestamp_is_date = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("asset_class", th.StringType),
        th.Property("category", th.StringType),
        th.Property("composite_ticker", th.StringType),
        th.Property("country", th.StringType),
        th.Property("credit_quality_rating", th.StringType),
        th.Property("description", th.StringType),
        th.Property("development_class", th.StringType),
        th.Property("duration", th.StringType),
        th.Property("effective_date", th.DateType),
        th.Property("esg", th.StringType),
        th.Property("exposure_mechanism", th.StringType),
        th.Property("factor", th.StringType),
        th.Property("focus", th.StringType),
        th.Property("hedge_reset", th.StringType),
        th.Property("holdings_disclosure_frequency", th.StringType),
        th.Property("inception_date", th.DateType),
        th.Property("isin", th.StringType),
        th.Property("issuer", th.StringType),
        th.Property("leverage_reset", th.StringType),
        th.Property("leverage_style", th.StringType),
        th.Property("levered_amount", th.NumberType),
        th.Property("management_classification", th.StringType),
        th.Property("management_style", th.StringType),
        th.Property("maturity", th.StringType),
        th.Property("objective", th.StringType),
        th.Property("primary_benchmark", th.StringType),
        th.Property("processed_date", th.DateType),
        th.Property("product_type", th.StringType),
        th.Property("rebalance_frequency", th.StringType),
        th.Property("reconstitution_frequency", th.StringType),
        th.Property("region", th.StringType),
        th.Property("secondary_objective", th.StringType),
        th.Property("selection_methodology", th.StringType),
        th.Property("selection_universe", th.StringType),
        th.Property("strategic_focus", th.StringType),
        th.Property("targeted_focus", th.StringType),
        th.Property("tax_classification", th.StringType),
        th.Property("us_code", th.StringType),
        th.Property("weighting_methodology", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/etf-global/v1/taxonomies"
