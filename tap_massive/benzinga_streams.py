"""Benzinga stream type classes for tap-massive."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.client import MassiveRestStream, OptionalTickerPartitionStream

# --- FULL_TABLE Streams ---


class BenzingaAnalystDetailsStream(MassiveRestStream):
    """Benzinga Analyst Details Stream."""

    name = "benzinga_analyst_details"

    primary_keys = ["benzinga_id"]

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("benzinga_id", th.StringType),
        th.Property("benzinga_firm_id", th.StringType),
        th.Property("firm_name", th.StringType),
        th.Property("full_name", th.StringType),
        th.Property("last_updated", th.DateTimeType),
        th.Property("overall_avg_return", th.NumberType),
        th.Property("overall_avg_return_percentile", th.NumberType),
        th.Property("overall_success_rate", th.NumberType),
        th.Property("smart_score", th.NumberType),
        th.Property("total_ratings", th.NumberType),
        th.Property("total_ratings_percentile", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/benzinga/v1/analysts"


class BenzingaFirmDetailsStream(MassiveRestStream):
    """Benzinga Firm Details Stream."""

    name = "benzinga_firm_details"

    primary_keys = ["benzinga_id"]

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("benzinga_id", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("last_updated", th.DateTimeType),
        th.Property("name", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/benzinga/v1/firms"


class BenzingaConsensusRatingsStream(OptionalTickerPartitionStream):
    """Benzinga Consensus Ratings Stream (per-ticker aggregated ratings)."""

    name = "benzinga_consensus_ratings"

    primary_keys = ["ticker"]

    _use_cached_tickers_default = True
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("buy_ratings", th.IntegerType),
        th.Property("consensus_price_target", th.NumberType),
        th.Property("consensus_rating", th.StringType),
        th.Property("consensus_rating_value", th.NumberType),
        th.Property("high_price_target", th.NumberType),
        th.Property("hold_ratings", th.IntegerType),
        th.Property("low_price_target", th.NumberType),
        th.Property("price_target_contributors", th.IntegerType),
        th.Property("ratings_contributors", th.IntegerType),
        th.Property("sell_ratings", th.IntegerType),
        th.Property("strong_buy_ratings", th.IntegerType),
        th.Property("strong_sell_ratings", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context = None):
        path_params = context.get("path_params", {})
        ticker = path_params.get(self._ticker_param)
        return f"{self.url_base}/benzinga/v1/consensus-ratings/{ticker}"


# --- INCREMENTAL Streams (replication key: last_updated) ---


class BenzingaAnalystInsightsStream(MassiveRestStream):
    """Benzinga Analyst Insights Stream."""

    name = "benzinga_analyst_insights"

    primary_keys = ["benzinga_id"]
    replication_key = "last_updated"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("benzinga_id", th.StringType),
        th.Property("benzinga_firm_id", th.StringType),
        th.Property("benzinga_rating_id", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("date", th.DateType),
        th.Property("firm", th.StringType),
        th.Property("insight", th.StringType),
        th.Property("last_updated", th.DateTimeType),
        th.Property("price_target", th.NumberType),
        th.Property("rating", th.StringType),
        th.Property("rating_action", th.StringType),
        th.Property("ticker", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/benzinga/v1/analyst-insights"


class BenzingaAnalystRatingsStream(MassiveRestStream):
    """Benzinga Analyst Ratings Stream."""

    name = "benzinga_analyst_ratings"

    primary_keys = ["benzinga_id"]
    replication_key = "last_updated"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("adjusted_price_target", th.NumberType),
        th.Property("analyst", th.StringType),
        th.Property("benzinga_analyst_id", th.StringType),
        th.Property("benzinga_calendar_url", th.StringType),
        th.Property("benzinga_firm_id", th.StringType),
        th.Property("benzinga_id", th.StringType),
        th.Property("benzinga_news_url", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("date", th.DateType),
        th.Property("firm", th.StringType),
        th.Property("importance", th.IntegerType),
        th.Property("last_updated", th.DateTimeType),
        th.Property("notes", th.StringType),
        th.Property("previous_adjusted_price_target", th.NumberType),
        th.Property("previous_price_target", th.NumberType),
        th.Property("previous_rating", th.StringType),
        th.Property("price_percent_change", th.NumberType),
        th.Property("price_target", th.NumberType),
        th.Property("price_target_action", th.StringType),
        th.Property("rating", th.StringType),
        th.Property("rating_action", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("time", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/benzinga/v1/ratings"


class BenzingaBullsBearsSayStream(MassiveRestStream):
    """Benzinga Bulls Bears Say Stream."""

    name = "benzinga_bulls_bears_say"

    primary_keys = ["benzinga_id"]
    replication_key = "last_updated"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("bear_case", th.StringType),
        th.Property("benzinga_id", th.StringType),
        th.Property("bull_case", th.StringType),
        th.Property("last_updated", th.DateTimeType),
        th.Property("ticker", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/benzinga/v1/bulls-bears-say"


class BenzingaCorporateGuidanceStream(MassiveRestStream):
    """Benzinga Corporate Guidance Stream."""

    name = "benzinga_corporate_guidance"

    primary_keys = ["benzinga_id"]
    replication_key = "last_updated"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("benzinga_id", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("date", th.DateType),
        th.Property("eps_method", th.StringType),
        th.Property("estimated_eps_guidance", th.NumberType),
        th.Property("estimated_revenue_guidance", th.NumberType),
        th.Property("fiscal_period", th.StringType),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("importance", th.IntegerType),
        th.Property("last_updated", th.DateTimeType),
        th.Property("max_eps_guidance", th.NumberType),
        th.Property("max_revenue_guidance", th.NumberType),
        th.Property("min_eps_guidance", th.NumberType),
        th.Property("min_revenue_guidance", th.NumberType),
        th.Property("notes", th.StringType),
        th.Property("positioning", th.StringType),
        th.Property("previous_max_eps_guidance", th.NumberType),
        th.Property("previous_max_revenue_guidance", th.NumberType),
        th.Property("previous_min_eps_guidance", th.NumberType),
        th.Property("previous_min_revenue_guidance", th.NumberType),
        th.Property("release_type", th.StringType),
        th.Property("revenue_method", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("time", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/benzinga/v1/guidance"


class BenzingaEarningsStream(MassiveRestStream):
    """Benzinga Earnings Stream."""

    name = "benzinga_earnings"

    primary_keys = ["benzinga_id", "ticker", "last_updated"]
    replication_key = "last_updated"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("actual_eps", th.NumberType),
        th.Property("actual_revenue", th.NumberType),
        th.Property("benzinga_id", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("date", th.DateType),
        th.Property("date_status", th.StringType),
        th.Property("eps_method", th.StringType),
        th.Property("eps_surprise", th.NumberType),
        th.Property("eps_surprise_percent", th.NumberType),
        th.Property("estimated_eps", th.NumberType),
        th.Property("estimated_revenue", th.NumberType),
        th.Property("fiscal_period", th.StringType),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("importance", th.IntegerType),
        th.Property("last_updated", th.DateTimeType),
        th.Property("notes", th.StringType),
        th.Property("previous_eps", th.NumberType),
        th.Property("previous_revenue", th.NumberType),
        th.Property("revenue_method", th.StringType),
        th.Property("revenue_surprise", th.NumberType),
        th.Property("revenue_surprise_percent", th.NumberType),
        th.Property("ticker", th.StringType),
        th.Property("time", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/benzinga/v1/earnings"


# --- INCREMENTAL Stream (replication key: published) ---


class BenzingaNewsStream(MassiveRestStream):
    """Benzinga News Stream (v2 endpoint)."""

    name = "benzinga_news"

    primary_keys = ["benzinga_id"]
    replication_key = "published"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("author", th.StringType),
        th.Property("benzinga_id", th.IntegerType),
        th.Property("body", th.StringType),
        th.Property("channels", th.ArrayType(th.StringType)),
        th.Property("images", th.ArrayType(th.StringType)),
        th.Property("last_updated", th.DateTimeType),
        th.Property("published", th.DateTimeType),
        th.Property("tags", th.ArrayType(th.StringType)),
        th.Property("teaser", th.StringType),
        th.Property("tickers", th.ArrayType(th.StringType)),
        th.Property("title", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/benzinga/v2/news"
