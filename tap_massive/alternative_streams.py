"""Alternative data stream type classes for tap-massive."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.client import MassiveRestStream


class MerchantAggregatesStream(MassiveRestStream):
    """EU consumer spending aggregates (Fable Data)."""

    name = "merchant_aggregates"

    primary_keys = [
        "transaction_date",
        "name",
        "user_country",
        "channel",
        "consumer_type",
    ]
    replication_key = "transaction_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True

    schema = th.PropertiesList(
        th.Property("channel", th.StringType),
        th.Property("consumer_type", th.StringType),
        th.Property("eight_day_rolling_category_accounts", th.IntegerType),
        th.Property("eight_day_rolling_total_accounts", th.IntegerType),
        th.Property("mcc_group", th.StringType),
        th.Property("merchant_industry", th.StringType),
        th.Property("merchant_ticker", th.StringType),
        th.Property("name", th.StringType),
        th.Property("parent_name", th.StringType),
        th.Property("published_date", th.DateType),
        th.Property("spend_in_distinct_account_key_count", th.IntegerType),
        th.Property("spend_in_spend", th.NumberType),
        th.Property("spend_in_transaction_count", th.IntegerType),
        th.Property("spend_out_distinct_account_key_count", th.IntegerType),
        th.Property("spend_out_spend", th.NumberType),
        th.Property("spend_out_transaction_count", th.IntegerType),
        th.Property("total_accounts", th.IntegerType),
        th.Property("total_spend", th.NumberType),
        th.Property("total_transactions", th.IntegerType),
        th.Property("transaction_currency", th.StringType),
        th.Property("transaction_date", th.DateType),
        th.Property("twenty_eight_day_rolling_category_accounts", th.IntegerType),
        th.Property("twenty_eight_day_rolling_total_accounts", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("user_country", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/consumer-spending/eu/v1/merchant-aggregates"


class MerchantHierarchyStream(MassiveRestStream):
    """Merchant-to-company hierarchy and classification reference data (Fable Data)."""

    name = "merchant_hierarchy"

    primary_keys = ["lookup_name", "active_from", "active_to"]

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("active_from", th.DateType),
        th.Property("active_to", th.DateType),
        th.Property("category", th.StringType),
        th.Property("grandparent_name", th.StringType),
        th.Property("grandparent_ticker", th.StringType),
        th.Property("great_grandparent_name", th.StringType),
        th.Property("great_grandparent_ticker", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("industry_group", th.StringType),
        th.Property("listing_status", th.StringType),
        th.Property("lookup_name", th.StringType),
        th.Property("normalized_name", th.StringType),
        th.Property("parent_name", th.StringType),
        th.Property("parent_ticker", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("sub_industry", th.StringType),
        th.Property("ticker", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/consumer-spending/eu/v1/merchant-hierarchy"
