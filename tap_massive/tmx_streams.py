"""TMX stream type classes for tap-massive."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.client import MassiveRestStream


class TmxCorporateEventsStream(MassiveRestStream):
    """TMX Corporate Events Stream (Wall Street Horizon)."""

    name = "tmx_corporate_events"

    primary_keys = ["tmx_record_id"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    _incremental_timestamp_is_date = True

    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("company_name", th.StringType),
        th.Property("date", th.DateType),
        th.Property("isin", th.StringType),
        th.Property("name", th.StringType),
        th.Property("status", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("tmx_company_id", th.IntegerType),
        th.Property("tmx_record_id", th.StringType),
        th.Property("trading_venue", th.StringType),
        th.Property("type", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/tmx/v1/corporate-events"
