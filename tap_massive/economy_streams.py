"""Economy stream type classes for tap-massive."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
from tap_massive.client import MassiveRestStream


class TreasuryYieldStream(MassiveRestStream):
    """Yield Stream"""

    name = "treasury_yields"

    primary_keys = ["date"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("yield_1_month", th.NumberType),
        th.Property("yield_3_month", th.NumberType),
        th.Property("yield_6_month", th.NumberType),
        th.Property("yield_1_year", th.NumberType),
        th.Property("yield_2_year", th.NumberType),
        th.Property("yield_3_year", th.NumberType),
        th.Property("yield_5_year", th.NumberType),
        th.Property("yield_7_year", th.NumberType),
        th.Property("yield_10_year", th.NumberType),
        th.Property("yield_20_year", th.NumberType),
        th.Property("yield_30_year", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/fed/v1/treasury-yields"


class InflationStream(MassiveRestStream):
    """Inflation Stream

    Delivers key indicators of realized inflation, reflecting actual changes in consumer prices
    and spending behavior. Includes CPI, PCE, and other inflation metrics.
    """

    name = "inflation"

    primary_keys = ["date"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("cpi", th.NumberType),
        th.Property("cpi_core", th.NumberType),
        th.Property("cpi_year_over_year", th.NumberType),
        th.Property("pce", th.NumberType),
        th.Property("pce_core", th.NumberType),
        th.Property("pce_spending", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/fed/v1/inflation"


class InflationExpectationsStream(MassiveRestStream):
    """Inflation Expectations Stream

    Provides a broad view of how inflation is expected to evolve over time in the U.S. economy.
    Includes market-based and model-based inflation expectations across various time horizons.
    """

    name = "inflation_expectations"

    primary_keys = ["date"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("market_5_year", th.NumberType),
        th.Property("market_10_year", th.NumberType),
        th.Property("model_1_year", th.NumberType),
        th.Property("model_5_year", th.NumberType),
        th.Property("model_10_year", th.NumberType),
        th.Property("model_30_year", th.NumberType),
        th.Property("forward_years_5_to_10", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/fed/v1/inflation-expectations"


class LaborMarketStream(MassiveRestStream):
    """Labor Market Stream

    Provides key labor market indicators including unemployment rate,
    job openings, labor force participation, and average hourly earnings.
    Data is updated monthly from FRED sources.
    """

    name = "labor_market"

    primary_keys = ["date"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    _use_cached_tickers_default = False
    _incremental_timestamp_is_date = True

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("unemployment_rate", th.NumberType),
        th.Property("job_openings", th.NumberType),
        th.Property("labor_force_participation_rate", th.NumberType),
        th.Property("avg_hourly_earnings", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context = None):
        return f"{self.url_base}/fed/v1/labor-market"
