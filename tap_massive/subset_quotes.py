"""Earnings-filtered quote streams.

Pulls tick-level quotes only for tickers on their earnings dates
(plus the previous and next NYSE trading day). Earnings data comes
from either a local PostgreSQL database or the Massive API.
"""

from __future__ import annotations

import bisect
import logging
import typing as t
from datetime import date, datetime, timedelta, timezone

import pandas_market_calendars as mcal
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_massive.base_streams import _NanosecondIncrementalMixin, safe_int
from tap_massive.client import MassiveRestStream

# ---------------------------------------------------------------------------
# EarningsCalendar — query earnings dates and resolve NYSE trading days
# ---------------------------------------------------------------------------


class EarningsCalendar:
    """Query earnings from postgres or API and compute quote dates using NYSE calendar."""

    TMX_EARNINGS_TYPES = (
        "earnings_announcement_date",
        "earnings_conference_call",
        "earnings_results_announcement",
    )

    _CALENDAR_BUFFER_DAYS = 10

    _VALID_DATA_SOURCES = ("benzinga", "tmx", "both")

    def __init__(
        self,
        *,
        earnings_source: str,
        start_date: str,
        end_date: str,
        db_host: str | None = None,
        db_port: int = 5432,
        db_name: str | None = None,
        db_user: str | None = None,
        db_password: str | None = None,
        db_schema: str = "public",
        us_only: bool = True,
        earnings_data_source: str = "both",
        api_key: str | None = None,
        base_url: str = "https://api.massive.com",
        request_timeout: int = 300,
    ) -> None:
        self._start_date = date.fromisoformat(start_date)
        self._end_date = date.fromisoformat(end_date)
        self._db_host = db_host
        self._db_port = db_port
        self._db_name = db_name
        self._db_user = db_user
        self._db_password = db_password
        self._db_schema = db_schema
        self._us_only = us_only
        self._api_key = api_key
        self._base_url = base_url
        self._request_timeout = request_timeout

        if earnings_data_source not in self._VALID_DATA_SOURCES:
            msg = (
                f"Unknown earnings_data_source: {earnings_data_source!r}. "
                f"Use one of {self._VALID_DATA_SOURCES}."
            )
            raise ValueError(msg)
        self._earnings_data_source = earnings_data_source

        if earnings_source == "postgres":
            raw_earnings = self._query_earnings_from_postgres()
        elif earnings_source == "api":
            raw_earnings = self._query_earnings_from_api()
        else:
            msg = f"Unknown earnings_source: {earnings_source!r}. Use 'postgres' or 'api'."
            raise ValueError(msg)

        if self._us_only:
            before = len(raw_earnings)
            raw_earnings = [(t, d) for t, d in raw_earnings if self._is_us_ticker(t)]
            logging.info(
                f"EarningsCalendar: US-only filter kept {len(raw_earnings)}/{before} rows."
            )

        self._nyse_trading_days = self._build_nyse_trading_days()
        self._trading_days_set = frozenset(self._nyse_trading_days)

        self._ticker_to_quote_dates: dict[str, list[date]] = self._compute_quote_dates(
            raw_earnings
        )

        total_partitions = sum(len(v) for v in self._ticker_to_quote_dates.values())
        logging.info(
            f"EarningsCalendar: {len(self._ticker_to_quote_dates)} tickers, "
            f"{total_partitions} total (ticker, date) partitions."
        )

    # -- Ticker filtering ---------------------------------------------------

    @staticmethod
    def _is_us_ticker(ticker: str) -> bool:
        """Return True if the ticker looks like a US equity symbol.

        US tickers are plain uppercase letters (e.g. AAPL, BRK-A).
        Non-US tickers have an exchange suffix after a dot (e.g. 0011.HK, BTOU.SG).
        """
        if not ticker:
            return False
        # Tickers containing a '.' are exchange-suffixed (non-US)
        return "." not in ticker

    # -- NYSE trading day helpers ------------------------------------------

    def _build_nyse_trading_days(self) -> list[date]:
        """Build sorted list of NYSE trading days covering the earnings range with buffer."""
        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(
            start_date=self._start_date - timedelta(days=self._CALENDAR_BUFFER_DAYS),
            end_date=self._end_date + timedelta(days=self._CALENDAR_BUFFER_DAYS),
        )
        return sorted(schedule.index.date)

    def _prev_trading_day(self, d: date) -> date | None:
        idx = bisect.bisect_left(self._nyse_trading_days, d)
        if idx > 0:
            return self._nyse_trading_days[idx - 1]
        return None

    def _next_trading_day(self, d: date) -> date | None:
        idx = bisect.bisect_left(self._nyse_trading_days, d)
        if idx < len(self._nyse_trading_days) and self._nyse_trading_days[idx] == d:
            if idx + 1 < len(self._nyse_trading_days):
                return self._nyse_trading_days[idx + 1]
        elif idx < len(self._nyse_trading_days):
            return self._nyse_trading_days[idx]
        return None

    # -- Compute quote dates per ticker ------------------------------------

    def _compute_quote_dates(
        self, raw_earnings: list[tuple[str, date]]
    ) -> dict[str, list[date]]:
        """For each (ticker, earnings_date), compute unique quote dates."""
        ticker_dates: dict[str, set[date]] = {}
        for ticker, earnings_date in raw_earnings:
            if not ticker:
                continue
            dates = ticker_dates.setdefault(ticker, set())
            prev_td = self._prev_trading_day(earnings_date)
            if prev_td:
                dates.add(prev_td)
            if earnings_date in self._trading_days_set:
                dates.add(earnings_date)
            next_td = self._next_trading_day(earnings_date)
            if next_td:
                dates.add(next_td)
        return {ticker: sorted(ds) for ticker, ds in sorted(ticker_dates.items()) if ds}

    # -- Public accessors --------------------------------------------------

    def get_stock_partitions(self) -> list[dict[str, str]]:
        """Return [{"ticker": "AAPL", "quote_date": "2025-06-17"}, ...]."""
        partitions = []
        for ticker, quote_dates in self._ticker_to_quote_dates.items():
            for qd in quote_dates:
                partitions.append({"ticker": ticker, "quote_date": qd.isoformat()})
        return partitions

    def get_earnings_underlyings(self) -> list[str]:
        """Return sorted unique underlying tickers."""
        return sorted(self._ticker_to_quote_dates.keys())

    def get_quote_dates_for_underlying(self, underlying: str) -> list[date]:
        """Return quote dates for a given underlying ticker."""
        return self._ticker_to_quote_dates.get(underlying, [])

    # -- Data source: PostgreSQL -------------------------------------------

    def _query_earnings_from_postgres(self) -> list[tuple[str, date]]:
        if not self._db_host or not self._db_name:
            msg = (
                "postgres_host and postgres_database must be set in tap config "
                "when earnings_source='postgres'."
            )
            raise ValueError(msg)

        import psycopg2  # noqa: PLC0415
        import psycopg2.sql as pgsql  # noqa: PLC0415

        schema = pgsql.Identifier(self._db_schema)
        parts: list[pgsql.Composable] = []
        params: list = []

        if self._earnings_data_source in ("benzinga", "both"):
            parts.append(
                pgsql.SQL(
                    "SELECT DISTINCT ticker, date AS earnings_date "
                    "FROM {schema}.benzinga_earnings "
                    "WHERE date >= %s AND date <= %s"
                ).format(schema=schema)
            )
            params.extend([self._start_date, self._end_date])

        if self._earnings_data_source in ("tmx", "both"):
            types_placeholders = ", ".join(["%s"] * len(self.TMX_EARNINGS_TYPES))
            parts.append(
                pgsql.SQL(
                    "SELECT DISTINCT ticker, date AS earnings_date "
                    "FROM {schema}.tmx_corporate_events "
                    "WHERE type IN ({types}) AND date >= %s AND date <= %s"
                ).format(schema=schema, types=pgsql.SQL(types_placeholders))
            )
            params.extend([*self.TMX_EARNINGS_TYPES, self._start_date, self._end_date])

        query = pgsql.SQL(" UNION ").join(parts)

        conn = psycopg2.connect(
            host=self._db_host,
            port=self._db_port,
            dbname=self._db_name,
            user=self._db_user,
            password=self._db_password,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
        finally:
            conn.close()

        logging.info(
            f"EarningsCalendar: fetched {len(rows)} earnings rows from postgres "
            f"(data_source={self._earnings_data_source})."
        )
        return [(row[0], row[1]) for row in rows]

    # -- Data source: Massive API ------------------------------------------

    def _query_earnings_from_api(self) -> list[tuple[str, date]]:
        if not self._api_key:
            msg = "api_key must be set when earnings_source='api'."
            raise ValueError(msg)

        results: list[tuple[str, date]] = []
        if self._earnings_data_source in ("benzinga", "both"):
            results.extend(self._fetch_benzinga_earnings_api())
        if self._earnings_data_source in ("tmx", "both"):
            results.extend(self._fetch_tmx_earnings_api())

        seen: set[tuple[str, date]] = set()
        deduped: list[tuple[str, date]] = []
        for item in results:
            if item not in seen:
                seen.add(item)
                deduped.append(item)

        logging.info(
            f"EarningsCalendar: fetched {len(deduped)} unique earnings rows from API "
            f"(data_source={self._earnings_data_source})."
        )
        return deduped

    def _paginate_api(self, url: str, query_params: dict) -> list[dict]:
        """Paginate through a Massive API endpoint."""
        import requests  # noqa: PLC0415

        query_params = {**query_params, "apiKey": self._api_key}
        all_results: list[dict] = []
        current_url: str | None = url

        while current_url:
            response = requests.get(
                current_url, params=query_params, timeout=self._request_timeout
            )
            response.raise_for_status()
            data = response.json()
            all_results.extend(data.get("results", []))
            current_url = data.get("next_url")
            if current_url:
                query_params = {"apiKey": self._api_key}

        return all_results

    def _fetch_benzinga_earnings_api(self) -> list[tuple[str, date]]:
        url = f"{self._base_url}/benzinga/v1/earnings"
        params = {
            "date.gte": self._start_date.isoformat(),
            "date.lte": self._end_date.isoformat(),
            "limit": 1000,
            "sort": "date",
        }
        records = self._paginate_api(url, params)
        results = []
        for r in records:
            ticker = r.get("ticker")
            raw_date = r.get("date")
            if ticker and raw_date:
                results.append((ticker, date.fromisoformat(str(raw_date)[:10])))
        return results

    def _fetch_tmx_earnings_api(self) -> list[tuple[str, date]]:
        url = f"{self._base_url}/tmx/v1/corporate-events"
        params = {
            "date.gte": self._start_date.isoformat(),
            "date.lte": self._end_date.isoformat(),
            "limit": 1000,
            "sort": "date",
        }
        records = self._paginate_api(url, params)
        results = []
        for r in records:
            event_type = r.get("type")
            if event_type not in self.TMX_EARNINGS_TYPES:
                continue
            ticker = r.get("ticker")
            raw_date = r.get("date")
            if ticker and raw_date:
                results.append((ticker, date.fromisoformat(str(raw_date)[:10])))
        return results


# ---------------------------------------------------------------------------
# Shared base for earnings quote streams
# ---------------------------------------------------------------------------


class _EarningsQuoteStreamBase(_NanosecondIncrementalMixin, MassiveRestStream):
    """Common logic for earnings-filtered quote streams."""

    replication_key = "sip_timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    is_sorted = False

    _use_cached_tickers_default = False
    _api_expects_unix_timestamp = True
    _unix_timestamp_unit = "ns"

    def get_starting_replication_key_value(
        self, context: Context | None
    ) -> t.Any | None:
        """Only use state for incremental recovery; ignore global start_date.

        The timestamp range for these streams is fully controlled by
        the partition's quote_date, so the global start_date config
        must not influence which records are accepted.
        """
        if self.replication_method != "INCREMENTAL" or not self.replication_key:
            return None
        state = self.get_context_state(context)
        if state:
            val = state.get(
                "replication_key_value", state.get("starting_replication_value")
            )
            if val is not None:
                return int(val) if isinstance(val, (int, float)) else val
        return None

    def _get_earnings_calendar(self) -> EarningsCalendar:
        """Get or create the cached EarningsCalendar from the tap."""
        start = self.other_params.get("earnings_start_date")
        end = self.other_params.get("earnings_end_date")
        if not start or not end:
            msg = (
                f"Stream {self.name} requires 'earnings_start_date' and "
                "'earnings_end_date' in other_params."
            )
            raise ValueError(msg)

        earnings_source = self.other_params.get("earnings_source", "postgres")
        db_schema = self.other_params.get("earnings_db_schema", "public")
        us_only = self.other_params.get("us_only", True)
        earnings_data_source = self.other_params.get("earnings_data_source", "both")
        return self._tap.get_earnings_calendar(
            earnings_source=earnings_source,
            start_date=start,
            end_date=end,
            db_schema=db_schema,
            us_only=us_only,
            earnings_data_source=earnings_data_source,
        )

    @staticmethod
    def _date_to_ns_range(quote_date_str: str) -> tuple[int, int]:
        """Convert a date string to (start_ns, end_ns) covering the full UTC day."""
        d = date.fromisoformat(quote_date_str)
        day_start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        start_ns = int(day_start.timestamp() * 1_000_000_000)
        end_ns = int(day_end.timestamp() * 1_000_000_000)
        return start_ns, end_ns


# ---------------------------------------------------------------------------
# StockEarningsQuotesStream
# ---------------------------------------------------------------------------


class StockEarningsQuotesStream(_EarningsQuoteStreamBase):
    """Tick-level stock quotes for tickers around earnings dates."""

    name = "stock_earnings_quotes"
    primary_keys = ["ticker", "sip_timestamp", "sequence_number"]
    state_partitioning_keys = ["ticker", "quote_date"]

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

    @property
    def partitions(self) -> list[dict[str, str]]:
        return self._get_earnings_calendar().get_stock_partitions()

    def get_url(self, context: Context) -> str:
        ticker = context.get("ticker")
        return f"{self.url_base}/v3/quotes/{ticker}"

    def get_records(
        self, context: dict[str, t.Any] | None
    ) -> t.Iterable[dict[str, t.Any]]:
        if not context:
            return

        quote_date_str = context.get("quote_date")
        if not quote_date_str:
            return

        start_ns, end_ns = self._date_to_ns_range(quote_date_str)

        _, base_query_params, base_path_params = self._prepare_context_and_params(
            dict(context)
        )
        base_query_params["timestamp.gte"] = start_ns
        base_query_params["timestamp.lte"] = end_ns

        ctx: dict[str, t.Any] = {
            "ticker": context["ticker"],
            "quote_date": quote_date_str,
            "query_params": base_query_params,
            "path_params": base_path_params,
        }
        yield from self.paginate_records(ctx)

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        row["ticker"] = context.get("ticker")
        row[self.replication_key] = safe_int(row.get(self.replication_key))
        return row


# ---------------------------------------------------------------------------
# OptionsEarningsQuotesStream
# ---------------------------------------------------------------------------

_OPTIONS_QUOTE_FIELDS = frozenset(
    {
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
)


class OptionsEarningsQuotesStream(_EarningsQuoteStreamBase):
    """Tick-level options quotes for contracts of earnings-window underlyings."""

    name = "options_earnings_quotes"
    primary_keys = ["ticker", "sip_timestamp", "sequence_number"]
    state_partitioning_keys = ["underlying"]

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

    @property
    def partitions(self) -> list[dict[str, str]]:
        return [
            {"underlying": u}
            for u in self._get_earnings_calendar().get_earnings_underlyings()
        ]

    def get_url(self, context: Context) -> str:
        ticker = context.get("ticker")
        return f"{self.url_base}/v3/quotes/{ticker}"

    def get_records(
        self, context: dict[str, t.Any] | None
    ) -> t.Iterable[dict[str, t.Any]]:
        underlying = (context or {}).get("underlying")
        if not underlying:
            return

        contracts = self._tap.get_option_contracts_for_underlying(underlying)
        if not contracts:
            logging.info(
                f"No option contracts for {underlying} in {self.name}, skipping."
            )
            return

        calendar = self._get_earnings_calendar()
        quote_dates = calendar.get_quote_dates_for_underlying(underlying)
        if not quote_dates:
            return

        state = self.get_context_state(context)
        initial_bookmark = state.copy() if state else {}

        _, base_query_params, base_path_params = self._prepare_context_and_params(
            dict(context)
        )

        for contract in contracts:
            for qd in quote_dates:
                if state is not None:
                    state.clear()
                    state.update(initial_bookmark)

                start_ns, end_ns = self._date_to_ns_range(qd.isoformat())
                qp = base_query_params.copy()
                qp["timestamp.gte"] = start_ns
                qp["timestamp.lte"] = end_ns

                contract_ctx: dict[str, t.Any] = {
                    "ticker": contract["ticker"],
                    "underlying": underlying,
                    "query_params": qp,
                    "path_params": base_path_params.copy(),
                }
                yield from self.paginate_records(contract_ctx)

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        row["ticker"] = context.get("ticker")
        row[self.replication_key] = safe_int(row.get(self.replication_key))
        return {k: row.get(k) for k in _OPTIONS_QUOTE_FIELDS if k in row}
