"""Massive tap class."""

from __future__ import annotations

import logging
import os
import threading
import typing as t
from collections import OrderedDict
from pathlib import Path, PurePath

from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.helpers._util import read_json_file

from tap_massive.base_streams import (
    AllConditionCodesStream,
    AllExchangesStream,
    AllTickersStream,
    AllTickerTypesStream,
    BaseTickerStream,
)
from tap_massive.benzinga_streams import (
    BenzingaAnalystDetailsStream,
    BenzingaAnalystInsightsStream,
    BenzingaAnalystRatingsStream,
    BenzingaBullsBearsSayStream,
    BenzingaConsensusRatingsStream,
    BenzingaCorporateGuidanceStream,
    BenzingaEarningsStream,
    BenzingaFirmDetailsStream,
    BenzingaNewsStream,
)
from tap_massive.crypto_streams import (
    CryptoBars1DayStream,
    CryptoBars1HourStream,
    CryptoBars1MinuteStream,
    CryptoBars1MonthStream,
    CryptoBars1SecondStream,
    CryptoBars1WeekStream,
    CryptoBars5MinuteStream,
    CryptoDailyMarketSummaryStream,
    CryptoDailyTickerSummaryStream,
    CryptoEmaStream,
    CryptoFullMarketSnapshotStream,
    CryptoLastTradeStream,
    CryptoMACDStream,
    CryptoPreviousDayBarStream,
    CryptoRSIStream,
    CryptoSmaStream,
    CryptoTickerDetailsStream,
    CryptoTickerSnapshotStream,
    CryptoTickerStream,
    CryptoTopMarketMoversStream,
    CryptoTradeStream,
)
from tap_massive.disk_cache import DiskCache, compute_fingerprint, resolve_home
from tap_massive.earnings_subset import (
    EarningsCalendar,
    EarningsFlatFilesStreamOption1m,
    EarningsFlatFilesStreamOptionEod,
    EarningsFlatFilesStreamOptionQuotes,
    EarningsFlatFilesStreamOptionTrades,
    EarningsFlatFilesStreamStock1m,
    EarningsFlatFilesStreamStockEod,
    EarningsFlatFilesStreamStockQuotes,
    EarningsFlatFilesStreamStockTrades,
    OptionsEarningsQuoteSnapshot1HourStream,
    OptionsEarningsQuoteSnapshot1MinuteStream,
    OptionsEarningsQuoteSnapshot1SecondStream,
    OptionsEarningsQuoteSnapshot5MinuteStream,
    OptionsEarningsQuoteSnapshot30MinuteStream,
    OptionsEarningsQuoteSnapshot30SecondStream,
    OptionsEarningsQuotesStream,
    StockEarningsQuoteSnapshot1HourStream,
    StockEarningsQuoteSnapshot1MinuteStream,
    StockEarningsQuoteSnapshot1SecondStream,
    StockEarningsQuoteSnapshot5MinuteStream,
    StockEarningsQuoteSnapshot30MinuteStream,
    StockEarningsQuoteSnapshot30SecondStream,
    StockEarningsQuotesStream,
)
from tap_massive.economy_streams import (
    InflationExpectationsStream,
    InflationStream,
    LaborMarketStream,
    TreasuryYieldStream,
)
from tap_massive.etf_global_streams import (
    EtfGlobalAnalyticsStream,
    EtfGlobalConstituentsStream,
    EtfGlobalFundFlowsStream,
    EtfGlobalProfilesStream,
    EtfGlobalTaxonomiesStream,
)
from tap_massive.flat_files_streams import (
    FlatFilesStreamCrypto1m,
    FlatFilesStreamCryptoEod,
    FlatFilesStreamCryptoTrades,
    FlatFilesStreamForex1m,
    FlatFilesStreamForexEod,
    FlatFilesStreamForexQuotes,
    FlatFilesStreamFuture1m,
    FlatFilesStreamFutureEod,
    FlatFilesStreamFutureQuotes,
    FlatFilesStreamFutureTrades,
    FlatFilesStreamIndex1m,
    FlatFilesStreamIndexEod,
    FlatFilesStreamIndexValues,
    FlatFilesStreamOption1m,
    FlatFilesStreamOptionEod,
    FlatFilesStreamOptionQuotes,
    FlatFilesStreamOptionTrades,
    FlatFilesStreamStock1m,
    FlatFilesStreamStockEod,
    FlatFilesStreamStockQuotes,
    FlatFilesStreamStockTrades,
    OptionsQuoteSnapshotFlatFiles1MinuteStream,
    OptionsQuoteSnapshotFlatFiles1SecondStream,
    OptionsQuoteSnapshotFlatFiles5MinuteStream,
    OptionsQuoteSnapshotFlatFiles30MinuteStream,
    OptionsQuoteSnapshotFlatFiles30SecondStream,
)
from tap_massive.forex_streams import (
    ForexBars1DayStream,
    ForexBars1HourStream,
    ForexBars1MinuteStream,
    ForexBars1MonthStream,
    ForexBars1SecondStream,
    ForexBars1WeekStream,
    ForexBars5MinuteStream,
    ForexCurrencyConversionStream,
    ForexDailyMarketSummaryStream,
    ForexEmaStream,
    ForexFullMarketSnapshotStream,
    ForexLastQuoteStream,
    ForexMACDStream,
    ForexPreviousDayBarStream,
    ForexQuoteStream,
    ForexRSIStream,
    ForexSmaStream,
    ForexTickerDetailsStream,
    ForexTickerSnapshotStream,
    ForexTickerStream,
    ForexTopMarketMoversStream,
)
from tap_massive.fundamental_streams import (
    Stock8KTextStream,
    Stock10KSectionsStream,
    StockBalanceSheetsStream,
    StockCashFlowStatementsStream,
    StockFilingsIndexStream,
    StockFinancialsDeprecatedStream,
    StockFloatStream,
    StockIncomeStatementsStream,
    StockNewsStream,
    StockRatiosStream,
    StockRiskCategoriesStream,
    StockRiskFactorsStream,
    StockShortInterestStream,
    StockShortVolumeStream,
)
from tap_massive.futures_streams import (
    FuturesBars1DayStream,
    FuturesBars1HourStream,
    FuturesBars1MinuteStream,
    FuturesBars1MonthStream,
    FuturesBars1SecondStream,
    FuturesBars1WeekStream,
    FuturesBars5MinuteStream,
    FuturesContractsSnapshotStream,
    FuturesContractsStream,
    FuturesExchangesStream,
    FuturesMarketStatusStream,
    FuturesProductsStream,
    FuturesQuoteStream,
    FuturesSchedulesStream,
    FuturesTradeStream,
)
from tap_massive.indices_streams import (
    IndicesBars1DayStream,
    IndicesBars1HourStream,
    IndicesBars1MinuteStream,
    IndicesBars1MonthStream,
    IndicesBars1SecondStream,
    IndicesBars1WeekStream,
    IndicesBars5MinuteStream,
    IndicesDailyTickerSummaryStream,
    IndicesEmaStream,
    IndicesMACDStream,
    IndicesPreviousDayBarStream,
    IndicesRSIStream,
    IndicesSmaStream,
    IndicesSnapshotStream,
    IndicesTickerDetailsStream,
    IndicesTickerStream,
)
from tap_massive.option_streams import (
    ActiveOptionsContractsStream,
    OptionsBars1DayStream,
    OptionsBars1HourStream,
    OptionsBars1MinuteStream,
    OptionsBars1MonthStream,
    OptionsBars1SecondStream,
    OptionsBars1WeekStream,
    OptionsBars5MinuteStream,
    OptionsBars30MinuteStream,
    OptionsBars30SecondStream,
    OptionsChainSnapshotStream,
    OptionsConditionCodesStream,
    OptionsContractOverviewStream,
    OptionsContractSnapshotStream,
    OptionsContractsStream,
    OptionsDailyTickerSummaryStream,
    OptionsEmaStream,
    OptionsExchangesStream,
    OptionsLastTradeStream,
    OptionsMACDStream,
    OptionsPreviousDayBarStream,
    OptionsQuoteSnapshot1HourStream,
    OptionsQuoteSnapshot1MinuteStream,
    OptionsQuoteSnapshot1SecondStream,
    OptionsQuoteSnapshot5MinuteStream,
    OptionsQuoteSnapshot30MinuteStream,
    OptionsQuoteSnapshot30SecondStream,
    OptionsQuoteStream,
    OptionsRSIStream,
    OptionsSmaStream,
    OptionsTradeStream,
    OptionsUnifiedSnapshotStream,
)
from tap_massive.stock_streams import (
    EmaStream,
    MACDStream,
    MarketHolidaysStream,
    MarketStatusStream,
    MassiveRestStream,
    RSIStream,
    SmaStream,
    StockBars1DayStream,
    StockBars1HourStream,
    StockBars1MinuteStream,
    StockBars1MonthStream,
    StockBars1SecondStream,
    StockBars1WeekStream,
    StockBars5MinuteStream,
    StockBars30MinuteStream,
    StockBars30SecondStream,
    StockConditionCodesStream,
    StockDailyMarketSummaryStream,
    StockDailyTickerSummaryStream,
    StockDividendsDeprecatedStream,
    StockDividendsStream,
    StockExchangesStream,
    StockFullMarketSnapshotStream,
    StockIPOsStream,
    StockLastQuoteStream,
    StockLastTradeStream,
    StockPreviousDayBarSummaryStream,
    StockQuoteSnapshot1HourStream,
    StockQuoteSnapshot1MinuteStream,
    StockQuoteSnapshot1SecondStream,
    StockQuoteSnapshot5MinuteStream,
    StockQuoteSnapshot30MinuteStream,
    StockQuoteSnapshot30SecondStream,
    StockQuoteStream,
    StockRelatedTickersStream,
    StockSplitsDeprecatedStream,
    StockSplitsStream,
    StockTickerDetailsStream,
    StockTickerEventsStream,
    StockTickerSnapshotStream,
    StockTickerStream,
    StockTickerTypesStream,
    StockTopMarketMoversStream,
    StockTradeStream,
    StockUnifiedSnapshotStream,
)
from tap_massive.tmx_streams import TmxCorporateEventsStream
from tap_massive.utils import compute_nyse_market_windows, compute_nyse_session_opens


class TapMassive(Tap):
    name = "tap-massive"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
        ),
        th.Property(
            "request_timeout",
            th.IntegerType,
            default=300,
        ),
        th.Property("postgres_host", th.StringType),
        th.Property("postgres_port", th.StringType, default="5432"),
        th.Property("postgres_database", th.StringType),
        th.Property("postgres_user", th.StringType),
        th.Property("postgres_password", th.StringType, secret=True),
        th.Property("flat_files_base_dir", th.StringType),
        th.Property("flat_files_aws_key", th.StringType, secret=True),
        th.Property("flat_files_aws_secret", th.StringType, secret=True),
        th.Property("disk_cache_ttl_hours", th.NumberType, default=36),
        th.Property(
            "duckdb_params",
            th.ObjectType(
                th.Property("threads", th.IntegerType),
                th.Property("memory_limit", th.StringType),
                th.Property("temp_directory", th.StringType),
                th.Property("preserve_insertion_order", th.BooleanType),
                th.Property("duckdb_batch_size", th.IntegerType),
            ),
        ),
    ).to_dict()

    # Ticker caching: one registry instead of 6 copy-pasted blocks.
    # Each entry maps asset name -> stream class used to fetch tickers.
    _ticker_stream_classes: t.ClassVar[dict[str, type]] = {
        "stock": StockTickerStream,
        "option": OptionsContractsStream,
        "forex": ForexTickerStream,
        "crypto": CryptoTickerStream,
        "indices": IndicesTickerStream,
        "futures": FuturesContractsStream,
    }

    def __init__(self, *args, **kwargs):
        self._ticker_caches: dict[str, list[dict] | None] = {}
        self._ticker_streams: dict[str, MassiveRestStream | None] = {}
        self._ticker_locks: dict[str, threading.Lock] = {
            asset: threading.Lock() for asset in self._ticker_stream_classes
        }
        self._earnings_calendar_cache: dict[str, EarningsCalendar] = {}
        self._earnings_calendar_lock = threading.Lock()
        self._market_windows_cache: dict[str, list[int]] = {}
        self._market_windows_lock = threading.Lock()
        self._nyse_sessions_cache: dict[str, list[tuple[int, int]]] = {}
        self._nyse_sessions_lock = threading.Lock()
        self._option_contracts_cache: OrderedDict[str, list[dict]] = OrderedDict()
        self._option_contracts_lock = threading.Lock()
        self._option_contracts_cache_max = 20  # LRU: keep last N underlyings
        catalog_arg = kwargs.get("catalog")
        if isinstance(catalog_arg, (str, PurePath)):
            kwargs["catalog"] = read_json_file(catalog_arg)
        state_arg = kwargs.get("state")
        if isinstance(state_arg, (str, PurePath)):
            kwargs["state"] = read_json_file(state_arg)
        super().__init__(*args, **kwargs)

        shared_cache_dir = os.environ.get(
            "MELTANO_SHARED_CACHE_DIR",
            str(resolve_home(Path("~/.cache"))),
        )
        self._disk_cache: DiskCache | None = DiskCache(
            cache_dir=shared_cache_dir,
            namespace="tap_massive",
            ttl_hours=float(self.config.get("disk_cache_ttl_hours", 36)),
        )

    def _get_ticker_stream(self, asset: str) -> MassiveRestStream:
        """Get or create the ticker stream instance for an asset class."""
        if self._ticker_streams.get(asset) is None:
            cls = self._ticker_stream_classes[asset]
            self.logger.info(f"Creating {cls.__name__} instance...")
            self._ticker_streams[asset] = cls(self)
        return self._ticker_streams[asset]

    _DISK_CACHEABLE_ASSETS = frozenset(
        {"stock", "forex", "crypto", "indices", "futures"}
    )

    def _build_cache_fingerprint(
        self,
        *,
        query_params: dict[str, t.Any],
        other_params: dict[str, t.Any] | None = None,
        exclude_qp_keys: frozenset[str] = frozenset({"apiKey"}),
        extra_canonical: dict[str, t.Any] | None = None,
        boolean_flag_key: str = "",
    ) -> str:
        """Build a config-aware cache fingerprint from effective stream params.

        Uses the stream's already-parsed query_params and other_params (set by
        parse_config_params() in __init__), not raw config sections. This ensures
        the fingerprint captures the effective config regardless of which config
        section it originated from (e.g., futures_contracts vs futures_tickers).
        """
        other = other_params or {}
        normalized_qp = {
            k.replace("__", "."): str(v)
            for k, v in sorted(query_params.items())
            if k not in exclude_qp_keys
        }
        canonical: dict[str, t.Any] = {
            "base_url": self.config.get("base_url", ""),
            "query_params": normalized_qp,
            "boolean_flag_mode": (
                other.get(boolean_flag_key) if boolean_flag_key else None
            ),
        }
        if extra_canonical:
            canonical.update(extra_canonical)
        return compute_fingerprint(canonical)

    @staticmethod
    def _fetch_and_sort_tickers(stream: MassiveRestStream) -> list[dict]:
        """Fetch all tickers from a stream and sort by ticker field."""
        tickers = list(stream.get_records(context=None))
        tickers.sort(key=lambda rec: rec.get("ticker", ""))
        return tickers

    def _get_cached_tickers(self, asset: str) -> list[dict]:
        """Thread-safe cached ticker fetch for any asset class."""
        if self._ticker_caches.get(asset) is None:
            with self._ticker_locks[asset]:
                if self._ticker_caches.get(asset) is None:
                    self.logger.info(f"Fetching and caching {asset} tickers...")
                    stream = self._get_ticker_stream(asset)

                    if (
                        asset in self._DISK_CACHEABLE_ASSETS
                        and self._disk_cache is not None
                    ):
                        select_tickers = stream.get_ticker_list()
                        fingerprint = self._build_cache_fingerprint(
                            query_params=stream.query_params,
                            other_params=stream.other_params,
                            boolean_flag_key=getattr(stream, "_boolean_flag_key", ""),
                            extra_canonical={
                                "asset": asset,
                                "select_tickers": (
                                    select_tickers
                                    if select_tickers is not None
                                    else "*"
                                ),
                            },
                        )
                        cache_key = f"tickers/{asset}/{fingerprint}"
                        self._ticker_caches[asset] = self._disk_cache.get_or_fetch(
                            cache_key, lambda s=stream: self._fetch_and_sort_tickers(s)
                        )
                    else:
                        self._ticker_caches[asset] = self._fetch_and_sort_tickers(
                            stream
                        )

                    self.logger.info(
                        f"Cached {len(self._ticker_caches[asset])} {asset} tickers."
                    )
        return self._ticker_caches[asset]

    # Public accessors — thin wrappers for callers and _cached_tickers_getter.
    def get_stock_tickers_stream(self) -> StockTickerStream:
        return self._get_ticker_stream("stock")

    def get_cached_stock_tickers(self) -> list[dict]:
        return self._get_cached_tickers("stock")

    def get_option_tickers_stream(self) -> OptionsContractsStream:
        return self._get_ticker_stream("option")

    def get_cached_option_tickers(self) -> list[dict]:
        return self._get_cached_tickers("option")

    def _paginate_option_contracts(
        self, underlying: str, query_params: dict
    ) -> list[dict]:
        """Paginate through all option contracts for an underlying ticker."""
        query_params = query_params.copy()
        query_params["underlying_ticker"] = underlying
        query_params["apiKey"] = self.config["api_key"]
        query_params.setdefault("limit", 1000)
        query_params.setdefault("sort", "ticker")

        # Normalize __ separators to . for API
        query_params = MassiveRestStream._normalize_cfg_param_keys(query_params)

        base_url = self.config.get("base_url", "https://api.massive.com")
        url: str | None = f"{base_url}/v3/reference/options/contracts"
        option_tickers_stream = self.get_option_tickers_stream()

        contracts: list[dict] = []
        while url:
            response = option_tickers_stream.get_response(url, query_params)
            if response is None:
                break
            data = response.json()
            contracts.extend(data.get("results", []))
            url = data.get("next_url")
            if url:
                query_params = {"apiKey": self.config["api_key"]}

        return contracts

    _OPTION_CONTRACTS_EXCLUDE_QP = frozenset(
        {"apiKey", "underlying_ticker", "limit", "sort"}
    )
    _option_contracts_fingerprint_cache: str | None = None

    def _option_contracts_fingerprint(
        self, query_params: dict, other_params: dict
    ) -> str:
        """Compute and cache the disk-cache fingerprint for option contracts.

        The fingerprint depends only on config (not on the underlying ticker),
        so it's the same for every call and can be cached.
        """
        if self._option_contracts_fingerprint_cache is None:
            self._option_contracts_fingerprint_cache = self._build_cache_fingerprint(
                query_params=query_params,
                other_params=other_params,
                exclude_qp_keys=self._OPTION_CONTRACTS_EXCLUDE_QP,
                extra_canonical={
                    "expired_mode": other_params.get("expired"),
                    "version": "v1",
                },
            )
        return self._option_contracts_fingerprint_cache

    def get_option_contracts_for_underlying(self, underlying: str) -> list[dict]:
        """Thread-safe LRU-cached fetch of option contracts for a single underlying.

        Supports ``option_tickers.other_params.expired: "both"`` to fetch
        expired and active contracts in two passes and union by ticker.
        Caches last N underlyings (LRU) to bound memory for full-universe runs.
        L2 disk cache (when MELTANO_SHARED_CACHE_DIR is set) shares contract data
        across parallel Meltano subprocesses.
        """
        # Double-checked locking: fast path without lock for cache hits
        if underlying in self._option_contracts_cache:
            with self._option_contracts_lock:
                if underlying in self._option_contracts_cache:
                    self._option_contracts_cache.move_to_end(underlying)
                    return self._option_contracts_cache[underlying]

        # Cache miss: fetch outside lock, then store under lock
        option_cfg = self.config.get("option_tickers", {})
        option_qp = option_cfg.get("query_params", {})
        option_other = option_cfg.get("other_params", {})

        if self._disk_cache is not None:
            fingerprint = self._option_contracts_fingerprint(option_qp, option_other)
            disk_key = f"options_contracts/v1/{underlying}/{fingerprint}"
            contracts = self._disk_cache.get_or_fetch(
                disk_key,
                lambda u=underlying, qp=option_qp, op=option_other: self._fetch_option_contracts(
                    u, qp, op
                ),
            )
        else:
            contracts = self._fetch_option_contracts(
                underlying, option_qp, option_other
            )

        with self._option_contracts_lock:
            # Evict oldest if at capacity
            while len(self._option_contracts_cache) >= self._option_contracts_cache_max:
                evicted = self._option_contracts_cache.popitem(last=False)
                logging.debug(f"Evicted contract cache for {evicted[0]}.")
            self._option_contracts_cache[underlying] = contracts

        return contracts

    def _fetch_option_contracts(
        self,
        underlying: str,
        query_params: dict[str, t.Any] | None = None,
        other_params: dict[str, t.Any] | None = None,
    ) -> list[dict]:
        """Fetch and deduplicate option contracts for a single underlying."""
        if query_params is None or other_params is None:
            option_cfg = self.config.get("option_tickers", {})
            query_params = query_params or option_cfg.get("query_params", {})
            other_params = other_params or option_cfg.get("other_params", {})
        query_params = query_params.copy()
        expired_mode = other_params.get("expired")
        query_variants = BaseTickerStream.boolean_query_variants(
            query_params, "expired", expired_mode
        )
        contracts: list[dict] = []
        for params in query_variants:
            variant_expired = params.get("expired")
            variant_contracts = self._paginate_option_contracts(underlying, params)
            for contract in variant_contracts:
                if variant_expired in (True, False) and contract.get("expired") is None:
                    contract = {**contract, "expired": variant_expired}
                contracts.append(contract)

        if len(query_variants) > 1:
            # Dedupe by ticker for expired+active union.
            seen: dict[str, dict] = {}
            for contract in contracts:
                seen.setdefault(contract["ticker"], contract)
            contracts = list(seen.values())

        # Unconditional sort ensures deterministic ordering for disk caching
        # regardless of API sort param.
        contracts.sort(key=lambda c: c["ticker"])

        logging.info(f"Fetched {len(contracts)} option contracts for {underlying}.")
        return contracts

    def get_cached_forex_tickers(self) -> list[dict]:
        return self._get_cached_tickers("forex")

    def get_cached_crypto_tickers(self) -> list[dict]:
        return self._get_cached_tickers("crypto")

    def get_cached_indices_tickers(self) -> list[dict]:
        return self._get_cached_tickers("indices")

    def get_cached_futures_tickers(self) -> list[dict]:
        return self._get_cached_tickers("futures")

    def get_earnings_calendar(
        self,
        *,
        earnings_source: str,
        start_date: str,
        end_date: str,
        db_schema: str = "public",
        us_only: bool = True,
        earnings_data_source: str = "both",
    ) -> EarningsCalendar:
        """Thread-safe cached earnings calendar construction."""
        cache_key = (
            f"{earnings_source}:{start_date}:{end_date}:"
            f"{db_schema}:{us_only}:{earnings_data_source}"
        )
        if cache_key not in self._earnings_calendar_cache:
            with self._earnings_calendar_lock:
                if cache_key not in self._earnings_calendar_cache:
                    self.logger.info(
                        f"Building EarningsCalendar ({earnings_source}) "
                        f"for {start_date} to {end_date} "
                        f"(schema={db_schema}, us_only={us_only}, "
                        f"data_source={earnings_data_source})..."
                    )
                    self._earnings_calendar_cache[cache_key] = EarningsCalendar(
                        earnings_source=earnings_source,
                        start_date=start_date,
                        end_date=end_date,
                        db_host=self.config.get("postgres_host"),
                        db_port=int(self.config.get("postgres_port", 5432)),
                        db_name=self.config.get("postgres_database"),
                        db_user=self.config.get("postgres_user"),
                        db_password=self.config.get("postgres_password"),
                        db_schema=db_schema,
                        us_only=us_only,
                        earnings_data_source=earnings_data_source,
                        api_key=self.config.get("api_key"),
                        base_url=self.config.get("base_url", "https://api.massive.com"),
                        request_timeout=self.config.get("request_timeout", 300),
                    )
        return self._earnings_calendar_cache[cache_key]

    def get_market_windows(
        self, interval_seconds: int, start_ns: int, end_ns: int
    ) -> list[int]:
        """Thread-safe cached market window computation for quote snapshot streams.

        Returns sorted list of asof_timestamp nanosecond timestamps during NYSE hours
        for the given interval and time range.
        """
        cache_key = f"{interval_seconds}:{start_ns}:{end_ns}"
        if cache_key not in self._market_windows_cache:
            with self._market_windows_lock:
                if cache_key not in self._market_windows_cache:
                    self._market_windows_cache[cache_key] = compute_nyse_market_windows(
                        interval_seconds, start_ns, end_ns
                    )
        return self._market_windows_cache[cache_key]

    def get_nyse_sessions(self, start_ns: int, end_ns: int) -> list[tuple[int, int]]:
        """Thread-safe cached NYSE session boundaries for freshness checks."""
        cache_key = f"{start_ns}:{end_ns}"
        if cache_key not in self._nyse_sessions_cache:
            with self._nyse_sessions_lock:
                if cache_key not in self._nyse_sessions_cache:
                    self._nyse_sessions_cache[cache_key] = compute_nyse_session_opens(
                        start_ns, end_ns
                    )
        return self._nyse_sessions_cache[cache_key]

    def discover_streams(self) -> list[MassiveRestStream]:
        stock_tickers_stream = self.get_stock_tickers_stream()

        streams: list[MassiveRestStream] = [
            # Stock streams
            stock_tickers_stream,
            StockTickerDetailsStream(self),
            StockTickerTypesStream(self),
            StockRelatedTickersStream(self),
            StockDailyMarketSummaryStream(self),
            StockDailyTickerSummaryStream(self),
            StockPreviousDayBarSummaryStream(self),
            StockTopMarketMoversStream(self),
            StockTickerSnapshotStream(self),
            StockFullMarketSnapshotStream(self),
            StockUnifiedSnapshotStream(self),
            StockTradeStream(self),
            StockLastTradeStream(self),
            StockQuoteStream(self),
            StockLastQuoteStream(self),
            StockQuoteSnapshot1SecondStream(self),
            StockQuoteSnapshot30SecondStream(self),
            StockQuoteSnapshot1MinuteStream(self),
            StockQuoteSnapshot5MinuteStream(self),
            StockQuoteSnapshot30MinuteStream(self),
            StockQuoteSnapshot1HourStream(self),
            SmaStream(self),
            EmaStream(self),
            MACDStream(self),
            RSIStream(self),
            # Cross-asset reference streams (All + Stock-specific)
            AllTickersStream(self),
            AllTickerTypesStream(self),
            AllExchangesStream(self),
            AllConditionCodesStream(self),
            StockExchangesStream(self),
            StockConditionCodesStream(self),
            MarketHolidaysStream(self),
            MarketStatusStream(self),
            StockIPOsStream(self),
            StockSplitsDeprecatedStream(self),
            StockSplitsStream(self),
            StockDividendsDeprecatedStream(self),
            StockDividendsStream(self),
            StockTickerEventsStream(self),
            StockFinancialsDeprecatedStream(self),
            StockBalanceSheetsStream(self),
            StockCashFlowStatementsStream(self),
            StockIncomeStatementsStream(self),
            StockRatiosStream(self),
            StockShortInterestStream(self),
            StockShortVolumeStream(self),
            StockFloatStream(self),
            Stock8KTextStream(self),
            Stock10KSectionsStream(self),
            StockFilingsIndexStream(self),
            StockRiskFactorsStream(self),
            StockRiskCategoriesStream(self),
            StockNewsStream(self),
            StockBars1SecondStream(self),
            StockBars30SecondStream(self),
            StockBars1MinuteStream(self),
            StockBars5MinuteStream(self),
            StockBars30MinuteStream(self),
            StockBars1HourStream(self),
            StockBars1DayStream(self),
            StockBars1WeekStream(self),
            StockBars1MonthStream(self),
            # Options streams
            OptionsContractsStream(self),
            ActiveOptionsContractsStream(self),
            OptionsContractOverviewStream(self),
            OptionsExchangesStream(self),
            OptionsConditionCodesStream(self),
            OptionsBars1SecondStream(self),
            OptionsBars30SecondStream(self),
            OptionsBars1MinuteStream(self),
            OptionsBars5MinuteStream(self),
            OptionsBars30MinuteStream(self),
            OptionsBars1HourStream(self),
            OptionsBars1DayStream(self),
            OptionsBars1WeekStream(self),
            OptionsBars1MonthStream(self),
            OptionsDailyTickerSummaryStream(self),
            OptionsPreviousDayBarStream(self),
            OptionsTradeStream(self),
            OptionsLastTradeStream(self),
            OptionsQuoteStream(self),
            OptionsQuoteSnapshot1SecondStream(self),
            OptionsQuoteSnapshot30SecondStream(self),
            OptionsQuoteSnapshot1MinuteStream(self),
            OptionsQuoteSnapshot5MinuteStream(self),
            OptionsQuoteSnapshot30MinuteStream(self),
            OptionsQuoteSnapshot1HourStream(self),
            OptionsContractSnapshotStream(self),
            OptionsChainSnapshotStream(self),
            OptionsUnifiedSnapshotStream(self),
            OptionsSmaStream(self),
            OptionsEmaStream(self),
            OptionsMACDStream(self),
            OptionsRSIStream(self),
            # Forex streams
            ForexTickerStream(self),
            ForexTickerDetailsStream(self),
            ForexBars1SecondStream(self),
            ForexBars1MinuteStream(self),
            ForexBars5MinuteStream(self),
            ForexBars1HourStream(self),
            ForexBars1DayStream(self),
            ForexBars1WeekStream(self),
            ForexBars1MonthStream(self),
            ForexDailyMarketSummaryStream(self),
            ForexPreviousDayBarStream(self),
            ForexQuoteStream(self),
            ForexLastQuoteStream(self),
            ForexTopMarketMoversStream(self),
            ForexTickerSnapshotStream(self),
            ForexFullMarketSnapshotStream(self),
            ForexSmaStream(self),
            ForexEmaStream(self),
            ForexMACDStream(self),
            ForexRSIStream(self),
            ForexCurrencyConversionStream(self),
            # Crypto streams
            CryptoTickerStream(self),
            CryptoTickerDetailsStream(self),
            CryptoBars1SecondStream(self),
            CryptoBars1MinuteStream(self),
            CryptoBars5MinuteStream(self),
            CryptoBars1HourStream(self),
            CryptoBars1DayStream(self),
            CryptoBars1WeekStream(self),
            CryptoBars1MonthStream(self),
            CryptoDailyMarketSummaryStream(self),
            CryptoDailyTickerSummaryStream(self),
            CryptoPreviousDayBarStream(self),
            CryptoTradeStream(self),
            CryptoLastTradeStream(self),
            CryptoTopMarketMoversStream(self),
            CryptoTickerSnapshotStream(self),
            CryptoFullMarketSnapshotStream(self),
            CryptoSmaStream(self),
            CryptoEmaStream(self),
            CryptoMACDStream(self),
            CryptoRSIStream(self),
            # Indices streams
            IndicesTickerStream(self),
            IndicesTickerDetailsStream(self),
            IndicesBars1SecondStream(self),
            IndicesBars1MinuteStream(self),
            IndicesBars5MinuteStream(self),
            IndicesBars1HourStream(self),
            IndicesBars1DayStream(self),
            IndicesBars1WeekStream(self),
            IndicesBars1MonthStream(self),
            IndicesDailyTickerSummaryStream(self),
            IndicesPreviousDayBarStream(self),
            IndicesSnapshotStream(self),
            IndicesSmaStream(self),
            IndicesEmaStream(self),
            IndicesMACDStream(self),
            IndicesRSIStream(self),
            # Futures streams
            FuturesContractsStream(self),
            FuturesProductsStream(self),
            FuturesSchedulesStream(self),
            FuturesExchangesStream(self),
            FuturesMarketStatusStream(self),
            FuturesBars1SecondStream(self),
            FuturesBars1MinuteStream(self),
            FuturesBars5MinuteStream(self),
            FuturesBars1HourStream(self),
            FuturesBars1DayStream(self),
            FuturesBars1WeekStream(self),
            FuturesBars1MonthStream(self),
            FuturesTradeStream(self),
            FuturesQuoteStream(self),
            FuturesContractsSnapshotStream(self),
            # Economy streams
            TreasuryYieldStream(self),
            InflationExpectationsStream(self),
            InflationStream(self),
            LaborMarketStream(self),
            # Benzinga streams
            BenzingaAnalystDetailsStream(self),
            BenzingaAnalystInsightsStream(self),
            BenzingaAnalystRatingsStream(self),
            BenzingaBullsBearsSayStream(self),
            BenzingaConsensusRatingsStream(self),
            BenzingaCorporateGuidanceStream(self),
            BenzingaEarningsStream(self),
            BenzingaFirmDetailsStream(self),
            BenzingaNewsStream(self),
            # TMX streams
            TmxCorporateEventsStream(self),
            # ETF Global streams
            EtfGlobalConstituentsStream(self),
            EtfGlobalFundFlowsStream(self),
            EtfGlobalAnalyticsStream(self),
            EtfGlobalProfilesStream(self),
            EtfGlobalTaxonomiesStream(self),
            # Earnings-filtered quote streams
            StockEarningsQuotesStream(self),
            OptionsEarningsQuotesStream(self),
            # Earnings-filtered quote update bar streams
            StockEarningsQuoteSnapshot1SecondStream(self),
            StockEarningsQuoteSnapshot30SecondStream(self),
            StockEarningsQuoteSnapshot1MinuteStream(self),
            StockEarningsQuoteSnapshot5MinuteStream(self),
            StockEarningsQuoteSnapshot30MinuteStream(self),
            StockEarningsQuoteSnapshot1HourStream(self),
            OptionsEarningsQuoteSnapshot1SecondStream(self),
            OptionsEarningsQuoteSnapshot30SecondStream(self),
            OptionsEarningsQuoteSnapshot1MinuteStream(self),
            OptionsEarningsQuoteSnapshot5MinuteStream(self),
            OptionsEarningsQuoteSnapshot30MinuteStream(self),
            OptionsEarningsQuoteSnapshot1HourStream(self),
            # Flat file streams
            FlatFilesStreamStockEod(self),
            FlatFilesStreamStock1m(self),
            FlatFilesStreamStockTrades(self),
            FlatFilesStreamStockQuotes(self),
            FlatFilesStreamOptionEod(self),
            FlatFilesStreamOption1m(self),
            FlatFilesStreamOptionTrades(self),
            FlatFilesStreamOptionQuotes(self),
            OptionsQuoteSnapshotFlatFiles1SecondStream(self),
            OptionsQuoteSnapshotFlatFiles30SecondStream(self),
            OptionsQuoteSnapshotFlatFiles1MinuteStream(self),
            OptionsQuoteSnapshotFlatFiles5MinuteStream(self),
            OptionsQuoteSnapshotFlatFiles30MinuteStream(self),
            FlatFilesStreamIndexEod(self),
            FlatFilesStreamIndex1m(self),
            FlatFilesStreamIndexValues(self),
            FlatFilesStreamForexEod(self),
            FlatFilesStreamForex1m(self),
            FlatFilesStreamForexQuotes(self),
            FlatFilesStreamCryptoEod(self),
            FlatFilesStreamCrypto1m(self),
            FlatFilesStreamCryptoTrades(self),
            FlatFilesStreamFutureEod(self),
            FlatFilesStreamFuture1m(self),
            FlatFilesStreamFutureTrades(self),
            FlatFilesStreamFutureQuotes(self),
            # Earnings-filtered flat file streams
            EarningsFlatFilesStreamStockEod(self),
            EarningsFlatFilesStreamStock1m(self),
            EarningsFlatFilesStreamStockTrades(self),
            EarningsFlatFilesStreamStockQuotes(self),
            EarningsFlatFilesStreamOptionEod(self),
            EarningsFlatFilesStreamOption1m(self),
            EarningsFlatFilesStreamOptionTrades(self),
            EarningsFlatFilesStreamOptionQuotes(self),
        ]

        return streams


if __name__ == "__main__":
    TapMassive.cli()
