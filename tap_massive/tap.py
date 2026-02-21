"""Massive tap class."""

from __future__ import annotations

import logging
import threading
import typing as t

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_massive.base_streams import (
    AllConditionCodesStream,
    AllExchangesStream,
    AllTickerTypesStream,
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
from tap_massive.economy_streams import (
    InflationExpectationsStream,
    InflationStream,
    LaborMarketStream,
    TreasuryYieldStream,
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
    Stock10KSectionsStream,
    StockBalanceSheetsStream,
    StockCashFlowStatementsStream,
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
    OptionsQuoteStream,
    OptionsRSIStream,
    OptionsSmaStream,
    OptionsTradeStream,
    OptionsUnifiedSnapshotStream,
    expired_query_variants,
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


class TapMassive(Tap):
    name = "tap-massive"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
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
        super().__init__(*args, **kwargs)

    def _get_ticker_stream(self, asset: str) -> MassiveRestStream:
        """Get or create the ticker stream instance for an asset class."""
        if self._ticker_streams.get(asset) is None:
            cls = self._ticker_stream_classes[asset]
            self.logger.info(f"Creating {cls.__name__} instance...")
            self._ticker_streams[asset] = cls(self)
        return self._ticker_streams[asset]

    def _get_cached_tickers(self, asset: str) -> list[dict]:
        """Thread-safe cached ticker fetch for any asset class."""
        if self._ticker_caches.get(asset) is None:
            with self._ticker_locks[asset]:
                if self._ticker_caches.get(asset) is None:
                    self.logger.info(f"Fetching and caching {asset} tickers...")
                    stream = self._get_ticker_stream(asset)
                    self._ticker_caches[asset] = list(stream.get_records(context=None))
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
        query_params = {k.replace("__", "."): v for k, v in query_params.items()}

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

    def get_option_contracts_for_underlying(self, underlying: str) -> list[dict]:
        """Fetch all option contracts for a single underlying ticker.

        Supports ``option_tickers.other_params.expired: "both"`` to fetch
        expired and active contracts in two passes and union by ticker.
        """
        option_cfg = self.config.get("option_tickers", {})
        query_params = option_cfg.get("query_params", {}).copy()
        other_params = option_cfg.get("other_params", {})
        expired_mode = other_params.get("expired")
        query_variants = expired_query_variants(query_params, expired_mode)
        contracts: list[dict] = []
        for params in query_variants:
            contracts.extend(self._paginate_option_contracts(underlying, params))

        if len(query_variants) > 1:
            # Dedupe by ticker for expired+active union and keep deterministic ordering.
            seen: dict[str, dict] = {}
            for contract in contracts:
                seen.setdefault(contract["ticker"], contract)
            contracts = sorted(seen.values(), key=lambda c: c["ticker"])

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
            SmaStream(self),
            EmaStream(self),
            MACDStream(self),
            RSIStream(self),
            # Cross-asset reference streams (All + Stock-specific)
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
            Stock10KSectionsStream(self),
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
        ]

        return streams


if __name__ == "__main__":
    TapMassive.cli()
