"""Massive tap class."""

from __future__ import annotations

import logging
import threading
import typing as t
from datetime import datetime, timedelta, timezone

import requests
from singer_sdk import Tap
from singer_sdk import typing as th

from tap_massive.base_streams import (
    AllConditionCodesStream,
    AllExchangesStream,
    AllTickerTypesStream,
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
    IndicesTickerTypesStream,
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
    OptionsTickerTypesStream,
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

    # Stock ticker caching
    _cached_stock_tickers: t.List[dict] | None = None
    _stock_tickers_stream_instance: StockTickerStream | None = None
    _stock_tickers_lock = threading.Lock()

    # Option ticker caching
    _cached_option_tickers: t.List[dict] | None = None
    _option_tickers_stream_instance: OptionsContractsStream | None = None
    _option_tickers_lock = threading.Lock()
    _spot_price_cache: dict[str, float | None] = {}

    # Forex ticker caching
    _cached_forex_tickers: t.List[dict] | None = None
    _forex_tickers_stream_instance: ForexTickerStream | None = None
    _forex_tickers_lock = threading.Lock()

    # Crypto ticker caching
    _cached_crypto_tickers: t.List[dict] | None = None
    _crypto_tickers_stream_instance: CryptoTickerStream | None = None
    _crypto_tickers_lock = threading.Lock()

    # Indices ticker caching
    _cached_indices_tickers: t.List[dict] | None = None
    _indices_tickers_stream_instance: IndicesTickerStream | None = None
    _indices_tickers_lock = threading.Lock()

    # Futures ticker caching
    _cached_futures_tickers: t.List[dict] | None = None
    _futures_tickers_stream_instance: FuturesContractsStream | None = None
    _futures_tickers_lock = threading.Lock()

    # Stock methods
    def get_stock_tickers_stream(self) -> StockTickerStream:
        if self._stock_tickers_stream_instance is None:
            self.logger.info("Creating StockTickerStream instance...")
            self._stock_tickers_stream_instance = StockTickerStream(self)
        return self._stock_tickers_stream_instance

    def get_cached_stock_tickers(self) -> t.List[dict]:
        if self._cached_stock_tickers is None:
            with self._stock_tickers_lock:
                if self._cached_stock_tickers is None:
                    self.logger.info("Fetching and caching stock tickers...")
                    stock_tickers_stream = self.get_stock_tickers_stream()
                    self._cached_stock_tickers = list(
                        stock_tickers_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_stock_tickers)} stock tickers."
                    )
        return self._cached_stock_tickers

    # Option methods
    def get_option_tickers_stream(self) -> OptionsContractsStream:
        if self._option_tickers_stream_instance is None:
            self.logger.info("Creating OptionsContractsStream instance...")
            self._option_tickers_stream_instance = OptionsContractsStream(self)
        return self._option_tickers_stream_instance

    def get_cached_option_tickers(self) -> t.List[dict]:
        if self._cached_option_tickers is None:
            with self._option_tickers_lock:
                if self._cached_option_tickers is None:
                    self.logger.info("Fetching and caching option tickers...")
                    option_tickers_stream = self.get_option_tickers_stream()
                    self._cached_option_tickers = list(
                        option_tickers_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_option_tickers)} option tickers."
                    )
        return self._cached_option_tickers

    # Option per-underlying methods
    def get_spot_price(self, ticker: str) -> float | None:
        """Get previous close price for an underlying ticker. Cached per ticker."""
        if ticker in self._spot_price_cache:
            return self._spot_price_cache[ticker]

        base_url = self.config.get("base_url", "https://api.massive.com")
        url = f"{base_url}/v2/aggs/ticker/{ticker}/prev"
        try:
            resp = requests.get(
                url,
                params={"apiKey": self.config["api_key"]},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if results:
                price = results[0].get("c")
                self._spot_price_cache[ticker] = price
                return price
        except (requests.RequestException, ValueError, KeyError) as e:
            logging.warning(f"Could not fetch spot price for {ticker}: {e}")

        self._spot_price_cache[ticker] = None
        return None

    def get_option_contracts_for_underlying(self, underlying: str) -> list[dict]:
        """Fetch filtered option contracts for a single underlying ticker.

        Applies moneyness and DTE filters from option_tickers.other_params config.
        Can optionally cap total contracts per underlying with max_contracts_per_underlying.
        """
        option_cfg = self.config.get("option_tickers", {})
        query_params = option_cfg.get("query_params", {}).copy()
        other_params = option_cfg.get("other_params", {})

        moneyness_min = other_params.get("moneyness_min")
        moneyness_max = other_params.get("moneyness_max")
        max_dte = other_params.get("max_dte")
        max_contracts = other_params.get("max_contracts_per_underlying")

        if moneyness_min is not None or moneyness_max is not None:
            spot_price = self.get_spot_price(underlying)
            if spot_price:
                if moneyness_min is not None:
                    query_params["strike_price.gte"] = spot_price * moneyness_min
                if moneyness_max is not None:
                    query_params["strike_price.lte"] = spot_price * moneyness_max
            else:
                logging.warning(
                    f"No spot price for {underlying}, skipping moneyness filter."
                )

        if max_dte is not None and not query_params.get("expired", False):
            max_exp = (
                datetime.now(tz=timezone.utc) + timedelta(days=int(max_dte))
            ).strftime("%Y-%m-%d")
            query_params.setdefault("expiration_date.lte", max_exp)

        query_params["underlying_ticker"] = underlying
        query_params["apiKey"] = self.config["api_key"]
        query_params.setdefault("limit", 1000)
        query_params.setdefault("sort", "ticker")

        # Normalize __ separators to . for API
        query_params = {k.replace("__", "."): v for k, v in query_params.items()}

        base_url = self.config.get("base_url", "https://api.massive.com")
        url: str | None = f"{base_url}/v3/reference/options/contracts"

        contracts: list[dict] = []
        while url:
            try:
                resp = requests.get(url, params=query_params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except (requests.RequestException, ValueError) as e:
                logging.warning(
                    f"Failed to fetch option contracts for {underlying}: {e}"
                )
                break

            results = data.get("results", [])
            contracts.extend(results)

            # Cap total contracts per underlying if max_contracts_per_underlying is set
            if max_contracts and len(contracts) >= max_contracts:
                logging.warning(
                    f"Reached max_contracts_per_underlying ({max_contracts}) for {underlying}, "
                    f"stopping pagination. Total fetched: {len(contracts)}"
                )
                break

            url = data.get("next_url")
            # next_url already has query params baked in, only need apiKey
            if url:
                query_params = {"apiKey": self.config["api_key"]}

        logging.info(
            f"Fetched {len(contracts)} filtered option contracts for {underlying}."
        )
        return contracts

    # Forex methods
    def get_forex_tickers_stream(self) -> ForexTickerStream:
        if self._forex_tickers_stream_instance is None:
            self.logger.info("Creating ForexTickerStream instance...")
            self._forex_tickers_stream_instance = ForexTickerStream(self)
        return self._forex_tickers_stream_instance

    def get_cached_forex_tickers(self) -> t.List[dict]:
        if self._cached_forex_tickers is None:
            with self._forex_tickers_lock:
                if self._cached_forex_tickers is None:
                    self.logger.info("Fetching and caching forex tickers...")
                    forex_tickers_stream = self.get_forex_tickers_stream()
                    self._cached_forex_tickers = list(
                        forex_tickers_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_forex_tickers)} forex tickers."
                    )
        return self._cached_forex_tickers

    # Crypto methods
    def get_crypto_tickers_stream(self) -> CryptoTickerStream:
        if self._crypto_tickers_stream_instance is None:
            self.logger.info("Creating CryptoTickerStream instance...")
            self._crypto_tickers_stream_instance = CryptoTickerStream(self)
        return self._crypto_tickers_stream_instance

    def get_cached_crypto_tickers(self) -> t.List[dict]:
        if self._cached_crypto_tickers is None:
            with self._crypto_tickers_lock:
                if self._cached_crypto_tickers is None:
                    self.logger.info("Fetching and caching crypto tickers...")
                    crypto_tickers_stream = self.get_crypto_tickers_stream()
                    self._cached_crypto_tickers = list(
                        crypto_tickers_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_crypto_tickers)} crypto tickers."
                    )
        return self._cached_crypto_tickers

    # Indices methods
    def get_indices_tickers_stream(self) -> IndicesTickerStream:
        if self._indices_tickers_stream_instance is None:
            self.logger.info("Creating IndicesTickerStream instance...")
            self._indices_tickers_stream_instance = IndicesTickerStream(self)
        return self._indices_tickers_stream_instance

    def get_cached_indices_tickers(self) -> t.List[dict]:
        if self._cached_indices_tickers is None:
            with self._indices_tickers_lock:
                if self._cached_indices_tickers is None:
                    self.logger.info("Fetching and caching indices tickers...")
                    indices_tickers_stream = self.get_indices_tickers_stream()
                    self._cached_indices_tickers = list(
                        indices_tickers_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_indices_tickers)} indices tickers."
                    )
        return self._cached_indices_tickers

    # Futures methods
    def get_futures_tickers_stream(self) -> FuturesContractsStream:
        if self._futures_tickers_stream_instance is None:
            self.logger.info("Creating FuturesContractsStream instance...")
            self._futures_tickers_stream_instance = FuturesContractsStream(self)
        return self._futures_tickers_stream_instance

    def get_cached_futures_tickers(self) -> t.List[dict]:
        if self._cached_futures_tickers is None:
            with self._futures_tickers_lock:
                if self._cached_futures_tickers is None:
                    self.logger.info("Fetching and caching futures tickers...")
                    futures_tickers_stream = self.get_futures_tickers_stream()
                    self._cached_futures_tickers = list(
                        futures_tickers_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_futures_tickers)} futures tickers."
                    )
        return self._cached_futures_tickers

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
            OptionsTickerTypesStream(self),
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
            IndicesTickerTypesStream(self),
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
        ]

        return streams


if __name__ == "__main__":
    TapMassive.cli()
