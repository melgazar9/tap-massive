"""Massive tap class."""

from __future__ import annotations

import threading
import typing as t

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_massive.stock_streams import (
    StockTickerStream,
    ConditionCodesStream,
    DividendsStream,
    EmaStream,
    ExchangesStream,
    FinancialsStream,
    IPOsStream,
    MACDStream,
    MarketHolidaysStream,
    MarketStatusStream,
    NewsStream,
    MassiveRestStream,
    RelatedCompaniesStream,
    RSIStream,
    ShortInterestStream,
    ShortVolumeStream,
    SmaStream,
    SplitsStream,
    StockBars1DayStream,
    StockBars1HourStream,
    StockBars1MinuteStream,
    StockBars1MonthStream,
    StockBars1SecondStream,
    StockBars1WeekStream,
    StockBars5MinuteStream,
    StockBars30MinuteStream,
    StockBars30SecondStream,
    StockDailyMarketSummaryStream,
    StockDailyTickerSummaryStream,
    StockPreviousDayBarSummaryStream,
    StockTickerDetailsStream,
    StockTickerSnapshotStream,
    StockTopMarketMoversStream,
    StockTradeStream,
    TickerEventsStream,
    TickerTypesStream,
)

from tap_massive.option_streams import (
    OptionsContractsStream,
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
    OptionsContractOverviewStream,
    OptionsContractSnapshotStream,
    OptionsDailyTickerSummaryStream,
    OptionsEmaStream,
    OptionsLastTradeStream,
    OptionsMACDStream,
    OptionsPreviousDayBarStream,
    OptionsQuoteStream,
    OptionsRSIStream,
    OptionsSmaStream,
    OptionsTradeStream,
    OptionsUnifiedSnapshotStream,
)

from tap_massive.forex_streams import (
    ForexTickerStream,
    ForexTickerDetailsStream,
    ForexBars1SecondStream,
    ForexBars1MinuteStream,
    ForexBars5MinuteStream,
    ForexBars1HourStream,
    ForexBars1DayStream,
    ForexBars1WeekStream,
    ForexBars1MonthStream,
    ForexDailyMarketSummaryStream,
    ForexPreviousDayBarStream,
    ForexQuoteStream,
    ForexLastQuoteStream,
    ForexTopMarketMoversStream,
    ForexTickerSnapshotStream,
    ForexFullMarketSnapshotStream,
    ForexSmaStream,
    ForexEmaStream,
    ForexMACDStream,
    ForexRSIStream,
    ForexCurrencyConversionStream,
)

from tap_massive.crypto_streams import (
    CryptoTickerStream,
    CryptoTickerDetailsStream,
    CryptoBars1SecondStream,
    CryptoBars1MinuteStream,
    CryptoBars5MinuteStream,
    CryptoBars1HourStream,
    CryptoBars1DayStream,
    CryptoBars1WeekStream,
    CryptoBars1MonthStream,
    CryptoDailyMarketSummaryStream,
    CryptoDailyTickerSummaryStream,
    CryptoPreviousDayBarStream,
    CryptoTradeStream,
    CryptoLastTradeStream,
    CryptoTopMarketMoversStream,
    CryptoTickerSnapshotStream,
    CryptoFullMarketSnapshotStream,
    CryptoSmaStream,
    CryptoEmaStream,
    CryptoMACDStream,
    CryptoRSIStream,
)

from tap_massive.indices_streams import (
    IndicesTickerStream,
    IndicesTickerDetailsStream,
    IndicesBars1SecondStream,
    IndicesBars1MinuteStream,
    IndicesBars5MinuteStream,
    IndicesBars1HourStream,
    IndicesBars1DayStream,
    IndicesBars1WeekStream,
    IndicesBars1MonthStream,
    IndicesDailyTickerSummaryStream,
    IndicesPreviousDayBarStream,
    IndicesSnapshotStream,
    IndicesSmaStream,
    IndicesEmaStream,
    IndicesMACDStream,
    IndicesRSIStream,
)

from tap_massive.futures_streams import (
    FuturesContractsStream,
    FuturesProductsStream,
    FuturesSchedulesStream,
    FuturesBars1SecondStream,
    FuturesBars1MinuteStream,
    FuturesBars5MinuteStream,
    FuturesBars1HourStream,
    FuturesBars1DayStream,
    FuturesBars1WeekStream,
    FuturesBars1MonthStream,
    FuturesTradeStream,
    FuturesQuoteStream,
    FuturesContractsSnapshotStream,
)

from tap_massive.economy_streams import (
    InflationExpectationsStream,
    InflationStream,
    LaborMarketStream,
    TreasuryYieldStream,
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
    _option_tickers_stream_instance: "OptionsContractsStream" | None = None
    _option_tickers_lock = threading.Lock()

    # Forex ticker caching
    _cached_forex_tickers: t.List[dict] | None = None
    _forex_tickers_stream_instance: "ForexTickerStream" | None = None
    _forex_tickers_lock = threading.Lock()

    # Crypto ticker caching
    _cached_crypto_tickers: t.List[dict] | None = None
    _crypto_tickers_stream_instance: "CryptoTickerStream" | None = None
    _crypto_tickers_lock = threading.Lock()

    # Indices ticker caching
    _cached_indices_tickers: t.List[dict] | None = None
    _indices_tickers_stream_instance: "IndicesTickerStream" | None = None
    _indices_tickers_lock = threading.Lock()

    # Futures ticker caching
    _cached_futures_tickers: t.List[dict] | None = None
    _futures_tickers_stream_instance: "FuturesContractsStream" | None = None
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
    def get_option_tickers_stream(self) -> "OptionsContractsStream":
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

    # Forex methods
    def get_forex_tickers_stream(self) -> "ForexTickerStream":
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
    def get_crypto_tickers_stream(self) -> "CryptoTickerStream":
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
    def get_indices_tickers_stream(self) -> "IndicesTickerStream":
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
    def get_futures_tickers_stream(self) -> "FuturesContractsStream":
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
            TickerTypesStream(self),
            RelatedCompaniesStream(self),
            StockDailyMarketSummaryStream(self),
            StockDailyTickerSummaryStream(self),
            StockPreviousDayBarSummaryStream(self),
            StockTopMarketMoversStream(self),
            StockTickerSnapshotStream(self),
            StockTradeStream(self),
            SmaStream(self),
            EmaStream(self),
            MACDStream(self),
            RSIStream(self),
            ExchangesStream(self),
            MarketHolidaysStream(self),
            MarketStatusStream(self),
            ConditionCodesStream(self),
            IPOsStream(self),
            SplitsStream(self),
            DividendsStream(self),
            TickerEventsStream(self),
            FinancialsStream(self),
            ShortInterestStream(self),
            ShortVolumeStream(self),
            NewsStream(self),
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
        ]

        return streams


if __name__ == "__main__":
    TapMassive.cli()
