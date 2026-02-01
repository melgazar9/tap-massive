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

from tap_massive.economy_streams import (
    InflationExpectationsStream,
    InflationStream,
    LaborMarketStream,
    TreasuryYieldStream,
)

# Streams to implement later:
# - TickerSnapshotStream
# - FullMarketSnapshotStream
# - UnifiedSnapshotStream

# Streams not imported because advanced subscription is required
# - QuoteStream
# - LastQuoteStream


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

    _cached_stock_tickers: t.List[dict] | None = None
    _stock_tickers_stream_instance: StockTickerStream | None = None
    _stock_tickers_lock = threading.Lock()

    _cached_option_tickers: t.List[dict] | None = None
    _option_tickers_stream_instance: "OptionsContractsStream" | None = None
    _option_tickers_lock = threading.Lock()

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

    def get_option_tickers_stream(self) -> "OptionsContractsStream":
        if self._option_tickers_stream_instance is None:
            self.logger.info("Creating OptionsContractsStream instance...")
            from tap_massive.option_streams import OptionsContractsStream

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
            TreasuryYieldStream(self),
            InflationExpectationsStream(self),
            InflationStream(self),
            LaborMarketStream(self),
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
        ]

        return streams


if __name__ == "__main__":
    TapMassive.cli()
