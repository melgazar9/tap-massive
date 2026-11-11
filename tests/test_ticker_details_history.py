"""Forex and crypto ticker details keep a row per day; a date loop resumes at its bookmark."""

from __future__ import annotations

import typing as t
from datetime import date, timedelta
from unittest import mock
from urllib.parse import parse_qs, urlparse

import pytest

from tap_massive.crypto_streams import CryptoTickerDetailsStream
from tap_massive.forex_streams import (
    ForexDailyMarketSummaryStream,
    ForexTickerDetailsStream,
)
from tap_massive.tap import TapMassive

from .local_massive import LocalMassive


class DateLoopRun:
    """A date-looped stream run on a local Massive: its records and the day each request asked."""

    TODAY: t.ClassVar[date] = date.today()

    @classmethod
    def day(cls, days_ago: int) -> str:
        return (cls.TODAY - timedelta(days=days_ago)).isoformat()

    @staticmethod
    def requested_day(path: str) -> str:
        parsed = urlparse(path)
        return parse_qs(parsed.query).get("date", [parsed.path.rsplit("/", 1)[-1]])[0]

    @classmethod
    def run(
        cls,
        stream_cls,
        stream_config: dict,
        respond,
        context: dict | None = None,
        state: dict | None = None,
    ):
        with LocalMassive(respond) as server:
            config = {
                "api_key": "k",
                "base_url": server.url,
                stream_cls.name: stream_config,
            }
            with mock.patch.object(TapMassive, "discover_streams", return_value=[]):
                tap = TapMassive(config=config, state=state or {})
            records = list(stream_cls(tap=tap).get_records(context))
        return records, [cls.requested_day(p) for p in server.paths]


@pytest.fixture(autouse=True)
def no_retry_sleep() -> t.Iterator[None]:
    with mock.patch("time.sleep"):
        yield


def _details_answer(ticker: str):
    return lambda path: (
        200,
        {"status": "OK", "results": {"ticker": ticker, "name": "pair", "active": True}},
    )


def _details_config(start: str) -> dict:
    return {
        "query_params": {"date": start},
        "other_params": {"loop_over_dates_gte_date": True},
    }


@pytest.mark.parametrize(
    "stream_cls, ticker",
    [(ForexTickerDetailsStream, "C:EURUSD"), (CryptoTickerDetailsStream, "X:BTCUSD")],
)
def test_every_day_asked_is_kept_as_its_own_row(stream_cls, ticker):
    records, asked = DateLoopRun.run(
        stream_cls,
        _details_config(DateLoopRun.day(2)),
        _details_answer(ticker),
        context={"ticker": ticker},
    )

    assert asked == [DateLoopRun.day(2), DateLoopRun.day(1), DateLoopRun.day(0)]
    assert [(r["ticker"], r["date"]) for r in records] == [(ticker, d) for d in asked]
    assert stream_cls.primary_keys == ["ticker", "date"]
    assert stream_cls.replication_key == "date"


def test_a_rerun_asks_each_ticker_only_from_its_own_bookmark():
    bookmark = {
        "context": {"ticker": "C:EURUSD"},
        "replication_key": "date",
        "replication_key_value": DateLoopRun.day(1),
    }
    state = {"bookmarks": {"forex_ticker_details": {"partitions": [bookmark]}}}

    _, asked = DateLoopRun.run(
        ForexTickerDetailsStream,
        _details_config(DateLoopRun.day(5)),
        _details_answer("C:EURUSD"),
        context={"ticker": "C:EURUSD"},
        state=state,
    )

    assert asked == [DateLoopRun.day(1), DateLoopRun.day(0)]


def test_a_date_looped_market_summary_resumes_at_its_bookmark_not_the_config_start():
    # production 2026-11-09: bookmark 2026-04-13, yet the loop began again at 1970-01-01
    bookmark = {
        "replication_key": "timestamp",
        "replication_key_value": f"{DateLoopRun.day(1)}T23:59:59.999000+00:00",
    }
    state = {"bookmarks": {"forex_daily_market_summary": bookmark}}
    config = {
        "path_params": {"date": DateLoopRun.day(5)},
        "other_params": {"loop_over_dates_gte_date": True},
    }

    _, asked = DateLoopRun.run(
        ForexDailyMarketSummaryStream,
        config,
        lambda path: (200, {"status": "OK", "results": []}),
        state=state,
    )

    assert asked == [DateLoopRun.day(1), DateLoopRun.day(0)]
