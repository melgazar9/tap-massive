"""A daily ticker summary date loop asks for each day once, never the start again."""

from __future__ import annotations

import typing as t
from datetime import date, timedelta
from unittest import mock

from tap_massive.indices_streams import IndicesDailyTickerSummaryStream
from tap_massive.tap import TapMassive

from .local_massive import LocalMassive


class OpenClose:
    """A local open-close endpoint: one summary row for the day named in the path."""

    TICKER: t.ClassVar[str] = "I:SPX"

    @staticmethod
    def respond(path: str) -> tuple[int, dict]:
        day = path.split("?")[0].rstrip("/").split("/")[-1]
        return 200, {
            "status": "OK",
            "from": day,
            "symbol": OpenClose.TICKER,
            "open": 1.0,
            "close": 2.0,
        }

    @staticmethod
    def stream(server: LocalMassive, start: date) -> IndicesDailyTickerSummaryStream:
        config = {
            "api_key": "k",
            "base_url": server.url,
            IndicesDailyTickerSummaryStream.name: {
                "path_params": {"date": start.isoformat()},
                "other_params": {
                    "loop_over_dates_gte_date": True,
                    "use_cached_tickers": True,
                },
            },
        }
        with mock.patch.object(
            TapMassive,
            "discover_streams",
            lambda tap: [IndicesDailyTickerSummaryStream(tap=tap)],
        ):
            tap = TapMassive(config=config)
        return tap.streams[IndicesDailyTickerSummaryStream.name]


def test_the_date_loop_requests_each_day_from_the_start_through_today():
    start = date.today() - timedelta(days=2)
    days = [(start + timedelta(days=n)).isoformat() for n in range(3)]
    with LocalMassive(OpenClose.respond) as server:
        stream = OpenClose.stream(server, start)
        records = list(stream.get_records({"ticker": OpenClose.TICKER}))
    requested = [path.split("?")[0].rstrip("/").split("/")[-1] for path in server.paths]
    assert requested == days
    assert [record["from"] for record in records] == days
