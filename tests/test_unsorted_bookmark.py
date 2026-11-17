"""An unsorted stream's bookmark moves only when its sync completes.

A run cut off mid-stream resumes where it began, never past rows it did not read
(futures_contracts, the bug of 2026-11-10); every stream still pages to its end.
"""

from __future__ import annotations

import io
import typing as t
from contextlib import redirect_stdout
from unittest import mock

import pytest
import requests

from tap_massive.client import MassiveRestStream
from tap_massive.earnings_subset import StockEarningsQuotesStream
from tap_massive.futures_streams import FuturesContractsStream, FuturesTradeStream
from tap_massive.tap import TapMassive

from .local_massive import LocalMassive


class LocalPages:
    """A stream on a local Massive: a first page of rows, then the page it names."""

    NEXT: t.ClassVar[str] = "cursor=next"

    def __init__(
        self,
        stream_cls: type[MassiveRestStream],
        first_page: list[dict],
        next_page: tuple[int, dict],
        state: dict | None = None,
    ) -> None:
        self._stream_cls = stream_cls
        self._first_page = first_page
        self._next_page = next_page
        self._state = state or {}
        self.url = ""

    def respond(self, path: str) -> tuple[int, dict]:
        if self.NEXT in path:
            return self._next_page
        return 200, {
            "status": "OK",
            "results": self._first_page,
            "next_url": f"{self.url}/page?{self.NEXT}",
        }

    def stream(self, server: LocalMassive) -> MassiveRestStream:
        self.url = server.url
        # The catalog holds this stream alone, so a full sync finds its stream maps.
        with mock.patch.object(
            TapMassive,
            "discover_streams",
            lambda tap: [self._stream_cls(tap=tap)],
        ):
            tap = TapMassive(
                config={"api_key": "k", "base_url": server.url}, state=self._state
            )
        return tap.streams[self._stream_cls.name]


class Contracts:
    """futures_contracts rows out of date order, read from a bookmark before them."""

    BOOKMARK: t.ClassVar[str] = "2026-11-01"
    FIRST_PAGE: t.ClassVar[list[dict]] = [
        {"ticker": "ZNZ6", "date": "2026-11-10"},
        {"ticker": "ZNH7", "date": "2026-11-03"},
    ]

    @classmethod
    def pages(cls, next_page: tuple[int, dict]) -> LocalPages:
        state = {
            "bookmarks": {
                "futures_contracts": {
                    "replication_key": "date",
                    "replication_key_value": cls.BOOKMARK,
                }
            }
        }
        return LocalPages(FuturesContractsStream, cls.FIRST_PAGE, next_page, state)


@pytest.fixture(autouse=True)
def no_retry_sleep() -> t.Iterator[None]:
    with mock.patch("time.sleep"):
        yield


def test_a_run_cut_off_mid_stream_keeps_the_bookmark_where_it_began():
    pages = Contracts.pages(next_page=(500, {"status": "ERROR"}))
    read: list[dict] = []
    with LocalMassive(pages.respond) as server:
        stream = pages.stream(server)
        with pytest.raises(requests.exceptions.RequestException):
            for record in stream.get_records(None):
                read.append(record)
    assert [record["date"] for record in read] == ["2026-11-10", "2026-11-03"]
    assert stream.get_context_state(None)["replication_key_value"] == Contracts.BOOKMARK


def test_a_completed_run_moves_the_bookmark_to_the_furthest_row_it_read():
    last_page = {
        "status": "OK",
        "results": [{"ticker": "ZNM7", "date": "2026-11-05"}],
        "next_url": None,
    }
    pages = Contracts.pages(next_page=(200, last_page))
    with LocalMassive(pages.respond) as server:
        stream = pages.stream(server)
        with redirect_stdout(io.StringIO()):
            stream.sync(None)
    assert stream.get_context_state(None)["replication_key_value"] == "2026-11-10"


@pytest.mark.parametrize(
    ("stream_cls", "context", "key"),
    [
        (FuturesTradeStream, {"ticker": "ZNZ6"}, "timestamp"),
        (
            StockEarningsQuotesStream,
            {"ticker": "AAPL", "snapshot_date": "2026-11-10"},
            "sip_timestamp",
        ),
    ],
)
def test_a_nanosecond_stream_pages_through_to_its_last_page(stream_cls, context, key):
    first_ns, last_ns = 1_794_322_800_000_000_000, 1_794_322_800_000_000_500
    last_page = {
        "status": "OK",
        "results": [{key: last_ns, "sequence_number": 2}],
        "next_url": None,
    }
    pages = LocalPages(
        stream_cls, [{key: first_ns, "sequence_number": 1}], (200, last_page)
    )
    with LocalMassive(pages.respond) as server:
        records = list(pages.stream(server).get_records(context))
    assert [record[key] for record in records] == [first_ns, last_ns]
