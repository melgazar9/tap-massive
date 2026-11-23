"""The date-keyed options analytics streams read in date order and bookmark as they go.

A backfill cut off mid-stream resumes from the last day it reached, not from its start
date. The API defaults to newest-first, so the streams ask for date.asc themselves.
"""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from unittest import mock
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from singer_sdk.exceptions import ConfigValidationError

from tap_massive.option_streams import (
    OptionsGreeksStream,
    OptionsOpenInterestByUnderlyingStream,
    OptionsOpenInterestStream,
)
from tap_massive.tap import TapMassive

from .local_massive import LocalMassive
from .test_unsorted_bookmark import LocalPages

STREAMS = [
    OptionsOpenInterestStream,
    OptionsOpenInterestByUnderlyingStream,
    OptionsGreeksStream,
]
START, REACHED = "2025-02-18", "2026-01-22"


def _one_day_page(stream_cls) -> list[dict]:
    """A first page long enough to pass the SDK's STATE checkpoint, all one trading day."""
    key = stream_cls.primary_keys[0]
    return [
        {key: f"ROW{i}", "date": REACHED}
        for i in range(stream_cls.STATE_MSG_FREQUENCY + 1)
    ]


@pytest.mark.parametrize("stream_cls", STREAMS)
def test_a_backfill_cut_off_mid_stream_resumes_from_the_last_day_read(stream_cls):
    bookmark = {"replication_key": "date", "replication_key_value": START}
    pages = LocalPages(
        stream_cls,
        _one_day_page(stream_cls),
        next_page=(500, {"status": "ERROR"}),
        state={"bookmarks": {stream_cls.name: bookmark}},
    )
    out = io.StringIO()
    with LocalMassive(pages.respond) as server, mock.patch("time.sleep"):
        stream = pages.stream(server)
        with redirect_stdout(out), pytest.raises(requests.exceptions.RequestException):
            stream.sync(None)

    states = [
        message["value"]
        for message in map(json.loads, filter(None, out.getvalue().splitlines()))
        if message["type"] == "STATE"
    ]
    saved = states[-1]["bookmarks"][stream_cls.name]["replication_key_value"]
    assert saved == REACHED


@pytest.mark.parametrize("stream_cls", STREAMS)
def test_rows_are_asked_for_in_date_order_when_no_sort_is_configured(stream_cls):
    pages = LocalPages(stream_cls, [], next_page=(200, {"status": "OK", "results": []}))
    with LocalMassive(pages.respond) as server:
        list(pages.stream(server).get_records(None))
    assert parse_qs(urlparse(server.paths[0]).query)["sort"] == ["date.asc"]


@pytest.mark.parametrize("stream_cls", STREAMS)
def test_a_newest_first_sort_is_refused_at_startup(stream_cls):
    config = {"api_key": "k", stream_cls.name: {"query_params": {"sort": "date.desc"}}}
    with (
        mock.patch.object(
            TapMassive, "discover_streams", lambda tap: [stream_cls(tap=tap)]
        ),
        pytest.raises(ConfigValidationError, match="date.asc"),
    ):
        TapMassive(config=config)
