"""The Massive key rides only in the Authorization header: no URL, log line or traceback may carry it.

Runs against a real local server, so requests, urllib3 and backoff build their real error text around the real URL.
"""

from __future__ import annotations

import json
import logging
import socket
import threading
import traceback
import typing as t
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

import pytest
import requests

from tap_massive.earnings_subset import EarningsCalendar
from tap_massive.stock_streams import MarketHolidaysStream
from tap_massive.tap import TapMassive


class LocalMassive:
    """A local server answering each GET from `respond(path)`, recording the path and auth header it was sent."""

    def __init__(self, respond: t.Callable[[str], tuple[int, dict | list]]) -> None:
        self.paths: list[str] = []
        self.auth_headers: list[str | None] = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                owner.paths.append(self.path)
                owner.auth_headers.append(self.headers.get("Authorization"))
                status, body = respond(self.path)
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args: t.Any) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_port}"

    def __enter__(self) -> LocalMassive:
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc: t.Any) -> None:
        self._server.shutdown()
        self._server.server_close()


class KeyInvariant:
    """What every test checks: the key reached the server in the header, and nothing printed carries it."""

    KEY: t.ClassVar[str] = "SuperSecretMassiveKey0123456789"

    @classmethod
    def assert_sent_in_header_only(cls, server: LocalMassive) -> None:
        assert server.paths, "the server was never asked"
        assert all(header == f"Bearer {cls.KEY}" for header in server.auth_headers)
        assert not any(cls.KEY in path for path in server.paths)

    @classmethod
    def assert_not_printed(cls, error: BaseException, log_text: str) -> None:
        assert cls.KEY not in "".join(traceback.format_exception(error)) + log_text


@pytest.fixture(autouse=True)
def no_retry_sleep() -> t.Iterator[None]:
    with mock.patch("time.sleep"):
        yield


def _stream(base_url: str) -> MarketHolidaysStream:
    with mock.patch.object(TapMassive, "discover_streams", return_value=[]):
        tap = TapMassive(config={"api_key": KeyInvariant.KEY, "base_url": base_url})
    return MarketHolidaysStream(tap=tap)


@pytest.mark.parametrize("status", [429, 500, 403])
def test_http_error_after_retries_never_prints_the_key(
    status: int, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    with LocalMassive(lambda path: (status, {"status": "ERROR"})) as server:
        stream = _stream(server.url)
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            stream.get_response(stream.get_url(), dict(stream.query_params))

    KeyInvariant.assert_sent_in_header_only(server)
    KeyInvariant.assert_not_printed(exc_info.value, caplog.text)


def test_connection_refused_never_prints_the_key(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port_url = f"http://127.0.0.1:{probe.getsockname()[1]}"
    stream = _stream(closed_port_url)

    with pytest.raises(requests.exceptions.ConnectionError) as exc_info:
        stream.get_response(stream.get_url(), dict(stream.query_params))

    KeyInvariant.assert_not_printed(exc_info.value, caplog.text)


def test_stream_query_params_never_hold_the_key() -> None:
    with LocalMassive(lambda path: (200, [])) as server:
        stream = _stream(server.url)
        stream.get_response(stream.get_url(), dict(stream.query_params))

    assert KeyInvariant.KEY not in json.dumps(stream.query_params)
    KeyInvariant.assert_sent_in_header_only(server)


def test_earnings_api_pagination_sends_the_key_in_the_header_on_every_page() -> None:
    def respond(path: str) -> tuple[int, dict]:
        if "cursor=p2" in path:
            return 200, {"results": [{"page": 2}]}
        return 200, {
            "results": [{"page": 1}],
            "next_url": f"{server.url}/benzinga/v1/earnings?cursor=p2",
        }

    calendar = EarningsCalendar.__new__(EarningsCalendar)
    calendar._api_key = KeyInvariant.KEY
    calendar._request_timeout = 5
    with LocalMassive(respond) as server:
        records = calendar._paginate_api(
            f"{server.url}/benzinga/v1/earnings", {"limit": 1}
        )

    assert records == [{"page": 1}, {"page": 2}]
    assert len(server.paths) == 2
    KeyInvariant.assert_sent_in_header_only(server)
