"""Reliability tests for top market movers stream HTTP handling."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from tap_massive.stock_streams import StockTopMarketMoversStream


def _make_stream(direction: str | None = None) -> StockTopMarketMoversStream:
    stream = StockTopMarketMoversStream.__new__(StockTopMarketMoversStream)
    stream.path_params = {} if direction is None else {"direction": direction}
    stream.query_params = {}
    stream.logger = MagicMock()
    stream.get_url = MagicMock(
        side_effect=lambda context=None: f"https://example/{context['direction']}"
    )
    stream.post_process = MagicMock(side_effect=lambda row, context=None: row)
    return stream


def _make_response(records):
    response = MagicMock()
    response.json.return_value = {"tickers": records}
    return response


def test_top_movers_uses_get_response_for_both_directions():
    stream = _make_stream(direction="both")
    stream.get_response = MagicMock(
        side_effect=[
            _make_response([{"ticker": "AAPL", "updated": 1}]),
            _make_response([{"ticker": "MSFT", "updated": 2}]),
        ]
    )

    records = list(stream.get_records(None))

    assert len(records) == 2
    assert records[0]["direction"] == "gainers"
    assert records[1]["direction"] == "losers"
    assert stream.get_response.call_count == 2


def test_top_movers_403_fails_the_stream_instead_of_skipping_a_direction():
    stream = _make_stream(direction="both")
    http_403 = requests.exceptions.HTTPError(response=MagicMock(status_code=403))
    stream.get_response = MagicMock(
        side_effect=[
            http_403,
            _make_response([{"ticker": "NVDA", "updated": 1}]),
        ]
    )

    with pytest.raises(requests.exceptions.HTTPError):
        list(stream.get_records(None))
    assert stream.get_response.call_count == 1


def test_top_movers_not_authorized_body_fails_the_stream():
    stream = _make_stream(direction="gainers")
    response = MagicMock()
    response.json.return_value = {"status": "NOT_AUTHORIZED", "message": "plan"}
    stream.get_response = MagicMock(return_value=response)

    with pytest.raises(PermissionError, match="NOT_AUTHORIZED"):
        list(stream.get_records(None))


def test_top_movers_single_direction_calls_once():
    stream = _make_stream(direction="gainers")
    stream.get_response = MagicMock(
        return_value=_make_response([{"ticker": "META", "updated": 1}])
    )

    records = list(stream.get_records(None))

    assert len(records) == 1
    assert records[0]["direction"] == "gainers"
    assert stream.get_response.call_count == 1
