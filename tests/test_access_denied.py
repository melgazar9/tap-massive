"""A stream the plan does not cover fails its sync, never a completed one of zero rows."""

from __future__ import annotations

import io
from contextlib import redirect_stdout

import pytest
import requests

from tap_massive.benzinga_streams import BenzingaEarningsStream

from .local_massive import LocalMassive
from .test_unsorted_bookmark import LocalPages


def test_an_unentitled_stream_fails_its_sync_instead_of_completing_empty():
    refusal = (403, {"status": "NOT_AUTHORIZED", "message": "not entitled"})
    pages = LocalPages(BenzingaEarningsStream, first_page=[], next_page=refusal)
    emitted = io.StringIO()
    with LocalMassive(lambda _path: refusal) as server:
        stream = pages.stream(server)
        with (
            redirect_stdout(emitted),
            pytest.raises(requests.exceptions.HTTPError) as exc_info,
        ):
            stream.sync(None)
    assert exc_info.value.response.status_code == 403
    assert '"type":"RECORD"' not in emitted.getvalue().replace(" ", "")
    assert len(server.paths) == 1
