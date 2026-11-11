"""A real local HTTP server standing in for Massive, so requests, urllib3 and backoff run unmocked."""

from __future__ import annotations

import json
import threading
import typing as t
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


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

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()
