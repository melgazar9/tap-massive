from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from tap_massive.disk_cache import resolve_home
from tap_massive.flat_files_streams import FlatFilesStream, QuoteSnapshotFlatFilesStream


def test_resolve_home_xdg_cache(monkeypatch) -> None:
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("APP_HOME", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", "/app/.cache")

    assert resolve_home(Path("~/.cache/massive_flat_files")) == Path(
        "/app/.cache/massive_flat_files"
    )
    assert resolve_home(Path("~/.cache")) == Path("/app/.cache")
    assert resolve_home(Path("~/.cache/duckdb_spill")) == Path(
        "/app/.cache/duckdb_spill"
    )


def test_resolve_home_with_home_env(monkeypatch) -> None:
    monkeypatch.setenv("HOME", "/home/testuser")
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)

    assert resolve_home(Path("~/.cache/massive_flat_files")) == Path(
        "/home/testuser/.cache/massive_flat_files"
    )
    assert resolve_home(Path("~/somedir")) == Path("/home/testuser/somedir")


def test_resolve_home_noop_for_absolute_path() -> None:
    assert resolve_home(Path("/absolute/path")) == Path("/absolute/path")
    assert resolve_home(Path("relative/path")) == Path("relative/path")


def test_flat_files_base_dir_expands_tilde_via_xdg_cache_home(monkeypatch) -> None:
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("APP_HOME", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", "/app/.cache")

    stream = MagicMock(spec=QuoteSnapshotFlatFilesStream)
    stream.config = {"flat_files_base_dir": "~/.cache/massive_flat_files"}
    stream.name = "options_quote_snapshots_flat_files_1_minute"
    stream.SUBDIR = "us_options_opra/quotes_v1"

    resolved = FlatFilesStream._flat_files_dir.fget(stream)

    assert resolved == Path("/app/.cache/massive_flat_files/us_options_opra/quotes_v1")


def test_stream_override_flat_files_dir_expands_tilde_via_xdg_cache_home(
    monkeypatch,
) -> None:
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("APP_HOME", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", "/app/.cache")

    stream = MagicMock(spec=QuoteSnapshotFlatFilesStream)
    stream.config = {
        "options_quote_snapshots_flat_files_1_minute": {
            "flat_files_dir": "~/.cache/massive_flat_files/us_options_opra/quotes_v1",
        }
    }
    stream.name = "options_quote_snapshots_flat_files_1_minute"
    stream.SUBDIR = "us_options_opra/quotes_v1"

    resolved = FlatFilesStream._flat_files_dir.fget(stream)

    assert resolved == Path("/app/.cache/massive_flat_files/us_options_opra/quotes_v1")


def test_duckdb_temp_directory_expands_tilde_via_xdg_cache_home(monkeypatch) -> None:
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("APP_HOME", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", "/app/.cache")

    stream = MagicMock(spec=QuoteSnapshotFlatFilesStream)
    stream.config = {
        "duckdb_params": {
            "temp_directory": "~/.cache/duckdb_spill",
            "duckdb_batch_size": 1,
        }
    }
    stream.name = "options_quote_snapshots_flat_files_1_minute"
    stream._interval_ns = 60_000_000_000
    stream._RAW_SQL_TEMPLATE = "select {file_path}"
    stream._process_sequential.return_value = iter(())
    stream._iter_files_in_bounds.return_value = []

    executed: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            executed.append(sql)

        def close(self) -> None:
            return None

    with patch(
        "tap_massive.flat_files_streams.duckdb.connect", return_value=FakeConn()
    ):
        list(QuoteSnapshotFlatFilesStream.get_records(stream, None))

    assert "SET temp_directory='/app/.cache/duckdb_spill'" in executed
