from __future__ import annotations

from pathlib import Path

from tap_massive.flat_files_streams import (
    _OPTIONS_QUOTE_SNAPSHOT_SQL,
    FlatFilesStreamOptionQuotes,
)

from .duckdb_helpers import run_duckdb_bar_query

FIXTURE = Path(__file__).parent / "fixtures" / "test_options_quotes.csv"
INTERVAL_NS = 60_000_000_000  # 1 minute


def _run_options_query(filepath: str, interval_ns: int = INTERVAL_NS) -> list[dict]:
    return run_duckdb_bar_query(_OPTIONS_QUOTE_SNAPSHOT_SQL, filepath, interval_ns)


def test_options_flat_file_quote_schema_matches_docs() -> None:
    assert list(FlatFilesStreamOptionQuotes.schema["properties"]) == [
        "file_date",
        "ticker",
        "sip_timestamp",
        "sequence_number",
        "ask_exchange",
        "ask_price",
        "ask_size",
        "bid_exchange",
        "bid_price",
        "bid_size",
    ]


def test_options_quote_snapshot_sql_accepts_options_shape() -> None:
    rows = _run_options_query(str(FIXTURE))

    assert len(rows) == 3
    assert set(rows[0]) == {
        "ticker",
        "asof_timestamp",
        "sip_timestamp",
        "sequence_number",
        "ask_exchange",
        "ask_price",
        "ask_size",
        "bid_exchange",
        "bid_price",
        "bid_size",
    }
    assert "participant_timestamp" not in rows[0]
    assert "trf_timestamp" not in rows[0]
    assert "conditions" not in rows[0]
    assert "indicators" not in rows[0]
    assert "tape" not in rows[0]

    call_120 = [
        row
        for row in rows
        if row["ticker"] == "O:TEST260320C00100000"
        and row["asof_timestamp"] == 120_000_000_000
    ]
    assert len(call_120) == 1
    assert call_120[0]["sip_timestamp"] == 100_000_000_000
    assert call_120[0]["ask_exchange"] == 301
    assert call_120[0]["ask_price"] == 11.5
