from __future__ import annotations

from pathlib import Path

from tap_massive.flat_files_streams import (
    _QUOTE_SNAPSHOT_SQL,
    FlatFilesStreamStockQuotes,
)

from .duckdb_helpers import run_duckdb_bar_query

FIXTURE = Path(__file__).parent / "fixtures" / "test_stock_quotes.csv"
INTERVAL_NS = 60_000_000_000  # 1 minute


def _run_stock_query(filepath: str, interval_ns: int = INTERVAL_NS) -> list[dict]:
    return run_duckdb_bar_query(_QUOTE_SNAPSHOT_SQL, filepath, interval_ns)


def test_stock_flat_file_quote_schema_matches_docs() -> None:
    """Stock flat file quote schema must match the 13 documented CSV columns + file_date."""
    assert list(FlatFilesStreamStockQuotes.schema["properties"]) == [
        "file_date",
        "ticker",
        "sip_timestamp",
        "participant_timestamp",
        "trf_timestamp",
        "sequence_number",
        "ask_exchange",
        "ask_price",
        "ask_size",
        "bid_exchange",
        "bid_price",
        "bid_size",
        "conditions",
        "indicators",
        "tape",
    ]


def test_stock_quote_snapshot_sql_accepts_stock_shape() -> None:
    """DuckDB aggregation query must work with all 13 stock CSV columns."""
    rows = _run_stock_query(str(FIXTURE))

    assert len(rows) == 3
    assert set(rows[0]) == {
        "ticker",
        "asof_timestamp",
        "sip_timestamp",
        "participant_timestamp",
        "trf_timestamp",
        "sequence_number",
        "ask_exchange",
        "ask_price",
        "ask_size",
        "bid_exchange",
        "bid_price",
        "bid_size",
        "conditions",
        "indicators",
        "tape",
    }

    # Verify MSFT rows in the second 1-min window (asof_timestamp=120B ns)
    msft_120 = [
        row
        for row in rows
        if row["ticker"] == "MSFT" and row["asof_timestamp"] == 120_000_000_000
    ]
    assert len(msft_120) == 1
    # Last MSFT quote in that window (by sip_timestamp, sequence_number)
    assert msft_120[0]["sip_timestamp"] == 100_000_000_000
    assert msft_120[0]["ask_exchange"] == 11
    assert msft_120[0]["ask_price"] == 276.18
    assert msft_120[0]["tape"] == 3
