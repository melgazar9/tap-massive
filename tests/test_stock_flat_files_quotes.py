from __future__ import annotations

from pathlib import Path

from tap_massive.flat_files_streams import (
    _STOCK_QUOTE_SNAPSHOT_BATCH_SQL,
    _STOCK_QUOTE_SNAPSHOT_SQL,
    FlatFilesStreamStockQuotes,
)

from .duckdb_helpers import run_duckdb_bar_query, run_duckdb_batch_bar_query

FIXTURE = Path(__file__).parent / "fixtures" / "test_stock_quotes.csv"
INTERVAL_NS = 60_000_000_000  # 1 minute


def _run_stock_query(filepath: str, interval_ns: int = INTERVAL_NS) -> list[dict]:
    return run_duckdb_bar_query(_STOCK_QUOTE_SNAPSHOT_SQL, filepath, interval_ns)


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
        "file_date",
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


def test_stock_batch_query_matches_single_file() -> None:
    """Batch SQL with one file should produce same rows as single-file SQL."""
    single_rows = _run_stock_query(str(FIXTURE))
    batch_rows = run_duckdb_batch_bar_query(
        _STOCK_QUOTE_SNAPSHOT_BATCH_SQL, [str(FIXTURE)], INTERVAL_NS
    )
    # Strip file_date from both — single uses a test literal, batch extracts from filename
    for row in single_rows:
        row.pop("file_date", None)
    for row in batch_rows:
        row.pop("file_date", None)
    assert batch_rows == single_rows


def test_stock_batch_has_file_date_and_stock_columns() -> None:
    """Batch query must include file_date and all stock-specific columns."""
    batch_rows = run_duckdb_batch_bar_query(
        _STOCK_QUOTE_SNAPSHOT_BATCH_SQL, [str(FIXTURE)], INTERVAL_NS
    )
    assert all("file_date" in row for row in batch_rows)
    assert "participant_timestamp" in batch_rows[0]
    assert "trf_timestamp" in batch_rows[0]
    assert "conditions" in batch_rows[0]
    assert "tape" in batch_rows[0]


def test_stock_batch_results_ordered_by_file_date() -> None:
    """Batch results must be ordered by file_date (replication key for incremental state)."""
    batch_rows = run_duckdb_batch_bar_query(
        _STOCK_QUOTE_SNAPSHOT_BATCH_SQL, [str(FIXTURE)], INTERVAL_NS
    )
    file_dates = [r["file_date"] for r in batch_rows]
    assert file_dates == sorted(file_dates)
