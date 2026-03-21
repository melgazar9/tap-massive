from __future__ import annotations

from pathlib import Path

from tap_massive.flat_files_streams import (
    _OPTIONS_QUOTE_SNAPSHOT_BATCH_SQL,
    _OPTIONS_QUOTE_SNAPSHOT_SQL,
    FlatFilesStreamOptionQuotes,
)

from .duckdb_helpers import run_duckdb_bar_query, run_duckdb_batch_bar_query

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
        "file_date",
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


def test_options_batch_query_matches_single_file() -> None:
    """Batch SQL with one file should produce same rows as single-file SQL."""
    single_rows = _run_options_query(str(FIXTURE))
    batch_rows = run_duckdb_batch_bar_query(
        _OPTIONS_QUOTE_SNAPSHOT_BATCH_SQL, [str(FIXTURE)], INTERVAL_NS
    )
    # Strip file_date from both — single uses a test literal, batch extracts from filename
    for row in single_rows:
        row.pop("file_date", None)
    for row in batch_rows:
        row.pop("file_date", None)
    assert batch_rows == single_rows


def test_options_batch_has_file_date_column() -> None:
    """Batch query must include file_date extracted from filename."""
    batch_rows = run_duckdb_batch_bar_query(
        _OPTIONS_QUOTE_SNAPSHOT_BATCH_SQL, [str(FIXTURE)], INTERVAL_NS
    )
    assert all("file_date" in row for row in batch_rows)
    assert "participant_timestamp" not in batch_rows[0]
    assert "tape" not in batch_rows[0]


def test_options_batch_results_ordered_by_file_date() -> None:
    """Batch results must be ordered by file_date, ticker, asof_timestamp."""
    batch_rows = run_duckdb_batch_bar_query(
        _OPTIONS_QUOTE_SNAPSHOT_BATCH_SQL, [str(FIXTURE)], INTERVAL_NS
    )
    sort_keys = [(r["file_date"], r["ticker"], r["asof_timestamp"]) for r in batch_rows]
    assert sort_keys == sorted(sort_keys)
