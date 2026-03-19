"""Tests for QuoteSnapshotFlatFilesStream DuckDB aggregation."""

from __future__ import annotations

from pathlib import Path

from tap_massive.flat_files_streams import (
    _STOCK_QUOTE_SNAPSHOT_BATCH_SQL,
    _STOCK_QUOTE_SNAPSHOT_SQL,
)

from .duckdb_helpers import run_duckdb_bar_query, run_duckdb_batch_bar_query

FIXTURE = Path(__file__).parent / "fixtures" / "test_quotes.csv.gz"
INTERVAL_NS = 60_000_000_000  # 1 minute


def _run_query(filepath: str, interval_ns: int = INTERVAL_NS) -> list[dict]:
    return run_duckdb_bar_query(_STOCK_QUOTE_SNAPSHOT_SQL, filepath, interval_ns)


class TestFlatFileBucketMath:
    """Verify asof_timestamp assignment and last-quote selection."""

    def test_exact_boundary_stays_in_current_window(self):
        """A quote at exactly 60B ns should be in the 60B window, not 120B."""
        rows = _run_query(str(FIXTURE))
        # Find the row for ticker C (call) at asof_timestamp=60B
        call_60 = [
            r
            for r in rows
            if r["ticker"] == "O:TEST260320C00100000"
            and r["asof_timestamp"] == 60_000_000_000
        ]
        assert len(call_60) == 1
        assert call_60[0]["sip_timestamp"] == 60_000_000_000

    def test_last_quote_by_sip_and_sequence(self):
        """Multiple quotes in same window: last by sip_timestamp wins."""
        rows = _run_query(str(FIXTURE))
        # Quotes at 90B and 100B are both in (60B, 120B] window
        call_120 = [
            r
            for r in rows
            if r["ticker"] == "O:TEST260320C00100000"
            and r["asof_timestamp"] == 120_000_000_000
        ]
        assert len(call_120) == 1
        # Should be the 100B quote (last), not the 90B one
        assert call_120[0]["sip_timestamp"] == 100_000_000_000
        assert call_120[0]["ask_exchange"] == 301
        assert call_120[0]["ask_price"] == 11.5

    def test_different_tickers_separate_windows(self):
        """Each ticker gets its own rows."""
        rows = _run_query(str(FIXTURE))
        tickers = {r["ticker"] for r in rows}
        assert tickers == {"O:TEST260320C00100000", "O:TEST260320P00100000"}

    def test_put_in_correct_window(self):
        """Put at 95B should be in the 120B window."""
        rows = _run_query(str(FIXTURE))
        put_rows = [r for r in rows if r["ticker"] == "O:TEST260320P00100000"]
        assert len(put_rows) == 1
        assert put_rows[0]["asof_timestamp"] == 120_000_000_000
        assert put_rows[0]["bid_price"] == 4.5

    def test_no_future_leakage(self):
        """Every sip_timestamp must be <= asof_timestamp."""
        rows = _run_query(str(FIXTURE))
        for r in rows:
            assert (
                r["sip_timestamp"] <= r["asof_timestamp"]
            ), f"Leakage: sip={r['sip_timestamp']} > window={r['asof_timestamp']}"

    def test_asof_timestamp_is_interval_aligned(self):
        """All asof_timestamp values should be multiples of interval_ns."""
        rows = _run_query(str(FIXTURE))
        for r in rows:
            assert r["asof_timestamp"] % INTERVAL_NS == 0

    def test_total_row_count(self):
        """4 raw quotes should produce 3 bars (2 call windows + 1 put window)."""
        rows = _run_query(str(FIXTURE))
        assert len(rows) == 3


class TestBatchQueryProducesSameResults:
    """Verify batch SQL template produces identical results to single-file template."""

    def test_batch_with_single_file_matches_single_query(self):
        """Batch query with one file should produce same rows as single-file query."""
        single_rows = _run_query(str(FIXTURE))
        batch_rows = run_duckdb_batch_bar_query(
            _STOCK_QUOTE_SNAPSHOT_BATCH_SQL, [str(FIXTURE)], INTERVAL_NS
        )
        # Batch adds file_date column; strip it for comparison
        for row in batch_rows:
            row.pop("file_date")
        assert batch_rows == single_rows

    def test_batch_with_same_file_twice_deduplicates(self):
        """Same file passed twice has identical filename/file_date, so QUALIFY deduplicates."""
        batch_rows = run_duckdb_batch_bar_query(
            _STOCK_QUOTE_SNAPSHOT_BATCH_SQL, [str(FIXTURE), str(FIXTURE)], INTERVAL_NS
        )
        assert len(batch_rows) == 3

    def test_batch_file_date_extracted_from_filename(self):
        """file_date column should be extracted from the filename via regex."""
        batch_rows = run_duckdb_batch_bar_query(
            _STOCK_QUOTE_SNAPSHOT_BATCH_SQL, [str(FIXTURE)], INTERVAL_NS
        )
        assert all("file_date" in row for row in batch_rows)

    def test_batch_results_ordered_by_file_date_first(self):
        """Results must be ordered by file_date, then ticker, then asof_timestamp."""
        batch_rows = run_duckdb_batch_bar_query(
            _STOCK_QUOTE_SNAPSHOT_BATCH_SQL, [str(FIXTURE)], INTERVAL_NS
        )
        sort_keys = [
            (r["file_date"], r["ticker"], r["asof_timestamp"]) for r in batch_rows
        ]
        assert sort_keys == sorted(sort_keys)

    def test_batch_dedup_within_file(self):
        """Last-quote-wins dedup should still work within each file in a batch."""
        batch_rows = run_duckdb_batch_bar_query(
            _STOCK_QUOTE_SNAPSHOT_BATCH_SQL, [str(FIXTURE)], INTERVAL_NS
        )
        call_120 = [
            r
            for r in batch_rows
            if r["ticker"] == "O:TEST260320C00100000"
            and r["asof_timestamp"] == 120_000_000_000
        ]
        assert len(call_120) == 1
        assert call_120[0]["sip_timestamp"] == 100_000_000_000
