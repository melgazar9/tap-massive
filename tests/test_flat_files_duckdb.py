"""Tests for QuoteUpdateBarFlatFilesStream DuckDB aggregation."""

from __future__ import annotations

from pathlib import Path

from tap_massive.flat_files_streams import _QUOTE_UPDATE_BAR_SQL

FIXTURE = Path(__file__).parent / "fixtures" / "test_quotes.csv.gz"
INTERVAL_NS = 60_000_000_000  # 1 minute


def _run_query(filepath: str, interval_ns: int = INTERVAL_NS) -> list[dict]:
    import duckdb

    sql = _QUOTE_UPDATE_BAR_SQL.format(interval_ns=interval_ns, file_path=filepath)
    conn = duckdb.connect()
    result = conn.execute(sql)
    columns = [d[0] for d in result.description]
    rows = [dict(zip(columns, r)) for r in result.fetchall()]
    conn.close()
    return rows


class TestFlatFileBucketMath:
    """Verify window_start assignment and last-quote selection."""

    def test_exact_boundary_stays_in_current_window(self):
        """A quote at exactly 60B ns should be in the 60B window, not 120B."""
        rows = _run_query(str(FIXTURE))
        # Find the row for ticker C (call) at window_start=60B
        call_60 = [
            r
            for r in rows
            if r["ticker"] == "O:TEST260320C00100000"
            and r["window_start"] == 60_000_000_000
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
            and r["window_start"] == 120_000_000_000
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
        assert put_rows[0]["window_start"] == 120_000_000_000
        assert put_rows[0]["bid_price"] == 4.5

    def test_no_future_leakage(self):
        """Every sip_timestamp must be <= window_start."""
        rows = _run_query(str(FIXTURE))
        for r in rows:
            assert (
                r["sip_timestamp"] <= r["window_start"]
            ), f"Leakage: sip={r['sip_timestamp']} > window={r['window_start']}"

    def test_window_start_is_interval_aligned(self):
        """All window_start values should be multiples of interval_ns."""
        rows = _run_query(str(FIXTURE))
        for r in rows:
            assert r["window_start"] % INTERVAL_NS == 0

    def test_total_row_count(self):
        """4 raw quotes should produce 3 bars (2 call windows + 1 put window)."""
        rows = _run_query(str(FIXTURE))
        assert len(rows) == 3
