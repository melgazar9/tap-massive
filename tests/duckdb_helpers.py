"""Shared DuckDB test helpers for flat-file quote snapshot tests."""

from __future__ import annotations


def run_duckdb_bar_query(
    sql_template_raw: str,
    filepath: str,
    interval_ns: int,
) -> list[dict]:
    """Format and execute a quote update bar SQL template against a CSV fixture.

    Mirrors the two-step substitution used in production:
    1. .format() substitutes interval_ns (and preserves {file_path} placeholder)
    2. .replace() injects the actual file path
    """
    import duckdb

    sql_template = sql_template_raw.format(
        interval_ns=interval_ns,
        file_path="{file_path}",
    )
    sql = sql_template.replace("{file_path}", filepath)
    conn = duckdb.connect()
    try:
        result = conn.execute(sql)
        columns = [d[0] for d in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]
    finally:
        conn.close()
