"""Shared DuckDB test helpers for flat-file quote snapshot tests."""

from __future__ import annotations

import duckdb


def _execute_duckdb_sql(sql: str) -> list[dict]:
    """Execute SQL against an in-memory DuckDB and return rows as dicts."""
    conn = duckdb.connect()
    try:
        result = conn.execute(sql)
        columns = [d[0] for d in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]
    finally:
        conn.close()


def run_duckdb_bar_query(
    sql_template_raw: str,
    filepath: str,
    interval_ns: int,
) -> list[dict]:
    """Format and execute a single-file quote bar SQL template against a CSV fixture."""
    sql = (
        sql_template_raw.format(
            interval_ns=interval_ns,
            file_path="{file_path}",
            file_date="{file_date}",
        )
        .replace("{file_path}", filepath)
        .replace("{file_date}", "2026-01-01")
    )
    return _execute_duckdb_sql(sql)


def run_duckdb_batch_bar_query(
    sql_template_raw: str,
    filepaths: list[str],
    interval_ns: int,
) -> list[dict]:
    """Format and execute a batch quote bar SQL template against multiple CSV fixtures."""
    file_list_literal = "[" + ", ".join(f"'{p}'" for p in filepaths) + "]"
    sql = sql_template_raw.format(
        interval_ns=interval_ns,
        file_list="{file_list}",
    ).replace("{file_list}", file_list_literal)
    return _execute_duckdb_sql(sql)
