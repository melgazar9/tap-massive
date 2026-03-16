"""Shared utilities for tap-massive (type conversion, NYSE calendar)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

import pandas_market_calendars as mcal

# ---------------------------------------------------------------------------
# Type conversion helpers
# ---------------------------------------------------------------------------


def safe_decimal(x):
    """Convert to Decimal, returning None on failure."""
    try:
        if x is None or x == "":
            return None
        return Decimal(str(x))
    except (ValueError, TypeError, InvalidOperation):
        return None


def safe_int(x):
    """Convert to int via Decimal, returning None on failure."""
    try:
        if x is None or x == "":
            return None
        return int(Decimal(str(x)))
    except (ValueError, TypeError, InvalidOperation):
        return None


# ---------------------------------------------------------------------------
# NYSE calendar helpers
# ---------------------------------------------------------------------------


def _fetch_nyse_schedule(start_ns: int, end_ns: int):
    """Fetch NYSE schedule for the date range (shared by windows and sessions)."""
    start_dt = datetime.fromtimestamp(start_ns / 1_000_000_000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ns / 1_000_000_000, tz=timezone.utc)
    nyse = mcal.get_calendar("NYSE")
    return nyse.schedule(start_date=start_dt.date(), end_date=end_dt.date())


def compute_nyse_market_windows(
    interval_seconds: int, start_ns: int, end_ns: int
) -> list[int]:
    """Return asof_timestamp nanosecond timestamps during NYSE hours."""
    schedule = _fetch_nyse_schedule(start_ns, end_ns)
    windows: list[int] = []
    interval_td = timedelta(seconds=interval_seconds)

    for _, row in schedule.iterrows():
        current = row["market_open"]
        market_close = row["market_close"]
        while current < market_close:
            current_ns = int(current.timestamp() * 1_000_000_000)
            if start_ns <= current_ns < end_ns:
                windows.append(current_ns)
            current = current + interval_td

    return windows


def compute_nyse_session_opens(start_ns: int, end_ns: int) -> list[tuple[int, int]]:
    """Return [(session_open_ns, session_close_ns), ...] for NYSE trading days in range."""
    schedule = _fetch_nyse_schedule(start_ns, end_ns)
    return [
        (
            int(row["market_open"].timestamp() * 1_000_000_000),
            int(row["market_close"].timestamp() * 1_000_000_000),
        )
        for _, row in schedule.iterrows()
    ]
