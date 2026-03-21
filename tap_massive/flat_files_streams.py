"""Flat file streams for reading local CSV / CSV.gz historical data.

Reads date-sorted flat files from a configured directory and yields
records incrementally using ``file_date`` (extracted from each filename)
as the replication key.

Every stream defines an explicit schema with proper types.  CSV string
values are coerced to the declared types (integer, number, boolean) at
read time so that downstream targets receive correctly-typed data.
"""

from __future__ import annotations

import csv
import fcntl
import functools
import gzip
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import typing as t
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date as date_cls
from datetime import timedelta
from pathlib import Path

import duckdb
from singer_sdk import Stream
from singer_sdk import typing as th

from tap_massive.disk_cache import resolve_home
from tap_massive.utils import safe_int

try:
    from isal import igzip

    _open_gz = igzip.open
except ImportError:
    _open_gz = gzip.open

logger = logging.getLogger(__name__)

_FILE_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")

# ---------------------------------------------------------------------------
# DuckDB configuration
# ---------------------------------------------------------------------------

_ALLOWED_DUCKDB_PARAMS = frozenset({
    "threads",
    "memory_limit",
    "temp_directory",
    "max_temp_directory_size",
    "preserve_insertion_order",
})

INTERVAL_NS_MAP = {
    "1_second": 1_000_000_000,
    "30_second": 30_000_000_000,
    "1_minute": 60_000_000_000,
    "5_minute": 300_000_000_000,
    "30_minute": 1_800_000_000_000,
}


def configure_duckdb_connection(
    conn,  # noqa: ANN001
    params: dict,
    resolve_paths: bool = False,
) -> int:
    """Apply validated DuckDB SET parameters to a connection.

    Returns the duckdb_batch_size value (popped from params), defaults to 1.
    """
    params = dict(params)
    batch_size = int(params.pop("duckdb_batch_size", 1))

    for param, value in params.items():
        if param not in _ALLOWED_DUCKDB_PARAMS:
            msg = f"DuckDB param '{param}' is not in the allowlist: {sorted(_ALLOWED_DUCKDB_PARAMS)}"
            raise ValueError(msg)

        if isinstance(value, bool):
            conn.execute(f"SET {param}={str(value).lower()}")
        elif isinstance(value, int):
            conn.execute(f"SET {param}={value}")
        else:
            str_value = str(value)
            if resolve_paths:
                str_value = str(Path(str_value).expanduser())
            str_value = str_value.replace("'", "''")
            conn.execute(f"SET {param}='{str_value}'")

    return batch_size


def resolve_interval_ns(stream_name: str) -> int:
    """Extract interval_ns from a stream name suffix."""
    for suffix, ns in INTERVAL_NS_MAP.items():
        if stream_name.endswith(suffix):
            return ns
    msg = f"Cannot determine interval_ns from stream name: {stream_name}"
    raise ValueError(msg)


# ---------------------------------------------------------------------------
# Type coercion helpers (CSV string → Python)
# ---------------------------------------------------------------------------


def _to_float(value: str) -> float | None:
    """Coerce a CSV string to float, returning None for empty/invalid values."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _to_bool(value: str) -> bool | None:
    """Coerce a CSV string to bool, returning None for empty/invalid values."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1")


_JSON_TYPE_COERCERS: dict[str, t.Callable] = {
    "integer": safe_int,
    "number": _to_float,
    "boolean": _to_bool,
}


# ---------------------------------------------------------------------------
# Shared explicit schemas
# ---------------------------------------------------------------------------

_BARS_SCHEMA = th.PropertiesList(
    th.Property("file_date", th.StringType),
    th.Property("ticker", th.StringType),
    th.Property("window_start", th.IntegerType),
    th.Property("open", th.NumberType),
    th.Property("close", th.NumberType),
    th.Property("high", th.NumberType),
    th.Property("low", th.NumberType),
    th.Property("volume", th.NumberType),
    th.Property("vwap", th.NumberType),
    th.Property("transactions", th.IntegerType),
    th.Property("otc", th.BooleanType),
).to_dict()

_SIP_TRADES_SCHEMA = th.PropertiesList(
    th.Property("file_date", th.StringType),
    th.Property("ticker", th.StringType),
    th.Property("sip_timestamp", th.IntegerType),
    th.Property("participant_timestamp", th.IntegerType),
    th.Property("trf_timestamp", th.IntegerType),
    th.Property("sequence_number", th.IntegerType),
    th.Property("id", th.StringType),
    th.Property("price", th.NumberType),
    th.Property("size", th.NumberType),
    th.Property("conditions", th.StringType),
    th.Property("correction", th.IntegerType),
    th.Property("exchange", th.IntegerType),
    th.Property("tape", th.IntegerType),
    th.Property("trf_id", th.IntegerType),
).to_dict()

_SIP_QUOTES_SCHEMA = th.PropertiesList(
    th.Property("file_date", th.StringType),
    th.Property("ticker", th.StringType),
    th.Property("sip_timestamp", th.IntegerType),
    th.Property("participant_timestamp", th.IntegerType),
    th.Property("trf_timestamp", th.IntegerType),
    th.Property("sequence_number", th.IntegerType),
    th.Property("ask_exchange", th.IntegerType),
    th.Property("ask_price", th.NumberType),
    th.Property("ask_size", th.NumberType),
    th.Property("bid_exchange", th.IntegerType),
    th.Property("bid_price", th.NumberType),
    th.Property("bid_size", th.NumberType),
    th.Property("conditions", th.StringType),
    th.Property("indicators", th.StringType),
    th.Property("tape", th.IntegerType),
).to_dict()

# Options quotes have fewer columns than stock quotes — no participant_timestamp,
# trf_timestamp, conditions, indicators, or tape.
_OPTIONS_QUOTES_SCHEMA = th.PropertiesList(
    th.Property("file_date", th.StringType),
    th.Property("ticker", th.StringType),
    th.Property("sip_timestamp", th.IntegerType),
    th.Property("sequence_number", th.IntegerType),
    th.Property("ask_exchange", th.IntegerType),
    th.Property("ask_price", th.NumberType),
    th.Property("ask_size", th.NumberType),
    th.Property("bid_exchange", th.IntegerType),
    th.Property("bid_price", th.NumberType),
    th.Property("bid_size", th.NumberType),
).to_dict()

_CRYPTO_TRADES_SCHEMA = th.PropertiesList(
    th.Property("file_date", th.StringType),
    th.Property("ticker", th.StringType),
    th.Property("participant_timestamp", th.IntegerType),
    th.Property("id", th.StringType),
    th.Property("price", th.NumberType),
    th.Property("size", th.NumberType),
    th.Property("conditions", th.StringType),
    th.Property("exchange", th.IntegerType),
).to_dict()

_FOREX_QUOTES_SCHEMA = th.PropertiesList(
    th.Property("file_date", th.StringType),
    th.Property("ticker", th.StringType),
    th.Property("participant_timestamp", th.IntegerType),
    th.Property("ask_price", th.NumberType),
    th.Property("bid_price", th.NumberType),
    th.Property("exchange", th.IntegerType),
).to_dict()

_INDEX_VALUES_SCHEMA = th.PropertiesList(
    th.Property("file_date", th.StringType),
    th.Property("ticker", th.StringType),
    th.Property("timestamp", th.IntegerType),
    th.Property("value", th.NumberType),
).to_dict()


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------


class FlatFilesStream(Stream):
    """Base stream that reads sorted local CSV/CSV.gz flat files.

    Subclasses set ``name``, ``SUBDIR``, ``primary_keys``, and ``schema``.
    CSV string values are automatically coerced to the types declared in
    the schema (integer, number, boolean).
    """

    SUBDIR: t.ClassVar[str] = ""

    replication_method = "INCREMENTAL"
    replication_key = "file_date"
    is_sorted = True

    # -- type coercion -------------------------------------------------------

    @functools.cached_property
    def _field_coercers(self) -> dict[str, t.Callable]:
        """Build field → coercion function map from the declared schema."""
        props = self.schema.get("properties", {})
        coercers: dict[str, t.Callable] = {}
        for field, prop_def in props.items():
            json_types = prop_def.get("type", [])
            if isinstance(json_types, str):
                json_types = [json_types]
            for jt in json_types:
                if jt in _JSON_TYPE_COERCERS:
                    coercers[field] = _JSON_TYPE_COERCERS[jt]
                    break
        return coercers

    def _coerce_row(self, row: dict) -> None:
        """Coerce CSV string values to the types declared in the schema."""
        for field, coercer in self._field_coercers.items():
            if field in row:
                row[field] = coercer(row[field])

    # -- directory / file helpers --------------------------------------------

    @property
    def _flat_files_dir(self) -> Path | None:
        """Resolve the flat files directory for this stream.

        Returns None when neither ``flat_files_base_dir`` nor a per-stream
        ``flat_files_dir`` is configured, so discovery can still succeed with
        a minimal schema.
        """
        stream_config = self.config.get(self.name)
        if isinstance(stream_config, dict):
            override = stream_config.get("flat_files_dir")
            if override:
                return resolve_home(Path(override))
        base = self.config.get("flat_files_base_dir", "")
        if not base:
            return None
        return resolve_home(Path(base)) / self.SUBDIR

    def _list_files(self) -> list[tuple[str, Path]]:
        """Return ``(date_str, path)`` pairs sorted ascending by date."""
        directory = self._flat_files_dir
        if directory is None or not directory.exists():
            return []

        files: list[tuple[str, Path]] = []
        for entry in directory.iterdir():
            if not entry.is_file():
                continue
            if not (entry.suffix == ".csv" or entry.name.endswith(".csv.gz")):
                continue
            match = _FILE_DATE_RE.search(entry.name)
            if match:
                files.append((match.group(1), entry))

        files.sort(key=lambda x: x[0])
        return files

    # -- record emission -----------------------------------------------------

    @staticmethod
    def _read_csv(filepath: Path) -> t.Iterator[dict[str, str]]:
        """Yield row dicts from a CSV file (supports .gz via igzip/gzip)."""
        if filepath.name.endswith(".gz"):
            with _open_gz(filepath, "rt") as fh:
                yield from csv.DictReader(fh)
        else:
            with filepath.open() as fh:
                yield from csv.DictReader(fh)

    def _iter_files_in_bounds(
        self,
        context: dict | None,
    ) -> t.Iterator[tuple[str, Path]]:
        """Yield ``(file_date, filepath)`` pairs within configured date bounds.

        Handles incremental state, ``start_file_date``, and ``end_file_date``
        config. Shared by ``FlatFilesStream`` and ``EarningsFlatFilesStream``.
        """
        if self._flat_files_dir is None:
            msg = (
                f"Stream {self.name}: set 'flat_files_base_dir' in tap config "
                f"or '{self.name}.flat_files_dir' per-stream."
            )
            raise ValueError(msg)

        start_value = self.get_starting_replication_key_value(context)

        # Per-stream date bounds from config.
        stream_cfg = self.config.get(self.name)
        cfg_start = (
            stream_cfg.get("start_file_date") if isinstance(stream_cfg, dict) else None
        )
        cfg_end = (
            stream_cfg.get("end_file_date") if isinstance(stream_cfg, dict) else None
        )

        # Config start_file_date acts as a floor when no state exists.
        if cfg_start and (not start_value or cfg_start > start_value):
            start_value = cfg_start

        for file_date, filepath in self._list_files():
            # Re-process the bookmarked date (idempotent); skip older files.
            if start_value and file_date < start_value:
                continue
            if cfg_end and file_date > cfg_end:
                continue
            yield file_date, filepath

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Yield records incrementally, resuming from the last bookmark."""
        for file_date, filepath in self._iter_files_in_bounds(context):
            self.logger.info("Processing %s (date=%s)", filepath.name, file_date)
            for row in self._read_csv(filepath):
                row["file_date"] = file_date
                self._coerce_row(row)
                yield row


# ---------------------------------------------------------------------------
# Stocks
# ---------------------------------------------------------------------------


class FlatFilesStreamStockEod(FlatFilesStream):
    """Stock end-of-day bars from flat files."""

    name = "stocks_flat_files_eod"
    SUBDIR = "us_stocks_sip/eod"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamStock1m(FlatFilesStream):
    """Stock 1-minute bars from flat files."""

    name = "stocks_flat_files_1m"
    SUBDIR = "us_stocks_sip/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamStockTrades(FlatFilesStream):
    """Stock trades from flat files."""

    name = "stocks_flat_files_trades"
    SUBDIR = "us_stocks_sip/trades"
    primary_keys = ["file_date", "ticker", "sip_timestamp", "sequence_number"]
    schema = _SIP_TRADES_SCHEMA


class FlatFilesStreamStockQuotes(FlatFilesStream):
    """Stock quotes from flat files."""

    name = "stocks_flat_files_quotes"
    SUBDIR = "us_stocks_sip/quotes"
    primary_keys = ["file_date", "ticker", "sip_timestamp", "sequence_number"]
    schema = _SIP_QUOTES_SCHEMA


# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------


class FlatFilesStreamOptionEod(FlatFilesStream):
    """Options end-of-day bars from flat files."""

    name = "options_flat_files_eod"
    SUBDIR = "us_options_opra/eod"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamOption1m(FlatFilesStream):
    """Options 1-minute bars from flat files."""

    name = "options_flat_files_1m"
    SUBDIR = "us_options_opra/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamOptionTrades(FlatFilesStream):
    """Options trades from flat files."""

    name = "options_flat_files_trades"
    SUBDIR = "us_options_opra/trades"
    primary_keys = ["file_date", "ticker", "sip_timestamp"]
    schema = _SIP_TRADES_SCHEMA


class FlatFilesStreamOptionQuotes(FlatFilesStream):
    """Options quotes from flat files."""

    name = "options_flat_files_quotes"
    SUBDIR = "us_options_opra/quotes"
    primary_keys = ["file_date", "ticker", "sip_timestamp", "sequence_number"]
    schema = _OPTIONS_QUOTES_SCHEMA


# ---------------------------------------------------------------------------
# Indices
# ---------------------------------------------------------------------------


class FlatFilesStreamIndexEod(FlatFilesStream):
    """Indices end-of-day bars from flat files."""

    name = "indices_flat_files_eod"
    SUBDIR = "us_indices/eod"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamIndex1m(FlatFilesStream):
    """Indices 1-minute bars from flat files."""

    name = "indices_flat_files_1m"
    SUBDIR = "us_indices/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamIndexValues(FlatFilesStream):
    """Indices tick-level values from flat files."""

    name = "indices_flat_files_values"
    SUBDIR = "us_indices/values"
    primary_keys = ["file_date", "ticker", "timestamp"]
    schema = _INDEX_VALUES_SCHEMA


# ---------------------------------------------------------------------------
# Forex
# ---------------------------------------------------------------------------


class FlatFilesStreamForexEod(FlatFilesStream):
    """Forex end-of-day bars from flat files."""

    name = "forex_flat_files_eod"
    SUBDIR = "global_forex/eod"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamForex1m(FlatFilesStream):
    """Forex 1-minute bars from flat files."""

    name = "forex_flat_files_1m"
    SUBDIR = "global_forex/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamForexQuotes(FlatFilesStream):
    """Forex quotes from flat files."""

    name = "forex_flat_files_quotes"
    SUBDIR = "global_forex/quotes"
    primary_keys = ["file_date", "ticker", "participant_timestamp"]
    schema = _FOREX_QUOTES_SCHEMA


# ---------------------------------------------------------------------------
# Crypto
# ---------------------------------------------------------------------------


class FlatFilesStreamCryptoEod(FlatFilesStream):
    """Crypto end-of-day bars from flat files."""

    name = "crypto_flat_files_eod"
    SUBDIR = "global_crypto/eod"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamCrypto1m(FlatFilesStream):
    """Crypto 1-minute bars from flat files."""

    name = "crypto_flat_files_1m"
    SUBDIR = "global_crypto/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamCryptoTrades(FlatFilesStream):
    """Crypto trades from flat files."""

    name = "crypto_flat_files_trades"
    SUBDIR = "global_crypto/trades"
    primary_keys = ["file_date", "ticker", "participant_timestamp", "id"]
    schema = _CRYPTO_TRADES_SCHEMA


# ---------------------------------------------------------------------------
# Futures
# ---------------------------------------------------------------------------


class FlatFilesStreamFutureEod(FlatFilesStream):
    """Futures end-of-day bars from flat files."""

    name = "futures_flat_files_eod"
    SUBDIR = "us_futures/eod"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamFuture1m(FlatFilesStream):
    """Futures 1-minute bars from flat files."""

    name = "futures_flat_files_1m"
    SUBDIR = "us_futures/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]
    schema = _BARS_SCHEMA


class FlatFilesStreamFutureTrades(FlatFilesStream):
    """Futures trades from flat files."""

    name = "futures_flat_files_trades"
    SUBDIR = "us_futures/trades"
    primary_keys = ["file_date", "ticker", "sip_timestamp", "sequence_number"]
    schema = _SIP_TRADES_SCHEMA


class FlatFilesStreamFutureQuotes(FlatFilesStream):
    """Futures quotes from flat files."""

    name = "futures_flat_files_quotes"
    SUBDIR = "us_futures/quotes"
    primary_keys = ["file_date", "ticker", "sip_timestamp", "sequence_number"]
    schema = _SIP_QUOTES_SCHEMA


# ---------------------------------------------------------------------------
# Quote update bars from flat files (DuckDB aggregation)
# ---------------------------------------------------------------------------

_STOCK_QUOTE_SNAPSHOT_SQL = """
WITH bucketed AS (
    SELECT
        ticker,
        (((sip_timestamp - 1) // {interval_ns}) + 1) * {interval_ns} AS asof_timestamp,
        sip_timestamp,
        participant_timestamp,
        trf_timestamp,
        sequence_number,
        ask_exchange,
        ask_price,
        ask_size,
        bid_exchange,
        bid_price,
        bid_size,
        conditions,
        indicators,
        tape
    FROM read_csv(
        '{file_path}',
        header=true,
        auto_detect=false,
        delim=',',
        quote='"',
        columns={{
            'ticker': 'VARCHAR',
            'ask_exchange': 'INTEGER',
            'ask_price': 'DOUBLE',
            'ask_size': 'DOUBLE',
            'bid_exchange': 'INTEGER',
            'bid_price': 'DOUBLE',
            'bid_size': 'DOUBLE',
            'conditions': 'VARCHAR',
            'indicators': 'VARCHAR',
            'participant_timestamp': 'BIGINT',
            'sequence_number': 'BIGINT',
            'sip_timestamp': 'BIGINT',
            'tape': 'INTEGER',
            'trf_timestamp': 'BIGINT'
        }}
    )
),
grouped AS (
    SELECT
        ticker,
        asof_timestamp,
        arg_max(
            {{
                sip_timestamp: sip_timestamp,
                participant_timestamp: participant_timestamp,
                trf_timestamp: trf_timestamp,
                sequence_number: sequence_number,
                ask_exchange: ask_exchange,
                ask_price: ask_price,
                ask_size: ask_size,
                bid_exchange: bid_exchange,
                bid_price: bid_price,
                bid_size: bid_size,
                conditions: conditions,
                indicators: indicators,
                tape: tape
            }},
            struct_pack(ts := sip_timestamp, seq := sequence_number)
        ) AS last_quote
    FROM bucketed
    GROUP BY ticker, asof_timestamp
)
SELECT
    '{file_date}' AS file_date,
    ticker,
    asof_timestamp,
    (last_quote).sip_timestamp AS sip_timestamp,
    (last_quote).participant_timestamp AS participant_timestamp,
    (last_quote).trf_timestamp AS trf_timestamp,
    (last_quote).sequence_number AS sequence_number,
    (last_quote).ask_exchange AS ask_exchange,
    (last_quote).ask_price AS ask_price,
    (last_quote).ask_size AS ask_size,
    (last_quote).bid_exchange AS bid_exchange,
    (last_quote).bid_price AS bid_price,
    (last_quote).bid_size AS bid_size,
    (last_quote).conditions AS conditions,
    (last_quote).indicators AS indicators,
    (last_quote).tape AS tape
FROM grouped
"""

_STOCK_QUOTE_SNAPSHOT_FLAT_FILES_SCHEMA = th.PropertiesList(
    th.Property("file_date", th.StringType),
    th.Property("ticker", th.StringType),
    th.Property("asof_timestamp", th.IntegerType),
    th.Property("sip_timestamp", th.IntegerType),
    th.Property("participant_timestamp", th.IntegerType),
    th.Property("trf_timestamp", th.IntegerType),
    th.Property("sequence_number", th.IntegerType),
    th.Property("ask_exchange", th.IntegerType),
    th.Property("ask_price", th.NumberType),
    th.Property("ask_size", th.NumberType),
    th.Property("bid_exchange", th.IntegerType),
    th.Property("bid_price", th.NumberType),
    th.Property("bid_size", th.NumberType),
    th.Property("conditions", th.StringType),
    th.Property("indicators", th.StringType),
    th.Property("tape", th.IntegerType),
).to_dict()

# Options quote update bar SQL — options CSVs have no participant_timestamp,
# trf_timestamp, conditions, indicators, or tape columns.
_OPTIONS_QUOTE_SNAPSHOT_SQL = """
WITH bucketed AS (
    SELECT
        ticker,
        (((sip_timestamp - 1) // {interval_ns}) + 1) * {interval_ns} AS asof_timestamp,
        sip_timestamp,
        sequence_number,
        ask_exchange,
        ask_price,
        ask_size,
        bid_exchange,
        bid_price,
        bid_size
    FROM read_csv(
        '{file_path}',
        header=true,
        auto_detect=false,
        delim=',',
        quote='"',
        columns={{
            'ticker': 'VARCHAR',
            'ask_exchange': 'INTEGER',
            'ask_price': 'DOUBLE',
            'ask_size': 'DOUBLE',
            'bid_exchange': 'INTEGER',
            'bid_price': 'DOUBLE',
            'bid_size': 'DOUBLE',
            'sequence_number': 'BIGINT',
            'sip_timestamp': 'BIGINT'
        }}
    )
),
grouped AS (
    SELECT
        ticker,
        asof_timestamp,
        arg_max(
            {{
                sip_timestamp: sip_timestamp,
                sequence_number: sequence_number,
                ask_exchange: ask_exchange,
                ask_price: ask_price,
                ask_size: ask_size,
                bid_exchange: bid_exchange,
                bid_price: bid_price,
                bid_size: bid_size
            }},
            struct_pack(ts := sip_timestamp, seq := sequence_number)
        ) AS last_quote
    FROM bucketed
    GROUP BY ticker, asof_timestamp
)
SELECT
    '{file_date}' AS file_date,
    ticker,
    asof_timestamp,
    (last_quote).sip_timestamp AS sip_timestamp,
    (last_quote).sequence_number AS sequence_number,
    (last_quote).ask_exchange AS ask_exchange,
    (last_quote).ask_price AS ask_price,
    (last_quote).ask_size AS ask_size,
    (last_quote).bid_exchange AS bid_exchange,
    (last_quote).bid_price AS bid_price,
    (last_quote).bid_size AS bid_size
FROM grouped
"""

_STOCK_QUOTE_SNAPSHOT_BATCH_SQL = """
WITH bucketed AS (
    SELECT
        regexp_extract(filename, '(\\d{{4}}-\\d{{2}}-\\d{{2}})', 1) AS file_date,
        ticker,
        (((sip_timestamp - 1) // {interval_ns}) + 1) * {interval_ns} AS asof_timestamp,
        sip_timestamp,
        participant_timestamp,
        trf_timestamp,
        sequence_number,
        ask_exchange,
        ask_price,
        ask_size,
        bid_exchange,
        bid_price,
        bid_size,
        conditions,
        indicators,
        tape
    FROM read_csv(
        {file_list},
        filename=true,
        header=true,
        auto_detect=false,
        delim=',',
        quote='"',
        columns={{
            'ticker': 'VARCHAR',
            'ask_exchange': 'INTEGER',
            'ask_price': 'DOUBLE',
            'ask_size': 'DOUBLE',
            'bid_exchange': 'INTEGER',
            'bid_price': 'DOUBLE',
            'bid_size': 'DOUBLE',
            'conditions': 'VARCHAR',
            'indicators': 'VARCHAR',
            'participant_timestamp': 'BIGINT',
            'sequence_number': 'BIGINT',
            'sip_timestamp': 'BIGINT',
            'tape': 'INTEGER',
            'trf_timestamp': 'BIGINT'
        }}
    )
),
grouped AS (
    SELECT
        file_date,
        ticker,
        asof_timestamp,
        arg_max(
            {{
                sip_timestamp: sip_timestamp,
                participant_timestamp: participant_timestamp,
                trf_timestamp: trf_timestamp,
                sequence_number: sequence_number,
                ask_exchange: ask_exchange,
                ask_price: ask_price,
                ask_size: ask_size,
                bid_exchange: bid_exchange,
                bid_price: bid_price,
                bid_size: bid_size,
                conditions: conditions,
                indicators: indicators,
                tape: tape
            }},
            struct_pack(ts := sip_timestamp, seq := sequence_number)
        ) AS last_quote
    FROM bucketed
    GROUP BY file_date, ticker, asof_timestamp
)
SELECT
    file_date,
    ticker,
    asof_timestamp,
    (last_quote).sip_timestamp AS sip_timestamp,
    (last_quote).participant_timestamp AS participant_timestamp,
    (last_quote).trf_timestamp AS trf_timestamp,
    (last_quote).sequence_number AS sequence_number,
    (last_quote).ask_exchange AS ask_exchange,
    (last_quote).ask_price AS ask_price,
    (last_quote).ask_size AS ask_size,
    (last_quote).bid_exchange AS bid_exchange,
    (last_quote).bid_price AS bid_price,
    (last_quote).bid_size AS bid_size,
    (last_quote).conditions AS conditions,
    (last_quote).indicators AS indicators,
    (last_quote).tape AS tape
FROM grouped
ORDER BY file_date
"""

_OPTIONS_QUOTE_SNAPSHOT_BATCH_SQL = """
WITH bucketed AS (
    SELECT
        regexp_extract(filename, '(\\d{{4}}-\\d{{2}}-\\d{{2}})', 1) AS file_date,
        ticker,
        (((sip_timestamp - 1) // {interval_ns}) + 1) * {interval_ns} AS asof_timestamp,
        sip_timestamp,
        sequence_number,
        ask_exchange,
        ask_price,
        ask_size,
        bid_exchange,
        bid_price,
        bid_size
    FROM read_csv(
        {file_list},
        filename=true,
        header=true,
        auto_detect=false,
        delim=',',
        quote='"',
        columns={{
            'ticker': 'VARCHAR',
            'ask_exchange': 'INTEGER',
            'ask_price': 'DOUBLE',
            'ask_size': 'DOUBLE',
            'bid_exchange': 'INTEGER',
            'bid_price': 'DOUBLE',
            'bid_size': 'DOUBLE',
            'sequence_number': 'BIGINT',
            'sip_timestamp': 'BIGINT'
        }}
    )
),
grouped AS (
    SELECT
        file_date,
        ticker,
        asof_timestamp,
        arg_max(
            {{
                sip_timestamp: sip_timestamp,
                sequence_number: sequence_number,
                ask_exchange: ask_exchange,
                ask_price: ask_price,
                ask_size: ask_size,
                bid_exchange: bid_exchange,
                bid_price: bid_price,
                bid_size: bid_size
            }},
            struct_pack(ts := sip_timestamp, seq := sequence_number)
        ) AS last_quote
    FROM bucketed
    GROUP BY file_date, ticker, asof_timestamp
)
SELECT
    file_date,
    ticker,
    asof_timestamp,
    (last_quote).sip_timestamp AS sip_timestamp,
    (last_quote).sequence_number AS sequence_number,
    (last_quote).ask_exchange AS ask_exchange,
    (last_quote).ask_price AS ask_price,
    (last_quote).ask_size AS ask_size,
    (last_quote).bid_exchange AS bid_exchange,
    (last_quote).bid_price AS bid_price,
    (last_quote).bid_size AS bid_size
FROM grouped
ORDER BY file_date
"""

_OPTIONS_QUOTE_SNAPSHOT_FLAT_FILES_SCHEMA = th.PropertiesList(
    th.Property("file_date", th.StringType),
    th.Property("ticker", th.StringType),
    th.Property("asof_timestamp", th.IntegerType),
    th.Property("sip_timestamp", th.IntegerType),
    th.Property("sequence_number", th.IntegerType),
    th.Property("ask_exchange", th.IntegerType),
    th.Property("ask_price", th.NumberType),
    th.Property("ask_size", th.NumberType),
    th.Property("bid_exchange", th.IntegerType),
    th.Property("bid_price", th.NumberType),
    th.Property("bid_size", th.NumberType),
).to_dict()


class QuoteSnapshotFlatFilesStream(FlatFilesStream):
    """Quote update bars aggregated from flat file tick quotes via DuckDB.

    Reads daily CSV.gz quote files and aggregates to interval-contained bars:
    last quote per (ticker, asof_timestamp) where asof_timestamp = ceil(sip_timestamp / interval).

    Downloads are parallelized via a prefetch buffer (configurable workers).
    Processing + yielding remains sequential (no SQLite state contention).
    """

    _interval_ns: int = 60_000_000_000  # 1 minute default
    _RAW_SQL_TEMPLATE: str = _STOCK_QUOTE_SNAPSHOT_SQL
    _RAW_BATCH_SQL_TEMPLATE: str = _STOCK_QUOTE_SNAPSHOT_BATCH_SQL
    _S3_ENDPOINT = "https://files.massive.com"
    _S3_BUCKET_TEMPLATE = "s3://flatfiles/{subdir}/{year}/{month}/{date}.csv.gz"
    FILE_PREFIX: str = "stock_quotes"

    schema = _STOCK_QUOTE_SNAPSHOT_FLAT_FILES_SCHEMA
    primary_keys = ["file_date", "ticker", "asof_timestamp"]

    @property
    def _effective_subdir(self) -> str:
        """Return SUBDIR, allowing per-stream config override via s3_subdir."""
        stream_cfg = self.config.get(self.name)
        if isinstance(stream_cfg, dict) and stream_cfg.get("s3_subdir"):
            return stream_cfg["s3_subdir"]
        return self.SUBDIR

    def _s3_url(self, file_date: str) -> str:
        """Build S3 URL for a given date."""
        year, month, _day = file_date.split("-")
        return self._S3_BUCKET_TEMPLATE.format(
            subdir=self._effective_subdir,
            year=year,
            month=month,
            date=file_date,
        )

    @staticmethod
    def _check_aws_cli() -> None:
        """Verify the AWS CLI is installed. Raises if not found."""

        if shutil.which("aws") is None:
            msg = (
                "AWS CLI ('aws') is required for flat file downloads but was not "
                "found on PATH. Install it: https://docs.aws.amazon.com/cli/"
            )
            raise RuntimeError(msg)

    def _download_file(self, file_date: str, dest_dir: Path) -> Path | None:
        """Download a single flat file from S3. Returns local path or None if not found.

        Uses a lock file to prevent parallel processes from downloading
        the same file simultaneously (race condition between orchestrator workers).
        Downloads to a temp file first, then renames atomically.
        """

        s3_url = self._s3_url(file_date)
        local_path = dest_dir / f"{self.FILE_PREFIX}_{file_date}.csv.gz"
        if local_path.exists():
            return local_path

        # Cross-process lock: other workers wait instead of downloading in parallel
        lock_path = dest_dir / f".{self.FILE_PREFIX}_{file_date}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("w") as lock_fh:
            fcntl.flock(lock_fh, fcntl.LOCK_EX)
            # Re-check after acquiring lock — another process may have finished
            if local_path.exists():
                return local_path

            aws_key = self.config.get("flat_files_aws_key", "")
            aws_secret = self.config.get("flat_files_aws_secret", "")
            env = {
                **os.environ,
                "AWS_ACCESS_KEY_ID": aws_key,
                "AWS_SECRET_ACCESS_KEY": aws_secret,
            }
            # Download to temp file, rename atomically on success
            tmp_path = dest_dir / f".{self.FILE_PREFIX}_{file_date}.tmp.csv.gz"
            self.logger.info("Downloading %s → %s", s3_url, local_path)
            result = subprocess.run(  # noqa: S603
                [
                    "aws",
                    "s3",
                    "cp",
                    s3_url,
                    str(tmp_path),
                    "--endpoint-url",
                    self._S3_ENDPOINT,
                ],
                env=env,
                capture_output=True,
            )
            if result.returncode != 0:
                tmp_path.unlink(missing_ok=True)
                stderr = result.stderr.decode(errors="replace") if result.stderr else ""
                self.logger.warning("aws s3 cp stderr: %s", stderr.strip())
                if (
                    "404" in stderr
                    or "NoSuchKey" in stderr
                    or "does not exist" in stderr
                ):
                    self.logger.info("No file on S3 for %s, skipping.", file_date)
                    return None
                msg = f"S3 download failed for {file_date}: {stderr.strip()}"
                raise RuntimeError(msg)
            # Atomic rename — other processes see complete file or nothing
            tmp_path.rename(local_path)
        # Lock file cleaned up (lock released when fh closes)
        lock_path.unlink(missing_ok=True)
        return local_path

    def _iter_dates_for_download(
        self, context: dict | None
    ) -> t.Iterator[tuple[str, Path | None]]:
        """Generate (date, None) pairs from config for S3 download mode.

        Unlike _iter_files_in_bounds which requires local files,
        this generates dates from start_file_date/end_file_date config.
        """
        stream_cfg = self.config.get(self.name)
        cfg_start = (
            stream_cfg.get("start_file_date") if isinstance(stream_cfg, dict) else None
        )
        cfg_end = (
            stream_cfg.get("end_file_date") if isinstance(stream_cfg, dict) else None
        )
        if not cfg_start or not cfg_end:
            msg = (
                f"Stream {self.name}: flat_file_download_workers requires "
                f"start_file_date and end_file_date in stream config."
            )
            raise ValueError(msg)

        start_value = self.get_starting_replication_key_value(context)
        if start_value and start_value > cfg_start:
            cfg_start = start_value

        current = date_cls.fromisoformat(cfg_start)
        end = date_cls.fromisoformat(cfg_end)
        while current <= end:
            yield current.isoformat(), None
            current += timedelta(days=1)

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Aggregate quote ticks to update bars using DuckDB.

        If flat_file_download_workers > 0, downloads files from S3 in parallel
        into a temp directory, then processes each sequentially with DuckDB.
        Downloads and DuckDB processing are decoupled — downloads prefetch ahead
        while processing yields results, avoiding SQLite state contention.
        """

        stream_cfg = self.config.get(self.name)
        workers = (
            int(stream_cfg.get("flat_file_download_workers", 0))
            if isinstance(stream_cfg, dict)
            else 0
        )

        conn = duckdb.connect()
        duckdb_params = dict(self.config.get("duckdb_params") or {})
        duckdb_batch_size = configure_duckdb_connection(conn, duckdb_params, resolve_paths=True)

        if duckdb_batch_size > 1:
            sql_template = self._RAW_BATCH_SQL_TEMPLATE.format(
                interval_ns=self._interval_ns,
                file_list="{file_list}",
            )
        else:
            sql_template = self._RAW_SQL_TEMPLATE.format(
                interval_ns=self._interval_ns,
                file_path="{file_path}",
                file_date="{file_date}",
            )

        try:
            if workers > 0:
                # Download mode: verify AWS CLI, iterate configured date range.
                self._check_aws_cli()
                files_to_process = list(self._iter_dates_for_download(context))
                if not files_to_process:
                    return
                yield from self._process_with_prefetch(
                    conn,
                    sql_template,
                    files_to_process,
                    workers,
                    duckdb_batch_size,
                )
            else:
                # Local mode: requires flat_files_base_dir with pre-downloaded files
                yield from self._process_sequential(
                    conn,
                    sql_template,
                    self._iter_files_in_bounds(context),
                    duckdb_batch_size,
                )
        finally:
            conn.close()

    def _process_sequential(
        self,
        conn,  # noqa: ANN001
        sql_template: str,
        files: t.Iterable[tuple[str, Path]],
        duckdb_batch_size: int = 1,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Process local files with DuckDB, optionally batching multiple files."""
        if duckdb_batch_size <= 1:
            for file_date, filepath in files:
                self.logger.info(
                    "Processing %s via DuckDB (date=%s)", filepath, file_date
                )
                yield from self._run_duckdb_query(
                    conn, sql_template, str(filepath), file_date
                )
            return

        batch: list[tuple[str, Path]] = []
        for file_date, filepath in files:
            batch.append((file_date, filepath))
            if len(batch) >= duckdb_batch_size:
                yield from self._process_file_batch(conn, sql_template, batch)
                batch.clear()
        if batch:
            yield from self._process_file_batch(conn, sql_template, batch)

    def _process_with_prefetch(
        self,
        conn,  # noqa: ANN001
        sql_template: str,
        files: list[tuple[str, Path]],
        workers: int,
        duckdb_batch_size: int = 1,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Download files in parallel, process with DuckDB.

        When duckdb_batch_size > 1, accumulates downloaded files into batches
        and processes each batch in a single DuckDB query for better throughput.

        Uses flat_files_base_dir as the download directory if configured,
        otherwise falls back to a temporary directory (cleaned up after).
        """
        # Use configured flat files dir if available, otherwise temp dir
        cache_dir = self._flat_files_dir
        use_temp = cache_dir is None
        tmp_ctx = (
            tempfile.TemporaryDirectory(prefix="tap_massive_ff_") if use_temp else None
        )  # noqa: SIM108
        download_dir = Path(tmp_ctx.name) if tmp_ctx else cache_dir
        download_dir.mkdir(parents=True, exist_ok=True)

        pool = ThreadPoolExecutor(max_workers=workers)
        pending: deque[tuple[str, Future]] = deque()
        file_iter = iter(files)

        def _submit_next() -> bool:
            """Submit next download. Returns False when exhausted."""
            try:
                file_date, filepath = next(file_iter)
            except StopIteration:
                return False
            if filepath and filepath.exists():
                fut: Future = pool.submit(lambda p: p, filepath)
            else:
                fut = pool.submit(self._download_file, file_date, download_dir)
            pending.append((file_date, fut))
            return True

        try:
            # Fill prefetch buffer — enough to fill a batch
            prefetch_count = (
                max(workers, duckdb_batch_size) if duckdb_batch_size > 1 else workers
            )
            for _ in range(prefetch_count):
                if not _submit_next():
                    break

            if duckdb_batch_size <= 1:
                # Single-file mode: existing behavior
                while pending:
                    file_date, fut = pending.popleft()
                    local_path = fut.result()
                    if local_path is None:
                        _submit_next()
                        continue
                    self.logger.info(
                        "Processing %s via DuckDB (date=%s)", local_path, file_date
                    )
                    yield from self._run_duckdb_query(
                        conn,
                        sql_template,
                        str(local_path),
                        file_date,
                    )
                    if use_temp:
                        local_path.unlink(missing_ok=True)
                    _submit_next()
            else:
                # Batch mode: accumulate files, process in batches
                ready_batch: list[tuple[str, Path]] = []

                while pending:
                    file_date, fut = pending.popleft()
                    local_path = fut.result()
                    if local_path is None:
                        _submit_next()
                        continue

                    ready_batch.append((file_date, local_path))
                    _submit_next()

                    if len(ready_batch) >= duckdb_batch_size:
                        yield from self._process_file_batch(
                            conn,
                            sql_template,
                            ready_batch,
                            use_temp,
                        )
                        ready_batch.clear()

                # Flush remaining
                if ready_batch:
                    yield from self._process_file_batch(
                        conn,
                        sql_template,
                        ready_batch,
                        use_temp,
                    )
        finally:
            pool.shutdown(wait=True)
            if tmp_ctx:
                tmp_ctx.cleanup()

    _FETCH_BATCH_SIZE = 1_000_000

    def _iter_duckdb_result(
        self,
        result,  # noqa: ANN001
        execute_elapsed: float,
        label: str,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Yield dicts from a DuckDB result with per-file perf timing.

        Uses Arrow-based to_arrow_reader for zero-copy transfer from
        DuckDB, then batch.to_pylist() converts to list[dict] in C.
        Tracks fetch time separately from target overhead to isolate
        DuckDB vs Postgres bottlenecks.
        """
        reader = result.to_arrow_reader(rows_per_batch=self._FETCH_BATCH_SIZE)
        row_count = 0
        fetch_elapsed = 0.0
        wall_start = time.perf_counter()

        while True:
            ft0 = time.perf_counter()
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                fetch_elapsed += time.perf_counter() - ft0
                break
            rows = batch.to_pylist()
            fetch_elapsed += time.perf_counter() - ft0

            row_count += len(rows)
            yield from rows

        wall_elapsed = time.perf_counter() - wall_start
        target_overhead = wall_elapsed - fetch_elapsed
        total = execute_elapsed + wall_elapsed
        self.logger.info(
            "PERF duckdb_complete %s rows=%d execute=%.2fs fetch=%.2fs "
            "target_overhead=%.2fs total=%.2fs",
            label,
            row_count,
            execute_elapsed,
            fetch_elapsed,
            target_overhead,
            total,
        )

    def _run_duckdb_query(
        self,
        conn,  # noqa: ANN001
        sql_template: str,
        source: str,
        file_date: str,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Execute the aggregation query for a single file and yield rows."""
        query = sql_template.replace("{file_path}", source).replace("{file_date}", file_date)

        t0 = time.perf_counter()
        result = conn.execute(query)
        execute_elapsed = time.perf_counter() - t0
        self.logger.info("PERF duckdb_execute file=%s elapsed=%.2fs", file_date, execute_elapsed)

        yield from self._iter_duckdb_result(result, execute_elapsed, f"file={file_date}")

    def _run_duckdb_batch_query(
        self,
        conn,  # noqa: ANN001
        sql_template: str,
        file_paths: list[str],
    ) -> t.Iterable[dict[str, t.Any]]:
        """Execute batch aggregation query across multiple files and yield rows.

        Unlike _run_duckdb_query, file_date is extracted from filenames by
        DuckDB via regexp_extract(filename, ...) and returned as a column.
        """
        file_list_literal = "[" + ", ".join(f"'{p}'" for p in file_paths) + "]"
        query = sql_template.replace("{file_list}", file_list_literal)

        t0 = time.perf_counter()
        result = conn.execute(query)
        execute_elapsed = time.perf_counter() - t0
        self.logger.info("PERF duckdb_execute files=%d elapsed=%.2fs", len(file_paths), execute_elapsed)

        yield from self._iter_duckdb_result(result, execute_elapsed, f"files={len(file_paths)}")

    def _process_file_batch(
        self,
        conn,  # noqa: ANN001
        sql_template: str,
        ready_batch: list[tuple[str, Path]],
        use_temp: bool = False,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Log, execute a batch DuckDB query, and clean up temp files."""
        self.logger.info(
            "Processing batch of %d files via DuckDB (%s to %s)",
            len(ready_batch),
            ready_batch[0][0],
            ready_batch[-1][0],
        )
        yield from self._run_duckdb_batch_query(
            conn, sql_template, [str(p) for _, p in ready_batch]
        )
        if use_temp:
            for _, p in ready_batch:
                p.unlink(missing_ok=True)


class _OptionsQuoteSnapshotFlatFilesBase(QuoteSnapshotFlatFilesStream):
    # Massive docs use quotes_v1; override SUBDIR if your account uses "quotes"
    SUBDIR = "us_options_opra/quotes_v1"
    FILE_PREFIX = "options_quotes"
    _RAW_SQL_TEMPLATE = _OPTIONS_QUOTE_SNAPSHOT_SQL
    _RAW_BATCH_SQL_TEMPLATE = _OPTIONS_QUOTE_SNAPSHOT_BATCH_SQL
    schema = _OPTIONS_QUOTE_SNAPSHOT_FLAT_FILES_SCHEMA


class OptionsQuoteSnapshotFlatFiles1SecondStream(_OptionsQuoteSnapshotFlatFilesBase):
    name = "options_quote_snapshots_flat_files_1_second"
    _interval_ns = 1_000_000_000


class OptionsQuoteSnapshotFlatFiles30SecondStream(_OptionsQuoteSnapshotFlatFilesBase):
    name = "options_quote_snapshots_flat_files_30_second"
    _interval_ns = 30_000_000_000


class OptionsQuoteSnapshotFlatFiles1MinuteStream(_OptionsQuoteSnapshotFlatFilesBase):
    name = "options_quote_snapshots_flat_files_1_minute"
    _interval_ns = 60_000_000_000


class OptionsQuoteSnapshotFlatFiles5MinuteStream(_OptionsQuoteSnapshotFlatFilesBase):
    name = "options_quote_snapshots_flat_files_5_minute"
    _interval_ns = 300_000_000_000


class OptionsQuoteSnapshotFlatFiles30MinuteStream(_OptionsQuoteSnapshotFlatFilesBase):
    name = "options_quote_snapshots_flat_files_30_minute"
    _interval_ns = 1_800_000_000_000
