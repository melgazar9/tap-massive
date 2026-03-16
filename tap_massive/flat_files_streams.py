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
import typing as t
from pathlib import Path

import duckdb
from singer_sdk import Stream
from singer_sdk import typing as th

from tap_massive.utils import safe_int

try:
    from isal import igzip

    _open_gz = igzip.open
except ImportError:
    _open_gz = gzip.open

logger = logging.getLogger(__name__)

_FILE_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


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
                return Path(override)
        base = self.config.get("flat_files_base_dir", "")
        if not base:
            return None
        return Path(base) / self.SUBDIR

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
    schema = _SIP_QUOTES_SCHEMA


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

_QUOTE_UPDATE_BAR_SQL = """
SELECT
    ticker,
    (((sip_timestamp - 1) // {interval_ns}) + 1) * {interval_ns} AS window_start,
    last(sip_timestamp ORDER BY sip_timestamp, sequence_number) AS sip_timestamp,
    last(participant_timestamp ORDER BY sip_timestamp, sequence_number) AS participant_timestamp,
    last(trf_timestamp ORDER BY sip_timestamp, sequence_number) AS trf_timestamp,
    last(sequence_number ORDER BY sip_timestamp, sequence_number) AS sequence_number,
    last(ask_exchange ORDER BY sip_timestamp, sequence_number) AS ask_exchange,
    last(ask_price ORDER BY sip_timestamp, sequence_number) AS ask_price,
    last(ask_size ORDER BY sip_timestamp, sequence_number) AS ask_size,
    last(bid_exchange ORDER BY sip_timestamp, sequence_number) AS bid_exchange,
    last(bid_price ORDER BY sip_timestamp, sequence_number) AS bid_price,
    last(bid_size ORDER BY sip_timestamp, sequence_number) AS bid_size,
    last(conditions ORDER BY sip_timestamp, sequence_number) AS conditions,
    last(indicators ORDER BY sip_timestamp, sequence_number) AS indicators,
    last(tape ORDER BY sip_timestamp, sequence_number) AS tape
FROM read_csv(
    '{file_path}',
    header=true,
    columns={{
        'ticker': 'VARCHAR',
        'sip_timestamp': 'BIGINT',
        'participant_timestamp': 'BIGINT',
        'trf_timestamp': 'BIGINT',
        'sequence_number': 'BIGINT',
        'ask_exchange': 'INTEGER',
        'ask_price': 'DOUBLE',
        'ask_size': 'DOUBLE',
        'bid_exchange': 'INTEGER',
        'bid_price': 'DOUBLE',
        'bid_size': 'DOUBLE',
        'conditions': 'VARCHAR',
        'indicators': 'VARCHAR',
        'tape': 'INTEGER'
    }}
)
GROUP BY ticker, window_start
ORDER BY ticker, window_start
"""

_QUOTE_UPDATE_BAR_FLAT_FILES_SCHEMA = th.PropertiesList(
    th.Property("file_date", th.StringType),
    th.Property("ticker", th.StringType),
    th.Property("window_start", th.IntegerType),
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


class QuoteUpdateBarFlatFilesStream(FlatFilesStream):
    """Quote update bars aggregated from flat file tick quotes via DuckDB.

    Reads daily CSV.gz quote files and aggregates to interval-contained bars:
    last quote per (ticker, window) where window = ceil(sip_timestamp / interval).

    Downloads are parallelized via a prefetch buffer (configurable workers).
    Processing + yielding remains sequential (no SQLite state contention).
    """

    _interval_ns: int = 60_000_000_000  # 1 minute default
    _S3_ENDPOINT = "https://files.massive.com"
    _S3_BUCKET_TEMPLATE = "s3://flatfiles/{subdir}/{year}/{month}/{date}.csv.gz"

    schema = _QUOTE_UPDATE_BAR_FLAT_FILES_SCHEMA
    primary_keys = ["file_date", "ticker", "window_start"]

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
        local_path = dest_dir / f"{file_date}.csv.gz"
        if local_path.exists():
            return local_path

        # Cross-process lock: other workers wait instead of downloading in parallel
        lock_path = dest_dir / f".{file_date}.lock"
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
            tmp_path = dest_dir / f".{file_date}.tmp.csv.gz"
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
                stderr=subprocess.PIPE,
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
        from datetime import date as date_cls  # noqa: PLC0415
        from datetime import timedelta  # noqa: PLC0415

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

        sql_template = _QUOTE_UPDATE_BAR_SQL.format(
            interval_ns=self._interval_ns,
            file_path="{file_path}",
        )
        conn = duckdb.connect()

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
                )
            else:
                # Local mode: requires flat_files_base_dir with pre-downloaded files
                yield from self._process_sequential(
                    conn,
                    sql_template,
                    self._iter_files_in_bounds(context),
                )
        finally:
            conn.close()

    def _process_sequential(
        self,
        conn,  # noqa: ANN001
        sql_template: str,
        files: t.Iterable[tuple[str, Path]],
    ) -> t.Iterable[dict[str, t.Any]]:
        """Process local files sequentially with DuckDB."""
        for file_date, filepath in files:
            self.logger.info("Processing %s via DuckDB (date=%s)", filepath, file_date)
            yield from self._run_duckdb_query(
                conn, sql_template, str(filepath), file_date
            )

    def _process_with_prefetch(
        self,
        conn,  # noqa: ANN001
        sql_template: str,
        files: list[tuple[str, Path]],
        workers: int,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Download files in parallel, process each sequentially after download.

        Uses flat_files_base_dir as the download directory if configured,
        otherwise falls back to a temporary directory (cleaned up after).
        """
        import tempfile  # noqa: PLC0415
        from collections import deque  # noqa: PLC0415
        from concurrent.futures import Future, ThreadPoolExecutor  # noqa: PLC0415

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
            # Fill prefetch buffer
            for _ in range(workers):
                if not _submit_next():
                    break

            while pending:
                file_date, fut = pending.popleft()
                local_path = fut.result()
                if local_path is None:
                    # File doesn't exist on S3 (weekend/holiday) — skip
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
                # Delete temp files after processing to free disk
                if use_temp:
                    local_path.unlink(missing_ok=True)
                _submit_next()
        finally:
            pool.shutdown(wait=True)
            if tmp_ctx:
                tmp_ctx.cleanup()

    def _run_duckdb_query(
        self,
        conn,  # noqa: ANN001
        sql_template: str,
        source: str,
        file_date: str,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Execute the aggregation query and yield rows."""
        query = sql_template.format(file_path=source)
        result = conn.execute(query)
        columns = [desc[0] for desc in result.description]
        while batch := result.fetchmany(10000):
            for row_tuple in batch:
                row = dict(zip(columns, row_tuple))
                row["file_date"] = file_date
                yield row


class _OptionsQuoteUpdateBarFlatFilesBase(QuoteUpdateBarFlatFilesStream):
    # Massive docs use quotes_v1; override SUBDIR if your account uses "quotes"
    SUBDIR = "us_options_opra/quotes_v1"


class OptionsQuoteUpdateBarFlatFiles1SecondStream(_OptionsQuoteUpdateBarFlatFilesBase):
    name = "options_quote_update_bars_flat_files_1_second"
    _interval_ns = 1_000_000_000


class OptionsQuoteUpdateBarFlatFiles30SecondStream(_OptionsQuoteUpdateBarFlatFilesBase):
    name = "options_quote_update_bars_flat_files_30_second"
    _interval_ns = 30_000_000_000


class OptionsQuoteUpdateBarFlatFiles1MinuteStream(_OptionsQuoteUpdateBarFlatFilesBase):
    name = "options_quote_update_bars_flat_files_1_minute"
    _interval_ns = 60_000_000_000


class OptionsQuoteUpdateBarFlatFiles5MinuteStream(_OptionsQuoteUpdateBarFlatFilesBase):
    name = "options_quote_update_bars_flat_files_5_minute"
    _interval_ns = 300_000_000_000


class OptionsQuoteUpdateBarFlatFiles30MinuteStream(_OptionsQuoteUpdateBarFlatFilesBase):
    name = "options_quote_update_bars_flat_files_30_minute"
    _interval_ns = 1_800_000_000_000