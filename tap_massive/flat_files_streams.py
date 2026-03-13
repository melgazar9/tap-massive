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
import functools
import gzip
import logging
import re
import typing as t
from pathlib import Path

from singer_sdk import Stream
from singer_sdk import typing as th

from tap_massive.base_streams import safe_int

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
