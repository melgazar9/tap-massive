"""Flat file streams for reading local CSV / CSV.gz historical data.

Reads date-sorted flat files from a configured directory, discovers
schema dynamically from CSV headers, and yields records incrementally
using ``file_date`` (extracted from each filename) as the replication key.
"""

from __future__ import annotations

import csv
import gzip
import logging
import re
import typing as t
from pathlib import Path

from singer_sdk import Stream
from singer_sdk import typing as th

try:
    from isal import igzip

    _open_gz = igzip.open
except ImportError:
    _open_gz = gzip.open

if t.TYPE_CHECKING:
    from singer_sdk import Tap

logger = logging.getLogger(__name__)

_FILE_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------


class FlatFilesStream(Stream):
    """Base stream that reads sorted local CSV/CSV.gz flat files.

    Schema is discovered dynamically from the first file's CSV headers.
    All columns are emitted as strings; the target handles type coercion.

    Subclasses set ``name``, ``SUBDIR``, and ``primary_keys``.
    """

    SUBDIR: t.ClassVar[str] = ""

    replication_method = "INCREMENTAL"
    replication_key = "file_date"
    is_sorted = True

    # -- lifecycle -----------------------------------------------------------

    def __init__(self, tap: Tap, **kwargs: t.Any) -> None:
        self._discovered_schema: dict | None = None
        super().__init__(tap=tap, **kwargs)

    # -- schema --------------------------------------------------------------

    @property
    def schema(self) -> dict:
        """Lazily discover schema from the first file's CSV headers."""
        if self._discovered_schema is None:
            self._discover_schema()
        return self._discovered_schema  # type: ignore[return-value]

    def _discover_schema(self) -> None:
        files = self._list_files()
        if not files:
            logger.warning(
                "No files found for %s — minimal schema only.",
                self.name,
            )
            self._discovered_schema = th.PropertiesList(
                th.Property("file_date", th.StringType),
            ).to_dict()
            return

        headers = self._read_headers(files[0][1])
        props: list[th.Property] = [th.Property("file_date", th.StringType)]
        for col in headers:
            props.append(th.Property(col, th.StringType))
        self._discovered_schema = th.PropertiesList(*props).to_dict()

    @staticmethod
    def _read_headers(filepath: Path) -> list[str]:
        """Read CSV column names from the first line of a file."""
        if filepath.name.endswith(".gz"):
            with _open_gz(filepath, "rt") as fh:
                return next(csv.reader(fh))
        with filepath.open() as fh:
            return next(csv.reader(fh))

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
                yield row


# ---------------------------------------------------------------------------
# Stocks
# ---------------------------------------------------------------------------


class FlatFilesStreamStockEod(FlatFilesStream):
    """Stock end-of-day bars from flat files."""

    name = "stocks_flat_files_eod"
    SUBDIR = "us_stocks_sip/eod"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamStock1m(FlatFilesStream):
    """Stock 1-minute bars from flat files."""

    name = "stocks_flat_files_1m"
    SUBDIR = "us_stocks_sip/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamStockTrades(FlatFilesStream):
    """Stock trades from flat files."""

    name = "stocks_flat_files_trades"
    SUBDIR = "us_stocks_sip/trades"
    primary_keys = ["file_date", "ticker", "sip_timestamp", "sequence_number"]


class FlatFilesStreamStockQuotes(FlatFilesStream):
    """Stock quotes from flat files."""

    name = "stocks_flat_files_quotes"
    SUBDIR = "us_stocks_sip/quotes"
    primary_keys = ["file_date", "ticker", "sip_timestamp", "sequence_number"]


# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------


class FlatFilesStreamOptionEod(FlatFilesStream):
    """Options end-of-day bars from flat files."""

    name = "options_flat_files_eod"
    SUBDIR = "us_options_opra/eod"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamOption1m(FlatFilesStream):
    """Options 1-minute bars from flat files."""

    name = "options_flat_files_1m"
    SUBDIR = "us_options_opra/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamOptionTrades(FlatFilesStream):
    """Options trades from flat files."""

    name = "options_flat_files_trades"
    SUBDIR = "us_options_opra/trades"
    primary_keys = ["file_date", "ticker", "sip_timestamp"]


class FlatFilesStreamOptionQuotes(FlatFilesStream):
    """Options quotes from flat files."""

    name = "options_flat_files_quotes"
    SUBDIR = "us_options_opra/quotes"
    primary_keys = ["file_date", "ticker", "sip_timestamp", "sequence_number"]


# ---------------------------------------------------------------------------
# Indices
# ---------------------------------------------------------------------------


class FlatFilesStreamIndexEod(FlatFilesStream):
    """Indices end-of-day bars from flat files."""

    name = "indices_flat_files_eod"
    SUBDIR = "us_indices/eod"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamIndex1m(FlatFilesStream):
    """Indices 1-minute bars from flat files."""

    name = "indices_flat_files_1m"
    SUBDIR = "us_indices/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamIndexValues(FlatFilesStream):
    """Indices tick-level values from flat files."""

    name = "indices_flat_files_values"
    SUBDIR = "us_indices/values"
    primary_keys = ["file_date", "ticker", "timestamp"]


# ---------------------------------------------------------------------------
# Forex
# ---------------------------------------------------------------------------


class FlatFilesStreamForexEod(FlatFilesStream):
    """Forex end-of-day bars from flat files."""

    name = "forex_flat_files_eod"
    SUBDIR = "global_forex/eod"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamForex1m(FlatFilesStream):
    """Forex 1-minute bars from flat files."""

    name = "forex_flat_files_1m"
    SUBDIR = "global_forex/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamForexQuotes(FlatFilesStream):
    """Forex quotes from flat files."""

    name = "forex_flat_files_quotes"
    SUBDIR = "global_forex/quotes"
    primary_keys = ["file_date", "ticker", "participant_timestamp"]


# ---------------------------------------------------------------------------
# Crypto
# ---------------------------------------------------------------------------


class FlatFilesStreamCryptoEod(FlatFilesStream):
    """Crypto end-of-day bars from flat files."""

    name = "crypto_flat_files_eod"
    SUBDIR = "global_crypto/eod"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamCrypto1m(FlatFilesStream):
    """Crypto 1-minute bars from flat files."""

    name = "crypto_flat_files_1m"
    SUBDIR = "global_crypto/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamCryptoTrades(FlatFilesStream):
    """Crypto trades from flat files."""

    name = "crypto_flat_files_trades"
    SUBDIR = "global_crypto/trades"
    primary_keys = ["file_date", "ticker", "participant_timestamp", "id"]


# ---------------------------------------------------------------------------
# Futures
# ---------------------------------------------------------------------------


class FlatFilesStreamFutureEod(FlatFilesStream):
    """Futures end-of-day bars from flat files."""

    name = "futures_flat_files_eod"
    SUBDIR = "us_futures/eod"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamFuture1m(FlatFilesStream):
    """Futures 1-minute bars from flat files."""

    name = "futures_flat_files_1m"
    SUBDIR = "us_futures/bars_1m"
    primary_keys = ["file_date", "ticker", "window_start"]


class FlatFilesStreamFutureTrades(FlatFilesStream):
    """Futures trades from flat files."""

    name = "futures_flat_files_trades"
    SUBDIR = "us_futures/trades"
    primary_keys = ["file_date", "ticker", "sip_timestamp", "sequence_number"]


class FlatFilesStreamFutureQuotes(FlatFilesStream):
    """Futures quotes from flat files."""

    name = "futures_flat_files_quotes"
    SUBDIR = "us_futures/quotes"
    primary_keys = ["file_date", "ticker", "sip_timestamp", "sequence_number"]
