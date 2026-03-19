"""Cross-process disk cache and path resolution for parallel Meltano subprocesses.

Each Meltano subprocess gets its own Python process and in-memory state. This module
provides a file-backed cache so that parallel subprocesses of the same tap can share
expensive API discovery results (ticker lists, option contracts) without each
independently fetching from the upstream API.

Uses:
- JSON manifests with schema versioning and TTL-based invalidation
- fcntl.flock for cross-process mutual exclusion
- tempfile + os.replace for atomic writes (POSIX)
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import pwd
import re
import tempfile
import typing as t
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Path resolution — single source of truth for tilde expansion in Docker.
# Meltano strips HOME from plugin environments, so stdlib expanduser() breaks.
# ---------------------------------------------------------------------------


def resolve_home(path: Path) -> Path:
    """Expand ~ reliably, even when HOME is stripped from the environment."""
    s = str(path)
    if s == "~/.cache" or s.startswith("~/.cache/"):
        xdg_cache_home = os.environ.get("XDG_CACHE_HOME")
        if xdg_cache_home:
            suffix = s.removeprefix("~/.cache").lstrip("/")
            return Path(xdg_cache_home) / suffix if suffix else Path(xdg_cache_home)

    if s != "~" and not s.startswith("~/"):
        return path

    home = os.environ.get("HOME") or os.environ.get("APP_HOME") or ""
    if not home or home == "/":
        try:
            home = pwd.getpwuid(os.getuid()).pw_dir
        except KeyError:
            home = ""
    if not home or home == "/":
        raise RuntimeError(
            f"Cannot resolve '~' in path '{s}': HOME is not set and no passwd entry for UID {os.getuid()}. "
            f"Set HOME in the environment or use an absolute path in meltano.yml."
        )
    return Path(s.replace("~", home, 1))


_SCHEMA_VERSION = "v1"
_UNSAFE_PATH_RE = re.compile(r"[/\\\x00]|\.\.")


def _sanitize_path_component(component: str) -> str:
    """Remove path separators, null bytes, and '..' from a single path component."""
    return _UNSAFE_PATH_RE.sub("_", component)


def compute_fingerprint(canonical: dict[str, t.Any]) -> str:
    """SHA-256 hex digest of a deterministically serialized dict."""
    serialized = json.dumps(canonical, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


class DiskCache:
    """File-backed cross-process cache with get-or-fetch semantics.

    Parameters
    ----------
    cache_dir : str
        Root directory from ``MELTANO_SHARED_CACHE_DIR`` env var.
    namespace : str
        Tap-specific subdirectory, e.g. ``"tap_massive"``.
    ttl_hours : float
        Manifest expiry in hours. Must be positive.
    """

    def __init__(self, cache_dir: str, namespace: str, ttl_hours: float = 36.0) -> None:
        if ttl_hours <= 0:
            raise ValueError(f"ttl_hours must be positive, got {ttl_hours}")
        self._cache_dir = cache_dir
        self._namespace = _sanitize_path_component(namespace)
        self._ttl_hours = ttl_hours

    def _manifest_path(self, key: str) -> str:
        """Build the absolute path for a cache manifest file.

        The *key* is expected to be a ``/``-separated logical path like
        ``tickers/stock/<fingerprint>`` or ``options_contracts/v1/AAPL/<fingerprint>``.
        Each segment is sanitized individually.
        """
        parts = [_sanitize_path_component(p) for p in key.split("/") if p]
        return os.path.join(self._cache_dir, self._namespace, *parts) + ".json"

    def _lock_path(self, manifest_path: str) -> str:
        return manifest_path + ".lock"

    def _read_manifest(self, manifest_path: str) -> t.Any | None:
        """Read and validate a cache manifest. Returns None on miss/expired/corrupt."""
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
            if not isinstance(exc, FileNotFoundError):
                logger.warning(
                    "Disk cache: corrupt or unreadable manifest %s: %s",
                    manifest_path,
                    exc,
                )
            return None

        try:
            if manifest["schema_version"] != _SCHEMA_VERSION:
                logger.warning(
                    "Disk cache: schema version mismatch in %s", manifest_path
                )
                return None
            created = datetime.fromisoformat(manifest["created_utc"])
            ttl = manifest.get("ttl_hours", self._ttl_hours)
            age_hours = (datetime.now(timezone.utc) - created).total_seconds() / 3600
            if age_hours > ttl:
                logger.debug(
                    "Disk cache: expired manifest %s (%.1fh old, ttl=%.1fh)",
                    manifest_path,
                    age_hours,
                    ttl,
                )
                return None
            return manifest["data"]
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Disk cache: invalid manifest %s: %s", manifest_path, exc)
            return None

    def _write_manifest(self, manifest_path: str, data: t.Any) -> None:
        """Write a cache manifest atomically via tempfile + os.replace.

        Caller is responsible for ensuring the parent directory exists.
        """
        manifest = {
            "schema_version": _SCHEMA_VERSION,
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "ttl_hours": self._ttl_hours,
            "data": data,
        }
        parent = os.path.dirname(manifest_path)
        try:
            fd, tmp_path = tempfile.mkstemp(dir=parent, suffix=".tmp")
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(manifest, f)
                os.replace(tmp_path, manifest_path)
            except Exception:
                os.unlink(tmp_path)
                raise
        except OSError as exc:
            logger.warning("Disk cache: failed to write %s: %s", manifest_path, exc)

    def get_or_fetch(
        self,
        key: str,
        fetch_fn: t.Callable[[], t.Any],
    ) -> t.Any:
        """Return cached data or fetch, with cross-process locking to prevent stampede.

        1. Fast path (unlocked): read manifest → return on valid hit.
        2. Slow path (locked): acquire exclusive flock → recheck manifest →
           call fetch_fn() on miss → write atomically → return.

        Disk I/O errors are swallowed with a warning and fall through to fetch_fn().
        fetch_fn() failures propagate to the caller.
        """
        manifest_path = self._manifest_path(key)

        # Fast path: unlocked read
        cached = self._read_manifest(manifest_path)
        if cached is not None:
            return cached

        # Slow path: lock, recheck, fetch, write
        lock_path = self._lock_path(manifest_path)
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        try:
            with open(lock_path, "w") as lock_fd:
                fcntl.flock(lock_fd, fcntl.LOCK_EX)
                cached = self._read_manifest(manifest_path)
                if cached is not None:
                    return cached
                data = fetch_fn()
                self._write_manifest(manifest_path, data)
                return data
        except OSError as exc:
            logger.warning(
                "Disk cache: lock failed for %s: %s — falling back to direct fetch",
                key,
                exc,
            )
            return fetch_fn()
