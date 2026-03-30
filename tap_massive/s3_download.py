"""Massive S3 flat file download with locking, retry, and atomic rename."""

from __future__ import annotations

import dataclasses
import fcntl
import logging
import os
import random
import shutil
import subprocess
import time
from pathlib import Path

logger = logging.getLogger(__name__)

S3_ENDPOINT = "https://files.massive.com"
S3_BUCKET_TEMPLATE = "s3://flatfiles/{subdir}/{year}/{month}/{date}.csv.gz"


@dataclasses.dataclass
class DownloadConfig:
    """Optional knobs for ``download_flat_file``."""

    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    timeout_seconds: int = 0
    max_retries: int = 3
    jitter_max_seconds: float = 30.0


def _build_s3_url(s3_subdir: str, file_date: str) -> str:
    year, month, _day = file_date.split("-")
    return S3_BUCKET_TEMPLATE.format(
        subdir=s3_subdir, year=year, month=month, date=file_date
    )


def _cleanup_temp_files(tmp_path: Path) -> None:
    """Remove temp and AWS CLI multipart intermediates.

    AWS CLI creates intermediate files with random suffixes
    (e.g. .tmp.csv.gz.5cA0b359) during multipart downloads.
    If the process is killed mid-download, these are left
    behind.
    """
    for f in tmp_path.parent.glob(f"{tmp_path.name}*"):
        f.unlink(missing_ok=True)
        logger.info("Cleaned temp file: %s", f.name)


def _download_with_retries(
    *,
    s3_url: str,
    tmp_path: Path,
    file_date: str,
    env: dict[str, str],
    cfg: DownloadConfig,
) -> subprocess.CompletedProcess[bytes] | None:
    """Run ``aws s3 cp`` with retry on 429/403.

    Returns the successful CompletedProcess, or None when the
    file does not exist on S3 (404 / NoSuchKey).
    """
    for attempt in range(cfg.max_retries):
        try:
            result = subprocess.run(  # noqa: S603
                [  # noqa: S607
                    "aws",
                    "s3",
                    "cp",
                    s3_url,
                    str(tmp_path),
                    "--endpoint-url",
                    S3_ENDPOINT,
                ],
                env=env,
                capture_output=True,
                check=False,
                timeout=cfg.timeout_seconds or None,
            )
        except subprocess.TimeoutExpired:
            _cleanup_temp_files(tmp_path)
            msg = (
                f"S3 download timed out after "
                f"{cfg.timeout_seconds}s for {file_date}. "
                f"Partial file cleaned up."
            )
            raise RuntimeError(msg) from None

        if result.returncode == 0:
            return result

        _cleanup_temp_files(tmp_path)
        stderr = result.stderr.decode(errors="replace") if result.stderr else ""
        if _is_not_found(stderr):
            logger.info("No file on S3 for %s, skipping.", file_date)
            return None
        if _is_retryable(stderr):
            label = _retryable_label(stderr)
            wait = min(30 * (2**attempt), 300)
            logger.warning(
                "%s for %s, retrying in %ds (attempt %d/%d)",
                label,
                file_date,
                wait,
                attempt + 1,
                cfg.max_retries,
            )
            time.sleep(wait)
            continue
        msg = f"S3 download failed for {file_date}: {stderr.strip()}"
        raise RuntimeError(msg)

    msg = (
        f"S3 download failed for {file_date} "
        f"after {cfg.max_retries} retries "
        f"(rate limited or forbidden)"
    )
    raise RuntimeError(msg)


def _is_not_found(stderr: str) -> bool:
    return "404" in stderr or "NoSuchKey" in stderr or "does not exist" in stderr


def _is_retryable(stderr: str) -> bool:
    return (
        "429" in stderr
        or "Too Many Requests" in stderr
        or "403" in stderr
        or "Forbidden" in stderr
    )


def _retryable_label(stderr: str) -> str:
    if "429" in stderr or "Too Many Requests" in stderr:
        return "Rate limited (429)"
    return "Forbidden (403)"


def download_flat_file(
    file_date: str,
    dest_dir: str,
    s3_subdir: str,
    local_filename: str,
    config: DownloadConfig | None = None,
) -> Path | None:
    """Download a flat file from Massive S3.

    Returns local path or None if not on S3. Uses fcntl locking
    to prevent parallel downloads of the same file. Downloads to
    a temp file first, then renames atomically. Retries on 429
    (rate limited) and 403 (forbidden) with exponential backoff.
    """
    cfg = config or DownloadConfig()

    if not local_filename.endswith(".csv.gz"):
        msg = f"local_filename must end with '.csv.gz', got: {local_filename!r}"
        raise ValueError(msg)
    if shutil.which("aws") is None:
        msg = (
            "AWS CLI ('aws') is required for flat file downloads "
            "but was not found on PATH. "
            "Install it: https://docs.aws.amazon.com/cli/"
        )
        raise RuntimeError(msg)

    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    local_path = dest / local_filename
    if local_path.exists():
        return local_path

    if cfg.jitter_max_seconds > 0:
        jitter = random.uniform(0, cfg.jitter_max_seconds)  # noqa: S311
        logger.info("Waiting %.1fs before download (jitter)", jitter)
        time.sleep(jitter)

    stem = local_filename.removesuffix(".csv.gz")
    lock_path = dest / f".{stem}.lock"
    s3_url = _build_s3_url(s3_subdir, file_date)
    env = {
        **os.environ,
        "AWS_ACCESS_KEY_ID": cfg.aws_access_key_id,
        "AWS_SECRET_ACCESS_KEY": cfg.aws_secret_access_key,
    }

    with lock_path.open("w") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        try:
            if local_path.exists():
                return local_path
            tmp_path = dest / f".{stem}.tmp.csv.gz"
            logger.info("Downloading %s → %s", s3_url, local_path)
            result = _download_with_retries(
                s3_url=s3_url,
                tmp_path=tmp_path,
                file_date=file_date,
                env=env,
                cfg=cfg,
            )
            if result is None:
                return None
            tmp_path.rename(local_path)
        finally:
            # Unlinking while the fd is open is a known POSIX
            # fcntl race: another process could create a new
            # file at the same path and lock a different inode.
            # The idempotency re-check (local_path.exists())
            # above mitigates this — a racing process would see
            # the completed file and return immediately.
            lock_path.unlink(missing_ok=True)
    return local_path
