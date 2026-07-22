"""Tests for s3_download retry classification and awscli retry env."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tap_massive.s3_download import (
    DownloadConfig,
    _download_with_retries,
    _is_retryable,
    _retryable_label,
    download_flat_file,
)


class TestRetryClassification:
    def test_429_is_retryable(self):
        assert _is_retryable("An error occurred (429): Too Many Requests")

    def test_403_is_retryable(self):
        assert _is_retryable("An error occurred (403): Forbidden")

    def test_503_is_retryable(self):
        stderr = (
            "An error occurred (503) when calling the GetObject operation "
            "(reached max retries: 2): Service Unavailable"
        )
        assert _is_retryable(stderr)

    def test_connection_closed_is_retryable(self):
        stderr = (
            "Connection was closed before we received a valid response "
            'from endpoint URL: "https://files.massive.com/flatfiles/x.csv.gz".'
        )
        assert _is_retryable(stderr)

    def test_unrelated_error_not_retryable(self):
        assert not _is_retryable("An error occurred (400): Bad Request")

    def test_labels(self):
        assert _retryable_label("(429) Too Many Requests") == "Rate limited (429)"
        assert _retryable_label("(403) Forbidden") == "Forbidden (403)"
        assert (
            _retryable_label("(503) Service Unavailable") == "Service unavailable (503)"
        )
        assert _retryable_label("Connection was closed before") == "Connection closed"


class TestDownloadWithRetries:
    def test_503_then_success_retries(self, tmp_path):
        responses = [
            MagicMock(
                returncode=1,
                stderr=b"An error occurred (503) when calling the GetObject operation: Service Unavailable",
            ),
            MagicMock(returncode=0, stderr=b""),
        ]
        with (
            patch("tap_massive.s3_download.subprocess.run", side_effect=responses),
            patch("tap_massive.s3_download.time.sleep") as sleep_mock,
        ):
            result = _download_with_retries(
                s3_url="s3://flatfiles/us_options_opra/quotes_v1/2026/06/2026-06-04.csv.gz",
                tmp_path=tmp_path / ".x.tmp.csv.gz",
                file_date="2026-06-04",
                env={},
                cfg=DownloadConfig(),
            )
        assert result is not None
        assert result.returncode == 0
        sleep_mock.assert_called_once()

    def test_non_retryable_raises_immediately(self, tmp_path):
        responses = [
            MagicMock(returncode=1, stderr=b"An error occurred (400): Bad Request")
        ]
        with (
            patch("tap_massive.s3_download.subprocess.run", side_effect=responses),
            patch("tap_massive.s3_download.time.sleep") as sleep_mock,
        ):
            with pytest.raises(RuntimeError, match="S3 download failed"):
                _download_with_retries(
                    s3_url="s3://flatfiles/us_options_opra/quotes_v1/2026/06/2026-06-04.csv.gz",
                    tmp_path=tmp_path / ".x.tmp.csv.gz",
                    file_date="2026-06-04",
                    env={},
                    cfg=DownloadConfig(),
                )
        sleep_mock.assert_not_called()


class TestAwscliRetryEnv:
    def test_env_enables_adaptive_retry(self, tmp_path):
        captured: dict[str, str] = {}

        def fake_run(cmd, **kwargs):
            captured.update(kwargs["env"])
            Path(cmd[4]).write_bytes(b"data")
            return MagicMock(returncode=0, stderr=b"")

        with patch("tap_massive.s3_download.subprocess.run", side_effect=fake_run):
            result = download_flat_file(
                "2026-06-04",
                str(tmp_path),
                "us_options_opra/quotes_v1",
                "options_quotes_2026-06-04.csv.gz",
                DownloadConfig(jitter_max_seconds=0),
            )

        assert result is not None
        assert captured["AWS_RETRY_MODE"] == "adaptive"
        assert captured["AWS_MAX_ATTEMPTS"] == "3"
        assert captured["AWS_RESPONSE_CHECKSUM_VALIDATION"] == "when_required"
