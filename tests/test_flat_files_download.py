"""Tests for QuoteUpdateBarFlatFilesStream download/cache/locking path."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from tap_massive.flat_files_streams import QuoteUpdateBarFlatFilesStream


def _make_stream() -> MagicMock:
    stream = MagicMock(spec=QuoteUpdateBarFlatFilesStream)
    stream.config = {
        "flat_files_aws_key": "test-key",
        "flat_files_aws_secret": "test-secret",
    }
    stream.logger = MagicMock()
    stream._S3_ENDPOINT = "https://files.massive.com"
    stream.SUBDIR = "us_options_opra/quotes_v1"
    stream._S3_BUCKET_TEMPLATE = "s3://flatfiles/{subdir}/{year}/{month}/{date}.csv.gz"
    stream._s3_url = QuoteUpdateBarFlatFilesStream._s3_url.__get__(stream)
    stream._download_file = QuoteUpdateBarFlatFilesStream._download_file.__get__(stream)
    return stream


class TestDownloadFile:
    """Test _download_file behavior."""

    def test_existing_file_skips_download(self, tmp_path):
        """If file already exists locally, return it without downloading."""
        stream = _make_stream()
        existing = tmp_path / "2026-02-18.csv.gz"
        existing.write_bytes(b"fake data")

        result = stream._download_file("2026-02-18", tmp_path)

        assert result == existing
        # subprocess should not have been called
        stream.logger.info.assert_not_called()

    def test_successful_download(self, tmp_path):
        """Successful download creates file via atomic rename."""
        stream = _make_stream()

        def fake_run(cmd, **kwargs):
            # Simulate aws s3 cp writing to the tmp path
            tmp_file = Path(cmd[4])  # the destination arg
            tmp_file.write_bytes(b"downloaded data")
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=fake_run):
            result = stream._download_file("2026-02-18", tmp_path)

        assert result is not None
        assert result.name == "2026-02-18.csv.gz"
        assert result.exists()

    def test_404_returns_none(self, tmp_path):
        """Missing file on S3 (404) returns None and logs skip."""
        stream = _make_stream()

        mock_result = MagicMock(
            returncode=1, stderr=b"An error occurred (404) NoSuchKey"
        )
        with patch("subprocess.run", return_value=mock_result):
            result = stream._download_file("2026-02-18", tmp_path)

        assert result is None
        stream.logger.info.assert_any_call(
            "No file on S3 for %s, skipping.", "2026-02-18"
        )

    def test_auth_failure_raises(self, tmp_path):
        """Auth/network errors raise RuntimeError, not silently skip."""
        stream = _make_stream()

        mock_result = MagicMock(returncode=1, stderr=b"AccessDenied: Access Denied")
        with patch("subprocess.run", return_value=mock_result):
            try:
                stream._download_file("2026-02-18", tmp_path)
                raised = False
            except RuntimeError as e:
                raised = True
                assert "AccessDenied" in str(e)

        assert raised, "Should have raised RuntimeError for auth failure"

    def test_second_process_reuses_file(self, tmp_path):
        """Second call finds file already downloaded and skips."""
        stream = _make_stream()

        # First "process" creates the file
        final = tmp_path / "2026-02-18.csv.gz"
        final.write_bytes(b"already downloaded")

        # Second "process" should find it
        result = stream._download_file("2026-02-18", tmp_path)

        assert result == final
        # No subprocess call needed


class TestIterDatesForDownload:
    """Test _iter_dates_for_download date generation."""

    def test_generates_sequential_dates(self):
        stream = MagicMock(spec=QuoteUpdateBarFlatFilesStream)
        stream.config = {
            "options_quote_update_bars_flat_files_1_minute": {
                "start_file_date": "2026-02-17",
                "end_file_date": "2026-02-19",
            }
        }
        stream.name = "options_quote_update_bars_flat_files_1_minute"
        stream.get_starting_replication_key_value = MagicMock(return_value=None)
        stream._iter_dates_for_download = (
            QuoteUpdateBarFlatFilesStream._iter_dates_for_download.__get__(stream)
        )

        dates = list(stream._iter_dates_for_download(None))

        assert [d[0] for d in dates] == [
            "2026-02-17",
            "2026-02-18",
            "2026-02-19",
        ]
        # All filepaths should be None (download needed)
        assert all(d[1] is None for d in dates)

    def test_respects_bookmark(self):
        stream = MagicMock(spec=QuoteUpdateBarFlatFilesStream)
        stream.config = {
            "options_quote_update_bars_flat_files_1_minute": {
                "start_file_date": "2026-02-17",
                "end_file_date": "2026-02-19",
            }
        }
        stream.name = "options_quote_update_bars_flat_files_1_minute"
        # Bookmark at 02-18 means skip 02-17
        stream.get_starting_replication_key_value = MagicMock(return_value="2026-02-18")
        stream._iter_dates_for_download = (
            QuoteUpdateBarFlatFilesStream._iter_dates_for_download.__get__(stream)
        )

        dates = [d[0] for d in stream._iter_dates_for_download(None)]

        assert dates == ["2026-02-18", "2026-02-19"]


class TestCheckAwsCli:
    def test_raises_when_aws_missing(self):
        with patch("shutil.which", return_value=None):
            try:
                QuoteUpdateBarFlatFilesStream._check_aws_cli()
                raised = False
            except RuntimeError as e:
                raised = True
                assert "AWS CLI" in str(e)
            assert raised

    def test_passes_when_aws_present(self):
        with patch("shutil.which", return_value="/usr/local/bin/aws"):
            QuoteUpdateBarFlatFilesStream._check_aws_cli()  # should not raise
