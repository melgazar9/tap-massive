"""fetch_massive_flat_files.sh against a stub aws: scratch work stays in MASSIVE_CACHE_DIR."""

from __future__ import annotations

import gzip
import os
import stat
import subprocess
from pathlib import Path
from typing import ClassVar


class StubbedFetch:
    """The real script with a stub aws first on PATH that writes a given payload and logs where it wrote."""

    SCRIPT: ClassVar[Path] = (
        Path(__file__).resolve().parents[1] / "fetch_massive_flat_files.sh"
    )
    DAY: ClassVar[str] = "2025-05-12"
    LANDED: ClassVar[str] = "us_stocks_sip/eod/stocks_eod_2025-05-12.csv.gz"
    GOOD_PAYLOAD: ClassVar[bytes] = gzip.compress(b"ticker,volume\nAAPL,1\n")
    STUB: ClassVar[str] = (
        "#!/usr/bin/env bash\n"
        'if [ "$1 $2" = "s3 cp" ]; then echo "$4" >> "$STUB_LOG"; cp "$STUB_PAYLOAD" "$4"; exit 0; fi\n'
        "wc -c < \"$STUB_PAYLOAD\" | tr -d ' '\n"
    )

    def __init__(self, tmp_path: Path, payload: bytes) -> None:
        self.cache = tmp_path / "cache"
        self.destination = tmp_path / "data"
        self.log = tmp_path / "aws_writes.log"
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        stub = bin_dir / "aws"
        stub.write_text(self.STUB)
        stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
        payload_file = tmp_path / "payload"
        payload_file.write_bytes(payload)
        self.env = {
            **os.environ,
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "MASSIVE_AWS_ACCESS_KEY_ID": "stub",
            "MASSIVE_API_KEY": "stub",
            "MASSIVE_CACHE_DIR": str(self.cache),
            "STUB_PAYLOAD": str(payload_file),
            "STUB_LOG": str(self.log),
        }

    def run(
        self, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        flags = [
            "--eod",
            "--asset-class",
            "us_stocks_sip",
            "--start-date",
            self.DAY,
            "--end-date",
            self.DAY,
        ]
        return subprocess.run(
            ["bash", str(self.SCRIPT), *flags, "--destination", str(self.destination)],
            env=env or self.env,
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )

    @staticmethod
    def files_under(root: Path) -> list[str]:
        if not root.exists():
            return []
        return sorted(
            path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_file()
        )


def test_a_download_is_checked_on_the_cache_disk_and_only_the_finished_file_reaches_the_destination(
    tmp_path,
):
    fetch = StubbedFetch(tmp_path, StubbedFetch.GOOD_PAYLOAD)

    result = fetch.run()

    assert result.returncode == 0, result.stdout + result.stderr
    assert StubbedFetch.files_under(fetch.destination) == [StubbedFetch.LANDED]
    written = [Path(line) for line in fetch.log.read_text().splitlines()]
    assert written and all(path.is_relative_to(fetch.cache) for path in written)
    assert StubbedFetch.files_under(fetch.cache) == []


def test_a_corrupt_download_never_reaches_the_destination(tmp_path):
    fetch = StubbedFetch(tmp_path, b"not a gzip stream")

    result = fetch.run()

    assert result.returncode != 0
    assert "gzip integrity check failed" in result.stdout
    assert StubbedFetch.files_under(fetch.destination) == []
    assert StubbedFetch.files_under(fetch.cache) == []


def test_pieces_left_by_a_killed_attempt_are_cleared(tmp_path):
    fetch = StubbedFetch(tmp_path, StubbedFetch.GOOD_PAYLOAD)
    hidden = f".stocks_eod_{StubbedFetch.DAY}.tmp.csv.gz"
    (fetch.cache / "flat_file_downloads").mkdir(parents=True)
    (fetch.cache / "flat_file_downloads" / f"{hidden}.5cA0b359").write_bytes(
        b"partial download"
    )
    (fetch.destination / "us_stocks_sip" / "eod").mkdir(parents=True)
    (fetch.destination / "us_stocks_sip" / "eod" / hidden).write_bytes(
        b"half-copied file"
    )

    result = fetch.run()

    assert result.returncode == 0, result.stdout + result.stderr
    assert StubbedFetch.files_under(fetch.destination) == [StubbedFetch.LANDED]
    assert StubbedFetch.files_under(fetch.cache) == []


def test_without_a_cache_dir_it_refuses_before_downloading(tmp_path):
    fetch = StubbedFetch(tmp_path, StubbedFetch.GOOD_PAYLOAD)
    env = {
        name: value for name, value in fetch.env.items() if name != "MASSIVE_CACHE_DIR"
    }

    result = fetch.run(env)

    assert result.returncode == 1
    assert "MASSIVE_CACHE_DIR must be set" in result.stdout
    assert not fetch.log.exists()
