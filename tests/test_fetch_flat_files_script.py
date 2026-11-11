"""The real fetch_massive_flat_files.sh against a stub `aws` serving a local S3 tree: layout, ranges, re-runs, failures."""

from __future__ import annotations

import gzip
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

import pytest


class StubAws:
    """An `aws` executable that answers `s3 ls --recursive` and `s3 cp` from FAKE_S3_ROOT, the way aws-cli v2 does.

    Everything else (s3api head-object included) exits 2, so a test sees any call the script should no longer make.
    """

    SOURCE: ClassVar[str] = r"""
import json, os, shutil, sys, time
from pathlib import Path

args = sys.argv[1:]
root = Path(os.environ["FAKE_S3_ROOT"])
with open(os.environ["FAKE_AWS_LOG"], "a") as log:
    log.write(json.dumps({
        "args": args,
        "region": os.environ.get("AWS_DEFAULT_REGION"),
        "imds_disabled": os.environ.get("AWS_EC2_METADATA_DISABLED"),
    }) + "\n")

def keys_in(env_name):
    return {k for k in os.environ.get(env_name, "").split(",") if k}

if args[:2] == ["s3", "ls"] and "--recursive" in args:
    prefix = args[2].removeprefix("s3://flatfiles/")
    if any(prefix.startswith(denied) for denied in keys_in("FAKE_S3_DENY_LIST")):
        sys.stderr.write("An error occurred (AccessDenied) when calling the ListObjectsV2 operation: Access Denied\n")
        sys.exit(1)
    bucket = root / "flatfiles"
    found = sorted(p for p in (bucket / prefix).rglob("*") if p.is_file()) if (bucket / prefix).is_dir() else []
    if not found:
        sys.exit(1)
    for path in found:
        print(f"2026-11-10 10:00:00 {path.stat().st_size:>10} {path.relative_to(bucket).as_posix()}")
    sys.exit(0)

if args[:2] == ["s3", "cp"]:
    key, destination = args[2].removeprefix("s3://flatfiles/"), Path(args[3])
    time.sleep(float(os.environ.get("FAKE_S3_CP_SECONDS", "0")))
    if key in keys_in("FAKE_S3_FORBIDDEN"):
        sys.stderr.write("fatal error: An error occurred (403) when calling the HeadObject operation: Forbidden\n")
        sys.exit(1)
    data = (root / "flatfiles" / key).read_bytes()
    if key in keys_in("FAKE_S3_TRUNCATE"):
        data = data[: len(data) // 2]
    destination.write_bytes(data)
    sys.exit(0)

sys.stderr.write(f"stub aws: unexpected call {args}\n")
sys.exit(2)
"""

    # chattr +i makes `mv` onto a locked file fail; root is needed to set it, so a stub `mv` stands in for it.
    MV_SOURCE: ClassVar[str] = r"""
import os, sys
if sys.argv[-1] in os.environ.get("FAKE_MV_LOCKED", "").split(","):
    sys.stderr.write(f"mv: cannot move '{sys.argv[-2]}' to '{sys.argv[-1]}': Operation not permitted\n")
    sys.exit(1)
os.execv("/bin/mv", ["mv", *sys.argv[1:]])
"""

    @classmethod
    def install(cls, bin_dir: Path) -> None:
        bin_dir.mkdir(parents=True)
        for name, source in (("aws", cls.SOURCE), ("mv", cls.MV_SOURCE)):
            path = bin_dir / name
            path.write_text(f"#!{sys.executable}\n{source}")
            path.chmod(0o755)


@dataclass
class FetchRun:
    returncode: int
    lines: list[str]
    aws_calls: list[dict]
    seconds: float

    def cp_calls(self) -> list[dict]:
        return [call for call in self.aws_calls if call["args"][:2] == ["s3", "cp"]]


@dataclass
class FlatFileWorld:
    """A fake Massive bucket, a destination tree and the script, wired through PATH."""

    root: Path
    env_overrides: dict[str, str] = field(default_factory=dict)

    SCRIPT: ClassVar[Path] = (
        Path(__file__).resolve().parents[1] / "fetch_massive_flat_files.sh"
    )

    @property
    def bucket(self) -> Path:
        return self.root / "s3" / "flatfiles"

    @property
    def destination(self) -> Path:
        return self.root / "massive_data"

    def put(self, key: str, rows: int = 3) -> bytes:
        """A real gzip CSV at `key`; returns its bytes."""
        body = "ticker,session_end_date,close\n" + "".join(
            f"ZNM7,{key[-17:-7]},{125 + i}\n" for i in range(rows)
        )
        data = gzip.compress(body.encode())
        path = self.bucket / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return data

    def put_raw(self, key: str, data: bytes) -> None:
        path = self.bucket / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def run(self, *flags: str) -> FetchRun:
        log = self.root / "aws_calls.jsonl"
        log.unlink(missing_ok=True)
        env = {
            "PATH": f"{self.root / 'bin'}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(self.root),
            "MASSIVE_AWS_ACCESS_KEY_ID": "test-access-key",
            "MASSIVE_API_KEY": "test-secret",
            "FAKE_S3_ROOT": str(self.root / "s3"),
            "FAKE_AWS_LOG": str(log),
            **self.env_overrides,
        }
        started = time.monotonic()
        result = subprocess.run(
            ["bash", str(self.SCRIPT), *flags, "--destination", str(self.destination)],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        seconds = time.monotonic() - started
        calls = (
            [json.loads(line) for line in log.read_text().splitlines()]
            if log.exists()
            else []
        )
        return FetchRun(
            result.returncode,
            (result.stdout + result.stderr).splitlines(),
            calls,
            seconds,
        )


@pytest.fixture
def world(tmp_path: Path) -> FlatFileWorld:
    StubAws.install(tmp_path / "bin")
    return FlatFileWorld(tmp_path)


def eod_key(exchange: str, day: str) -> str:
    return f"us_futures_{exchange}/session_aggs_v1/{day[:4]}/{day[5:7]}/{day}.csv.gz"


def eod_file(world: FlatFileWorld, exchange: str, day: str) -> Path:
    return (
        world.destination
        / "futures"
        / "eod"
        / exchange
        / f"futures_eod_{exchange}_{day}.csv.gz"
    )


class TestLayoutAndRange:
    def test_futures_files_land_in_the_archive_layout_byte_for_byte(self, world):
        sent = {
            (ex, day): world.put(eod_key(ex, day))
            for ex in ("cbot", "cme")
            for day in ("2017-04-03", "2017-04-04")
        }

        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT,CME",
            "--start-date",
            "2017-04-03",
            "--end-date",
            "2017-04-09",
        )

        assert run.returncode == 0, run.lines
        for (exchange, day), data in sent.items():
            assert eod_file(world, exchange, day).read_bytes() == data
        assert not list(world.destination.rglob(".*.tmp.csv.gz"))

    def test_every_aws_call_skips_the_metadata_service_and_no_head_object_is_made(
        self, world
    ):
        world.put(eod_key("cbot", "2017-04-03"))

        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT",
            "--start-date",
            "2017-04-03",
            "--end-date",
            "2017-04-03",
        )

        assert run.returncode == 0, run.lines
        assert run.aws_calls, "the script made no aws call at all"
        assert {(c["region"], c["imds_disabled"]) for c in run.aws_calls} == {
            ("us-east-1", "true")
        }
        assert all(c["args"][:2] in (["s3", "ls"], ["s3", "cp"]) for c in run.aws_calls)

    def test_only_listed_days_inside_the_range_are_fetched_across_a_year_boundary(
        self, world
    ):
        for day in ("2017-12-28", "2017-12-29", "2018-01-02", "2018-01-03"):
            world.put(eod_key("cbot", day))

        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT",
            "--start-date",
            "2017-12-29",
            "--end-date",
            "2018-01-02",
        )

        assert run.returncode == 0, run.lines
        fetched = sorted(
            p.name for p in (world.destination / "futures" / "eod" / "cbot").iterdir()
        )
        assert fetched == [
            "futures_eod_cbot_2017-12-29.csv.gz",
            "futures_eod_cbot_2018-01-02.csv.gz",
        ]
        assert len(run.cp_calls()) == 2

    def test_non_futures_asset_class_keeps_its_flat_layout(self, world):
        world.put("us_stocks_sip/day_aggs_v1/2025/05/2025-05-12.csv.gz")

        run = world.run(
            "--eod",
            "--asset-class",
            "us_stocks_sip",
            "--start-date",
            "2025-05-12",
            "--end-date",
            "2025-05-13",
        )

        assert run.returncode == 0, run.lines
        assert (
            world.destination / "us_stocks_sip" / "eod" / "stocks_eod_2025-05-12.csv.gz"
        ).is_file()

    def test_all_with_exclude_fetches_only_the_remaining_futures_datasets(self, world):
        day = "2017-04-03"
        for data_type in (
            "trades_v1",
            "quotes_v1",
            "minute_aggs_v1",
            "session_aggs_v1",
        ):
            world.put(f"us_futures_cbot/{data_type}/2017/04/{day}.csv.gz")

        run = world.run(
            "--all",
            "--exclude",
            "quotes,trades",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT",
            "--start-date",
            day,
            "--end-date",
            day,
        )

        assert run.returncode == 0, run.lines
        assert sorted(
            p.parent.parent.name
            for p in (world.destination / "futures").rglob("*.csv.gz")
        ) == [
            "bars_1m",
            "eod",
        ]

    def test_an_empty_listing_is_a_warning_not_silence(self, world):
        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT",
            "--start-date",
            "2017-04-03",
            "--end-date",
            "2017-04-04",
        )

        assert run.returncode == 0, run.lines
        assert any(
            line.startswith("Warning: Massive lists no files under")
            for line in run.lines
        )


class TestReRuns:
    def test_a_rerun_downloads_nothing_already_on_disk(self, world):
        for day in ("2017-04-03", "2017-04-04"):
            world.put(eod_key("cbot", day))
        flags = (
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT",
            "--start-date",
            "2017-04-03",
            "--end-date",
            "2017-04-04",
        )
        assert world.run(*flags).returncode == 0

        rerun = world.run(*flags)

        assert rerun.returncode == 0, rerun.lines
        assert rerun.cp_calls() == []
        assert any(
            "Queued 0 file(s)" in line and "2 already on disk" in line
            for line in rerun.lines
        )

    def test_a_file_on_disk_at_a_different_size_is_fetched_again(self, world):
        day = "2017-04-03"
        data = world.put(eod_key("cbot", day))
        stale = eod_file(world, "cbot", day)
        stale.parent.mkdir(parents=True)
        stale.write_bytes(b"partial")

        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT",
            "--start-date",
            day,
            "--end-date",
            day,
        )

        assert run.returncode == 0, run.lines
        assert stale.read_bytes() == data

    def test_a_locked_file_that_differs_is_an_error_and_leaves_no_temp_file(
        self, world
    ):
        day = "2017-04-03"
        world.put(eod_key("cbot", day))
        locked = eod_file(world, "cbot", day)
        locked.parent.mkdir(parents=True)
        locked.write_bytes(b"older vendor version")
        world.env_overrides["FAKE_MV_LOCKED"] = str(locked)

        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT",
            "--start-date",
            day,
            "--end-date",
            day,
        )

        assert run.returncode == 1
        assert any(
            line.startswith("Error: could not rename the download into place")
            for line in run.lines
        )
        assert locked.read_bytes() == b"older vendor version"
        assert not list(locked.parent.glob(".*"))


class TestFailures:
    def test_a_truncated_download_fails_the_size_check_and_never_takes_the_final_name(
        self, world
    ):
        day = "2017-04-03"
        world.put(eod_key("cbot", day), rows=2000)
        world.env_overrides["FAKE_S3_TRUNCATE"] = eod_key("cbot", day)

        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT",
            "--start-date",
            day,
            "--end-date",
            day,
        )

        assert run.returncode == 1
        assert any(
            line.startswith("Error: size mismatch for s3://flatfiles/")
            for line in run.lines
        )
        assert not eod_file(world, "cbot", day).exists()
        assert not list(eod_file(world, "cbot", day).parent.glob(".*"))

    def test_a_corrupt_gzip_at_the_listed_size_is_rejected(self, world):
        day = "2017-04-03"
        world.put_raw(eod_key("cbot", day), b"this is not gzip data at all")

        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT",
            "--start-date",
            day,
            "--end-date",
            day,
        )

        assert run.returncode == 1
        assert any(
            line.startswith("Error: gzip integrity check failed") for line in run.lines
        )
        assert not eod_file(world, "cbot", day).exists()

    def test_a_denied_listing_fails_the_run_but_not_the_other_exchange(self, world):
        day = "2017-04-03"
        world.put(eod_key("cbot", day))
        world.put(eod_key("cme", day))
        world.env_overrides["FAKE_S3_DENY_LIST"] = "us_futures_cme/"

        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT,CME",
            "--start-date",
            day,
            "--end-date",
            day,
        )

        assert run.returncode == 1
        assert any(
            line.startswith(
                "Error: aws s3 ls failed for s3://flatfiles/us_futures_cme/"
            )
            for line in run.lines
        )
        assert any("AccessDenied" in line for line in run.lines)
        assert eod_file(world, "cbot", day).is_file()
        assert not eod_file(world, "cme", day).exists()

    def test_every_forbidden_error_line_is_followed_by_its_403_under_parallel_workers(
        self, world
    ):
        days = [f"2017-04-{d:02d}" for d in range(1, 25)]
        for day in days:
            world.put(eod_key("cbot", day))
        forbidden = {eod_key("cbot", day) for day in days[::4]}
        world.env_overrides |= {
            "FAKE_S3_FORBIDDEN": ",".join(sorted(forbidden)),
            "FAKE_S3_CP_SECONDS": "0.05",
        }

        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT",
            "--start-date",
            days[0],
            "--end-date",
            days[-1],
            "--parallel",
            "8",
        )

        assert run.returncode == 1
        error_rows = [
            i
            for i, line in enumerate(run.lines)
            if line.startswith("Error: aws s3 cp failed for ")
        ]
        assert len(error_rows) == len(forbidden)
        assert all("(403)" in run.lines[i + 1] for i in error_rows)
        assert (
            f"Done with {len(forbidden)} download error(s). Check messages above."
            in run.lines
        )
        assert len(
            list((world.destination / "futures" / "eod" / "cbot").glob("*.csv.gz"))
        ) == len(days) - len(forbidden)


class TestParallelism:
    def test_downloads_overlap(self, world):
        days = [f"2017-04-{d:02d}" for d in range(3, 11)]
        for day in days:
            world.put(eod_key("cbot", day))
        world.env_overrides["FAKE_S3_CP_SECONDS"] = "1"

        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--exchange",
            "CBOT",
            "--start-date",
            days[0],
            "--end-date",
            days[-1],
            "--parallel",
            "8",
        )

        assert run.returncode == 0, run.lines
        assert len(run.cp_calls()) == len(days)
        # Serial would take one second per file; overlapping workers finish all of them in about one.
        assert run.seconds < len(days) / 2

    @pytest.mark.parametrize("value", ["0", "-2", "eight", ""])
    def test_parallel_must_be_a_positive_whole_number(self, world, value):
        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--start-date",
            "2017-04-03",
            "--parallel",
            value,
        )

        assert run.returncode == 1
        assert any(line.startswith("Error: --parallel") for line in run.lines)

    @pytest.mark.parametrize("value", ["2017/04/03", "2017-4-3", "2017-02-30"])
    def test_dates_must_be_real_yyyy_mm_dd_days(self, world, value):
        run = world.run(
            "--eod",
            "--asset-class",
            "futures",
            "--start-date",
            value,
            "--end-date",
            "2017-04-05",
        )

        assert run.returncode == 1
        assert any(line.startswith("Error: --start-date") for line in run.lines)
