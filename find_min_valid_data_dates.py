#!/usr/bin/env python3
"""Find minimum date that returns non-empty data per stream.

Usage:
  MASSIVE_API_KEY=... uv run python find_min_valid_data_dates.py
  MASSIVE_API_KEY=... uv run python find_min_valid_data_dates.py --asset-class indices
  MASSIVE_API_KEY=... uv run python find_min_valid_data_dates.py --asset-class options --asset-class indices
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

API_BASE = "https://api.massive.com"
API_KEY = os.getenv("MASSIVE_API_KEY")

if not API_KEY:
    raise SystemExit("Missing MASSIVE_API_KEY")

ROOT = Path(__file__).resolve().parent
STREAMS_DIR = ROOT / "tap_massive"
ASSET_TO_FILE_PREFIX = {
    "options": "option",
    "indices": "indices",
    "futures": "futures",
    "stocks": "stock",
    "fundamentals": "fundamental",
    "forex": "forex",
    "crypto": "crypto",
    "economy": "economy",
}
ASSET_ALIASES = {
    "option": "options",
    "options": "options",
    "index": "indices",
    "indices": "indices",
    "future": "futures",
    "futures": "futures",
    "stock": "stocks",
    "stocks": "stocks",
    "fundamental": "fundamentals",
    "fundamentals": "fundamentals",
    "forex": "forex",
    "crypto": "crypto",
    "economy": "economy",
}


def iso_to_ns(d: date) -> int:
    dt = datetime(d.year, d.month, d.day)
    return int(dt.timestamp() * 1_000_000_000)


def get_json(path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    q = dict(params)
    q["apiKey"] = API_KEY
    url = f"{API_BASE}{path}?{urllib.parse.urlencode(q)}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.getcode(), json.loads(body or "{}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(body or "{}")
        except json.JSONDecodeError:
            return exc.code, {"status": "HTTP_ERROR", "message": body[:500]}


def has_non_empty_data(payload: dict[str, Any]) -> bool:
    if isinstance(payload.get("results"), list):
        return len(payload["results"]) > 0
    if isinstance(payload.get("results"), dict):
        res = payload["results"]
        if isinstance(res.get("values"), list):
            return len(res["values"]) > 0
        if isinstance(res.get("results"), list):
            return len(res["results"]) > 0
    if isinstance(payload.get("values"), list):
        return len(payload["values"]) > 0
    if isinstance(payload.get("queryCount"), int):
        return payload["queryCount"] > 0
    return False


class OptionContractResolver:
    """Resolve date-appropriate option contract candidates for probing."""

    def __init__(self) -> None:
        self._cache: dict[str, list[str]] = {}

    def candidates_for_date(self, d: date) -> list[str]:
        # Month bucket keeps request count reasonable during binary search.
        key = f"{d.year:04d}-{d.month:02d}"
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        underlyings = ("SPY", "QQQ", "AAPL")
        out: list[str] = []
        for underlying in underlyings:
            code, payload = get_json(
                "/v3/reference/options/contracts",
                {
                    "underlying_ticker": underlying,
                    "as_of": d.isoformat(),
                    "expired": "true",
                    "sort": "expiration_date",
                    "order": "desc",
                    "limit": 25,
                },
            )
            if code == 200 and isinstance(payload.get("results"), list):
                for r in payload["results"]:
                    t = r.get("ticker")
                    if isinstance(t, str) and t not in out:
                        out.append(t)
            if len(out) >= 25:
                break

        self._cache[key] = out
        return out


def date_midpoint(lo: date, hi: date) -> date:
    return lo + timedelta(days=(hi - lo).days // 2)


@dataclass
class Probe:
    stream: str
    category: str
    checker: Callable[[date], bool]


def build_option_probes() -> list[Probe]:
    resolver = OptionContractResolver()

    def any_option_contract_has_data(
        d: date,
        checker: Callable[[str, date], bool],
        max_contracts: int = 8,
    ) -> bool:
        contracts = resolver.candidates_for_date(d)
        for contract in contracts[:max_contracts]:
            if checker(contract, d):
                return True
        return False

    def options_bars(multiplier: int, timespan: str) -> Callable[[date], bool]:
        def _check(d: date) -> bool:
            def _single(contract: str, dt: date) -> bool:
                code, payload = get_json(
                    f"/v2/aggs/ticker/{contract}/range/{multiplier}/{timespan}/{dt.isoformat()}/{dt.isoformat()}",
                    {"adjusted": "true", "sort": "asc", "limit": 120},
                )
                return code == 200 and has_non_empty_data(payload)

            return any_option_contract_has_data(d, _single)

        return _check

    def options_trades(d: date) -> bool:
        def _single(contract: str, dt: date) -> bool:
            code, payload = get_json(
                f"/v3/trades/{contract}",
                {
                    "timestamp.gte": iso_to_ns(dt),
                    "timestamp.lt": iso_to_ns(dt + timedelta(days=1)),
                    "sort": "timestamp",
                    "order": "asc",
                    "limit": 1,
                },
            )
            return code == 200 and has_non_empty_data(payload)

        return any_option_contract_has_data(d, _single)

    def options_quotes(d: date) -> bool:
        def _single(contract: str, dt: date) -> bool:
            code, payload = get_json(
                f"/v3/quotes/{contract}",
                {
                    "timestamp.gte": iso_to_ns(dt),
                    "timestamp.lt": iso_to_ns(dt + timedelta(days=1)),
                    "sort": "timestamp",
                    "order": "asc",
                    "limit": 1,
                },
            )
            return code == 200 and has_non_empty_data(payload)

        return any_option_contract_has_data(d, _single)

    def options_indicator(kind: str) -> Callable[[date], bool]:
        def _check(d: date) -> bool:
            def _single(contract: str, dt: date) -> bool:
                code, payload = get_json(
                    f"/v1/indicators/{kind}/{contract}",
                    {
                        "timespan": "day",
                        "window": 14,
                        "series_type": "close",
                        "order": "desc",
                        "limit": 10,
                        "timestamp.gte": int(
                            datetime(dt.year, dt.month, dt.day).timestamp() * 1000
                        ),
                        "timestamp.lt": int(
                            datetime(
                                (dt + timedelta(days=1)).year,
                                (dt + timedelta(days=1)).month,
                                (dt + timedelta(days=1)).day,
                            ).timestamp()
                            * 1000
                        ),
                    },
                )
                return code == 200 and has_non_empty_data(payload)

            return any_option_contract_has_data(d, _single)

        return _check

    def options_contracts(d: date) -> bool:
        code, payload = get_json(
            "/v3/reference/options/contracts",
            {
                "underlying_ticker": "SPY",
                "expired": "true",
                "expiration_date.gte": d.isoformat(),
                "sort": "ticker",
                "limit": 1,
            },
        )
        return code == 200 and has_non_empty_data(payload)

    def options_daily_summary(d: date) -> bool:
        def _single(contract: str, dt: date) -> bool:
            code, payload = get_json(
                f"/v1/open-close/{contract}/{dt.isoformat()}", {"adjusted": "true"}
            )
            return code == 200 and has_non_empty_data(payload)

        return any_option_contract_has_data(d, _single)

    return [
        Probe("options_contracts", "options", options_contracts),
        Probe("options_bars_1_day", "options", options_bars(1, "day")),
        Probe("options_bars_1_week", "options", options_bars(1, "week")),
        Probe("options_bars_1_month", "options", options_bars(1, "month")),
        Probe("options_daily_ticker_summary", "options", options_daily_summary),
        Probe("options_trades", "options", options_trades),
        Probe("options_quotes", "options", options_quotes),
        Probe("options_sma", "options", options_indicator("sma")),
        Probe("options_ema", "options", options_indicator("ema")),
        Probe("options_macd", "options", options_indicator("macd")),
        Probe("options_rsi", "options", options_indicator("rsi")),
    ]


def build_indices_probes() -> list[Probe]:
    def indices_bars(multiplier: int, timespan: str) -> Callable[[date], bool]:
        def _check(d: date) -> bool:
            code, payload = get_json(
                f"/v2/aggs/ticker/I:NDX/range/{multiplier}/{timespan}/{d.isoformat()}/{d.isoformat()}",
                {"sort": "asc", "limit": 120},
            )
            return code == 200 and has_non_empty_data(payload)

        return _check

    def indices_daily_summary(d: date) -> bool:
        code, payload = get_json(f"/v1/open-close/I:NDX/{d.isoformat()}", {})
        return code == 200 and has_non_empty_data(payload)

    def indices_indicator(kind: str) -> Callable[[date], bool]:
        def _check(d: date) -> bool:
            code, payload = get_json(
                f"/v1/indicators/{kind}/I:NDX",
                {
                    "timespan": "day",
                    "window": 14,
                    "series_type": "close",
                    "order": "desc",
                    "limit": 10,
                    "timestamp.gte": int(
                        datetime(d.year, d.month, d.day).timestamp() * 1000
                    ),
                    "timestamp.lt": int(
                        datetime(
                            (d + timedelta(days=1)).year,
                            (d + timedelta(days=1)).month,
                            (d + timedelta(days=1)).day,
                        ).timestamp()
                        * 1000
                    ),
                },
            )
            return code == 200 and has_non_empty_data(payload)

        return _check

    def indices_ticker_details(d: date) -> bool:
        code, payload = get_json("/v3/reference/tickers/I:NDX", {"date": d.isoformat()})
        return code == 200 and has_non_empty_data(payload)

    def indices_tickers(d: date) -> bool:
        code, payload = get_json(
            "/v3/reference/tickers",
            {"market": "indices", "date": d.isoformat(), "limit": 1},
        )
        return code == 200 and has_non_empty_data(payload)

    return [
        Probe("indices_tickers", "indices", indices_tickers),
        Probe("indices_ticker_details", "indices", indices_ticker_details),
        Probe("indices_bars_1_day", "indices", indices_bars(1, "day")),
        Probe("indices_bars_1_week", "indices", indices_bars(1, "week")),
        Probe("indices_bars_1_month", "indices", indices_bars(1, "month")),
        Probe("indices_daily_ticker_summary", "indices", indices_daily_summary),
        Probe("indices_sma", "indices", indices_indicator("sma")),
        Probe("indices_ema", "indices", indices_indicator("ema")),
        Probe("indices_macd", "indices", indices_indicator("macd")),
        Probe("indices_rsi", "indices", indices_indicator("rsi")),
    ]


def earliest_valid_date(
    checker: Callable[[date], bool],
    start: date,
    end: date,
    end_anchor_lookback_days: int = 14,
) -> tuple[str, str]:
    # Direct hit at lower bound.
    if checker(start):
        return start.isoformat(), "n/a"

    # Anchor on end date; if end date itself is empty (weekend/holiday), look back a bit.
    hi = end
    hi_valid = checker(hi)
    if not hi_valid:
        for i in range(1, end_anchor_lookback_days + 1):
            cand = end - timedelta(days=i)
            if cand < start:
                break
            if checker(cand):
                hi = cand
                hi_valid = True
                break

    if not hi_valid:
        return "none_found", end.isoformat()

    lo = start
    # Binary search to isolate invalid/valid boundary.
    while (hi - lo).days > 1:
        mid = date_midpoint(lo, hi)
        if checker(mid):
            hi = mid
        else:
            lo = mid
        time.sleep(0.05)

    # Forward scan from day after last invalid date to first actual valid date.
    d = lo + timedelta(days=1)
    while d <= end:
        if checker(d):
            return d.isoformat(), lo.isoformat()
        d += timedelta(days=1)
    return "none_found", lo.isoformat()


def normalize_assets(raw_assets: list[str]) -> list[str]:
    out: list[str] = []
    for raw in raw_assets:
        parts = [x.strip().lower() for x in raw.split(",") if x.strip()]
        for p in parts:
            if p == "all":
                out.extend(["options", "indices"])
                continue
            mapped = ASSET_ALIASES.get(p)
            if not mapped:
                raise SystemExit(
                    f"Unsupported asset class '{p}'. "
                    "Use one of: options, indices, futures, stocks, fundamentals, forex, crypto, economy, all."
                )
            out.append(mapped)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in out:
        if item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped


def stream_names_for_asset(asset: str) -> set[str]:
    prefix = ASSET_TO_FILE_PREFIX[asset]
    f = STREAMS_DIR / f"{prefix}_streams.py"
    if not f.exists():
        return set()
    txt = f.read_text(encoding="utf-8", errors="replace")
    return set(re.findall(r'name\s*=\s*"([^"]+)"', txt))


def probes_for_assets(selected_assets: list[str]) -> list[Probe]:
    probes: list[Probe] = []
    if "options" in selected_assets:
        print("Using options contract resolver: as_of date-based (SPY/QQQ/AAPL)")
    if "indices" in selected_assets:
        print("Using indices ticker probe: I:NDX")
    if "options" in selected_assets or "indices" in selected_assets:
        print()

    for asset in selected_assets:
        if asset == "options":
            probes.extend(build_option_probes())
        elif asset == "indices":
            probes.extend(build_indices_probes())
        else:
            expected = stream_names_for_asset(asset)
            print(
                f"Skipping unsupported asset class '{asset}' for now "
                f"(no probe builders yet). Streams in file: {len(expected)}."
            )
    return probes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--asset-class",
        dest="asset_classes",
        action="append",
        default=[],
        help=(
            "Asset class to test (repeatable or comma-separated): "
            "options, indices, futures, stocks, fundamentals, forex, crypto, economy, all. "
            "Default: all (currently expands to options, indices)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    selected_assets = normalize_assets(args.asset_classes or ["all"])

    start = date(1900, 1, 1)
    end = date.today()
    probes = probes_for_assets(selected_assets)
    if not probes:
        raise SystemExit("No probes to run for the selected asset classes.")

    rows: list[tuple[str, str, str, str]] = []
    for p in probes:
        print(f"Checking {p.stream} ...")
        min_valid, last_invalid = earliest_valid_date(p.checker, start, end)
        rows.append((p.stream, p.category, min_valid, last_invalid))

    print()
    print("| stream | category | min_valid_data_date | last_invalid_date |")
    print("|---|---|---|---|")
    for stream, category, min_valid, last_invalid in rows:
        print(f"| {stream} | {category} | {min_valid} | {last_invalid} |")

    print()
    print("Coverage by asset file:")
    for asset in selected_assets:
        file_streams = stream_names_for_asset(asset)
        if not file_streams:
            print(f"- {asset}: no stream file found.")
            continue
        tested_streams = {r[0] for r in rows if r[1] == asset}
        missing = sorted(file_streams - tested_streams)
        print(
            f"- {asset}: tested {len(tested_streams)}/{len(file_streams)} streams from tap_massive/{ASSET_TO_FILE_PREFIX[asset]}_streams.py"
        )
        if missing:
            print(f"  missing probes ({len(missing)}): {', '.join(missing)}")


if __name__ == "__main__":
    main()
