# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`tap-massive` is a Singer tap for the Massive financial data API, built with the [Meltano Singer SDK](https://sdk.meltano.com). It extracts market data across six asset classes: stocks, options, forex, crypto, indices, and futures, plus economy data (treasury yields, inflation, labor market).

## Development Commands

```bash
# Install dependencies
uv sync

# Lint
./lint.sh

# Run unit tests (mocked, fast)
uv run pytest tests/test_options_partitioning.py -v

# Run the tap directly
uv run tap-massive --help
uv run tap-massive --about
```

**Critical Universal Rules**

Epistemic Honesty : You must never guess. If you do not have information in your context, you must explicitly say "I do not know" or use a tool to fetch it. If you still do not know after using the tool to fetch the information, it's ok to say "I do not know and could not find anything after attempting to fetch."

CODE MUST REMAIN DRY AND FOLLOW SOLID PRINCIPLES WHEN MAKES SENSE. Do not repeat yourself. Keep code clean and follow best practices, especially around Singer SDK, OOP, and DRY and SOLID principles.

No Silent Failures : If a command fails, you must report the failure immediately. You are forbidden from pretending it worked or suppressing the error message.

Evidence-Based Coding : You must verify the existence of a file before attempting to edit it. You must run ls or cat to confirm paths.

Audit Trail : You must explain your reasoning before executing any write command.

Respecting the codebase: Do not use tools to run the code that you think are best without full context (i.e. context within the codebase). In this codebase we use UV to manage packages and run python code, so do not run things natively with pip, python, poetry, etc... or any others that developers might use. You must respect the package manager. Additionally, in this root directory we use docker to orchestrate the codebase.


**Operational Etiquette**

FAILURE IS ACCEPTABLE: It is perfectly acceptable to fail at a task if the request is impossible or the code is broken or if you do not have extremely high confidence in your proposed solution. Do not attempt to force a success state by modifying tests or deleting checks. That means no cherry picking cases that pass unit tests, writing the tests as you're writing the code so it won't appear broken, or tests that are too simplistic and do not represent the real world. Tests should represent real world problems, issues, and edge cases that are often missed when writing code. If the tests do not represent the real-world, it is not a valid test.

REPORT CHEATING OPPORTUNITIES: If you identify a way to satisfy the user's request technically but deceptively (e.g., by hardcoding a test response), you must flag this as a potential misalignment and ask for clarification.

## Testing Streams

**Always test streams this way** — not with `uv run pytest` against real endpoints.

1. Run a stream via Meltano for ~30 seconds, then kill it:
   ```bash
   # Single stream
   meltano el tap-massive target-jsonl --force --select "<stream_name>.*" 2>/tmp/tap-massive-logs/<stream_name>.log &
   PID=$!; sleep 30; kill $PID 2>/dev/null

   # Multiple streams in parallel — use separate terminal sessions (separate Bash tool calls).
   # Each stream needs its own meltano invocation with --force since meltano locks the pipeline.
   ```
2. Check the JSONL output in the `output/` directory — verify records look correct and fields match the schema.
   ```bash
   wc -l output/<stream_name>.jsonl                    # record count
   head -1 output/<stream_name>.jsonl | python3 -m json.tool  # inspect first record
   ```
3. Check logs for errors or fields the API returns that aren't in our schema:
   ```bash
   grep -i "error\|traceback\|exception" /tmp/tap-massive-logs/<stream_name>.log
   ```
   The logs are extremely detailed — compare the expected schema from the codebase against the exact output in `output/`.

This is the standard integration test workflow. No need for additional pytest tests against real API endpoints. When testing a group of related streams (e.g., all options streams), launch them all in parallel using separate Bash tool calls.

## Architecture

### Class Hierarchy

The core inheritance chain is:

1. **`MassiveRestStream`** (`client.py`) - Base class extending Singer SDK's `RESTStream`. Handles all pagination (`paginate_records`), API key auth, timestamp/state management, backoff/retry logic, and camelCase-to-snake_case key conversion. Every stream ultimately inherits from this.

2. **`OptionalTickerPartitionStream`** (`client.py`) - Extends `MassiveRestStream` for streams that optionally loop over cached tickers (e.g., IPOs, splits, dividends, news). Uses `use_cached_tickers` config to decide whether to iterate per-ticker or fetch all at once.

3. **Base stream classes** (`base_streams.py`) - Reusable base classes for common API patterns:
   - `BaseTickerStream` - Fetches ticker lists, supports `select_tickers` filtering, provides `get_child_context` for partitioning
   - `BaseTickerPartitionStream` - Abstract base requiring subclasses to define `partitions` property
   - `BaseCustomBarsStream` - OHLCV bar data at various timespans (maps single-letter API keys like `t`, `o`, `h`, `l`, `c`, `v` to full names)
   - `BaseIndicatorStream` - Technical indicators (SMA, EMA, MACD, RSI) with underlying aggregate data joining
   - `BaseTradeStream`, `BaseQuoteStream`, `BaseLastTradeStream`, `BaseLastQuoteStream` - Tick-level data
   - `BaseDailyMarketSummaryStream`, `BaseDailyTickerSummaryStream`, `BasePreviousDayBarSummaryStream`, `BaseTopMarketMoversStream`
   - **Cross-asset base classes** (see below): `BaseTickerTypesStream`, `BaseExchangesStream`, `BaseConditionCodesStream`

4. **Asset-specific stream modules** (`stock_streams.py`, `option_streams.py`, `forex_streams.py`, `crypto_streams.py`, `indices_streams.py`, `futures_streams.py`, `economy_streams.py`) - Concrete stream classes that inherit from base classes, typically only setting `name` and `market` attributes. Some override `get_url` or `partitions`.

### Cross-Asset Base Class Pattern

Three Massive API endpoints support `asset_class` query parameter filtering across asset classes:

| Base Class | Endpoint | Asset Classes |
|---|---|---|
| `BaseTickerTypesStream` | `/v3/reference/tickers/types` | stocks |
| `BaseExchangesStream` | `/v3/reference/exchanges` | stocks, options, crypto, fx, futures |
| `BaseConditionCodesStream` | `/v3/reference/conditions` | stocks, options, crypto, fx |

Each base class in `base_streams.py` holds the full schema, endpoint URL, and logic. Subclasses are thin config classes:

```python
# base_streams.py - base with all logic
class BaseTickerTypesStream(MassiveRestStream):
    ASSET_CLASS = None  # Override in subclasses
    # ... schema, get_url, __init__ sets query_params["asset_class"]

class AllTickerTypesStream(BaseTickerTypesStream):
    name = "ticker_types"  # No filter = all asset classes

# stock_streams.py - thin subclass
class StockTickerTypesStream(BaseTickerTypesStream):
    name = "stock_ticker_types"
    ASSET_CLASS = "stocks"
```

When adding a new asset class subclass (e.g., `OptionsTickerTypesStream`), only set `name` and `ASSET_CLASS`.

### Stock Financial Statement Streams

The Massive API has two generations of financial endpoints:

| Generation | Endpoint | Schema | Base Class |
|---|---|---|---|
| New (v1) | `/stocks/financials/v1/balance-sheets`, `.../cash-flow-statements`, `.../income-statements` | **Flat** fields (e.g., `total_assets`, `revenue`) | `StockFinancialStatementStream` |
| New (v1) | `/stocks/financials/v1/ratios` | **Flat**, different PKs (`ticker`, `date`) | `OptionalTickerPartitionStream` directly |
| Old (vX) | `/vX/reference/financials` | **Nested** (`financials.balance_sheet.assets.value`) | `MassiveRestStream` |

The new v1 endpoints return flat responses. `StockRatiosStream` has different primary keys (`ticker`, `date`) than the other financial streams (`cik`, `period_end`, `timeframe`), so it inherits from `OptionalTickerPartitionStream` directly instead of `StockFinancialStatementStream`.

### Ticker Caching

`TapMassive` in `tap.py` maintains thread-safe cached ticker lists per asset class (stock, option, forex, crypto, indices, futures). Streams with `use_cached_tickers=True` iterate over these cached tickers to make per-ticker API calls. The caching uses double-checked locking with `threading.Lock`.

### Configuration Flow

Stream configuration comes from `meltano.yml` under each stream's name key, structured as:
- `path_params` - URL path parameters (e.g., `multiplier`, `timespan`, `from`, `to`)
- `query_params` - URL query parameters (e.g., `limit`, `sort`, timestamp filters)
- `other_params` - Tap-specific behavior flags (e.g., `use_cached_tickers`, `loop_over_dates_gte_date`)

Timestamp filter params use `__` as separator in config (e.g., `timestamp__gte`) which gets normalized to `.` (e.g., `timestamp.gte`) for the API.

### Options Contract Discovery

Options bars/snapshot/trades streams (`OptionsTickerPartitionStream` subclasses in `option_streams.py`) depend on `TapMassive.get_option_contracts_for_underlying()` (`tap.py`) to discover which contracts to fetch data for. This method reads `option_tickers.query_params` for API params and `option_tickers.other_params` for tap-level behavior.

**`expired` parameter semantics**: The Massive API's `/v3/reference/options/contracts` endpoint treats `expired` as a **disjoint selector** — `expired=false` returns only active (non-expired) contracts, `expired=true` returns only expired contracts. There is no single API call that returns both.

**`other_params.expired: "both"`**: When set in `option_tickers.other_params`, the tap makes two API calls (expired=true + expired=false) and unions the results, deduplicating by ticker. This is required for full historical backfill since expired contracts won't appear with `expired=false`.

```yaml
option_tickers:
  query_params:
    sort: ticker          # Do NOT set expired here when using "both"
  other_params:
    expired: "both"       # Fetch expired + active, union by ticker
  select_tickers:
    - "AAPL"
```

**Retry logic**: contract discovery in `tap.py` calls `get_option_tickers_stream().get_response()` from `MassiveRestStream` (`client.py`), reusing the existing exponential backoff policy (max 10 tries, 600s max, full jitter, giveup on 403). Errors raise instead of silently returning partial contract lists.

**Scale reference** (AAPL, Feb 2026): ~119K expired contracts + ~3K active = ~122K total. Each contract requires a separate bars API call.

### Incremental Replication

Most streams use `INCREMENTAL` replication with timestamp-based replication keys. The `get_starting_replication_key_value` method resolves the effective start timestamp by taking the max of the Singer state value and the configured `start_date`. Some streams use date-only timestamps (`_incremental_timestamp_is_date = True`), while others expect Unix timestamps (`_api_expects_unix_timestamp = True` with `_unix_timestamp_unit` of `s`, `ms`, or `ns`).

## Stream Naming Conventions

Stock-specific streams use the `stock_` prefix. Cross-asset reference streams (ticker_types, exchanges, condition_codes) have both an unfiltered `All*Stream` and per-asset filtered versions (e.g., `stock_ticker_types`, `stock_exchanges`).

The meltano.yml `select:` and `config:` keys must match the stream's `name` attribute exactly.

## Endpoint Version Prefixes

- `/v1/`, `/v2/`, `/v3/` - Stable endpoints
- `/vX/` - Experimental/preview endpoints (IPOs, float, ticker events, financials deprecated, filings)
- `/stocks/v1/` - New stock-specific stable endpoints (splits, dividends, short interest, short volume)
- `/stocks/financials/v1/` - New flat financial statement endpoints (balance sheets, cash flow, income, ratios)

## Code Conventions

- Ruff is configured with `select = ["ALL"]` and `ignore = ["COM812"]`, targeting Python 3.9+, using Google-style docstrings
- API response keys are converted from camelCase to snake_case via `clean_keys()` / `post_process()`
- Numeric values use `Decimal` for precision (`safe_decimal()`, `safe_int()` helpers in `base_streams.py`)
- API key is always redacted from log messages via `redact_api_key()`
- The `massive` Python package (`RESTClient`) is used for market status but most streams use raw `requests` with custom pagination
- All imports at top of file, no inline imports

## Primary Key & Correctness Rules

**Correctness is paramount. Never silently truncate, drop, or misrepresent data.** When in doubt, stop and ask rather than guess.

### Choosing a Primary Key

Every stream must have a primary key that uniquely identifies a row. Be careful — a PK that looks unique in a small sample frequently collides in practice. The cost of getting this wrong is silent data corruption downstream (rows overwriting each other in the warehouse).

**Prefer a natural composite PK** built from the API's actual identity fields (e.g. for 13-F: `accession_number, cusip, title_of_class, put_call`). A surrogate key is a fallback, not the default. If a subset of fields already makes every row unique, use that subset directly — adding a `_surrogate_key` column on top of a working natural PK is wasted data.

Process:

1. **Identify what makes a row truly unique**: entity + time-period + variant axes.
2. **Inspect a multi-record sample**, not the docs alone. Look for any combination of fields that repeats across two rows.
3. **If a natural composite is unique across all real cases, use it directly as `primary_keys`.** This is the answer in almost every case.
4. **Handle nullable PK fields with coercion in `post_process`**, not by reaching for a surrogate. Example: in 13-F, `put_call` is null for common stock and gets coerced to `""` so the composite PK is non-null per row. Only one or two PK fields nullable → coerce.
5. **Reach for a surrogate key only when the natural composite genuinely fails** — i.e., multiple identity fields are simultaneously nullable in real data (so coercion would collide), or the API has no field combination that distinguishes rows. `EtfGlobalConstituentsStream` is the existing example: both `constituent_ticker` and `constituent_name` can be missing for the same constituent.

### Surrogate Key Pattern (fallback only)

When the natural composite genuinely fails per (5), hash the row's distinguishing fields into a deterministic UUID5 stored as `_surrogate_key`. See `tap_massive/etf_global_streams.py:EtfGlobalConstituentsStream`. Exclude fields whose values change between fetches of the same logical row (prices, weights, processed-at timestamps), or re-fetches produce different keys for the same row and break dedup.

If you find yourself listing identity fields for a surrogate, first ask: "why not just `primary_keys = [those fields]`, with `post_process` coercion if any are nullable?" Usually there's no good answer.

### No Silent Failures

- **Never return partial data without surfacing the issue.** If pagination breaks, raise — do not return what you have.
- **Never drop fields the API returns** just because they aren't in the schema. `additionalProperties: True` lets unknown fields through.
- **Never silently skip rows that fail validation.** Log and surface, do not swallow.
- **Never invent fields** that aren't in the API response or docs. If the schema must include a derived field, derive it explicitly in `post_process`.
- **If you cannot determine the correct schema or PK from the docs and a real sample**, stop and report rather than guess.

## Automated Changelog Monitoring

**Setup**: GitHub Actions workflow in `.github/workflows/monitor-massive-changelog.yml` automatically monitors Massive API changes

**How it works**:
1. Weekly automated check of Massive changelog RSS feed (https://massive.com/changelog/rss.xml)
2. If new changes detected, Claude reads the changelog + all stream files + CLAUDE.md
3. Claude identifies schema updates and creates a PR directly (no issue creation)

**Configuration**: See `.github/CHANGELOG_AUTOMATION.md` for setup instructions

**Required Secrets**:
- `ANTHROPIC_API_KEY`: Your Anthropic API key (for AI analysis)


STREAMS:

There are 132 streams as of today, but some may have been deprecated, renamed, or added since this list was made.

Stocks: 43

Options: 20

Futures: 9

Indices: 14

Forex: 20

Crypto: 21

Economy: 5




STOCKS

Stocks Overview

Tickers

All Tickers

Ticker Overview

Ticker Types

Related Tickers

Aggregate Bars (OHLC)

Custom Bars

Daily Market Summary

Daily Ticker Summary

Previous Day Bar

Snapshots

Single Ticker Snapshot

Full Market Snapshot

Unified Snapshot

Top Market Movers

Trades & Quotes

Trades

Last Trade

Quotes

Last Quote

Technical Indicators

SMA

EMA

MACD

RSI

Market Operations

Exchanges

Market Holidays

Market Status

Condition Codes

Corporate Actions

IPOs

Splits (Deprecated)

Splits

Dividends (Deprecated)

Dividends

Ticker Events

Fundamentals

Financials (Deprecated)

Balance Sheets

Cash Flow Statements

Income Statements

Ratios

Short Interest

Short Volume

Float

Filings & Disclosures

10-K Sections

Risk Factors

Risk Categories

News

OPTIONS

Options Overview

Contracts

All Contracts

Contract Overview

Aggregate Bars (OHLC)

Custom Bars

Daily Ticker Summary

Previous Day Bar

Snapshots

Option Contract Snapshot

Option Chain Snapshot

Unified Snapshot

Trades & Quotes

Trades

Last Trade

Quotes

Technical Indicators

SMA

EMA

MACD

RSI

Market Operations

Exchanges

Market Holidays

Market Status

Condition Codes

FUTURES

Futures Overview

Contracts

Products

Schedules

Aggregate Bars (OHLC)
Snapshots

Futures Contracts Snapshot

Trades & Quotes

Trades

Quotes

Market Operations

Market Status

Exchanges

INDICES

Indices Overview

Tickers

All Tickers

Ticker Overview

Aggregate Bars (OHLC)

Custom Bars

Previous Day Bar

Daily Ticker Summary

Snapshots

Indices Snapshot

Unified Snapshot

Technical Indicators

SMA

EMA

MACD

RSI

Market Operations

Market Holidays

Market Status

FOREX

Forex Overview

Tickers

All Tickers

Ticker Overview

Currency Conversion

Aggregate Bars (OHLC)

Custom Bars

Daily Market Summary

Previous Day Bar

Snapshots

Single Ticker Snapshot

Full Market Snapshot

Unified Snapshot

Top Market Movers

Quotes

Quotes

Last Quote

Technical Indicators

SMA

EMA

MACD

RSI

Market Operations

Exchanges

Market Holidays

Market Status

CRYPTO

Crypto Overview

Tickers

All Tickers

Ticker Overview

Aggregate Bars (OHLC)

Custom Bars

Daily Market Summary

Daily Ticker Summary

Previous Day Bar

Snapshots

Single Ticker Snapshot

Full Market Snapshot

Unified Snapshot

Top Market Movers

Trades

Trades

Last Trade

Technical Indicators

SMA

EMA

MACD

RSI

Market Operations

Exchanges

Market Holidays

Market Status

Condition Codes

ECONOMY

Economy Overview

Treasury Yields

Inflation

Inflation Expectations

Labor Market
