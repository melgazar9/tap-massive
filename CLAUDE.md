# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`tap-massive` is a Singer tap for the Massive financial data API, built with the [Meltano Singer SDK](https://sdk.meltano.com). It extracts market data across six asset classes: stocks, options, forex, crypto, indices, and futures, plus economy data (treasury yields, inflation, labor market).

## Development Commands

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Run a single test
uv run pytest tests/test_core.py -k "test_name"

# Lint (pre-commit hooks: ruff, ruff-format, mypy)
pre-commit run --all-files

# Run the tap directly
uv run tap-massive --help
uv run tap-massive --about

# Run via Meltano (requires meltano install first)
meltano run tap-massive target-jsonl
```

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
| `BaseTickerTypesStream` | `/v3/reference/tickers/types` | stocks, options, crypto, fx, indices |
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

## Automated Changelog Monitoring

**Setup**: GitHub Actions workflows in `.github/workflows/` automatically monitor Massive API changes

**How it works**:
1. Weekly automated check of Massive changelog RSS feed (https://massive.com/changelog/rss.xml)
2. Creates GitHub issues when changes detected
3. Email notifications via GitHub
4. Claude AI analyzes changes and creates PRs on-demand

**Configuration**: See `.github/CHANGELOG_AUTOMATION.md` for setup instructions

**Required Secrets**:
- `ANTHROPIC_API_KEY`: Your Anthropic API key (for AI analysis)

**Usage**: Comment `/analyze-changes` on any changelog issue to trigger AI analysis and PR creation
