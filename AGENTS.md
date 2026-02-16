# Repository Guidelines

## Project Structure & Module Organization
- `tap_massive/`: Python package. `tap.py` registers stream classes; `*_streams.py` hold stream implementations; `client.py` contains API client helpers.
- `tests/`: pytest tests (currently `tests/test_core.py` uses Singer SDK built-in tests).
- `meltano.yml`: Meltano project config for running the tap in a pipeline.
- `plugins/`: Meltano plugin lockfiles (for example, `plugins/loaders/`).
- `output/`: local run artifacts or sample outputs.

## Build, Test, and Development Commands
- `uv sync`: install dev dependencies from `pyproject.toml`/`uv.lock`.
- `uv run tap-massive --help`: run the tap CLI in the project environment.
- `uv run pytest`: run the test suite.
- `tox -e py311`: run tests in a specific Python version (optional).
- `./lint.sh`: run `black`, `isort`, `flake8` (if installed).
- `pre-commit run --all-files`: run repo hooks (ruff, ruff-format, mypy, uv checks).
- `meltano install` then `meltano run tap-massive target-jsonl`: end-to-end orchestration.

## Coding Style & Naming Conventions
- Python, 4-space indentation, PEP 8 naming.
- Modules and functions use `snake_case`; classes use `CamelCase`.
- Stream modules follow `*_streams.py`; stream classes end with `Stream`.
- Formatting/linting: prefer `pre-commit` (ruff + ruff-format). `lint.sh` is a legacy path.

## Testing Guidelines
- Framework: pytest + Singer SDK test harness.
- Tests live in `tests/` and should be named `test_*.py`.
- If you add required tap settings, update `SAMPLE_CONFIG` in `tests/test_core.py`.
- No explicit coverage threshold is enforced.

## Commit & Pull Request Guidelines
- Commits are short, imperative summaries (for example, “add economy streams”); no enforced conventional prefix.
- PRs should include a brief summary of behavior changes, how you tested (`uv run pytest`, `tox -e ...`, or a Meltano command), and any changes to `meltano.yml` or plugin lockfiles.

## Configuration & Secrets
- Prefer environment variables or a local `.env` file; the tap supports `--config=ENV`.
- Avoid committing credentials; keep local secrets out of version control.

## Massive Options Reliability Notes
- `options_bars_*` streams only emit bars for contracts/intervals with qualifying trades; missing bars do not always mean ingest failure.
- For historical backfills, option contract discovery is point-in-time unless `as_of` is pinned. If `as_of` is omitted, the provider defaults to "today", which can silently narrow historical contract coverage.
- The options contracts endpoint `expired` flag is a selector (active vs expired), not an "include all" toggle.
- For full historical coverage in one run, use `option_tickers.other_params.expired: "both"`; the tap makes two calls (`expired=true` and `expired=false`) and unions by contract ticker.
- For non-`"both"` mode, options bars/snapshot/trades contract discovery uses `option_tickers.query_params` in `TapMassive.get_option_contracts_for_underlying()`, not `options_contracts.query_params`.
- Contract discovery requests now retry with exponential backoff and raise on unrecoverable failures (including exhausted retries), avoiding silent partial contract universes.
