import logging
import re
import socket
import typing as t
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import backoff
import requests
from massive import RESTClient
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers.types import Context
from singer_sdk.streams import RESTStream


class MassiveRestStream(RESTStream):
    _incremental_timestamp_is_date = False
    _api_expects_unix_timestamp = False
    _unix_timestamp_unit = None
    _use_cached_tickers_default = True
    _requires_end_timestamp_in_query_params = False
    _requires_end_timestamp_in_path_params = False
    _ticker_param = "ticker"

    # Subclasses override to control which config sections are checked for select_tickers.
    ticker_selector_keys: tuple[str, ...] = ()

    @staticmethod
    def to_snake_case(s: str) -> str:
        """Convert camelCase or PascalCase string to snake_case."""
        return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()

    def clean_keys(self, d: dict) -> None:
        """Recursively convert all dict keys to snake_case in-place."""
        keys = list(d.keys())
        for key in keys:
            value = d.pop(key)
            new_key = self.to_snake_case(key)
            if isinstance(value, dict):
                self.clean_keys(value)
            d[new_key] = value

    def __init__(self, tap):
        super().__init__(tap=tap)
        self.tap = tap
        self.client = RESTClient(self.config["api_key"])
        self.parse_config_params()

        self._cfg_starting_timestamp_key: str | None = None
        self._cfg_ending_timestamp_key: str | None = None
        self._cfg_starting_timestamp_value: str | None = None
        self._cfg_ending_timestamp_value: str | None = None

        self.timestamp_filter_fields = [
            "from",
            "from_",
            "to",
            "to_",
            "date",
            "timestamp",
            "ex_dividend_date",
            "record_date",
            "declaration_date",
            "pay_date",
            "listing_date",
            "execution_date",
            "filing_date",
            "period_of_report_date",
            "settlement_date",
            "published_utc",
            "published",
            "last_updated",
        ]
        self.record_timestamp_keys = [
            "timestamp",
            "date",
            "last_updated",
            "announced_date",
            "published_utc",
            "published",
            "ex_dividend_date",
            "sip_timestamp",
            "record_date",
            "trf_timestamp",
            "declaration_date",
            "pay_date",
            "last_updated_utc",
            "listing_date",
            "execution_date",
            "participant_timestamp",
            "filing_date",
            "acceptance_datetime",
            "end_date",
            "settlement_date",
        ]

        timestamp_filter_suffixes = ["gt", "gte", "lt", "lte"]

        self.timestamp_field_combos = self.timestamp_filter_fields + [
            f"{field}.{suffix}"
            for field in self.timestamp_filter_fields
            for suffix in timestamp_filter_suffixes
        ]

        self._starting_timestamp_keys = ["from", "from_"] + [
            i
            for i in self.timestamp_field_combos
            if i.endswith("gte") or i.endswith("gt")
        ]

        self._ending_timestamp_keys = ["to", "to_"] + [
            i
            for i in self.timestamp_field_combos
            if i.endswith("lte") or i.endswith("lt")
        ]

        self._set_timestamp_config_keys()

        if self._cfg_starting_timestamp_key and not self._cfg_starting_timestamp_value:
            raise ConfigValidationError(
                f"For stream {self.name} the starting timestamp field "
                f"'{self._cfg_starting_timestamp_key}' is configured but has no value."
            )

        if getattr(self, "_asset_class", None) is not None:
            self.query_params["asset_class"] = self._asset_class

    @staticmethod
    def iso_to_unix_timestamp(dt_str: str, unit: str) -> int:
        """Convert ISO8601 string to Unix timestamp.
        unit: "s" (seconds), "ms" (milliseconds), "ns" (nanoseconds)
        """
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        ts = dt.timestamp()
        if unit == "s":
            return int(ts)
        if unit == "ms":
            return int(ts * 1000)
        if unit == "ns":
            return int(ts * 1_000_000_000)
        raise ValueError(f"Unknown unit: {unit}")

    def _get_config_child_params(
        self, child_key: str, parent_key: str = "other_params"
    ):
        """Fetch stream-specific config value, e.g. 'other_params'"""
        stream_configs = self.config.get(self.name, [])
        if parent_key in stream_configs:
            value = stream_configs.get(parent_key).get(child_key)
            return value
        return None

    @staticmethod
    def _config_key_for_market(market: str | None) -> str | None:
        """Map a market name (e.g. 'stock', 'fx') to its config section (e.g. 'stock_tickers')."""
        if not market:
            return None
        return {
            "stock": "stock_tickers",
            "stocks": "stock_tickers",
            "fx": "forex_tickers",
            "forex": "forex_tickers",
            "crypto": "crypto_tickers",
            "indices": "indices_tickers",
            "option": "option_tickers",
            "options": "option_tickers",
            "future": "futures_tickers",
            "futures": "futures_tickers",
        }.get(market, f"{market}_tickers")

    @staticmethod
    def _canonicalize_ticker(ticker: str, market: str | None) -> str:
        """Strip and apply minimal market-specific formatting (e.g. forex C: prefix)."""
        normalized = ticker.strip()
        if not normalized:
            return normalized
        if market in ("fx", "forex"):
            if normalized.startswith("C:"):
                return normalized
            letters = re.sub(r"[^A-Za-z]", "", normalized)
            if len(letters) == 6:
                return f"C:{letters}"
        return normalized

    def resolve_select_tickers(
        self,
        *,
        selector_keys: tuple[str, ...] = (),
        market: str | None = None,
        include_stream_name: bool = False,
    ) -> list[str] | None:
        """Resolve select_tickers from config, checking sections in precedence order.

        Returns a canonicalized list of ticker strings, or None if no filter is configured.
        """
        sections: list[str] = []
        sections.extend(selector_keys)

        market_key = self._config_key_for_market(market)
        if market_key:
            sections.append(market_key)

        if include_stream_name:
            sections.append(self.name)

        # Deduplicate while preserving order
        seen: set[str] = set()
        ordered_sections: list[str] = []
        for section in sections:
            if section and section not in seen:
                seen.add(section)
                ordered_sections.append(section)

        for section in ordered_sections:
            cfg = self.config.get(section, {})
            if not isinstance(cfg, dict):
                continue
            raw = cfg.get("select_tickers")
            if not raw or raw in ("*", ["*"]):
                continue
            if isinstance(raw, str):
                resolved = [
                    self._canonicalize_ticker(t, market)
                    for t in raw.split(",")
                    if t.strip()
                ]
            elif isinstance(raw, list):
                resolved = [
                    self._canonicalize_ticker(str(t), market)
                    for t in raw
                    if str(t).strip()
                ]
            else:
                continue
            if resolved and resolved != ["*"]:
                return resolved
        return None

    @staticmethod
    def _filter_by_tickers(
        records: list[dict[str, t.Any]],
        allowed: list[str] | None,
        key: str = "ticker",
    ) -> list[dict[str, t.Any]]:
        """Filter records to only those whose key is in the allowed ticker list."""
        if not allowed:
            return records
        allowed_set = set(allowed)
        return [r for r in records if r.get(key) in allowed_set]

    def get_ticker_list(self) -> list[str] | None:
        """Return the resolved select_tickers for this stream, or None if unfiltered."""
        return self.resolve_select_tickers(
            selector_keys=self.ticker_selector_keys,
            market=getattr(self, "market", None),
        )

    @property
    def use_cached_tickers(self) -> bool:
        config_val = self._get_config_child_params("use_cached_tickers")
        if config_val is not None:
            assert isinstance(
                config_val, bool
            ), f"Config for use_cached_tickers must be bool, got {type(config_val)}"
            return config_val
        if hasattr(type(self), "_use_cached_tickers_default"):
            return type(self)._use_cached_tickers_default
        raise AttributeError("use_cached_tickers is not defined in config or class")

    def _set_timestamp_config_keys(self) -> None:
        if self._cfg_starting_timestamp_key and self._cfg_ending_timestamp_key:
            return

        self.query_params = self._normalize_cfg_param_keys(self.query_params)
        self.path_params = self._normalize_cfg_param_keys(self.path_params)

        for params in (self.query_params.copy(), self.path_params.copy()):
            for k, v in params.items():
                if k not in self.timestamp_field_combos:
                    continue

                if (
                    not self._cfg_starting_timestamp_key
                    and k not in self._ending_timestamp_keys
                ):
                    self._cfg_starting_timestamp_key = k
                    self._cfg_starting_timestamp_value = v

                if (
                    not self._cfg_ending_timestamp_key
                    and k in self._ending_timestamp_keys
                    and k not in self._starting_timestamp_keys
                ):
                    self._cfg_ending_timestamp_key = k
                    self._cfg_ending_timestamp_value = v

                if self._cfg_starting_timestamp_key and self._cfg_ending_timestamp_key:
                    break
            if self._cfg_starting_timestamp_key and self._cfg_ending_timestamp_key:
                break

        if (
            not self._cfg_ending_timestamp_key
            and self._cfg_starting_timestamp_key
            and self.config.get(self.name, {})
            .get("other_params", {})
            .get("loop_over_dates_gte_date")
        ):
            self._cfg_ending_timestamp_key = self._cfg_starting_timestamp_key
            self._cfg_ending_timestamp_value = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d"
            )
        return

    @property
    def url_base(self) -> str:
        return self.config.get("base_url", "https://api.massive.com")

    def safe_parse_datetime(self, dt_value: t.Any) -> datetime | None:
        if isinstance(dt_value, datetime):
            return (
                dt_value.replace(tzinfo=timezone.utc)
                if dt_value.tzinfo is None
                else dt_value
            )
        if isinstance(dt_value, (int, float, Decimal)):
            try:
                seconds = self._timestamp_to_epoch(dt_value)
                return datetime.fromtimestamp(seconds, tz=timezone.utc)
            except (ValueError, OSError) as e:
                logging.warning(
                    f"Could not parse numeric timestamp: {dt_value}, error: {e}"
                )
                return None
        if isinstance(dt_value, str):
            return datetime.fromisoformat(dt_value.replace("Z", "+00:00")).replace(
                tzinfo=timezone.utc
            )
        return None

    @staticmethod
    def _timestamp_to_epoch(ts: float | Decimal | str | None) -> float | None:
        if ts is None:
            return None
        if isinstance(ts, (int, float)):
            if abs(ts) > 1e15:  # nanoseconds
                return ts / 1e9
            if abs(ts) > 1e13:  # microseconds
                return abs(ts) / 1e6
            if abs(ts) > 1e10:  # milliseconds
                return ts / 1e3
            return ts
        return None

    def get_starting_replication_key_value(
        self, context: Context | None
    ) -> t.Any | None:
        if (
            not self.is_timestamp_replication_key
            or self.replication_method != "INCREMENTAL"
        ):
            return None

        state = self.get_context_state(context)

        state_replication_value = (
            state.get("replication_key_value", state.get("starting_replication_value"))
            if state
            else None
        )

        state_dt = self.safe_parse_datetime(state_replication_value)
        cfg_dt = self.safe_parse_datetime(self._cfg_starting_timestamp_value)
        start_date_config = self.safe_parse_datetime(self.config.get("start_date"))

        if (
            state is not None
            and state_replication_value is None
            and "replication_key_value" in state
        ) or (state_dt is None and cfg_dt is None):
            logging.critical(
                f"Unable to parse datetimes for state_dt and cfg_dt! Check stream {self.name}."
                f"State: {state}, "
                f"state_replication_value: {state_replication_value}, "
                f"self._cfg_starting_timestamp_value: {self._cfg_starting_timestamp_value}"
                f"CONFIG: {self.config}"
            )

        if (
            self.is_timestamp_replication_key
            and self.replication_method == "INCREMENTAL"
        ):
            candidates = [dt for dt in (state_dt, cfg_dt, start_date_config) if dt]
            if candidates:
                return max(candidates).isoformat()
        else:
            return self.get_starting_timestamp(context)

    def get_record_timestamp_key(self, record: dict | list) -> str | None:
        target_record = None
        if isinstance(record, dict):
            target_record = record
        elif isinstance(record, list) and record and isinstance(record[0], dict):
            target_record = record[0]
        if target_record:
            for preferred_key in self.record_timestamp_keys:
                if preferred_key in target_record:
                    return preferred_key
        return None

    @staticmethod
    def _normalize_cfg_param_keys(d):
        return {k.replace("__", "."): v for k, v in d.items()}

    def _update_query_path_params_with_state(
        self,
        context: Context,
        query_params: dict,
        path_params: dict,
        pop_timestamp: bool = False,
    ) -> tuple[dict, dict]:
        if pop_timestamp:
            query_params = {
                k: v
                for k, v in query_params.items()
                if k not in self.timestamp_field_combos
            }
            path_params = {
                k: v
                for k, v in path_params.items()
                if k not in self.timestamp_field_combos
            }
        elif self.is_timestamp_replication_key and self._cfg_starting_timestamp_key:
            starting_replication_key_value = self.get_starting_replication_key_value(
                context
            )
            if starting_replication_key_value:
                if self._cfg_starting_timestamp_key in query_params:
                    query_params[self._cfg_starting_timestamp_key] = (
                        starting_replication_key_value
                    )
                if self._cfg_starting_timestamp_key in path_params:
                    path_params[self._cfg_starting_timestamp_key] = (
                        starting_replication_key_value
                    )
        return query_params, path_params

    def _prepare_context_and_params(
        self, context: Context | None
    ) -> tuple[Context, dict, dict]:
        """Helper method to prepare the context, query_params, and path_params
        common to both get_records and get_records_optional.
        """
        context = context if context is not None else {}
        base_query_params = self.query_params.copy()
        base_path_params = self.path_params.copy()

        query_params, path_params = self._update_query_path_params_with_state(
            context, base_query_params, base_path_params
        )

        if (
            self._requires_end_timestamp_in_path_params
            and self._cfg_ending_timestamp_key not in path_params
        ):
            path_params[self._cfg_ending_timestamp_key] = (
                self._get_end_timestamp_value()
            )

        if (
            self._requires_end_timestamp_in_query_params
            and self._cfg_ending_timestamp_key not in query_params
        ):
            query_params[self._cfg_ending_timestamp_key] = (
                self._get_end_timestamp_value()
            )

        context["query_params"] = query_params
        context["path_params"] = path_params

        return context, query_params, path_params

    @staticmethod
    def redact_api_key(msg):
        return re.sub(r"(apiKey=)([^\s&]+)", r"\1<REDACTED>", msg)

    @staticmethod
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException,),
        max_tries=10,
        max_time=3600,
        jitter=backoff.full_jitter,
        giveup=lambda e: isinstance(e, requests.exceptions.HTTPError)
        and e.response is not None
        and e.response.status_code in (403, 404),
        on_backoff=lambda details: logging.warning(
            f"API request failed, retrying in {details['wait']:.1f}s "
            f"(attempt {details['tries']}): {details['exception']}"
        ),
    )
    def fetch_with_retry(request_get, url: str, params: dict, timeout: int = 300):
        response = request_get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response

    def get_response(self, url, query_params):
        timeout = self.config.get("request_timeout", 300)
        try:
            return self.fetch_with_retry(
                request_get=self.requests_session.get,
                url=url,
                params=query_params,
                timeout=timeout,
            )
        except requests.exceptions.ConnectionError as ce:
            log_url = self.redact_api_key(url)
            log_exception = self.redact_api_key(str(ce))
            if isinstance(ce.__cause__, socket.gaierror):
                logging.exception(
                    f"DNS resolution failed for {log_url}: {log_exception}"
                )
            else:
                logging.exception(f"Connection error for {log_url}: {log_exception}")
            raise
        except requests.HTTPError as e:
            log_exception = self.redact_api_key(str(e))
            if e.response is not None and e.response.status_code in (404, 204):
                log_url = self.redact_api_key(url)
                logging.warning(
                    f"No data for {log_url} (status {e.response.status_code}): {log_exception}"
                )
                return None
            logging.exception(f"HTTP Error: {log_exception}")
            redacted_url = self.redact_api_key(e.request.url if e.request else url)
            error_message = (
                f"{e.response.status_code} Client Error: {e.response.reason} for url: {redacted_url}"
                if e.response and e.request
                else str(e)
            )
            error_message = self.redact_api_key(error_message)
            raise requests.exceptions.HTTPError(
                error_message,
                response=e.response,
                request=e.request,
            )

    def paginate_records(self, context: Context) -> t.Iterable[dict[str, t.Any]]:
        query_params = context.get("query_params", {}).copy()
        path_params = context.get("path_params", {}).copy()
        self.normalize_date_params(
            query_params, force_date=self._incremental_timestamp_is_date
        )
        self.normalize_date_params(
            path_params, force_date=self._incremental_timestamp_is_date
        )

        request_context = dict(
            ticker=context.get(self._ticker_param),
            query_params=query_params,
            path_params=path_params,
        )

        if "query_params" in context:
            context.pop("query_params")
        if "path_params" in context:
            context.pop("path_params")

        next_url = None
        no_records_counter = 0

        state = self.get_context_state(context)

        while True:
            request_url = next_url or self.get_url(request_context)

            query_params_to_log = {
                k: v for k, v in query_params.items() if k != "apiKey"
            }
            logging.info(
                f"Streaming {self.name} from URL: {request_url} with query_params: {query_params_to_log}..."
            )
            try:
                response = self.get_response(url=request_url, query_params=query_params)
                if response is None:
                    break
                data = response.json()
            except requests.exceptions.RequestException as e:
                safe_exception = self.redact_api_key(str(e))
                # For 403 errors, log and continue to next ticker instead of breaking
                if (
                    isinstance(e, requests.exceptions.HTTPError)
                    and e.response is not None
                    and e.response.status_code == 403
                ):
                    logging.warning(
                        f"*** Access denied (403) for {self.name} at {request_url}: {safe_exception} - skipping this ticker ***"
                    )
                    break  # Break the pagination loop for this ticker, but don't crash
                logging.exception(
                    f"*** Request failed for {self.name} at {request_url}: {safe_exception} ***"
                )
                break
            except ValueError as e:
                safe_exception = self.redact_api_key(str(e))
                logging.exception(
                    f"Failed to decode JSON for {self.name} at {request_url}: {safe_exception}"
                )
                break

            if isinstance(data, dict):
                records = data.get("results", data)
            elif isinstance(data, list):
                records = data
            else:
                raise ValueError(
                    f"Expecting response data to be type list or dict, got type {type(data)} for stream {self.name}."
                )

            if not records:
                logging.info(
                    f"No records returned for {self.name} in this batch. Checking if it's a persistent empty response."
                )
                no_records_counter += 1
                if no_records_counter >= 3:
                    logging.info(
                        f"Breaking pagination for {self.name} due to {no_records_counter} consecutive empty record batches."
                    )
                    break
            else:
                no_records_counter = 0

            if not isinstance(records, list):
                records = [records]

            latest_record = None
            for raw_record in records:
                try:
                    for record in self.parse_response(raw_record, context):
                        try:
                            record = self.post_process(record, context)
                        except Exception as e:
                            logging.exception(
                                f"Failed to post-process record for {self.name} at {request_url}: {e}. RECORD: {record}"
                            )
                            continue
                        if not record:
                            continue
                        try:
                            self._check_missing_fields(self.schema, record)
                        except TypeError as e:
                            logging.exception(
                                f"Failed to parse record for {self.name} at {request_url}: {e}. RECORD: {record}"
                            )
                        latest_record = record
                        yield record
                except Exception as e:
                    logging.exception(
                        f"Failed to parse raw record for {self.name} at {request_url}: {e}. RECORD: {raw_record}"
                    )
                    raise

            if self.replication_method == "INCREMENTAL" and latest_record is not None:
                increment_state(
                    state,
                    replication_key=self.replication_key,
                    latest_record=latest_record,
                    is_sorted=self.is_sorted,
                    check_sorted=self.check_sorted,
                )

                if (
                    "progress_markers" in state
                    and "replication_key_value" in state["progress_markers"]
                ):
                    state["replication_key_value"] = state["progress_markers"][
                        "replication_key_value"
                    ]

            if isinstance(data, list):
                logging.info(
                    f"Breaking out of loop for stream {self.name}. Not checking pagination for next_url."
                )
                break

            next_url = data.get("next_url")
            query_params, path_params = self._update_query_path_params_with_state(
                context, query_params, path_params, pop_timestamp=True
            )

            replication_key_value = self.get_starting_replication_key_value(context)
            if self._break_loop_check(next_url, replication_key_value):
                break

    def get_url(self, context: Context) -> str:
        raise NotImplementedError(
            "Method get_url must be overridden in the stream class."
        )

    def parse_response(self, record: dict, context: Context) -> t.Iterable[dict]:
        """Default passthrough: yield the record unchanged."""
        yield record

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Default post-processing: convert all keys to snake_case."""
        self.clean_keys(row)
        return row

    def parse_config_params(self) -> None:
        cfg_params = self.config.get(self.name)
        self.path_params = {}
        self.query_params = {}
        self.other_params = {}

        if not cfg_params:
            logging.warning(f"No config set for stream '{self.name}', using defaults.")
        elif isinstance(cfg_params, dict):
            self.path_params = cfg_params.get("path_params", {})
            self.query_params = cfg_params.get("query_params", {})
            self.other_params = cfg_params.get("other_params", {})
        elif isinstance(cfg_params, list):
            for params_dict in cfg_params:
                if not isinstance(params_dict, dict):
                    raise ConfigValidationError(
                        f"Expected dict in '{self.name}', but got {type(params_dict)}: {params_dict}"
                    )
                self.path_params.update(params_dict.get("path_params", {}))
                self.query_params.update(params_dict.get("query_params", {}))
                self.other_params.update(params_dict.get("other_params", {}))
        else:
            raise ConfigValidationError(
                f"Config key '{self.name}' must be a dict or list of dicts."
            )

        self.query_params["apiKey"] = self.config.get("api_key")

    def normalize_date_params(self, params: dict, force_date: bool = False) -> None:
        """Normalize date/datetime params inplace, for (query and path)
        params prior to sending a request or building the url endpoint.
        """
        if self.is_timestamp_replication_key:
            for timestamp_col in (
                self._cfg_starting_timestamp_key,
                self._cfg_ending_timestamp_key,
            ):
                if timestamp_col in params:
                    if self._api_expects_unix_timestamp and isinstance(
                        params[timestamp_col], int
                    ):
                        continue

                    dt_obj = self.safe_parse_datetime(params[timestamp_col])

                    if dt_obj:
                        if self._api_expects_unix_timestamp:
                            assert self._unix_timestamp_unit in (
                                "s",
                                "ms",
                                "ns",
                            ), (
                                f"_unix_timestamp_unit in stream {self.name} must be 's', 'ms', or 'ns'."
                                f"Currently it's set to value {self._unix_timestamp_unit}"
                            )
                            params[timestamp_col] = self.iso_to_unix_timestamp(
                                dt_obj.isoformat(),
                                unit=self._unix_timestamp_unit,
                            )
                        elif self._incremental_timestamp_is_date or force_date:
                            params[timestamp_col] = dt_obj.date().isoformat()
                        else:
                            params[timestamp_col] = dt_obj.isoformat()
                    else:
                        logging.info(
                            f"Could not parse timestamp value '{params[timestamp_col]}' for key '{timestamp_col}'."
                            f"Skipping normalization."
                        )

    def _get_end_timestamp_value(self):
        """Helper to calculate the end timestamp value based on configuration."""
        if self._incremental_timestamp_is_date:
            return datetime.today().date().isoformat()
        if self._api_expects_unix_timestamp:
            return self.iso_to_unix_timestamp(
                datetime.today().isoformat(), unit=self._unix_timestamp_unit
            )
        return datetime.today().isoformat()

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Only handle one partition per get_records call.
        Let the tap framework handle looping over partitions (i.e., per ticker).
        """
        context, query_params, path_params = self._prepare_context_and_params(context)

        loop_over_dates = self.other_params.get("loop_over_dates_gte_date", False)

        if not loop_over_dates:
            yield from self.paginate_records(context)
        else:
            if not self._cfg_starting_timestamp_value:
                raise ConfigValidationError(
                    f"Stream {self.name} is configured to loop over dates, but "
                    "'_cfg_starting_timestamp' is not set."
                )

            start_date = datetime.strptime(
                self._cfg_starting_timestamp_value.split("T")[0], "%Y-%m-%d"
            ).date()

            end_date = datetime.today().date()

            if self._cfg_ending_timestamp_key and self._cfg_ending_timestamp_value:
                try:
                    configured_end_date = datetime.strptime(
                        self._cfg_ending_timestamp_value.split("T")[0], "%Y-%m-%d"
                    ).date()
                    end_date = min(configured_end_date, end_date)
                except ValueError:
                    logging.warning(
                        f"Could not parse _cfg_end_timestamp '{self._cfg_ending_timestamp_value}'. Using today's date."
                    )

            current_timestamp = start_date

            logging.info(
                f"*** Looping over dates for stream '{self.name}' where start_date is {start_date}"
                f"and end_date is {end_date} ***"
            )
            while current_timestamp <= end_date:
                context["query_params"] = query_params.copy()
                context["path_params"] = path_params.copy()
                if self._cfg_starting_timestamp_key in context.get("query_params"):
                    context["query_params"][
                        self._cfg_starting_timestamp_key
                    ] = current_timestamp.isoformat()
                if self._cfg_starting_timestamp_key in context.get("path_params"):
                    context["path_params"][
                        self._cfg_starting_timestamp_key
                    ] = current_timestamp.isoformat()
                yield from self.paginate_records(context)
                current_timestamp += timedelta(days=1)

    def _check_missing_fields(self, schema: dict, record: dict):
        schema_fields = set(schema.get("properties", {}).keys())
        record_keys = set(record.keys())
        missing_in_record = schema_fields - record_keys
        if missing_in_record:
            logging.debug(
                f"*** Missing fields in record that are present in schema: {missing_in_record} for tap {self.name} ***"
            )
        missing_in_schema = record_keys - schema_fields
        if missing_in_schema:
            logging.critical(
                f"*** URGENT: Missing fields in schema that are present record: {missing_in_schema} ***"
            )

    def _has_timestamp_field(self) -> bool:
        """Return True if this stream has a valid timestamp/replication key field."""
        return bool(
            self.replication_key and self.replication_key in self.record_timestamp_keys
        )

    def _break_loop_check(self, next_url, replication_key_value) -> bool:
        if not next_url:
            logging.debug(
                f"No 'next_url' in context for stream {self.name}. Breaking pagination."
            )
            return True

        if not self._has_timestamp_field():
            return False

        if self.replication_method != "INCREMENTAL":
            return False

        if replication_key_value is None and self.replication_method == "INCREMENTAL":
            logging.error(
                f"*** No '{self.replication_key}' found in context for stream {self.name}. Continuing pagination... ***"
            )
            return False

        last_ts_dt = self.safe_parse_datetime(replication_key_value)

        if last_ts_dt is None:
            logging.warning(
                f"Could not parse '{self.replication_key}' from context for stream {self.name}. Continuing."
            )
            return False

        if self._cfg_starting_timestamp_key:
            cutoff_start_dt = self.safe_parse_datetime(
                self._cfg_starting_timestamp_value
            )

            if cutoff_start_dt and last_ts_dt < cutoff_start_dt:
                logging.info(
                    f"Last record timestamp ({last_ts_dt}) in batch is older than 'from' timestamp ({cutoff_start_dt}). "
                    f"Breaking pagination for {self.name}."
                )
                return True

        if self._cfg_ending_timestamp_key and self._cfg_ending_timestamp_value:
            cutoff_end_dt = self.safe_parse_datetime(self._cfg_ending_timestamp_value)
            if cutoff_end_dt and last_ts_dt > cutoff_end_dt:
                logging.info(
                    f"Latest record timestamp ({last_ts_dt}) in batch exceeds 'to' timestamp ({cutoff_end_dt}). "
                    f"Breaking pagination for {self.name}."
                )
                return True
        return False


class OptionalTickerPartitionStream(MassiveRestStream):
    _ticker_in_path_params = None
    _ticker_in_query_params = None
    ticker_selector_keys: tuple[str, ...] = ("stock_tickers",)
    ticker_record_key = "ticker"

    def get_cached_ticker_records(self) -> list[dict[str, t.Any]]:
        """Return cached ticker records used for manual ticker looping."""
        tap_obj = getattr(self, "tap", None) or getattr(self, "_tap")
        return tap_obj.get_cached_stock_tickers()

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Loops over tickers manually instead of calling built-in partitions for flexibility in meltano.yml other_params."""
        if self.use_cached_tickers:
            assert (
                self._ticker_in_path_params is not None
                or self._ticker_in_query_params is not None
            ), "Both _ticker_in_path_params and _ticker_in_query_params cannot be None."

        context, query_params, path_params = self._prepare_context_and_params(context)

        if (
            self._requires_end_timestamp_in_path_params
            and self._cfg_ending_timestamp_key not in path_params
        ):
            path_params[self._cfg_ending_timestamp_key] = (
                self._get_end_timestamp_value()
            )

        if (
            self._requires_end_timestamp_in_query_params
            and self._cfg_ending_timestamp_key not in query_params
        ):
            query_params[self._cfg_ending_timestamp_key] = (
                self._get_end_timestamp_value()
            )

        if self.use_cached_tickers:
            ticker_records = self.get_cached_ticker_records()
            ticker_records = self._filter_by_tickers(
                ticker_records,
                allowed=self.get_ticker_list(),
                key=self.ticker_record_key,
            )

            for ticker_record in ticker_records:
                context["query_params"] = query_params
                context["path_params"] = path_params
                if self._ticker_in_query_params:
                    query_params[self._ticker_param] = ticker_record[
                        self.ticker_record_key
                    ]
                if self._ticker_in_path_params:
                    path_params[self._ticker_param] = ticker_record[
                        self.ticker_record_key
                    ]
                yield from self.paginate_records(context)
        else:
            yield from self.paginate_records(context)
