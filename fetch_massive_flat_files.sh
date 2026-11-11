#!/usr/bin/env bash

# Example usage:
# ./fetch_massive_flat_files.sh --eod --start-date "2025-05-12" --end-date "2025-05-13" --asset-class us_stocks_sip
# ./fetch_massive_flat_files.sh --trades --start-date "2025-05-12" --end-date "2025-05-13" --asset-class us_options_opra
# ./fetch_massive_flat_files.sh --all --start-date "2025-05-12" --end-date "2025-05-13" --asset-class global_forex
# ./fetch_massive_flat_files.sh --all --exclude quotes --start-date "2025-05-12" --end-date "2025-05-13" --asset-class global_forex
# ./fetch_massive_flat_files.sh --all --exclude quotes,trades --start-date "2025-05-12" --end-date "2025-05-13" --asset-class us_stocks_sip
# ./fetch_massive_flat_files.sh --all --start-date "2025-05-12" --end-date "2025-05-13" --asset-class global_crypto
# ./fetch_massive_flat_files.sh --bars-1m --start-date "2025-05-12" --end-date "2025-05-13" --asset-class us_indices
# ./fetch_massive_flat_files.sh --values --start-date "2025-05-12" --end-date "2025-05-13" --asset-class us_indices
# ./fetch_massive_flat_files.sh --bars-1m --exchange CBOT --start-date "2025-05-12" --end-date "2025-05-13" --asset-class futures
# ./fetch_massive_flat_files.sh --all --exchange CBOT,CME --start-date "2025-05-12" --end-date "2025-05-13" --asset-class futures
# ./fetch_massive_flat_files.sh --all --start-date "2025-05-12" --end-date "2025-05-13" --asset-class futures  # all 4 exchanges

if [[ -z "$MASSIVE_AWS_ACCESS_KEY_ID" || -z "$MASSIVE_API_KEY" ]]; then
  echo "Error: MASSIVE_AWS_ACCESS_KEY_ID and MASSIVE_API_KEY must be set"
  exit 1
fi

export AWS_ACCESS_KEY_ID="$MASSIVE_AWS_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$MASSIVE_API_KEY"
# Unset, every aws call first waits on an EC2 metadata service this host never has; unset region signs as us-east-1 anyway.
export AWS_DEFAULT_REGION="us-east-1"
export AWS_EC2_METADATA_DISABLED="true"

ENDPOINT="--endpoint-url https://files.massive.com"
# Bytes of aws's own error text kept per failure: an error and its reason must stay one atomic write between workers.
MAX_REASON_BYTES=2000

usage() {
  echo "Usage: $0 --trades|--quotes|--bars-1m|--eod|--values|--all [--exclude TYPE,...] [--exchange LIST] [--destination DIR] [--parallel N] --start-date YYYY-MM-DD [--end-date YYYY-MM-DD] --asset-class ASSET_CLASS"
  echo ""
  echo "Asset class examples:"
  echo "  us_stocks_sip, us_options_opra, us_indices"
  echo "  global_forex, global_crypto, futures"
  echo ""
  echo "Options:"
  echo "  --end-date DATE     Last date to download (default: today, UTC)"
  echo "  --destination DIR   Base directory for downloads (default: \$HOME/massive_data)"
  echo "  --exclude TYPE,...  Exclude dataset types when using --all."
  echo "                      Valid types: trades, quotes, bars-1m, eod, values"
  echo "                      Example: --all --exclude quotes,trades"
  echo "  --exchange LIST     Comma-separated futures exchanges (CBOT,CME,NYMEX,COMEX)."
  echo "                      Only valid with --asset-class futures. Default: all four."
  echo "  --parallel N        Files downloaded at once (default: $PARALLEL_DOWNLOADS)."
  echo ""
  echo "Notes:"
  echo "  - All downloads are validated (listed size, gzip integrity) before being renamed from"
  echo "    .<file>.tmp to <file>, so a partial or corrupt download never takes the final name."
  echo "  - The files to fetch are the ones Massive lists in the date range; a file already on disk"
  echo "    at the listed size is kept, so a re-run fetches only what is missing or changed."
  echo "  - Indices flat files support minute aggregates, day aggregates (--eod), and values."
  echo "  - Forex flat files support quotes and minute/eod aggregates only."
  echo "  - Crypto flat files support trades and minute/eod aggregates only."
  echo "  - Futures flat files support trades, quotes, --bars-1m (minute-aggregates), and"
  echo "    --eod (session-aggregates) per exchange (CBOT, CME, NYMEX, COMEX)."
  exit 1
}

DATA_TYPE=""
START_DATE=""
END_DATE=""
PREFIX=""
ASSET_CLASS=""
ALL_DATASETS="false"
EXCLUDE_TYPES=""
EXCHANGES_INPUT=""
ASSET_FILENAME_PREFIX=""
DESTINATION="$HOME/massive_data"
PARALLEL_DOWNLOADS=8

# Default futures exchanges when --exchange is not specified.
DEFAULT_FUTURES_EXCHANGES=("cbot" "cme" "nymex" "comex")

OS_NAME="$(uname -s)"
DATE_STYLE="gnu"
DATE_BIN="date"
if [[ "$OS_NAME" == "Darwin" ]]; then
  if command -v gdate >/dev/null 2>&1; then
    DATE_BIN="gdate"
    DATE_STYLE="gnu"
  else
    DATE_BIN="date"
    DATE_STYLE="bsd"
  fi
fi

today_date() {
  TZ=UTC "$DATE_BIN" +%Y-%m-%d
}

date_to_epoch() {
  local d="$1"
  if [[ "$DATE_STYLE" == "gnu" ]]; then
    TZ=UTC "$DATE_BIN" -d "$d" +%s
  else
    TZ=UTC "$DATE_BIN" -j -f "%Y-%m-%d" "$d" +%s
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
      ALL_DATASETS="true"
      shift
      ;;
    --trades)
      DATA_TYPE="trades_v1"
      PREFIX="trades"
      shift
      ;;
    --quotes)
      DATA_TYPE="quotes_v1"
      PREFIX="quotes"
      shift
      ;;
    --bars-1m)
      DATA_TYPE="minute_aggs_v1"
      PREFIX="bars_1m"
      shift
      ;;
    --eod)
      DATA_TYPE="day_aggs_v1"
      PREFIX="eod"
      shift
      ;;
    --values)
      DATA_TYPE="values_v1"
      PREFIX="values"
      shift
      ;;
    --destination)
      DESTINATION="$2"
      shift 2
      ;;
    --exclude)
      EXCLUDE_TYPES="$2"
      shift 2
      ;;
    --exchange)
      EXCHANGES_INPUT="$2"
      shift 2
      ;;
    --parallel)
      PARALLEL_DOWNLOADS="$2"
      shift 2
      ;;
    --start-date)
      START_DATE="$2"
      shift 2
      ;;
    --end-date)
      END_DATE="$2"
      shift 2
      ;;
    --asset-class)
      ASSET_CLASS="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

if [[ -z "$START_DATE" || -z "$ASSET_CLASS" ]]; then
  usage
fi

# --end-date is optional; default to today (UTC, matching the date helpers above).
# Dates with no file on S3 yet (today before publish, weekends, holidays) are
# skipped by download_with_validation's 404 handling, not treated as failures.
if [[ -z "$END_DATE" ]]; then
  END_DATE="$(today_date)"
  echo "No --end-date given; defaulting to today (UTC): $END_DATE"
fi

if [[ "$ALL_DATASETS" == "true" && -n "$DATA_TYPE" ]]; then
  echo "Error: --all cannot be combined with a specific data type flag."
  exit 1
fi

if [[ -n "$EXCLUDE_TYPES" && "$ALL_DATASETS" != "true" ]]; then
  echo "Error: --exclude can only be used with --all."
  exit 1
fi

if [[ -n "$EXCHANGES_INPUT" && "$ASSET_CLASS" != "futures" ]]; then
  echo "Error: --exchange is only valid with --asset-class futures."
  exit 1
fi

if [[ ! "$PARALLEL_DOWNLOADS" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: --parallel '$PARALLEL_DOWNLOADS' is not a positive whole number."
  exit 1
fi

case "$ASSET_CLASS" in
  us_stocks_sip)
    ASSET_FILENAME_PREFIX="stocks"
    ;;
  us_options_opra)
    ASSET_FILENAME_PREFIX="options"
    ;;
  us_indices)
    ASSET_FILENAME_PREFIX="indices"
    ;;
  global_forex)
    ASSET_FILENAME_PREFIX="forex"
    ;;
  global_crypto)
    ASSET_FILENAME_PREFIX="crypto"
    ;;
  futures)
    ASSET_FILENAME_PREFIX="futures"
    ;;
  *)
    echo "Error: unsupported asset class '$ASSET_CLASS'. Use Massive S3 prefixes exactly."
    exit 1
    ;;
esac

# Resolve futures exchanges. Empty input → all four. Comma-separated, case-insensitive.
EXCHANGES=()
if [[ "$ASSET_CLASS" == "futures" ]]; then
  if [[ -z "$EXCHANGES_INPUT" ]]; then
    EXCHANGES=("${DEFAULT_FUTURES_EXCHANGES[@]}")
  else
    IFS=',' read -ra raw_exchanges <<< "$EXCHANGES_INPUT"
    for ex in "${raw_exchanges[@]}"; do
      ex_lower="$(echo "$ex" | xargs | tr '[:upper:]' '[:lower:]')"
      case "$ex_lower" in
        cbot|cme|nymex|comex)
          EXCHANGES+=("$ex_lower")
          ;;
        *)
          echo "Error: unknown futures exchange '$ex'. Valid: CBOT, CME, NYMEX, COMEX."
          exit 1
          ;;
      esac
    done
  fi
fi

# Exact YYYY-MM-DD: the range is compared as text against the listed file names, and must also be a real day.
for flag_date in "--start-date:$START_DATE" "--end-date:$END_DATE"; do
  if [[ ! "${flag_date#*:}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! date_to_epoch "${flag_date#*:}" >/dev/null 2>&1; then
    echo "Error: ${flag_date%%:*} '${flag_date#*:}' is not a YYYY-MM-DD date."
    exit 1
  fi
done
start_sec=$(date_to_epoch "$START_DATE")
end_sec=$(date_to_epoch "$END_DATE")

if (( end_sec < start_sec )); then
  echo "Error: end-date must be after or equal to start-date"
  exit 1
fi

datasets=()
if [[ "$ALL_DATASETS" == "true" ]]; then
  case "$ASSET_CLASS" in
    us_options_opra)
      datasets=("trades_v1:trades" "quotes_v1:quotes" "minute_aggs_v1:bars_1m" "day_aggs_v1:eod")
      ;;
    us_indices)
      datasets=("minute_aggs_v1:bars_1m" "day_aggs_v1:eod" "values_v1:values")
      ;;
    global_forex)
      datasets=("quotes_v1:quotes" "minute_aggs_v1:bars_1m" "day_aggs_v1:eod")
      ;;
    global_crypto)
      datasets=("trades_v1:trades" "minute_aggs_v1:bars_1m" "day_aggs_v1:eod")
      ;;
    futures)
      datasets=("trades_v1:trades" "quotes_v1:quotes" "minute_aggs_v1:bars_1m" "day_aggs_v1:eod")
      ;;
    *)
      datasets=("trades_v1:trades" "quotes_v1:quotes" "minute_aggs_v1:bars_1m" "day_aggs_v1:eod")
      ;;
  esac
else
  if [[ -z "$DATA_TYPE" ]]; then
    usage
  fi
  if [[ "$ASSET_CLASS" == "us_indices" && "$DATA_TYPE" != "minute_aggs_v1" && "$DATA_TYPE" != "day_aggs_v1" && "$DATA_TYPE" != "values_v1" ]]; then
    echo "Error: indices flat files support only minute aggregates, --eod, and values."
    exit 1
  fi
  if [[ "$ASSET_CLASS" == "global_forex" && "$DATA_TYPE" == "trades_v1" ]]; then
    echo "Error: forex flat files do not include trades; use --quotes or aggregates."
    exit 1
  fi
  if [[ "$ASSET_CLASS" == "global_crypto" && "$DATA_TYPE" == "quotes_v1" ]]; then
    echo "Error: crypto flat files do not include quotes; use --trades or aggregates."
    exit 1
  fi
  if [[ "$ASSET_CLASS" != "us_indices" && "$DATA_TYPE" == "values_v1" ]]; then
    echo "Error: --values is only supported for us_indices."
    exit 1
  fi
  if [[ "$ASSET_CLASS" == "futures" && "$DATA_TYPE" == "values_v1" ]]; then
    echo "Error: --values is not supported for futures."
    exit 1
  fi
  datasets=("${DATA_TYPE}:${PREFIX}")
fi

# Apply --exclude filter
if [[ -n "$EXCLUDE_TYPES" ]]; then
  # Map user-facing names to internal data_type prefixes
  declare -A EXCLUDE_MAP=(
    [trades]="trades_v1"
    [quotes]="quotes_v1"
    [bars-1m]="minute_aggs_v1"
    [eod]="day_aggs_v1"
    [values]="values_v1"
  )

  IFS=',' read -ra exclude_arr <<< "$EXCLUDE_TYPES"
  for ex in "${exclude_arr[@]}"; do
    ex="$(echo "$ex" | xargs)"  # trim whitespace
    if [[ -z "${EXCLUDE_MAP[$ex]+x}" ]]; then
      echo "Error: unknown exclude type '$ex'. Valid types: trades, quotes, bars-1m, eod, values"
      exit 1
    fi
    exclude_data_type="${EXCLUDE_MAP[$ex]}"
    filtered=()
    for ds in "${datasets[@]}"; do
      IFS=":" read -r dt pf <<< "$ds"
      if [[ "$dt" != "$exclude_data_type" ]]; then
        filtered+=("$ds")
      fi
    done
    datasets=("${filtered[@]}")
  done

  if [[ ${#datasets[@]} -eq 0 ]]; then
    echo "Error: all dataset types were excluded; nothing to download."
    exit 1
  fi
fi

# Futures uses per-exchange top-level prefixes
# (s3://flatfiles/us_futures_<exchange>/<data_type>/...) and renames the daily-bars
# segment from "day_aggs_v1" to "session_aggs_v1". All other data types match.
futures_data_type() {
  if [[ "$1" == "day_aggs_v1" ]]; then
    echo "session_aggs_v1"
  else
    echo "$1"
  fi
}

# Each failure is ONE write of its error line and aws's reason, so parallel workers never split the pair a
# reader matches on (a 403's status line follows its error line), and one line in FAILURES_FILE for the count.
record_failure() {
  local failed_path="$1" message="$2" reason="${3:-}"
  if [[ -n "$reason" ]]; then
    printf '%s\n%s\n' "$message" "${reason:0:$MAX_REASON_BYTES}"
  else
    printf '%s\n' "$message"
  fi
  printf '%s\n' "$failed_path" >> "$FAILURES_FILE"
}

# One listed object into place: a hidden temp file, checked against the listed size and for gzip integrity,
# then renamed, so a partial or corrupt download never takes the final name. Always returns 0: failures are
# counted through record_failure, and a non-zero xargs exit then means a worker died, not a download failed.
download_with_validation() {
  local s3_path="$1" local_file="$2" listed_size="$3"
  local base tmp_file reason local_size
  base="$(basename "$local_file")"
  tmp_file="$(dirname "$local_file")/.${base%.csv.gz}.tmp.csv.gz"

  echo "Downloading $s3_path -> $local_file"
  if ! reason="$(aws s3 cp "$s3_path" "$tmp_file" $ENDPOINT --only-show-errors 2>&1)"; then
    # Glob "${tmp_file}".* mops up aws-cli multipart intermediates with random suffixes.
    rm -f "$tmp_file" "$tmp_file".*
    record_failure "$s3_path" "Error: aws s3 cp failed for $s3_path" "$reason"
    return 0
  fi

  local_size="$(wc -c < "$tmp_file" | tr -d ' ')"
  if [[ "$local_size" != "$listed_size" ]]; then
    rm -f "$tmp_file"
    record_failure "$s3_path" "Error: size mismatch for $s3_path (S3 lists $listed_size, local is $local_size)"
    return 0
  fi

  if ! gzip -t "$tmp_file" 2>/dev/null; then
    rm -f "$tmp_file"
    record_failure "$s3_path" "Error: gzip integrity check failed for $s3_path; removed corrupt temp file."
    return 0
  fi

  if ! reason="$(mv -f "$tmp_file" "$local_file" 2>&1)"; then
    rm -f "$tmp_file"
    record_failure "$s3_path" "Error: could not rename the download into place as $local_file" "$reason"
    return 0
  fi
  echo "Downloaded $local_file ($local_size bytes)"
}

# "size key" for every dated object under one dataset prefix inside [START_DATE, END_DATE], listing one year
# prefix at a time because Massive keys files <prefix>/YYYY/MM/YYYY-MM-DD.csv.gz. Errors go to stderr: stdout
# is the listing its caller reads.
list_dated_objects() {
  local s3_prefix="$1" year listing stderr_file
  stderr_file="$(mktemp)"
  for (( year = ${START_DATE:0:4}; year <= ${END_DATE:0:4}; year++ )); do
    if ! listing="$(aws s3 ls "s3://flatfiles/${s3_prefix}/${year}/" --recursive $ENDPOINT 2>"$stderr_file")"; then
      # A prefix holding no objects also exits non-zero, but says nothing.
      if [[ -s "$stderr_file" ]]; then
        record_failure "s3://flatfiles/${s3_prefix}/${year}/" \
          "Error: aws s3 ls failed for s3://flatfiles/${s3_prefix}/${year}/" "$(cat "$stderr_file")" >&2
      fi
      continue
    fi
    awk -v from="$START_DATE" -v to="$END_DATE" '
      { n = split($4, parts, "/"); name = parts[n]; day = substr(name, 1, 10) }
      name ~ /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]\.csv\.gz$/ && day >= from && day <= to { print $3, $4 }
    ' <<< "$listing"
  done
  rm -f "$stderr_file"
}

# Appends every listed file of one dataset (and exchange) that is not already on disk at its listed size to
# JOBS_FILE as NUL-separated "s3 path, local file, listed size" triples, in listing (oldest first) order.
queue_downloads() {
  local data_type="$1" prefix="$2" exchange="$3"
  local s3_prefix output_dir name_prefix size key day local_file listed=0
  if [[ -n "$exchange" ]]; then
    s3_prefix="us_futures_${exchange}/$(futures_data_type "$data_type")"
    output_dir="${DESTINATION}/${ASSET_CLASS}/${prefix}/${exchange}"
    name_prefix="${ASSET_FILENAME_PREFIX}_${prefix}_${exchange}"
  else
    s3_prefix="${ASSET_CLASS}/${data_type}"
    output_dir="${DESTINATION}/${ASSET_CLASS}/${prefix}"
    name_prefix="${ASSET_FILENAME_PREFIX}_${prefix}"
  fi
  mkdir -p "$output_dir"

  while read -r size key; do
    listed=$((listed + 1))
    day="${key##*/}"
    local_file="${output_dir}/${name_prefix}_${day}"
    if [[ -f "$local_file" && "$(wc -c < "$local_file" | tr -d ' ')" == "$size" ]]; then
      already_on_disk=$((already_on_disk + 1))
      continue
    fi
    printf '%s\0%s\0%s\0' "s3://flatfiles/${key}" "$local_file" "$size" >> "$JOBS_FILE"
    queued=$((queued + 1))
  done < <(list_dated_objects "$s3_prefix")

  if (( listed == 0 )); then
    echo "Warning: Massive lists no files under s3://flatfiles/${s3_prefix}/ from $START_DATE to $END_DATE"
  fi
}

JOBS_FILE="$(mktemp)"
FAILURES_FILE="$(mktemp)"
trap 'rm -f "$JOBS_FILE" "$FAILURES_FILE"' EXIT
queued=0
already_on_disk=0

for dataset in "${datasets[@]}"; do
  IFS=":" read -r data_type prefix <<< "$dataset"
  if [[ "$ASSET_CLASS" == "futures" ]]; then
    for exchange in "${EXCHANGES[@]}"; do
      queue_downloads "$data_type" "$prefix" "$exchange"
    done
  else
    queue_downloads "$data_type" "$prefix" ""
  fi
done

echo "Queued $queued file(s) to download, $PARALLEL_DOWNLOADS at a time; $already_on_disk already on disk at the listed size."
if (( queued > 0 )); then
  export -f download_with_validation record_failure
  export ENDPOINT FAILURES_FILE MAX_REASON_BYTES
  if ! xargs -0 -n 3 -P "$PARALLEL_DOWNLOADS" bash -c 'download_with_validation "$@"' _ < "$JOBS_FILE"; then
    echo "Error: a download worker ended abnormally; files it held are neither downloaded nor counted below."
    printf '%s\n' "download worker" >> "$FAILURES_FILE"
  fi
fi

failure_count="$(wc -l < "$FAILURES_FILE" | tr -d ' ')"
if (( failure_count > 0 )); then
  echo "Done with $failure_count download error(s). Check messages above."
  exit 1
fi
