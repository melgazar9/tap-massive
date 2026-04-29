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

ENDPOINT="--endpoint-url https://files.massive.com"

usage() {
  echo "Usage: $0 --trades|--quotes|--bars-1m|--eod|--values|--all [--exclude TYPE,...] [--exchange LIST] [--destination DIR] --start-date YYYY-MM-DD --end-date YYYY-MM-DD --asset-class ASSET_CLASS"
  echo ""
  echo "Asset class examples:"
  echo "  us_stocks_sip, us_options_opra, us_indices"
  echo "  global_forex, global_crypto, futures"
  echo ""
  echo "Options:"
  echo "  --destination DIR   Base directory for downloads (default: \$HOME/massive_data)"
  echo "  --exclude TYPE,...  Exclude dataset types when using --all."
  echo "                      Valid types: trades, quotes, bars-1m, eod, values"
  echo "                      Example: --all --exclude quotes,trades"
  echo "  --exchange LIST     Comma-separated futures exchanges (CBOT,CME,NYMEX,COMEX)."
  echo "                      Only valid with --asset-class futures. Default: all four."
  echo ""
  echo "Notes:"
  echo "  - Indices flat files support minute aggregates, day aggregates (--eod), and values."
  echo "  - Forex flat files support quotes and minute/eod aggregates only."
  echo "  - Crypto flat files support trades and minute/eod aggregates only."
  echo "  - Futures flat files support trades, quotes, --bars-1m (minute-aggregates), and"
  echo "    --eod (session-aggregates) per exchange (CBOT, CME, NYMEX, COMEX)."
  echo "  - All downloads are validated (gzip integrity check) before being renamed from"
  echo "    .<file>.tmp to <file> to avoid leaving corrupt files in place."
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

date_to_epoch() {
  local d="$1"
  if [[ "$DATE_STYLE" == "gnu" ]]; then
    TZ=UTC "$DATE_BIN" -d "$d" +%s
  else
    TZ=UTC "$DATE_BIN" -j -f "%Y-%m-%d" "$d" +%s
  fi
}

epoch_to_date() {
  local s="$1"
  if [[ "$DATE_STYLE" == "gnu" ]]; then
    TZ=UTC "$DATE_BIN" -d "@$s" +%Y-%m-%d
  else
    TZ=UTC "$DATE_BIN" -r "$s" +%Y-%m-%d
  fi
}

epoch_to_year() {
  local s="$1"
  if [[ "$DATE_STYLE" == "gnu" ]]; then
    TZ=UTC "$DATE_BIN" -d "@$s" +%Y
  else
    TZ=UTC "$DATE_BIN" -r "$s" +%Y
  fi
}

epoch_to_month() {
  local s="$1"
  if [[ "$DATE_STYLE" == "gnu" ]]; then
    TZ=UTC "$DATE_BIN" -d "@$s" +%m
  else
    TZ=UTC "$DATE_BIN" -r "$s" +%m
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

if [[ -z "$START_DATE" || -z "$END_DATE" || -z "$ASSET_CLASS" ]]; then
  usage
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

# For futures, the actual S3 prefix is per-exchange (s3://flatfiles/us_futures_<exchange>/...),
# so BASE_PATH is rebuilt inside the per-exchange loop below.
if [[ "$ASSET_CLASS" != "futures" ]]; then
  BASE_PATH="s3://flatfiles/${ASSET_CLASS}"
fi

start_sec=$(date_to_epoch "$START_DATE")
end_sec=$(date_to_epoch "$END_DATE")

if (( end_sec < start_sec )); then
  echo "Error: end-date must be after or equal to start-date"
  exit 1
fi

current_sec=$start_sec

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

# Download a single S3 object with validation:
#   1. Write to a hidden temp file (.<stem>.tmp.csv.gz) in the destination dir.
#   2. Verify gzip integrity (gzip -t).
#   3. Atomic rename to the final filename only on success.
# A partial or corrupt download never appears as a final filename.
# Return codes: 0 = success, 1 = real failure, 2 = file not on S3 (skip).
download_with_validation() {
  local s3_path="$1"
  local local_file="$2"

  local dir base stem tmp_file stderr_file
  dir="$(dirname "$local_file")"
  base="$(basename "$local_file")"
  stem="${base%.csv.gz}"
  tmp_file="${dir}/.${stem}.tmp.csv.gz"
  # Glob "${tmp_file}.*" mops up aws-cli multipart intermediates with random suffixes.
  cleanup_tmp() { rm -f "$tmp_file" "$tmp_file".*; }

  mkdir -p "$dir"

  echo "Downloading $s3_path -> $local_file"
  stderr_file="$(mktemp)"
  if ! aws s3 cp "$s3_path" "$tmp_file" $ENDPOINT 2>"$stderr_file"; then
    local stderr_content
    stderr_content="$(cat "$stderr_file")"
    rm -f "$stderr_file"
    cleanup_tmp
    # 404 / NoSuchKey = expected (weekend, holiday, future date); not a failure.
    if echo "$stderr_content" | grep -qE "404|NoSuchKey|does not exist"; then
      echo "Skip: $s3_path (no file on S3 for this date)"
      return 2
    fi
    echo "Error: aws s3 cp failed for $s3_path"
    echo "$stderr_content" >&2
    return 1
  fi
  rm -f "$stderr_file"

  if [[ ! -s "$tmp_file" ]]; then
    echo "Error: empty file after download for $s3_path"
    cleanup_tmp
    return 1
  fi

  if ! gzip -t "$tmp_file" 2>/dev/null; then
    echo "Error: gzip integrity check failed for $s3_path; removed corrupt temp file."
    cleanup_tmp
    return 1
  fi

  # Source-destination size check. Verifies the local file's byte count matches
  # what S3 reports for the object. Catches the rare case where aws s3 cp
  # returns 0 but the local file diverges from the canonical S3 object
  # (e.g., partial multipart that happened to land on a valid gzip boundary).
  local s3_no_proto="${s3_path#s3://}"
  local bucket="${s3_no_proto%%/*}"
  local key="${s3_no_proto#*/}"
  local expected_size local_size
  expected_size="$(aws s3api head-object --bucket "$bucket" --key "$key" $ENDPOINT --query 'ContentLength' --output text 2>/dev/null)"
  local_size="$(wc -c < "$tmp_file" | tr -d ' ')"
  if [[ -z "$expected_size" || "$expected_size" == "None" ]]; then
    echo "Warning: could not fetch ContentLength from S3 for $s3_path; skipping size check."
  elif [[ "$expected_size" != "$local_size" ]]; then
    echo "Error: size mismatch for $s3_path (S3 says $expected_size, local is $local_size)"
    cleanup_tmp
    return 1
  fi

  mv "$tmp_file" "$local_file"
  return 0
}

failure_count=0

for dataset in "${datasets[@]}"; do
  IFS=":" read -r data_type prefix <<< "$dataset"

  if [[ "$ASSET_CLASS" == "futures" ]]; then
    futures_dt="$(futures_data_type "$data_type")"
    exchanges_to_iterate=("${EXCHANGES[@]}")
  else
    exchanges_to_iterate=("")  # single sentinel iteration for non-futures
  fi

  for exchange in "${exchanges_to_iterate[@]}"; do
    if [[ "$ASSET_CLASS" == "futures" ]]; then
      output_dir="${DESTINATION}/${ASSET_CLASS}/${prefix}/${exchange}"
    else
      output_dir="${DESTINATION}/${ASSET_CLASS}/${prefix}"
    fi
    mkdir -p "$output_dir"

    current_sec=$start_sec
    while (( current_sec <= end_sec )); do
      current_date=$(epoch_to_date "$current_sec")
      year=$(epoch_to_year "$current_sec")
      month=$(epoch_to_month "$current_sec")

      if [[ "$ASSET_CLASS" == "futures" ]]; then
        s3_path="s3://flatfiles/us_futures_${exchange}/$futures_dt/$year/$month/$current_date.csv.gz"
        local_file="$output_dir/${ASSET_FILENAME_PREFIX}_${prefix}_${exchange}_${current_date}.csv.gz"
      else
        s3_path="$BASE_PATH/$data_type/$year/$month/$current_date.csv.gz"
        local_file="$output_dir/${ASSET_FILENAME_PREFIX}_${prefix}_${current_date}.csv.gz"
      fi

      download_with_validation "$s3_path" "$local_file"
      rc=$?
      # rc=0 success, rc=2 file-not-on-S3 (skip), rc=1 real failure.
      if (( rc == 1 )); then
        failure_count=$((failure_count + 1))
      fi
      current_sec=$((current_sec + 86400))
    done
  done
done

if (( failure_count > 0 )); then
  echo "Done with $failure_count download error(s). Check messages above."
  exit 1
fi
