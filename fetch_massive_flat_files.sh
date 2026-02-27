#!/bin/bash

# Example usage:
# ./fetch_massive_flat_files.sh --days --start-date "2025-05-12" --end-date "2025-05-13" --asset-class us_stocks_sip
# ./fetch_massive_flat_files.sh --trades --start-date "2025-05-12" --end-date "2025-05-13" --asset-class us_options_opra
# ./fetch_massive_flat_files.sh --all --start-date "2025-05-12" --end-date "2025-05-13" --asset-class global_forex
# ./fetch_massive_flat_files.sh --all --start-date "2025-05-12" --end-date "2025-05-13" --asset-class global_crypto
# ./fetch_massive_flat_files.sh --bars-1m --start-date "2025-05-12" --end-date "2025-05-13" --asset-class us_indices
# ./fetch_massive_flat_files.sh --values --start-date "2025-05-12" --end-date "2025-05-13" --asset-class us_indices

if [[ -z "$MASSIVE_FLAT_FILE_AWS_KEY" || -z "$MASSIVE_FLAT_FILE_AWS_SECRET_KEY" ]]; then
  echo "Error: MASSIVE_FLAT_FILE_AWS_KEY and MASSIVE_FLAT_FILE_AWS_SECRET_KEY must be set"
  exit 1
fi

ENDPOINT="--endpoint-url https://files.massive.com"

usage() {
  echo "Usage: $0 --trades|--quotes|--bars-1m|--days|--values|--all --start-date YYYY-MM-DD --end-date YYYY-MM-DD --asset-class ASSET_CLASS"
  echo ""
  echo "Asset class examples:"
  echo "  us_stocks_sip, us_options_opra, us_indices"
  echo "  global_forex, global_crypto"
  echo ""
  echo "Notes:"
  echo "  - Indices flat files support minute aggregates, day aggregates, and values."
  echo "  - Forex flat files support quotes and minute/day aggregates only."
  echo "  - Crypto flat files support trades and minute/day aggregates only."
  echo "  - Futures flat files are in beta; S3 prefixes are not published in docs yet."
  exit 1
}

DATA_TYPE=""
START_DATE=""
END_DATE=""
PREFIX=""
ASSET_CLASS=""
ALL_DATASETS="false"
ASSET_FILENAME_PREFIX=""

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
    --days)
      DATA_TYPE="day_aggs_v1"
      PREFIX="eod"
      shift
      ;;
    --values)
      DATA_TYPE="values_v1"
      PREFIX="values"
      shift
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

if [[ "$ASSET_CLASS" == "futures" ]]; then
  echo "Error: futures flat files are in beta and Massive docs do not publish S3 prefixes yet."
  exit 1
fi

BASE_PATH="s3://flatfiles/${ASSET_CLASS}"

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
    *)
      datasets=("trades_v1:trades" "quotes_v1:quotes" "minute_aggs_v1:bars_1m" "day_aggs_v1:eod")
      ;;
  esac
else
  if [[ -z "$DATA_TYPE" ]]; then
    usage
  fi
  if [[ "$ASSET_CLASS" == "us_indices" && "$DATA_TYPE" != "minute_aggs_v1" && "$DATA_TYPE" != "day_aggs_v1" && "$DATA_TYPE" != "values_v1" ]]; then
    echo "Error: indices flat files support only minute aggregates, day aggregates, and values."
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
  datasets=("${DATA_TYPE}:${PREFIX}")
fi

for dataset in "${datasets[@]}"; do
  IFS=":" read -r data_type prefix <<< "$dataset"
  output_dir="$HOME/massive_data/${ASSET_CLASS}/${prefix}"
  mkdir -p "$output_dir"

  current_sec=$start_sec
  while (( current_sec <= end_sec )); do
    current_date=$(epoch_to_date "$current_sec")
    year=$(epoch_to_year "$current_sec")
    month=$(epoch_to_month "$current_sec")

    s3_path="$BASE_PATH/$data_type/$year/$month/$current_date.csv.gz"
    local_file="$output_dir/${ASSET_FILENAME_PREFIX}_${prefix}_${current_date}.csv.gz"

    echo "Downloading $s3_path as $local_file"
    aws s3 cp "$s3_path" "$local_file" $ENDPOINT
    current_sec=$((current_sec + 86400))
  done
done
