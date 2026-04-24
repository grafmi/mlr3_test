#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="${CONFIG_PATH:-$SCRIPT_DIR/configs/base_config.R}"

CONFIG_RESULTS_ROOT="${CONFIG_RESULTS_ROOT:-results}"
VERSION_RUNS="${VERSION_RUNS:-true}"
RUN_ID="${RUN_ID:-}"
RESULTS_ROOT_DIR="${RESULTS_ROOT_DIR:-$SCRIPT_DIR/$CONFIG_RESULTS_ROOT}"
RUN_OUTPUT_ROOT="${RUN_OUTPUT_ROOT:-}"

next_available_run_id() {
  local base_id="$1"
  local root_dir="$2"
  local candidate="$base_id"
  local suffix=1

  while [[ -e "$root_dir/$candidate" ]]; do
    candidate="$(printf '%s_%02d' "$base_id" "$suffix")"
    suffix=$((suffix + 1))
  done

  printf '%s\n' "$candidate"
}

if [[ -z "$RUN_ID" && "$VERSION_RUNS" == "true" ]]; then
  RUN_ID="$(date '+%Y%m%d_%H%M')"
fi

if [[ "$VERSION_RUNS" == "true" ]]; then
  if [[ -z "$RUN_OUTPUT_ROOT" ]]; then
    mkdir -p "$RESULTS_ROOT_DIR"
    RUN_ID="$(next_available_run_id "$RUN_ID" "$RESULTS_ROOT_DIR")"
    RUN_OUTPUT_ROOT="$RESULTS_ROOT_DIR/$RUN_ID"
  fi
else
  if [[ -z "$RUN_OUTPUT_ROOT" ]]; then
    RUN_OUTPUT_ROOT="$SCRIPT_DIR"
  fi
fi

MLR3_DATA_PATH="${MLR3_DATA_PATH:-$SCRIPT_DIR/testfile_zinb_nonlinear_eintritte.csv}"
RANGER_OUTPUT_DIR="${RANGER_OUTPUT_DIR:-$RUN_OUTPUT_ROOT/outputs_ranger}"
XGB_OUTPUT_DIR="${XGB_OUTPUT_DIR:-$RUN_OUTPUT_ROOT/outputs_xgb}"
ZINB_OUTPUT_DIR="${ZINB_OUTPUT_DIR:-$RUN_OUTPUT_ROOT/outputs_zinb}"
COMPARISON_OUTPUT_DIR="${COMPARISON_OUTPUT_DIR:-$RUN_OUTPUT_ROOT/outputs_model_comparison}"
RUN_SUMMARY_OUTPUT_DIR="${RUN_SUMMARY_OUTPUT_DIR:-$RUN_OUTPUT_ROOT}"

run_step() {
  local label="$1"
  shift
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$label"
  "$@"
}

if ! command -v Rscript >/dev/null 2>&1; then
  echo "Error: Rscript was not found in PATH." >&2
  exit 1
fi

if [[ ! -f "$MLR3_DATA_PATH" ]]; then
  echo "Error: Data file not found: $MLR3_DATA_PATH" >&2
  exit 1
fi

export RANGER_OUTPUT_DIR
export XGB_OUTPUT_DIR
export ZINB_OUTPUT_DIR
export COMPARISON_OUTPUT_DIR
export MLR3_DATA_PATH
export RUN_OUTPUT_ROOT
export RUN_SUMMARY_OUTPUT_DIR
export CONFIG_PATH

echo "Repository directory: $SCRIPT_DIR"
echo "Config file: $CONFIG_PATH"
echo "Data file: $MLR3_DATA_PATH"
echo "Version runs: $VERSION_RUNS"
if [[ "$VERSION_RUNS" == "true" ]]; then
  echo "Run ID: $RUN_ID"
fi
echo "Run output root: $RUN_OUTPUT_ROOT"
echo "Ranger output: $RANGER_OUTPUT_DIR"
echo "XGBoost output: $XGB_OUTPUT_DIR"
echo "ZINB output: $ZINB_OUTPUT_DIR"
echo "Comparison output: $COMPARISON_OUTPUT_DIR"
echo "Run summary output: $RUN_SUMMARY_OUTPUT_DIR"

run_step "Running ranger tuning" Rscript "$SCRIPT_DIR/mlr3_ranger_tuning.R"
run_step "Running xgboost tuning" Rscript "$SCRIPT_DIR/mlr3_xgb_tuning.R"
run_step "Running ZINB stepwise CV" Rscript "$SCRIPT_DIR/zinb_stepwise_cv.R"
run_step "Comparing best models" Rscript "$SCRIPT_DIR/compare_best_models.R"
run_step "Writing run summary" Rscript "$SCRIPT_DIR/write_run_summary.R"

printf '\n[%s] Full run completed successfully.\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')"
