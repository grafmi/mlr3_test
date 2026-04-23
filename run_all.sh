#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

MLR3_DATA_PATH="${MLR3_DATA_PATH:-$SCRIPT_DIR/testfile_zinb_nonlinear_eintritte.csv}"
RANGER_OUTPUT_DIR="${RANGER_OUTPUT_DIR:-$SCRIPT_DIR/outputs_ranger}"
XGB_OUTPUT_DIR="${XGB_OUTPUT_DIR:-$SCRIPT_DIR/outputs_xgb}"
ZINB_OUTPUT_DIR="${ZINB_OUTPUT_DIR:-$SCRIPT_DIR/outputs_zinb}"
COMPARISON_OUTPUT_DIR="${COMPARISON_OUTPUT_DIR:-$SCRIPT_DIR/outputs_model_comparison}"

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

echo "Repository directory: $SCRIPT_DIR"
echo "Data file: $MLR3_DATA_PATH"
echo "Ranger output: $RANGER_OUTPUT_DIR"
echo "XGBoost output: $XGB_OUTPUT_DIR"
echo "ZINB output: $ZINB_OUTPUT_DIR"
echo "Comparison output: $COMPARISON_OUTPUT_DIR"

run_step "Running ranger tuning" Rscript "$SCRIPT_DIR/mlr3_ranger_tuning.R"
run_step "Running xgboost tuning" Rscript "$SCRIPT_DIR/mlr3_xgb_tuning.R"
run_step "Running ZINB stepwise CV" Rscript "$SCRIPT_DIR/zinb_stepwise_cv.R"
run_step "Comparing best models" Rscript "$SCRIPT_DIR/compare_best_models.R"

printf '\n[%s] Full run completed successfully.\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')"
