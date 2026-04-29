#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="${CONFIG_PATH:-$SCRIPT_DIR/configs/base_config.R}"

if ! command -v Rscript >/dev/null 2>&1; then
  echo "Error: Rscript was not found in PATH." >&2
  exit 1
fi

config_query_all() {
  Rscript - "$SCRIPT_DIR" "$CONFIG_PATH" <<'RS'
args <- commandArgs(trailingOnly = TRUE)
repo_dir <- args[[1]]
config_path <- args[[2]]

sys.source(file.path(repo_dir, "experiment_utils.R"), envir = environment())
Sys.setenv(CONFIG_PATH = config_path)
config <- load_project_config(repo_dir)

values <- list(
  data_path = resolve_path(config_value(config, c("experiment", "data_path")), repo_dir),
  results_root_dir = resolve_path(config_value_or(config, c("results", "root_dir"), "results"), repo_dir),
  version_runs = tolower(as.character(config_value_or(config, c("results", "version_runs"), TRUE))),
  run_name = {
    run_name <- normalize_optional_string(config_value_or(config, c("results", "run_name"), ""))
    if (is.na(run_name)) "" else run_name
  }
)
for (nm in names(values)) {
  cat(nm, "=", values[[nm]], "\n", sep = "")
}
RS
}

CONFIG_DATA_PATH_VALUE=""
CONFIG_RESULTS_ROOT_DIR_VALUE=""
CONFIG_VERSION_RUNS_VALUE=""
CONFIG_RUN_NAME_VALUE=""
while IFS='=' read -r key value; do
  case "$key" in
    data_path) CONFIG_DATA_PATH_VALUE="$value" ;;
    results_root_dir) CONFIG_RESULTS_ROOT_DIR_VALUE="$value" ;;
    version_runs) CONFIG_VERSION_RUNS_VALUE="$value" ;;
    run_name) CONFIG_RUN_NAME_VALUE="$value" ;;
  esac
done < <(config_query_all)

CONFIG_RESULTS_ROOT_DIR="${CONFIG_RESULTS_ROOT_DIR:-$CONFIG_RESULTS_ROOT_DIR_VALUE}"
VERSION_RUNS="${VERSION_RUNS:-$CONFIG_VERSION_RUNS_VALUE}"
RUN_NAME="${RUN_NAME:-$CONFIG_RUN_NAME_VALUE}"
RUN_ID="${RUN_ID:-}"
RESULTS_ROOT_DIR="${RESULTS_ROOT_DIR:-$CONFIG_RESULTS_ROOT_DIR}"
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

MLR3_DATA_PATH="${MLR3_DATA_PATH:-$CONFIG_DATA_PATH_VALUE}"
RANGER_OUTPUT_DIR="${RANGER_OUTPUT_DIR:-$RUN_OUTPUT_ROOT/outputs_ranger}"
XGB_OUTPUT_DIR="${XGB_OUTPUT_DIR:-$RUN_OUTPUT_ROOT/outputs_xgb}"
ZINB_OUTPUT_DIR="${ZINB_OUTPUT_DIR:-$RUN_OUTPUT_ROOT/outputs_zinb}"
COMPARISON_OUTPUT_DIR="${COMPARISON_OUTPUT_DIR:-$RUN_OUTPUT_ROOT/outputs_model_comparison}"
VALIDATION_OUTPUT_DIR="${VALIDATION_OUTPUT_DIR:-$RUN_OUTPUT_ROOT/outputs_validation}"
RUN_SUMMARY_OUTPUT_DIR="${RUN_SUMMARY_OUTPUT_DIR:-$RUN_OUTPUT_ROOT}"

run_step() {
  local label="$1"
  shift
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$label"
  "$@"
}

if [[ ! -f "$MLR3_DATA_PATH" ]]; then
  echo "Error: Data file not found: $MLR3_DATA_PATH" >&2
  exit 1
fi

export RANGER_OUTPUT_DIR
export XGB_OUTPUT_DIR
export ZINB_OUTPUT_DIR
export COMPARISON_OUTPUT_DIR
export VALIDATION_OUTPUT_DIR
export MLR3_DATA_PATH
export RUN_OUTPUT_ROOT
export RUN_SUMMARY_OUTPUT_DIR
export CONFIG_PATH
export RUN_NAME

echo "Repository directory: $SCRIPT_DIR"
echo "Config file: $CONFIG_PATH"
echo "Data file: $MLR3_DATA_PATH"
echo "Version runs: $VERSION_RUNS"
if [[ -n "$RUN_NAME" ]]; then
  echo "Run name: $RUN_NAME"
fi
if [[ "$VERSION_RUNS" == "true" ]]; then
  echo "Run ID: $RUN_ID"
fi
echo "Run output root: $RUN_OUTPUT_ROOT"
echo "Ranger output: $RANGER_OUTPUT_DIR"
echo "XGBoost output: $XGB_OUTPUT_DIR"
echo "ZINB output: $ZINB_OUTPUT_DIR"
echo "Comparison output: $COMPARISON_OUTPUT_DIR"
echo "Validation output: $VALIDATION_OUTPUT_DIR"
echo "Run summary output: $RUN_SUMMARY_OUTPUT_DIR"

run_step "Running repository validation" Rscript "$SCRIPT_DIR/validate_repo.R"
run_step "Running ranger tuning" Rscript "$SCRIPT_DIR/mlr3_ranger_tuning.R"
run_step "Running xgboost tuning" Rscript "$SCRIPT_DIR/mlr3_xgb_tuning.R"
run_step "Running ZINB stepwise CV" Rscript "$SCRIPT_DIR/zinb_stepwise_cv.R"
run_step "Comparing best models" Rscript "$SCRIPT_DIR/compare_best_models.R"
run_step "Writing run summary" Rscript "$SCRIPT_DIR/write_run_summary.R"

printf '\n[%s] Full run completed successfully.\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')"
