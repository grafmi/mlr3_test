#!/usr/bin/env Rscript

source_experiment_utils <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", cmd_args, value = TRUE)
  script_dir <- if (length(file_arg) > 0) {
    dirname(normalizePath(sub("^--file=", "", file_arg[1]), mustWork = TRUE))
  } else if (length(Filter(Negate(is.null), lapply(sys.frames(), function(x) x$ofile))) > 0) {
    ofiles <- Filter(Negate(is.null), lapply(sys.frames(), function(x) x$ofile))
    dirname(normalizePath(ofiles[[length(ofiles)]], mustWork = TRUE))
  } else {
    getwd()
  }
  candidates <- unique(c(file.path(script_dir, "experiment_utils.R"), file.path(getwd(), "experiment_utils.R")))
  for (path in candidates) {
    if (file.exists(path)) {
      source(path)
      return(normalizePath(path, mustWork = TRUE))
    }
  }
  stop("Could not find experiment_utils.R. Run from the repository root or keep it next to this script.")
}

UTILS_PATH <- source_experiment_utils()
REPO_DIR <- dirname(UTILS_PATH)

require_packages(c("data.table"))
suppressPackageStartupMessages(library(data.table))

RUN_OUTPUT_ROOT <- get_path_setting("run-output-root", "RUN_OUTPUT_ROOT", getwd(), base_dir = REPO_DIR)
RUN_ID <- basename(normalizePath(RUN_OUTPUT_ROOT, mustWork = FALSE))
OUTPUT_DIR <- get_path_setting("output-dir", "RUN_SUMMARY_OUTPUT_DIR", RUN_OUTPUT_ROOT, base_dir = REPO_DIR)

read_manifest_with_label <- function(output_dir, script_label) {
  manifest <- read_csv_if_exists(file.path(output_dir, "run_manifest.csv"))
  if (is.null(manifest) || nrow(manifest) == 0) {
    return(data.table(
      script_name = script_label,
      status = NA_character_,
      start_time = NA_character_,
      end_time = NA_character_,
      runtime_seconds = NA_real_
    ))
  }

  manifest[, .(
    script_name,
    status,
    start_time,
    end_time,
    runtime_seconds
  )][1]
}

dirs <- list(
  ranger = get_path_setting("ranger-dir", "RANGER_OUTPUT_DIR", file.path(RUN_OUTPUT_ROOT, "outputs_ranger"), base_dir = REPO_DIR),
  xgb = get_path_setting("xgb-dir", "XGB_OUTPUT_DIR", file.path(RUN_OUTPUT_ROOT, "outputs_xgb"), base_dir = REPO_DIR),
  zinb = get_path_setting("zinb-dir", "ZINB_OUTPUT_DIR", file.path(RUN_OUTPUT_ROOT, "outputs_zinb"), base_dir = REPO_DIR),
  comparison = get_path_setting("comparison-dir", "COMPARISON_OUTPUT_DIR", file.path(RUN_OUTPUT_ROOT, "outputs_model_comparison"), base_dir = REPO_DIR)
)

script_summary <- rbindlist(list(
  cbind(read_manifest_with_label(dirs$ranger, "mlr3_ranger_tuning"), data.table(output_dir = normalizePath(dirs$ranger, mustWork = FALSE))),
  cbind(read_manifest_with_label(dirs$xgb, "mlr3_xgb_tuning"), data.table(output_dir = normalizePath(dirs$xgb, mustWork = FALSE))),
  cbind(read_manifest_with_label(dirs$zinb, "zinb_stepwise_cv"), data.table(output_dir = normalizePath(dirs$zinb, mustWork = FALSE))),
  cbind(read_manifest_with_label(dirs$comparison, "compare_best_models"), data.table(output_dir = normalizePath(dirs$comparison, mustWork = FALSE)))
), fill = TRUE)

comparison <- read_csv_if_exists(file.path(dirs$comparison, "best_models_comparison.csv"))
winner_row <- if (!is.null(comparison) && nrow(comparison) > 0) comparison[!is.na(rank)][order(rank)][1] else NULL

run_status <- if (all(script_summary$status == "completed", na.rm = TRUE) && all(!is.na(script_summary$status))) {
  "completed"
} else if (any(script_summary$status == "failed", na.rm = TRUE)) {
  "failed"
} else {
  "partial"
}

run_start <- suppressWarnings(min(as.POSIXct(script_summary$start_time, tz = Sys.timezone()), na.rm = TRUE))
run_end <- suppressWarnings(max(as.POSIXct(script_summary$end_time, tz = Sys.timezone()), na.rm = TRUE))
has_run_start <- !is.na(as.numeric(run_start))
has_run_end <- !is.na(as.numeric(run_end))
run_runtime <- if (has_run_start && has_run_end) as.numeric(difftime(run_end, run_start, units = "secs")) else NA_real_

summary_dt <- data.table(
  run_id = RUN_ID,
  run_output_root = normalizePath(RUN_OUTPUT_ROOT, mustWork = FALSE),
  run_status = run_status,
  started_at = if (has_run_start) format(run_start, "%Y-%m-%d %H:%M:%S %Z") else NA_character_,
  ended_at = if (has_run_end) format(run_end, "%Y-%m-%d %H:%M:%S %Z") else NA_character_,
  runtime_seconds = run_runtime,
  winner_model = if (is.null(winner_row)) NA_character_ else winner_row$model[[1]],
  winner_metric = if (is.null(winner_row)) NA_character_ else "rank_1",
  winner_rmse = if (is.null(winner_row)) NA_real_ else winner_row$rmse[[1]],
  winner_mae = if (is.null(winner_row)) NA_real_ else winner_row$mae[[1]],
  winner_r2 = if (is.null(winner_row)) NA_real_ else winner_row$r2[[1]]
)

safe_write_csv(summary_dt, file.path(OUTPUT_DIR, "run_summary.csv"))
safe_write_csv(script_summary, file.path(OUTPUT_DIR, "run_summary_scripts.csv"))
