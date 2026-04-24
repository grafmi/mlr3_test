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
CONFIG <- load_project_config(REPO_DIR)
CONFIG_PATH <- get_project_config_path(REPO_DIR)
RUN_NAME <- get_run_name_setting(CONFIG)

require_packages(c("data.table"))
suppressPackageStartupMessages(library(data.table))

RUN_OUTPUT_ROOT <- get_path_setting("run-output-root", "RUN_OUTPUT_ROOT", getwd(), base_dir = REPO_DIR)
RUN_ID <- basename(normalizePath(RUN_OUTPUT_ROOT, mustWork = FALSE))
OUTPUT_DIR <- get_path_setting("output-dir", "RUN_SUMMARY_OUTPUT_DIR", RUN_OUTPUT_ROOT, base_dir = REPO_DIR)
RESULTS_ROOT_DIR <- dirname(normalizePath(RUN_OUTPUT_ROOT, mustWork = FALSE))
COMPARISON_METRIC <- config_value(CONFIG, c("comparison", "metric"))

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
manifests <- lapply(dirs, function(path) read_csv_if_exists(file.path(path, "run_manifest.csv")))
first_manifest <- Filter(function(x) !is.null(x) && nrow(x) > 0, manifests)
first_manifest <- if (length(first_manifest) > 0) first_manifest[[1]] else NULL

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
  run_name = RUN_NAME,
  run_output_root = normalizePath(RUN_OUTPUT_ROOT, mustWork = FALSE),
  run_status = run_status,
  started_at = if (has_run_start) format(run_start, "%Y-%m-%d %H:%M:%S %Z") else NA_character_,
  ended_at = if (has_run_end) format(run_end, "%Y-%m-%d %H:%M:%S %Z") else NA_character_,
  runtime_seconds = run_runtime,
  data_path = if (is.null(first_manifest)) NA_character_ else first_manifest$data_path[[1]],
  seed = if (is.null(first_manifest)) NA_real_ else first_manifest$seed[[1]],
  feature_list = if (is.null(first_manifest)) NA_character_ else first_manifest$feature_list[[1]],
  winner_model = if (is.null(winner_row)) NA_character_ else winner_row$model[[1]],
  winner_metric = if (is.null(winner_row)) NA_character_ else COMPARISON_METRIC,
  winner_rmse = if (is.null(winner_row)) NA_real_ else winner_row$rmse[[1]],
  winner_mae = if (is.null(winner_row)) NA_real_ else winner_row$mae[[1]],
  winner_r2 = if (is.null(winner_row)) NA_real_ else winner_row$r2[[1]],
  winner_poisson_deviance = if (is.null(winner_row) || !"poisson_deviance" %in% names(winner_row)) NA_real_ else winner_row$poisson_deviance[[1]],
  winner_negloglik = if (is.null(winner_row) || !"negloglik" %in% names(winner_row)) NA_real_ else winner_row$negloglik[[1]]
)

safe_write_csv(summary_dt, file.path(OUTPUT_DIR, "run_summary.csv"))
safe_write_csv(script_summary, file.path(OUTPUT_DIR, "run_summary_scripts.csv"))

run_context_dt <- data.table(
  run_id = RUN_ID,
  run_name = RUN_NAME,
  repo_dir = normalizePath(REPO_DIR, mustWork = FALSE),
  run_output_root = normalizePath(RUN_OUTPUT_ROOT, mustWork = FALSE),
  results_root_dir = RESULTS_ROOT_DIR,
  config_path = normalizePath(CONFIG_PATH, mustWork = FALSE),
  data_path = Sys.getenv("MLR3_DATA_PATH", unset = NA_character_),
  ranger_output_dir = Sys.getenv("RANGER_OUTPUT_DIR", unset = NA_character_),
  xgb_output_dir = Sys.getenv("XGB_OUTPUT_DIR", unset = NA_character_),
  zinb_output_dir = Sys.getenv("ZINB_OUTPUT_DIR", unset = NA_character_),
  comparison_output_dir = Sys.getenv("COMPARISON_OUTPUT_DIR", unset = NA_character_)
)
safe_write_csv(run_context_dt, file.path(OUTPUT_DIR, "run_context.csv"))
write_config_snapshot(OUTPUT_DIR, CONFIG, prefix = "config_snapshot")
write_text_file(file.path(OUTPUT_DIR, "config_snapshot.R"), readLines(CONFIG_PATH, warn = FALSE))

report_lines <- c(
  sprintf("# Run Report: %s", RUN_ID),
  "",
  "## Run",
  sprintf("- status: `%s`", summary_dt$run_status[[1]]),
  sprintf("- run_name: `%s`", if (is.na(summary_dt$run_name[[1]])) "<none>" else summary_dt$run_name[[1]]),
  sprintf("- started_at: `%s`", summary_dt$started_at[[1]]),
  sprintf("- ended_at: `%s`", summary_dt$ended_at[[1]]),
  sprintf("- runtime_seconds: `%s`", summary_dt$runtime_seconds[[1]]),
  sprintf("- data_path: `%s`", summary_dt$data_path[[1]]),
  sprintf("- seed: `%s`", summary_dt$seed[[1]]),
  sprintf("- feature_list: `%s`", summary_dt$feature_list[[1]]),
  "",
  "## Winner",
  sprintf("- winner_model: `%s`", summary_dt$winner_model[[1]]),
  sprintf("- ranking_metric: `%s`", summary_dt$winner_metric[[1]]),
  sprintf("- winner_rmse: `%s`", summary_dt$winner_rmse[[1]]),
  sprintf("- winner_mae: `%s`", summary_dt$winner_mae[[1]]),
  sprintf("- winner_r2: `%s`", summary_dt$winner_r2[[1]]),
  sprintf("- winner_poisson_deviance: `%s`", summary_dt$winner_poisson_deviance[[1]]),
  sprintf("- winner_negloglik: `%s`", summary_dt$winner_negloglik[[1]]),
  "",
  "## Script Status",
  sprintf("- ranger: `%s`", script_summary[script_name == "mlr3_ranger_tuning"]$status[[1]]),
  sprintf("- xgboost: `%s`", script_summary[script_name == "mlr3_xgb_tuning"]$status[[1]]),
  sprintf("- zinb: `%s`", script_summary[script_name == "zinb_stepwise_cv"]$status[[1]]),
  sprintf("- comparison: `%s`", script_summary[script_name == "compare_best_models"]$status[[1]]),
  "",
  "## Files",
  "- `run_summary.csv` / `.rds`",
  "- `run_summary_scripts.csv` / `.rds`",
  "- `run_context.csv` / `.rds`",
  "- `config_snapshot.R`",
  "- `config_snapshot.txt` / `.rds`"
)
write_text_file(file.path(OUTPUT_DIR, "run_report.md"), report_lines)

append_registry_entry(file.path(RESULTS_ROOT_DIR, "run_registry.csv"), summary_dt)
