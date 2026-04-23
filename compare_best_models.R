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

suppressPackageStartupMessages({
  library(data.table)
})

# =========================
# User settings
# =========================
RANGER_DIR <- get_path_setting("ranger-dir", "RANGER_OUTPUT_DIR", "outputs_ranger", base_dir = REPO_DIR)
XGB_DIR <- get_path_setting("xgb-dir", "XGB_OUTPUT_DIR", "outputs_xgb", base_dir = REPO_DIR)
ZINB_DIR <- get_path_setting("zinb-dir", "ZINB_OUTPUT_DIR", "outputs_zinb", base_dir = REPO_DIR)
OUTPUT_DIR <- get_path_setting(
  "output-dir", "COMPARISON_OUTPUT_DIR",
  "outputs_model_comparison",
  base_dir = REPO_DIR
)
METRIC_TO_RANK <- get_setting("metric", "METRIC_TO_RANK", "rmse")

# =========================
# Helpers
# =========================
read_if_exists <- function(path) {
  if (!file.exists(path)) {
    message("Missing optional input: ", normalizePath(path, mustWork = FALSE))
    return(NULL)
  }
  fread(path)
}

safe_metrics_row <- function(dt, model_name) {
  needed <- c("rmse", "mae", "max_error", "sae", "mse", "bias", "r2")
  out <- data.table(model = model_name)
  for (nm in needed) {
    value <- if (!is.null(dt) && nm %in% names(dt) && nrow(dt) > 0) dt[[nm]][1] else NA_real_
    out[, (nm) := value]
  }
  out
}

collapse_best_params <- function(dt) {
  if (is.null(dt) || nrow(dt) == 0) return(NA_character_)
  drop_cols <- c(
    "learner_param_vals", "x_domain", "timestamp", "batch_nr", "warnings",
    "errors", "runtime_learners", "uhash"
  )
  keep <- setdiff(names(dt), drop_cols)
  if (length(keep) == 0) return(NA_character_)

  # Nested CV has one tuned configuration per outer fold; show a compact summary.
  rows <- dt[, ..keep]
  if ("regr.rmse" %in% names(rows)) setorder(rows, regr.rmse)
  rows <- head(rows, 3)

  paste(apply(rows, 1, function(row) {
    vals <- sprintf("%s=%s", names(row), as.character(row))
    paste(vals, collapse = "; ")
  }), collapse = " || ")
}

rank_values <- function(values, metric) {
  if (metric == "r2") {
    frank(-values, ties.method = "min", na.last = "keep")
  } else {
    frank(values, ties.method = "min", na.last = "keep")
  }
}

# =========================
# Main
# =========================
.script_ok <- FALSE
LOG_STATE <- start_logging(OUTPUT_DIR, "compare_best_models")

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
log_info("Using metric to rank: ", METRIC_TO_RANK)
log_info("Using ranger directory: ", normalizePath(RANGER_DIR, mustWork = FALSE))
log_info("Using xgb directory: ", normalizePath(XGB_DIR, mustWork = FALSE))
log_info("Using zinb directory: ", normalizePath(ZINB_DIR, mustWork = FALSE))

ranger_metrics <- read_if_exists(file.path(RANGER_DIR, "ranger_overall_metrics.csv"))
ranger_params <- read_if_exists(file.path(RANGER_DIR, "ranger_best_params.csv"))
ranger_row <- safe_metrics_row(ranger_metrics, "ranger")
ranger_row[, details := collapse_best_params(ranger_params)]

xgb_metrics <- read_if_exists(file.path(XGB_DIR, "xgb_overall_metrics.csv"))
xgb_params <- read_if_exists(file.path(XGB_DIR, "xgb_best_params.csv"))
xgb_row <- safe_metrics_row(xgb_metrics, "xgb")
xgb_row[, details := collapse_best_params(xgb_params)]

zinb_metrics <- read_if_exists(file.path(ZINB_DIR, "zinb_best_global_overall_metrics.csv"))
zinb_steps <- read_if_exists(file.path(ZINB_DIR, "zinb_best_model_per_step.csv"))
zinb_row <- safe_metrics_row(zinb_metrics, "zinb")

if (!is.null(zinb_steps) && nrow(zinb_steps) > 0) {
  if (!(METRIC_TO_RANK %in% names(zinb_steps))) {
    stop(sprintf("METRIC_TO_RANK '%s' not found in ZINB step table.", METRIC_TO_RANK), call. = FALSE)
  }
  if (METRIC_TO_RANK == "r2") {
    setorderv(zinb_steps, cols = c(METRIC_TO_RANK, "mae", "max_error"), order = c(-1, 1, 1))
  } else {
    setorderv(zinb_steps, cols = c(METRIC_TO_RANK, "mae", "max_error"), order = c(1, 1, 1))
  }
  best_zinb <- zinb_steps[1]
  zinb_row[, details := sprintf(
    "step=%s; variable=%s; transformation=%s; formula=%s; selected_terms=%s",
    best_zinb$step,
    best_zinb$variable,
    best_zinb$transformation,
    best_zinb$formula,
    best_zinb$selected_terms
  )]
} else if (!is.null(zinb_metrics) && nrow(zinb_metrics) > 0) {
  zinb_row[, details := "intercept-only baseline; no selected step model found"]
} else {
  zinb_row[, details := NA_character_]
}

comparison <- rbindlist(list(ranger_row, xgb_row, zinb_row), fill = TRUE)

if (!(METRIC_TO_RANK %in% names(comparison))) {
  stop(sprintf("METRIC_TO_RANK '%s' not found in comparison table.", METRIC_TO_RANK), call. = FALSE)
}

comparison[, rank := rank_values(get(METRIC_TO_RANK), METRIC_TO_RANK)]
setorder(comparison, rank, model)
setcolorder(comparison, c("rank", "model", "rmse", "mae", "max_error", "sae", "mse", "bias", "r2", "details"))

out_path <- file.path(OUTPUT_DIR, "best_models_comparison.csv")
safe_write_csv(comparison, out_path)

log_info("Done. Comparison written to: ", normalizePath(out_path, mustWork = FALSE))
print(comparison)
.script_ok <- TRUE
stop_logging(LOG_STATE, if (.script_ok) "completed" else "failed")
