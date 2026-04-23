#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

# =========================
# User settings
# =========================
RANGER_DIR <- "outputs_ranger"
XGB_DIR    <- "outputs_xgb"
ZINB_DIR   <- "outputs_zinb"
OUTPUT_DIR <- "outputs_model_comparison"
METRIC_TO_RANK <- "rmse"   # e.g. rmse, mae, max_error

# =========================
# Helpers
# =========================
read_if_exists <- function(path) {
  if (!file.exists(path)) return(NULL)
  fread(path)
}

safe_first <- function(dt, col, default = NA_character_) {
  if (is.null(dt) || !col %in% names(dt) || nrow(dt) == 0) return(default)
  as.character(dt[[col]][1])
}

safe_metrics_row <- function(dt, model_name) {
  needed <- c("rmse", "mae", "max_error", "sae", "mse", "bias")
  out <- data.table(model = model_name)
  for (nm in needed) {
    out[, (nm) := if (!is.null(dt) && nm %in% names(dt) && nrow(dt) > 0) dt[[nm]][1] else NA_real_]
  }
  out
}

collapse_best_params <- function(dt, drop_cols = c("learner_param_vals", "x_domain", "timestamp", "batch_nr", "warnings", "errors", "runtime_learners", "uhash")) {
  if (is.null(dt) || nrow(dt) == 0) return(NA_character_)
  keep <- setdiff(names(dt), drop_cols)
  if (length(keep) == 0) return(NA_character_)

  vals <- vapply(keep, function(nm) {
    val <- dt[[nm]][1]
    if (length(val) > 1) val <- paste(val, collapse = ",")
    sprintf("%s=%s", nm, as.character(val))
  }, character(1))

  paste(vals, collapse = "; ")
}

# =========================
# Main
# =========================
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# Ranger
ranger_metrics <- read_if_exists(file.path(RANGER_DIR, "ranger_overall_metrics.csv"))
ranger_params  <- read_if_exists(file.path(RANGER_DIR, "ranger_best_params.csv"))

ranger_row <- safe_metrics_row(ranger_metrics, "ranger")
ranger_row[, details := collapse_best_params(ranger_params)]

# XGB
xgb_metrics <- read_if_exists(file.path(XGB_DIR, "xgb_overall_metrics.csv"))
xgb_params  <- read_if_exists(file.path(XGB_DIR, "xgb_best_params.csv"))

xgb_row <- safe_metrics_row(xgb_metrics, "xgb")
xgb_row[, details := collapse_best_params(xgb_params)]

# ZINB
zinb_metrics <- read_if_exists(file.path(ZINB_DIR, "zinb_best_global_overall_metrics.csv"))
zinb_steps   <- read_if_exists(file.path(ZINB_DIR, "zinb_best_model_per_step.csv"))

zinb_row <- safe_metrics_row(zinb_metrics, "zinb")
if (!is.null(zinb_steps) && nrow(zinb_steps) > 0) {
  if (!(METRIC_TO_RANK %in% names(zinb_steps))) {
    stop(sprintf("METRIC_TO_RANK '%s' not found in ZINB step table.", METRIC_TO_RANK))
  }
  setorderv(zinb_steps, cols = c(METRIC_TO_RANK, "mae", "max_error"), order = c(1, 1, 1))
  best_zinb <- zinb_steps[1]
  zinb_row[, details := sprintf(
    "step=%s; variable=%s; transformation=%s; formula=%s; selected_terms=%s",
    best_zinb$step,
    best_zinb$variable,
    best_zinb$transformation,
    best_zinb$formula,
    best_zinb$selected_terms
  )]
} else {
  zinb_row[, details := NA_character_]
}

comparison <- rbindlist(list(ranger_row, xgb_row, zinb_row), fill = TRUE)

if (!(METRIC_TO_RANK %in% names(comparison))) {
  stop(sprintf("METRIC_TO_RANK '%s' not found in comparison table.", METRIC_TO_RANK))
}

comparison[, rank := frank(get(METRIC_TO_RANK), ties.method = "min", na.last = "keep")]
setorder(comparison, rank, model)
setcolorder(comparison, c("rank", "model", "rmse", "mae", "max_error", "sae", "mse", "bias", "details"))

fwrite(comparison, file.path(OUTPUT_DIR, "best_models_comparison.csv"))

cat("Done. Comparison written to:", normalizePath(file.path(OUTPUT_DIR, "best_models_comparison.csv")), "\n")
print(comparison)
