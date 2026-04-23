#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(mlr3)
  library(mlr3learners)
  library(mlr3tuning)
  library(paradox)
  library(bbotk)
})

# =========================
# User settings
# =========================
# Default input file: the synthetic ZINB test dataset saved next to this script.
# You can still replace this with your own loader if needed.

TARGET <- "n_eintritte"
ID_COLS <- character(0)        # e.g. c("id")
OUTPUT_DIR <- "outputs_ranger"
N_FOLDS <- 10
SEED <- 123
TUNE_EVALS <- 50
STRATA_BINS <- 10

get_script_dir <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", cmd_args, value = TRUE)
  if (length(file_arg) > 0) {
    return(dirname(normalizePath(sub("^--file=", "", file_arg[1]))))
  }
  if (!is.null(sys.frames()[[1]]$ofile)) {
    return(dirname(normalizePath(sys.frames()[[1]]$ofile)))
  }
  getwd()
}

SCRIPT_DIR <- get_script_dir()
DATA_PATH <- file.path(SCRIPT_DIR, "testfile_zinb_nonlinear_eintritte.csv")

# =========================
# Helpers
# =========================
load_default_df <- function() {
  if (!file.exists(DATA_PATH)) {
    stop(sprintf("Default data file not found: %s", DATA_PATH))
  }
  df <- fread(DATA_PATH, header = TRUE, )
  invisible(TRUE)
}

make_strata <- function(y, n_bins = 10) {
  probs <- seq(0, 1, length.out = n_bins + 1)
  brks <- unique(quantile(y, probs = probs, na.rm = TRUE, type = 7))
  if (length(brks) < 3) {
    return(factor(rep("all", length(y))))
  }
  cut(y, breaks = brks, include.lowest = TRUE, ordered_result = TRUE)
}

make_stratified_custom_cv <- function(task, target, nfolds = 10, seed = 123, n_bins = 10) {
  set.seed(seed)
  y <- task$data(cols = target)[[1]]
  strata <- make_strata(y, n_bins = n_bins)
  idx_by_stratum <- split(seq_along(y), strata, drop = TRUE)

  fold_ids <- integer(length(y))
  for (ids in idx_by_stratum) {
    ids <- sample(ids)
    parts <- split(ids, rep(seq_len(nfolds), length.out = length(ids)))
    for (k in seq_along(parts)) fold_ids[parts[[k]]] <- k
  }

  test_sets <- lapply(seq_len(nfolds), function(k) which(fold_ids == k))
  train_sets <- lapply(test_sets, function(test) setdiff(seq_along(y), test))

  rsmp_custom <- rsmp("custom")
  rsmp_custom$instantiate(task, train_sets = train_sets, test_sets = test_sets)
  rsmp_custom
}

reg_metrics <- function(truth, response) {
  err <- response - truth
  data.table(
    rmse = sqrt(mean(err^2, na.rm = TRUE)),
    mae = mean(abs(err), na.rm = TRUE),
    max_error = max(abs(err), na.rm = TRUE),
    sae = sum(err, na.rm = TRUE),
    mse = mean(err^2, na.rm = TRUE),
    bias = mean(err, na.rm = TRUE)
  )
}

fold_metrics_from_prediction <- function(pred) {
  dt <- data.table(
    row_id = pred$row_ids,
    truth = pred$truth,
    response = pred$response
  )
  dt[, fold := pred$fold]
  dt[, reg_metrics(truth, response), by = fold][order(fold)]
}

aggregate_predictions <- function(pred) {
  reg_metrics(pred$truth, pred$response)
}

# =========================
# Main
# =========================
set.seed(SEED)
load_default_df()
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
cat("Using data file:", normalizePath(DATA_PATH, mustWork = FALSE), "\n")

if (!is.data.frame(df)) stop("df must be a data.frame or data.table")
if (!TARGET %in% names(df)) stop(sprintf("TARGET '%s' not found in df", TARGET))

work_df <- as.data.table(copy(df))
keep_cols <- setdiff(names(work_df), ID_COLS)
work_df <- work_df[, ..keep_cols]
work_df <- na.omit(work_df)

if (!is.numeric(work_df[[TARGET]])) stop("TARGET must be numeric for regression")

# Convert character columns to factor
char_cols <- names(which(vapply(work_df, is.character, logical(1))))
if (length(char_cols) > 0) {
  work_df[, (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
}

backend <- as.data.frame(work_df)
task <- TaskRegr$new(id = "regr_df", backend = backend, target = TARGET)
rsmp_cv <- make_stratified_custom_cv(task, target = TARGET, nfolds = N_FOLDS, seed = SEED, n_bins = STRATA_BINS)

learner <- lrn(
  "regr.ranger",
  predict_type = "response",
  importance = "impurity"
)

search_space <- ps(
  num.trees = p_int(lower = 300, upper = 1500),
  mtry = p_int(lower = 1, upper = max(1L, ncol(backend) - 1L)),
  min.node.size = p_int(lower = 1, upper = 25),
  sample.fraction = p_dbl(lower = 0.5, upper = 1.0)
)

at <- auto_tuner(
  tuner = tnr("random_search"),
  learner = learner,
  resampling = rsmp_cv$clone(deep = TRUE),
  measure = msr("regr.rmse"),
  search_space = search_space,
  terminator = trm("evals", n_evals = TUNE_EVALS),
  store_tuning_instance = TRUE,
  store_models = FALSE
)

rr <- resample(task, at, rsmp_cv$clone(deep = TRUE), store_models = FALSE)
pred <- rr$prediction()

fold_metrics <- fold_metrics_from_prediction(pred)
overall_metrics <- aggregate_predictions(pred)
predictions <- data.table(
  row_id = pred$row_ids,
  fold = pred$fold,
  truth = pred$truth,
  response = pred$response,
  error = pred$response - pred$truth,
  abs_error = abs(pred$response - pred$truth)
)

best_params <- as.data.table(rr$learners[[1]]$tuning_result)
if (nrow(best_params) == 0) {
  best_params <- as.data.table(rr$learners[[1]]$archive$data[which.min(regr.rmse)])
}

fwrite(fold_metrics, file.path(OUTPUT_DIR, "ranger_fold_metrics.csv"))
fwrite(overall_metrics, file.path(OUTPUT_DIR, "ranger_overall_metrics.csv"))
fwrite(predictions, file.path(OUTPUT_DIR, "ranger_cv_predictions.csv"))
fwrite(best_params, file.path(OUTPUT_DIR, "ranger_best_params.csv"))

cat("Done. Files written to:", normalizePath(OUTPUT_DIR), "\n")
print(overall_metrics)
