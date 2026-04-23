#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(mlr3)
  library(mlr3learners)
  library(mlr3tuning)
  library(paradox)
  library(bbotk)
  library(future)
})

# =========================
# User settings
# =========================
# Default input file: the synthetic ZINB test dataset saved next to this script.
# You can still replace this with your own loader if needed.

TARGET <- "n_eintritte"
FEATURE_COLS <- c("prcrank", "potenzielle_kunden", "unfalldeckung")
ID_COLS <- character(0)
OUTPUT_DIR <- "outputs_xgb"
N_FOLDS <- 10
SEED <- 123
TUNE_EVALS <- 5
STRATA_BINS <- 10
N_WORKERS <- 16

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
  fread(DATA_PATH)
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

predictions_from_resample <- function(rr) {
  pred_list <- rr$predictions()
  rbindlist(lapply(seq_along(pred_list), function(fold) {
    pred <- pred_list[[fold]]
    data.table(
      row_id = pred$row_ids,
      fold = fold,
      truth = pred$truth,
      response = pred$response,
      error = pred$response - pred$truth,
      abs_error = abs(pred$response - pred$truth)
    )
  }))
}

fold_metrics_from_predictions <- function(predictions) {
  predictions[, reg_metrics(truth, response), by = fold][order(fold)]
}

aggregate_predictions <- function(pred) {
  reg_metrics(pred$truth, pred$response)
}

encode_factor_features <- function(dt, target) {
  feature_cols <- setdiff(names(dt), target)
  factor_cols <- names(which(vapply(dt[, ..feature_cols], is.factor, logical(1))))
  if (length(factor_cols) == 0) {
    return(dt)
  }

  x <- model.matrix(~ . - 1, data = as.data.frame(dt[, ..feature_cols]))
  data.table(dt[, ..target], as.data.table(x, check.names = TRUE))
}

# =========================
# Main
# =========================
set.seed(SEED)
future::plan(future::multisession, workers = N_WORKERS)

df <- load_default_df()
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
cat("Using data file:", normalizePath(DATA_PATH, mustWork = FALSE), "\n")
cat("Using future workers:", N_WORKERS, "\n")

if (!is.data.frame(df)) stop("df must be a data.frame or data.table")
if (!TARGET %in% names(df)) stop(sprintf("TARGET '%s' not found in df", TARGET))
if (length(FEATURE_COLS) == 0) stop("FEATURE_COLS must contain at least one predictor.")
missing_features <- setdiff(FEATURE_COLS, names(df))
if (length(missing_features) > 0) {
  stop("FEATURE_COLS not found in df: ", paste(missing_features, collapse = ", "))
}
if (TARGET %in% FEATURE_COLS) stop("TARGET must not be included in FEATURE_COLS.")
feature_id_overlap <- intersect(FEATURE_COLS, ID_COLS)
if (length(feature_id_overlap) > 0) {
  stop("FEATURE_COLS must not overlap with ID_COLS: ", paste(feature_id_overlap, collapse = ", "))
}

work_df <- as.data.table(copy(df))
keep_cols <- setdiff(c(TARGET, FEATURE_COLS), ID_COLS)
work_df <- work_df[, ..keep_cols]
work_df <- na.omit(work_df)
cat("Using features:", paste(FEATURE_COLS, collapse = ", "), "\n")

if (!is.numeric(work_df[[TARGET]])) stop("TARGET must be numeric for regression")

char_cols <- names(which(vapply(work_df, is.character, logical(1))))
if (length(char_cols) > 0) {
  work_df[, (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
}

work_df <- encode_factor_features(work_df, TARGET)

backend <- as.data.frame(work_df)
task <- TaskRegr$new(id = "regr_df", backend = backend, target = TARGET)
rsmp_cv <- make_stratified_custom_cv(task, target = TARGET, nfolds = N_FOLDS, seed = SEED, n_bins = STRATA_BINS)

learner <- lrn(
  "regr.xgboost",
  predict_type = "response",
  booster = "gbtree",
  objective = "reg:squarederror",
  nthread = 1
)

search_space <- ps(
  nrounds = p_int(lower = 100, upper = 1500),
  eta = p_dbl(lower = 0.01, upper = 0.3),
  max_depth = p_int(lower = 2, upper = 10),
  min_child_weight = p_dbl(lower = 1, upper = 15),
  subsample = p_dbl(lower = 0.5, upper = 1.0),
  colsample_bytree = p_dbl(lower = 0.4, upper = 1.0),
  lambda = p_dbl(lower = 0, upper = 10),
  alpha = p_dbl(lower = 0, upper = 10)
)

at <- auto_tuner(
  tuner = tnr("random_search"),
  learner = learner,
  resampling = rsmp("cv", folds = N_FOLDS),
  measure = msr("regr.rmse"),
  search_space = search_space,
  terminator = trm("evals", n_evals = TUNE_EVALS),
  store_tuning_instance = TRUE,
  store_models = FALSE
)

rr <- resample(task, at, rsmp_cv$clone(deep = TRUE), store_models = TRUE)
pred <- rr$prediction()

predictions <- predictions_from_resample(rr)
fold_metrics <- fold_metrics_from_predictions(predictions)
overall_metrics <- aggregate_predictions(pred)

best_params <- as.data.table(rr$learners[[1]]$tuning_result)
if (nrow(best_params) == 0 || ncol(best_params) == 0) {
  archive_dt <- as.data.table(rr$learners[[1]]$archive$data)
  best_params <- archive_dt[which.min(archive_dt[["regr.rmse"]])]
}
list_cols <- names(which(vapply(best_params, is.list, logical(1))))
if (length(list_cols) > 0) best_params[, (list_cols) := NULL]

fwrite(fold_metrics, file.path(OUTPUT_DIR, "xgb_fold_metrics.csv"))
fwrite(overall_metrics, file.path(OUTPUT_DIR, "xgb_overall_metrics.csv"))
fwrite(predictions, file.path(OUTPUT_DIR, "xgb_cv_predictions.csv"))
fwrite(best_params, file.path(OUTPUT_DIR, "xgb_best_params.csv"))

cat("Done. Files written to:", normalizePath(OUTPUT_DIR), "\n")
print(overall_metrics)
future::plan(future::sequential)
