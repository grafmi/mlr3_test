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
SCRIPT_NAME <- "mlr3_ranger_tuning"
SCRIPT_PACKAGES <- c("data.table", "mlr3", "mlr3learners", "mlr3tuning", "paradox", "bbotk", "future", "ranger")

require_packages(c(
  "data.table", "mlr3", "mlr3learners", "mlr3tuning", "paradox", "bbotk", "future", "ranger"
))

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
TARGET <- get_setting("target", "TARGET", config_value(CONFIG, c("experiment", "target")))
FEATURE_COLS <- parse_csv_setting(get_setting(
  "features", "FEATURE_COLS",
  paste(config_value(CONFIG, c("experiment", "feature_cols")), collapse = ",")
))
ID_COLS <- parse_csv_setting(get_setting(
  "id-cols", "ID_COLS",
  paste(config_value(CONFIG, c("experiment", "id_cols")), collapse = ",")
))

DATA_PATH <- get_path_setting(
  "data", "MLR3_DATA_PATH",
  config_value(CONFIG, c("experiment", "data_path")),
  base_dir = REPO_DIR
)
OUTPUT_DIR <- get_path_setting("output-dir", "RANGER_OUTPUT_DIR", config_value(CONFIG, c("ranger", "output_dir")), base_dir = REPO_DIR)
N_FOLDS <- get_int_setting("folds", "N_FOLDS", config_value(CONFIG, c("experiment", "n_folds")), min_value = 2)
SEED <- get_int_setting("seed", "SEED", config_value(CONFIG, c("experiment", "seed")))
TUNE_EVALS <- get_int_setting("tune-evals", "TUNE_EVALS", config_value(CONFIG, c("ranger", "tune_evals")), min_value = 1)
STRATA_BINS <- get_int_setting("strata-bins", "STRATA_BINS", config_value(CONFIG, c("experiment", "strata_bins")), min_value = 2)
N_WORKERS <- get_int_setting("workers", "N_WORKERS", config_value(CONFIG, c("experiment", "n_workers")), min_value = 1)

# =========================
# Main
# =========================
.script_ok <- FALSE
LOG_STATE <- start_logging(OUTPUT_DIR, SCRIPT_NAME)

set.seed(SEED)
if (N_WORKERS > 1) {
  future::plan(future::multisession, workers = N_WORKERS)
} else {
  future::plan(future::sequential)
}
on.exit(future::plan(future::sequential), add = TRUE)

df <- load_csv_checked(DATA_PATH)
work_dt <- prepare_modeling_data(df, TARGET, FEATURE_COLS, ID_COLS)
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

log_info("Using data file: ", normalizePath(DATA_PATH, mustWork = FALSE))
log_info("Using output directory: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
log_info("Using features: ", paste(FEATURE_COLS, collapse = ", "))
log_info("Using folds / tuning evals / workers: ", N_FOLDS, " / ", TUNE_EVALS, " / ", N_WORKERS)

backend <- add_regression_stratum(as.data.frame(work_dt), target = TARGET, n_bins = STRATA_BINS)
task <- make_regr_task("ranger_regression", backend = backend, target = TARGET)
outer_cv <- make_stratified_custom_cv(task, target = TARGET, nfolds = N_FOLDS, seed = SEED, n_bins = STRATA_BINS)

learner <- lrn(
  "regr.ranger",
  predict_type = "response",
  importance = "impurity",
  num.threads = 1,
  seed = SEED
)

search_space <- ps(
  num.trees = p_int(
    lower = config_value(CONFIG, c("ranger", "search_space", "num_trees"))[[1]],
    upper = config_value(CONFIG, c("ranger", "search_space", "num_trees"))[[2]]
  ),
  mtry = p_int(lower = 1, upper = max(1L, length(task$feature_names))),
  min.node.size = p_int(
    lower = config_value(CONFIG, c("ranger", "search_space", "min_node_size"))[[1]],
    upper = config_value(CONFIG, c("ranger", "search_space", "min_node_size"))[[2]]
  ),
  sample.fraction = p_dbl(
    lower = config_value(CONFIG, c("ranger", "search_space", "sample_fraction"))[[1]],
    upper = config_value(CONFIG, c("ranger", "search_space", "sample_fraction"))[[2]]
  ),
  replace = p_lgl()
)

inner_cv <- rsmp("cv", folds = N_FOLDS)

at <- auto_tuner(
  tuner = tnr("random_search"),
  learner = learner,
  resampling = inner_cv,
  measure = msr("regr.rmse"),
  search_space = search_space,
  terminator = trm("evals", n_evals = TUNE_EVALS),
  store_tuning_instance = TRUE,
  store_models = FALSE
)

rr <- resample(task, at, outer_cv$clone(deep = TRUE), store_models = TRUE)
predictions <- predictions_from_resample(rr)
fold_metrics <- fold_metrics_from_predictions(predictions)
overall_metrics <- aggregate_predictions(predictions)
best_params <- collect_tuning_results(rr, measure_col = "regr.rmse")

safe_write_csv(fold_metrics, file.path(OUTPUT_DIR, "ranger_fold_metrics.csv"))
safe_write_csv(overall_metrics, file.path(OUTPUT_DIR, "ranger_overall_metrics.csv"))
safe_write_csv(predictions, file.path(OUTPUT_DIR, "ranger_cv_predictions.csv"))
safe_write_csv(best_params, file.path(OUTPUT_DIR, "ranger_best_params.csv"))

log_info("Done. Files written to: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
print(overall_metrics)
.script_ok <- TRUE
invisible(write_run_manifest(
  output_dir = OUTPUT_DIR,
  script_name = SCRIPT_NAME,
  log_state = LOG_STATE,
  repo_dir = REPO_DIR,
  packages = SCRIPT_PACKAGES,
  status = "completed",
  seed = SEED,
  data_path = DATA_PATH,
  feature_cols = FEATURE_COLS,
  n_workers = N_WORKERS
))
stop_logging(LOG_STATE, "completed")
