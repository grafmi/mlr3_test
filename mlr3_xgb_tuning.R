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
RUN_NAME <- get_run_name_setting(CONFIG)
SCRIPT_NAME <- "mlr3_xgb_tuning"
SCRIPT_PACKAGES <- c("data.table", "mlr3", "mlr3learners", "mlr3tuning", "paradox", "bbotk", "future", "xgboost")

require_packages(c(
  "data.table", "mlr3", "mlr3learners", "mlr3tuning", "paradox", "bbotk", "future", "xgboost"
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
ROW_FILTER <- get_setting("row-filter", "ROW_FILTER", config_value(CONFIG, c("experiment", "row_filter")))

DATA_PATH <- get_path_setting(
  "data", "MLR3_DATA_PATH",
  config_value(CONFIG, c("experiment", "data_path")),
  base_dir = REPO_DIR
)
OUTPUT_DIR <- get_path_setting("output-dir", "XGB_OUTPUT_DIR", config_value(CONFIG, c("xgboost", "output_dir")), base_dir = REPO_DIR)
N_FOLDS <- get_int_setting("folds", "N_FOLDS", config_value(CONFIG, c("experiment", "n_folds")), min_value = 2)
INNER_FOLDS <- get_int_setting("inner-folds", "INNER_FOLDS", config_value(CONFIG, c("experiment", "inner_folds")), min_value = 2)
OUTER_REPEATS <- get_int_setting("outer-repeats", "OUTER_REPEATS", config_value_or(CONFIG, c("experiment", "outer_repeats"), 1L), min_value = 1)
SEED <- get_int_setting("seed", "SEED", config_value(CONFIG, c("experiment", "seed")))
TUNE_EVALS <- get_int_setting("tune-evals", "TUNE_EVALS", config_value(CONFIG, c("xgboost", "tune_evals")), min_value = 1)
STRATA_BINS <- get_int_setting("strata-bins", "STRATA_BINS", config_value(CONFIG, c("experiment", "strata_bins")), min_value = 2)
MISSING_DROP_WARN_FRACTION <- get_optional_numeric_setting(
  "missing-drop-warn-fraction", "MISSING_DROP_WARN_FRACTION",
  config_value_or(CONFIG, c("experiment", "missing_drop_warn_fraction"), 0.05),
  min_value = 0
)
N_WORKERS <- get_int_setting("workers", "N_WORKERS", config_value(CONFIG, c("experiment", "n_workers")), min_value = 1)

# =========================
# Main
# =========================
.script_ok <- FALSE
LOG_STATE <- start_logging(OUTPUT_DIR, SCRIPT_NAME)
with_run_finalizer({
  if (INNER_FOLDS > N_FOLDS) {
    stop("INNER_FOLDS must be less than or equal to N_FOLDS.", call. = FALSE)
  }

  set.seed(SEED)
  if (N_WORKERS > 1) {
    future::plan(future::multisession, workers = N_WORKERS)
  } else {
    future::plan(future::sequential)
  }
  on.exit(future::plan(future::sequential), add = TRUE)

  df <- load_csv_checked(DATA_PATH)
  work_dt <- prepare_modeling_data(
    df, TARGET, FEATURE_COLS, ID_COLS,
    row_filter = ROW_FILTER,
    missing_drop_warn_fraction = MISSING_DROP_WARN_FRACTION
  )
  full_data_encoded <- encode_features_train_test(work_dt, work_dt, target = TARGET)$train
  dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
  resolved_config <- list(
    script_name = SCRIPT_NAME,
    run_name = RUN_NAME,
    data_path = normalizePath(DATA_PATH, mustWork = FALSE),
    output_dir = normalizePath(OUTPUT_DIR, mustWork = FALSE),
    target = TARGET,
    feature_cols = FEATURE_COLS,
    id_cols = ID_COLS,
    row_filter = ROW_FILTER,
    seed = SEED,
    n_folds = N_FOLDS,
    inner_folds = INNER_FOLDS,
    outer_repeats = OUTER_REPEATS,
    strata_bins = STRATA_BINS,
    missing_drop_warn_fraction = MISSING_DROP_WARN_FRACTION,
    tune_evals = TUNE_EVALS,
    n_workers = N_WORKERS
  )
  write_config_snapshot(OUTPUT_DIR, resolved_config)

  log_info("Using data file: ", normalizePath(DATA_PATH, mustWork = FALSE))
  log_info("Using output directory: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
  if (!is.na(RUN_NAME)) log_info("Run name: ", RUN_NAME)
  if (nzchar(trimws(ROW_FILTER))) log_info("Using row filter: ", ROW_FILTER)
  log_dataset_overview(
    full_data_encoded,
    target = TARGET,
    feature_cols = setdiff(names(full_data_encoded), TARGET),
    id_cols = ID_COLS,
    metric = "rmse",
    extra = list(
      "Original feature columns" = paste(FEATURE_COLS, collapse = ", "),
      "Feature encoding" = "train-fold one-hot encoding for outer CV",
      "Outer repeats" = OUTER_REPEATS,
      "Outer folds / inner folds" = paste(N_FOLDS, INNER_FOLDS, sep = " / "),
      "Missing-drop warn fraction" = if (is.na(MISSING_DROP_WARN_FRACTION)) "<disabled>" else MISSING_DROP_WARN_FRACTION,
      "Tuning evals" = TUNE_EVALS,
      "Workers" = N_WORKERS
    )
  )
  overview_dt <- dataset_overview(full_data_encoded, target = TARGET, feature_cols = setdiff(names(full_data_encoded), TARGET), id_cols = ID_COLS)
  safe_write_csv(overview_dt, file.path(OUTPUT_DIR, "xgb_dataset_overview.csv"))

  learner <- lrn(
    "regr.xgboost",
    predict_type = "response",
    booster = "gbtree",
    objective = "reg:squarederror",
    nthread = 1,
    seed = SEED
  )

  xgb_space_args <- list(
    nrounds = p_int(
      lower = config_value(CONFIG, c("xgboost", "search_space", "nrounds"))[[1]],
      upper = config_value(CONFIG, c("xgboost", "search_space", "nrounds"))[[2]]
    ),
    eta = p_dbl(
      lower = config_value(CONFIG, c("xgboost", "search_space", "eta"))[[1]],
      upper = config_value(CONFIG, c("xgboost", "search_space", "eta"))[[2]]
    ),
    max_depth = p_int(
      lower = config_value(CONFIG, c("xgboost", "search_space", "max_depth"))[[1]],
      upper = config_value(CONFIG, c("xgboost", "search_space", "max_depth"))[[2]]
    ),
    min_child_weight = p_dbl(
      lower = config_value(CONFIG, c("xgboost", "search_space", "min_child_weight"))[[1]],
      upper = config_value(CONFIG, c("xgboost", "search_space", "min_child_weight"))[[2]]
    ),
    subsample = p_dbl(
      lower = config_value(CONFIG, c("xgboost", "search_space", "subsample"))[[1]],
      upper = config_value(CONFIG, c("xgboost", "search_space", "subsample"))[[2]]
    ),
    colsample_bytree = p_dbl(
      lower = config_value(CONFIG, c("xgboost", "search_space", "colsample_bytree"))[[1]],
      upper = config_value(CONFIG, c("xgboost", "search_space", "colsample_bytree"))[[2]]
    ),
    lambda = p_dbl(
      lower = config_value(CONFIG, c("xgboost", "search_space", "lambda"))[[1]],
      upper = config_value(CONFIG, c("xgboost", "search_space", "lambda"))[[2]]
    ),
    alpha = p_dbl(
      lower = config_value(CONFIG, c("xgboost", "search_space", "alpha"))[[1]],
      upper = config_value(CONFIG, c("xgboost", "search_space", "alpha"))[[2]]
    )
  )

  if (has_config_value(CONFIG, c("xgboost", "search_space", "gamma"))) {
    xgb_space_args$gamma <- p_dbl(
      lower = config_value(CONFIG, c("xgboost", "search_space", "gamma"))[[1]],
      upper = config_value(CONFIG, c("xgboost", "search_space", "gamma"))[[2]]
    )
  }

  search_space <- do.call(ps, xgb_space_args)

  inner_cv <- rsmp("cv", folds = INNER_FOLDS)

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

  cv_run <- run_repeated_encoded_autotuner_outer_cv(
    raw_dt = work_dt,
    auto_tuner = at,
    target = TARGET,
    n_folds = N_FOLDS,
    outer_repeats = OUTER_REPEATS,
    seed = SEED,
    n_bins = STRATA_BINS,
    task_id = "xgb_regression",
    progress_prefix = "XGBoost"
  )
  predictions <- cv_run$predictions
  fold_metrics <- fold_metrics_from_predictions(predictions)
  overall_metrics <- aggregate_predictions(predictions)
  best_params <- cv_run$tuning_results
  unseen_levels <- cv_run$unseen_levels

  safe_write_csv(fold_metrics, file.path(OUTPUT_DIR, "xgb_fold_metrics.csv"))
  safe_write_csv(overall_metrics, file.path(OUTPUT_DIR, "xgb_overall_metrics.csv"))
  safe_write_csv(predictions, file.path(OUTPUT_DIR, "xgb_cv_predictions.csv"))
  safe_write_csv(best_params, file.path(OUTPUT_DIR, "xgb_best_params.csv"))
  if (!is.null(unseen_levels) && nrow(unseen_levels) > 0) {
    safe_write_csv(unseen_levels, file.path(OUTPUT_DIR, "xgb_unseen_factor_levels.csv"))
  }

  diagnostic_artifacts <- character(0)
  diagnostic_fit <- tryCatch({
    best_param_values <- extract_param_values_from_tuning(best_params, sort_measure = "regr.rmse")
    final_learner <- learner$clone(deep = TRUE)
    final_learner$param_set$values <- modifyList(final_learner$param_set$values, best_param_values)
    final_task <- mlr3::TaskRegr$new(
      id = "xgb_regression_full_data_diagnostic",
      backend = as.data.frame(full_data_encoded),
      target = TARGET
    )
    final_learner$train(final_task)

    importance_dt <- tryCatch(
      data.table::as.data.table(xgboost::xgb.importance(model = final_learner$model, feature_names = final_task$feature_names)),
      error = function(e) NULL
    )
    if (!is.null(importance_dt) && nrow(importance_dt) > 0) {
      setnames(importance_dt, old = names(importance_dt), new = tolower(names(importance_dt)))
      safe_write_csv(importance_dt, file.path(OUTPUT_DIR, "xgb_feature_importance.csv"))
      diagnostic_artifacts <<- c(diagnostic_artifacts, "`xgb_feature_importance.csv` / `.rds`")
    }
    TRUE
  }, error = function(e) {
    log_info("Warning: xgboost diagnostic fit failed: ", conditionMessage(e))
    FALSE
  })

  report_lines <- c(
    sprintf("# %s Report", SCRIPT_NAME),
    "",
    "## Run",
    sprintf("- data_path: `%s`", normalizePath(DATA_PATH, mustWork = FALSE)),
    sprintf("- output_dir: `%s`", normalizePath(OUTPUT_DIR, mustWork = FALSE)),
    sprintf("- target: `%s`", TARGET),
    sprintf("- original_feature_cols: `%s`", paste(FEATURE_COLS, collapse = ", ")),
    sprintf("- encoded_feature_count: `%s`", length(setdiff(names(full_data_encoded), TARGET))),
    "- feature_encoding: `one-hot encoding is learned separately inside each outer-CV training split`",
    sprintf("- seed: `%s`", SEED),
    sprintf("- outer_folds: `%s`", N_FOLDS),
    sprintf("- inner_folds: `%s`", INNER_FOLDS),
    sprintf("- outer_repeats: `%s`", OUTER_REPEATS),
    sprintf("- tune_evals: `%s`", TUNE_EVALS),
    sprintf("- workers: `%s`", N_WORKERS),
    "",
    "## Dataset",
    sprintf("- rows: `%s`", overview_dt$n_rows[[1]]),
    sprintf("- cols: `%s`", overview_dt$n_cols[[1]]),
    "",
    "## Metrics",
    sprintf("- rmse: `%.6f`", overall_metrics$rmse[[1]]),
    sprintf("- mae: `%.6f`", overall_metrics$mae[[1]]),
    sprintf("- r2: `%.6f`", overall_metrics$r2[[1]]),
    sprintf("- poisson_deviance: `%.6f`", overall_metrics$poisson_deviance[[1]]),
    "",
    "## Outputs",
    "- `xgb_fold_metrics.csv` / `.rds`",
    "- `xgb_overall_metrics.csv` / `.rds`",
    "- `xgb_cv_predictions.csv` / `.rds`",
    "- `xgb_best_params.csv` / `.rds`",
    "- `xgb_unseen_factor_levels.csv` / `.rds` when unseen outer-test factor levels occur",
    "- `resolved_config.txt` / `.rds`",
    "- `run_manifest.csv` / `.rds`"
  )
  if (length(diagnostic_artifacts) > 0) {
    report_lines <- append(report_lines, diagnostic_artifacts)
  }
  write_text_file(file.path(OUTPUT_DIR, "xgb_model_report.md"), report_lines)

  log_info("Done. Files written to: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
  print(overall_metrics)
  .script_ok <- TRUE
}, function() finalize_run(
  log_state = LOG_STATE,
  output_dir = OUTPUT_DIR,
  script_name = SCRIPT_NAME,
  repo_dir = REPO_DIR,
  packages = SCRIPT_PACKAGES,
  status = if (.script_ok) "completed" else "failed",
  seed = SEED,
  data_path = DATA_PATH,
  feature_cols = FEATURE_COLS,
  n_workers = N_WORKERS,
  run_name = RUN_NAME
))
