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
ROW_FILTER <- get_setting("row-filter", "ROW_FILTER", config_value(CONFIG, c("experiment", "row_filter")))

DATA_PATH <- get_path_setting(
  "data", "MLR3_DATA_PATH",
  config_value(CONFIG, c("experiment", "data_path")),
  base_dir = REPO_DIR
)
OUTPUT_DIR <- get_path_setting("output-dir", "RANGER_OUTPUT_DIR", config_value(CONFIG, c("ranger", "output_dir")), base_dir = REPO_DIR)
N_FOLDS <- get_int_setting("folds", "N_FOLDS", config_value(CONFIG, c("experiment", "n_folds")), min_value = 2)
INNER_FOLDS <- get_int_setting("inner-folds", "INNER_FOLDS", config_value(CONFIG, c("experiment", "inner_folds")), min_value = 2)
SEED <- get_int_setting("seed", "SEED", config_value(CONFIG, c("experiment", "seed")))
TUNE_EVALS <- get_int_setting("tune-evals", "TUNE_EVALS", config_value(CONFIG, c("ranger", "tune_evals")), min_value = 1)
STRATA_BINS <- get_int_setting("strata-bins", "STRATA_BINS", config_value(CONFIG, c("experiment", "strata_bins")), min_value = 2)
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
  work_dt <- prepare_modeling_data(df, TARGET, FEATURE_COLS, ID_COLS, row_filter = ROW_FILTER)
  dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
  resolved_config <- list(
    script_name = SCRIPT_NAME,
    data_path = normalizePath(DATA_PATH, mustWork = FALSE),
    output_dir = normalizePath(OUTPUT_DIR, mustWork = FALSE),
    target = TARGET,
    feature_cols = FEATURE_COLS,
    id_cols = ID_COLS,
    row_filter = ROW_FILTER,
    seed = SEED,
    n_folds = N_FOLDS,
    inner_folds = INNER_FOLDS,
    strata_bins = STRATA_BINS,
    tune_evals = TUNE_EVALS,
    n_workers = N_WORKERS
  )
  write_config_snapshot(OUTPUT_DIR, resolved_config)

  log_info("Using data file: ", normalizePath(DATA_PATH, mustWork = FALSE))
  log_info("Using output directory: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
  if (nzchar(trimws(ROW_FILTER))) log_info("Using row filter: ", ROW_FILTER)
  log_dataset_overview(
    work_dt,
    target = TARGET,
    feature_cols = FEATURE_COLS,
    id_cols = ID_COLS,
    metric = "rmse",
    extra = list(
      "Outer folds / inner folds" = paste(N_FOLDS, INNER_FOLDS, sep = " / "),
      "Tuning evals" = TUNE_EVALS,
      "Workers" = N_WORKERS
    )
  )
  overview_dt <- dataset_overview(work_dt, target = TARGET, feature_cols = FEATURE_COLS, id_cols = ID_COLS)
  safe_write_csv(overview_dt, file.path(OUTPUT_DIR, "ranger_dataset_overview.csv"))

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

  ranger_space_args <- list(
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

  if (has_config_value(CONFIG, c("ranger", "search_space", "max_depth"))) {
    ranger_space_args$max.depth <- p_int(
      lower = config_value(CONFIG, c("ranger", "search_space", "max_depth"))[[1]],
      upper = config_value(CONFIG, c("ranger", "search_space", "max_depth"))[[2]]
    )
  }

  if (has_config_value(CONFIG, c("ranger", "search_space", "splitrule"))) {
    ranger_space_args$splitrule <- p_fct(levels = config_value(CONFIG, c("ranger", "search_space", "splitrule")))
  }

  search_space <- do.call(ps, ranger_space_args)

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

  rr <- resample(task, at, outer_cv$clone(deep = TRUE), store_models = TRUE)
  predictions <- predictions_from_resample(rr)
  fold_metrics <- fold_metrics_from_predictions(predictions)
  overall_metrics <- aggregate_predictions(predictions)
  best_params <- collect_tuning_results(rr, measure_col = "regr.rmse")

  safe_write_csv(fold_metrics, file.path(OUTPUT_DIR, "ranger_fold_metrics.csv"))
  safe_write_csv(overall_metrics, file.path(OUTPUT_DIR, "ranger_overall_metrics.csv"))
  safe_write_csv(predictions, file.path(OUTPUT_DIR, "ranger_cv_predictions.csv"))
  safe_write_csv(best_params, file.path(OUTPUT_DIR, "ranger_best_params.csv"))

  diagnostic_artifacts <- character(0)
  diagnostic_fit <- tryCatch({
    best_param_values <- extract_param_values_from_tuning(best_params, sort_measure = "regr.rmse")
    final_learner <- learner$clone(deep = TRUE)
    final_learner$param_set$values <- modifyList(final_learner$param_set$values, best_param_values)
    final_learner$train(task)
    importance_vec <- final_learner$model$model$variable.importance
    if (!is.null(importance_vec) && length(importance_vec) > 0) {
      importance_dt <- data.table(
        feature = names(importance_vec),
        importance = as.numeric(importance_vec)
      )[order(-importance, feature)]
      safe_write_csv(importance_dt, file.path(OUTPUT_DIR, "ranger_feature_importance.csv"))
      diagnostic_artifacts <<- c(diagnostic_artifacts, "`ranger_feature_importance.csv` / `.rds`")
    }
    TRUE
  }, error = function(e) {
    log_info("Warning: ranger diagnostic fit failed: ", conditionMessage(e))
    FALSE
  })

  report_lines <- c(
    sprintf("# %s Report", SCRIPT_NAME),
    "",
    "## Run",
    sprintf("- data_path: `%s`", normalizePath(DATA_PATH, mustWork = FALSE)),
    sprintf("- output_dir: `%s`", normalizePath(OUTPUT_DIR, mustWork = FALSE)),
    sprintf("- target: `%s`", TARGET),
    sprintf("- feature_cols: `%s`", paste(FEATURE_COLS, collapse = ", ")),
    sprintf("- id_cols: `%s`", if (length(ID_COLS) > 0) paste(ID_COLS, collapse = ", ") else "<none>"),
    sprintf("- seed: `%s`", SEED),
    sprintf("- outer_folds: `%s`", N_FOLDS),
    sprintf("- inner_folds: `%s`", INNER_FOLDS),
    sprintf("- tune_evals: `%s`", TUNE_EVALS),
    sprintf("- workers: `%s`", N_WORKERS),
    "",
    "## Dataset",
    sprintf("- rows: `%s`", overview_dt$n_rows[[1]]),
    sprintf("- cols: `%s`", overview_dt$n_cols[[1]]),
    sprintf("- factor_cols: `%s`", overview_dt$n_factor_cols[[1]]),
    "",
    "## Metrics",
    sprintf("- rmse: `%.6f`", overall_metrics$rmse[[1]]),
    sprintf("- mae: `%.6f`", overall_metrics$mae[[1]]),
    sprintf("- r2: `%.6f`", overall_metrics$r2[[1]]),
    sprintf("- poisson_deviance: `%.6f`", overall_metrics$poisson_deviance[[1]]),
    "",
    "## Outputs",
    "- `ranger_fold_metrics.csv` / `.rds`",
    "- `ranger_overall_metrics.csv` / `.rds`",
    "- `ranger_cv_predictions.csv` / `.rds`",
    "- `ranger_best_params.csv` / `.rds`",
    "- `resolved_config.txt` / `.rds`",
    "- `run_manifest.csv` / `.rds`"
  )
  if (length(diagnostic_artifacts) > 0) {
    report_lines <- append(report_lines, diagnostic_artifacts)
  }
  write_text_file(file.path(OUTPUT_DIR, "ranger_model_report.md"), report_lines)

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
  n_workers = N_WORKERS
))
