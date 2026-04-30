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
OUTER_REPEATS <- get_int_setting("outer-repeats", "OUTER_REPEATS", config_value_or(CONFIG, c("experiment", "outer_repeats"), 1L), min_value = 1)
OUTER_RESAMPLING <- normalize_outer_resampling(get_setting(
  "outer-resampling", "OUTER_RESAMPLING",
  config_value_or(CONFIG, c("experiment", "outer_resampling"), "stratified")
))
OUTER_BLOCK_COL <- normalize_optional_string(get_setting(
  "outer-block-col", "OUTER_BLOCK_COL",
  get_setting("outer-year-col", "OUTER_YEAR_COL", config_value_or(CONFIG, c("experiment", "outer_block_col"), "year"))
))
if (identical(OUTER_RESAMPLING, "year_blocked") && is.na(OUTER_BLOCK_COL)) OUTER_BLOCK_COL <- "year"
SEED <- get_int_setting("seed", "SEED", config_value(CONFIG, c("experiment", "seed")))
TUNE_EVALS <- get_int_setting("tune-evals", "TUNE_EVALS", config_value(CONFIG, c("ranger", "tune_evals")), min_value = 1)
TUNE_BATCH_SIZE <- get_int_setting(
  "tune-batch-size", "RANGER_TUNE_BATCH_SIZE",
  get_setting("tune-batch-size", "TUNE_BATCH_SIZE", config_value_or(CONFIG, c("ranger", "tune_batch_size"), 1L)),
  min_value = 1
)
EFFECTIVE_TUNE_BATCH_SIZE <- min(TUNE_BATCH_SIZE, TUNE_EVALS)
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
  if (identical(OUTER_RESAMPLING, "stratified") && INNER_FOLDS > N_FOLDS) {
    stop("INNER_FOLDS must be less than or equal to N_FOLDS.", call. = FALSE)
  }
  if (identical(OUTER_RESAMPLING, "year_blocked") && OUTER_REPEATS > 1L) {
    stop("OUTER_REPEATS must be 1 when OUTER_RESAMPLING='year_blocked'.", call. = FALSE)
  }
  if (identical(OUTER_RESAMPLING, "year_blocked") && identical(OUTER_BLOCK_COL, TARGET)) {
    stop("OUTER_BLOCK_COL must not be the target column.", call. = FALSE)
  }

  set.seed(SEED)
  if (N_WORKERS > 1) {
    future::plan(future::multisession, workers = N_WORKERS)
  } else {
    future::plan(future::sequential)
  }
  on.exit(future::plan(future::sequential), add = TRUE)
  EFFECTIVE_FUTURE_WORKERS <- future::nbrOfWorkers()

  df <- load_csv_checked(DATA_PATH)
  work_dt <- prepare_modeling_data(
    df, TARGET, FEATURE_COLS, ID_COLS,
    row_filter = ROW_FILTER,
    extra_feature_cols = if (identical(OUTER_RESAMPLING, "year_blocked")) OUTER_BLOCK_COL else character(0),
    missing_drop_warn_fraction = MISSING_DROP_WARN_FRACTION
  )
  outer_block_values <- if (identical(OUTER_RESAMPLING, "year_blocked")) work_dt[[OUTER_BLOCK_COL]] else NULL
  model_dt <- if (identical(OUTER_RESAMPLING, "year_blocked") && !(OUTER_BLOCK_COL %in% FEATURE_COLS)) {
    work_dt[, setdiff(names(work_dt), OUTER_BLOCK_COL), with = FALSE]
  } else {
    work_dt
  }
  effective_outer_folds <- if (identical(OUTER_RESAMPLING, "year_blocked")) {
    year_plan <- make_year_blocked_fold_ids(outer_block_values, block_col = OUTER_BLOCK_COL)
    min_train_rows <- nrow(model_dt) - max(year_plan$fold_plan$n_rows)
    if (INNER_FOLDS > min_train_rows) {
      stop(
        "INNER_FOLDS must be less than or equal to the smallest year-blocked outer-training split (",
        min_train_rows, " row(s)).",
        call. = FALSE
      )
    }
    nrow(year_plan$fold_plan)
  } else {
    N_FOLDS
  }
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
    effective_outer_folds = effective_outer_folds,
    inner_folds = INNER_FOLDS,
    outer_repeats = OUTER_REPEATS,
    outer_resampling = OUTER_RESAMPLING,
    outer_block_col = if (identical(OUTER_RESAMPLING, "year_blocked")) OUTER_BLOCK_COL else NA_character_,
    strata_bins = STRATA_BINS,
    missing_drop_warn_fraction = MISSING_DROP_WARN_FRACTION,
    tune_evals = TUNE_EVALS,
    tune_batch_size = TUNE_BATCH_SIZE,
    effective_tune_batch_size = EFFECTIVE_TUNE_BATCH_SIZE,
    n_workers = N_WORKERS,
    future_workers = EFFECTIVE_FUTURE_WORKERS,
    ranger_num_threads = 1L
  )
  write_config_snapshot(OUTPUT_DIR, resolved_config)

  log_info("Using data file: ", normalizePath(DATA_PATH, mustWork = FALSE))
  log_info("Using output directory: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
  if (!is.na(RUN_NAME)) log_info("Run name: ", RUN_NAME)
  if (nzchar(trimws(ROW_FILTER))) log_info("Using row filter: ", ROW_FILTER)
  log_dataset_overview(
    model_dt,
    target = TARGET,
    feature_cols = FEATURE_COLS,
    id_cols = ID_COLS,
    metric = "rmse",
    extra = list(
      "Outer repeats" = OUTER_REPEATS,
      "Outer resampling" = OUTER_RESAMPLING,
      "Outer block column" = if (identical(OUTER_RESAMPLING, "year_blocked")) OUTER_BLOCK_COL else "<none>",
      "Outer folds / inner folds" = paste(effective_outer_folds, INNER_FOLDS, sep = " / "),
      "Missing-drop warn fraction" = if (is.na(MISSING_DROP_WARN_FRACTION)) "<disabled>" else MISSING_DROP_WARN_FRACTION,
      "Tuning evals" = TUNE_EVALS,
      "Tune batch size" = EFFECTIVE_TUNE_BATCH_SIZE,
      "Configured workers" = N_WORKERS,
      "Future workers" = EFFECTIVE_FUTURE_WORKERS,
      "Ranger num.threads" = 1L
    )
  )
  overview_dt <- dataset_overview(model_dt, target = TARGET, feature_cols = FEATURE_COLS, id_cols = ID_COLS)
  safe_write_csv(overview_dt, file.path(OUTPUT_DIR, "ranger_dataset_overview.csv"))

  backend <- add_regression_stratum(as.data.frame(model_dt), target = TARGET, n_bins = STRATA_BINS)
  task <- make_regr_task("ranger_regression", backend = backend, target = TARGET)

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
    tuner = tnr("random_search", batch_size = EFFECTIVE_TUNE_BATCH_SIZE),
    learner = learner,
    resampling = inner_cv,
    measure = msr("regr.rmse"),
    search_space = search_space,
    terminator = trm("evals", n_evals = TUNE_EVALS),
    store_tuning_instance = TRUE,
    store_models = FALSE
  )

  cv_run <- run_repeated_autotuner_outer_cv(
    task = task,
    auto_tuner = at,
    target = TARGET,
    n_folds = N_FOLDS,
    outer_repeats = OUTER_REPEATS,
    seed = SEED,
    n_bins = STRATA_BINS,
    progress_prefix = "Ranger",
    outer_resampling = OUTER_RESAMPLING,
    outer_block_values = outer_block_values,
    outer_block_col = OUTER_BLOCK_COL
  )
  predictions <- cv_run$predictions
  fold_metrics <- fold_metrics_from_predictions(predictions)
  overall_metrics <- aggregate_predictions(predictions)
  best_params <- cv_run$tuning_results

  safe_write_csv(fold_metrics, file.path(OUTPUT_DIR, "ranger_fold_metrics.csv"))
  safe_write_csv(overall_metrics, file.path(OUTPUT_DIR, "ranger_overall_metrics.csv"))
  safe_write_csv(predictions, file.path(OUTPUT_DIR, "ranger_cv_predictions.csv"))
  safe_write_csv(best_params, file.path(OUTPUT_DIR, "ranger_best_params.csv"))
  if (!is.null(cv_run$fold_plan) && nrow(cv_run$fold_plan) > 0) {
    safe_write_csv(cv_run$fold_plan, file.path(OUTPUT_DIR, "ranger_outer_fold_blocks.csv"))
  }

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
    sprintf("- outer_folds: `%s`", effective_outer_folds),
    sprintf("- inner_folds: `%s`", INNER_FOLDS),
    sprintf("- outer_repeats: `%s`", OUTER_REPEATS),
    sprintf("- outer_resampling: `%s`", OUTER_RESAMPLING),
    sprintf("- outer_block_col: `%s`", if (identical(OUTER_RESAMPLING, "year_blocked")) OUTER_BLOCK_COL else "<none>"),
    sprintf("- tune_evals: `%s`", TUNE_EVALS),
    sprintf("- tune_batch_size: `%s`", TUNE_BATCH_SIZE),
    sprintf("- effective_tune_batch_size: `%s`", EFFECTIVE_TUNE_BATCH_SIZE),
    sprintf("- workers: `%s`", N_WORKERS),
    sprintf("- future_workers: `%s`", EFFECTIVE_FUTURE_WORKERS),
    "- ranger_num_threads: `1`",
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
    "- `ranger_outer_fold_blocks.csv` / `.rds` when year-blocked outer validation is used",
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
  n_workers = N_WORKERS,
  run_name = RUN_NAME
))
