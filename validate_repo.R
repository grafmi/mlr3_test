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
SCRIPT_NAME <- "validate_repo"
SCRIPT_PACKAGES <- c("data.table")

suppressPackageStartupMessages(library(data.table))

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
DATA_PATH <- get_path_setting("data", "MLR3_DATA_PATH", config_value(CONFIG, c("experiment", "data_path")), base_dir = REPO_DIR)
OUTPUT_DIR <- get_path_setting("output-dir", "VALIDATION_OUTPUT_DIR", config_value(CONFIG, c("validation", "output_dir")), base_dir = REPO_DIR)
N_FOLDS <- get_int_setting("folds", "N_FOLDS", config_value(CONFIG, c("experiment", "n_folds")), min_value = 2)
INNER_FOLDS <- get_int_setting("inner-folds", "INNER_FOLDS", config_value(CONFIG, c("experiment", "inner_folds")), min_value = 2)
MISSING_DROP_WARN_FRACTION <- get_optional_numeric_setting(
  "missing-drop-warn-fraction", "MISSING_DROP_WARN_FRACTION",
  config_value_or(CONFIG, c("experiment", "missing_drop_warn_fraction"), 0.05),
  min_value = 0
)

required_packages <- c(
  "data.table", "mlr3", "mlr3learners", "mlr3tuning", "paradox",
  "bbotk", "future", "ranger", "xgboost", "pscl"
)

record_check <- function(name, ok, details) {
  data.table(check = name, ok = isTRUE(ok), details = as.character(details))
}

check_output_dir_writable <- function(path) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  probe <- file.path(path, ".write_probe")
  ok <- file.create(probe)
  if (isTRUE(ok)) unlink(probe)
  isTRUE(ok)
}

validate_zero_formula_rhs <- function(rhs, dt) {
  rhs_formula <- as.formula(sprintf("~ %s", rhs))
  missing_vars <- setdiff(all.vars(rhs_formula), names(dt))
  if (length(missing_vars) > 0) {
    stop("Missing zero-formula column(s): ", paste(missing_vars, collapse = ", "), call. = FALSE)
  }
  stats::model.frame(rhs_formula, data = as.data.frame(dt), na.action = stats::na.pass)
  invisible(TRUE)
}

.script_ok <- FALSE
LOG_STATE <- start_logging(OUTPUT_DIR, SCRIPT_NAME)
with_run_finalizer({
  if (!is.na(RUN_NAME)) log_info("Run name: ", RUN_NAME)
  resolved_config <- list(
    script_name = SCRIPT_NAME,
    run_name = RUN_NAME,
    data_path = normalizePath(DATA_PATH, mustWork = FALSE),
    output_dir = normalizePath(OUTPUT_DIR, mustWork = FALSE),
    target = TARGET,
    feature_cols = FEATURE_COLS,
    id_cols = ID_COLS,
    row_filter = ROW_FILTER,
    n_folds = N_FOLDS,
    inner_folds = INNER_FOLDS,
    missing_drop_warn_fraction = MISSING_DROP_WARN_FRACTION
  )
  write_config_snapshot(OUTPUT_DIR, resolved_config)
  checks <- list()

  missing_packages <- required_packages[!vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)]
  checks[[length(checks) + 1L]] <- record_check(
    "required_packages",
    length(missing_packages) == 0,
    if (length(missing_packages) == 0) "all required packages installed" else paste("missing package(s):", paste(missing_packages, collapse = ", "))
  )

  checks[[length(checks) + 1L]] <- record_check(
    "data_path_exists",
    file.exists(DATA_PATH),
    normalizePath(DATA_PATH, mustWork = FALSE)
  )

  checks[[length(checks) + 1L]] <- record_check(
    "validation_output_dir_writable",
    check_output_dir_writable(OUTPUT_DIR),
    normalizePath(OUTPUT_DIR, mustWork = FALSE)
  )

  checks[[length(checks) + 1L]] <- record_check(
    "inner_folds_not_greater_than_outer_folds",
    INNER_FOLDS <= N_FOLDS,
    sprintf("inner_folds=%s, n_folds=%s", INNER_FOLDS, N_FOLDS)
  )

  dataset_check <- tryCatch({
    df <- load_tabular_data_checked(DATA_PATH)
    zero_formula <- trimws(get_setting(
      "zero-formula", "ZINB_ZERO_FORMULA",
      config_value(CONFIG, c("zinb", "zero_inflation_formula"))
    ))
    zero_formula_cols <- if (identical(zero_formula, "same_as_count")) character(0) else {
      formula_referenced_columns(zero_formula, label = "ZINB zero-formula")
    }
    work_dt <- prepare_modeling_data(
      df, TARGET, FEATURE_COLS, ID_COLS,
      require_count_target = FALSE,
      row_filter = ROW_FILTER,
      extra_feature_cols = zero_formula_cols,
      missing_drop_warn_fraction = MISSING_DROP_WARN_FRACTION
    )

    checks_local <- list(
      record_check("modeling_data_loads", TRUE, sprintf("%s row(s), %s column(s)", nrow(work_dt), ncol(work_dt))),
      record_check("target_and_features_present", TRUE, paste(c(TARGET, FEATURE_COLS), collapse = ", "))
    )
    warn_if_small_cv_folds(nrow(work_dt), N_FOLDS, context = "modeling data")
    warn_if_low_information_features(work_dt, FEATURE_COLS, context = "modeling data")

    if (!identical(zero_formula, "same_as_count")) {
      validate_zero_formula_rhs(zero_formula, work_dt)
    }
    checks_local[[length(checks_local) + 1L]] <- record_check(
      "zinb_zero_formula_valid",
      TRUE,
      zero_formula
    )

    checks_local
  }, error = function(e) {
    list(record_check("modeling_data_validation", FALSE, conditionMessage(e)))
  })

  checks <- c(checks, dataset_check)
  checks_dt <- rbindlist(checks, fill = TRUE)
  safe_write_csv(checks_dt, file.path(OUTPUT_DIR, "validation_checks.csv"))

  failed_checks <- checks_dt[ok == FALSE]
  if (nrow(failed_checks) > 0) {
    for (i in seq_len(nrow(failed_checks))) {
      log_info("Validation failed: ", failed_checks$check[[i]], " - ", failed_checks$details[[i]])
    }
    stop("Repository validation failed. See validation_checks.csv for details.", call. = FALSE)
  }

  log_info("Repository validation completed successfully.")
  .script_ok <- TRUE
}, function() finalize_run(
  log_state = LOG_STATE,
  output_dir = OUTPUT_DIR,
  script_name = SCRIPT_NAME,
  repo_dir = REPO_DIR,
  packages = SCRIPT_PACKAGES,
  status = if (.script_ok) "completed" else "failed",
  data_path = DATA_PATH,
  feature_cols = FEATURE_COLS,
  run_name = RUN_NAME
))
