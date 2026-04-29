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

markdown_escape <- function(x) {
  x <- as.character(x)
  x <- gsub("\\|", "\\\\|", x)
  x <- gsub("\r?\n", " ", x)
  x
}

format_dictionary_rows <- function(dictionary_dt) {
  if (is.null(dictionary_dt) || nrow(dictionary_dt) == 0) return(character(0))
  vapply(seq_len(nrow(dictionary_dt)), function(i) {
    row <- dictionary_dt[i]
    sprintf(
      "| `%s` | `%s` | `%s` | `%s` | `%s` | `%s` |",
      markdown_escape(row$column[[1]]),
      markdown_escape(row$role[[1]]),
      markdown_escape(row$class[[1]]),
      markdown_escape(row$n_missing[[1]]),
      markdown_escape(sprintf("%.2f%%", 100 * row$pct_missing[[1]])),
      markdown_escape(row$n_unique[[1]])
    )
  }, character(1))
}

write_validation_report <- function(output_dir, checks_dt, resolved_config, dictionary_dt = NULL) {
  n_failed <- nrow(checks_dt[ok == FALSE])
  status <- if (n_failed == 0) "passed" else "failed"
  check_rows <- apply(checks_dt, 1, function(row) {
    sprintf(
      "| `%s` | `%s` | %s |",
      markdown_escape(row[["check"]]),
      markdown_escape(row[["ok"]]),
      markdown_escape(row[["details"]])
    )
  })
  dictionary_rows <- format_dictionary_rows(dictionary_dt)
  dictionary_section <- if (length(dictionary_rows) > 0) {
    c(
      "",
      "## Data Dictionary",
      "| column | role | class | missing | pct_missing | unique |",
      "|---|---|---|---:|---:|---:|",
      dictionary_rows
    )
  } else {
    character(0)
  }

  report_lines <- c(
    sprintf("# %s Report", SCRIPT_NAME),
    "",
    "## Status",
    sprintf("- status: `%s`", status),
    sprintf("- failed_checks: `%s`", n_failed),
    "",
    "## Run",
    sprintf("- data_path: `%s`", resolved_config$data_path),
    sprintf("- output_dir: `%s`", resolved_config$output_dir),
    sprintf("- target: `%s`", resolved_config$target),
    sprintf("- feature_cols: `%s`", paste(resolved_config$feature_cols, collapse = ", ")),
    sprintf("- id_cols: `%s`", if (length(resolved_config$id_cols) > 0) paste(resolved_config$id_cols, collapse = ", ") else "<none>"),
    sprintf("- row_filter: `%s`", if (nzchar(trimws(resolved_config$row_filter))) resolved_config$row_filter else "<none>"),
    sprintf("- outer_folds: `%s`", resolved_config$n_folds),
    sprintf("- inner_folds: `%s`", resolved_config$inner_folds),
    sprintf("- missing_drop_warn_fraction: `%s`", if (is.na(resolved_config$missing_drop_warn_fraction)) "<disabled>" else resolved_config$missing_drop_warn_fraction),
    "",
    "## Checks",
    "| check | ok | details |",
    "|---|---:|---|",
    check_rows,
    dictionary_section,
    "",
    "## Notes",
    "- Diagnostic warnings, such as small folds or low-information features, are written to `validate_repo.log`."
  )
  write_text_file(file.path(output_dir, "validation_report.md"), report_lines)
}

warn_if_target_extremes <- function(dt, target) {
  if (!target %in% names(dt) || !is.numeric(dt[[target]])) return(invisible(NULL))

  y <- dt[[target]]
  y_obs <- y[!is.na(y)]
  if (length(y_obs) == 0) return(invisible(NULL))

  zero_fraction <- mean(y_obs == 0)
  if (zero_fraction >= 0.5) {
    log_info(
      "Warning: Target '",
      target,
      "' has many zeros (",
      sprintf("%.2f%%", 100 * zero_fraction),
      " of non-missing rows). Check that the chosen model family and metrics match the zero-heavy target distribution."
    )
  }

  qs <- stats::quantile(y_obs, probs = c(0.25, 0.75), na.rm = TRUE, type = 7)
  iqr <- as.numeric(qs[[2]] - qs[[1]])
  if (is.finite(iqr) && iqr > 0) {
    upper_extreme_threshold <- as.numeric(qs[[2]] + 10 * iqr)
    max_y <- max(y_obs, na.rm = TRUE)
    if (is.finite(max_y) && max_y > upper_extreme_threshold) {
      log_info(
        "Warning: Target '",
        target,
        "' has a high maximum value (max=",
        format(max_y, digits = 8),
        ", Q3 + 10*IQR=",
        format(upper_extreme_threshold, digits = 8),
        "). Check whether extreme target values are expected real observations."
      )
    }
  }

  invisible(NULL)
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
  validation_dictionary <- NULL

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
    dictionary_dt <- data.table::as.data.table(data.table::copy(df))
    dictionary_filter <- apply_row_filter_checked(dictionary_dt, ROW_FILTER, label = "Validation data-dictionary row filter")
    dictionary_dt <- dictionary_filter$data
    dictionary_cols <- intersect(unique(c(TARGET, FEATURE_COLS, ID_COLS, zero_formula_cols)), names(dictionary_dt))
    dictionary_dt <- dictionary_dt[, ..dictionary_cols]
    validation_dictionary <<- data_dictionary(dictionary_dt, target = TARGET, feature_cols = FEATURE_COLS, id_cols = ID_COLS)
    safe_write_csv(validation_dictionary, file.path(OUTPUT_DIR, "validation_data_dictionary.csv"))

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
    warn_if_target_extremes(work_dt, TARGET)

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
  write_validation_report(OUTPUT_DIR, checks_dt, resolved_config, dictionary_dt = validation_dictionary)

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
