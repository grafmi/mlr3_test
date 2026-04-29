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

require_packages(c("data.table"))
suppressPackageStartupMessages(library(data.table))

R_SCRIPT <- Sys.which("Rscript")
if (!nzchar(R_SCRIPT)) {
  stop("Rscript was not found in PATH.", call. = FALSE)
}

TEST_OUTPUT_DIR <- get_path_setting("output-dir", "TEST_OUTPUT_DIR", "outputs_regression_tests", base_dir = REPO_DIR)
dir.create(TEST_OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

REGRESSION_CONFIG_PATH <- file.path(TEST_OUTPUT_DIR, "regression_test_config.R")
writeLines(
  c(
    sprintf(
      "sys.source(\"%s\", envir = environment())",
      gsub("\\\\", "/", file.path(REPO_DIR, "configs", "base_config.R"))
    ),
    "CONFIG$experiment$data_path <- \"testfile_zinb_nonlinear_eintritte.csv\"",
    "CONFIG$experiment$target <- \"n_eintritte\"",
    "CONFIG$experiment$feature_cols <- c(\"prcrank\", \"potenzielle_kunden\", \"unfalldeckung\")",
    "CONFIG$experiment$id_cols <- character(0)",
    "CONFIG$experiment$row_filter <- \"\"",
    "CONFIG$results$run_name <- \"\"",
    "CONFIG$results$version_runs <- FALSE"
  ),
  REGRESSION_CONFIG_PATH
)

record_test <- function(name, ok, details) {
  data.table(test = name, ok = isTRUE(ok), details = as.character(details))
}

merge_env <- function(base_env, extra_env) {
  parse_names <- function(values) sub("=.*$", "", values)
  if (length(extra_env) == 0) return(base_env)
  base_names <- parse_names(base_env)
  extra_names <- parse_names(extra_env)
  keep <- !(base_names %in% extra_names)
  c(base_env[keep], extra_env)
}

run_script <- function(script_name, args = character(0), env = character(0), workdir = REPO_DIR) {
  script_path <- file.path(REPO_DIR, script_name)
  output <- tempfile(pattern = "regression_test_", tmpdir = TEST_OUTPUT_DIR, fileext = ".txt")
  base_env <- c(sprintf("CONFIG_PATH=%s", REGRESSION_CONFIG_PATH))
  status <- system2(
    R_SCRIPT,
    c(script_path, args),
    stdout = output,
    stderr = output,
    env = merge_env(base_env, env),
    wait = TRUE
  )
  list(
    status = status,
    output = if (file.exists(output)) paste(readLines(output, warn = FALSE), collapse = "\n") else ""
  )
}

tests <- list()

# validate_repo.R should succeed and write expected artifacts
validate_out <- file.path(TEST_OUTPUT_DIR, "validate_repo")
unlink(validate_out, recursive = TRUE, force = TRUE)
validate_run <- run_script("validate_repo.R", args = c(sprintf("--output-dir=%s", validate_out)))
validate_checks_path <- file.path(validate_out, "validation_checks.csv")
validate_log_path <- file.path(validate_out, "validate_repo.log")
validate_manifest_path <- file.path(validate_out, "run_manifest.csv")
validate_config_path <- file.path(validate_out, "resolved_config.rds")
validate_report_path <- file.path(validate_out, "validation_report.md")
validate_report <- if (file.exists(validate_report_path)) {
  paste(readLines(validate_report_path, warn = FALSE), collapse = "\n")
} else {
  ""
}

validate_ok <- validate_run$status == 0 &&
  file.exists(validate_checks_path) &&
  file.exists(validate_log_path) &&
  file.exists(validate_manifest_path) &&
  file.exists(validate_config_path) &&
  file.exists(validate_report_path) &&
  grepl("# validate_repo Report", validate_report, fixed = TRUE) &&
  grepl("| `required_packages` | `TRUE` |", validate_report, fixed = TRUE)

tests[[length(tests) + 1L]] <- record_test(
  "validate_repo_success",
  validate_ok,
  if (validate_ok) {
    "validate_repo.R completed and wrote checks, log, manifest, config snapshot, and report"
  } else {
    paste("validate_repo.R failed:", validate_run$output)
  }
)

# run_all.sh should validate before running model scripts
run_all_lines <- readLines(file.path(REPO_DIR, "run_all.sh"), warn = FALSE)
validation_step <- grep("Running repository validation", run_all_lines, fixed = TRUE)
ranger_step <- grep("Running ranger tuning", run_all_lines, fixed = TRUE)
run_all_preflight_ok <- length(validation_step) == 1L &&
  length(ranger_step) == 1L &&
  validation_step < ranger_step &&
  any(grepl("^export VALIDATION_OUTPUT_DIR$", run_all_lines))

tests[[length(tests) + 1L]] <- record_test(
  "run_all_runs_validation_before_models",
  run_all_preflight_ok,
  if (run_all_preflight_ok) {
    "run_all.sh runs validate_repo.R before model scripts and exports validation output"
  } else {
    "run_all.sh does not appear to run validation before model scripts"
  }
)

# Failed ranger run should still write log and manifest
ranger_fail_out <- file.path(TEST_OUTPUT_DIR, "ranger_failed_run")
unlink(ranger_fail_out, recursive = TRUE, force = TRUE)
ranger_fail_run <- run_script(
  "mlr3_ranger_tuning.R",
  args = c("--data=/tmp/does_not_exist.csv", sprintf("--output-dir=%s", ranger_fail_out))
)
ranger_manifest <- read_csv_if_exists(file.path(ranger_fail_out, "run_manifest.csv"))
ranger_log_path <- file.path(ranger_fail_out, "mlr3_ranger_tuning.log")
ranger_fail_ok <- ranger_fail_run$status != 0 &&
  !is.null(ranger_manifest) &&
  nrow(ranger_manifest) == 1 &&
  identical(as.character(ranger_manifest$status[[1]]), "failed") &&
  file.exists(ranger_log_path)

tests[[length(tests) + 1L]] <- record_test(
  "failed_run_writes_manifest",
  ranger_fail_ok,
  if (ranger_fail_ok) {
    "failed ranger run wrote run_manifest.csv with status=failed"
  } else {
    paste("failed ranger run did not produce expected outputs:", ranger_fail_run$output)
  }
)

# Repeated outer CV should add repeat metadata without changing overall metrics
ranger_repeat_out <- file.path(TEST_OUTPUT_DIR, "ranger_repeated_run")
unlink(ranger_repeat_out, recursive = TRUE, force = TRUE)
ranger_repeat_run <- run_script(
  "mlr3_ranger_tuning.R",
  args = c(
    sprintf("--output-dir=%s", ranger_repeat_out),
    "--folds=2",
    "--inner-folds=2",
    "--outer-repeats=2",
    "--tune-evals=2",
    "--workers=1"
  )
)
ranger_repeat_predictions <- read_csv_if_exists(file.path(ranger_repeat_out, "ranger_cv_predictions.csv"))
ranger_repeat_fold_metrics <- read_csv_if_exists(file.path(ranger_repeat_out, "ranger_fold_metrics.csv"))
ranger_repeat_best_params <- read_csv_if_exists(file.path(ranger_repeat_out, "ranger_best_params.csv"))
ranger_repeat_overall_metrics <- read_csv_if_exists(file.path(ranger_repeat_out, "ranger_overall_metrics.csv"))
ranger_repeat_ok <- ranger_repeat_run$status == 0 &&
  !is.null(ranger_repeat_predictions) &&
  !is.null(ranger_repeat_fold_metrics) &&
  !is.null(ranger_repeat_best_params) &&
  !is.null(ranger_repeat_overall_metrics) &&
  all(c("repeat", "fold") %in% names(ranger_repeat_fold_metrics)) &&
  "repeat" %in% names(ranger_repeat_predictions) &&
  "repeat" %in% names(ranger_repeat_best_params) &&
  !("repeat" %in% names(ranger_repeat_overall_metrics)) &&
  length(unique(ranger_repeat_predictions[["repeat"]])) == 2L

tests[[length(tests) + 1L]] <- record_test(
  "repeated_outer_cv_writes_repeat_metadata",
  ranger_repeat_ok,
  if (ranger_repeat_ok) {
    "repeated outer CV added repeat metadata while keeping overall metrics schema stable"
  } else {
    paste("repeated ranger run did not produce expected repeat-aware outputs:", ranger_repeat_run$output)
  }
)

# validate_repo.R should warn when missing-value row drops exceed the threshold
missing_warn_fixture_dir <- file.path(TEST_OUTPUT_DIR, "missing_warn_fixture")
unlink(missing_warn_fixture_dir, recursive = TRUE, force = TRUE)
dir.create(missing_warn_fixture_dir, recursive = TRUE, showWarnings = FALSE)

missing_warn_input <- file.path(missing_warn_fixture_dir, "input.csv")
safe_write_csv(
  data.table(
    target = c(1, 2, 3, 4),
    feature_a = c(10, NA, NA, 40)
  ),
  missing_warn_input
)

missing_warn_out <- file.path(missing_warn_fixture_dir, "outputs")
missing_warn_run <- run_script(
  "validate_repo.R",
  args = c(
    sprintf("--data=%s", missing_warn_input),
    sprintf("--output-dir=%s", missing_warn_out),
    "--target=target",
    "--features=feature_a",
    "--missing-drop-warn-fraction=0.25"
  )
)
missing_warn_log_path <- file.path(missing_warn_out, "validate_repo.log")
missing_warn_log <- if (file.exists(missing_warn_log_path)) {
  paste(readLines(missing_warn_log_path, warn = FALSE), collapse = "\n")
} else {
  ""
}
missing_warn_ok <- missing_warn_run$status == 0 &&
  grepl("Warning: Dropped 2 of 4 row\\(s\\) with missing values in modeling data", missing_warn_log)

tests[[length(tests) + 1L]] <- record_test(
  "validate_repo_warns_on_high_missing_row_drop",
  missing_warn_ok,
  if (missing_warn_ok) {
    "validate_repo.R warns when modeling na.omit drops exceed the configured threshold"
  } else {
    paste("validate_repo.R did not emit the expected missing-drop warning:", missing_warn_run$output)
  }
)

# validate_repo.R should warn when ID columns have repeated values
duplicate_id_fixture_dir <- file.path(TEST_OUTPUT_DIR, "duplicate_id_fixture")
unlink(duplicate_id_fixture_dir, recursive = TRUE, force = TRUE)
dir.create(duplicate_id_fixture_dir, recursive = TRUE, showWarnings = FALSE)

duplicate_id_input <- file.path(duplicate_id_fixture_dir, "input.csv")
safe_write_csv(
  data.table(
    target = c(1, 2, 3, 4),
    feature_a = c(10, 20, 30, 40),
    customer_id = c("a", "a", "b", "c")
  ),
  duplicate_id_input
)

duplicate_id_out <- file.path(duplicate_id_fixture_dir, "outputs")
duplicate_id_run <- run_script(
  "validate_repo.R",
  args = c(
    sprintf("--data=%s", duplicate_id_input),
    sprintf("--output-dir=%s", duplicate_id_out),
    "--target=target",
    "--features=feature_a",
    "--id-cols=customer_id"
  )
)
duplicate_id_log_path <- file.path(duplicate_id_out, "validate_repo.log")
duplicate_id_log <- if (file.exists(duplicate_id_log_path)) {
  paste(readLines(duplicate_id_log_path, warn = FALSE), collapse = "\n")
} else {
  ""
}
duplicate_id_ok <- duplicate_id_run$status == 0 &&
  grepl("Warning: ID column\\(s\\) have repeated value combinations", duplicate_id_log) &&
  grepl("Standard row-wise CV may leak grouped entities", duplicate_id_log)

tests[[length(tests) + 1L]] <- record_test(
  "validate_repo_warns_on_duplicate_id_values",
  duplicate_id_ok,
  if (duplicate_id_ok) {
    "validate_repo.R warns when ID columns suggest possible grouped-CV leakage"
  } else {
    paste("validate_repo.R did not emit the expected duplicate-ID warning:", duplicate_id_run$output)
  }
)

# validate_repo.R should warn when validation folds are small
small_fold_fixture_dir <- file.path(TEST_OUTPUT_DIR, "small_fold_fixture")
unlink(small_fold_fixture_dir, recursive = TRUE, force = TRUE)
dir.create(small_fold_fixture_dir, recursive = TRUE, showWarnings = FALSE)

small_fold_input <- file.path(small_fold_fixture_dir, "input.csv")
safe_write_csv(
  data.table(
    target = seq_len(20),
    feature_a = seq_len(20)
  ),
  small_fold_input
)

small_fold_out <- file.path(small_fold_fixture_dir, "outputs")
small_fold_run <- run_script(
  "validate_repo.R",
  args = c(
    sprintf("--data=%s", small_fold_input),
    sprintf("--output-dir=%s", small_fold_out),
    "--target=target",
    "--features=feature_a",
    "--folds=10",
    "--inner-folds=5"
  )
)
small_fold_log_path <- file.path(small_fold_out, "validate_repo.log")
small_fold_log <- if (file.exists(small_fold_log_path)) {
  paste(readLines(small_fold_log_path, warn = FALSE), collapse = "\n")
} else {
  ""
}
small_fold_ok <- small_fold_run$status == 0 &&
  grepl("Warning: Small validation folds for modeling data", small_fold_log)

tests[[length(tests) + 1L]] <- record_test(
  "validate_repo_warns_on_small_validation_folds",
  small_fold_ok,
  if (small_fold_ok) {
    "validate_repo.R warns when fold sizes may make CV metrics unstable"
  } else {
    paste("validate_repo.R did not emit the expected small-fold warning:", small_fold_run$output)
  }
)

# validate_repo.R should warn about constant and near-constant features
low_info_fixture_dir <- file.path(TEST_OUTPUT_DIR, "low_info_fixture")
unlink(low_info_fixture_dir, recursive = TRUE, force = TRUE)
dir.create(low_info_fixture_dir, recursive = TRUE, showWarnings = FALSE)

low_info_input <- file.path(low_info_fixture_dir, "input.csv")
safe_write_csv(
  data.table(
    target = seq_len(20),
    constant_feature = rep(1, 20),
    near_constant_feature = c(rep(0, 19), 1)
  ),
  low_info_input
)

low_info_out <- file.path(low_info_fixture_dir, "outputs")
low_info_run <- run_script(
  "validate_repo.R",
  args = c(
    sprintf("--data=%s", low_info_input),
    sprintf("--output-dir=%s", low_info_out),
    "--target=target",
    "--features=constant_feature,near_constant_feature",
    "--folds=2",
    "--inner-folds=2"
  )
)
low_info_log_path <- file.path(low_info_out, "validate_repo.log")
low_info_log <- if (file.exists(low_info_log_path)) {
  paste(readLines(low_info_log_path, warn = FALSE), collapse = "\n")
} else {
  ""
}
low_info_ok <- low_info_run$status == 0 &&
  grepl("Warning: Constant feature\\(s\\) found in modeling data: constant_feature", low_info_log) &&
  grepl("Warning: Near-constant numeric feature\\(s\\) found in modeling data: near_constant_feature", low_info_log)

tests[[length(tests) + 1L]] <- record_test(
  "validate_repo_warns_on_low_information_features",
  low_info_ok,
  if (low_info_ok) {
    "validate_repo.R warns about constant and near-constant predictors"
  } else {
    paste("validate_repo.R did not emit the expected low-information feature warnings:", low_info_run$output)
  }
)

# mlr3_xgb_tuning.R should run with factor features using fold-local encoding
xgb_factor_fixture_dir <- file.path(TEST_OUTPUT_DIR, "xgb_factor_fixture")
unlink(xgb_factor_fixture_dir, recursive = TRUE, force = TRUE)
dir.create(xgb_factor_fixture_dir, recursive = TRUE, showWarnings = FALSE)

xgb_factor_input <- file.path(xgb_factor_fixture_dir, "input.csv")
safe_write_csv(
  data.table(
    target = rep(0:5, length.out = 30),
    region = rep(c("north", "south", "west"), each = 10),
    exposure = seq(1, 30)
  ),
  xgb_factor_input
)

xgb_factor_out <- file.path(xgb_factor_fixture_dir, "outputs")
xgb_factor_run <- run_script(
  "mlr3_xgb_tuning.R",
  args = c(
    sprintf("--data=%s", xgb_factor_input),
    sprintf("--output-dir=%s", xgb_factor_out),
    "--target=target",
    "--features=region,exposure",
    "--folds=2",
    "--inner-folds=2",
    "--tune-evals=1",
    "--workers=1"
  )
)
xgb_factor_predictions <- read_csv_if_exists(file.path(xgb_factor_out, "xgb_cv_predictions.csv"))
xgb_factor_overall <- read_csv_if_exists(file.path(xgb_factor_out, "xgb_overall_metrics.csv"))
xgb_factor_report <- file.path(xgb_factor_out, "xgb_model_report.md")
xgb_factor_report_text <- if (file.exists(xgb_factor_report)) {
  paste(readLines(xgb_factor_report, warn = FALSE), collapse = "\n")
} else {
  ""
}
xgb_factor_ok <- xgb_factor_run$status == 0 &&
  !is.null(xgb_factor_predictions) &&
  !is.null(xgb_factor_overall) &&
  nrow(xgb_factor_predictions) == 30 &&
  grepl("one-hot encoding is learned separately inside each outer-CV training split", xgb_factor_report_text, fixed = TRUE)

tests[[length(tests) + 1L]] <- record_test(
  "xgb_uses_fold_local_encoding_for_factor_features",
  xgb_factor_ok,
  if (xgb_factor_ok) {
    "mlr3_xgb_tuning.R runs factor features through fold-local one-hot encoding"
  } else {
    paste("mlr3_xgb_tuning.R did not complete factor-feature run:", xgb_factor_run$output)
  }
)

# compare_best_models.R should report availability for missing/failed outputs
compare_fixture_root <- file.path(TEST_OUTPUT_DIR, "compare_fixture")
unlink(compare_fixture_root, recursive = TRUE, force = TRUE)
dir.create(file.path(compare_fixture_root, "outputs_ranger"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(compare_fixture_root, "outputs_xgb_failed"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(compare_fixture_root, "outputs_model_comparison"), recursive = TRUE, showWarnings = FALSE)

safe_write_csv(
  data.table(script_name = "mlr3_ranger_tuning", status = "completed"),
  file.path(compare_fixture_root, "outputs_ranger", "run_manifest.csv")
)
safe_write_csv(
  data.table(
    rmse = 1.2, mae = 0.8, max_error = 2.0, sae = 0.1, mse = 1.44,
    bias = 0.05, r2 = 0.3, poisson_deviance = 0.9, negloglik = NA_real_
  ),
  file.path(compare_fixture_root, "outputs_ranger", "ranger_overall_metrics.csv")
)
safe_write_csv(
  data.table("repeat" = 1L, num.trees = 500L, regr.rmse = 1.2),
  file.path(compare_fixture_root, "outputs_ranger", "ranger_best_params.csv")
)
safe_write_csv(
  data.table(script_name = "mlr3_xgb_tuning", status = "failed"),
  file.path(compare_fixture_root, "outputs_xgb_failed", "run_manifest.csv")
)
safe_write_csv(
  data.table(
    rmse = 0.1, mae = 0.1, max_error = 0.2, sae = 0.0, mse = 0.01,
    bias = 0.0, r2 = 0.99, poisson_deviance = 0.1, negloglik = NA_real_
  ),
  file.path(compare_fixture_root, "outputs_xgb_failed", "xgb_overall_metrics.csv")
)
safe_write_csv(
  data.table(nrounds = 100L, regr.rmse = 0.1),
  file.path(compare_fixture_root, "outputs_xgb_failed", "xgb_best_params.csv")
)

compare_run <- run_script(
  "compare_best_models.R",
  args = c(
    sprintf("--ranger-dir=%s", file.path(compare_fixture_root, "outputs_ranger")),
    sprintf("--xgb-dir=%s", file.path(compare_fixture_root, "outputs_xgb_failed")),
    sprintf("--zinb-dir=%s", file.path(compare_fixture_root, "outputs_zinb_missing")),
    sprintf("--output-dir=%s", file.path(compare_fixture_root, "outputs_model_comparison"))
  )
)
comparison_dt <- read_csv_if_exists(file.path(compare_fixture_root, "outputs_model_comparison", "best_models_comparison.csv"))
compare_ok <- compare_run$status == 0 &&
  !is.null(comparison_dt) &&
  all(c("availability_status", "availability_reason", "manifest_status") %in% names(comparison_dt)) &&
  "missing_directory" %in% comparison_dt$availability_status &&
  "failed_run" %in% comparison_dt$availability_status &&
  identical(as.integer(comparison_dt[model == "ranger"]$rank[[1]]), 1L) &&
  is.na(comparison_dt[model == "xgb"]$rank[[1]])

tests[[length(tests) + 1L]] <- record_test(
  "compare_best_models_reports_unavailable_inputs_without_ranking_them",
  compare_ok,
  if (compare_ok) {
    "comparison output reports unavailable models explicitly and excludes them from ranking"
  } else {
    paste("compare_best_models.R did not produce expected availability diagnostics:", compare_run$output)
  }
)

# compare_best_models.R should allow a valid baseline-only ZINB run
zinb_baseline_fixture_root <- file.path(TEST_OUTPUT_DIR, "compare_zinb_baseline_fixture")
unlink(zinb_baseline_fixture_root, recursive = TRUE, force = TRUE)
dir.create(file.path(zinb_baseline_fixture_root, "outputs_ranger"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(zinb_baseline_fixture_root, "outputs_xgb"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(zinb_baseline_fixture_root, "outputs_zinb"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(zinb_baseline_fixture_root, "outputs_model_comparison"), recursive = TRUE, showWarnings = FALSE)

safe_write_csv(
  data.table(script_name = "zinb_stepwise_cv", status = "completed"),
  file.path(zinb_baseline_fixture_root, "outputs_zinb", "run_manifest.csv")
)
safe_write_csv(
  data.table(
    rmse = 1.1, mae = 0.7, max_error = 2.0, sae = 0.0, mse = 1.21,
    bias = 0.0, r2 = 0.2, poisson_deviance = 0.8, negloglik = 1.5
  ),
  file.path(zinb_baseline_fixture_root, "outputs_zinb", "zinb_best_global_overall_metrics.csv")
)
safe_write_csv(
  data.table(
    formula = "target ~ 1 | 1",
    selected_terms = NA_character_,
    stop_reason = "baseline_retained"
  ),
  file.path(zinb_baseline_fixture_root, "outputs_zinb", "zinb_final_model_summary.csv")
)

zinb_baseline_compare_run <- run_script(
  "compare_best_models.R",
  args = c(
    sprintf("--ranger-dir=%s", file.path(zinb_baseline_fixture_root, "outputs_ranger")),
    sprintf("--xgb-dir=%s", file.path(zinb_baseline_fixture_root, "outputs_xgb")),
    sprintf("--zinb-dir=%s", file.path(zinb_baseline_fixture_root, "outputs_zinb")),
    sprintf("--output-dir=%s", file.path(zinb_baseline_fixture_root, "outputs_model_comparison"))
  )
)
zinb_baseline_comparison <- read_csv_if_exists(file.path(zinb_baseline_fixture_root, "outputs_model_comparison", "best_models_comparison.csv"))
zinb_baseline_ok <- zinb_baseline_compare_run$status == 0 &&
  !is.null(zinb_baseline_comparison) &&
  identical(as.character(zinb_baseline_comparison[model == "zinb"]$availability_status[[1]]), "ok") &&
  identical(as.integer(zinb_baseline_comparison[model == "zinb"]$rank[[1]]), 1L) &&
  grepl("<baseline>", zinb_baseline_comparison[model == "zinb"]$details[[1]], fixed = TRUE)

tests[[length(tests) + 1L]] <- record_test(
  "compare_best_models_accepts_baseline_only_zinb",
  zinb_baseline_ok,
  if (zinb_baseline_ok) {
    "baseline-only ZINB outputs are considered complete and comparable"
  } else {
    paste("compare_best_models.R did not accept baseline-only ZINB outputs:", zinb_baseline_compare_run$output)
  }
)

# write_run_summary.R should create summary files from run manifests
summary_fixture_root <- file.path(TEST_OUTPUT_DIR, "run_summary_fixture")
unlink(summary_fixture_root, recursive = TRUE, force = TRUE)
dir.create(file.path(summary_fixture_root, "outputs_ranger"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(summary_fixture_root, "outputs_model_comparison"), recursive = TRUE, showWarnings = FALSE)

safe_write_csv(
  data.table(
    script_name = "mlr3_ranger_tuning",
    status = "completed",
    start_time = "2026-04-23 10:00:00 CEST",
    end_time = "2026-04-23 10:01:00 CEST",
    runtime_seconds = 60
  ),
  file.path(summary_fixture_root, "outputs_ranger", "run_manifest.csv")
)
safe_write_csv(
  data.table(
    script_name = "compare_best_models",
    status = "completed",
    start_time = "2026-04-23 10:01:00 CEST",
    end_time = "2026-04-23 10:01:05 CEST",
    runtime_seconds = 5
  ),
  file.path(summary_fixture_root, "outputs_model_comparison", "run_manifest.csv")
)
safe_write_csv(
  data.table(rank = 1, model = "ranger", rmse = 1.2, mae = 0.8, r2 = 0.3),
  file.path(summary_fixture_root, "outputs_model_comparison", "best_models_comparison.csv")
)

summary_run <- run_script(
  "write_run_summary.R",
  env = c(sprintf("RUN_OUTPUT_ROOT=%s", summary_fixture_root))
)
summary_dt <- read_csv_if_exists(file.path(summary_fixture_root, "run_summary.csv"))
summary_scripts_dt <- read_csv_if_exists(file.path(summary_fixture_root, "run_summary_scripts.csv"))
summary_ok <- summary_run$status == 0 &&
  !is.null(summary_dt) &&
  !is.null(summary_scripts_dt) &&
  nrow(summary_dt) == 1 &&
  "winner_model" %in% names(summary_dt) &&
  identical(as.character(summary_dt$winner_model[[1]]), "ranger")

tests[[length(tests) + 1L]] <- record_test(
  "write_run_summary_creates_expected_files",
  summary_ok,
  if (summary_ok) {
    "run_summary.csv and run_summary_scripts.csv were created"
  } else {
    paste("write_run_summary.R did not create expected summary files:", summary_run$output)
  }
)

# preprocess_data.R should report row removals separately for filter and na.omit()
preprocess_fixture_dir <- file.path(TEST_OUTPUT_DIR, "preprocess_fixture")
unlink(preprocess_fixture_dir, recursive = TRUE, force = TRUE)
dir.create(preprocess_fixture_dir, recursive = TRUE, showWarnings = FALSE)

preprocess_input <- file.path(preprocess_fixture_dir, "input.csv")
safe_write_csv(
  data.table(
    id = 1:5,
    keep_flag = c(TRUE, TRUE, FALSE, TRUE, FALSE),
    x = c(10, NA, 30, 40, 50),
    y = c("a", "b", "c", "d", "e")
  ),
  preprocess_input
)

preprocess_out <- file.path(preprocess_fixture_dir, "outputs")
preprocess_run <- run_script(
  "preprocess_data.R",
  args = c(
    sprintf("--input=%s", preprocess_input),
    sprintf("--output-dir=%s", preprocess_out),
    "--filter=keep_flag == TRUE",
    "--drop-missing-rows=true"
  )
)
preprocess_summary <- read_csv_if_exists(file.path(preprocess_out, "preprocessed_dataset_metadata_summary.csv"))
preprocess_log_path <- file.path(preprocess_out, "preprocess_data.log")
preprocess_log <- if (file.exists(preprocess_log_path)) {
  paste(readLines(preprocess_log_path, warn = FALSE), collapse = "\n")
} else {
  ""
}

preprocess_ok <- preprocess_run$status == 0 &&
  !is.null(preprocess_summary) &&
  nrow(preprocess_summary) == 1 &&
  "rows_removed_by_filter" %in% names(preprocess_summary) &&
  "rows_removed_by_na_omit" %in% names(preprocess_summary) &&
  identical(as.integer(preprocess_summary$rows_removed_by_filter[[1]]), 2L) &&
  identical(as.integer(preprocess_summary$rows_removed_by_na_omit[[1]]), 1L) &&
  grepl("Dropped 2 row\\(s\\) via row filter during preprocessing\\.", preprocess_log) &&
  grepl("Dropped 1 row\\(s\\) with missing values during preprocessing\\.", preprocess_log)

tests[[length(tests) + 1L]] <- record_test(
  "preprocess_reports_filter_and_na_omit_rows_separately",
  preprocess_ok,
  if (preprocess_ok) {
    "preprocess metadata and log separate filter and na.omit row removals"
  } else {
    paste("preprocess_data.R did not report separate row removals as expected:", preprocess_run$output)
  }
)

# preprocess_data.R should support reproducible random subsampling
preprocess_sample_fixture_dir <- file.path(TEST_OUTPUT_DIR, "preprocess_sample_fixture")
unlink(preprocess_sample_fixture_dir, recursive = TRUE, force = TRUE)
dir.create(preprocess_sample_fixture_dir, recursive = TRUE, showWarnings = FALSE)

preprocess_sample_input <- file.path(preprocess_sample_fixture_dir, "input.csv")
safe_write_csv(
  data.table(
    id = 1:6,
    x = seq(10, 60, by = 10),
    y = letters[1:6]
  ),
  preprocess_sample_input
)

preprocess_sample_out <- file.path(preprocess_sample_fixture_dir, "outputs")
preprocess_sample_run <- run_script(
  "preprocess_data.R",
  args = c(
    sprintf("--input=%s", preprocess_sample_input),
    sprintf("--output-dir=%s", preprocess_sample_out),
    "--sample-rows=3",
    "--sample-seed=7"
  )
)
preprocess_sample_dt <- read_csv_if_exists(file.path(preprocess_sample_out, "preprocessed_dataset.csv"))
preprocess_sample_summary <- read_csv_if_exists(file.path(preprocess_sample_out, "preprocessed_dataset_metadata_summary.csv"))
preprocess_sample_log_path <- file.path(preprocess_sample_out, "preprocess_data.log")
preprocess_sample_log <- if (file.exists(preprocess_sample_log_path)) {
  paste(readLines(preprocess_sample_log_path, warn = FALSE), collapse = "\n")
} else {
  ""
}

preprocess_sample_ok <- preprocess_sample_run$status == 0 &&
  !is.null(preprocess_sample_dt) &&
  !is.null(preprocess_sample_summary) &&
  nrow(preprocess_sample_dt) == 3 &&
  identical(as.integer(preprocess_sample_dt$id), c(2L, 3L, 5L)) &&
  identical(as.integer(preprocess_sample_summary$sample_rows[[1]]), 3L) &&
  identical(as.integer(preprocess_sample_summary$sample_seed[[1]]), 7L) &&
  identical(as.integer(preprocess_sample_summary$rows_removed_by_subsampling[[1]]), 3L) &&
  grepl("Using random subset rows: 3", preprocess_sample_log) &&
  grepl("Dropped 3 row\\(s\\) via random subsampling during preprocessing\\.", preprocess_sample_log)

tests[[length(tests) + 1L]] <- record_test(
  "preprocess_supports_random_subsampling",
  preprocess_sample_ok,
  if (preprocess_sample_ok) {
    "preprocess_data.R supports reproducible random subsampling"
  } else {
    paste("preprocess_data.R did not produce the expected random subset outputs:", preprocess_sample_run$output)
  }
)

# validate_repo.R should allow modeling row filters on non-feature columns
row_filter_fixture_dir <- file.path(TEST_OUTPUT_DIR, "row_filter_fixture")
unlink(row_filter_fixture_dir, recursive = TRUE, force = TRUE)
dir.create(row_filter_fixture_dir, recursive = TRUE, showWarnings = FALSE)

row_filter_input <- file.path(row_filter_fixture_dir, "input.csv")
safe_write_csv(
  data.table(
    target = c(1, 2, 3, 4),
    feature_a = c(10, 20, 30, 40),
    keep_group = c(1, 0, 1, 0)
  ),
  row_filter_input
)

row_filter_out <- file.path(row_filter_fixture_dir, "outputs")
row_filter_run <- run_script(
  "validate_repo.R",
  args = c(
    sprintf("--data=%s", row_filter_input),
    sprintf("--output-dir=%s", row_filter_out),
    "--target=target",
    "--features=feature_a"
  ),
  env = c("ROW_FILTER=keep_group==1")
)
row_filter_checks <- read_csv_if_exists(file.path(row_filter_out, "validation_checks.csv"))
row_filter_log_path <- file.path(row_filter_out, "validate_repo.log")
row_filter_log <- if (file.exists(row_filter_log_path)) {
  paste(readLines(row_filter_log_path, warn = FALSE), collapse = "\n")
} else {
  ""
}

row_filter_ok <- row_filter_run$status == 0 &&
  !is.null(row_filter_checks) &&
  nrow(row_filter_checks[check == "modeling_data_loads" & ok == TRUE]) == 1 &&
  grepl("Dropped 2 row\\(s\\) via modeling row filter\\.", row_filter_log)

tests[[length(tests) + 1L]] <- record_test(
  "validate_repo_allows_row_filter_on_non_feature_columns",
  row_filter_ok,
  if (row_filter_ok) {
    "validate_repo.R accepts row filtering on columns outside FEATURE_COLS"
  } else {
    paste("validate_repo.R did not allow row filtering on non-feature columns:", row_filter_run$output)
  }
)

# validate_repo.R should allow ZINB zero-formula columns outside FEATURE_COLS
zero_formula_fixture_dir <- file.path(TEST_OUTPUT_DIR, "zero_formula_fixture")
unlink(zero_formula_fixture_dir, recursive = TRUE, force = TRUE)
dir.create(zero_formula_fixture_dir, recursive = TRUE, showWarnings = FALSE)

zero_formula_input <- file.path(zero_formula_fixture_dir, "input.csv")
safe_write_csv(
  data.table(
    target = c(1, 2, 3, 4),
    feature_a = c(10, 20, 30, 40),
    keep_group = c(1, 0, 1, 0)
  ),
  zero_formula_input
)

zero_formula_out <- file.path(zero_formula_fixture_dir, "outputs")
zero_formula_run <- run_script(
  "validate_repo.R",
  args = c(
    sprintf("--data=%s", zero_formula_input),
    sprintf("--output-dir=%s", zero_formula_out),
    "--target=target",
    "--features=feature_a"
  ),
  env = c(
    "ROW_FILTER=keep_group==1",
    "ZINB_ZERO_FORMULA=keep_group"
  )
)
zero_formula_checks <- read_csv_if_exists(file.path(zero_formula_out, "validation_checks.csv"))
zero_formula_ok <- zero_formula_run$status == 0 &&
  !is.null(zero_formula_checks) &&
  nrow(zero_formula_checks[check == "zinb_zero_formula_valid" & ok == TRUE]) == 1

tests[[length(tests) + 1L]] <- record_test(
  "validate_repo_allows_zero_formula_on_non_feature_columns",
  zero_formula_ok,
  if (zero_formula_ok) {
    "validate_repo.R accepts ZINB zero-formula columns outside FEATURE_COLS"
  } else {
    paste("validate_repo.R did not allow a non-feature zero-formula column:", zero_formula_run$output)
  }
)

# zinb_stepwise_cv.R should consider factor() candidates for low-cardinality numeric features
zinb_factor_fixture_dir <- file.path(TEST_OUTPUT_DIR, "zinb_factor_fixture")
unlink(zinb_factor_fixture_dir, recursive = TRUE, force = TRUE)
dir.create(zinb_factor_fixture_dir, recursive = TRUE, showWarnings = FALSE)

zinb_factor_input <- file.path(zinb_factor_fixture_dir, "input.csv")
safe_write_csv(
  data.table(
    target = c(0, 1, 2, 0, 1, 3, 0, 2, 1, 4, 0, 3, 1, 2, 3, 0, 1, 2, 1, 3, 0, 2, 1, 4),
    age_band_num = rep(c(1, 2, 3), length.out = 24),
    exposure = c(10, 12, 14, 11, 13, 15, 10, 12, 14, 11, 13, 15, 16, 18, 20, 17, 19, 21, 16, 18, 20, 17, 19, 21)
  ),
  zinb_factor_input
)

zinb_factor_out <- file.path(zinb_factor_fixture_dir, "outputs")
zinb_factor_run <- run_script(
  "zinb_stepwise_cv.R",
  args = c(
    sprintf("--data=%s", zinb_factor_input),
    sprintf("--output-dir=%s", zinb_factor_out),
    "--target=target",
    "--features=age_band_num,exposure",
    "--folds=2",
    "--metric=rmse",
    "--max-vars=1",
    "--workers=1",
    "--numeric-as-factor-max-levels=5",
    "--zero-formula=1"
  )
)
zinb_candidates <- read_csv_if_exists(file.path(zinb_factor_out, "zinb_all_candidates_by_step.csv"))
zinb_factor_ok <- zinb_factor_run$status == 0 &&
  !is.null(zinb_candidates) &&
  nrow(zinb_candidates[variable == "age_band_num" & transformation == "factor"]) >= 1

tests[[length(tests) + 1L]] <- record_test(
  "zinb_considers_factor_candidates_for_numeric_features",
  zinb_factor_ok,
  if (zinb_factor_ok) {
    "zinb_stepwise_cv.R considers factor() candidates for low-cardinality numeric features"
  } else {
    paste("zinb_stepwise_cv.R did not expose the expected factor() candidate:", zinb_factor_run$output)
  }
)

# zinb_stepwise_cv.R should allow explicit numeric-as-factor variable overrides
zinb_factor_override_fixture_dir <- file.path(TEST_OUTPUT_DIR, "zinb_factor_override_fixture")
unlink(zinb_factor_override_fixture_dir, recursive = TRUE, force = TRUE)
dir.create(zinb_factor_override_fixture_dir, recursive = TRUE, showWarnings = FALSE)

zinb_factor_override_input <- file.path(zinb_factor_override_fixture_dir, "input.csv")
safe_write_csv(
  data.table(
    target = c(0, 1, 2, 0, 1, 3, 0, 2, 1, 4, 0, 3, 1, 2, 3, 0, 1, 2, 1, 3, 0, 2, 1, 4),
    age_band_num = rep(c(1, 2, 3, 4, 5, 6), length.out = 24),
    exposure = c(10, 12, 14, 11, 13, 15, 10, 12, 14, 11, 13, 15, 16, 18, 20, 17, 19, 21, 16, 18, 20, 17, 19, 21)
  ),
  zinb_factor_override_input
)

zinb_factor_override_out <- file.path(zinb_factor_override_fixture_dir, "outputs")
zinb_factor_override_run <- run_script(
  "zinb_stepwise_cv.R",
  args = c(
    sprintf("--data=%s", zinb_factor_override_input),
    sprintf("--output-dir=%s", zinb_factor_override_out),
    "--target=target",
    "--features=age_band_num,exposure",
    "--folds=2",
    "--metric=rmse",
    "--max-vars=1",
    "--workers=1",
    "--numeric-as-factor-max-levels=3",
    "--numeric-as-factor-vars=age_band_num",
    "--zero-formula=1"
  )
)
zinb_override_candidates <- read_csv_if_exists(file.path(zinb_factor_override_out, "zinb_all_candidates_by_step.csv"))
zinb_factor_override_ok <- zinb_factor_override_run$status == 0 &&
  !is.null(zinb_override_candidates) &&
  nrow(zinb_override_candidates[variable == "age_band_num" & transformation == "factor"]) >= 1

tests[[length(tests) + 1L]] <- record_test(
  "zinb_allows_explicit_numeric_factor_overrides",
  zinb_factor_override_ok,
  if (zinb_factor_override_ok) {
    "zinb_stepwise_cv.R allows explicit numeric-as-factor variable overrides"
  } else {
    paste("zinb_stepwise_cv.R did not honor the numeric-as-factor variable override:", zinb_factor_override_run$output)
  }
)

results <- rbindlist(tests, fill = TRUE)
safe_write_csv(results, file.path(TEST_OUTPUT_DIR, "regression_test_results.csv"))

failed <- results[ok == FALSE]
if (nrow(failed) > 0) {
  print(results)
  stop("Regression tests failed. See regression_test_results.csv for details.", call. = FALSE)
}

cat("Regression tests passed.\n")
print(results)
