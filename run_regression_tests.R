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

record_test <- function(name, ok, details) {
  data.table(test = name, ok = isTRUE(ok), details = as.character(details))
}

run_script <- function(script_name, args = character(0), env = character(0), workdir = REPO_DIR) {
  script_path <- file.path(REPO_DIR, script_name)
  output <- tempfile(pattern = "regression_test_", tmpdir = TEST_OUTPUT_DIR, fileext = ".txt")
  status <- system2(
    R_SCRIPT,
    c(script_path, args),
    stdout = output,
    stderr = output,
    env = env,
    wait = TRUE
  )
  list(
    status = status,
    output = if (file.exists(output)) paste(readLines(output, warn = FALSE), collapse = "\n") else ""
  )
}

tests <- list()

# 1. validate_repo.R should succeed and write expected artifacts
validate_out <- file.path(TEST_OUTPUT_DIR, "validate_repo")
unlink(validate_out, recursive = TRUE, force = TRUE)
validate_run <- run_script("validate_repo.R", args = c(sprintf("--output-dir=%s", validate_out)))
validate_checks_path <- file.path(validate_out, "validation_checks.csv")
validate_log_path <- file.path(validate_out, "validate_repo.log")
validate_manifest_path <- file.path(validate_out, "run_manifest.csv")

validate_ok <- validate_run$status == 0 &&
  file.exists(validate_checks_path) &&
  file.exists(validate_log_path) &&
  file.exists(validate_manifest_path)

tests[[length(tests) + 1L]] <- record_test(
  "validate_repo_success",
  validate_ok,
  if (validate_ok) {
    "validate_repo.R completed and wrote checks, log, and manifest"
  } else {
    paste("validate_repo.R failed:", validate_run$output)
  }
)

# 2. Failed ranger run should still write log and manifest
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

# 3. compare_best_models.R should report availability for missing outputs
compare_fixture_root <- file.path(TEST_OUTPUT_DIR, "compare_fixture")
unlink(compare_fixture_root, recursive = TRUE, force = TRUE)
dir.create(file.path(compare_fixture_root, "outputs_ranger"), recursive = TRUE, showWarnings = FALSE)
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
  data.table(num.trees = 500L, regr.rmse = 1.2),
  file.path(compare_fixture_root, "outputs_ranger", "ranger_best_params.csv")
)

compare_run <- run_script(
  "compare_best_models.R",
  args = c(
    sprintf("--ranger-dir=%s", file.path(compare_fixture_root, "outputs_ranger")),
    sprintf("--xgb-dir=%s", file.path(compare_fixture_root, "outputs_xgb_missing")),
    sprintf("--zinb-dir=%s", file.path(compare_fixture_root, "outputs_zinb_missing")),
    sprintf("--output-dir=%s", file.path(compare_fixture_root, "outputs_model_comparison"))
  )
)
comparison_dt <- read_csv_if_exists(file.path(compare_fixture_root, "outputs_model_comparison", "best_models_comparison.csv"))
compare_ok <- compare_run$status == 0 &&
  !is.null(comparison_dt) &&
  all(c("availability_status", "availability_reason", "manifest_status") %in% names(comparison_dt)) &&
  "missing_directory" %in% comparison_dt$availability_status &&
  "ranger" %in% comparison_dt$model

tests[[length(tests) + 1L]] <- record_test(
  "compare_best_models_reports_missing_inputs",
  compare_ok,
  if (compare_ok) {
    "comparison output reports missing model directories explicitly"
  } else {
    paste("compare_best_models.R did not produce expected availability diagnostics:", compare_run$output)
  }
)

# 4. write_run_summary.R should create summary files from run manifests
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

# 5. preprocess_data.R should report row removals separately for filter and na.omit()
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

results <- rbindlist(tests, fill = TRUE)
safe_write_csv(results, file.path(TEST_OUTPUT_DIR, "regression_test_results.csv"))

failed <- results[ok == FALSE]
if (nrow(failed) > 0) {
  print(results)
  stop("Regression tests failed. See regression_test_results.csv for details.", call. = FALSE)
}

cat("Regression tests passed.\n")
print(results)
