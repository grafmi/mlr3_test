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
SCRIPT_NAME <- "preprocess_data"
SCRIPT_PACKAGES <- c("data.table")

require_packages(c("data.table"))

suppressPackageStartupMessages({
  library(data.table)
})

# =========================
# User settings
# =========================
INPUT_PATH <- get_path_setting(
  "input", "INPUT_DATA_PATH",
  config_value(CONFIG, c("experiment", "data_path")),
  base_dir = REPO_DIR
)
OUTPUT_DIR <- get_path_setting(
  "output-dir", "PREPROCESS_OUTPUT_DIR",
  config_value(CONFIG, c("preprocess", "output_dir")),
  base_dir = REPO_DIR
)
OUTPUT_BASENAME <- get_setting("output-name", "PREPROCESS_OUTPUT_NAME", config_value(CONFIG, c("preprocess", "output_name")))
OUTPUT_FORMATS <- parse_csv_setting(get_setting(
  "formats", "PREPROCESS_OUTPUT_FORMATS",
  paste(config_value(CONFIG, c("preprocess", "output_formats")), collapse = ",")
))
ROW_FILTER <- get_setting("filter", "PREPROCESS_FILTER", config_value(CONFIG, c("preprocess", "filter")))
KEEP_COLS <- parse_csv_setting(get_setting(
  "keep-cols", "PREPROCESS_KEEP_COLS",
  paste(config_value(CONFIG, c("preprocess", "keep_cols")), collapse = ",")
))
DROP_COLS <- parse_csv_setting(get_setting(
  "drop-cols", "PREPROCESS_DROP_COLS",
  paste(config_value(CONFIG, c("preprocess", "drop_cols")), collapse = ",")
))
DROP_MISSING_ROWS <- get_bool_setting(
  "drop-missing-rows", "PREPROCESS_DROP_MISSING_ROWS",
  config_value(CONFIG, c("preprocess", "drop_missing_rows"))
)
SAMPLE_ROWS <- get_optional_int_setting(
  "sample-rows", "PREPROCESS_SAMPLE_ROWS",
  config_value(CONFIG, c("preprocess", "sample_rows")),
  min_value = 1
)
SAMPLE_SEED <- get_int_setting(
  "sample-seed", "PREPROCESS_SAMPLE_SEED",
  config_value(CONFIG, c("preprocess", "sample_seed"))
)
CHARS_TO_FACTORS <- get_bool_setting(
  "chars-to-factors", "PREPROCESS_CHARS_TO_FACTORS",
  config_value(CONFIG, c("preprocess", "chars_to_factors"))
)
FACTOR_MIN_COUNT <- get_int_setting(
  "factor-min-count", "PREPROCESS_FACTOR_MIN_COUNT",
  config_value(CONFIG, c("preprocess", "factor_min_count")),
  min_value = 1
)

# =========================
# Helpers
# =========================
validate_preprocess_settings <- function(dt, keep_cols, drop_cols, output_formats) {
  if (length(output_formats) == 0) {
    stop("At least one output format must be provided.", call. = FALSE)
  }

  overlap <- intersect(keep_cols, drop_cols)
  if (length(overlap) > 0) {
    stop(
      "Columns must not be listed in both keep-cols and drop-cols: ",
      paste(overlap, collapse = ", "),
      call. = FALSE
    )
  }

  missing_keep <- setdiff(keep_cols, names(dt))
  if (length(missing_keep) > 0) {
    stop("keep-cols not found in data: ", paste(missing_keep, collapse = ", "), call. = FALSE)
  }

  missing_drop <- setdiff(drop_cols, names(dt))
  if (length(missing_drop) > 0) {
    stop("drop-cols not found in data: ", paste(missing_drop, collapse = ", "), call. = FALSE)
  }
}

apply_keep_drop_columns <- function(dt, keep_cols, drop_cols) {
  out <- data.table::copy(dt)

  if (length(keep_cols) > 0) {
    out <- out[, ..keep_cols]
  }

  if (length(drop_cols) > 0) {
    out[, (drop_cols) := NULL]
  }

  if (ncol(out) == 0) {
    stop("No columns remain after applying keep-cols and drop-cols.", call. = FALSE)
  }

  out
}

apply_row_filter <- function(dt, filter_expression) {
  apply_row_filter_checked(dt, filter_expression, label = "Filter expression")
}

drop_missing_rows_if_requested <- function(dt, drop_missing_rows) {
  if (!isTRUE(drop_missing_rows)) {
    return(list(data = data.table::copy(dt), rows_dropped = 0L))
  }

  rows_before <- nrow(dt)
  cleaned <- stats::na.omit(dt)
  list(data = cleaned, rows_dropped = rows_before - nrow(cleaned))
}

apply_random_subset_if_requested <- function(dt, sample_rows, sample_seed) {
  if (is.na(sample_rows)) {
    return(list(data = data.table::copy(dt), rows_removed = 0L))
  }

  if (sample_rows > nrow(dt)) {
    stop(
      "sample-rows (", sample_rows, ") exceeds the number of rows remaining after preprocessing steps (",
      nrow(dt), ").",
      call. = FALSE
    )
  }

  if (sample_rows == nrow(dt)) {
    return(list(data = data.table::copy(dt), rows_removed = 0L))
  }

  set.seed(sample_seed)
  selected_rows <- sort(sample.int(nrow(dt), size = sample_rows, replace = FALSE))
  subset_dt <- dt[selected_rows]
  list(data = subset_dt, rows_removed = nrow(dt) - nrow(subset_dt))
}

metadata_output_table <- function(dataset_files, metadata_prefix) {
  rows <- rbindlist(list(
    data.table(file_role = "dataset", dataset_files),
    data.table(
      file_role = "metadata",
      format = c("csv", "rds", "csv", "rds", "csv", "rds", "rds"),
      path = c(
        file.path(OUTPUT_DIR, paste0(metadata_prefix, "_summary.csv")),
        file.path(OUTPUT_DIR, paste0(metadata_prefix, "_summary.rds")),
        file.path(OUTPUT_DIR, paste0(metadata_prefix, "_columns.csv")),
        file.path(OUTPUT_DIR, paste0(metadata_prefix, "_columns.rds")),
        file.path(OUTPUT_DIR, paste0(metadata_prefix, "_files.csv")),
        file.path(OUTPUT_DIR, paste0(metadata_prefix, "_files.rds")),
        file.path(OUTPUT_DIR, paste0(metadata_prefix, ".rds"))
      )
    )
  ), fill = TRUE)

  rows[, path := normalizePath(path, mustWork = FALSE)]
  rows
}

# =========================
# Main
# =========================
.script_ok <- FALSE
LOG_STATE <- start_logging(OUTPUT_DIR, SCRIPT_NAME)
with_run_finalizer({
  dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

  log_info("Using input file: ", normalizePath(INPUT_PATH, mustWork = FALSE))
  log_info("Using output directory: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
  log_info("Using output basename: ", OUTPUT_BASENAME)
  log_info("Using output formats: ", paste(OUTPUT_FORMATS, collapse = ", "))
  if (nzchar(trimws(ROW_FILTER))) log_info("Using row filter: ", ROW_FILTER)
  if (length(KEEP_COLS) > 0) log_info("Keeping columns: ", paste(KEEP_COLS, collapse = ", "))
  if (length(DROP_COLS) > 0) log_info("Dropping columns: ", paste(DROP_COLS, collapse = ", "))
  log_info("Drop rows with missing values: ", DROP_MISSING_ROWS)
  if (!is.na(SAMPLE_ROWS)) {
    log_info("Using random subset rows: ", SAMPLE_ROWS)
    log_info("Using random subset seed: ", SAMPLE_SEED)
  }
  log_info("Convert character columns to factors: ", CHARS_TO_FACTORS)
  log_info("Minimum count for rare factor level warning: ", FACTOR_MIN_COUNT)

  original_dt <- load_tabular_data_checked(INPUT_PATH)
  validate_preprocess_settings(original_dt, KEEP_COLS, DROP_COLS, OUTPUT_FORMATS)

  processed_dt <- apply_keep_drop_columns(original_dt, KEEP_COLS, DROP_COLS)
  filter_result <- apply_row_filter(processed_dt, ROW_FILTER)
  processed_dt <- filter_result$data
  if (CHARS_TO_FACTORS) {
    processed_dt <- coerce_character_columns_to_factor(processed_dt)
  }
  drop_result <- drop_missing_rows_if_requested(processed_dt, DROP_MISSING_ROWS)
  processed_dt <- drop_result$data
  subset_result <- apply_random_subset_if_requested(processed_dt, SAMPLE_ROWS, SAMPLE_SEED)
  processed_dt <- subset_result$data
  processed_dt <- drop_unused_factor_levels(processed_dt)

  if (nrow(processed_dt) == 0) {
    stop("No rows remain after preprocessing.", call. = FALSE)
  }

  if (filter_result$rows_removed > 0) {
    log_info("Dropped ", filter_result$rows_removed, " row(s) via row filter during preprocessing.")
  }
  if (drop_result$rows_dropped > 0) {
    log_info("Dropped ", drop_result$rows_dropped, " row(s) with missing values during preprocessing.")
  }
  if (subset_result$rows_removed > 0) {
    log_info("Dropped ", subset_result$rows_removed, " row(s) via random subsampling during preprocessing.")
  }

  validate_factor_columns(processed_dt, min_level_count = FACTOR_MIN_COUNT, context = "preprocessed data")

  dataset_files <- write_dataset_formats(processed_dt, OUTPUT_DIR, OUTPUT_BASENAME, OUTPUT_FORMATS)
  metadata_prefix <- paste0(OUTPUT_BASENAME, "_metadata")
  output_files <- metadata_output_table(dataset_files, metadata_prefix)

  metadata <- build_dataset_metadata(
    original_dt = original_dt,
    processed_dt = processed_dt,
    source_path = INPUT_PATH,
    filter_expression = ROW_FILTER,
    rows_removed_by_filter = filter_result$rows_removed,
    keep_cols = KEEP_COLS,
    drop_cols = DROP_COLS,
    drop_missing_rows = DROP_MISSING_ROWS,
    rows_removed_by_na_omit = drop_result$rows_dropped,
    sample_rows = SAMPLE_ROWS,
    sample_seed = SAMPLE_SEED,
    rows_removed_by_subsampling = subset_result$rows_removed,
    output_files = output_files
  )
  write_metadata_bundle(metadata, OUTPUT_DIR, prefix = metadata_prefix)

  log_info("Rows before / after: ", nrow(original_dt), " / ", nrow(processed_dt))
  log_info("Columns before / after: ", ncol(original_dt), " / ", ncol(processed_dt))
  log_info("Done. Files written to: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
  print(metadata$summary)

  .script_ok <- TRUE
}, function() finalize_run(
  log_state = LOG_STATE,
  output_dir = OUTPUT_DIR,
  script_name = SCRIPT_NAME,
  repo_dir = REPO_DIR,
  packages = SCRIPT_PACKAGES,
  status = if (.script_ok) "completed" else "failed",
  data_path = INPUT_PATH
))
