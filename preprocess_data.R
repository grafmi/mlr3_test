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

require_packages(c("data.table"))

suppressPackageStartupMessages({
  library(data.table)
})

# =========================
# User settings
# =========================
INPUT_PATH <- get_path_setting(
  "input", "INPUT_DATA_PATH",
  "testfile_zinb_nonlinear_eintritte.csv",
  base_dir = REPO_DIR
)
OUTPUT_DIR <- get_path_setting(
  "output-dir", "PREPROCESS_OUTPUT_DIR",
  "outputs_preprocessed",
  base_dir = REPO_DIR
)
OUTPUT_BASENAME <- get_setting("output-name", "PREPROCESS_OUTPUT_NAME", "preprocessed_dataset")
OUTPUT_FORMATS <- parse_csv_setting(get_setting("formats", "PREPROCESS_OUTPUT_FORMATS", "csv,rds"))
ROW_FILTER <- get_setting("filter", "PREPROCESS_FILTER", "")
KEEP_COLS <- parse_csv_setting(get_setting("keep-cols", "PREPROCESS_KEEP_COLS", ""))
DROP_COLS <- parse_csv_setting(get_setting("drop-cols", "PREPROCESS_DROP_COLS", ""))
DROP_MISSING_ROWS <- get_bool_setting("drop-missing-rows", "PREPROCESS_DROP_MISSING_ROWS", FALSE)
CHARS_TO_FACTORS <- get_bool_setting("chars-to-factors", "PREPROCESS_CHARS_TO_FACTORS", TRUE)
FACTOR_MIN_COUNT <- get_int_setting("factor-min-count", "PREPROCESS_FACTOR_MIN_COUNT", 5, min_value = 1)

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
  if (!nzchar(trimws(filter_expression))) return(data.table::copy(dt))

  filter_call <- tryCatch(
    parse(text = filter_expression)[[1]],
    error = function(e) {
      stop("Could not parse filter expression: ", conditionMessage(e), call. = FALSE)
    }
  )

  keep_rows <- tryCatch(
    dt[, eval(filter_call)],
    error = function(e) {
      stop("Could not evaluate filter expression: ", conditionMessage(e), call. = FALSE)
    }
  )

  if (!is.logical(keep_rows)) {
    stop("Filter expression must return a logical vector.", call. = FALSE)
  }

  if (length(keep_rows) == 1) {
    keep_rows <- rep(keep_rows, nrow(dt))
  }

  if (length(keep_rows) != nrow(dt)) {
    stop(
      "Filter expression must return length 1 or one logical value per row (",
      nrow(dt),
      ").",
      call. = FALSE
    )
  }

  if (anyNA(keep_rows)) {
    stop("Filter expression returned NA values. Please make the condition explicit.", call. = FALSE)
  }

  dt[keep_rows]
}

drop_missing_rows_if_requested <- function(dt, drop_missing_rows) {
  if (!isTRUE(drop_missing_rows)) {
    return(list(data = data.table::copy(dt), rows_dropped = 0L))
  }

  rows_before <- nrow(dt)
  cleaned <- stats::na.omit(dt)
  list(data = cleaned, rows_dropped = rows_before - nrow(cleaned))
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
LOG_STATE <- start_logging(OUTPUT_DIR, "preprocess_data")

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

log_info("Using input file: ", normalizePath(INPUT_PATH, mustWork = FALSE))
log_info("Using output directory: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
log_info("Using output basename: ", OUTPUT_BASENAME)
log_info("Using output formats: ", paste(OUTPUT_FORMATS, collapse = ", "))
if (nzchar(trimws(ROW_FILTER))) log_info("Using row filter: ", ROW_FILTER)
if (length(KEEP_COLS) > 0) log_info("Keeping columns: ", paste(KEEP_COLS, collapse = ", "))
if (length(DROP_COLS) > 0) log_info("Dropping columns: ", paste(DROP_COLS, collapse = ", "))
log_info("Drop rows with missing values: ", DROP_MISSING_ROWS)
log_info("Convert character columns to factors: ", CHARS_TO_FACTORS)
log_info("Minimum count for rare factor level warning: ", FACTOR_MIN_COUNT)

original_dt <- load_tabular_data_checked(INPUT_PATH)
validate_preprocess_settings(original_dt, KEEP_COLS, DROP_COLS, OUTPUT_FORMATS)

processed_dt <- apply_keep_drop_columns(original_dt, KEEP_COLS, DROP_COLS)
processed_dt <- apply_row_filter(processed_dt, ROW_FILTER)
if (CHARS_TO_FACTORS) {
  processed_dt <- coerce_character_columns_to_factor(processed_dt)
}
drop_result <- drop_missing_rows_if_requested(processed_dt, DROP_MISSING_ROWS)
processed_dt <- drop_result$data
processed_dt <- drop_unused_factor_levels(processed_dt)

if (nrow(processed_dt) == 0) {
  stop("No rows remain after preprocessing.", call. = FALSE)
}

if (drop_result$rows_dropped > 0) {
  log_info("Dropped ", drop_result$rows_dropped, " row(s) with missing values during preprocessing.")
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
  keep_cols = KEEP_COLS,
  drop_cols = DROP_COLS,
  drop_missing_rows = DROP_MISSING_ROWS,
  output_files = output_files
)
write_metadata_bundle(metadata, OUTPUT_DIR, prefix = metadata_prefix)

log_info("Rows before / after: ", nrow(original_dt), " / ", nrow(processed_dt))
log_info("Columns before / after: ", ncol(original_dt), " / ", ncol(processed_dt))
log_info("Done. Files written to: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
print(metadata$summary)

.script_ok <- TRUE
stop_logging(LOG_STATE, if (.script_ok) "completed" else "failed")
