#!/usr/bin/env Rscript

require_packages <- function(packages) {
  missing <- packages[!vapply(packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0) {
    stop(
      "Required R package(s) are not installed: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }
}

script_cli_args <- function() {
  commandArgs(trailingOnly = TRUE)
}

get_arg_value <- function(name, default = NULL) {
  args <- script_cli_args()
  patterns <- c(sprintf("^--%s=", name), sprintf("^%s=", name))
  for (pattern in patterns) {
    hit <- grep(pattern, args, value = TRUE)
    if (length(hit) > 0) {
      return(sub(pattern, "", hit[1]))
    }
  }
  default
}

get_setting <- function(arg_name, env_name, default) {
  arg_value <- get_arg_value(arg_name, default = NULL)
  # An explicit CLI argument should override config defaults even when it is
  # intentionally set to the empty string (for example `--row-filter=`).
  if (!is.null(arg_value)) return(arg_value)

  env_value <- Sys.getenv(env_name, unset = NA_character_)
  if (!is.na(env_value) && nzchar(env_value)) return(env_value)

  default
}

get_int_setting <- function(arg_name, env_name, default, min_value = NULL) {
  value <- suppressWarnings(as.integer(get_setting(arg_name, env_name, default)))
  if (is.na(value)) {
    stop(sprintf("Setting '%s' must be an integer.", arg_name), call. = FALSE)
  }
  if (!is.null(min_value) && value < min_value) {
    stop(sprintf("Setting '%s' must be at least %s.", arg_name, min_value), call. = FALSE)
  }
  value
}

get_optional_int_setting <- function(arg_name, env_name, default = NA_integer_, min_value = NULL) {
  raw_value <- get_setting(arg_name, env_name, default)
  raw_value_chr <- trimws(as.character(raw_value))
  if (is.na(raw_value_chr) || !nzchar(raw_value_chr) || tolower(raw_value_chr) %in% c("na", "null", "none")) {
    return(NA_integer_)
  }

  value <- suppressWarnings(as.integer(raw_value_chr))
  if (is.na(value)) {
    stop(sprintf("Setting '%s' must be an integer or NA.", arg_name), call. = FALSE)
  }
  if (!is.null(min_value) && value < min_value) {
    stop(sprintf("Setting '%s' must be at least %s when provided.", arg_name, min_value), call. = FALSE)
  }
  value
}

get_numeric_setting <- function(arg_name, env_name, default, min_value = NULL) {
  value <- suppressWarnings(as.numeric(get_setting(arg_name, env_name, default)))
  if (is.na(value)) {
    stop(sprintf("Setting '%s' must be numeric.", arg_name), call. = FALSE)
  }
  if (!is.null(min_value) && value < min_value) {
    stop(sprintf("Setting '%s' must be at least %s.", arg_name, min_value), call. = FALSE)
  }
  value
}

get_optional_numeric_setting <- function(arg_name, env_name, default = NA_real_, min_value = NULL) {
  raw_value <- get_setting(arg_name, env_name, default)
  raw_value_chr <- trimws(as.character(raw_value))
  if (is.na(raw_value_chr) || !nzchar(raw_value_chr) || tolower(raw_value_chr) %in% c("na", "null", "none")) {
    return(NA_real_)
  }

  value <- suppressWarnings(as.numeric(raw_value_chr))
  if (is.na(value)) {
    stop(sprintf("Setting '%s' must be numeric or NA.", arg_name), call. = FALSE)
  }
  if (!is.null(min_value) && value < min_value) {
    stop(sprintf("Setting '%s' must be at least %s when provided.", arg_name, min_value), call. = FALSE)
  }
  value
}

is_absolute_path <- function(path) {
  grepl("^(/|[A-Za-z]:[/\\\\]|~)", path)
}

resolve_path <- function(path, base_dir) {
  path <- path.expand(path)
  if (is_absolute_path(path)) return(path)
  file.path(base_dir, path)
}

get_path_setting <- function(arg_name, env_name, default, base_dir) {
  resolve_path(get_setting(arg_name, env_name, default), base_dir)
}

get_project_config_path <- function(repo_dir) {
  get_path_setting("config", "CONFIG_PATH", file.path("configs", "base_config.R"), base_dir = repo_dir)
}

load_project_config <- function(repo_dir) {
  config_path <- get_project_config_path(repo_dir)
  if (!file.exists(config_path)) {
    stop("Could not find config file: ", normalizePath(config_path, mustWork = FALSE), call. = FALSE)
  }

  config_env <- new.env(parent = baseenv())
  sys.source(config_path, envir = config_env)
  if (!exists("CONFIG", envir = config_env, inherits = FALSE)) {
    stop("Config file must define an object named CONFIG.", call. = FALSE)
  }

  get("CONFIG", envir = config_env, inherits = FALSE)
}

config_value <- function(config, path) {
  value <- config
  for (name in path) {
    if (!is.list(value) || is.null(value[[name]])) {
      stop("Missing config value: ", paste(path, collapse = "."), call. = FALSE)
    }
    value <- value[[name]]
  }
  value
}

has_config_value <- function(config, path) {
  value <- config
  for (name in path) {
    if (!is.list(value) || is.null(value[[name]])) {
      return(FALSE)
    }
    value <- value[[name]]
  }
  TRUE
}

config_value_or <- function(config, path, default = NULL) {
  if (!has_config_value(config, path)) return(default)
  config_value(config, path)
}

parse_csv_setting <- function(value) {
  if (is.null(value) || !nzchar(trimws(value))) return(character(0))
  out <- trimws(unlist(strsplit(value, ",", fixed = TRUE), use.names = FALSE))
  unique(out[nzchar(out)])
}

normalize_optional_string <- function(value) {
  value <- trimws(as.character(value)[1])
  if (is.na(value) || !nzchar(value)) return(NA_character_)
  value
}

get_run_name_setting <- function(config) {
  normalize_optional_string(get_setting("run-name", "RUN_NAME", config_value_or(config, c("results", "run_name"), "")))
}

get_bool_setting <- function(arg_name, env_name, default = FALSE) {
  value <- tolower(trimws(as.character(get_setting(arg_name, env_name, default))))
  if (value %in% c("1", "true", "yes", "y")) return(TRUE)
  if (value %in% c("0", "false", "no", "n")) return(FALSE)
  stop(sprintf("Setting '%s' must be TRUE/FALSE.", arg_name), call. = FALSE)
}

normalize_outer_resampling <- function(value) {
  value <- tolower(trimws(as.character(value)[1]))
  value <- gsub("-", "_", value, fixed = TRUE)
  if (value %in% c("stratified", "stratified_cv", "default")) return("stratified")
  if (value %in% c("year", "year_blocked", "blocked_year", "by_year", "leave_one_year_out")) return("year_blocked")
  stop("outer_resampling must be 'stratified' or 'year_blocked'.", call. = FALSE)
}

normalize_target_mode <- function(value) {
  value <- tolower(trimws(as.character(value)[1]))
  value <- gsub("-", "_", value, fixed = TRUE)
  if (value %in% c("count", "raw", "default")) return("count")
  if (value %in% c("rate", "exposure_rate")) return("rate")
  stop("target_mode must be 'count' or 'rate'.", call. = FALSE)
}

normalize_target_transform <- function(value) {
  value <- tolower(trimws(as.character(value)[1]))
  value <- gsub("-", "_", value, fixed = TRUE)
  if (is.na(value) || !nzchar(value) || value %in% c("none", "identity", "raw", "default")) return("none")
  if (value %in% c("log1p", "log_plus_one", "log")) return("log1p")
  stop("target_transform must be 'none' or 'log1p'.", call. = FALSE)
}

apply_target_transform <- function(values, transform, context = "target") {
  transform <- normalize_target_transform(transform)
  if (identical(transform, "none")) return(values)
  if (identical(transform, "log1p")) {
    if (anyNA(values) || any(!is.finite(values)) || any(values < 0)) {
      stop(context, " must contain only finite values >= 0 when target_transform='log1p'.", call. = FALSE)
    }
    return(log1p(values))
  }
  stop("Unsupported target_transform: ", transform, call. = FALSE)
}

inverse_target_transform <- function(values, transform) {
  transform <- normalize_target_transform(transform)
  if (identical(transform, "none")) return(values)
  if (identical(transform, "log1p")) return(pmax(expm1(values), 0))
  stop("Unsupported target_transform: ", transform, call. = FALSE)
}

coerce_character_columns_to_factor <- function(dt, exclude_cols = character(0)) {
  out <- data.table::copy(dt)
  candidate_cols <- setdiff(names(out), exclude_cols)
  char_cols <- names(which(vapply(out[, ..candidate_cols], is.character, logical(1))))
  if (length(char_cols) > 0) {
    out[, (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
  }
  out
}

drop_unused_factor_levels <- function(dt) {
  out <- data.table::copy(dt)
  factor_cols <- names(which(vapply(out, is.factor, logical(1))))
  if (length(factor_cols) > 0) {
    out[, (factor_cols) := lapply(.SD, droplevels), .SDcols = factor_cols]
  }
  out
}

validate_factor_columns <- function(dt, min_level_count = 5L, context = "dataset") {
  factor_cols <- names(which(vapply(dt, is.factor, logical(1))))
  if (length(factor_cols) == 0) {
    return(invisible(list(single_level = character(0), rare_levels = data.table::data.table())))
  }

  single_level <- character(0)
  rare_levels <- vector("list", length = 0L)

  for (col in factor_cols) {
    values <- droplevels(dt[[col]])
    level_counts <- table(values, useNA = "no")

    if (length(level_counts) <= 1L) {
      single_level <- c(single_level, col)
      next
    }

    rare <- level_counts[level_counts < min_level_count]
    if (length(rare) > 0) {
      rare_levels[[length(rare_levels) + 1L]] <- data.table::data.table(
        column = col,
        level = names(rare),
        count = as.integer(rare)
      )
    }
  }

  if (length(single_level) > 0) {
    stop(
      "Factor column(s) with fewer than 2 observed levels found in ",
      context,
      ": ",
      paste(single_level, collapse = ", "),
      ". Remove them, filter differently, or convert them before modeling.",
      call. = FALSE
    )
  }

  rare_levels_dt <- data.table::rbindlist(rare_levels, fill = TRUE)
  if (nrow(rare_levels_dt) > 0) {
    warning(
      "Rare factor levels found in ",
      context,
      " (count < ",
      min_level_count,
      "): ",
      paste(sprintf("%s=%s (%s)", rare_levels_dt$column, rare_levels_dt$level, rare_levels_dt$count), collapse = ", "),
      call. = FALSE
    )
  }

  invisible(list(single_level = single_level, rare_levels = rare_levels_dt))
}

file_extension <- function(path) {
  tolower(tools::file_ext(path))
}

guess_delimiter <- function(path) {
  ext <- file_extension(path)
  if (ext %in% c("tsv", "tab", "txt")) return("\t")
  ","
}

extract_tabular_object <- function(object, source_path) {
  if (is.data.frame(object) || data.table::is.data.table(object)) {
    return(data.table::as.data.table(data.table::copy(object)))
  }

  if (is.matrix(object)) {
    return(data.table::as.data.table(object))
  }

  if (is.list(object) && !is.data.frame(object)) {
    tabular <- Filter(function(x) is.data.frame(x) || data.table::is.data.table(x) || is.matrix(x), object)
    if (length(tabular) == 1) {
      return(extract_tabular_object(tabular[[1]], source_path))
    }
  }

  stop(
    "Could not extract a tabular dataset from: ",
    normalizePath(source_path, mustWork = FALSE),
    call. = FALSE
  )
}

load_tabular_data_checked <- function(data_path) {
  if (!file.exists(data_path)) {
    stop("Data file not found: ", normalizePath(data_path, mustWork = FALSE), call. = FALSE)
  }

  ext <- file_extension(data_path)

  if (ext %in% c("csv", "tsv", "txt", "tab")) {
    return(data.table::fread(data_path, header = TRUE, sep = guess_delimiter(data_path)))
  }

  if (ext == "rds") {
    return(extract_tabular_object(readRDS(data_path), data_path))
  }

  if (ext %in% c("rda", "rdata")) {
    env <- new.env(parent = emptyenv())
    loaded_names <- load(data_path, envir = env)
    if (length(loaded_names) == 0) {
      stop("No objects found in: ", normalizePath(data_path, mustWork = FALSE), call. = FALSE)
    }

    objects <- mget(loaded_names, envir = env, inherits = FALSE)
    tabular_names <- names(Filter(function(x) {
      is.data.frame(x) || data.table::is.data.table(x) || is.matrix(x)
    }, objects))

    if (length(tabular_names) == 0) {
      stop(
        "No data.frame-like object found in: ",
        normalizePath(data_path, mustWork = FALSE),
        call. = FALSE
      )
    }
    if (length(tabular_names) > 1) {
      stop(
        "Found multiple data.frame-like objects in ",
        normalizePath(data_path, mustWork = FALSE),
        ": ",
        paste(tabular_names, collapse = ", "),
        ". Keep only one tabular object in the file.",
        call. = FALSE
      )
    }

    return(extract_tabular_object(objects[[tabular_names]], data_path))
  }

  stop(
    "Unsupported data file extension '.", ext, "' for ",
    normalizePath(data_path, mustWork = FALSE),
    ". Supported extensions: csv, tsv, txt, tab, rds, rda, RData.",
    call. = FALSE
  )
}

load_csv_checked <- function(data_path) {
  load_tabular_data_checked(data_path)
}

validate_columns <- function(df, target, feature_cols, id_cols = character(0)) {
  if (!is.data.frame(df)) stop("Input data must be a data.frame or data.table.", call. = FALSE)
  if (!target %in% names(df)) stop(sprintf("Target '%s' was not found in the data.", target), call. = FALSE)
  if (length(feature_cols) == 0) stop("FEATURE_COLS must contain at least one predictor.", call. = FALSE)

  missing_features <- setdiff(feature_cols, names(df))
  if (length(missing_features) > 0) {
    stop("FEATURE_COLS not found in data: ", paste(missing_features, collapse = ", "), call. = FALSE)
  }
  if (target %in% feature_cols) stop("TARGET must not be included in FEATURE_COLS.", call. = FALSE)

  overlap <- intersect(feature_cols, id_cols)
  if (length(overlap) > 0) {
    stop("FEATURE_COLS must not overlap with ID_COLS: ", paste(overlap, collapse = ", "), call. = FALSE)
  }
}

warn_if_duplicate_id_rows <- function(dt, id_cols = character(0), context = "modeling data") {
  id_cols <- unique(id_cols[nzchar(id_cols)])
  if (length(id_cols) == 0) return(invisible(NULL))

  missing_id_cols <- setdiff(id_cols, names(dt))
  if (length(missing_id_cols) > 0) {
    log_info(
      "Warning: ID_COLS not found in ",
      context,
      " and could not be checked for grouped-CV leakage: ",
      paste(missing_id_cols, collapse = ", ")
    )
  }

  available_id_cols <- intersect(id_cols, names(dt))
  if (length(available_id_cols) == 0 || nrow(dt) == 0) return(invisible(NULL))

  id_dt <- dt[, ..available_id_cols]
  n_unique_ids <- data.table::uniqueN(id_dt, na.rm = FALSE)
  n_duplicate_rows <- nrow(dt) - n_unique_ids
  if (n_duplicate_rows > 0) {
    log_info(
      "Warning: ID column(s) have repeated value combinations in ",
      context,
      " (",
      n_duplicate_rows,
      " row(s) beyond the first occurrence across ",
      paste(available_id_cols, collapse = ", "),
      "). Standard row-wise CV may leak grouped entities; consider grouped CV or aggregation for real data."
    )
  }

  invisible(NULL)
}

warn_if_missing_drop_high <- function(rows_before, rows_dropped, warn_fraction = NA_real_,
                                      context = "modeling data") {
  if (is.null(warn_fraction) || is.na(warn_fraction) || !is.finite(warn_fraction)) {
    return(invisible(NULL))
  }
  if (rows_before <= 0 || rows_dropped <= 0) return(invisible(NULL))

  drop_fraction <- rows_dropped / rows_before
  if (drop_fraction > warn_fraction) {
    log_info(
      "Warning: Dropped ",
      rows_dropped,
      " of ",
      rows_before,
      " row(s) with missing values in ",
      context,
      " (",
      sprintf("%.2f%%", 100 * drop_fraction),
      "), above configured warning threshold ",
      sprintf("%.2f%%", 100 * warn_fraction),
      ". Check whether missingness changes the modeled population."
    )
  }

  invisible(NULL)
}

warn_if_small_cv_folds <- function(n_rows, n_folds, min_rows_per_fold = 10L,
                                   context = "modeling data") {
  if (is.na(n_rows) || is.na(n_folds) || n_folds < 2L) return(invisible(NULL))
  fold_size_floor <- floor(n_rows / n_folds)
  fold_size_ceiling <- ceiling(n_rows / n_folds)
  if (fold_size_floor < min_rows_per_fold) {
    log_info(
      "Warning: Small validation folds for ",
      context,
      " (",
      n_rows,
      " row(s), ",
      n_folds,
      " fold(s), approximately ",
      fold_size_floor,
      "-",
      fold_size_ceiling,
      " row(s) per fold). CV metrics may be unstable after filtering."
    )
  }

  invisible(NULL)
}

warn_if_low_information_features <- function(dt, feature_cols, near_constant_unique_max = 2L,
                                             near_constant_dominance = 0.95,
                                             context = "modeling data") {
  feature_cols <- intersect(feature_cols, names(dt))
  if (length(feature_cols) == 0 || nrow(dt) == 0) return(invisible(NULL))

  constant_features <- character(0)
  near_constant_features <- character(0)

  for (col in feature_cols) {
    values <- dt[[col]]
    non_missing <- values[!is.na(values)]
    n_unique <- data.table::uniqueN(non_missing)
    if (n_unique <= 1L) {
      constant_features <- c(constant_features, col)
      next
    }

    if (is.numeric(values) && n_unique <= near_constant_unique_max && length(non_missing) > 0) {
      counts <- table(non_missing, useNA = "no")
      dominance <- max(counts) / length(non_missing)
      if (dominance >= near_constant_dominance) {
        near_constant_features <- c(
          near_constant_features,
          sprintf("%s (%s unique, dominant value %.1f%%)", col, n_unique, 100 * dominance)
        )
      }
    }
  }

  if (length(constant_features) > 0) {
    log_info(
      "Warning: Constant feature(s) found in ",
      context,
      ": ",
      paste(constant_features, collapse = ", "),
      ". They add no predictive information and may make model diagnostics harder to interpret."
    )
  }
  if (length(near_constant_features) > 0) {
    log_info(
      "Warning: Near-constant numeric feature(s) found in ",
      context,
      ": ",
      paste(near_constant_features, collapse = ", "),
      ". Check whether these predictors are useful after filtering."
    )
  }

  invisible(NULL)
}

formula_referenced_columns <- function(rhs, label = "Formula") {
  rhs <- trimws(rhs)
  if (!nzchar(rhs)) return(character(0))

  rhs_formula <- tryCatch(
    stats::as.formula(sprintf("~ %s", rhs)),
    error = function(e) {
      stop(label, " is not a valid formula right-hand side: ", conditionMessage(e), call. = FALSE)
    }
  )

  all.vars(rhs_formula)
}

apply_row_filter_checked <- function(dt, filter_expression, label = "Row filter") {
  if (!nzchar(trimws(filter_expression))) {
    return(list(data = data.table::copy(dt), rows_removed = 0L))
  }

  filter_call <- tryCatch(
    parse(text = filter_expression)[[1]],
    error = function(e) {
      stop(label, " could not be parsed: ", conditionMessage(e), call. = FALSE)
    }
  )

  keep_rows <- tryCatch(
    dt[, eval(filter_call)],
    error = function(e) {
      stop(label, " could not be evaluated: ", conditionMessage(e), call. = FALSE)
    }
  )

  if (!is.logical(keep_rows)) {
    stop(label, " must return a logical vector.", call. = FALSE)
  }

  if (length(keep_rows) == 1) {
    keep_rows <- rep(keep_rows, nrow(dt))
  }

  if (length(keep_rows) != nrow(dt)) {
    stop(
      label, " must return length 1 or one logical value per row (", nrow(dt), ").",
      call. = FALSE
    )
  }

  if (anyNA(keep_rows)) {
    stop(label, " returned NA values. Please make the condition explicit.", call. = FALSE)
  }

  filtered_dt <- dt[keep_rows]
  list(data = filtered_dt, rows_removed = nrow(dt) - nrow(filtered_dt))
}

prepare_modeling_data <- function(df, target, feature_cols, id_cols = character(0),
                                  require_count_target = FALSE, row_filter = "",
                                  extra_feature_cols = character(0),
                                  missing_drop_warn_fraction = NA_real_) {
  validate_columns(df, target, feature_cols, id_cols)
  extra_feature_cols <- unique(extra_feature_cols[nzchar(extra_feature_cols)])
  missing_extra <- setdiff(extra_feature_cols, names(df))
  if (length(missing_extra) > 0) {
    stop("Additional modeling columns not found in data: ", paste(missing_extra, collapse = ", "), call. = FALSE)
  }

  dt <- data.table::as.data.table(data.table::copy(df))
  filter_result <- apply_row_filter_checked(dt, row_filter, label = "Modeling row filter")
  dt <- filter_result$data
  if (filter_result$rows_removed > 0) {
    log_info("Dropped ", filter_result$rows_removed, " row(s) via modeling row filter.")
  }

  warn_if_duplicate_id_rows(dt, id_cols = id_cols, context = "modeling data after row filtering")

  keep_cols <- unique(c(setdiff(c(target, feature_cols), id_cols), extra_feature_cols))
  dt <- dt[, ..keep_cols]

  if (!is.numeric(dt[[target]])) {
    stop(sprintf("Target '%s' must be numeric.", target), call. = FALSE)
  }
  if (require_count_target) {
    if (any(dt[[target]] < 0, na.rm = TRUE)) {
      stop(sprintf("Target '%s' must be non-negative for count models.", target), call. = FALSE)
    }
    non_integer <- abs(dt[[target]] - round(dt[[target]])) > .Machine$double.eps^0.5
    if (any(non_integer, na.rm = TRUE)) {
      stop(sprintf("Target '%s' must contain integer-like counts for ZINB.", target), call. = FALSE)
    }
  }

  dt <- coerce_character_columns_to_factor(dt)

  rows_before <- nrow(dt)
  dt <- stats::na.omit(dt)
  rows_dropped <- rows_before - nrow(dt)
  if (rows_dropped > 0) {
    log_info("Dropped ", rows_dropped, " row(s) with missing values.")
    warn_if_missing_drop_high(
      rows_before = rows_before,
      rows_dropped = rows_dropped,
      warn_fraction = missing_drop_warn_fraction,
      context = "modeling data"
    )
  }
  if (nrow(dt) == 0) stop("No rows remain after removing missing values.", call. = FALSE)

  dt <- drop_unused_factor_levels(dt)
  validate_factor_columns(dt, min_level_count = 5L, context = "modeling data")

  dt
}

make_strata <- function(y, n_bins = 10) {
  if (length(y) == 0 || all(is.na(y))) return(factor(character(length(y))))
  if (length(unique(y[!is.na(y)])) < 2) return(factor(rep("all", length(y))))

  n_bins <- max(2L, min(as.integer(n_bins), length(y)))
  probs <- seq(0, 1, length.out = n_bins + 1)
  breaks <- unique(stats::quantile(y, probs = probs, na.rm = TRUE, type = 7))

  if (length(breaks) >= 3) {
    return(cut(y, breaks = breaks, include.lowest = TRUE, ordered_result = TRUE))
  }

  # Repeated count values can collapse quantile breaks; rank bins still balance folds.
  rank_y <- rank(y, ties.method = "average", na.last = "keep")
  rank_breaks <- unique(stats::quantile(rank_y, probs = probs, na.rm = TRUE, type = 7))
  if (length(rank_breaks) < 3) return(factor(rep("all", length(y))))
  cut(rank_y, breaks = rank_breaks, include.lowest = TRUE, ordered_result = TRUE)
}

make_stratified_fold_ids <- function(y, nfolds = 10, seed = 123, n_bins = 10) {
  if (length(y) < nfolds) {
    stop(sprintf("Need at least as many rows (%s) as folds (%s).", length(y), nfolds), call. = FALSE)
  }
  if (nfolds < 2) stop("nfolds must be at least 2.", call. = FALSE)

  set.seed(seed)
  strata <- make_strata(y, n_bins = n_bins)
  idx_by_stratum <- split(seq_along(y), strata, drop = TRUE)
  fold_ids <- integer(length(y))
  offset <- 0L

  for (ids in idx_by_stratum) {
    ids <- sample(ids)
    assigned <- ((seq_along(ids) + offset - 1L) %% nfolds) + 1L
    fold_ids[ids] <- assigned
    offset <- offset + length(ids)
  }

  empty_folds <- setdiff(seq_len(nfolds), unique(fold_ids))
  for (fold in empty_folds) {
    counts <- tabulate(fold_ids, nbins = nfolds)
    donor_fold <- which.max(counts)
    donor_id <- which(fold_ids == donor_fold)[1]
    fold_ids[donor_id] <- fold
  }

  fold_ids
}

make_year_blocked_fold_ids <- function(year_values, block_col = "year") {
  if (length(year_values) == 0) {
    stop("Year-blocked outer validation needs at least one row.", call. = FALSE)
  }

  year_labels <- trimws(as.character(year_values))
  if (anyNA(year_labels) || any(!nzchar(year_labels))) {
    stop(
      "Year-blocked outer validation column '", block_col,
      "' must not contain missing or blank values after filtering.",
      call. = FALSE
    )
  }

  unique_years <- unique(year_labels)
  if (length(unique_years) < 2L) {
    stop(
      "Year-blocked outer validation needs at least two distinct values in column '",
      block_col, "'.",
      call. = FALSE
    )
  }

  numeric_years <- suppressWarnings(as.numeric(unique_years))
  year_order <- if (all(!is.na(numeric_years))) {
    order(numeric_years, unique_years)
  } else {
    order(unique_years)
  }
  ordered_years <- unique_years[year_order]
  fold_ids <- match(year_labels, ordered_years)
  fold_plan <- data.table::data.table(
    fold = seq_along(ordered_years),
    outer_block_col = block_col,
    outer_block_value = ordered_years,
    n_rows = as.integer(tabulate(fold_ids, nbins = length(ordered_years)))
  )

  list(fold_ids = fold_ids, fold_plan = fold_plan)
}

make_custom_cv_from_fold_ids <- function(task, fold_ids) {
  if (length(fold_ids) != task$nrow) {
    stop("Fold IDs must have the same length as the task row count.", call. = FALSE)
  }
  if (anyNA(fold_ids)) stop("Fold IDs must not contain NA values.", call. = FALSE)
  nfolds <- max(fold_ids)
  if (nfolds < 2L) stop("Outer CV needs at least two folds.", call. = FALSE)
  missing_folds <- setdiff(seq_len(nfolds), unique(fold_ids))
  if (length(missing_folds) > 0) {
    stop("Outer CV fold IDs contain empty fold(s): ", paste(missing_folds, collapse = ", "), call. = FALSE)
  }

  test_sets <- lapply(seq_len(nfolds), function(k) which(fold_ids == k))
  train_sets <- lapply(test_sets, function(test) setdiff(seq_len(task$nrow), test))

  rsmp_custom <- mlr3::rsmp("custom")
  rsmp_custom$instantiate(task, train_sets = train_sets, test_sets = test_sets)
  rsmp_custom
}

make_stratified_custom_cv <- function(task, target, nfolds = 10, seed = 123, n_bins = 10) {
  y <- task$data(cols = target)[[1]]
  fold_ids <- make_stratified_fold_ids(y, nfolds = nfolds, seed = seed, n_bins = n_bins)
  make_custom_cv_from_fold_ids(task, fold_ids)
}

poisson_deviance_score <- function(truth, mean_pred) {
  if (!is.numeric(truth) || !is.numeric(mean_pred)) return(NA_real_)
  if (length(truth) != length(mean_pred)) return(NA_real_)
  if (any(truth < 0, na.rm = TRUE)) return(NA_real_)

  mu <- pmax(mean_pred, .Machine$double.eps)
  y <- truth
  term <- ifelse(y == 0, 0, y * log(y / mu))
  mean(2 * (term - (y - mu)), na.rm = TRUE)
}

wape_score <- function(truth, response) {
  denom <- sum(abs(truth), na.rm = TRUE)
  if (!is.finite(denom) || denom <= 0) return(NA_real_)
  sum(abs(response - truth), na.rm = TRUE) / denom
}

reg_metrics <- function(truth, response, negloglik = NULL) {
  err <- response - truth
  sse <- sum(err^2, na.rm = TRUE)
  sst <- sum((truth - mean(truth, na.rm = TRUE))^2, na.rm = TRUE)
  poisson_deviance <- poisson_deviance_score(truth, response)
  mean_negloglik <- if (is.null(negloglik)) NA_real_ else mean(negloglik, na.rm = TRUE)

  data.table::data.table(
    rmse = sqrt(mean(err^2, na.rm = TRUE)),
    mae = mean(abs(err), na.rm = TRUE),
    max_error = max(abs(err), na.rm = TRUE),
    sae = sum(err, na.rm = TRUE),
    mse = mean(err^2, na.rm = TRUE),
    bias = mean(err, na.rm = TRUE),
    r2 = if (isTRUE(all.equal(sst, 0))) NA_real_ else 1 - sse / sst,
    wape = wape_score(truth, response),
    poisson_deviance = poisson_deviance,
    negloglik = mean_negloglik
  )
}

make_target_context <- function(dt, target, feature_cols,
                                target_mode = "count",
                                target_denominator_col = NA_character_,
                                weight_col = NA_character_,
                                target_transform = "none",
                                offset_col = NA_character_,
                                model_target_col = ".model_target",
                                model_weight_col = ".weight",
                                model_offset_col = ".offset") {
  target_mode <- normalize_target_mode(target_mode)
  target_denominator_col <- normalize_optional_string(target_denominator_col)
  weight_col <- normalize_optional_string(weight_col)
  target_transform <- normalize_target_transform(target_transform)
  offset_col <- normalize_optional_string(offset_col)
  dt <- data.table::as.data.table(data.table::copy(dt))

  if (!target %in% names(dt)) {
    stop("Target column not found in modeling data: ", target, call. = FALSE)
  }
  if (!is.numeric(dt[[target]])) {
    stop("Target column must be numeric: ", target, call. = FALSE)
  }

  denominator <- rep(NA_real_, nrow(dt))
  if (identical(target_mode, "rate")) {
    if (is.na(target_denominator_col)) {
      stop("target_denominator_col is required when target_mode='rate'.", call. = FALSE)
    }
    if (!target_denominator_col %in% names(dt)) {
      stop("Target denominator column not found in modeling data: ", target_denominator_col, call. = FALSE)
    }
    denominator <- dt[[target_denominator_col]]
    if (!is.numeric(denominator)) {
      stop("Target denominator column must be numeric: ", target_denominator_col, call. = FALSE)
    }
    if (anyNA(denominator) || any(!is.finite(denominator)) || any(denominator <= 0)) {
      stop("Target denominator column must contain only finite values > 0: ", target_denominator_col, call. = FALSE)
    }
    raw_model_target <- dt[[target]] / denominator
    if (anyNA(raw_model_target) || any(!is.finite(raw_model_target))) {
      stop("Rate target contains non-finite values after dividing by ", target_denominator_col, ".", call. = FALSE)
    }
  } else {
    raw_model_target <- dt[[target]]
  }
  model_target <- apply_target_transform(raw_model_target, target_transform, context = "Model target")

  weights <- rep(NA_real_, nrow(dt))
  if (!is.na(weight_col)) {
    if (!weight_col %in% names(dt)) {
      stop("Weight column not found in modeling data: ", weight_col, call. = FALSE)
    }
    weights <- dt[[weight_col]]
    if (!is.numeric(weights)) {
      stop("Weight column must be numeric: ", weight_col, call. = FALSE)
    }
    if (anyNA(weights) || any(!is.finite(weights)) || any(weights <= 0)) {
      stop("Weight column must contain only finite values > 0: ", weight_col, call. = FALSE)
    }
  }

  offset <- rep(NA_real_, nrow(dt))
  if (!is.na(offset_col)) {
    if (!offset_col %in% names(dt)) {
      stop("Offset column not found in modeling data: ", offset_col, call. = FALSE)
    }
    offset <- dt[[offset_col]]
    if (!is.numeric(offset)) {
      stop("Offset column must be numeric: ", offset_col, call. = FALSE)
    }
    if (anyNA(offset) || any(!is.finite(offset))) {
      stop("Offset column must contain only finite values: ", offset_col, call. = FALSE)
    }
  }

  feature_cols <- unique(feature_cols)
  missing_features <- setdiff(feature_cols, names(dt))
  if (length(missing_features) > 0) {
    stop("Feature column(s) missing after target setup: ", paste(missing_features, collapse = ", "), call. = FALSE)
  }

  model_dt <- data.table::data.table(.model_target_tmp = model_target)
  data.table::setnames(model_dt, ".model_target_tmp", model_target_col)
  if (length(feature_cols) > 0) {
    model_dt <- data.table::data.table(model_dt, dt[, ..feature_cols])
  }
  if (!is.na(weight_col)) {
    model_dt[, (model_weight_col) := weights]
  }
  if (!is.na(offset_col)) {
    model_dt[, (model_offset_col) := offset]
  }

  list(
    data = model_dt,
    target = model_target_col,
    original_target = target,
    feature_cols = feature_cols,
    target_mode = target_mode,
    target_transform = target_transform,
    target_denominator_col = if (identical(target_mode, "rate")) target_denominator_col else NA_character_,
    weight_col = weight_col,
    offset_col = offset_col,
    model_weight_col = if (!is.na(weight_col)) model_weight_col else NA_character_,
    model_offset_col = if (!is.na(offset_col)) model_offset_col else NA_character_,
    original_truth = as.numeric(dt[[target]]),
    model_truth_untransformed = as.numeric(raw_model_target),
    denominator = as.numeric(denominator),
    weights = as.numeric(weights),
    offset = as.numeric(offset)
  )
}

postprocess_prediction_dt <- function(pred_dt, target_context) {
  if (is.null(target_context)) return(pred_dt)

  out <- data.table::as.data.table(data.table::copy(pred_dt))
  row_ids <- out$row_id
  model_truth <- out$truth
  model_response <- out$response
  original_truth_values <- target_context$original_truth[row_ids]
  denominator <- target_context$denominator[row_ids]
  weights <- target_context$weights[row_ids]
  model_truth_untransformed <- inverse_target_transform(model_truth, target_context$target_transform)
  model_response_untransformed <- inverse_target_transform(model_response, target_context$target_transform)
  response_values <- if (identical(target_context$target_mode, "rate")) {
    model_response_untransformed * denominator
  } else {
    model_response_untransformed
  }

  out[, truth := original_truth_values]
  out[, response := response_values]
  out[, error := response_values - original_truth_values]
  out[, abs_error := abs(response_values - original_truth_values)]
  out[, target_mode := target_context$target_mode]
  out[, model_truth := model_truth]
  out[, model_response := model_response]
  out[, model_error := model_response - model_truth]
  out[, model_abs_error := abs(model_response - model_truth)]
  out[, model_truth_untransformed := model_truth_untransformed]
  out[, model_response_untransformed := model_response_untransformed]
  out[, model_untransformed_error := model_response_untransformed - model_truth_untransformed]
  out[, model_untransformed_abs_error := abs(model_response_untransformed - model_truth_untransformed)]
  out[, denominator := if (identical(target_context$target_mode, "rate")) denominator else NA_real_]
  out[, weight := if (!is.na(target_context$weight_col)) weights else NA_real_]
  out[, target_transform := target_context$target_transform]
  out[, postprocessed_response := response_values]

  leading <- c("repeat", "row_id", "fold", "target_mode", "truth", "response", "error", "abs_error")
  data.table::setcolorder(out, c(intersect(leading, names(out)), setdiff(names(out), leading)))
  out
}

model_scale_fold_metrics_from_predictions <- function(predictions) {
  if (!all(c("model_truth", "model_response") %in% names(predictions))) {
    return(fold_metrics_from_predictions(predictions))
  }
  by_cols <- if ("repeat" %in% names(predictions)) c("repeat", "fold") else "fold"
  out <- predictions[, reg_metrics(model_truth, model_response), by = by_cols]
  if ("repeat" %in% names(out)) {
    data.table::setorderv(out, c("repeat", "fold"))
  } else {
    data.table::setorder(out, fold)
  }
  out
}

model_scale_aggregate_predictions <- function(predictions) {
  if (!all(c("model_truth", "model_response") %in% names(predictions))) {
    return(aggregate_predictions(predictions))
  }
  reg_metrics(predictions$model_truth, predictions$model_response)
}

exposure_baseline_predictions_from_fold_ids <- function(fold_ids, target_context, repeat_id = NULL) {
  if (is.null(target_context) || !identical(target_context$target_mode, "rate")) {
    return(data.table::data.table())
  }
  if (anyNA(fold_ids) || length(fold_ids) != length(target_context$original_truth)) {
    stop("Fold IDs must align with target context for exposure baseline.", call. = FALSE)
  }

  rows <- lapply(seq_len(max(fold_ids)), function(fold) {
    test_ids <- which(fold_ids == fold)
    train_ids <- which(fold_ids != fold)
    train_exposure <- sum(target_context$denominator[train_ids], na.rm = TRUE)
    if (!is.finite(train_exposure) || train_exposure <= 0) {
      stop("Exposure baseline needs positive training exposure in every fold.", call. = FALSE)
    }
    train_rate <- sum(target_context$original_truth[train_ids], na.rm = TRUE) / train_exposure
    response <- train_rate * target_context$denominator[test_ids]
    out <- data.table::data.table(
      row_id = test_ids,
      fold = fold,
      truth = target_context$original_truth[test_ids],
      response = response,
      error = response - target_context$original_truth[test_ids],
      abs_error = abs(response - target_context$original_truth[test_ids]),
      target_mode = "exposure_baseline",
      baseline_train_rate = train_rate,
      denominator = target_context$denominator[test_ids]
    )
    if (!is.null(repeat_id)) out[, "repeat" := as.integer(repeat_id)]
    out
  })

  out <- data.table::rbindlist(rows, fill = TRUE)
  leading <- c("repeat", "row_id", "fold", "target_mode", "truth", "response", "error", "abs_error")
  data.table::setcolorder(out, c(intersect(leading, names(out)), setdiff(names(out), leading)))
  out
}

predictions_from_resample <- function(rr) {
  pred_list <- rr$predictions()
  data.table::rbindlist(lapply(seq_along(pred_list), function(fold) {
    pred <- pred_list[[fold]]
    data.table::data.table(
      row_id = pred$row_ids,
      fold = fold,
      truth = pred$truth,
      response = pred$response,
      error = pred$response - pred$truth,
      abs_error = abs(pred$response - pred$truth)
    )
  }))
}

prediction_dt_from_prediction <- function(pred, fold, repeat_id = NULL, target_context = NULL) {
  out <- data.table::data.table(
    row_id = pred$row_ids,
    fold = fold,
    truth = pred$truth,
    response = pred$response,
    error = pred$response - pred$truth,
    abs_error = abs(pred$response - pred$truth)
  )
  if (!is.null(repeat_id)) {
    out[, "repeat" := as.integer(repeat_id)]
    data.table::setcolorder(out, c("repeat", setdiff(names(out), "repeat")))
  }
  postprocess_prediction_dt(out, target_context)
}

fold_metrics_from_predictions <- function(predictions) {
  by_cols <- if ("repeat" %in% names(predictions)) c("repeat", "fold") else "fold"
  out <- predictions[, reg_metrics(
    truth,
    response,
    negloglik = if ("negloglik" %in% names(predictions)) negloglik else NULL
  ), by = by_cols]
  if ("repeat" %in% names(out)) {
    data.table::setorderv(out, c("repeat", "fold"))
  } else {
    data.table::setorder(out, fold)
  }
  out
}

aggregate_predictions <- function(predictions) {
  reg_metrics(
    predictions$truth,
    predictions$response,
    negloglik = if ("negloglik" %in% names(predictions)) predictions$negloglik else NULL
  )
}

add_regression_stratum <- function(backend, target, n_bins = 10, col_name = ".target_stratum") {
  backend[[col_name]] <- make_strata(backend[[target]], n_bins = n_bins)
  backend
}

make_regr_task <- function(id, backend, target, stratum_col = ".target_stratum", weight_col = NULL, offset_col = NULL) {
  task <- mlr3::TaskRegr$new(id = id, backend = as.data.frame(backend), target = target)
  if (stratum_col %in% names(backend)) {
    task$set_col_roles(stratum_col, roles = "stratum")
  }
  if (!is.null(weight_col) && !is.na(weight_col) && nzchar(weight_col) && weight_col %in% names(backend)) {
    task$set_col_roles(weight_col, roles = c("weights_learner", "weights_measure"))
  }
  if (!is.null(offset_col) && !is.na(offset_col) && nzchar(offset_col) && offset_col %in% names(backend)) {
    task$set_col_roles(offset_col, roles = "offset")
  }
  task
}

encode_features_train_test <- function(train_dt, test_dt, target, unseen_level = ".__unseen__", weight_col = NULL, offset_col = NULL) {
  train_encoded_source <- data.table::as.data.table(data.table::copy(train_dt))
  test_encoded_source <- data.table::as.data.table(data.table::copy(test_dt))
  excluded_cols <- c(
    target,
    if (!is.null(weight_col) && !is.na(weight_col) && nzchar(weight_col)) weight_col else character(0),
    if (!is.null(offset_col) && !is.na(offset_col) && nzchar(offset_col)) offset_col else character(0)
  )
  feature_cols <- setdiff(names(train_encoded_source), excluded_cols)
  if (length(feature_cols) == 0) {
    stop("At least one predictor is required for feature encoding.", call. = FALSE)
  }

  factor_cols <- names(which(vapply(train_encoded_source[, ..feature_cols], function(x) {
    is.factor(x) || is.character(x)
  }, logical(1))))
  unseen_rows <- list()

  for (col in factor_cols) {
    train_values <- droplevels(factor(train_encoded_source[[col]]))
    train_levels <- levels(train_values)
    if (length(train_levels) == 0) {
      stop("Training fold has no observed levels for factor feature: ", col, call. = FALSE)
    }

    sentinel <- unseen_level
    while (sentinel %in% train_levels) {
      sentinel <- paste0(sentinel, "_")
    }
    model_levels <- c(train_levels, sentinel)

    train_encoded_source[[col]] <- factor(as.character(train_encoded_source[[col]]), levels = model_levels)

    test_values <- as.character(test_encoded_source[[col]])
    unseen <- !is.na(test_values) & !(test_values %in% train_levels)
    if (any(unseen)) {
      unseen_rows[[length(unseen_rows) + 1L]] <- data.table::data.table(
        feature = col,
        unseen_level = sort(unique(test_values[unseen])),
        n_rows = as.integer(tabulate(match(test_values[unseen], sort(unique(test_values[unseen])))))
      )
      test_values[unseen] <- sentinel
    }
    test_encoded_source[[col]] <- factor(test_values, levels = model_levels)
  }

  rhs <- stats::reformulate(feature_cols, intercept = FALSE)
  train_x <- stats::model.matrix(rhs, data = as.data.frame(train_encoded_source))
  test_x <- stats::model.matrix(rhs, data = as.data.frame(test_encoded_source))
  encoded_feature_names <- make.names(colnames(train_x), unique = TRUE)
  colnames(train_x) <- encoded_feature_names
  colnames(test_x) <- encoded_feature_names

  encoded_train <- data.table::data.table(train_encoded_source[, ..target], data.table::as.data.table(train_x))
  encoded_test <- data.table::data.table(test_encoded_source[, ..target], data.table::as.data.table(test_x))
  if (!is.null(weight_col) && !is.na(weight_col) && nzchar(weight_col) && weight_col %in% names(train_encoded_source)) {
    encoded_train[, (weight_col) := train_encoded_source[[weight_col]]]
    encoded_test[, (weight_col) := test_encoded_source[[weight_col]]]
  }
  if (!is.null(offset_col) && !is.na(offset_col) && nzchar(offset_col) && offset_col %in% names(train_encoded_source)) {
    encoded_train[, (offset_col) := train_encoded_source[[offset_col]]]
    encoded_test[, (offset_col) := test_encoded_source[[offset_col]]]
  }
  unseen_dt <- data.table::rbindlist(unseen_rows, fill = TRUE)

  list(
    train = encoded_train,
    test = encoded_test,
    feature_names = encoded_feature_names,
    factor_cols = factor_cols,
    unseen_levels = unseen_dt
  )
}

collect_tuning_results <- function(rr, measure_col = "regr.rmse") {
  rows <- lapply(seq_along(rr$learners), function(i) {
    learner <- rr$learners[[i]]
    tr <- data.table::as.data.table(learner$tuning_result)

    if ((nrow(tr) == 0 || ncol(tr) == 0) && !is.null(learner$archive)) {
      archive_dt <- data.table::as.data.table(learner$archive$data)
      if (nrow(archive_dt) > 0 && measure_col %in% names(archive_dt)) {
        tr <- archive_dt[which.min(archive_dt[[measure_col]])]
      }
    }

    if (nrow(tr) == 0) return(NULL)
    list_cols <- names(which(vapply(tr, is.list, logical(1))))
    for (col in list_cols) {
      tr[, (col) := vapply(.SD[[col]], function(x) paste(as.character(x), collapse = ","), character(1))]
    }
    tr[, outer_fold := i]
    tr
  })

  out <- data.table::rbindlist(rows, fill = TRUE)
  if (nrow(out) > 0) data.table::setcolorder(out, c("outer_fold", setdiff(names(out), "outer_fold")))
  out
}

collect_tuning_result_from_learner <- function(learner, outer_fold, measure_col = "regr.rmse", repeat_id = NULL) {
  tr <- data.table::as.data.table(learner$tuning_result)

  if ((nrow(tr) == 0 || ncol(tr) == 0) && !is.null(learner$archive)) {
    archive_dt <- data.table::as.data.table(learner$archive$data)
    if (nrow(archive_dt) > 0 && measure_col %in% names(archive_dt)) {
      tr <- archive_dt[which.min(archive_dt[[measure_col]])]
    }
  }

  if (nrow(tr) == 0) return(NULL)
  list_cols <- names(which(vapply(tr, is.list, logical(1))))
  for (col in list_cols) {
    tr[, (col) := vapply(.SD[[col]], function(x) paste(as.character(x), collapse = ","), character(1))]
  }
  tr[, outer_fold := outer_fold]
  if (!is.null(repeat_id)) {
    tr[, "repeat" := as.integer(repeat_id)]
    data.table::setcolorder(tr, c("repeat", "outer_fold", setdiff(names(tr), c("repeat", "outer_fold"))))
  }
  tr
}

run_autotuner_outer_cv <- function(task, auto_tuner, outer_cv, seed,
                                   progress_prefix = NULL,
                                   measure_col = "regr.rmse",
                                   repeat_id = NULL,
                                   target_context = NULL) {
  n_folds <- outer_cv$iters
  prediction_rows <- vector("list", n_folds)
  tuning_rows <- vector("list", n_folds)
  fitted_learners <- vector("list", n_folds)

  log_progress <- function(...) {
    if (!is.null(progress_prefix) && nzchar(progress_prefix)) {
      log_info(progress_prefix, ": ", ...)
    } else {
      log_info(...)
    }
  }

  log_progress("starting outer CV across ", n_folds, " fold(s)")
  for (fold in seq_len(n_folds)) {
    fold_started_at <- Sys.time()
    train_ids <- outer_cv$train_set(fold)
    test_ids <- outer_cv$test_set(fold)
    log_progress(
      if (!is.null(repeat_id)) paste0("repeat ", repeat_id, ", ") else "",
      "outer fold ", fold, "/", n_folds,
      ": train rows=", length(train_ids),
      ", test rows=", length(test_ids)
    )

    at_fold <- auto_tuner$clone(deep = TRUE)
    set.seed(as.integer(seed) + fold - 1L)
    at_fold$train(task, row_ids = train_ids)
    pred <- at_fold$predict(task, row_ids = test_ids)

    prediction_rows[[fold]] <- prediction_dt_from_prediction(
      pred,
      fold = fold,
      repeat_id = repeat_id,
      target_context = target_context
    )
    tuning_rows[[fold]] <- collect_tuning_result_from_learner(
      at_fold,
      outer_fold = fold,
      measure_col = measure_col,
      repeat_id = repeat_id
    )
    fitted_learners[[fold]] <- at_fold

    log_progress(
      if (!is.null(repeat_id)) paste0("repeat ", repeat_id, ", ") else "",
      "outer fold ", fold, "/", n_folds,
      " finished in ",
      format(round(as.numeric(difftime(Sys.time(), fold_started_at, units = "secs")), 2), nsmall = 2),
      "s"
    )
  }

  predictions <- data.table::rbindlist(prediction_rows, fill = TRUE)
  tuning_results <- data.table::rbindlist(tuning_rows, fill = TRUE)
  if (nrow(tuning_results) > 0) {
    if ("repeat" %in% names(tuning_results)) {
      data.table::setcolorder(tuning_results, c("repeat", "outer_fold", setdiff(names(tuning_results), c("repeat", "outer_fold"))))
    } else {
      data.table::setcolorder(tuning_results, c("outer_fold", setdiff(names(tuning_results), "outer_fold")))
    }
  }

  list(
    predictions = predictions,
    tuning_results = tuning_results,
    learners = fitted_learners
  )
}

run_repeated_autotuner_outer_cv <- function(task, auto_tuner, target, n_folds,
                                            outer_repeats = 1L, seed, n_bins = 10,
                                            progress_prefix = NULL,
                                            measure_col = "regr.rmse",
                                            outer_resampling = "stratified",
                                            outer_block_values = NULL,
                                            outer_block_col = NULL,
                                            target_context = NULL) {
  if (outer_repeats < 1L) {
    stop("outer_repeats must be at least 1.", call. = FALSE)
  }
  outer_resampling <- normalize_outer_resampling(outer_resampling)
  if (identical(outer_resampling, "year_blocked") && outer_repeats > 1L) {
    stop("outer_repeats must be 1 when outer_resampling='year_blocked'.", call. = FALSE)
  }

  predictions_by_repeat <- vector("list", outer_repeats)
  baseline_by_repeat <- vector("list", outer_repeats)
  tuning_by_repeat <- vector("list", outer_repeats)
  learners_by_repeat <- vector("list", outer_repeats)
  include_repeat <- outer_repeats > 1L
  fold_plan <- NULL

  for (repeat_idx in seq_len(outer_repeats)) {
    repeat_seed <- as.integer(seed) + (repeat_idx - 1L) * 1000L
    if (identical(outer_resampling, "year_blocked")) {
      block_col <- if (is.null(outer_block_col) || is.na(outer_block_col) || !nzchar(outer_block_col)) "year" else outer_block_col
      blocked_plan <- make_year_blocked_fold_ids(outer_block_values, block_col = block_col)
      fold_ids <- blocked_plan$fold_ids
      fold_plan <- blocked_plan$fold_plan
      outer_cv <- make_custom_cv_from_fold_ids(task, fold_ids)
    } else {
      y <- task$data(cols = target)[[1]]
      fold_ids <- make_stratified_fold_ids(y, nfolds = n_folds, seed = repeat_seed, n_bins = n_bins)
      outer_cv <- make_custom_cv_from_fold_ids(task, fold_ids)
    }
    repeat_run <- run_autotuner_outer_cv(
      task = task,
      auto_tuner = auto_tuner,
      outer_cv = outer_cv,
      seed = repeat_seed,
      progress_prefix = progress_prefix,
      measure_col = measure_col,
      repeat_id = if (include_repeat) repeat_idx else NULL,
      target_context = target_context
    )
    predictions_by_repeat[[repeat_idx]] <- repeat_run$predictions
    baseline_by_repeat[[repeat_idx]] <- exposure_baseline_predictions_from_fold_ids(
      fold_ids,
      target_context,
      repeat_id = if (include_repeat) repeat_idx else NULL
    )
    tuning_by_repeat[[repeat_idx]] <- repeat_run$tuning_results
    learners_by_repeat[[repeat_idx]] <- repeat_run$learners
  }

  predictions <- data.table::rbindlist(predictions_by_repeat, fill = TRUE)
  baseline_predictions <- data.table::rbindlist(baseline_by_repeat, fill = TRUE)
  tuning_results <- data.table::rbindlist(tuning_by_repeat, fill = TRUE)
  if (nrow(tuning_results) > 0) {
    if ("repeat" %in% names(tuning_results)) {
      data.table::setcolorder(tuning_results, c("repeat", "outer_fold", setdiff(names(tuning_results), c("repeat", "outer_fold"))))
    } else {
      data.table::setcolorder(tuning_results, c("outer_fold", setdiff(names(tuning_results), "outer_fold")))
    }
  }

  list(
    predictions = predictions,
    baseline_predictions = baseline_predictions,
    tuning_results = tuning_results,
    learners = learners_by_repeat,
    fold_plan = fold_plan
  )
}

run_repeated_encoded_autotuner_outer_cv <- function(raw_dt, target, auto_tuner,
                                                    n_folds, outer_repeats = 1L,
                                                    seed, n_bins = 10,
                                                    task_id = "encoded_regression",
                                                    progress_prefix = NULL,
                                                    measure_col = "regr.rmse",
                                                    outer_resampling = "stratified",
                                                    outer_block_values = NULL,
                                                    outer_block_col = NULL,
                                                    target_context = NULL,
                                                    weight_col = NULL,
                                                    offset_col = NULL) {
  if (outer_repeats < 1L) {
    stop("outer_repeats must be at least 1.", call. = FALSE)
  }
  outer_resampling <- normalize_outer_resampling(outer_resampling)
  if (identical(outer_resampling, "year_blocked") && outer_repeats > 1L) {
    stop("outer_repeats must be 1 when outer_resampling='year_blocked'.", call. = FALSE)
  }

  raw_dt <- data.table::as.data.table(data.table::copy(raw_dt))
  predictions_by_repeat <- vector("list", outer_repeats)
  baseline_by_repeat <- vector("list", outer_repeats)
  tuning_by_repeat <- vector("list", outer_repeats)
  learners_by_repeat <- vector("list", outer_repeats)
  unseen_by_repeat <- vector("list", outer_repeats)
  include_repeat <- outer_repeats > 1L
  fold_plan <- NULL

  log_progress <- function(...) {
    if (!is.null(progress_prefix) && nzchar(progress_prefix)) {
      log_info(progress_prefix, ": ", ...)
    } else {
      log_info(...)
    }
  }

  for (repeat_idx in seq_len(outer_repeats)) {
    repeat_seed <- as.integer(seed) + (repeat_idx - 1L) * 1000L
    if (identical(outer_resampling, "year_blocked")) {
      block_col <- if (is.null(outer_block_col) || is.na(outer_block_col) || !nzchar(outer_block_col)) "year" else outer_block_col
      blocked_plan <- make_year_blocked_fold_ids(outer_block_values, block_col = block_col)
      fold_ids <- blocked_plan$fold_ids
      fold_plan <- blocked_plan$fold_plan
    } else {
      fold_ids <- make_stratified_fold_ids(raw_dt[[target]], nfolds = n_folds, seed = repeat_seed, n_bins = n_bins)
    }
    effective_n_folds <- max(fold_ids)
    prediction_rows <- vector("list", effective_n_folds)
    tuning_rows <- vector("list", effective_n_folds)
    fitted_learners <- vector("list", effective_n_folds)
    unseen_rows <- vector("list", effective_n_folds)

    log_progress(
      if (include_repeat) paste0("repeat ", repeat_idx, ": ") else "",
      "starting encoded outer CV across ", effective_n_folds, " fold(s)"
    )

    for (fold in seq_len(effective_n_folds)) {
      fold_started_at <- Sys.time()
      train_ids <- which(fold_ids != fold)
      test_ids <- which(fold_ids == fold)
      log_progress(
        if (include_repeat) paste0("repeat ", repeat_idx, ", ") else "",
        "outer fold ", fold, "/", effective_n_folds,
        ": train rows=", length(train_ids),
        ", test rows=", length(test_ids)
      )

      encoded <- encode_features_train_test(
        raw_dt[train_ids],
        raw_dt[test_ids],
        target = target,
        weight_col = weight_col,
        offset_col = offset_col
      )
      train_task <- mlr3::TaskRegr$new(
        id = sprintf("%s_train_r%s_f%s", task_id, repeat_idx, fold),
        backend = as.data.frame(encoded$train),
        target = target
      )
      test_task <- mlr3::TaskRegr$new(
        id = sprintf("%s_test_r%s_f%s", task_id, repeat_idx, fold),
        backend = as.data.frame(encoded$test),
        target = target
      )
      if (!is.null(weight_col) && !is.na(weight_col) && nzchar(weight_col) && weight_col %in% names(encoded$train)) {
        train_task$set_col_roles(weight_col, roles = c("weights_learner", "weights_measure"))
        test_task$set_col_roles(weight_col, roles = c("weights_learner", "weights_measure"))
      }
      if (!is.null(offset_col) && !is.na(offset_col) && nzchar(offset_col) && offset_col %in% names(encoded$train)) {
        train_task$set_col_roles(offset_col, roles = "offset")
        test_task$set_col_roles(offset_col, roles = "offset")
      }

      at_fold <- auto_tuner$clone(deep = TRUE)
      set.seed(repeat_seed + fold - 1L)
      at_fold$train(train_task)
      pred <- at_fold$predict(test_task)

      prediction_rows[[fold]] <- data.table::data.table(
        row_id = test_ids,
        fold = fold,
        truth = pred$truth,
        response = pred$response,
        error = pred$response - pred$truth,
        abs_error = abs(pred$response - pred$truth)
      )
      if (include_repeat) {
        prediction_rows[[fold]][, "repeat" := as.integer(repeat_idx)]
        data.table::setcolorder(prediction_rows[[fold]], c("repeat", setdiff(names(prediction_rows[[fold]]), "repeat")))
      }
      prediction_rows[[fold]] <- postprocess_prediction_dt(prediction_rows[[fold]], target_context)

      tuning_rows[[fold]] <- collect_tuning_result_from_learner(
        at_fold,
        outer_fold = fold,
        measure_col = measure_col,
        repeat_id = if (include_repeat) repeat_idx else NULL
      )
      fitted_learners[[fold]] <- at_fold

      if (nrow(encoded$unseen_levels) > 0) {
        unseen <- data.table::copy(encoded$unseen_levels)
        unseen[, outer_fold := fold]
        if (include_repeat) unseen[, "repeat" := as.integer(repeat_idx)]
        unseen_rows[[fold]] <- unseen
      }

      log_progress(
        if (include_repeat) paste0("repeat ", repeat_idx, ", ") else "",
        "outer fold ", fold, "/", effective_n_folds,
        " finished in ",
        format(round(as.numeric(difftime(Sys.time(), fold_started_at, units = "secs")), 2), nsmall = 2),
        "s"
      )
    }

    predictions_by_repeat[[repeat_idx]] <- data.table::rbindlist(prediction_rows, fill = TRUE)
    baseline_by_repeat[[repeat_idx]] <- exposure_baseline_predictions_from_fold_ids(
      fold_ids,
      target_context,
      repeat_id = if (include_repeat) repeat_idx else NULL
    )
    tuning_by_repeat[[repeat_idx]] <- data.table::rbindlist(tuning_rows, fill = TRUE)
    learners_by_repeat[[repeat_idx]] <- fitted_learners
    unseen_by_repeat[[repeat_idx]] <- data.table::rbindlist(unseen_rows, fill = TRUE)
  }

  predictions <- data.table::rbindlist(predictions_by_repeat, fill = TRUE)
  baseline_predictions <- data.table::rbindlist(baseline_by_repeat, fill = TRUE)
  tuning_results <- data.table::rbindlist(tuning_by_repeat, fill = TRUE)
  unseen_levels <- data.table::rbindlist(unseen_by_repeat, fill = TRUE)
  if (nrow(tuning_results) > 0) {
    if ("repeat" %in% names(tuning_results)) {
      data.table::setcolorder(tuning_results, c("repeat", "outer_fold", setdiff(names(tuning_results), c("repeat", "outer_fold"))))
    } else {
      data.table::setcolorder(tuning_results, c("outer_fold", setdiff(names(tuning_results), "outer_fold")))
    }
  }

  list(
    predictions = predictions,
    baseline_predictions = baseline_predictions,
    tuning_results = tuning_results,
    learners = learners_by_repeat,
    unseen_levels = unseen_levels,
    fold_plan = fold_plan
  )
}

safe_write_csv <- function(dt, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  data.table::fwrite(dt, path)
  rds_path <- if (grepl("\\.csv$", path)) sub("\\.csv$", ".rds", path) else paste0(path, ".rds")
  saveRDS(dt, rds_path)
}

safe_git_commit <- function(repo_dir) {
  git_path <- Sys.which("git")
  if (!nzchar(git_path)) return(NA_character_)

  commit <- tryCatch(
    system2(git_path, c("-C", repo_dir, "rev-parse", "HEAD"), stdout = TRUE, stderr = FALSE),
    error = function(e) character(0)
  )
  if (length(commit) == 0) return(NA_character_)
  trimws(commit[[1]])
}

package_versions_string <- function(packages) {
  packages <- unique(packages[nzchar(packages)])
  if (length(packages) == 0) return(NA_character_)

  versions <- vapply(packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) return(NA_character_)
    as.character(utils::packageVersion(pkg))
  }, character(1))

  paste(sprintf("%s==%s", packages, versions), collapse = "; ")
}

write_run_manifest <- function(output_dir, script_name, log_state, repo_dir,
                               packages = character(0), status = "completed",
                               seed = NA, data_path = NA_character_,
                               feature_cols = character(0), n_workers = NA,
                               run_name = NA_character_) {
  started_at <- if (!is.null(log_state$started_at)) log_state$started_at else Sys.time()
  ended_at <- Sys.time()
  runtime_seconds <- as.numeric(difftime(ended_at, started_at, units = "secs"))
  cpu_cores_physical <- detect_cpu_cores(logical = FALSE)
  cpu_cores_logical <- detect_cpu_cores(logical = TRUE)

  manifest <- data.table::data.table(
    script_name = script_name,
    run_name = if (is.na(run_name)[1]) NA_character_ else as.character(run_name)[1],
    status = status,
    seed = if (length(seed) == 0) NA_real_ else as.numeric(seed)[1],
    data_path = if (is.na(data_path)[1]) NA_character_ else normalizePath(data_path, mustWork = FALSE),
    feature_list = if (length(feature_cols) > 0) paste(feature_cols, collapse = ", ") else NA_character_,
    git_commit = safe_git_commit(repo_dir),
    start_time = format(started_at, "%Y-%m-%d %H:%M:%S %Z"),
    end_time = format(ended_at, "%Y-%m-%d %H:%M:%S %Z"),
    runtime_seconds = runtime_seconds,
    workers = if (length(n_workers) == 0) NA_real_ else as.numeric(n_workers)[1],
    cpu_cores_physical = if (length(cpu_cores_physical) == 0) NA_real_ else as.numeric(cpu_cores_physical)[1],
    cpu_cores_logical = if (length(cpu_cores_logical) == 0) NA_real_ else as.numeric(cpu_cores_logical)[1],
    package_versions = package_versions_string(packages)
  )

  safe_write_csv(manifest, file.path(output_dir, "run_manifest.csv"))
  manifest
}

write_text_file <- function(path, lines) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  writeLines(as.character(lines), con = path, useBytes = TRUE)
  invisible(path)
}

write_config_snapshot <- function(output_dir, resolved_config, prefix = "resolved_config") {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  saveRDS(resolved_config, file.path(output_dir, paste0(prefix, ".rds")))
  write_text_file(
    file.path(output_dir, paste0(prefix, ".txt")),
    capture.output(str(resolved_config, max.level = 4, give.attr = FALSE))
  )
}

write_session_info <- function(output_dir, prefix = "session_info") {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  old_tz <- Sys.getenv("TZ", unset = NA_character_)
  if (is.na(old_tz) || !nzchar(old_tz)) {
    Sys.setenv(TZ = "UTC")
    on.exit(Sys.unsetenv("TZ"), add = TRUE)
  }
  write_text_file(
    file.path(output_dir, paste0(prefix, ".txt")),
    capture.output(suppressWarnings(utils::sessionInfo()))
  )
}

dataset_overview <- function(dt, target = NULL, feature_cols = character(0), id_cols = character(0)) {
  factor_cols <- names(which(vapply(dt, is.factor, logical(1))))
  numeric_cols <- names(which(vapply(dt, is.numeric, logical(1))))

  data.table::data.table(
    n_rows = nrow(dt),
    n_cols = ncol(dt),
    target = if (is.null(target)) NA_character_ else target,
    n_features = length(feature_cols),
    feature_list = if (length(feature_cols) > 0) paste(feature_cols, collapse = ", ") else NA_character_,
    id_cols = if (length(id_cols) > 0) paste(id_cols, collapse = ", ") else NA_character_,
    n_factor_cols = length(factor_cols),
    factor_cols = if (length(factor_cols) > 0) paste(factor_cols, collapse = ", ") else NA_character_,
    n_numeric_cols = length(numeric_cols),
    numeric_cols = if (length(numeric_cols) > 0) paste(numeric_cols, collapse = ", ") else NA_character_
  )
}

log_dataset_overview <- function(dt, target = NULL, feature_cols = character(0), id_cols = character(0),
                                 metric = NULL, extra = NULL) {
  overview <- dataset_overview(dt, target = target, feature_cols = feature_cols, id_cols = id_cols)
  log_info("Dataset rows / cols: ", overview$n_rows[[1]], " / ", overview$n_cols[[1]])
  if (!is.null(target)) log_info("Target: ", target)
  if (length(feature_cols) > 0) log_info("Feature columns: ", overview$feature_list[[1]])
  if (length(id_cols) > 0) log_info("Identifier columns: ", overview$id_cols[[1]])
  log_info("Factor columns: ", overview$n_factor_cols[[1]], " | Numeric columns: ", overview$n_numeric_cols[[1]])
  if (!is.null(metric) && nzchar(metric)) log_info("Optimization / ranking metric: ", metric)
  if (!is.null(extra) && length(extra) > 0) {
    for (label in names(extra)) {
      log_info(label, ": ", extra[[label]])
    }
  }
  invisible(overview)
}

data_dictionary <- function(dt, target = NULL, feature_cols = character(0), id_cols = character(0)) {
  roles_for_column <- function(col) {
    roles <- character(0)
    if (!is.null(target) && identical(col, target)) roles <- c(roles, "target")
    if (col %in% feature_cols) roles <- c(roles, "feature")
    if (col %in% id_cols) roles <- c(roles, "id")
    if (length(roles) == 0) roles <- "other"
    paste(roles, collapse = ", ")
  }

  data.table::rbindlist(lapply(names(dt), function(col) {
    values <- dt[[col]]
    data.table::data.table(
      column = col,
      role = roles_for_column(col),
      class = paste(class(values), collapse = ", "),
      typeof = typeof(values),
      n_missing = sum(is.na(values)),
      pct_missing = mean(is.na(values)),
      n_unique = data.table::uniqueN(values, na.rm = FALSE),
      is_numeric = is.numeric(values),
      is_factor = is.factor(values),
      is_character = is.character(values)
    )
  }), fill = TRUE)
}

append_registry_entry <- function(registry_path, row_dt, key_cols = "run_id") {
  dir.create(dirname(registry_path), recursive = TRUE, showWarnings = FALSE)
  existing <- read_csv_if_exists(registry_path)
  if (is.null(existing)) {
    updated <- data.table::as.data.table(data.table::copy(row_dt))
  } else {
    existing <- data.table::as.data.table(existing)
    row_dt <- data.table::as.data.table(data.table::copy(row_dt))
    if (!all(key_cols %in% names(row_dt))) {
      stop("Registry row is missing required key column(s): ", paste(setdiff(key_cols, names(row_dt)), collapse = ", "), call. = FALSE)
    }
    if (!all(key_cols %in% names(existing))) {
      updated <- data.table::rbindlist(list(existing, row_dt), fill = TRUE)
    } else {
      existing_keys <- do.call(paste, c(existing[, ..key_cols], sep = "\r"))
      new_keys <- do.call(paste, c(row_dt[, ..key_cols], sep = "\r"))
      existing <- existing[!existing_keys %in% new_keys]
      updated <- data.table::rbindlist(list(existing, row_dt), fill = TRUE)
    }
  }
  safe_write_csv(updated, registry_path)
  invisible(updated)
}

coerce_value_for_param <- function(value) {
  if (length(value) != 1) return(value)
  if (is.logical(value) || is.numeric(value) || is.integer(value)) return(value)
  if (is.character(value)) {
    lower <- tolower(value)
    if (lower %in% c("true", "false")) return(lower == "true")
    numeric_value <- suppressWarnings(as.numeric(value))
    if (!is.na(numeric_value)) {
      if (grepl("^[+-]?[0-9]+$", value)) return(as.integer(numeric_value))
      return(numeric_value)
    }
  }
  value
}

extract_param_values_from_tuning <- function(dt, sort_measure = "regr.rmse") {
  if (is.null(dt) || nrow(dt) == 0) return(list())
  rows <- data.table::as.data.table(data.table::copy(dt))
  if (sort_measure %in% names(rows)) data.table::setorderv(rows, sort_measure, order = 1L)
  row <- rows[1]
  drop_cols <- c(
    "repeat", "outer_fold", "regr.rmse", "warnings", "errors", "batch_nr", "runtime_learners",
    "timestamp", "uhash", "learner_param_vals", "x_domain", "resample_result"
  )
  keep_cols <- setdiff(names(row), drop_cols)
  values <- lapply(keep_cols, function(col) coerce_value_for_param(row[[col]][[1]]))
  names(values) <- keep_cols
  values
}

read_csv_if_exists <- function(path) {
  if (!file.exists(path)) return(NULL)
  data.table::fread(path)
}

manifest_status_summary <- function(output_dir, required_files = character(0)) {
  manifest_path <- file.path(output_dir, "run_manifest.csv")
  manifest <- read_csv_if_exists(manifest_path)

  missing_files <- required_files[!file.exists(file.path(output_dir, required_files))]
  manifest_status <- if (!is.null(manifest) && "status" %in% names(manifest) && nrow(manifest) > 0) {
    as.character(manifest$status[[1]])
  } else {
    NA_character_
  }

  status <- "ok"
  reason <- "required files present"

  if (!file.exists(output_dir)) {
    status <- "missing_directory"
    reason <- "output directory does not exist"
  } else if (is.null(manifest)) {
    if (length(missing_files) == length(required_files)) {
      status <- "missing_outputs"
      reason <- "run manifest and required output files are missing"
    } else {
      status <- "incomplete_outputs"
      reason <- "required files exist but run manifest is missing"
    }
  } else if (identical(manifest_status, "skipped")) {
    status <- "skipped"
    reason <- "run intentionally skipped"
  } else if (!identical(manifest_status, "completed")) {
    status <- "failed_run"
    reason <- sprintf("run manifest status is '%s'", manifest_status)
  } else if (length(missing_files) > 0) {
    status <- "incomplete_outputs"
    reason <- paste("missing required file(s):", paste(missing_files, collapse = ", "))
  }

  data.table::data.table(
    output_dir = normalizePath(output_dir, mustWork = FALSE),
    manifest_path = normalizePath(manifest_path, mustWork = FALSE),
    manifest_status = manifest_status,
    availability_status = status,
    availability_reason = reason
  )
}

finalize_run <- function(log_state, output_dir, script_name, repo_dir,
                         packages = character(0), status = "completed",
                         seed = NA, data_path = NA_character_,
                         feature_cols = character(0), n_workers = NA,
                         run_name = NA_character_) {
  manifest_error <- NULL
  session_info_error <- NULL

  tryCatch(
    invisible(write_run_manifest(
      output_dir = output_dir,
      script_name = script_name,
      log_state = log_state,
      repo_dir = repo_dir,
      packages = packages,
      status = status,
      seed = seed,
      data_path = data_path,
      feature_cols = feature_cols,
      n_workers = n_workers,
      run_name = run_name
    )),
    error = function(e) {
      manifest_error <<- conditionMessage(e)
      NULL
    }
  )

  tryCatch(
    invisible(write_session_info(output_dir)),
    error = function(e) {
      session_info_error <<- conditionMessage(e)
      NULL
    }
  )

  if (!is.null(manifest_error)) {
    log_info("Warning: failed to write run_manifest: ", manifest_error)
  }
  if (!is.null(session_info_error)) {
    log_info("Warning: failed to write session_info: ", session_info_error)
  }

  stop_logging(log_state, status = status)
  invisible(NULL)
}

with_run_finalizer <- function(code, finalizer) {
  force(finalizer)
  eval_env <- parent.frame()
  expr <- substitute(code)

  runner <- function() {
    on.exit(finalizer(), add = TRUE)
    tryCatch(
      eval(expr, envir = eval_env),
      error = function(e) {
        log_info("Error: ", conditionMessage(e))
        stop(e)
      }
    )
  }

  runner()
}

write_tabular_dataset <- function(dt, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  ext <- file_extension(path)

  if (ext == "csv") {
    data.table::fwrite(dt, path)
    return(invisible(path))
  }
  if (ext %in% c("tsv", "txt", "tab")) {
    data.table::fwrite(dt, path, sep = "\t")
    return(invisible(path))
  }
  if (ext == "rds") {
    saveRDS(dt, path)
    return(invisible(path))
  }
  if (ext %in% c("rda", "rdata")) {
    dataset <- as.data.frame(dt)
    save(dataset, file = path)
    return(invisible(path))
  }

  stop(
    "Unsupported output extension '.", ext, "' for ",
    normalizePath(path, mustWork = FALSE),
    ". Supported extensions: csv, tsv, txt, tab, rds, rda, RData.",
    call. = FALSE
  )
}

write_dataset_formats <- function(dt, output_dir, output_basename, formats) {
  if (length(formats) == 0) {
    stop("At least one output format must be provided.", call. = FALSE)
  }

  formats <- unique(tolower(formats))
  paths <- file.path(output_dir, paste0(output_basename, ".", formats))
  for (path in paths) {
    write_tabular_dataset(dt, path)
  }
  data.table::data.table(format = formats, path = paths)
}

build_dataset_metadata <- function(original_dt, processed_dt, source_path,
                                   filter_expression = "", rows_removed_by_filter = 0L,
                                   keep_cols = character(0), drop_cols = character(0),
                                   drop_missing_rows = FALSE, rows_removed_by_na_omit = 0L,
                                   sample_rows = NA_integer_, sample_seed = NA_integer_,
                                   rows_removed_by_subsampling = 0L,
                                   output_files = NULL) {
  source_path <- normalizePath(source_path, mustWork = FALSE)

  summary_dt <- data.table::data.table(
    source_path = source_path,
    created_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"),
    rows_input = nrow(original_dt),
    cols_input = ncol(original_dt),
    rows_output = nrow(processed_dt),
    cols_output = ncol(processed_dt),
    rows_removed = nrow(original_dt) - nrow(processed_dt),
    rows_removed_by_filter = as.integer(rows_removed_by_filter),
    rows_removed_by_na_omit = as.integer(rows_removed_by_na_omit),
    rows_removed_by_subsampling = as.integer(rows_removed_by_subsampling),
    filter_expression = if (nzchar(filter_expression)) filter_expression else NA_character_,
    keep_cols = if (length(keep_cols) > 0) paste(keep_cols, collapse = ", ") else NA_character_,
    drop_cols = if (length(drop_cols) > 0) paste(drop_cols, collapse = ", ") else NA_character_,
    drop_missing_rows = drop_missing_rows,
    sample_rows = if (is.na(sample_rows)) NA_integer_ else as.integer(sample_rows),
    sample_seed = if (is.na(sample_seed)) NA_integer_ else as.integer(sample_seed)
  )

  column_metadata <- data.table::rbindlist(lapply(names(processed_dt), function(col) {
    values <- processed_dt[[col]]
    non_missing <- values[!is.na(values)]
    example_value <- if (length(non_missing) > 0) as.character(non_missing[[1]]) else NA_character_
    data.table::data.table(
      column = col,
      class = paste(class(values), collapse = ", "),
      typeof = typeof(values),
      n_levels = if (is.factor(values)) nlevels(values) else NA_integer_,
      n_missing = sum(is.na(values)),
      pct_missing = mean(is.na(values)),
      n_unique = data.table::uniqueN(values, na.rm = FALSE),
      example_value = example_value
    )
  }), fill = TRUE)

  output_files_dt <- if (is.null(output_files)) {
    data.table::data.table(file_role = character(0), path = character(0))
  } else {
    data.table::as.data.table(data.table::copy(output_files))
  }

  list(
    summary = summary_dt,
    columns = column_metadata,
    output_files = output_files_dt
  )
}

write_metadata_bundle <- function(metadata, output_dir, prefix = "dataset_metadata") {
  safe_write_csv(metadata$summary, file.path(output_dir, paste0(prefix, "_summary.csv")))
  safe_write_csv(metadata$columns, file.path(output_dir, paste0(prefix, "_columns.csv")))
  safe_write_csv(metadata$output_files, file.path(output_dir, paste0(prefix, "_files.csv")))
  saveRDS(metadata, file.path(output_dir, paste0(prefix, ".rds")))
}

timestamp <- function() {
  format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
}

.project_logging_state <- new.env(parent = emptyenv())
.project_logging_state$path <- NULL

detect_cpu_cores <- function(logical = TRUE) {
  cores <- tryCatch(
    parallel::detectCores(logical = logical),
    error = function(e) NA_integer_
  )
  if (length(cores) == 0 || is.na(cores) || cores < 1) {
    return(NA_integer_)
  }
  as.integer(cores[[1]])
}

cpu_core_summary <- function() {
  physical <- detect_cpu_cores(logical = FALSE)
  logical <- detect_cpu_cores(logical = TRUE)

  if (!is.na(physical) && !is.na(logical) && physical != logical) {
    return(sprintf("%s physical / %s logical", physical, logical))
  }
  if (!is.na(logical)) {
    return(sprintf("%s", logical))
  }
  if (!is.na(physical)) {
    return(sprintf("%s", physical))
  }
  "unknown"
}

log_info <- function(...) {
  line <- paste0("[", timestamp(), "] ", paste0(..., collapse = ""))
  cat(line, "\n", sep = "")
  log_path <- .project_logging_state$path
  if (!is.null(log_path) && nzchar(log_path)) {
    try(write(line, file = log_path, append = TRUE), silent = TRUE)
  }
  flush.console()
}

start_logging <- function(output_dir, script_name) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  log_path <- file.path(output_dir, sprintf("%s.log", script_name))
  started_at <- Sys.time()
  writeLines(character(0), con = log_path, useBytes = TRUE)
  .project_logging_state$path <- log_path

  log_info("Started ", script_name)
  log_info("Log file: ", normalizePath(log_path, mustWork = FALSE))
  log_info("R version: ", R.version.string)
  log_info("Detected CPU cores: ", cpu_core_summary())
  log_info("Command: ", paste(commandArgs(), collapse = " "))

  list(path = log_path, started_at = started_at, script_name = script_name)
}

stop_logging <- function(log_state, status = "completed") {
  if (is.null(log_state)) return(invisible(NULL))
  if (isTRUE(log_state$closed)) return(invisible(NULL))

  log_info("Finished with status: ", status)
  .project_logging_state$path <- NULL
  log_state$closed <- TRUE
  invisible(NULL)
}
