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
  if (!is.null(arg_value) && nzchar(arg_value)) return(arg_value)

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

load_project_config <- function(repo_dir) {
  config_path <- file.path(repo_dir, "config.R")
  if (!file.exists(config_path)) {
    stop("Could not find config.R in repository root: ", normalizePath(config_path, mustWork = FALSE), call. = FALSE)
  }

  config_env <- new.env(parent = baseenv())
  sys.source(config_path, envir = config_env)
  if (!exists("CONFIG", envir = config_env, inherits = FALSE)) {
    stop("config.R must define an object named CONFIG.", call. = FALSE)
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

parse_csv_setting <- function(value) {
  if (is.null(value) || !nzchar(trimws(value))) return(character(0))
  out <- trimws(unlist(strsplit(value, ",", fixed = TRUE), use.names = FALSE))
  unique(out[nzchar(out)])
}

get_bool_setting <- function(arg_name, env_name, default = FALSE) {
  value <- tolower(trimws(as.character(get_setting(arg_name, env_name, default))))
  if (value %in% c("1", "true", "yes", "y")) return(TRUE)
  if (value %in% c("0", "false", "no", "n")) return(FALSE)
  stop(sprintf("Setting '%s' must be TRUE/FALSE.", arg_name), call. = FALSE)
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

prepare_modeling_data <- function(df, target, feature_cols, id_cols = character(0),
                                  require_count_target = FALSE) {
  validate_columns(df, target, feature_cols, id_cols)

  dt <- data.table::as.data.table(data.table::copy(df))
  keep_cols <- setdiff(c(target, feature_cols), id_cols)
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
    message(sprintf("Dropped %s row(s) with missing values.", rows_dropped))
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

make_stratified_custom_cv <- function(task, target, nfolds = 10, seed = 123, n_bins = 10) {
  y <- task$data(cols = target)[[1]]
  fold_ids <- make_stratified_fold_ids(y, nfolds = nfolds, seed = seed, n_bins = n_bins)
  test_sets <- lapply(seq_len(nfolds), function(k) which(fold_ids == k))
  train_sets <- lapply(test_sets, function(test) setdiff(seq_along(y), test))

  rsmp_custom <- mlr3::rsmp("custom")
  rsmp_custom$instantiate(task, train_sets = train_sets, test_sets = test_sets)
  rsmp_custom
}

reg_metrics <- function(truth, response) {
  err <- response - truth
  sse <- sum(err^2, na.rm = TRUE)
  sst <- sum((truth - mean(truth, na.rm = TRUE))^2, na.rm = TRUE)

  data.table::data.table(
    rmse = sqrt(mean(err^2, na.rm = TRUE)),
    mae = mean(abs(err), na.rm = TRUE),
    max_error = max(abs(err), na.rm = TRUE),
    sae = sum(err, na.rm = TRUE),
    mse = mean(err^2, na.rm = TRUE),
    bias = mean(err, na.rm = TRUE),
    r2 = if (isTRUE(all.equal(sst, 0))) NA_real_ else 1 - sse / sst
  )
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

fold_metrics_from_predictions <- function(predictions) {
  predictions[, reg_metrics(truth, response), by = fold][order(fold)]
}

aggregate_predictions <- function(predictions) {
  reg_metrics(predictions$truth, predictions$response)
}

add_regression_stratum <- function(backend, target, n_bins = 10, col_name = ".target_stratum") {
  backend[[col_name]] <- make_strata(backend[[target]], n_bins = n_bins)
  backend
}

make_regr_task <- function(id, backend, target, stratum_col = ".target_stratum") {
  task <- mlr3::TaskRegr$new(id = id, backend = as.data.frame(backend), target = target)
  if (stratum_col %in% names(backend)) {
    task$set_col_roles(stratum_col, roles = "stratum")
  }
  task
}

encode_factor_features <- function(dt, target) {
  feature_cols <- setdiff(names(dt), target)
  factor_cols <- names(which(vapply(dt[, ..feature_cols], is.factor, logical(1))))
  if (length(factor_cols) == 0) return(dt)

  x <- stats::model.matrix(stats::reformulate(feature_cols, intercept = FALSE), data = as.data.frame(dt))
  data.table::data.table(dt[, ..target], data.table::as.data.table(x, check.names = TRUE))
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

safe_write_csv <- function(dt, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  data.table::fwrite(dt, path)
  rds_path <- if (grepl("\\.csv$", path)) sub("\\.csv$", ".rds", path) else paste0(path, ".rds")
  saveRDS(dt, rds_path)
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
                                   filter_expression = "", keep_cols = character(0),
                                   drop_cols = character(0), drop_missing_rows = FALSE,
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
    filter_expression = if (nzchar(filter_expression)) filter_expression else NA_character_,
    keep_cols = if (length(keep_cols) > 0) paste(keep_cols, collapse = ", ") else NA_character_,
    drop_cols = if (length(drop_cols) > 0) paste(drop_cols, collapse = ", ") else NA_character_,
    drop_missing_rows = drop_missing_rows
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

log_info <- function(...) {
  cat(sprintf("[%s] ", timestamp()), ..., "\n", sep = "")
}

start_logging <- function(output_dir, script_name) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  log_path <- file.path(output_dir, sprintf("%s.log", script_name))
  log_con <- file(log_path, open = "wt")

  sink(log_con, split = TRUE)
  sink(log_con, type = "message")

  log_info("Started ", script_name)
  log_info("Log file: ", normalizePath(log_path, mustWork = FALSE))
  log_info("R version: ", R.version.string)
  log_info("Command: ", paste(commandArgs(), collapse = " "))

  list(path = log_path, connection = log_con)
}

stop_logging <- function(log_state, status = "completed") {
  if (is.null(log_state)) return(invisible(NULL))

  log_info("Finished with status: ", status)
  if (sink.number(type = "message") > 0) sink(type = "message")
  if (sink.number() > 0) sink()
  close(log_state$connection)
  invisible(NULL)
}
