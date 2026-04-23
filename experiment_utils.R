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

load_csv_checked <- function(data_path) {
  if (!file.exists(data_path)) {
    stop("Data file not found: ", normalizePath(data_path, mustWork = FALSE), call. = FALSE)
  }
  data.table::fread(data_path, header = TRUE)
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

  char_cols <- names(which(vapply(dt, is.character, logical(1))))
  if (length(char_cols) > 0) {
    dt[, (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
  }

  rows_before <- nrow(dt)
  dt <- stats::na.omit(dt)
  rows_dropped <- rows_before - nrow(dt)
  if (rows_dropped > 0) {
    message(sprintf("Dropped %s row(s) with missing values.", rows_dropped))
  }
  if (nrow(dt) == 0) stop("No rows remain after removing missing values.", call. = FALSE)

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
