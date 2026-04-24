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
                                  require_count_target = FALSE, row_filter = "") {
  validate_columns(df, target, feature_cols, id_cols)

  dt <- data.table::as.data.table(data.table::copy(df))
  filter_result <- apply_row_filter_checked(dt, row_filter, label = "Modeling row filter")
  dt <- filter_result$data
  if (filter_result$rows_removed > 0) {
    message(sprintf("Dropped %s row(s) via modeling row filter.", filter_result$rows_removed))
  }

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

poisson_deviance_score <- function(truth, mean_pred) {
  if (!is.numeric(truth) || !is.numeric(mean_pred)) return(NA_real_)
  if (length(truth) != length(mean_pred)) return(NA_real_)
  if (any(truth < 0, na.rm = TRUE)) return(NA_real_)

  mu <- pmax(mean_pred, .Machine$double.eps)
  y <- truth
  term <- ifelse(y == 0, 0, y * log(y / mu))
  mean(2 * (term - (y - mu)), na.rm = TRUE)
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
    poisson_deviance = poisson_deviance,
    negloglik = mean_negloglik
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
  predictions[, reg_metrics(
    truth,
    response,
    negloglik = if ("negloglik" %in% names(predictions)) negloglik else NULL
  ), by = fold][order(fold)]
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
                               feature_cols = character(0), n_workers = NA) {
  started_at <- if (!is.null(log_state$started_at)) log_state$started_at else Sys.time()
  ended_at <- Sys.time()
  runtime_seconds <- as.numeric(difftime(ended_at, started_at, units = "secs"))

  manifest <- data.table::data.table(
    script_name = script_name,
    status = status,
    seed = if (length(seed) == 0) NA_real_ else as.numeric(seed)[1],
    data_path = if (is.na(data_path)[1]) NA_character_ else normalizePath(data_path, mustWork = FALSE),
    feature_list = if (length(feature_cols) > 0) paste(feature_cols, collapse = ", ") else NA_character_,
    git_commit = safe_git_commit(repo_dir),
    start_time = format(started_at, "%Y-%m-%d %H:%M:%S %Z"),
    end_time = format(ended_at, "%Y-%m-%d %H:%M:%S %Z"),
    runtime_seconds = runtime_seconds,
    workers = if (length(n_workers) == 0) NA_real_ else as.numeric(n_workers)[1],
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
    "outer_fold", "regr.rmse", "warnings", "errors", "batch_nr", "runtime_learners",
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
                         feature_cols = character(0), n_workers = NA) {
  manifest_error <- NULL

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
      n_workers = n_workers
    )),
    error = function(e) {
      manifest_error <<- conditionMessage(e)
      NULL
    }
  )

  if (!is.null(manifest_error)) {
    log_info("Warning: failed to write run_manifest: ", manifest_error)
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
    eval(expr, envir = eval_env)
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
  started_at <- Sys.time()

  sink(log_con, split = TRUE)
  sink(log_con, type = "message")

  log_info("Started ", script_name)
  log_info("Log file: ", normalizePath(log_path, mustWork = FALSE))
  log_info("R version: ", R.version.string)
  log_info("Command: ", paste(commandArgs(), collapse = " "))

  list(path = log_path, connection = log_con, started_at = started_at, script_name = script_name)
}

stop_logging <- function(log_state, status = "completed") {
  if (is.null(log_state)) return(invisible(NULL))
  if (isTRUE(log_state$closed)) return(invisible(NULL))

  log_info("Finished with status: ", status)
  if (sink.number(type = "message") > 0) sink(type = "message")
  if (sink.number() > 0) sink()
  if (!is.null(log_state$connection) && isOpen(log_state$connection)) {
    close(log_state$connection)
  }
  log_state$closed <- TRUE
  invisible(NULL)
}
