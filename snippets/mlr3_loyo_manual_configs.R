#!/usr/bin/env Rscript

# Explorative LOYO validation fuer manuell gesetzte Modellkonfigurationen.
# In RStudio Pro: Projekt-Working-Directory auf Repo-Root setzen, oben configs
# anpassen, dann das ganze Skript sourcen.

suppressPackageStartupMessages({
  library(data.table)
  library(mlr3)
  library(mlr3learners)
})

repo_dir <- normalizePath(getwd(), mustWork = TRUE)
source(file.path(repo_dir, "experiment_utils.R"))
reg_metrics <- get("reg_metrics", mode = "function")
make_year_blocked_fold_ids <- get("make_year_blocked_fold_ids", mode = "function")
encode_features_train_test <- get("encode_features_train_test", mode = "function")
safe_write_csv <- get("safe_write_csv", mode = "function")
write_config_snapshot <- get("write_config_snapshot", mode = "function")
write_session_info <- get("write_session_info", mode = "function")
write_text_file <- get("write_text_file", mode = "function")

# ---- anpassen ----------------------------------------------------------------
# Optional in RStudio vorher Daten in `dt` oder `df` laden, zum Beispiel
# aus einer CSV oder RDS-Datei. Das Snippet nutzt zuerst ein vorhandenes `dt`,
# dann `df`, sonst `data_path`.

target <- "n_eintritte"
year_col <- Sys.getenv("LOYO_YEAR_COL", "year")
data_path <- Sys.getenv("LOYO_DATA_PATH", file.path(repo_dir, "testfile_zinb_nonlinear_eintritte.csv"))
row_filter <- "" # z.B. "segment == 'A' & year >= 2021"; leer lassen fuer alles
output_dir <- Sys.getenv("LOYO_OUTPUT_DIR", file.path(repo_dir, "outputs_loyo_exploration"))
seed <- 123L
learner_threads <- 1L
parallel_folds <- TRUE
# "auto" probiert auf Ubuntu/Linux zuerst "multicore", dann "multisession".
# "multisession" ist RStudio-freundlich, braucht aber lokale Socket-Worker.
# Wenn beides hakt: "sequential" setzen.
parallel_backend <- "auto"
available_cores <- parallel::detectCores(logical = FALSE)
if (is.na(available_cores)) available_cores <- 2L
fold_workers <- max(1L, min(4L, available_cores - 1L))
run_models <- c("ranger", "xgb", "zinb")

configs <- list(
  small_fast = list(
    features = c("prcrank", "potenzielle_kunden", "unfalldeckung"),
    ranger = list(
      features = c("prcrank", "potenzielle_kunden", "unfalldeckung"),
      params = list(
        num.trees = 400L,
        mtry = 2L,
        min.node.size = 5L,
        sample.fraction = 0.8,
        # nolint start: commented_code_linter
        # max.depth = 12L,
        # replace = TRUE,
        # importance = "permutation",
        # respect.unordered.factors = "partition",
        # num.random.splits = 5L,
        # nolint end
        splitrule = "variance"
      )
    ),
    xgb = list(
      features = c("prcrank", "potenzielle_kunden", "unfalldeckung"),
      params = list(
        objective = "reg:squarederror",
        nrounds = 200L,
        eta = 0.05,
        max_depth = 3L,
        min_child_weight = 3,
        subsample = 0.8,
        # nolint start: commented_code_linter
        # gamma = 1,
        # lambda = 2,
        # alpha = 0.5,
        # colsample_bylevel = 0.8,
        # tree_method = "hist",
        # nolint end
        colsample_bytree = 0.8
      )
    ),
    zinb = list(
      formula = n_eintritte ~ prcrank + log1p(potenzielle_kunden) + unfalldeckung | 1,
      zeroinfl_args = list(dist = "negbin", EM = TRUE),
      control_args = list(maxit = 100L)
    )
  )
)

# ---- setup -------------------------------------------------------------------
needed <- c(
  "data.table",
  "mlr3",
  "mlr3learners",
  if (parallel_folds) "future" else character(0),
  if (parallel_folds) "future.apply" else character(0),
  if ("ranger" %in% run_models) "ranger" else character(0),
  if ("xgb" %in% run_models) "xgboost" else character(0)
)
missing <- needed[!vapply(needed, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0L) stop("Missing package(s): ", paste(missing, collapse = ", "), call. = FALSE)
if ("zinb" %in% run_models && !requireNamespace("pscl", quietly = TRUE)) {
  stop("Package `pscl` is needed when run_models contains 'zinb'.", call. = FALSE)
}
if (parallel_folds) {
  if (!parallel_backend %in% c("auto", "multisession", "multicore", "sequential")) {
    stop("parallel_backend must be one of: auto, multisession, multicore, sequential.", call. = FALSE)
  }
  if (identical(parallel_backend, "sequential") || fold_workers <= 1L) {
    parallel_folds <- FALSE
  }
}
active_parallel_backend <- "sequential"
active_fold_workers <- 1L
if (parallel_folds) {
  backend_candidates <- if (identical(parallel_backend, "auto")) {
    if (.Platform$OS.type == "unix") c("multicore", "multisession") else "multisession"
  } else {
    parallel_backend
  }
  future_plan_ok <- FALSE
  future_plan_errors <- character(0)
  for (backend in backend_candidates) {
    future_plan_ok <- tryCatch(
      {
        if (identical(backend, "multicore")) {
          if (!future::supportsMulticore()) {
            stop("future::supportsMulticore() is FALSE in this R session.", call. = FALSE)
          }
          future::plan(future::multicore, workers = fold_workers)
        } else {
          future::plan(future::multisession, workers = fold_workers)
        }
        active_parallel_backend <- backend
        active_fold_workers <- future::nbrOfWorkers()
        TRUE
      },
      error = function(e) {
        future_plan_errors <<- c(future_plan_errors, paste0(backend, ": ", conditionMessage(e)))
        FALSE
      }
    )
    if (future_plan_ok) break
  }
  if (future_plan_ok && active_fold_workers > 1L) {
    on.exit(future::plan(future::sequential), add = TRUE)
  } else {
    if (future_plan_ok) {
      future_plan_errors <- c(future_plan_errors, paste0(active_parallel_backend, ": active workers <= 1"))
    }
    warning(
      "Could not start parallel future workers; falling back to sequential folds. Tried: ",
      paste(future_plan_errors, collapse = " | ")
    )
    parallel_folds <- FALSE
    active_parallel_backend <- "sequential"
    active_fold_workers <- 1L
  }
}

raw_dt <- if (exists("dt", envir = .GlobalEnv, inherits = FALSE)) {
  as.data.table(copy(get("dt", envir = .GlobalEnv)))
} else if (exists("df", envir = .GlobalEnv, inherits = FALSE)) {
  as.data.table(copy(get("df", envir = .GlobalEnv)))
} else {
  if (!file.exists(data_path)) stop("Data file not found: ", data_path, call. = FALSE)
  fread(data_path)
}
raw_dt[, ".row_id" := .I]

if (nzchar(trimws(row_filter))) {
  keep <- raw_dt[, eval(parse(text = row_filter))]
  if (!is.logical(keep) || length(keep) != nrow(raw_dt)) {
    stop("row_filter must evaluate to a logical vector with one value per row.", call. = FALSE)
  }
  raw_dt <- raw_dt[which(!is.na(keep) & keep)]
}

`%||%` <- function(x, y) if (is.null(x)) y else x
elapsed <- function(started_at) paste0(round(as.numeric(difftime(Sys.time(), started_at, units = "mins")), 2), " min")
msg <- function(...) {
  message(format(Sys.time(), "%H:%M:%S"), " | ", ...)
  flush.console()
}

check_cols <- function(dt, cols, label) {
  missing <- setdiff(cols, names(dt))
  if (length(missing) > 0L) {
    stop(label, " missing column(s): ", paste(missing, collapse = ", "), call. = FALSE)
  }
}

configured_features <- function(x) {
  x$features %||% x$feature_cols
}

model_features <- function(conf, model) {
  model_conf <- conf[[model]] %||% list()
  if (identical(model, "zinb") && !is.null(model_conf$formula) && is.null(configured_features(model_conf))) {
    return(zinb_formula_features(conf))
  }
  features <- configured_features(model_conf) %||% configured_features(conf)
  features <- unique(as.character(features))
  if (length(features) == 0L || anyNA(features) || any(!nzchar(features))) {
    stop(
      "Config model '", model,
      "' needs at least one non-empty feature name via `features` or `feature_cols`.",
      call. = FALSE
    )
  }
  features
}

zinb_formula <- function(conf) {
  zconf <- conf$zinb %||% list()
  if (!is.null(zconf$formula)) {
    return(if (inherits(zconf$formula, "formula")) zconf$formula else as.formula(zconf$formula))
  }

  features <- model_features(conf, "zinb")
  count_rhs <- zconf$count_rhs %||% paste(features, collapse = " + ")
  zero_rhs <- zconf$zero_rhs %||% "1"
  as.formula(sprintf("%s ~ %s | %s", target, count_rhs, zero_rhs))
}

zinb_formula_features <- function(conf) {
  setdiff(all.vars(zinb_formula(conf)), target)
}

config_features <- function(conf) {
  unique(c(
    unlist(lapply(c("ranger", "xgb"), function(model) model_features(conf, model)), use.names = FALSE),
    model_features(conf, "zinb")
  ))
}

model_params <- function(conf, model) {
  model_conf <- conf[[model]] %||% list()
  reserved <- c(
    "features", "feature_cols", "params", "formula", "count_rhs", "zero_rhs",
    "zeroinfl_args", "control_args", "EM", "maxit", "dist"
  )
  flat_params <- model_conf[setdiff(names(model_conf), reserved)]
  nested_params <- model_conf$params %||% list()
  utils::modifyList(flat_params, nested_params)
}

as_config_dt <- function(conf, conf_name) {
  features <- config_features(conf)
  cols <- unique(c(".row_id", target, year_col, features))
  check_cols(raw_dt, cols, paste0("Config '", conf_name, "'"))

  out <- copy(raw_dt[, cols, with = FALSE])
  before <- nrow(out)
  out <- na.omit(out)
  dropped <- before - nrow(out)
  if (dropped > 0L) msg(conf_name, ": dropped ", dropped, " row(s) with missing model values")

  char_cols <- names(which(vapply(out[, features, with = FALSE], is.character, logical(1))))
  if (length(char_cols) > 0L) out[, (char_cols) := lapply(.SD, as.factor), .SDcols = char_cols]
  out
}

apply_params <- function(learner, params, label) {
  params <- params %||% list()
  bad <- setdiff(names(params), learner$param_set$ids())
  if (length(bad) > 0L) stop(label, " has unsupported parameter(s): ", paste(bad, collapse = ", "), call. = FALSE)
  learner$param_set$values <- utils::modifyList(learner$param_set$values, params)
  learner
}

prediction_rows <- function(work_dt, test_ids, fold, model, conf_name, response,
                            status = "ok", note = NA_character_,
                            fold_started_at = NA_real_, fold_finished_at = NA_real_) {
  out <- data.table(
    config = conf_name,
    model = model,
    row_id = work_dt$.row_id[test_ids],
    fold = fold,
    year = as.character(work_dt[[year_col]][test_ids]),
    truth = work_dt[[target]][test_ids],
    response = as.numeric(response),
    status = status,
    note = note,
    worker_pid = Sys.getpid(),
    worker_host = Sys.info()[["nodename"]],
    fold_started_at = as.numeric(fold_started_at),
    fold_finished_at = as.numeric(fold_finished_at),
    fold_elapsed_seconds = as.numeric(fold_finished_at) - as.numeric(fold_started_at)
  )
  out$error <- out$response - out$truth
  out$abs_error <- abs(out$error)
  out
}

score_block <- function(truth, response) {
  ok <- is.finite(truth) & is.finite(response)
  base <- data.table(n = length(truth), n_scored = sum(ok), n_failed = sum(!ok))
  volume_empty <- data.table(
    sum_truth = NA_real_,
    sum_pred = NA_real_,
    volume_error = NA_real_,
    volume_abs_error = NA_real_,
    volume_ratio = NA_real_,
    volume_pct_error = NA_real_
  )
  if (!any(ok)) {
    return(cbind(base, volume_empty, data.table(
      rmse = NA_real_, mae = NA_real_, max_error = NA_real_, sae = NA_real_,
      mse = NA_real_, bias = NA_real_, r2 = NA_real_, wape = NA_real_,
      poisson_deviance = NA_real_, negloglik = NA_real_
    )))
  }

  sum_truth <- sum(truth[ok], na.rm = TRUE)
  sum_pred <- sum(response[ok], na.rm = TRUE)
  volume_error <- sum_pred - sum_truth
  has_observed_volume <- is.finite(sum_truth) && abs(sum_truth) > .Machine$double.eps
  volume <- data.table(
    sum_truth = sum_truth,
    sum_pred = sum_pred,
    volume_error = volume_error,
    volume_abs_error = abs(volume_error),
    volume_ratio = if (has_observed_volume) sum_pred / sum_truth else NA_real_,
    volume_pct_error = if (has_observed_volume) volume_error / sum_truth else NA_real_
  )
  cbind(base, volume, reg_metrics(truth[ok], response[ok]))
}

score_predictions <- function(pred, by_cols) {
  out <- pred[, score_block(.SD[["truth"]], .SD[["response"]]), by = by_cols]
  setorderv(out, c(intersect(c("config", "model", "fold"), names(out)), "rmse"))
  out
}

make_loyo <- function(work_dt) {
  blocked <- make_year_blocked_fold_ids(work_dt[[year_col]], block_col = year_col)
  list(fold_ids = blocked$fold_ids, fold_plan = blocked$fold_plan)
}

run_fold_indices <- function(fold_count, fold_fun) {
  folds <- seq_len(fold_count)
  if (parallel_folds && active_fold_workers > 1L && fold_count > 1L) {
    if (identical(active_parallel_backend, "multicore")) {
      return(parallel::mclapply(folds, fold_fun, mc.cores = min(active_fold_workers, fold_count), mc.set.seed = TRUE))
    }
    return(future.apply::future_lapply(folds, fold_fun, future.seed = TRUE, future.scheduling = Inf))
  }
  lapply(folds, fold_fun)
}

run_mlr3_fixed <- function(work_dt, fold_ids, conf, conf_name, model) {
  features <- model_features(conf, model)
  params <- model_params(conf, model)
  fold_count <- max(fold_ids)

  rows <- run_fold_indices(fold_count, function(fold) {
    fold_started_at <- Sys.time()
    train_ids <- which(fold_ids != fold)
    test_ids <- which(fold_ids == fold)

    if (identical(model, "xgb")) {
      encoded <- encode_features_train_test(
        work_dt[train_ids, c(target, features), with = FALSE],
        work_dt[test_ids, c(target, features), with = FALSE],
        target = target
      )
      train_task <- mlr3::TaskRegr$new(
        sprintf("%s_%s_train_f%s", conf_name, model, fold),
        as.data.frame(encoded$train),
        target
      )
      test_task <- mlr3::TaskRegr$new(
        sprintf("%s_%s_test_f%s", conf_name, model, fold),
        as.data.frame(encoded$test),
        target
      )
      learner <- mlr3::lrn("regr.xgboost", predict_type = "response", nthread = learner_threads)
    } else {
      train_task <- mlr3::TaskRegr$new(
        sprintf("%s_%s_train_f%s", conf_name, model, fold),
        as.data.frame(work_dt[train_ids, c(target, features), with = FALSE]),
        target
      )
      test_task <- mlr3::TaskRegr$new(
        sprintf("%s_%s_test_f%s", conf_name, model, fold),
        as.data.frame(work_dt[test_ids, c(target, features), with = FALSE]),
        target
      )
      learner <- mlr3::lrn("regr.ranger", predict_type = "response", num.threads = learner_threads)
    }

    learner <- apply_params(learner, params, paste(conf_name, model))
    set.seed(seed + fold)
    learner$train(train_task)
    pred <- learner$predict(test_task)
    prediction_rows(
      work_dt, test_ids, fold, model, conf_name, pred$response,
      fold_started_at = fold_started_at,
      fold_finished_at = Sys.time()
    )
  })
  rbindlist(rows)
}

run_zinb_fixed <- function(work_dt, fold_ids, conf, conf_name) {
  zconf <- conf$zinb %||% list()
  form <- zinb_formula(conf)
  zeroinfl_args <- utils::modifyList(
    list(dist = zconf$dist %||% "negbin", EM = isTRUE(zconf$EM %||% TRUE)),
    zconf$zeroinfl_args %||% list()
  )
  control_args <- utils::modifyList(
    list(maxit = as.integer(zconf$maxit %||% 100L)),
    zconf$control_args %||% list()
  )
  reserved_fit_args <- intersect(names(zeroinfl_args), c("formula", "data", "control"))
  if (length(reserved_fit_args) > 0L) {
    stop("zinb$zeroinfl_args must not set: ", paste(reserved_fit_args, collapse = ", "), call. = FALSE)
  }
  fold_count <- max(fold_ids)

  rows <- run_fold_indices(fold_count, function(fold) {
    fold_started_at <- Sys.time()
    train_ids <- which(fold_ids != fold)
    test_ids <- which(fold_ids == fold)
    warnings_seen <- character()

    fit <- tryCatch(
      withCallingHandlers(
        do.call(
          pscl::zeroinfl,
          c(
            list(formula = form, data = work_dt[train_ids]),
            zeroinfl_args,
            list(control = do.call(pscl::zeroinfl.control, control_args))
          )
        ),
        warning = function(w) {
          warnings_seen <<- c(warnings_seen, conditionMessage(w))
          invokeRestart("muffleWarning")
        }
      ),
      error = function(e) structure(list(message = conditionMessage(e)), class = "zinb_error")
    )

    if (inherits(fit, "zinb_error")) {
      return(prediction_rows(
        work_dt, test_ids, fold, "zinb", conf_name, NA_real_, "fit_failed", fit$message,
        fold_started_at = fold_started_at,
        fold_finished_at = Sys.time()
      ))
    }

    pred <- tryCatch(
      as.numeric(predict(fit, newdata = work_dt[test_ids], type = "response")),
      error = function(e) structure(list(message = conditionMessage(e)), class = "zinb_error")
    )
    if (inherits(pred, "zinb_error") || length(pred) != length(test_ids) || any(!is.finite(pred))) {
      note <- if (inherits(pred, "zinb_error")) pred$message else "non-finite predictions"
      return(prediction_rows(
        work_dt, test_ids, fold, "zinb", conf_name, NA_real_, "predict_failed", note,
        fold_started_at = fold_started_at,
        fold_finished_at = Sys.time()
      ))
    }
    prediction_rows(
      work_dt, test_ids, fold, "zinb", conf_name, pred, "ok",
      if (length(warnings_seen) > 0L) paste(unique(warnings_seen), collapse = " | ") else NA_character_,
      fold_started_at = fold_started_at,
      fold_finished_at = Sys.time()
    )
  })
  rbindlist(rows)
}

# ---- run ---------------------------------------------------------------------
started_at <- Sys.time()
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
all_predictions <- list()
all_fold_plans <- list()

for (conf_name in names(configs)) {
  conf <- configs[[conf_name]]
  work_dt <- as_config_dt(conf, conf_name)
  check_cols(work_dt, c(target, year_col, config_features(conf)), paste0("Config '", conf_name, "'"))
  loyo <- make_loyo(work_dt)
  all_fold_plans[[conf_name]] <- copy(loyo$fold_plan)[, config := conf_name]

  msg(
    conf_name, ": LOYO folds=", max(loyo$fold_ids), ", rows=", nrow(work_dt),
    ", backend=", active_parallel_backend,
    ", requested_workers=", fold_workers,
    ", active_workers=", active_fold_workers,
    ", learner_threads=", learner_threads
  )
  for (model in intersect(run_models, c("ranger", "xgb", "zinb"))) {
    model_started <- Sys.time()
    msg("  ", conf_name, "/", model, ": start")
    all_predictions[[paste(conf_name, model, sep = "::")]] <- if (identical(model, "zinb")) {
      run_zinb_fixed(work_dt, loyo$fold_ids, conf, conf_name)
    } else {
      run_mlr3_fixed(work_dt, loyo$fold_ids, conf, conf_name, model)
    }
    msg("  ", conf_name, "/", model, ": done in ", elapsed(model_started))
  }
}

predictions <- rbindlist(all_predictions, fill = TRUE)
fold_plan <- rbindlist(all_fold_plans, fill = TRUE)
fold_metrics <- score_predictions(predictions, c("config", "model", "fold", "year"))
overall_metrics <- score_predictions(predictions, c("config", "model"))
parallel_diagnostics <- predictions[, .(
  n_folds = uniqueN(fold),
  n_worker_pids = uniqueN(worker_pid),
  worker_pids = paste(sort(unique(worker_pid)), collapse = ", "),
  worker_hosts = paste(sort(unique(worker_host)), collapse = ", "),
  first_fold_started_at = min(fold_started_at, na.rm = TRUE),
  last_fold_started_at = max(fold_started_at, na.rm = TRUE),
  first_fold_finished_at = min(fold_finished_at, na.rm = TRUE),
  last_fold_finished_at = max(fold_finished_at, na.rm = TRUE),
  max_fold_elapsed_seconds = max(fold_elapsed_seconds, na.rm = TRUE)
), by = .(config, model)]
parallel_diagnostics[, folds_overlap := last_fold_started_at < first_fold_finished_at]
comparison <- copy(overall_metrics)[order(rmse, mae, wape)]
comparison[, rank_rmse := seq_len(.N)]
setcolorder(comparison, c("rank_rmse", setdiff(names(comparison), "rank_rmse")))

safe_write_csv(predictions, file.path(output_dir, "loyo_predictions.csv"))
safe_write_csv(fold_metrics, file.path(output_dir, "loyo_fold_metrics.csv"))
safe_write_csv(overall_metrics, file.path(output_dir, "loyo_overall_metrics.csv"))
safe_write_csv(comparison, file.path(output_dir, "loyo_comparison.csv"))
safe_write_csv(parallel_diagnostics, file.path(output_dir, "loyo_parallel_diagnostics.csv"))
safe_write_csv(fold_plan, file.path(output_dir, "loyo_fold_plan.csv"))
write_config_snapshot(output_dir, list(target = target, year_col = year_col, configs = configs), prefix = "loyo_config")
write_session_info(output_dir)

if (requireNamespace("ggplot2", quietly = TRUE)) {
  ggplot <- getExportedValue("ggplot2", "ggplot")
  aes <- getExportedValue("ggplot2", "aes")
  coord_flip <- getExportedValue("ggplot2", "coord_flip")
  facet_wrap <- getExportedValue("ggplot2", "facet_wrap")
  geom_abline <- getExportedValue("ggplot2", "geom_abline")
  geom_col <- getExportedValue("ggplot2", "geom_col")
  geom_hline <- getExportedValue("ggplot2", "geom_hline")
  geom_line <- getExportedValue("ggplot2", "geom_line")
  geom_point <- getExportedValue("ggplot2", "geom_point")
  ggsave <- getExportedValue("ggplot2", "ggsave")
  labs <- getExportedValue("ggplot2", "labs")
  theme <- getExportedValue("ggplot2", "theme")
  theme_minimal <- getExportedValue("ggplot2", "theme_minimal")
  theme_set <- getExportedValue("ggplot2", "theme_set")
  theme_set(theme_minimal(base_size = 12))

  p_rmse <- ggplot(comparison, aes(x = reorder(paste(config, model, sep = " / "), rmse), y = rmse, fill = model)) +
    geom_col(width = 0.72) +
    coord_flip() +
    labs(x = NULL, y = "LOYO RMSE", title = "LOYO model comparison") +
    theme(legend.position = "bottom")

  p_year <- ggplot(fold_metrics, aes(x = year, y = rmse, color = model, group = interaction(config, model))) +
    geom_line(linewidth = 0.7) +
    geom_point(size = 2) +
    facet_wrap(~ config, scales = "free_y") +
    labs(x = year_col, y = "RMSE", title = "LOYO RMSE by held-out year") +
    theme(legend.position = "bottom")

  p_volume <- ggplot(fold_metrics, aes(x = year, y = volume_ratio, color = model, group = interaction(config, model))) +
    geom_hline(yintercept = 1, linewidth = 0.5, linetype = 2) +
    geom_line(linewidth = 0.7) +
    geom_point(size = 2) +
    facet_wrap(~ config, scales = "free_y") +
    labs(x = year_col, y = "Predicted / observed volume", title = "LOYO volume calibration by held-out year") +
    theme(legend.position = "bottom")

  plot_pred <- predictions[is.finite(response)]
  if (nrow(plot_pred) > 5000L) plot_pred <- plot_pred[sample(.N, 5000L)]
  p_pred <- ggplot(plot_pred, aes(x = truth, y = response, color = model)) +
    geom_abline(slope = 1, intercept = 0, linewidth = 0.5, linetype = 2) +
    geom_point(alpha = 0.45, size = 1.4) +
    facet_wrap(~ config, scales = "free") +
    labs(x = "Observed", y = "Predicted", title = "LOYO predictions") +
    theme(legend.position = "bottom")

  ggsave(file.path(output_dir, "loyo_comparison_rmse.png"), p_rmse, width = 9, height = 5, dpi = 150)
  ggsave(file.path(output_dir, "loyo_rmse_by_year.png"), p_year, width = 9, height = 5, dpi = 150)
  ggsave(file.path(output_dir, "loyo_volume_ratio_by_year.png"), p_volume, width = 9, height = 5, dpi = 150)
  ggsave(file.path(output_dir, "loyo_predicted_vs_observed.png"), p_pred, width = 9, height = 6, dpi = 150)
}

report_lines <- c(
  "# LOYO Exploratory Validation",
  "",
  paste0("- target: `", target, "`"),
  paste0("- year_col: `", year_col, "`"),
  paste0("- rows scored: `", sum(comparison$n_scored), "`"),
  paste0("- runtime: `", elapsed(started_at), "`"),
  "",
  "## Comparison",
  "",
  paste(capture.output(print(comparison)), collapse = "\n"),
  "",
  "## Output Files",
  "",
  "- `loyo_comparison.csv` / `.rds`",
  "- `loyo_overall_metrics.csv` / `.rds`",
  "- `loyo_fold_metrics.csv` / `.rds`",
  "- `loyo_predictions.csv` / `.rds`",
  "- `loyo_parallel_diagnostics.csv` / `.rds`",
  "- `loyo_fold_plan.csv` / `.rds`",
  paste0(
    "- `loyo_comparison_rmse.png`, `loyo_rmse_by_year.png`, ",
    "`loyo_volume_ratio_by_year.png`, `loyo_predicted_vs_observed.png` when ggplot2 is installed"
  )
)
write_text_file(file.path(output_dir, "loyo_validation_report.md"), report_lines)

loyo_results <- list(
  comparison = comparison,
  overall_metrics = overall_metrics,
  fold_metrics = fold_metrics,
  predictions = predictions,
  parallel_diagnostics = parallel_diagnostics,
  fold_plan = fold_plan,
  output_dir = output_dir
)

print(comparison)
print(parallel_diagnostics)
msg("LOYO validation written to: ", normalizePath(output_dir, mustWork = FALSE))
