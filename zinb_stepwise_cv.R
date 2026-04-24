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
SCRIPT_NAME <- "zinb_stepwise_cv"
SCRIPT_PACKAGES <- c("data.table", "pscl", "splines", "parallel")

require_packages(c("data.table", "pscl"))

suppressPackageStartupMessages({
  library(data.table)
  library(pscl)
  library(splines)
})

# =========================
# User settings
# =========================
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

DATA_PATH <- get_path_setting(
  "data", "MLR3_DATA_PATH",
  config_value(CONFIG, c("experiment", "data_path")),
  base_dir = REPO_DIR
)
OUTPUT_DIR <- get_path_setting("output-dir", "ZINB_OUTPUT_DIR", config_value(CONFIG, c("zinb", "output_dir")), base_dir = REPO_DIR)
N_FOLDS <- get_int_setting("folds", "N_FOLDS", config_value(CONFIG, c("experiment", "n_folds")), min_value = 2)
INNER_FOLDS <- get_int_setting("inner-folds", "INNER_FOLDS", config_value(CONFIG, c("experiment", "inner_folds")), min_value = 2)
SEED <- get_int_setting("seed", "SEED", config_value(CONFIG, c("experiment", "seed")))
STRATA_BINS <- get_int_setting("strata-bins", "STRATA_BINS", config_value(CONFIG, c("experiment", "strata_bins")), min_value = 2)
MAX_VARS <- get_numeric_setting("max-vars", "ZINB_MAX_VARS", config_value(CONFIG, c("zinb", "max_vars")), min_value = 1)
MIN_IMPROVEMENT <- get_numeric_setting("min-improvement", "ZINB_MIN_IMPROVEMENT", config_value(CONFIG, c("zinb", "min_improvement")), min_value = 0)
METRIC_TO_OPTIMIZE <- get_setting("metric", "METRIC_TO_OPTIMIZE", config_value(CONFIG, c("zinb", "metric")))
N_WORKERS <- get_int_setting(
  "workers", "ZINB_WORKERS",
  get_setting("workers", "N_WORKERS", config_value(CONFIG, c("zinb", "workers"))),
  min_value = 1
)
TRANSFORMATIONS_NUMERIC <- config_value(CONFIG, c("zinb", "transformations_numeric"))
TRANSFORMATIONS_FACTOR <- config_value(CONFIG, c("zinb", "transformations_factor"))
NUMERIC_AS_FACTOR_MAX_LEVELS <- get_int_setting(
  "numeric-as-factor-max-levels", "ZINB_NUMERIC_AS_FACTOR_MAX_LEVELS",
  config_value(CONFIG, c("zinb", "numeric_as_factor_max_levels")),
  min_value = 2
)
NUMERIC_AS_FACTOR_VARS <- parse_csv_setting(get_setting(
  "numeric-as-factor-vars", "ZINB_NUMERIC_AS_FACTOR_VARS",
  paste(config_value(CONFIG, c("zinb", "numeric_as_factor_vars")), collapse = ",")
))
ZERO_INFLATION_FORMULA <- trimws(get_setting(
  "zero-formula", "ZINB_ZERO_FORMULA",
  config_value(CONFIG, c("zinb", "zero_inflation_formula"))
))
if (!nzchar(ZERO_INFLATION_FORMULA)) {
  stop("ZINB zero-formula must not be empty.", call. = FALSE)
}

ALLOWED_METRICS <- c("rmse", "mae", "max_error", "mse", "r2", "poisson_deviance", "negloglik")
if (!METRIC_TO_OPTIMIZE %in% ALLOWED_METRICS) {
  stop("METRIC_TO_OPTIMIZE must be one of: ", paste(ALLOWED_METRICS, collapse = ", "), call. = FALSE)
}

# =========================
# ZINB helpers
# =========================
is_nonnegative <- function(x) {
  is.numeric(x) && all(x[!is.na(x)] >= 0)
}

valid_transformations <- function(x, var_name = NULL) {
  if (is.numeric(x)) {
    allowed <- c("raw", "ns2", "poly2")
    if (is_nonnegative(x)) allowed <- c(allowed, "sqrt", "log1p")
    n_unique <- data.table::uniqueN(x[!is.na(x)])
    force_factor <- !is.null(var_name) && nzchar(var_name) && var_name %in% NUMERIC_AS_FACTOR_VARS
    if (force_factor || (n_unique >= 2L && n_unique <= NUMERIC_AS_FACTOR_MAX_LEVELS)) {
      allowed <- c(allowed, "factor")
    }
    return(intersect(TRANSFORMATIONS_NUMERIC, allowed))
  }
  intersect(TRANSFORMATIONS_FACTOR, "raw")
}

quote_name <- function(var) {
  if (make.names(var) == var) return(var)
  sprintf("`%s`", gsub("`", "\\\\`", var))
}

term_for <- function(var, transformation) {
  qvar <- quote_name(var)
  switch(
    transformation,
    raw = qvar,
    factor = sprintf("factor(%s)", qvar),
    sqrt = sprintf("sqrt(%s)", qvar),
    log1p = sprintf("log1p(%s)", qvar),
    ns2 = sprintf("ns(%s, df = 2)", qvar),
    poly2 = sprintf("poly(%s, 2)", qvar),
    stop(sprintf("Unknown transformation: %s", transformation), call. = FALSE)
  )
}

make_formula <- function(target, terms, zero_formula_rhs = "1") {
  rhs_count <- if (length(terms) == 0) "1" else paste(terms, collapse = " + ")
  rhs_zero <- if (identical(zero_formula_rhs, "same_as_count")) rhs_count else zero_formula_rhs
  as.formula(sprintf("%s ~ %s | %s", quote_name(target), rhs_count, rhs_zero))
}

formula_label <- function(formula_obj) {
  gsub("\\s+", " ", paste(deparse(formula_obj), collapse = " "))
}

validate_formula_rhs_on_data <- function(rhs, dt, label) {
  rhs <- trimws(rhs)
  if (!nzchar(rhs)) {
    stop(label, " must not be empty.", call. = FALSE)
  }

  rhs_formula <- as.formula(sprintf("~ %s", rhs))
  referenced_vars <- formula_referenced_columns(rhs, label = label)
  missing_vars <- setdiff(referenced_vars, names(dt))
  if (length(missing_vars) > 0) {
    stop(label, " references columns that are not present in the data: ",
         paste(missing_vars, collapse = ", "), call. = FALSE)
  }

  tryCatch(
    stats::model.frame(rhs_formula, data = as.data.frame(dt), na.action = stats::na.pass),
    error = function(e) {
      stop(label, " cannot be evaluated on the modeling data: ", conditionMessage(e), call. = FALSE)
    }
  )

  invisible(TRUE)
}

validate_zinb_setup <- function(dt, target, feature_cols, zero_formula_rhs) {
  if (!identical(zero_formula_rhs, "same_as_count")) {
    validate_formula_rhs_on_data(zero_formula_rhs, dt, "ZINB zero-formula")
  }

  for (feature in feature_cols) {
    tfms <- valid_transformations(dt[[feature]], var_name = feature)
    if (length(tfms) == 0) {
      stop("Feature '", feature, "' has no valid ZINB transformation for its type or values.", call. = FALSE)
    }

    for (tfm in tfms) {
      validate_formula_rhs_on_data(term_for(feature, tfm), dt, sprintf("ZINB term for '%s' [%s]", feature, tfm))
    }
  }

  baseline_formula <- make_formula(target, character(0), zero_formula_rhs = zero_formula_rhs)
  validate_formula_rhs_on_data(as.character(baseline_formula)[3], dt, "ZINB baseline count formula")
  invisible(TRUE)
}

fit_convergence_reason <- function(fit, warnings = character(0)) {
  optim_convergence <- fit$optim$convergence
  converged <- isTRUE(fit$converged)
  optim_ok <- !is.null(optim_convergence) && identical(as.integer(optim_convergence), 0L)
  nonconv_warning <- any(grepl("converg", warnings, ignore.case = TRUE))

  if (converged && optim_ok && !nonconv_warning) return(NA_character_)

  warning_text <- if (length(warnings) > 0) paste(unique(warnings), collapse = " | ") else "none"
  sprintf(
    "fit did not converge: fit$converged=%s, optim$convergence=%s, warnings=%s",
    converged,
    if (is.null(optim_convergence)) "NULL" else as.character(optim_convergence),
    warning_text
  )
}

zinb_fit_attempts <- function() {
  list(
    list(label = "em100", EM = TRUE, maxit = 100L),
    list(label = "noem200", EM = FALSE, maxit = 200L),
    list(label = "em300", EM = TRUE, maxit = 300L)
  )
}

fit_zinb_with_retries <- function(dt, formula_obj) {
  failure_reasons <- character(0)

  for (attempt in zinb_fit_attempts()) {
    fit_warnings <- character(0)
    fit <- tryCatch(
      withCallingHandlers(
        zeroinfl(
          formula_obj,
          data = dt,
          dist = "negbin",
          EM = attempt$EM,
          control = zeroinfl.control(maxit = attempt$maxit)
        ),
        warning = function(w) {
          fit_warnings <<- c(fit_warnings, conditionMessage(w))
          invokeRestart("muffleWarning")
        }
      ),
      error = function(e) structure(list(message = conditionMessage(e)), class = "zinb_fit_error")
    )

    if (inherits(fit, "zinb_fit_error")) {
      failure_reasons <- c(failure_reasons, sprintf("%s: fit failed: %s", attempt$label, fit$message))
      next
    }

    convergence_reason <- fit_convergence_reason(fit, warnings = fit_warnings)
    if (!is.na(convergence_reason)) {
      failure_reasons <- c(failure_reasons, sprintf("%s: %s", attempt$label, convergence_reason))
      next
    }

    return(list(ok = TRUE, model = fit, attempt = attempt$label))
  }

  list(ok = FALSE, reason = paste(unique(failure_reasons), collapse = " || "))
}

fit_predict_one_fold <- function(train_dt, test_dt, formula_obj) {
  fit_result <- fit_zinb_with_retries(train_dt, formula_obj)
  if (!isTRUE(fit_result$ok)) {
    return(list(ok = FALSE, reason = fit_result$reason))
  }
  fit <- fit_result$model

  pred <- tryCatch(
    predict(fit, newdata = test_dt, type = "response"),
    error = function(e) structure(list(message = conditionMessage(e)), class = "zinb_predict_error")
  )
  if (inherits(pred, "zinb_predict_error")) {
    return(list(ok = FALSE, reason = sprintf("predict failed: %s", pred$message)))
  }
  if (length(pred) != nrow(test_dt) || all(is.na(pred))) {
    return(list(ok = FALSE, reason = "predict returned invalid or only NA values"))
  }

  count_mean <- tryCatch(
    predict(fit, newdata = test_dt, type = "count"),
    error = function(e) structure(list(message = conditionMessage(e)), class = "zinb_predict_error")
  )
  if (inherits(count_mean, "zinb_predict_error")) {
    return(list(ok = FALSE, reason = sprintf("count prediction failed: %s", count_mean$message)))
  }

  zero_prob <- tryCatch(
    predict(fit, newdata = test_dt, type = "zero"),
    error = function(e) structure(list(message = conditionMessage(e)), class = "zinb_predict_error")
  )
  if (inherits(zero_prob, "zinb_predict_error")) {
    return(list(ok = FALSE, reason = sprintf("zero prediction failed: %s", zero_prob$message)))
  }

  theta <- fit$theta
  if (is.null(theta) || !is.finite(theta) || theta <= 0) {
    return(list(ok = FALSE, reason = "fitted ZINB theta is missing or invalid"))
  }

  y <- test_dt[[all.vars(formula_obj)[1]]]
  mu <- pmax(as.numeric(count_mean), .Machine$double.eps)
  pi0 <- pmin(pmax(as.numeric(zero_prob), 0), 1)
  nb_prob <- stats::dnbinom(y, mu = mu, size = theta)
  point_prob <- ifelse(y == 0, pi0 + (1 - pi0) * nb_prob, (1 - pi0) * nb_prob)
  point_prob <- pmax(point_prob, .Machine$double.eps)

  list(
    ok = TRUE,
    model = fit,
    fit_attempt = fit_result$attempt,
    pred = as.numeric(pred),
    point_negloglik = -log(point_prob)
  )
}

fit_zinb_model <- function(dt, formula_obj) {
  fit_zinb_with_retries(dt, formula_obj)
}

evaluate_formula_cv <- function(dt, target, fold_ids, formula_obj, metric = "rmse") {
  n_folds <- max(fold_ids)
  fold_results <- vector("list", n_folds)
  pred_list <- vector("list", n_folds)

  for (fold in seq_len(n_folds)) {
    train_dt <- dt[fold_ids != fold]
    test_dt <- dt[fold_ids == fold]

    res <- fit_predict_one_fold(train_dt, test_dt, formula_obj)
    if (!isTRUE(res$ok)) {
      return(list(ok = FALSE, reason = sprintf("%s in fold %s", res$reason, fold)))
    }

    fm <- reg_metrics(test_dt[[target]], res$pred, negloglik = res$point_negloglik)
    fm[, fold := fold]
    fold_results[[fold]] <- fm

    pred_list[[fold]] <- data.table(
      row_id = which(fold_ids == fold),
      fold = fold,
      truth = test_dt[[target]],
      response = res$pred,
      error = res$pred - test_dt[[target]],
      abs_error = abs(res$pred - test_dt[[target]]),
      negloglik = res$point_negloglik
    )
  }

  fold_metrics <- rbindlist(fold_results, fill = TRUE)
  predictions <- rbindlist(pred_list, fill = TRUE)
  overall <- aggregate_predictions(predictions)

  list(
    ok = TRUE,
    formula = formula_obj,
    fold_metrics = fold_metrics,
    predictions = predictions,
    overall = overall,
    score = overall[[metric]][1]
  )
}

run_forward_selection <- function(work_dt, target, predictor_pool, fold_ids, zero_formula_rhs,
                                  metric, max_vars, min_improvement, workers) {
  baseline_formula <- make_formula(target, character(0), zero_formula_rhs = zero_formula_rhs)
  baseline_eval <- evaluate_formula_cv(work_dt, target, fold_ids, baseline_formula, metric = metric)
  if (!isTRUE(baseline_eval$ok)) {
    stop("The intercept-only ZINB baseline failed: ", baseline_eval$reason, call. = FALSE)
  }

  current_best_score <- baseline_eval$score
  selected <- character(0)
  selected_terms <- character(0)
  selected_transforms <- character(0)
  search_log <- list()
  failure_log <- list()
  best_step_results <- list()
  step_diagnostics <- list()
  top_candidates_log <- list()
  stop_reason <- NA_character_
  max_steps <- min(length(predictor_pool), max_vars)

  for (step_i in seq_len(max_steps)) {
    remaining <- setdiff(predictor_pool, selected)
    if (length(remaining) == 0) {
      stop_reason <- "no_remaining_predictors"
      break
    }

    candidate_specs <- list()
    spec_idx <- 1L

    for (v in remaining) {
      tfms <- valid_transformations(work_dt[[v]], var_name = v)
      if (length(tfms) == 0) {
        failure_log[[length(failure_log) + 1L]] <- data.table(
          step = step_i,
          variable = v,
          transformation = NA_character_,
          added_term = NA_character_,
          formula = NA_character_,
          reason = "no valid transformation for column type or values"
        )
        next
      }

      for (tfm in tfms) {
        term <- term_for(v, tfm)
        terms_now <- c(selected_terms, term)
        candidate_specs[[spec_idx]] <- list(
          step = step_i,
          candidate_order = spec_idx,
          variable = v,
          transformation = tfm,
          term = term,
          formula_obj = make_formula(target, terms_now, zero_formula_rhs = zero_formula_rhs)
        )
        spec_idx <- spec_idx + 1L
      }
    }

    n_candidates_total <- length(candidate_specs)
    results <- evaluate_candidates_parallel(
      candidate_specs,
      work_dt = work_dt,
      target = target,
      fold_ids = fold_ids,
      metric = metric,
      workers = workers
    )

    candidates <- Filter(function(x) isTRUE(x$ok), results)
    failures <- Filter(function(x) !isTRUE(x$ok), results)
    n_candidates_valid <- length(candidates)
    n_candidates_failed <- length(failures)
    if (length(failures) > 0) {
      failure_log <- c(failure_log, lapply(failures, `[[`, "failure"))
    }

    if (length(candidates) == 0) {
      step_diagnostics[[length(step_diagnostics) + 1L]] <- data.table(
        step = step_i,
        n_candidates_total = n_candidates_total,
        n_candidates_valid = n_candidates_valid,
        n_candidates_failed = n_candidates_failed,
        best_variable = NA_character_,
        best_transformation = NA_character_,
        best_formula = NA_character_,
        best_score = NA_real_,
        selected = FALSE,
        stop_reason = "no_valid_candidate"
      )
      stop_reason <- "no_valid_candidate"
      break
    }

    step_table <- rbindlist(lapply(candidates, `[[`, "summary"), fill = TRUE)
    order_cols <- intersect(c(metric, "mae", "max_error"), names(step_table))
    order_cols <- c(order_cols, "candidate_order")
    order_dir <- if (metric == "r2") c(-1, rep(1, length(order_cols) - 1L)) else rep(1, length(order_cols))
    setorderv(step_table, cols = order_cols, order = order_dir)
    top_candidates <- copy(head(step_table, 3))
    top_candidates[, rank_within_step := seq_len(.N)]
    top_candidates_log[[length(top_candidates_log) + 1L]] <- top_candidates
    best_row <- step_table[1]
    improved <- is_improvement(best_row$optimization_score, current_best_score, metric, min_improvement)

    step_diagnostics[[length(step_diagnostics) + 1L]] <- data.table(
      step = step_i,
      n_candidates_total = n_candidates_total,
      n_candidates_valid = n_candidates_valid,
      n_candidates_failed = n_candidates_failed,
      best_variable = best_row$variable,
      best_transformation = best_row$transformation,
      best_formula = best_row$formula,
      best_score = best_row$optimization_score,
      selected = improved,
      stop_reason = if (improved) NA_character_ else "no_min_improvement"
    )

    search_log[[step_i]] <- copy(step_table)
    if (!improved) {
      stop_reason <- "no_min_improvement"
      break
    }

    selected <- c(selected, best_row$variable)
    selected_terms <- c(selected_terms, best_row$added_term)
    selected_transforms <- c(selected_transforms, best_row$transformation)
    current_best_score <- best_row$optimization_score

    best_idx <- which(
      vapply(candidates, function(x) {
        identical(x$summary$variable[1], best_row$variable[1]) &&
          identical(x$summary$transformation[1], best_row$transformation[1])
      }, logical(1))
    )[1]
    best_eval <- candidates[[best_idx]]$eval

    step_summary <- copy(best_row)
    step_summary[, candidate_order := NULL]
    step_summary[, selected_variables := paste(selected, collapse = " | ")]
    step_summary[, selected_terms := paste(selected_terms, collapse = " + ")]
    best_step_results[[step_i]] <- list(summary = step_summary, eval = best_eval)
  }

  if (length(best_step_results) == 0) {
    if (is.na(stop_reason)) stop_reason <- "baseline_retained"
    return(list(
      baseline_formula = baseline_formula,
      baseline_eval = baseline_eval,
      best_global_eval = baseline_eval,
      final_formula_obj = baseline_formula,
      final_summary_line = "intercept-only baseline",
      final_selected_terms = character(0),
      final_selected_variables = character(0),
      best_by_step = NULL,
      search_log = search_log,
      top_candidates_log = top_candidates_log,
      step_diagnostics = step_diagnostics,
      failure_log = failure_log,
      stop_reason = stop_reason
    ))
  }

  best_by_step <- rbindlist(lapply(best_step_results, function(x) x$summary), fill = TRUE)
  best_global_idx <- if (metric == "r2") which.max(best_by_step[[metric]]) else which.min(best_by_step[[metric]])
  best_global <- best_step_results[[best_global_idx]]
  best_global_summary <- best_global$summary

  remaining_after_selection <- setdiff(predictor_pool, selected)
  if (is.na(stop_reason)) {
    stop_reason <- if (length(remaining_after_selection) == 0) "no_remaining_predictors" else "max_steps_reached"
  }

  list(
    baseline_formula = baseline_formula,
    baseline_eval = baseline_eval,
    best_global_eval = best_global$eval,
    final_formula_obj = best_global$eval$formula,
    final_summary_line = formula_label(best_global$eval$formula),
    final_selected_terms = strsplit(best_global_summary$selected_terms[[1]], " \\+ ", fixed = FALSE)[[1]],
    final_selected_variables = strsplit(best_global_summary$selected_variables[[1]], " \\| ", fixed = FALSE)[[1]],
    best_by_step = best_by_step,
    search_log = search_log,
    top_candidates_log = top_candidates_log,
    step_diagnostics = step_diagnostics,
    failure_log = failure_log,
    stop_reason = stop_reason
  )
}

is_improvement <- function(candidate_score, current_score, metric, min_improvement) {
  if (metric == "r2") {
    return(candidate_score > current_score + min_improvement)
  }
  candidate_score < current_score - min_improvement
}

evaluate_candidate <- function(spec, work_dt, target, fold_ids, metric) {
  ev <- evaluate_formula_cv(work_dt, target, fold_ids, spec$formula_obj, metric = metric)

  if (isTRUE(ev$ok)) {
    summary <- data.table(
      step = spec$step,
      candidate_order = spec$candidate_order,
      variable = spec$variable,
      transformation = spec$transformation,
      added_term = spec$term,
      formula = formula_label(spec$formula_obj),
      rmse = ev$overall$rmse,
      mae = ev$overall$mae,
      max_error = ev$overall$max_error,
      sae = ev$overall$sae,
      mse = ev$overall$mse,
      bias = ev$overall$bias,
      r2 = ev$overall$r2,
      poisson_deviance = ev$overall$poisson_deviance,
      negloglik = ev$overall$negloglik,
      optimization_score = ev$score
    )
    return(list(ok = TRUE, summary = summary, eval = ev))
  }

  list(
    ok = FALSE,
    failure = data.table(
      step = spec$step,
      candidate_order = spec$candidate_order,
      variable = spec$variable,
      transformation = spec$transformation,
      added_term = spec$term,
      formula = formula_label(spec$formula_obj),
      reason = ev$reason
    )
  )
}

evaluate_candidates_parallel <- function(candidate_specs, work_dt, target, fold_ids, metric, workers) {
  if (length(candidate_specs) == 0) return(list())
  if (workers <= 1) {
    return(lapply(candidate_specs, evaluate_candidate, work_dt = work_dt, target = target, fold_ids = fold_ids, metric = metric))
  }

  if (.Platform$OS.type != "unix") {
    message("Parallel ZINB candidate evaluation uses forked workers and is only enabled on Unix-like systems. Falling back to sequential execution.")
    return(lapply(candidate_specs, evaluate_candidate, work_dt = work_dt, target = target, fold_ids = fold_ids, metric = metric))
  }

  parallel::mclapply(
    candidate_specs,
    evaluate_candidate,
    work_dt = work_dt,
    target = target,
    fold_ids = fold_ids,
    metric = metric,
    mc.cores = min(workers, length(candidate_specs)),
    mc.preschedule = FALSE,
    mc.set.seed = TRUE
  )
}

# =========================
# Main
# =========================
.script_ok <- FALSE
LOG_STATE <- start_logging(OUTPUT_DIR, SCRIPT_NAME)
with_run_finalizer({
  set.seed(SEED)
  df <- load_csv_checked(DATA_PATH)
  zero_formula_cols <- if (identical(ZERO_INFLATION_FORMULA, "same_as_count")) character(0) else {
    formula_referenced_columns(ZERO_INFLATION_FORMULA, label = "ZINB zero-formula")
  }
  work_dt <- prepare_modeling_data(
    df, TARGET, FEATURE_COLS, ID_COLS,
    require_count_target = TRUE,
    row_filter = ROW_FILTER,
    extra_feature_cols = zero_formula_cols
  )
  dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
  resolved_config <- list(
    script_name = SCRIPT_NAME,
    data_path = normalizePath(DATA_PATH, mustWork = FALSE),
    output_dir = normalizePath(OUTPUT_DIR, mustWork = FALSE),
    target = TARGET,
    feature_cols = FEATURE_COLS,
    id_cols = ID_COLS,
    row_filter = ROW_FILTER,
    seed = SEED,
    n_folds = N_FOLDS,
    inner_folds = INNER_FOLDS,
    strata_bins = STRATA_BINS,
    metric = METRIC_TO_OPTIMIZE,
    max_vars = MAX_VARS,
    min_improvement = MIN_IMPROVEMENT,
    workers = N_WORKERS,
    zero_inflation_formula = ZERO_INFLATION_FORMULA,
    transformations_numeric = TRANSFORMATIONS_NUMERIC,
    transformations_factor = TRANSFORMATIONS_FACTOR,
    numeric_as_factor_max_levels = NUMERIC_AS_FACTOR_MAX_LEVELS,
    numeric_as_factor_vars = NUMERIC_AS_FACTOR_VARS
  )
  write_config_snapshot(OUTPUT_DIR, resolved_config)

  log_info("Using data file: ", normalizePath(DATA_PATH, mustWork = FALSE))
  log_info("Using output directory: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
  if (nzchar(trimws(ROW_FILTER))) log_info("Using row filter: ", ROW_FILTER)
  log_dataset_overview(
    work_dt,
    target = TARGET,
    feature_cols = FEATURE_COLS,
    id_cols = ID_COLS,
    metric = METRIC_TO_OPTIMIZE,
    extra = list(
      "Folds" = N_FOLDS,
      "Inner folds" = INNER_FOLDS,
      "Workers" = N_WORKERS,
      "Row filter" = if (nzchar(trimws(ROW_FILTER))) ROW_FILTER else "<none>",
      "Numeric-as-factor max levels" = NUMERIC_AS_FACTOR_MAX_LEVELS,
      "Numeric-as-factor vars" = if (length(NUMERIC_AS_FACTOR_VARS) > 0) paste(NUMERIC_AS_FACTOR_VARS, collapse = ", ") else "<none>",
      "Zero-inflation formula" = ZERO_INFLATION_FORMULA,
      "Max vars / min improvement" = paste(MAX_VARS, MIN_IMPROVEMENT, sep = " / ")
    )
  )
  overview_dt <- dataset_overview(work_dt, target = TARGET, feature_cols = FEATURE_COLS, id_cols = ID_COLS)
  safe_write_csv(overview_dt, file.path(OUTPUT_DIR, "zinb_dataset_overview.csv"))

  validate_zinb_setup(work_dt, TARGET, FEATURE_COLS, ZERO_INFLATION_FORMULA)

  predictor_pool <- FEATURE_COLS
  outer_fold_ids <- make_stratified_fold_ids(work_dt[[TARGET]], nfolds = N_FOLDS, seed = SEED, n_bins = STRATA_BINS)
  full_data_selection <- run_forward_selection(
    work_dt = work_dt,
    target = TARGET,
    predictor_pool = predictor_pool,
    fold_ids = outer_fold_ids,
    zero_formula_rhs = ZERO_INFLATION_FORMULA,
    metric = METRIC_TO_OPTIMIZE,
    max_vars = MAX_VARS,
    min_improvement = MIN_IMPROVEMENT,
    workers = N_WORKERS
  )

  safe_write_csv(full_data_selection$baseline_eval$overall, file.path(OUTPUT_DIR, "zinb_baseline_overall_metrics.csv"))
  if (length(full_data_selection$search_log) > 0) {
    safe_write_csv(rbindlist(full_data_selection$search_log, fill = TRUE), file.path(OUTPUT_DIR, "zinb_all_candidates_by_step.csv"))
  }
  if (length(full_data_selection$top_candidates_log) > 0) {
    safe_write_csv(rbindlist(full_data_selection$top_candidates_log, fill = TRUE), file.path(OUTPUT_DIR, "zinb_top_candidates_by_step.csv"))
  }
  if (length(full_data_selection$step_diagnostics) > 0) {
    safe_write_csv(rbindlist(full_data_selection$step_diagnostics, fill = TRUE), file.path(OUTPUT_DIR, "zinb_step_diagnostics.csv"))
  }
  if (length(full_data_selection$failure_log) > 0) {
    safe_write_csv(rbindlist(full_data_selection$failure_log, fill = TRUE), file.path(OUTPUT_DIR, "zinb_failed_candidates.csv"))
  }
  if (!is.null(full_data_selection$best_by_step) && nrow(full_data_selection$best_by_step) > 0) {
    safe_write_csv(full_data_selection$best_by_step, file.path(OUTPUT_DIR, "zinb_best_model_per_step.csv"))
  }

  outer_predictions <- list()
  outer_selected_models <- list()
  for (fold in seq_len(N_FOLDS)) {
    train_dt <- work_dt[outer_fold_ids != fold]
    test_dt <- work_dt[outer_fold_ids == fold]
    inner_folds_now <- min(INNER_FOLDS, nrow(train_dt))
    if (inner_folds_now < 2L) {
      stop("ZINB inner CV needs at least 2 rows in each outer-training split.", call. = FALSE)
    }
    inner_fold_ids <- make_stratified_fold_ids(
      train_dt[[TARGET]],
      nfolds = inner_folds_now,
      seed = SEED + fold,
      n_bins = STRATA_BINS
    )
    fold_selection <- run_forward_selection(
      work_dt = train_dt,
      target = TARGET,
      predictor_pool = predictor_pool,
      fold_ids = inner_fold_ids,
      zero_formula_rhs = ZERO_INFLATION_FORMULA,
      metric = METRIC_TO_OPTIMIZE,
      max_vars = MAX_VARS,
      min_improvement = MIN_IMPROVEMENT,
      workers = N_WORKERS
    )
    fold_fit <- fit_predict_one_fold(train_dt, test_dt, fold_selection$final_formula_obj)
    if (!isTRUE(fold_fit$ok)) {
      stop("Outer-fold ZINB evaluation failed in fold ", fold, ": ", fold_fit$reason, call. = FALSE)
    }
    outer_predictions[[fold]] <- data.table(
      row_id = which(outer_fold_ids == fold),
      fold = fold,
      truth = test_dt[[TARGET]],
      response = fold_fit$pred,
      error = fold_fit$pred - test_dt[[TARGET]],
      abs_error = abs(fold_fit$pred - test_dt[[TARGET]]),
      negloglik = fold_fit$point_negloglik
    )
    outer_selected_models[[fold]] <- data.table(
      outer_fold = fold,
      inner_folds = inner_folds_now,
      formula = formula_label(fold_selection$final_formula_obj),
      stop_reason = fold_selection$stop_reason,
      selected_variables = if (length(fold_selection$final_selected_variables) > 0) {
        paste(fold_selection$final_selected_variables, collapse = ", ")
      } else {
        NA_character_
      },
      selected_terms = if (length(fold_selection$final_selected_terms) > 0) {
        paste(fold_selection$final_selected_terms, collapse = " + ")
      } else {
        NA_character_
      }
    )
  }

  zinb_outer_predictions <- rbindlist(outer_predictions, fill = TRUE)
  zinb_outer_fold_metrics <- fold_metrics_from_predictions(zinb_outer_predictions)
  zinb_outer_overall_metrics <- aggregate_predictions(zinb_outer_predictions)
  safe_write_csv(zinb_outer_fold_metrics, file.path(OUTPUT_DIR, "zinb_best_global_fold_metrics.csv"))
  safe_write_csv(zinb_outer_overall_metrics, file.path(OUTPUT_DIR, "zinb_best_global_overall_metrics.csv"))
  safe_write_csv(zinb_outer_predictions, file.path(OUTPUT_DIR, "zinb_best_global_cv_predictions.csv"))
  safe_write_csv(rbindlist(outer_selected_models, fill = TRUE), file.path(OUTPUT_DIR, "zinb_outer_fold_selected_models.csv"))

  final_formula_obj <- full_data_selection$final_formula_obj
  final_summary_line <- full_data_selection$final_summary_line
  final_selected_terms <- full_data_selection$final_selected_terms
  final_selected_variables <- full_data_selection$final_selected_variables
  stop_reason <- full_data_selection$stop_reason

  log_info("Done. Files written to: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
  log_info("Outer-CV ZINB metrics:")
  print(zinb_outer_overall_metrics)
  log_info("Interpretation / refit formula on full data:")
  print(final_formula_obj)
  .script_ok <- TRUE

  final_fit <- fit_zinb_model(work_dt, final_formula_obj)
  if (isTRUE(final_fit$ok)) {
    final_fit_summary <- summary(final_fit$model)
    count_coef <- data.table::as.data.table(final_fit_summary$coefficients$count, keep.rownames = "term")
    zero_coef <- data.table::as.data.table(final_fit_summary$coefficients$zero, keep.rownames = "term")
    safe_write_csv(count_coef, file.path(OUTPUT_DIR, "zinb_final_model_count_coefficients.csv"))
    safe_write_csv(zero_coef, file.path(OUTPUT_DIR, "zinb_final_model_zero_coefficients.csv"))

    model_summary_dt <- data.table(
      formula = formula_label(final_formula_obj),
      zero_inflation_formula = ZERO_INFLATION_FORMULA,
      stop_reason = stop_reason,
      n_selected_variables = length(final_selected_variables),
      selected_variables = if (length(final_selected_variables) > 0) paste(final_selected_variables, collapse = ", ") else NA_character_,
      selected_terms = if (length(final_selected_terms) > 0) paste(final_selected_terms, collapse = " + ") else NA_character_,
      theta = final_fit$model$theta,
      loglik = as.numeric(logLik(final_fit$model)),
      aic = AIC(final_fit$model),
      bic = BIC(final_fit$model)
    )
    safe_write_csv(model_summary_dt, file.path(OUTPUT_DIR, "zinb_final_model_summary.csv"))
    write_text_file(
      file.path(OUTPUT_DIR, "zinb_final_model_summary.txt"),
      c(
        sprintf("Formula: %s", formula_label(final_formula_obj)),
        sprintf("Zero-inflation formula: %s", ZERO_INFLATION_FORMULA),
        sprintf("Stop reason: %s", stop_reason),
        sprintf("Final fit attempt: %s", final_fit$attempt),
        sprintf("Theta: %s", format(final_fit$model$theta, digits = 8)),
        sprintf("LogLik: %s", format(as.numeric(logLik(final_fit$model)), digits = 8)),
        sprintf("AIC: %s", format(AIC(final_fit$model), digits = 8)),
        sprintf("BIC: %s", format(BIC(final_fit$model), digits = 8))
      )
    )
  } else {
    log_info("Warning: final ZINB model summary could not be created: ", final_fit$reason)
  }

  report_lines <- c(
    sprintf("# %s Report", SCRIPT_NAME),
    "",
    "## Run",
    sprintf("- data_path: `%s`", normalizePath(DATA_PATH, mustWork = FALSE)),
    sprintf("- output_dir: `%s`", normalizePath(OUTPUT_DIR, mustWork = FALSE)),
    sprintf("- target: `%s`", TARGET),
    sprintf("- feature_cols: `%s`", paste(FEATURE_COLS, collapse = ", ")),
    sprintf("- zero_inflation_formula: `%s`", ZERO_INFLATION_FORMULA),
    sprintf("- seed: `%s`", SEED),
    sprintf("- folds: `%s`", N_FOLDS),
    sprintf("- inner_folds: `%s`", INNER_FOLDS),
    sprintf("- workers: `%s`", N_WORKERS),
    sprintf("- numeric_as_factor_max_levels: `%s`", NUMERIC_AS_FACTOR_MAX_LEVELS),
    sprintf("- numeric_as_factor_vars: `%s`", if (length(NUMERIC_AS_FACTOR_VARS) > 0) paste(NUMERIC_AS_FACTOR_VARS, collapse = ", ") else "<none>"),
    sprintf("- optimization_metric: `%s`", METRIC_TO_OPTIMIZE),
    sprintf("- stop_reason: `%s`", stop_reason),
    "",
    "## Dataset",
    sprintf("- rows: `%s`", overview_dt$n_rows[[1]]),
    sprintf("- cols: `%s`", overview_dt$n_cols[[1]]),
    sprintf("- factor_cols: `%s`", overview_dt$n_factor_cols[[1]]),
    "",
    "## Final Model",
    sprintf("- formula: `%s`", final_summary_line),
    sprintf("- selected_variables: `%s`", if (length(final_selected_variables) > 0) paste(final_selected_variables, collapse = ", ") else "<none>"),
    sprintf("- selected_terms: `%s`", if (length(final_selected_terms) > 0) paste(final_selected_terms, collapse = " + ") else "<baseline>"),
    "- reported ZINB metrics come from outer CV; the final formula and coefficient files are refit on the full dataset for interpretation.",
    "",
    "## Outputs",
    "- `zinb_step_diagnostics.csv` / `.rds`",
    "- `zinb_top_candidates_by_step.csv` / `.rds`",
    "- `zinb_all_candidates_by_step.csv` / `.rds`",
    "- `zinb_failed_candidates.csv` / `.rds`",
    "- `zinb_best_model_per_step.csv` / `.rds`",
    "- `zinb_outer_fold_selected_models.csv` / `.rds`",
    "- `zinb_final_model_summary.csv` / `.rds`",
    "- `zinb_final_model_count_coefficients.csv` / `.rds`",
    "- `zinb_final_model_zero_coefficients.csv` / `.rds`",
    "- `resolved_config.txt` / `.rds`",
    "- `run_manifest.csv` / `.rds`"
  )
  write_text_file(file.path(OUTPUT_DIR, "zinb_model_report.md"), report_lines)
}, function() finalize_run(
  log_state = LOG_STATE,
  output_dir = OUTPUT_DIR,
  script_name = SCRIPT_NAME,
  repo_dir = REPO_DIR,
  packages = SCRIPT_PACKAGES,
  status = if (.script_ok) "completed" else "failed",
  seed = SEED,
  data_path = DATA_PATH,
  feature_cols = FEATURE_COLS,
  n_workers = N_WORKERS
))
