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

DATA_PATH <- get_path_setting(
  "data", "MLR3_DATA_PATH",
  config_value(CONFIG, c("experiment", "data_path")),
  base_dir = REPO_DIR
)
OUTPUT_DIR <- get_path_setting("output-dir", "ZINB_OUTPUT_DIR", config_value(CONFIG, c("zinb", "output_dir")), base_dir = REPO_DIR)
N_FOLDS <- get_int_setting("folds", "N_FOLDS", config_value(CONFIG, c("experiment", "n_folds")), min_value = 2)
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

valid_transformations <- function(x) {
  if (is.numeric(x)) {
    allowed <- c("raw", "ns2", "poly2")
    if (is_nonnegative(x)) allowed <- c(allowed, "sqrt", "log1p")
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

  rhs_formula <- tryCatch(
    as.formula(sprintf("~ %s", rhs)),
    error = function(e) {
      stop(label, " is not a valid formula right-hand side: ", conditionMessage(e), call. = FALSE)
    }
  )

  referenced_vars <- all.vars(rhs_formula)
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
    tfms <- valid_transformations(dt[[feature]])
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

fit_predict_one_fold <- function(train_dt, test_dt, formula_obj) {
  fit_warnings <- character(0)
  fit <- tryCatch(
    withCallingHandlers(
      zeroinfl(
        formula_obj,
        data = train_dt,
        dist = "negbin",
        EM = TRUE,
        control = zeroinfl.control(maxit = 100)
      ),
      warning = function(w) {
        fit_warnings <<- c(fit_warnings, conditionMessage(w))
        invokeRestart("muffleWarning")
      }
    ),
    error = function(e) structure(list(message = conditionMessage(e)), class = "zinb_fit_error")
  )
  if (inherits(fit, "zinb_fit_error")) {
    return(list(ok = FALSE, reason = sprintf("fit failed: %s", fit$message)))
  }

  convergence_reason <- fit_convergence_reason(fit, warnings = fit_warnings)
  if (!is.na(convergence_reason)) return(list(ok = FALSE, reason = convergence_reason))

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
    pred = as.numeric(pred),
    point_negloglik = -log(point_prob)
  )
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
  work_dt <- prepare_modeling_data(df, TARGET, FEATURE_COLS, ID_COLS, require_count_target = TRUE)
  dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

  log_info("Using data file: ", normalizePath(DATA_PATH, mustWork = FALSE))
  log_info("Using output directory: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
  log_info("Using features: ", paste(FEATURE_COLS, collapse = ", "))
  log_info("Using folds / metric / workers: ", N_FOLDS, " / ", METRIC_TO_OPTIMIZE, " / ", N_WORKERS)
  log_info("Using zero-inflation formula: ", ZERO_INFLATION_FORMULA)

  validate_zinb_setup(work_dt, TARGET, FEATURE_COLS, ZERO_INFLATION_FORMULA)

  predictor_pool <- FEATURE_COLS
  fold_ids <- make_stratified_fold_ids(work_dt[[TARGET]], nfolds = N_FOLDS, seed = SEED, n_bins = STRATA_BINS)

  baseline_formula <- make_formula(TARGET, character(0), zero_formula_rhs = ZERO_INFLATION_FORMULA)
  baseline_eval <- evaluate_formula_cv(work_dt, TARGET, fold_ids, baseline_formula, metric = METRIC_TO_OPTIMIZE)
  if (!isTRUE(baseline_eval$ok)) {
    stop("The intercept-only ZINB baseline failed: ", baseline_eval$reason, call. = FALSE)
  }
  current_best_score <- baseline_eval$score
  safe_write_csv(baseline_eval$overall, file.path(OUTPUT_DIR, "zinb_baseline_overall_metrics.csv"))

  selected <- character(0)
  selected_terms <- character(0)
  selected_transforms <- character(0)
  search_log <- list()
  failure_log <- list()
  best_step_results <- list()

  max_steps <- min(length(predictor_pool), MAX_VARS)

  for (step_i in seq_len(max_steps)) {
    remaining <- setdiff(predictor_pool, selected)
    if (length(remaining) == 0) break

    candidate_specs <- list()
    spec_idx <- 1L

    for (v in remaining) {
      tfms <- valid_transformations(work_dt[[v]])
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
          formula_obj = make_formula(TARGET, terms_now, zero_formula_rhs = ZERO_INFLATION_FORMULA)
        )
        spec_idx <- spec_idx + 1L
      }
    }

    results <- evaluate_candidates_parallel(
      candidate_specs,
      work_dt = work_dt,
      target = TARGET,
      fold_ids = fold_ids,
      metric = METRIC_TO_OPTIMIZE,
      workers = N_WORKERS
    )

    candidates <- Filter(function(x) isTRUE(x$ok), results)
    failures <- Filter(function(x) !isTRUE(x$ok), results)
    if (length(failures) > 0) {
      failure_log <- c(failure_log, lapply(failures, `[[`, "failure"))
    }

    if (length(candidates) == 0) {
      log_info("No valid candidate found at step ", step_i, ". Stopping.")
      break
    }

    step_table <- rbindlist(lapply(candidates, `[[`, "summary"), fill = TRUE)
    order_cols <- intersect(c(METRIC_TO_OPTIMIZE, "mae", "max_error"), names(step_table))
    order_cols <- c(order_cols, "candidate_order")
    order_dir <- if (METRIC_TO_OPTIMIZE == "r2") c(-1, rep(1, length(order_cols) - 1L)) else rep(1, length(order_cols))
    setorderv(step_table, cols = order_cols, order = order_dir)
    best_row <- step_table[1]

    if (!is_improvement(best_row$optimization_score, current_best_score, METRIC_TO_OPTIMIZE, MIN_IMPROVEMENT)) {
      log_info("Step ", step_i, " did not improve ", METRIC_TO_OPTIMIZE, " beyond ", sprintf("%.5f", MIN_IMPROVEMENT), ". Stopping.")
      search_log[[step_i]] <- copy(step_table)
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

    search_log[[step_i]] <- copy(step_table)
    best_step_results[[step_i]] <- list(summary = step_summary, eval = best_eval)

    log_info(
      "Step ", step_i, " selected: ", best_row$variable, " [", best_row$transformation,
      "] | ", METRIC_TO_OPTIMIZE, " = ", sprintf("%.5f", best_row$optimization_score)
    )
  }

  if (length(search_log) > 0) {
    safe_write_csv(rbindlist(search_log, fill = TRUE), file.path(OUTPUT_DIR, "zinb_all_candidates_by_step.csv"))
  }
  if (length(failure_log) > 0) {
    safe_write_csv(rbindlist(failure_log, fill = TRUE), file.path(OUTPUT_DIR, "zinb_failed_candidates.csv"))
  }

  if (length(best_step_results) == 0) {
    safe_write_csv(baseline_eval$fold_metrics, file.path(OUTPUT_DIR, "zinb_best_global_fold_metrics.csv"))
    safe_write_csv(baseline_eval$overall, file.path(OUTPUT_DIR, "zinb_best_global_overall_metrics.csv"))
    safe_write_csv(baseline_eval$predictions, file.path(OUTPUT_DIR, "zinb_best_global_cv_predictions.csv"))
    log_info("Done. Files written to: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
    log_info("No ZINB candidate improved on the intercept-only baseline.")
    print(baseline_eval$overall)
    .script_ok <- TRUE
  } else {
    best_by_step <- rbindlist(lapply(best_step_results, function(x) x$summary), fill = TRUE)
    best_global_idx <- if (METRIC_TO_OPTIMIZE == "r2") {
      which.max(best_by_step[[METRIC_TO_OPTIMIZE]])
    } else {
      which.min(best_by_step[[METRIC_TO_OPTIMIZE]])
    }
    best_global <- best_step_results[[best_global_idx]]

    safe_write_csv(best_by_step, file.path(OUTPUT_DIR, "zinb_best_model_per_step.csv"))
    safe_write_csv(best_global$eval$fold_metrics, file.path(OUTPUT_DIR, "zinb_best_global_fold_metrics.csv"))
    safe_write_csv(best_global$eval$overall, file.path(OUTPUT_DIR, "zinb_best_global_overall_metrics.csv"))
    safe_write_csv(best_global$eval$predictions, file.path(OUTPUT_DIR, "zinb_best_global_cv_predictions.csv"))

    log_info("Done. Files written to: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
    log_info("Best global formula:")
    print(best_global$eval$formula)
    print(best_global$eval$overall)
    .script_ok <- TRUE
  }
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
