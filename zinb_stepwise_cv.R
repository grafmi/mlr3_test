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

require_packages(c("data.table", "pscl"))

suppressPackageStartupMessages({
  library(data.table)
  library(pscl)
  library(splines)
})

# =========================
# User settings
# =========================
TARGET <- "n_eintritte"
FEATURE_COLS <- c("prcrank", "potenzielle_kunden", "unfalldeckung")
ID_COLS <- character(0)

DATA_PATH <- get_path_setting(
  "data", "MLR3_DATA_PATH",
  "testfile_zinb_nonlinear_eintritte.csv",
  base_dir = REPO_DIR
)
OUTPUT_DIR <- get_path_setting("output-dir", "ZINB_OUTPUT_DIR", "outputs_zinb", base_dir = REPO_DIR)
N_FOLDS <- get_int_setting("folds", "N_FOLDS", 10, min_value = 2)
SEED <- get_int_setting("seed", "SEED", 123)
STRATA_BINS <- get_int_setting("strata-bins", "STRATA_BINS", 10, min_value = 2)
MAX_VARS <- get_numeric_setting("max-vars", "ZINB_MAX_VARS", Inf, min_value = 1)
MIN_IMPROVEMENT <- get_numeric_setting("min-improvement", "ZINB_MIN_IMPROVEMENT", 0, min_value = 0)
METRIC_TO_OPTIMIZE <- get_setting("metric", "METRIC_TO_OPTIMIZE", "rmse")
N_WORKERS <- get_int_setting("workers", "ZINB_WORKERS", Sys.getenv("N_WORKERS", unset = "1"), min_value = 1)
TRANSFORMATIONS_NUMERIC <- c("raw", "sqrt", "log1p", "ns2", "poly2")
TRANSFORMATIONS_FACTOR <- c("raw")
USE_SAME_FORMULA_FOR_ZERO_PART <- isTRUE(tolower(get_setting("same-zero-formula", "ZINB_SAME_ZERO_FORMULA", "false")) %in% c("1", "true", "yes"))

ALLOWED_METRICS <- c("rmse", "mae", "max_error", "mse", "r2")
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

make_formula <- function(target, terms, same_zero_formula = TRUE) {
  rhs_count <- if (length(terms) == 0) "1" else paste(terms, collapse = " + ")
  rhs_zero <- if (same_zero_formula) rhs_count else "1"
  as.formula(sprintf("%s ~ %s | %s", quote_name(target), rhs_count, rhs_zero))
}

formula_label <- function(formula_obj) {
  gsub("\\s+", " ", paste(deparse(formula_obj), collapse = " "))
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

  list(ok = TRUE, model = fit, pred = as.numeric(pred))
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

    fm <- reg_metrics(test_dt[[target]], res$pred)
    fm[, fold := fold]
    fold_results[[fold]] <- fm

    pred_list[[fold]] <- data.table(
      row_id = which(fold_ids == fold),
      fold = fold,
      truth = test_dt[[target]],
      response = res$pred,
      error = res$pred - test_dt[[target]],
      abs_error = abs(res$pred - test_dt[[target]])
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
LOG_STATE <- start_logging(OUTPUT_DIR, "zinb_stepwise_cv")

set.seed(SEED)
df <- load_csv_checked(DATA_PATH)
work_dt <- prepare_modeling_data(df, TARGET, FEATURE_COLS, ID_COLS, require_count_target = TRUE)
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

log_info("Using data file: ", normalizePath(DATA_PATH, mustWork = FALSE))
log_info("Using output directory: ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
log_info("Using features: ", paste(FEATURE_COLS, collapse = ", "))
log_info("Using folds / metric / workers: ", N_FOLDS, " / ", METRIC_TO_OPTIMIZE, " / ", N_WORKERS)

predictor_pool <- FEATURE_COLS
fold_ids <- make_stratified_fold_ids(work_dt[[TARGET]], nfolds = N_FOLDS, seed = SEED, n_bins = STRATA_BINS)

baseline_formula <- make_formula(TARGET, character(0), same_zero_formula = USE_SAME_FORMULA_FOR_ZERO_PART)
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
        formula_obj = make_formula(TARGET, terms_now, same_zero_formula = USE_SAME_FORMULA_FOR_ZERO_PART)
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
  stop_logging(LOG_STATE, if (.script_ok) "completed" else "failed")
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
  stop_logging(LOG_STATE, if (.script_ok) "completed" else "failed")
}
