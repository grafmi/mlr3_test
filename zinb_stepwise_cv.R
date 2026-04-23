#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(pscl)
  library(splines)
})

# =========================
# User settings
# =========================
# Default input file: the synthetic ZINB test dataset saved next to this script.
# You can still replace this with your own loader if needed.

TARGET <- "n_eintritte"
CANDIDATE_VARS <- NULL          # NULL = all predictors except TARGET and ID_COLS
ID_COLS <- character(0)
OUTPUT_DIR <- "outputs_zinb"
N_FOLDS <- 10
SEED <- 123
STRATA_BINS <- 10
MAX_VARS <- Inf                 # e.g. 15 if you want to stop early
METRIC_TO_OPTIMIZE <- "rmse"   # choose from: rmse, mae, max_error
TRANSFORMATIONS_NUMERIC <- c("raw", "sqrt", "log1p", "ns2", "poly2")
TRANSFORMATIONS_FACTOR  <- c("raw")
USE_SAME_FORMULA_FOR_ZERO_PART <- FALSE

get_script_dir <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", cmd_args, value = TRUE)
  if (length(file_arg) > 0) {
    return(dirname(normalizePath(sub("^--file=", "", file_arg[1]))))
  }
  if (!is.null(sys.frames()[[1]]$ofile)) {
    return(dirname(normalizePath(sys.frames()[[1]]$ofile)))
  }
  getwd()
}

SCRIPT_DIR <- get_script_dir()
DATA_PATH <- file.path(SCRIPT_DIR, "testfile_zinb_nonlinear_eintritte.csv")

# =========================
# Helpers
# =========================
load_default_df <- function() {
  if (!file.exists(DATA_PATH)) {
    stop(sprintf("Default data file not found: %s", DATA_PATH))
  }
  fread(DATA_PATH)
}

make_strata <- function(y, n_bins = 10) {
  probs <- seq(0, 1, length.out = n_bins + 1)
  brks <- unique(quantile(y, probs = probs, na.rm = TRUE, type = 7))
  if (length(brks) < 3) {
    return(factor(rep("all", length(y))))
  }
  cut(y, breaks = brks, include.lowest = TRUE, ordered_result = TRUE)
}

make_fold_ids <- function(y, nfolds = 10, seed = 123, n_bins = 10) {
  set.seed(seed)
  strata <- make_strata(y, n_bins = n_bins)
  idx_by_stratum <- split(seq_along(y), strata, drop = TRUE)
  fold_ids <- integer(length(y))
  for (ids in idx_by_stratum) {
    ids <- sample(ids)
    parts <- split(ids, rep(seq_len(nfolds), length.out = length(ids)))
    for (k in seq_along(parts)) fold_ids[parts[[k]]] <- k
  }
  fold_ids
}

reg_metrics <- function(truth, response) {
  err <- response - truth
  data.table(
    rmse = sqrt(mean(err^2, na.rm = TRUE)),
    mae = mean(abs(err), na.rm = TRUE),
    max_error = max(abs(err), na.rm = TRUE),
    sae = sum(err, na.rm = TRUE),
    mse = mean(err^2, na.rm = TRUE),
    bias = mean(err, na.rm = TRUE)
  )
}

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

term_for <- function(var, transformation) {
  switch(
    transformation,
    raw = var,
    sqrt = sprintf("sqrt(%s)", var),
    log1p = sprintf("log1p(%s)", var),
    ns2 = sprintf("ns(%s, df = 2)", var),
    poly2 = sprintf("poly(%s, 2)", var),
    stop(sprintf("Unknown transformation: %s", transformation))
  )
}

make_formula <- function(target, terms, same_zero_formula = TRUE) {
  rhs_count <- if (length(terms) == 0) "1" else paste(terms, collapse = " + ")
  rhs_zero  <- if (same_zero_formula) rhs_count else "1"
  as.formula(sprintf("%s ~ %s | %s", target, rhs_count, rhs_zero))
}

formula_label <- function(formula_obj) {
  gsub("\\s+", " ", paste(deparse(formula_obj), collapse = " "))
}

fit_convergence_reason <- function(fit, warnings = character(0)) {
  optim_convergence <- fit$optim$convergence
  converged <- isTRUE(fit$converged)
  optim_ok <- !is.null(optim_convergence) && identical(as.integer(optim_convergence), 0L)
  nonconv_warning <- any(grepl("converg", warnings, ignore.case = TRUE))

  if (converged && optim_ok && !nonconv_warning) {
    return(NA_character_)
  }

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
      zeroinfl(formula_obj, data = train_dt, dist = "negbin", EM = TRUE),
      warning = function(w) {
        fit_warnings <<- c(fit_warnings, conditionMessage(w))
        invokeRestart("muffleWarning")
      }
    ),
    error = function(e) {
      structure(list(message = conditionMessage(e)), class = "zinb_fit_error")
    }
  )
  if (inherits(fit, "zinb_fit_error")) {
    return(list(ok = FALSE, reason = sprintf("fit failed: %s", fit$message)))
  }

  convergence_reason <- fit_convergence_reason(fit, warnings = fit_warnings)
  if (!is.na(convergence_reason)) {
    return(list(ok = FALSE, reason = convergence_reason))
  }

  pred <- tryCatch(
    predict(fit, newdata = test_dt, type = "response"),
    error = function(e) {
      structure(list(message = conditionMessage(e)), class = "zinb_predict_error")
    }
  )
  if (inherits(pred, "zinb_predict_error")) {
    return(list(ok = FALSE, reason = sprintf("predict failed: %s", pred$message)))
  }
  if (all(is.na(pred))) {
    return(list(ok = FALSE, reason = "predict returned only NA values"))
  }

  list(ok = TRUE, model = fit, pred = pred)
}

evaluate_formula_cv <- function(dt, target, fold_ids, formula_obj, metric = "rmse") {
  fold_results <- vector("list", length = max(fold_ids))
  pred_list <- vector("list", length = max(fold_ids))

  for (fold in seq_len(max(fold_ids))) {
    train_dt <- dt[fold_ids != fold]
    test_dt  <- dt[fold_ids == fold]

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
  overall <- reg_metrics(predictions$truth, predictions$response)

  list(
    ok = TRUE,
    formula = formula_obj,
    fold_metrics = fold_metrics,
    predictions = predictions,
    overall = overall,
    score = overall[[metric]][1]
  )
}

# =========================
# Main
# =========================
set.seed(SEED)
df <- load_default_df()
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
cat("Using data file:", normalizePath(DATA_PATH, mustWork = FALSE), "\n")

if (!is.data.frame(df)) stop("df must be a data.frame or data.table")
if (!TARGET %in% names(df)) stop(sprintf("TARGET '%s' not found in df", TARGET))
if (!is.numeric(df[[TARGET]])) stop("TARGET must be numeric count-like for ZINB")
if (any(df[[TARGET]] < 0, na.rm = TRUE)) stop("TARGET must be non-negative for ZINB")

work_dt <- as.data.table(copy(df))
keep_cols <- setdiff(names(work_dt), ID_COLS)
work_dt <- work_dt[, ..keep_cols]

char_cols <- names(which(vapply(work_dt, is.character, logical(1))))
if (length(char_cols) > 0) {
  work_dt[, (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
}

work_dt <- na.omit(work_dt)

predictor_pool <- if (is.null(CANDIDATE_VARS)) {
  setdiff(names(work_dt), TARGET)
} else {
  missing_vars <- setdiff(CANDIDATE_VARS, names(work_dt))
  if (length(missing_vars) > 0) stop("Variables not found in df: ", paste(missing_vars, collapse = ", "))
  setdiff(CANDIDATE_VARS, TARGET)
}

if (length(predictor_pool) == 0) stop("No candidate predictors available.")

fold_ids <- make_fold_ids(work_dt[[TARGET]], nfolds = N_FOLDS, seed = SEED, n_bins = STRATA_BINS)
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

  candidates <- list()
  idx <- 1L

  for (v in remaining) {
    tfms <- valid_transformations(work_dt[[v]])
    for (tfm in tfms) {
      term <- term_for(v, tfm)
      terms_now <- c(selected_terms, term)
      formula_obj <- make_formula(TARGET, terms_now, same_zero_formula = USE_SAME_FORMULA_FOR_ZERO_PART)
      ev <- evaluate_formula_cv(work_dt, TARGET, fold_ids, formula_obj, metric = METRIC_TO_OPTIMIZE)

      if (isTRUE(ev$ok)) {
        candidates[[idx]] <- data.table(
          step = step_i,
          variable = v,
          transformation = tfm,
          added_term = term,
          formula = formula_label(formula_obj),
          rmse = ev$overall$rmse,
          mae = ev$overall$mae,
          max_error = ev$overall$max_error,
          sae = ev$overall$sae,
          mse = ev$overall$mse,
          bias = ev$overall$bias,
          optimization_score = ev$score
        )
        attr(candidates[[idx]], "eval_result") <- ev
        idx <- idx + 1L
      } else {
        failure_log[[length(failure_log) + 1L]] <- data.table(
          step = step_i,
          variable = v,
          transformation = tfm,
          added_term = term,
          formula = formula_label(formula_obj),
          reason = ev$reason
        )
      }
    }
  }

  if (length(candidates) == 0) {
    message(sprintf("No valid candidate found at step %s. Stopping.", step_i))
    break
  }

  step_table <- rbindlist(candidates, fill = TRUE)
  setorderv(step_table, cols = c(METRIC_TO_OPTIMIZE, "mae", "max_error"), order = c(1, 1, 1))
  best_row <- step_table[1]

  selected <- c(selected, best_row$variable)
  selected_terms <- c(selected_terms, best_row$added_term)
  selected_transforms <- c(selected_transforms, best_row$transformation)

  best_idx <- which(
    vapply(candidates, function(x) {
      identical(x$variable[1], best_row$variable[1]) && identical(x$transformation[1], best_row$transformation[1])
    }, logical(1))
  )[1]
  best_eval <- attr(candidates[[best_idx]], "eval_result")

  step_summary <- copy(best_row)
  step_summary[, selected_variables := paste(selected, collapse = " | ")]
  step_summary[, selected_terms := paste(selected_terms, collapse = " + ")]

  search_log[[step_i]] <- copy(step_table)
  best_step_results[[step_i]] <- list(summary = step_summary, eval = best_eval)

  message(sprintf(
    "Step %s selected: %s [%s] | %s = %.5f",
    step_i, best_row$variable, best_row$transformation, METRIC_TO_OPTIMIZE, best_row$optimization_score
  ))
}

if (length(best_step_results) == 0) stop("No ZINB model could be fit successfully.")

all_candidates <- rbindlist(search_log, fill = TRUE)
best_by_step <- rbindlist(lapply(best_step_results, function(x) x$summary), fill = TRUE)

best_global_idx <- which.min(best_by_step[[METRIC_TO_OPTIMIZE]])
best_global <- best_step_results[[best_global_idx]]

fwrite(all_candidates, file.path(OUTPUT_DIR, "zinb_all_candidates_by_step.csv"))
if (length(failure_log) > 0) {
  fwrite(rbindlist(failure_log, fill = TRUE), file.path(OUTPUT_DIR, "zinb_failed_candidates.csv"))
}
fwrite(best_by_step, file.path(OUTPUT_DIR, "zinb_best_model_per_step.csv"))
fwrite(best_global$eval$fold_metrics, file.path(OUTPUT_DIR, "zinb_best_global_fold_metrics.csv"))
fwrite(best_global$eval$overall, file.path(OUTPUT_DIR, "zinb_best_global_overall_metrics.csv"))
fwrite(best_global$eval$predictions, file.path(OUTPUT_DIR, "zinb_best_global_cv_predictions.csv"))

cat("Done. Files written to:", normalizePath(OUTPUT_DIR), "\n")
cat("Best global formula:\n")
print(best_global$eval$formula)
print(best_global$eval$overall)
