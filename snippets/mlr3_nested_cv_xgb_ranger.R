#!/usr/bin/env Rscript

# Data-Scientist-Style: nested CV fuer ranger/xgboost, optional auch ZINB.
# Erwartet ein data.frame/data.table namens `dt` oder `df` im Environment.

suppressPackageStartupMessages({
  library(data.table)
  library(mlr3)
  library(mlr3learners)
  library(mlr3tuning)
  library(paradox)
  library(bbotk)
  library(future)
})

# ---- minimale Anpassungen ----------------------------------------------------
target <- "n_eintritte"
features <- c("prcrank", "potenzielle_kunden", "unfalldeckung")
seed <- 123
outer_folds <- 10
inner_folds <- 5
tune_evals <- 10
strata_bins <- 10
run_zinb <- FALSE
# ZINB waehlt optional Count-Model-Terme per innerer Forward Selection.
zinb_max_features <- length(features)
zinb_min_improvement <- 0
zinb_numeric_transforms <- c("raw", "sqrt", "log1p", "ns2", "factor")
zinb_factor_numeric_max_levels <- 12L
zinb_zero <- "1"
cores <- parallel::detectCores(logical = FALSE)
workers <- max(1L, min(4L, ifelse(is.na(cores), 2L, cores - 1L)))
learner_threads <- 1L # Bei tune_evals=1 ggf. workers <- 1 und learner_threads > 1 setzen.
measures <- msrs(c("regr.rmse", "regr.mae", "regr.rsq"))

future::plan(future::multisession, workers = workers) # RStudio-freundlich; Learner-Threads bleiben unten bei 1.
on.exit(future::plan(future::sequential), add = TRUE)
message("Nested CV setup: outer=", outer_folds, ", inner=", inner_folds, ", tune_evals=", tune_evals, ", workers=", workers)

if (!exists("dt", inherits = FALSE)) {
  if (!exists("df", inherits = FALSE)) stop("Please define a data.frame/data.table named `dt` or `df` first.", call. = FALSE)
  dt <- as.data.table(df)
}
dt <- na.omit(as.data.table(dt)[, c(target, features), with = FALSE])
dt[, .stratum := cut(rank(get(target), ties.method = "average"), breaks = strata_bins, include.lowest = TRUE)]
set.seed(seed)

# Zielbasierte Stratifizierung: haelt die Verteilung von `target` in den Folds aehnlicher.
make_stratified_fold_id <- function(strata, folds, seed) {
  set.seed(seed)
  fold_id <- integer(length(strata))
  offset <- 0L
  for (ids in split(seq_along(strata), strata, drop = TRUE)) {
    ids <- sample(ids)
    fold_id[ids] <- ((seq_along(ids) + offset - 1L) %% folds) + 1L
    offset <- offset + length(ids)
  }
  fold_id
}

make_stratified_outer_cv <- function(task, strata, folds, seed) {
  if (length(strata) != task$nrow) stop("Strata length must match task rows.", call. = FALSE)
  if (task$nrow < folds) stop("Need at least as many rows as outer folds.", call. = FALSE)
  fold_id <- make_stratified_fold_id(strata, folds, seed)
  test_sets <- lapply(seq_len(folds), function(k) which(fold_id == k))
  train_sets <- lapply(test_sets, function(test) setdiff(seq_len(task$nrow), test))
  outer_cv <- rsmp("custom")
  outer_cv$instantiate(task, train_sets = train_sets, test_sets = test_sets)
  outer_cv
}

# xgboost braucht numerische Features; ranger kann Faktoren direkt verwenden.
xgb_dt <- data.table(dt[, c(target, ".stratum"), with = FALSE], as.data.table(model.matrix(reformulate(features, intercept = FALSE), dt)))
tasks <- list(
  ranger = TaskRegr$new("ranger", backend = dt, target = target),
  xgb = TaskRegr$new("xgb", backend = xgb_dt, target = target)
)
for (task in tasks) task$set_col_roles(".stratum", roles = "stratum")

learners <- list(
  ranger = lrn("regr.ranger", predict_type = "response", num.threads = learner_threads),
  xgb = lrn("regr.xgboost", predict_type = "response", objective = "reg:squarederror", nthread = learner_threads)
)

check_zinb_package <- function() {
  if (!requireNamespace("pscl", quietly = TRUE)) stop("Package `pscl` is needed for ZINB.", call. = FALSE)
}

# Aus Feature-Namen werden ZINB-Term-Kandidaten; pro Originalvariable wird spaeter max. ein Term gewaehlt.
make_zinb_candidates <- function(work_dt, pool) {
  rbindlist(lapply(pool, function(v) {
    x <- work_dt[[v]]
    if (!is.numeric(x)) return(data.table(var = v, transform = "raw", term = v))
    out <- list(data.table(var = v, transform = "raw", term = v))
    if ("sqrt" %in% zinb_numeric_transforms && all(x >= 0, na.rm = TRUE)) {
      out[[length(out) + 1L]] <- data.table(var = v, transform = "sqrt", term = sprintf("sqrt(%s)", v))
    }
    if ("log1p" %in% zinb_numeric_transforms && all(x > -1, na.rm = TRUE)) {
      out[[length(out) + 1L]] <- data.table(var = v, transform = "log1p", term = sprintf("log1p(%s)", v))
    }
    if ("ns2" %in% zinb_numeric_transforms && length(unique(x)) >= 4L) {
      out[[length(out) + 1L]] <- data.table(var = v, transform = "ns2", term = sprintf("splines::ns(%s, df = 2)", v))
    }
    if ("factor" %in% zinb_numeric_transforms && length(unique(x)) <= zinb_factor_numeric_max_levels) {
      out[[length(out) + 1L]] <- data.table(var = v, transform = "factor", term = sprintf("factor(%s)", v))
    }
    rbindlist(out)
  }))
}

# ZINB nutzt Standard-Fit-Parameter; optimiert wird hier nur die Count-Model-Formel.
zinb_formula <- function(terms, zero = "1") {
  rhs <- if (length(terms) > 0) paste(terms, collapse = " + ") else "1"
  as.formula(sprintf("%s ~ %s | %s", target, rhs, zero))
}

# Rueckgabe NULL bedeutet: Fit oder Prediction war unbrauchbar, Kandidat wird verworfen.
fit_predict_zinb <- function(train_dt, test_dt, terms, zero = "1") {
  fit_warnings <- character()
  fit <- tryCatch(
    withCallingHandlers(
      pscl::zeroinfl(zinb_formula(terms, zero), data = train_dt, dist = "negbin", EM = TRUE),
      warning = function(w) {
        fit_warnings <<- c(fit_warnings, conditionMessage(w))
        invokeRestart("muffleWarning")
      }
    ),
    error = function(e) NULL
  )
  if (is.null(fit)) return(NULL)
  pred <- tryCatch(as.numeric(predict(fit, newdata = test_dt, type = "response")), error = function(e) NULL)
  if (is.null(pred) || length(pred) != nrow(test_dt) || anyNA(pred) || any(!is.finite(pred))) return(NULL)
  list(pred = pred, warnings = unique(fit_warnings))
}

rmse <- function(truth, pred) sqrt(mean((pred - truth)^2))
r2_score <- function(truth, pred) {
  sst <- sum((truth - mean(truth))^2)
  if (isTRUE(all.equal(sst, 0))) return(NA_real_)
  1 - sum((pred - truth)^2) / sst
}

score_zinb_terms <- function(work_dt, terms, folds, seed, zero = zinb_zero) {
  fold_id <- make_stratified_fold_id(work_dt$.stratum, folds, seed)
  pred <- rep(NA_real_, nrow(work_dt))
  for (fold in seq_len(folds)) {
    fit <- fit_predict_zinb(work_dt[fold_id != fold], work_dt[fold_id == fold], terms, zero = zero)
    if (is.null(fit)) return(Inf)
    pred[fold_id == fold] <- fit$pred
  }
  rmse(work_dt[[target]], pred)
}

select_zinb_terms <- function(work_dt, pool, folds, seed, zero = zinb_zero) {
  candidates <- make_zinb_candidates(work_dt, pool)
  selected_terms <- character()
  selected_vars <- character()
  best <- score_zinb_terms(work_dt, selected_terms, folds, seed, zero = zero)
  # Greedy Forward Selection: fuege den Term hinzu, der den inner-CV-RMSE am staerksten senkt.
  while (length(selected_vars) < min(zinb_max_features, length(pool))) {
    remaining <- candidates[!var %in% selected_vars]
    scores <- sapply(seq_len(nrow(remaining)), function(i) {
      score_zinb_terms(work_dt, c(selected_terms, remaining$term[[i]]), folds, seed, zero = zero)
    })
    if (!length(scores) || all(!is.finite(scores))) break
    pick <- which.min(scores)
    if (best - scores[[pick]] <= zinb_min_improvement) break
    selected_terms <- c(selected_terms, remaining$term[[pick]])
    selected_vars <- c(selected_vars, remaining$var[[pick]])
    best <- scores[[pick]]
  }
  list(terms = selected_terms, vars = selected_vars)
}

# ZINB bleibt nested: Term-Auswahl nur im Outer-Train, Bewertung nur im Outer-Test.
run_zinb_nested_cv <- function() {
  outer_fold_id <- make_stratified_fold_id(dt$.stratum, outer_folds, seed)
  rows <- vector("list", outer_folds)
  selected_rows <- vector("list", outer_folds)
  for (fold in seq_len(outer_folds)) {
    message("  ZINB outer fold ", fold, "/", outer_folds)
    train_dt <- dt[outer_fold_id != fold]
    test_dt <- dt[outer_fold_id == fold]
    selected <- select_zinb_terms(train_dt, features, inner_folds, seed + fold, zero = zinb_zero)
    fit <- fit_predict_zinb(train_dt, test_dt, selected$terms, zero = zinb_zero)
    if (is.null(fit)) stop("ZINB outer fold ", fold, " failed.", call. = FALSE)
    rows[[fold]] <- data.table(row_id = which(outer_fold_id == fold), truth = test_dt[[target]], response = fit$pred)
    selected_rows[[fold]] <- data.table(
      outer_fold = fold,
      selected_features = paste(selected$vars, collapse = ", "),
      selected_terms = paste(selected$terms, collapse = " + ")
    )
  }
  pred <- rbindlist(rows)
  score <- data.table(
    regr.rmse = rmse(pred$truth, pred$response),
    regr.mae = mean(abs(pred$response - pred$truth)),
    regr.rsq = r2_score(pred$truth, pred$response)
  )
  list(score = score, predictions = pred, best_params = rbindlist(selected_rows))
}

if (run_zinb) check_zinb_package()

spaces <- list(
  ranger = ps(
    # Schlanker Search Space: zentrale Hebel aktiv, weniger zentrale Parameter bleiben Defaults.
    # num.trees = p_int(300, 1500),
    mtry = p_int(1, length(tasks$ranger$feature_names)),
    min.node.size = p_int(1, 25),
    sample.fraction = p_dbl(0.5, 1)
    # replace = p_lgl()
  ),
  xgb = ps(
    # XGBoost: Komplexitaet, Lernrate und Sampling tunen; Regularisierung bleibt Default.
    nrounds = p_int(50, 800),
    eta = p_dbl(0.01, 0.30),
    max_depth = p_int(2, 8),
    min_child_weight = p_dbl(1, 15),
    subsample = p_dbl(0.5, 1),
    colsample_bytree = p_dbl(0.4, 1)
    # lambda = p_dbl(0, 10),
    # alpha = p_dbl(0, 10),
    # gamma = p_dbl(0, 5)
  )
)

# ---- nested CV ---------------------------------------------------------------
model_names <- c(names(tasks), if (run_zinb) "zinb")
results <- lapply(model_names, function(model) {
  message("Starting ", model, " nested CV...")
  if (model == "zinb") {
    out <- run_zinb_nested_cv()
    message("Finished ", model, ".")
    return(out)
  }

  learner <- learners[[model]]
  space <- spaces[[model]]
  if (!is.null(space)) {
    # Inner CV waehlt Hyperparameter; Outer CV bewertet den getunten Workflow.
    learner <- auto_tuner(
      learner = learner,
      resampling = rsmp("cv", folds = inner_folds),
      measure = msr("regr.rmse"),
      tuner = tnr("random_search"),
      search_space = space,
      terminator = trm("evals", n_evals = tune_evals),
      store_tuning_instance = TRUE
    )
  }

  # Explizite Outer-Folds machen den Modellvergleich kontrolliert und reproduzierbar.
  outer_cv <- make_stratified_outer_cv(tasks[[model]], strata = dt$.stratum, folds = outer_folds, seed = seed)
  rr <- resample(tasks[[model]], learner, outer_cv, store_models = TRUE)
  pred <- rr$prediction()
  message("Finished ", model, ".")
  list(
    score = as.data.table(as.list(rr$aggregate(measures))),
    predictions = data.table(row_id = pred$row_ids, truth = pred$truth, response = pred$response),
    best_params = rbindlist(lapply(seq_along(rr$learners), function(i) {
      data.table(outer_fold = i, if (!is.null(space)) as.data.table(rr$learners[[i]]$tuning_result))
    }), fill = TRUE)
  )
})
names(results) <- model_names

scores <- rbindlist(lapply(names(results), function(m) cbind(model = m, results[[m]]$score)), fill = TRUE)
print(scores)

# Kleiner Modellvergleich: niedrigster Outer-CV-RMSE gewinnt.
comparison <- copy(scores)[order(regr.rmse)]
comparison[, rank_rmse := seq_len(.N)]
setcolorder(comparison, c("rank_rmse", "model", setdiff(names(comparison), c("rank_rmse", "model"))))
print(comparison)
