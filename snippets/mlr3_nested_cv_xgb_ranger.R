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
zinb_max_features <- length(features)
zinb_min_improvement <- 0
cores <- parallel::detectCores(logical = FALSE)
workers <- max(1L, min(4L, ifelse(is.na(cores), 2L, cores - 1L)))
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
  ranger = lrn("regr.ranger", predict_type = "response", num.threads = 1),
  xgb = lrn("regr.xgboost", predict_type = "response", objective = "reg:squarederror", nthread = 1)
)

check_zinb_package <- function() {
  if (!requireNamespace("pscl", quietly = TRUE)) stop("Package `pscl` is needed for ZINB.", call. = FALSE)
}

zinb_formula <- function(vars, zero = "1") {
  rhs <- if (length(vars) > 0) paste(vars, collapse = " + ") else "1"
  as.formula(sprintf("%s ~ %s | %s", target, rhs, zero))
}

fit_predict_zinb <- function(train_dt, test_dt, vars, zero = "1") {
  fit_warnings <- character()
  fit <- tryCatch(
    withCallingHandlers(
      pscl::zeroinfl(zinb_formula(vars, zero), data = train_dt, dist = "negbin", EM = TRUE),
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

score_zinb_vars <- function(work_dt, vars, folds, seed) {
  fold_id <- make_stratified_fold_id(work_dt$.stratum, folds, seed)
  pred <- rep(NA_real_, nrow(work_dt))
  for (fold in seq_len(folds)) {
    fit <- fit_predict_zinb(work_dt[fold_id != fold], work_dt[fold_id == fold], vars)
    if (is.null(fit)) return(Inf)
    pred[fold_id == fold] <- fit$pred
  }
  rmse(work_dt[[target]], pred)
}

select_zinb_features <- function(work_dt, pool, folds, seed) {
  selected <- character()
  best <- score_zinb_vars(work_dt, selected, folds, seed)
  while (length(selected) < min(zinb_max_features, length(pool))) {
    remaining <- setdiff(pool, selected)
    scores <- sapply(remaining, function(v) score_zinb_vars(work_dt, c(selected, v), folds, seed))
    if (!length(scores) || all(!is.finite(scores))) break
    pick <- names(which.min(scores))
    if (best - scores[[pick]] <= zinb_min_improvement) break
    selected <- c(selected, pick)
    best <- scores[[pick]]
  }
  selected
}

run_zinb_nested_cv <- function() {
  outer_fold_id <- make_stratified_fold_id(dt$.stratum, outer_folds, seed)
  rows <- vector("list", outer_folds)
  selected_rows <- vector("list", outer_folds)
  for (fold in seq_len(outer_folds)) {
    train_dt <- dt[outer_fold_id != fold]
    test_dt <- dt[outer_fold_id == fold]
    selected <- select_zinb_features(train_dt, features, inner_folds, seed + fold)
    fit <- fit_predict_zinb(train_dt, test_dt, selected)
    if (is.null(fit)) stop("ZINB outer fold ", fold, " failed.", call. = FALSE)
    rows[[fold]] <- data.table(row_id = which(outer_fold_id == fold), truth = test_dt[[target]], response = fit$pred)
    selected_rows[[fold]] <- data.table(outer_fold = fold, selected_features = paste(selected, collapse = ", "))
  }
  pred <- rbindlist(rows)
  score <- data.table(
    regr.rmse = rmse(pred$truth, pred$response),
    regr.mae = mean(abs(pred$response - pred$truth)),
    regr.rsq = 1 - sum((pred$response - pred$truth)^2) / sum((pred$truth - mean(pred$truth))^2)
  )
  list(score = score, predictions = pred, best_params = rbindlist(selected_rows))
}

if (run_zinb) check_zinb_package()

spaces <- list(
  ranger = ps(
    # num.trees = p_int(300, 1500),
    mtry = p_int(1, length(tasks$ranger$feature_names)),
    min.node.size = p_int(1, 25),
    sample.fraction = p_dbl(0.5, 1)
    # replace = p_lgl()
  ),
  xgb = ps(
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

comparison <- copy(scores)[order(regr.rmse)]
comparison[, rank_rmse := seq_len(.N)]
setcolorder(comparison, c("rank_rmse", "model", setdiff(names(comparison), c("rank_rmse", "model"))))
print(comparison)
