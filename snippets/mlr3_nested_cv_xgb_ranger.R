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
  library(R6)
})

# ---- minimale Anpassungen ----------------------------------------------------
target <- "n_eintritte"
features <- c("prcrank", "potenzielle_kunden", "unfalldeckung")
seed <- 123
outer_folds <- 10
inner_folds <- 5
tune_evals <- 10
strata_bins <- 10
measures <- msrs(c("regr.rmse", "regr.mae", "regr.rsq"))

if (!exists("dt", inherits = FALSE)) {
  if (!exists("df", inherits = FALSE)) stop("Please define a data.frame/data.table named `dt` or `df` first.", call. = FALSE)
  dt <- as.data.table(df)
}
dt <- na.omit(as.data.table(dt)[, c(target, features), with = FALSE])
dt[, .stratum := cut(rank(get(target), ties.method = "average"), breaks = strata_bins, include.lowest = TRUE)]
set.seed(seed)

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

make_zinb_learner <- function(zero = "1") {
  if (!requireNamespace("pscl", quietly = TRUE)) stop("Package `pscl` is needed for ZINB.", call. = FALSE)
  R6Class("LearnerRegrZINB", inherit = LearnerRegr,
    public = list(initialize = function() {
      super$initialize(
        id = "regr.zinb",
        feature_types = c("integer", "numeric", "factor", "ordered", "character"),
        predict_types = "response",
        param_set = ps(zero = p_uty(default = zero, tags = "train")),
        packages = "pscl"
      )
    }),
    private = list(
      .train = function(task) {
        d <- task$data()
        y <- task$target_names
        x <- setdiff(task$feature_names, ".stratum")
        z <- self$param_set$values$zero
        if (is.null(z)) z <- zero
        form <- as.formula(sprintf("%s ~ %s | %s", y, paste(x, collapse = " + "), z))
        pscl::zeroinfl(form, data = d, dist = "negbin", EM = TRUE)
      },
      .predict = function(task) {
        PredictionRegr$new(
          task = task,
          response = as.numeric(predict(self$model, newdata = task$data(), type = "response"))
        )
      }
    )
  )$new()
}

# Optional, falls ZINB mitlaufen soll:
# tasks$zinb <- TaskRegr$new("zinb", backend = dt, target = target)
# tasks$zinb$set_col_roles(".stratum", roles = "stratum")
# learners$zinb <- make_zinb_learner(zero = "1")

spaces <- list(
  ranger = ps(
    num.trees = p_int(300, 1500),
    mtry = p_int(1, length(tasks$ranger$feature_names)),
    min.node.size = p_int(1, 25),
    sample.fraction = p_dbl(0.5, 1),
    replace = p_lgl()
  ),
  xgb = ps(
    nrounds = p_int(50, 800),
    eta = p_dbl(0.01, 0.30),
    max_depth = p_int(2, 8),
    min_child_weight = p_dbl(1, 15),
    subsample = p_dbl(0.5, 1),
    colsample_bytree = p_dbl(0.4, 1),
    lambda = p_dbl(0, 10),
    alpha = p_dbl(0, 10)
  )
)

# ---- nested CV ---------------------------------------------------------------
results <- lapply(names(tasks), function(model) {
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

  set.seed(seed) # gleiche aeussere CV-Splits fuer beide Modelle
  rr <- resample(tasks[[model]], learner, rsmp("cv", folds = outer_folds), store_models = TRUE)
  pred <- rr$prediction()
  list(
    score = as.data.table(as.list(rr$aggregate(measures))),
    predictions = data.table(row_id = pred$row_ids, truth = pred$truth, response = pred$response),
    best_params = rbindlist(lapply(seq_along(rr$learners), function(i) {
      data.table(outer_fold = i, if (!is.null(space)) as.data.table(rr$learners[[i]]$tuning_result))
    }), fill = TRUE)
  )
})
names(results) <- names(tasks)

scores <- rbindlist(lapply(names(results), function(m) cbind(model = m, results[[m]]$score)), fill = TRUE)
print(scores)
