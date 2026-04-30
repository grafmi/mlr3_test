# Central project configuration.
# Important experiment settings are intentionally placed near the top so they
# are easy to find and adjust during model comparisons.

CONFIG <- list(
  experiment = list(
    # Main data and prediction setup
    data_path = "testfile_zinb_nonlinear_eintritte.csv",
    target = "n_eintritte",
    feature_cols = c("prcrank", "potenzielle_kunden", "unfalldeckung"),
    # Columns listed here are kept out of modeling even if they exist in the
    # dataset. Use this for row IDs, customer IDs, policy numbers, or other
    # technical identifiers that should never become predictors.
    id_cols = character(0),
    # Optional row filter applied before the modeling scripts subset to
    # target + feature columns. This means the filter may reference helper
    # columns that are not listed in feature_cols and are never modeled.
    row_filter = "",

    # Reproducibility and CV
    seed = 123L,
    n_folds = 10L,
    # Number of repeated outer-CV runs for the mlr3 nested validation. Keep
    # this at 1 for the historical single-run behavior.
    outer_repeats = 1L,
    # Outer validation split strategy. Use "stratified" for the historical
    # target-stratified folds, or "year_blocked" to hold out one complete year
    # per outer fold. In year-blocked mode `outer_block_col` is used only for
    # splitting unless it is also listed in feature_cols.
    outer_resampling = "stratified",
    outer_block_col = "year",
    # Target scale for ranger and XGBoost. "count" keeps the original target.
    # "rate" trains on target / target_denominator_col, postprocesses
    # predictions back to the original count scale, and can use weight_col as
    # an observation weight. ZINB always keeps the original count target.
    target_mode = "count",
    target_denominator_col = "",
    weight_col = "",
    # Number of folds used in the inner tuning loop for mlr3 models. This can
    # be smaller than `n_folds` to speed up tuning while keeping the outer
    # validation loop unchanged.
    inner_folds = 5L,
    strata_bins = 10L,
    # Warn if modeling drops more than this fraction of rows via na.omit().
    # This is a guardrail for real data where missingness can silently shift
    # the modeled population. Set to NA to disable the warning.
    missing_drop_warn_fraction = 0.05,
    # Worker processes for mlr3 tuning and resampling. A practical default on
    # a workstation is often physical CPU cores minus one.
    n_workers = 1L
  ),

  results = list(
    run_name = "",
    root_dir = "results",
    version_runs = TRUE
  ),

  preprocess = list(
    # Where the derived dataset and its metadata are written.
    output_dir = "outputs_preprocessed",

    # Base file name for the processed dataset, for example
    # "preprocessed_dataset.csv" and "preprocessed_dataset.rds".
    output_name = "preprocessed_dataset",

    # Output formats for the processed dataset. Supported values are:
    # csv, tsv, txt, tab, rds, rda, RData
    output_formats = c("csv", "rds"),

    # Optional row filter expression evaluated on the dataset, for example:
    # "n_eintritte >= 1 & prcrank <= 8"
    filter = "",

    # Optional allowlist of columns to keep. Leave empty to keep all columns.
    keep_cols = character(0),

    # Optional list of columns to remove after loading.
    drop_cols = character(0),

    # If TRUE, remove rows with missing values after filtering and column
    # selection.
    drop_missing_rows = FALSE,

    # Optional random subset size applied after filtering and missing-value
    # removal. Leave as NA to keep all remaining rows.
    sample_rows = NA_integer_,

    # Random seed used for preprocess subsampling when sample_rows is set.
    sample_seed = 123L,

    # If TRUE, convert character columns to factors before saving.
    chars_to_factors = TRUE,

    # Warn if a factor level appears fewer than this many times in the
    # processed dataset.
    factor_min_count = 5L
  ),

  ranger = list(
    output_dir = "outputs_ranger",
    tune_evals = 10L,
    # Optional target transform for count-like outcomes. "log1p" trains ranger
    # on log1p(target) or log1p(rate) and writes predictions back on the
    # original count scale for comparison outputs.
    target_transform = "none",
    # Number of random-search configurations evaluated per tuning batch.
    # Higher values can better utilize many workers: roughly
    # tune_batch_size * inner_folds jobs can be active during tuning.
    tune_batch_size = 1L,
    # Search-space entries can be removed or commented out. If a supported
    # optional entry is missing here, it is simply not tuned.
    search_space = list(
      # Number of trees. More trees can stabilize performance, but cost more
      # runtime.
      num_trees = c(300L, 1500L),

      # Minimum terminal node size. Smaller values allow more flexible trees;
      # larger values regularize more strongly.
      min_node_size = c(1L, 25L),

      # Row subsampling fraction per tree.
      sample_fraction = c(0.5, 1.0),

      # Optional tree depth limit. Larger values allow more complex trees.
      max_depth = c(1L, 12L),

      # Candidate split rules for regression forests. "variance" is the
      # standard baseline; "extratrees" adds more randomness.
      splitrule = c("variance", "extratrees")
    )
  ),

  xgboost = list(
    output_dir = "outputs_xgb",
    tune_evals = 10L,
    # Base XGBoost objective. For a Poisson count model with exposure offset,
    # set this to "count:poisson" and set exposure_col below.
    objective = "reg:squarederror",
    # Optional XGBoost eval_metric passed to the learner, for example
    # "poisson-nloglik" with objective = "count:poisson".
    eval_metric = "",
    # Optional exposure column for Poisson count models. When set, XGBoost uses
    # log(exposure_col) as an offset/base margin and the column must not also
    # be listed in CONFIG$experiment$feature_cols.
    exposure_col = "",
    # Number of random-search configurations evaluated per tuning batch.
    # Higher values can better utilize many workers while keeping xgboost
    # model-level threads at 1.
    tune_batch_size = 1L,
    # Search-space entries can be removed or commented out. If a supported
    # optional entry is missing here, it is simply not tuned.
    search_space = list(
      # Number of boosting rounds.
      nrounds = c(50L, 800L),

      # Learning rate. Smaller values are usually safer but need more rounds.
      eta = c(0.01, 0.30),

      # Maximum tree depth.
      max_depth = c(2L, 8L),

      # Minimum Hessian weight in a child node; larger values regularize more.
      min_child_weight = c(1.0, 15.0),

      # Row subsampling fraction per boosting round.
      subsample = c(0.5, 1.0),

      # Column subsampling fraction per tree.
      colsample_bytree = c(0.4, 1.0),

      # L2 regularization.
      lambda = c(0.0, 10.0),

      # L1 regularization.
      alpha = c(0.0, 10.0),

      # Minimum loss reduction needed for an extra split. Higher values make
      # trees more conservative.
      gamma = c(0.0, 5.0)
    )
  ),

  zinb = list(
    # Where all ZINB CV results, candidate tables, metrics, and logs are
    # written.
    output_dir = "outputs_zinb",

    # Optional reduced predictor set for ZINB only. Leave empty to reuse
    # CONFIG$experiment$feature_cols. This is useful when the full ranger/XGB
    # feature set makes ZINB stepwise selection too slow on real data.
    feature_cols = character(0),

    # Metric used to decide which candidate is best during forward selection.
    # Supported values include rmse, mae, max_error, mse, r2,
    # poisson_deviance, and negloglik. Keep rmse as the default if you want
    # the most straightforward model-agnostic ranking.
    metric = "rmse",

    # Maximum number of variables that may be selected into the final model.
    # `Inf` means that all variables in `feature_cols` may be considered.
    max_vars = Inf,

    # Minimum improvement required before a new step is accepted.
    min_improvement = 0.0,

    # Worker processes for parallel candidate evaluation on Linux. A practical
    # default on a workstation is often physical CPU cores minus one.
    workers = 16L,

    # Logging detail for ZINB runs. Use "batch" for compact progress logs,
    # "detailed" for fold- and retry-level fit tracing, or "quiet" to reduce
    # output to the main high-level milestones.
    verbosity = "batch",

    # Parallel backend for ZINB candidate evaluation.
    # "psock" is the safer default across mixed environments.
    # "fork" can be faster on some Linux systems, but has shown hangs in
    # certain external batch/runtime setups. "sequential" disables
    # parallelism for debugging or stability checks.
    parallel_backend = "psock",

    # Allowed transformations for numeric predictors during forward selection.
    transformations_numeric = c("raw", "sqrt", "log1p", "ns2", "poly2", "factor"),

    # Allowed transformations for factor predictors. In the current setup,
    # factors are only used in raw form.
    transformations_factor = c("raw"),

    # Numeric predictors with at most this many observed values are also
    # evaluated as factor-like candidates via factor(var). This is useful for
    # binned or class-coded numeric variables such as age classes.
    numeric_as_factor_max_levels = 12L,

    # Optional explicit allowlist of numeric feature names that should also be
    # evaluated as factor(var) candidates, even if they exceed the global
    # numeric_as_factor_max_levels threshold.
    numeric_as_factor_vars = character(0),

    # Formula right-hand side for the zero-inflation part. Use "1" for an
    # intercept-only zero part, or set an explicit formula such as
    # "prcrank + log1p(potenzielle_kunden)". The special value
    # "same_as_count" reuses the currently selected count-model terms.
    zero_inflation_formula = "1"
  ),

  comparison = list(
    output_dir = "outputs_model_comparison",
    # Metric used to rank models in the comparison output. rmse stays the
    # default because it is available across all model families in this repo.
    metric = "rmse",
    ranger_dir = "outputs_ranger",
    xgb_dir = "outputs_xgb",
    zinb_dir = "outputs_zinb"
  ),

  validation = list(
    # Output directory for repository smoke checks.
    output_dir = "outputs_validation"
  )
)
