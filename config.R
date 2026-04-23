# Central project configuration.
# Important experiment settings are intentionally placed near the top so they
# are easy to find and adjust during model comparisons.

CONFIG <- list(
  experiment = list(
    # Main data and prediction setup
    data_path = "testfile_zinb_nonlinear_eintritte.csv",
    target = "n_eintritte",
    feature_cols = c("prcrank", "potenzielle_kunden", "unfalldeckung"),
    id_cols = character(0),

    # Reproducibility and CV
    seed = 123L,
    n_folds = 10L,
    strata_bins = 10L,
    n_workers = 1L
  ),

  preprocess = list(
    output_dir = "outputs_preprocessed",
    output_name = "preprocessed_dataset",
    output_formats = c("csv", "rds"),
    filter = "",
    keep_cols = character(0),
    drop_cols = character(0),
    drop_missing_rows = FALSE,
    chars_to_factors = TRUE,
    factor_min_count = 5L
  ),

  ranger = list(
    output_dir = "outputs_ranger",
    tune_evals = 10L,
    search_space = list(
      num_trees = c(300L, 1500L),
      min_node_size = c(1L, 25L),
      sample_fraction = c(0.5, 1.0)
    )
  ),

  xgboost = list(
    output_dir = "outputs_xgb",
    tune_evals = 10L,
    search_space = list(
      nrounds = c(50L, 800L),
      eta = c(0.01, 0.30),
      max_depth = c(2L, 8L),
      min_child_weight = c(1.0, 15.0),
      subsample = c(0.5, 1.0),
      colsample_bytree = c(0.4, 1.0),
      lambda = c(0.0, 10.0),
      alpha = c(0.0, 10.0)
    )
  ),

  zinb = list(
    output_dir = "outputs_zinb",
    metric = "rmse",
    max_vars = Inf,
    min_improvement = 0.0,
    workers = 16L,
    transformations_numeric = c("raw", "sqrt", "log1p", "ns2", "poly2"),
    transformations_factor = c("raw"),
    same_zero_formula = FALSE
  ),

  comparison = list(
    output_dir = "outputs_model_comparison",
    metric = "rmse",
    ranger_dir = "outputs_ranger",
    xgb_dir = "outputs_xgb",
    zinb_dir = "outputs_zinb"
  )
)
