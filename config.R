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

    # Reproducibility and CV
    seed = 123L,
    n_folds = 10L,
    strata_bins = 10L,
    n_workers = 1L
  ),

  results = list(
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

    # If TRUE, convert character columns to factors before saving.
    chars_to_factors = TRUE,

    # Warn if a factor level appears fewer than this many times in the
    # processed dataset.
    factor_min_count = 5L
  ),

  ranger = list(
    output_dir = "outputs_ranger",
    tune_evals = 10L,
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
      max_depth = c(0L, 12L),

      # Candidate split rules for regression forests. "variance" is the
      # standard baseline; "extratrees" adds more randomness.
      splitrule = c("variance", "extratrees")
    )
  ),

  xgboost = list(
    output_dir = "outputs_xgb",
    tune_evals = 10L,
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
