# mlr3 Count Regression Experiments

This repository contains small R experiment scripts for predicting `n_eintritte`
from a synthetic zero-inflated count dataset.

The count nature of the target is handled explicitly by the ZINB experiment. The
`ranger` and XGBoost experiments intentionally treat the target as a regression
problem for now.

Most experiment defaults now live in `configs/base_config.R`. The scripts use those values
as readable defaults and still allow explicit overrides via command-line
arguments and environment variables.

## Quick Start

For most users, these are the main steps:

1. Adjust the dataset path and core experiment settings in `configs/base_config.R`
2. Optionally run `Rscript validate_repo.R` for a quick smoke check
3. Optionally run `Rscript run_regression_tests.R` for lightweight regression tests
4. Run the full pipeline on Linux with `./run_all.sh`
5. Inspect the versioned output folder under `results/YYYYMMDD_HHMM/`

Typical example:

```r
CONFIG$experiment$data_path <- "/absolute/path/to/my_dataset.csv"
CONFIG$experiment$feature_cols <- c("prcrank", "potenzielle_kunden", "unfalldeckung")
  CONFIG$experiment$seed <- 123L
  CONFIG$experiment$n_folds <- 10L
  CONFIG$experiment$inner_folds <- 5L
  CONFIG$experiment$missing_drop_warn_fraction <- 0.05
```

Then run:

```sh
Rscript validate_repo.R
Rscript run_regression_tests.R
./run_all.sh
```

The most relevant outputs per script are:

- metrics as `.csv` and `.rds`
- predictions as `.csv` and `.rds`
- human-readable reports as `.md` or `.txt`
- logs as `*.log`
- run metadata as `run_manifest.csv` and `run_manifest.rds`

## What The Scripts Do

- `preprocess_data.R`: load data from common R and tabular formats, apply light preprocessing, and write dataset metadata
- `mlr3_ranger_tuning.R`: nested CV for `ranger` regression with hyperparameter tuning, optionally repeated across multiple outer-CV draws
- `mlr3_xgb_tuning.R`: nested CV for `xgboost` regression with hyperparameter tuning, optionally repeated across multiple outer-CV draws
- `zinb_stepwise_cv.R`: nested-style ZINB selection with inner forward selection and outer CV reporting
- `compare_best_models.R`: read the best model outputs and create a ranked comparison
- `validate_repo.R`: quick repository smoke check for packages, config, data, and output paths
- `run_regression_tests.R`: lightweight regression suite for key failure and reporting paths

The training and comparison scripts now also write small transparency artifacts
such as config snapshots, model reports, dataset overviews, and summary files.

## Validation Design

The validation logic is intentionally different for the mlr3 models and the
ZINB model, but both are designed to give an honest out-of-sample estimate.

### Outer Loop

The outer loop is the main validation loop. Its job is to estimate how well the
modeling procedure performs on unseen data.

For the mlr3 scripts:
- the data is split into stratified outer folds
- for each outer fold, one part is held out as validation data
- everything else is used for model tuning and fitting
- the final reported CV predictions and metrics come from these held-out outer folds

This is the important protection against overly optimistic validation results.

### Inner Loop

The inner loop exists only in the mlr3 tuning scripts.

Its job is not to report final performance, but to choose hyperparameters such
as tree depth, learning rate, or node size using only the current outer-loop
training data.

That means:
- inner loop: select hyperparameters
- outer loop: estimate generalization performance

This is called nested cross-validation.

Why it matters:
- without an inner loop, tuned models often look better than they really are
- nested CV keeps tuning and evaluation separated
- that makes comparisons between `ranger` and `xgboost` much more trustworthy

### ZINB Stepwise CV

The ZINB script now uses a two-level validation design so its reported metrics
are more comparable to the nested mlr3 models:

- outer folds are used only for out-of-sample evaluation
- inside each outer-training split, a forward-selection search chooses the ZINB
  count-model terms by inner CV performance
- one original variable may be selected once, in one transformation, per inner
  selection path
- the reported `zinb_best_global_*` metrics and predictions come from the outer
  folds
- after evaluation, the selected full-data formula is refit once on the full
  dataset to produce interpretable coefficient tables and the final model
  summary

This means the ZINB ranking metrics are no longer taken from the same CV loop
that selected the formula, which makes comparisons with `ranger` and
`xgboost` materially fairer.

## Configuration

The main place to edit experiment settings is:

```text
configs/base_config.R
```

The file is grouped so that the most important settings appear first:

- `CONFIG$experiment`: data path, target, features, optional row filter, seed,
  folds, workers
- `CONFIG$results`: optional run naming plus results-root behavior
- `CONFIG$experiment$inner_folds`: folds used in the inner tuning loop for
  `ranger` and `xgboost`
- `CONFIG$experiment$outer_repeats`: optional number of repeated outer-CV runs
  for `ranger` and `xgboost`; missing values default to `1`
- `CONFIG$experiment$target_mode`: optional `ranger`/XGBoost target scale;
  `"count"` keeps the original target, while `"rate"` trains on
  `target / target_denominator_col`, optionally using `weight_col`
- `CONFIG$experiment$missing_drop_warn_fraction`: warn when modeling drops more
  than this fraction of rows with missing values; set to `NA` to disable
- `CONFIG$preprocess`: preprocessing defaults and factor handling
- `CONFIG$ranger`: ranger output directory, tuning defaults, and random-search
  batch size
- `CONFIG$xgboost`: xgboost output directory, tuning defaults, and random-search
  batch size
- `CONFIG$zinb`: optional ZINB-only feature set, output directory, metric,
  transformations, workers, parallel backend, numeric-as-factor controls, and
  zero-inflation formula
- `CONFIG$comparison`: model-comparison defaults

The scripts use this precedence order:

1. command-line argument
2. environment variable
3. the selected config file, defaulting to `configs/base_config.R`

You can also set an explicit data path directly in `configs/base_config.R`, which is often
the most convenient option for day-to-day experimentation:

```r
CONFIG$experiment$data_path <- "/absolute/path/to/my_dataset.csv"
```

Absolute paths, repo-relative paths, and `~/...` paths are supported.

If you want a human-readable label across manifests and run summaries, set
`CONFIG$results$run_name`, for example:

```r
CONFIG$results$run_name <- "age-band-debug-run"
```

`CONFIG$experiment$id_cols` can be used for identifier columns that exist in the
dataset but must not be used as predictors, for example customer IDs, policy
numbers, or technical row IDs.

`CONFIG$experiment$row_filter` is applied before the modeling scripts subset the
dataset to `target` and `feature_cols`. This allows row filtering on helper
columns that are present in the dataset but are not modeled as predictors.

For `CONFIG$ranger$search_space` and `CONFIG$xgboost$search_space`, supported
optional parameters can also be removed or commented out. If a known optional
entry is missing in the selected config file, that parameter is simply not
tuned in the corresponding script. XGBoost also supports `objective` as a
categorical search-space entry when no exposure offset is configured.

If you want to run a named variant, pass `--config=/path/to/configs/my_variant.R`
or set `CONFIG_PATH=/path/to/configs/my_variant.R`. Variant config files should
define a `CONFIG` object just like `configs/base_config.R`.

For ZINB, `CONFIG$zinb$zero_inflation_formula` controls the right-hand side of
the zero-inflation part. Examples:

```r
zero_inflation_formula = "1"
zero_inflation_formula = "same_as_count"
zero_inflation_formula = "prcrank + log1p(potenzielle_kunden)"
```

For faster ZINB runs on real data, `CONFIG$zinb$feature_cols` can define a
reduced ZINB-only predictor set while ranger and XGBoost continue to use
`CONFIG$experiment$feature_cols`:

```r
CONFIG$experiment$feature_cols <- c("prcrank", "potenzielle_kunden", "unfalldeckung", "region")
CONFIG$zinb$feature_cols <- c("prcrank", "potenzielle_kunden")
```

Leave `CONFIG$zinb$feature_cols` empty to reuse the global experiment feature
set. Command-line `--features` and environment variable `FEATURE_COLS` still
override the ZINB feature set for direct `zinb_stepwise_cv.R` runs.

Before the ZINB cross-validation starts, the script now validates the
zero-inflation formula and all configured feature transformations against the
modeling data. That catches missing columns and invalid formula terms earlier,
before a longer CV run starts.

For ZINB, numeric predictors can also be tested as categorical candidates via
`factor(var)`. By default this is enabled only for numeric variables with at
most `CONFIG$zinb$numeric_as_factor_max_levels` observed values, which works
well for binned features such as age classes. You can additionally force
specific numeric variables into this check via
`CONFIG$zinb$numeric_as_factor_vars`.

For count-aware evaluation, the scripts now also report:

- `poisson_deviance`: useful as a count-focused error measure across models
- `negloglik`: available for ZINB and based on the fitted ZINB probability model

`rmse` remains the default ranking metric because it stays easy to interpret and
works consistently across all model families in this repository.

## Running

Typical usage from the repository root:

```sh
Rscript preprocess_data.R
Rscript validate_repo.R
Rscript run_regression_tests.R
Rscript mlr3_ranger_tuning.R
Rscript mlr3_xgb_tuning.R
Rscript zinb_stepwise_cv.R
Rscript compare_best_models.R
```

For Linux, you can also run the complete pipeline with:

```sh
./run_all.sh
```

The wrapper resolves paths relative to the repository, checks that `Rscript` and
the input data exist, runs `validate_repo.R` as a preflight check, then runs the
modeling and comparison scripts in sequence.
At the end of a successful full run it also writes a small run-level summary in
the run root directory, including a run report, config snapshot, and registry
entry under the results root.

ZINB can be skipped for faster real-data runs while still using the same
wrapper:

```sh
RUN_ZINB=false ./run_all.sh
```

In that mode, `run_all.sh` fits ranger and XGBoost, writes a skipped ZINB
manifest under `outputs_zinb_skipped`, and still runs the model comparison and
run summary. The comparison marks ZINB as skipped and excludes it from ranking,
so stale ZINB results from an earlier run are not reused accidentally.

For real data, two additional guardrails are logged during validation and
modeling:

- if `na.omit()` drops more than `missing_drop_warn_fraction` of modeling rows,
  the scripts warn that missingness may have shifted the modeled population
- if `id_cols` are configured and repeated identifier combinations are found,
  the scripts warn that ordinary row-wise CV may leak grouped entities and that
  grouped CV or aggregation should be considered
- `validate_repo.R` also warns about very small validation folds, constant or
  near-constant predictors, and target extremes such as zero-heavy or high-maximum
  target distributions
- validation writes `resolved_config`, `validation_checks.csv`,
  `validation_data_dictionary.csv`, and `validation_report.md`
- every finalized script run writes `session_info.txt` with the R runtime and
  package environment used for that run

By default, `run_all.sh` versions each full run under a timestamped directory:

```text
results/YYYYMMDD_HHMM/
```

If a run folder with the same timestamp already exists, the wrapper adds a
suffix such as `_01` automatically.

For example:

```text
results/20260423_2055/outputs_ranger/
results/20260423_2055/outputs_xgb/
results/20260423_2055/outputs_zinb/
results/20260423_2055/outputs_model_comparison/
```

Useful overrides:

```sh
./run_all.sh
VERSION_RUNS=false ./run_all.sh
RUN_ID=baseline_a ./run_all.sh
RESULTS_ROOT_DIR=/tmp/my_results ./run_all.sh
CONFIG_PATH=configs/my_variant.R ./run_all.sh
RUN_ZINB=false ./run_all.sh
```

You can also run a lightweight preflight check before a longer experiment:

```sh
Rscript validate_repo.R
```

And you can run the lightweight regression suite after code changes:

```sh
Rscript run_regression_tests.R
```

The scripts also work when sourced interactively from VS Code, RStudio Pro, or
Positron, as long as `experiment_utils.R` is either next to the script or in the
current working directory.

Common command-line overrides:

```sh
Rscript preprocess_data.R --input=/path/to/data.rds --formats=csv,rds --filter="n_eintritte >= 0"
Rscript preprocess_data.R --keep-cols=n_eintritte,prcrank --drop-missing-rows=true
Rscript preprocess_data.R --sample-rows=500 --sample-seed=123
Rscript preprocess_data.R --chars-to-factors=true --factor-min-count=5
Rscript mlr3_ranger_tuning.R --row-filter="split == 'train'" --features=prcrank,potenzielle_kunden
Rscript mlr3_ranger_tuning.R --data=/path/to/data.csv --folds=10 --inner-folds=5 --tune-evals=20 --tune-batch-size=4 --workers=16
Rscript mlr3_ranger_tuning.R --outer-repeats=3 --folds=5 --inner-folds=3
Rscript mlr3_ranger_tuning.R --outer-resampling=year_blocked --outer-block-col=year
Rscript mlr3_ranger_tuning.R --target-mode=rate --target-denominator-col=potenzielle_kunden --weight-col=potenzielle_kunden
Rscript mlr3_xgb_tuning.R --output-dir=outputs_xgb_custom --tune-batch-size=4
Rscript zinb_stepwise_cv.R --metric=poisson_deviance --max-vars=3 --workers=4
Rscript zinb_stepwise_cv.R --features=prcrank,potenzielle_kunden
Rscript zinb_stepwise_cv.R --verbosity=detailed
Rscript zinb_stepwise_cv.R --numeric-as-factor-max-levels=8
Rscript zinb_stepwise_cv.R --numeric-as-factor-vars=age_band_num,tariff_class_num
Rscript compare_best_models.R --metric=rmse
```

The same settings can be controlled with environment variables, for example
`INPUT_DATA_PATH`, `PREPROCESS_OUTPUT_DIR`, `PREPROCESS_OUTPUT_NAME`,
`PREPROCESS_OUTPUT_FORMATS`, `PREPROCESS_FILTER`, `PREPROCESS_KEEP_COLS`,
`PREPROCESS_DROP_COLS`, `PREPROCESS_DROP_MISSING_ROWS`,
`PREPROCESS_SAMPLE_ROWS`, `PREPROCESS_SAMPLE_SEED`,
`PREPROCESS_CHARS_TO_FACTORS`, `PREPROCESS_FACTOR_MIN_COUNT`,
`MLR3_DATA_PATH`, `ROW_FILTER`, `N_FOLDS`, `INNER_FOLDS`, `OUTER_REPEATS`,
`OUTER_RESAMPLING`, `OUTER_BLOCK_COL`, `OUTER_YEAR_COL`, `TARGET_MODE`,
`TARGET_DENOMINATOR_COL`, `WEIGHT_COL`, `TUNE_EVALS`, `N_WORKERS`,
`TUNE_BATCH_SIZE`, `RANGER_TUNE_BATCH_SIZE`, `XGB_TUNE_BATCH_SIZE`,
`RANGER_OUTPUT_DIR`, `XGB_OUTPUT_DIR`, `ZINB_OUTPUT_DIR`, and
`COMPARISON_OUTPUT_DIR`. The full-run wrapper also supports `RUN_ZINB=false`
to skip the ZINB step while keeping validation, ranger, XGBoost, comparison,
and run-summary outputs. ZINB also supports `ZINB_WORKERS`, which takes
precedence over `N_WORKERS` for that script. Additional ZINB-specific overrides
include `ZINB_ZERO_FORMULA`, `ZINB_NUMERIC_AS_FACTOR_MAX_LEVELS`,
`ZINB_NUMERIC_AS_FACTOR_VARS`, `FEATURE_COLS`, `ZINB_PARALLEL_BACKEND`, and
`ZINB_VERBOSITY`.

For ZINB parallelization, you can choose:

- `parallel_backend = "fork"` for the current Linux fork-based worker path
- `parallel_backend = "psock"` to test socket-based workers when forked runs
  appear to stall or behave differently across environments
- `parallel_backend = "sequential"` to disable ZINB parallelism completely for
  debugging or stability checks

For reproducibility, the default worker count is `1`. Increase `--workers` for
faster mlr3 runs and for parallel ZINB candidate evaluation on Linux.
For `ranger` and `xgboost`, `CONFIG$ranger$tune_batch_size` and
`CONFIG$xgboost$tune_batch_size` control how many random-search parameter
configurations are evaluated per tuner batch. With `inner_folds = 5` and
`tune_batch_size = 4`, up to roughly `20` inner-CV jobs can be available to
the future worker pool. Ranger still uses `num.threads = 1`, and XGBoost still
uses `nthread = 1`, so this increases process-level parallelism rather than
model-internal threads.
Using a smaller `inner_folds` than `n_folds` is often a good compromise when
you want faster tuning runs without changing the outer validation design.
If you want a more stable validation estimate for `ranger` or `xgboost`,
increase `outer_repeats`; if it is omitted in the config or on the command
line, the scripts keep the historical single-repeat behavior.
Set `outer_resampling = "year_blocked"` (or pass
`--outer-resampling=year_blocked`) to hold out one complete year per outer
fold. `outer_block_col` / `--outer-block-col` names the year column; it is used
only for splitting unless it is also explicitly listed in the model features.
Year-blocked outer validation is deterministic, so `outer_repeats` must remain
`1`.
For `ranger` and XGBoost, `target_mode = "rate"` trains on
`target / target_denominator_col` and postprocesses predictions back to the
original count scale. The usual `truth`, `response`, `error`, and
`*_overall_metrics.csv` values remain on the original count scale so
`compare_best_models.R` can still rank count and rate runs consistently.
The direct model-scale values are written as `model_truth`, `model_response`,
`*_model_scale_*_metrics.csv`, and optional exposure-baseline outputs when a
rate denominator is configured.

For count-like targets with a long right tail, ranger can additionally train on
a transformed target. Set `CONFIG$ranger$target_transform = "log1p"` to train
on `log1p(target)` in count mode or `log1p(target / denominator)` in rate mode.
The main prediction and metric files are still back-transformed to the original
count scale, while `model_truth` and `model_response` keep the transformed
model scale.

For an XGBoost Poisson count model with an exposure offset, keep
`CONFIG$experiment$target_mode = "count"`, set `CONFIG$xgboost$objective =
"count:poisson"`, optionally set `CONFIG$xgboost$eval_metric =
"poisson-nloglik"`, and set `CONFIG$xgboost$exposure_col` to the exposure
column, for example `potenzielle_kunden`. The script uses
`log(exposure_col)` as the mlr3 offset/base margin in every inner and outer CV
split. The exposure column must be removed from
`CONFIG$experiment$feature_cols` so it is not also used as a normal predictor.

```r
CONFIG$experiment$target_mode <- "count"
CONFIG$experiment$feature_cols <- c("prcrank", "unfalldeckung")
CONFIG$xgboost$objective <- "count:poisson"
CONFIG$xgboost$eval_metric <- "poisson-nloglik"
CONFIG$xgboost$exposure_col <- "potenzielle_kunden"
```

As a practical rule of thumb:

- start with the physical CPU core count, not the number of hardware threads
- for interactive work, use roughly `cores - 1` so the machine stays responsive
- for longer batch runs on a dedicated machine, `cores - 1` or `cores` is
  usually sensible
- if RAM becomes tight or the data are large, reduce workers before increasing
  them

For this repository, `CONFIG$experiment$n_workers` controls the mlr3 worker
count and `CONFIG$zinb$workers` controls the ZINB candidate-evaluation worker
count. A good starting point on an 8-core machine is often:

```r
CONFIG$experiment$n_workers <- 7L
CONFIG$zinb$workers <- 7L
CONFIG$ranger$tune_batch_size <- 2L
CONFIG$xgboost$tune_batch_size <- 2L
```

On laptops or when running several jobs at once, values such as `4L` or `6L`
are often the more stable choice even if more cores are available.

For ZINB log output, `CONFIG$zinb$verbosity` (or `--verbosity`) supports:

- `quiet`: only the main run milestones
- `batch`: compact progress on selection steps, folds, and candidate batches
- `detailed`: additional fold- and retry-level fit tracing for debugging

## Outputs And Tracking

Each script writes its outputs into its configured output directory. Depending
on the script, this includes:

- result tables as `.csv` and `.rds`
- prediction tables as `.csv` and `.rds`
- parameter tables as `.csv` and `.rds`
- a log file for progress and debugging
- `run_manifest.csv` and `run_manifest.rds`

The full-run wrapper also writes:

- `run_summary.csv` and `run_summary.rds`
- `run_summary_scripts.csv` and `run_summary_scripts.rds`
- `run_context.csv` and `run_context.rds`
- `run_report.md`
- `config_snapshot.R`
- `config_snapshot.txt` and `config_snapshot.rds`

The results root also keeps a simple cross-run registry:

- `results/run_registry.csv`
- `results/run_registry.rds`

The run manifest is intended to make later comparisons easier. It includes:

- `seed`
- `data_path`
- `feature_list`
- `git_commit`
- `start_time`
- `end_time`
- `runtime_seconds`
- `workers`
- `package_versions`
- `status`

If a script fails after logging has started, it still writes a manifest with
`status = failed` and closes the log cleanly.
The run summary gives you a compact view over the script statuses, the overall
run status, and the top-ranked model from the comparison step.

The per-script reports and transparency files include:

- `resolved_config.txt` and `resolved_config.rds`
- `*_dataset_overview.csv` and `.rds`
- `*_model_report.md` or `comparison_report.md`
- feature importance files for `ranger` and `xgboost` when the final diagnostic
  fit succeeds

For repeated mlr3 runs, `*_cv_predictions.csv`, `*_fold_metrics.csv`, and
`*_best_params.csv` gain a `repeat` column when `outer_repeats > 1`. The
`*_overall_metrics.csv` files keep the same schema so downstream comparison
scripts remain compatible.
For rate-target mlr3 runs, `*_cv_predictions.csv` additionally includes
`model_truth`, `model_response`, `model_truth_untransformed`,
`model_response_untransformed`, `target_transform`, `denominator`, `weight`,
and `postprocessed_response`; count-scale metrics remain in
`*_overall_metrics.csv`, while direct model-scale metrics are written to
`*_model_scale_overall_metrics.csv` and `*_model_scale_fold_metrics.csv`.
Exposure-baseline files are written as `*_exposure_baseline_*` when
`target_mode = "rate"`.

Additional ZINB transparency outputs include:

- `zinb_step_diagnostics.csv` and `.rds`
- `zinb_top_candidates_by_step.csv` and `.rds`
- `zinb_outer_fold_selected_models.csv` and `.rds`
- `zinb_final_model_summary.csv` / `.txt`
- `zinb_final_model_count_coefficients.csv`
- `zinb_final_model_zero_coefficients.csv`

For ZINB specifically:

- `zinb_best_global_overall_metrics.csv` and related `zinb_best_global_*` files
  now summarize outer-CV performance
- `zinb_final_model_summary.csv` and the coefficient tables come from the final
  refit on the full dataset and are intended for interpretation, not unbiased
  performance estimation

`run_regression_tests.R` currently covers:

- successful `validate_repo.R`
- failed-run manifest writing for `mlr3_ranger_tuning.R`
- missing-output diagnostics in `compare_best_models.R`
- summary-file generation in `write_run_summary.R`

## Required R Packages

Install these packages before running all scripts:

```r
install.packages(c(
  "data.table",
  "mlr3",
  "mlr3learners",
  "mlr3tuning",
  "paradox",
  "bbotk",
  "future",
  "ranger",
  "xgboost",
  "pscl"
))
```

`splines` is used by the ZINB transformations and ships with base R.

## Data

Default input:

```text
testfile_zinb_nonlinear_eintritte.csv
```

Columns in the test data:

```text
n_eintritte
prcrank
potenzielle_kunden
unfalldeckung
```

`n_eintritte` is the target. The default feature set for the mlr3 scripts is:

```r
FEATURE_COLS <- c("prcrank", "potenzielle_kunden", "unfalldeckung")
```

For real data with more columns, edit `CONFIG$experiment$feature_cols` in
`configs/base_config.R` or in a variant config to control which predictors are
used.

Expected repository layout:

```text
.
├── configs/
│   └── base_config.R
├── preprocess_data.R
├── validate_repo.R
├── run_regression_tests.R
├── experiment_utils.R
├── mlr3_ranger_tuning.R
├── mlr3_xgb_tuning.R
├── zinb_stepwise_cv.R
├── compare_best_models.R
├── write_run_summary.R
└── testfile_zinb_nonlinear_eintritte.csv
```

## Preprocessing And Factors

`preprocess_data.R` can load:

- `csv`
- `tsv`
- `txt`
- `rds`
- `rda`
- `RData`

It writes the processed dataset in one or more configured formats, for example:

```text
outputs_preprocessed/preprocessed_dataset.csv
outputs_preprocessed/preprocessed_dataset.rds
```

It also writes metadata describing the derived dataset:

```text
outputs_preprocessed/preprocessed_dataset_metadata_summary.csv
outputs_preprocessed/preprocessed_dataset_metadata_summary.rds
outputs_preprocessed/preprocessed_dataset_metadata_columns.csv
outputs_preprocessed/preprocessed_dataset_metadata_columns.rds
outputs_preprocessed/preprocessed_dataset_metadata_files.csv
outputs_preprocessed/preprocessed_dataset_metadata_files.rds
outputs_preprocessed/preprocessed_dataset_metadata.rds
outputs_preprocessed/preprocess_data.log
```

The preprocessing summary metadata includes separate row-removal counts for
the row filter, `na.omit()`-based missing-value removal, and optional random
subsampling.

Factor handling policy:

- `preprocess_data.R` converts `character` columns to `factor` by default
  (`--chars-to-factors=true`)
- unused factor levels are dropped after filtering
- factors with fewer than 2 observed levels stop the run with an explicit error
- rare factor levels trigger warnings; the default threshold is
  `--factor-min-count=5`
- the modeling scripts keep factors for `ranger` and ZINB, while XGBoost encodes
  factors into dummy variables before fitting
