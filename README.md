# mlr3 Count Regression Experiments

This repository contains small R experiment scripts for predicting `n_eintritte`
from a synthetic zero-inflated count dataset.

The count nature of the target is handled explicitly by the ZINB experiment. The
`ranger` and XGBoost experiments intentionally treat the target as a regression
problem for now.

Most experiment defaults now live in `config.R`. The scripts use those values
as readable defaults and still allow explicit overrides via command-line
arguments and environment variables.

## Quick Start

For most users, these are the main steps:

1. Adjust the dataset path and core experiment settings in `config.R`
2. Run the full pipeline on Linux with `./run_all.sh`
3. Inspect the versioned output folder under `results/YYYYMMDD_HHMM/`

Typical example:

```r
CONFIG$experiment$data_path <- "/absolute/path/to/my_dataset.csv"
CONFIG$experiment$feature_cols <- c("prcrank", "potenzielle_kunden", "unfalldeckung")
CONFIG$experiment$seed <- 123L
CONFIG$experiment$n_folds <- 10L
```

Then run:

```sh
./run_all.sh
```

The most relevant outputs per script are:

- metrics as `.csv` and `.rds`
- predictions as `.csv` and `.rds`
- logs as `*.log`
- run metadata as `run_manifest.csv` and `run_manifest.rds`

## What The Scripts Do

- `preprocess_data.R`: load data from common R and tabular formats, apply light preprocessing, and write dataset metadata
- `mlr3_ranger_tuning.R`: nested CV for `ranger` regression with hyperparameter tuning
- `mlr3_xgb_tuning.R`: nested CV for `xgboost` regression with hyperparameter tuning
- `zinb_stepwise_cv.R`: stratified 10-fold CV with forward selection for a ZINB model
- `compare_best_models.R`: read the best model outputs and create a ranked comparison

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

The ZINB script follows a different but still explicit validation design:

- it uses stratified 10-fold CV
- in each forward-selection step, candidate terms are scored by CV performance
- one variable is added per step
- each original variable may appear only once in the final model, regardless of transformation

So the ZINB script does not have a separate inner and outer loop. Instead, the
same CV design is used to compare candidate formulas during forward selection.
This is simpler, but it also means that the ZINB selection path is more tightly
coupled to the evaluation data than the nested mlr3 setup.

## Configuration

The main place to edit experiment settings is:

```text
config.R
```

The file is grouped so that the most important settings appear first:

- `CONFIG$experiment`: data path, target, features, seed, folds, workers
- `CONFIG$preprocess`: preprocessing defaults and factor handling
- `CONFIG$ranger`: ranger output directory and tuning defaults
- `CONFIG$xgboost`: xgboost output directory and tuning defaults
- `CONFIG$zinb`: ZINB output directory, metric, transformations, workers, and
  zero-inflation formula
- `CONFIG$comparison`: model-comparison defaults

The scripts use this precedence order:

1. command-line argument
2. environment variable
3. `config.R`

You can also set an explicit data path directly in `config.R`, which is often
the most convenient option for day-to-day experimentation:

```r
CONFIG$experiment$data_path <- "/absolute/path/to/my_dataset.csv"
```

Absolute paths, repo-relative paths, and `~/...` paths are supported.

`CONFIG$experiment$id_cols` can be used for identifier columns that exist in the
dataset but must not be used as predictors, for example customer IDs, policy
numbers, or technical row IDs.

For `CONFIG$ranger$search_space` and `CONFIG$xgboost$search_space`, supported
optional parameters can also be removed or commented out. If a known optional
entry is missing in `config.R`, that parameter is simply not tuned in the
corresponding script.

For ZINB, `CONFIG$zinb$zero_inflation_formula` controls the right-hand side of
the zero-inflation part. Examples:

```r
zero_inflation_formula = "1"
zero_inflation_formula = "same_as_count"
zero_inflation_formula = "prcrank + log1p(potenzielle_kunden)"
```

## Running

Typical usage from the repository root:

```sh
Rscript preprocess_data.R
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
the input data exist, then runs the four scripts in sequence.

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
```

The scripts also work when sourced interactively from VS Code, RStudio Pro, or
Positron, as long as `experiment_utils.R` is either next to the script or in the
current working directory.

Common command-line overrides:

```sh
Rscript preprocess_data.R --input=/path/to/data.rds --formats=csv,rds --filter="n_eintritte >= 0"
Rscript preprocess_data.R --keep-cols=n_eintritte,prcrank --drop-missing-rows=true
Rscript preprocess_data.R --chars-to-factors=true --factor-min-count=5
Rscript mlr3_ranger_tuning.R --data=/path/to/data.csv --folds=10 --tune-evals=20 --workers=4
Rscript mlr3_xgb_tuning.R --output-dir=outputs_xgb_custom
Rscript zinb_stepwise_cv.R --metric=rmse --max-vars=3 --workers=4
Rscript compare_best_models.R --metric=rmse
```

The same settings can be controlled with environment variables, for example
`INPUT_DATA_PATH`, `PREPROCESS_OUTPUT_DIR`, `PREPROCESS_OUTPUT_NAME`,
`PREPROCESS_OUTPUT_FORMATS`, `PREPROCESS_FILTER`, `PREPROCESS_KEEP_COLS`,
`PREPROCESS_DROP_COLS`, `PREPROCESS_DROP_MISSING_ROWS`,
`PREPROCESS_CHARS_TO_FACTORS`, `PREPROCESS_FACTOR_MIN_COUNT`,
`MLR3_DATA_PATH`, `N_FOLDS`, `TUNE_EVALS`, `N_WORKERS`,
`RANGER_OUTPUT_DIR`, `XGB_OUTPUT_DIR`, `ZINB_OUTPUT_DIR`, and
`COMPARISON_OUTPUT_DIR`. ZINB also supports `ZINB_WORKERS`, which takes
precedence over `N_WORKERS` for that script.

For reproducibility, the default worker count is `1`. Increase `--workers` for
faster mlr3 runs and for parallel ZINB candidate evaluation on Linux.

## Outputs And Tracking

Each script writes its outputs into its configured output directory. Depending
on the script, this includes:

- result tables as `.csv` and `.rds`
- prediction tables as `.csv` and `.rds`
- parameter tables as `.csv` and `.rds`
- a log file for progress and debugging
- `run_manifest.csv` and `run_manifest.rds`

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

For real data with more columns, edit `FEATURE_COLS` in the mlr3 and ZINB
scripts to control which predictors are used.

Expected repository layout:

```text
.
├── config.R
├── preprocess_data.R
├── experiment_utils.R
├── mlr3_ranger_tuning.R
├── mlr3_xgb_tuning.R
├── zinb_stepwise_cv.R
├── compare_best_models.R
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

Factor handling policy:

- `preprocess_data.R` converts `character` columns to `factor` by default
  (`--chars-to-factors=true`)
- unused factor levels are dropped after filtering
- factors with fewer than 2 observed levels stop the run with an explicit error
- rare factor levels trigger warnings; the default threshold is
  `--factor-min-count=5`
- the modeling scripts keep factors for `ranger` and ZINB, while XGBoost encodes
  factors into dummy variables before fitting
