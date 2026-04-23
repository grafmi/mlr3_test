# mlr3 Count Regression Experiments

This repository contains small R experiment scripts for predicting `n_eintritte`
from a synthetic zero-inflated count dataset.

The count nature of the target is handled explicitly by the ZINB experiment. The
`ranger` and XGBoost experiments intentionally treat the target as a regression
problem for now.

Most experiment defaults now live in `config.R`. The scripts use those values
as readable defaults and still allow explicit overrides via command-line
arguments and environment variables.

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

## Scripts

```text
preprocess_data.R
```

Loads a tabular dataset from `csv`, `tsv`, `txt`, `rds`, `rda`, or `RData`,
applies simple preprocessing steps such as row filters and column selection,
then writes the processed dataset plus metadata files.

```text
mlr3_ranger_tuning.R
```

Runs a nested CV experiment for `regr.ranger`. Hyperparameters are tuned with
random search on an inner CV, and performance is estimated on an outer
stratified CV.

```text
mlr3_xgb_tuning.R
```

Runs the same nested CV structure for `regr.xgboost`.

```text
zinb_stepwise_cv.R
```

Runs stepwise feature and transformation selection for a zero-inflated negative
binomial model via `pscl::zeroinfl`. The count part is selected stepwise from
`FEATURE_COLS`; the zero-inflation part is currently fixed to an intercept
(`| 1`).

```text
compare_best_models.R
```

Reads the output CSV files from the model scripts and writes a simple ranked
comparison table.

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

Example preprocessing runs:

```sh
Rscript preprocess_data.R \
  --input=testfile_zinb_nonlinear_eintritte.csv \
  --output-dir=outputs_preprocessed \
  --formats=csv,rds

Rscript preprocess_data.R \
  --input=/path/to/data.RData \
  --filter="n_eintritte >= 1 & prcrank <= 8" \
  --keep-cols=n_eintritte,prcrank,potenzielle_kunden
```

Example full run with explicit output directories:

```sh
MLR3_DATA_PATH=/path/to/data.csv \
RANGER_OUTPUT_DIR=/tmp/outputs_ranger \
XGB_OUTPUT_DIR=/tmp/outputs_xgb \
ZINB_OUTPUT_DIR=/tmp/outputs_zinb \
COMPARISON_OUTPUT_DIR=/tmp/outputs_model_comparison \
./run_all.sh
```

For reproducibility, the default worker count is `1`. Increase `--workers` for
faster mlr3 runs and for parallel ZINB candidate evaluation on Linux.

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
- `CONFIG$zinb`: ZINB output directory, metric, transformations, and workers
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

## Outputs

Generated files are written to:

```text
outputs_preprocessed/
outputs_ranger/
outputs_xgb/
outputs_zinb/
outputs_model_comparison/
```

These directories are ignored by git.

Each model script writes:

```text
*_fold_metrics.csv
*_overall_metrics.csv
*_cv_predictions.csv
*_best_params.csv or ZINB step-selection tables
run_manifest.csv
matching *.rds files for each CSV result
*.log
```

`compare_best_models.R` reads those files and writes:

```text
outputs_model_comparison/best_models_comparison.csv
outputs_model_comparison/best_models_comparison.rds
outputs_model_comparison/compare_best_models.log
```

Every CSV result is also saved as a same-named RDS file, for example
`ranger_overall_metrics.csv` and `ranger_overall_metrics.rds`.

Each script now also writes a small `run_manifest.csv` plus `run_manifest.rds`
into its output directory. The manifest captures the seed, data path, feature
list, git commit, start and end time, runtime, worker count, and package
versions used for that run.

The log files are written into the same output directory as the CSV files. They
capture the script settings, progress messages, package output, warnings, and
errors, which makes long-running jobs easier to monitor and debug.

`preprocess_data.R` writes the processed dataset in the requested formats, for
example:

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
