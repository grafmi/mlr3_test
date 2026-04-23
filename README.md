# mlr3 Count Regression Experiments

This repository contains small R experiment scripts for predicting `n_eintritte`
from a synthetic zero-inflated count dataset.

The count nature of the target is handled explicitly by the ZINB experiment. The
`ranger` and XGBoost experiments intentionally treat the target as a regression
problem for now.

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
├── experiment_utils.R
├── mlr3_ranger_tuning.R
├── mlr3_xgb_tuning.R
├── zinb_stepwise_cv.R
├── compare_best_models.R
└── testfile_zinb_nonlinear_eintritte.csv
```

## Scripts

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

The scripts also work when sourced interactively from VS Code, RStudio Pro, or
Positron, as long as `experiment_utils.R` is either next to the script or in the
current working directory.

Common command-line overrides:

```sh
Rscript mlr3_ranger_tuning.R --data=/path/to/data.csv --folds=10 --tune-evals=20 --workers=4
Rscript mlr3_xgb_tuning.R --output-dir=outputs_xgb_custom
Rscript zinb_stepwise_cv.R --metric=rmse --max-vars=3 --workers=4
Rscript compare_best_models.R --metric=rmse
```

The same settings can be controlled with environment variables, for example
`MLR3_DATA_PATH`, `N_FOLDS`, `TUNE_EVALS`, `N_WORKERS`,
`RANGER_OUTPUT_DIR`, `XGB_OUTPUT_DIR`, `ZINB_OUTPUT_DIR`, and
`COMPARISON_OUTPUT_DIR`. ZINB also supports `ZINB_WORKERS`, which takes
precedence over `N_WORKERS` for that script.

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

## Outputs

Generated files are written to:

```text
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

The log files are written into the same output directory as the CSV files. They
capture the script settings, progress messages, package output, warnings, and
errors, which makes long-running jobs easier to monitor and debug.
