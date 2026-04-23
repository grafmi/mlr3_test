# mlr3 Count Regression Experiments

This repository contains small R experiment scripts for predicting `n_eintritte`
from a synthetic zero-inflated count dataset.

The count nature of the target is handled explicitly by the ZINB experiment. The
`ranger` and XGBoost experiments intentionally treat the target as a regression
problem for now.

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

For real data with more columns, edit `FEATURE_COLS` in the mlr3 scripts to
control which predictors are used.

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
binomial model via `pscl::zeroinfl`.

```text
compare_best_models.R
```

Reads the output CSV files from the model scripts and writes a simple ranked
comparison table.

## Parallelism

The mlr3 scripts use `future::multisession`. Configure the worker count in each
script:

```r
N_WORKERS <- 16
```

Reduce this value when running multiple scripts at the same time, for example
when starting Ranger and XGBoost in parallel.

## Outputs

Generated files are written to:

```text
outputs_ranger/
outputs_xgb/
outputs_zinb/
outputs_model_comparison/
```

These directories are ignored by git.

## Typical Usage

```sh
Rscript mlr3_ranger_tuning.R
Rscript mlr3_xgb_tuning.R
Rscript zinb_stepwise_cv.R
Rscript compare_best_models.R
```
