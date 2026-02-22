-- Which model is champion per dataset and (if present) backtest quality.
-- Use to justify ML choices and monitor model selection.

-- Champion selection (from ml_model_selection)
select
  dataset,
  preferred_model,
  selection_reason,
  score_logistic,
  score_xgboost
from main.ml_model_selection
order by dataset;

-- Latest backtest quality (renewals)
select
  'renewals' as dataset,
  cutoff_month,
  model_name,
  segment,
  auc,
  brier,
  logloss
from main.ml_renewal_backtest_metrics
where cutoff_month = (select max(cutoff_month) from main.ml_renewal_backtest_metrics)
order by model_name, segment;

-- Latest backtest quality (pipeline)
select
  'pipeline' as dataset,
  cutoff_month,
  model_name,
  segment,
  auc,
  brier,
  logloss
from main.ml_pipeline_backtest_metrics
where cutoff_month = (select max(cutoff_month) from main.ml_pipeline_backtest_metrics)
order by model_name, segment;
