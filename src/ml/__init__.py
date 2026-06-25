"""Machine-learning risk layer for the Alpha AML platform.

Two complementary models scored per customer over a 30-day behavioral window:

* Isolation Forest  — unsupervised anomaly detection (no labels needed).
* Supervised triage — learns the historical rule-engine alert label and
  produces a calibrated alert probability, benchmarked with ROC-AUC / PR-AUC.

Artifacts (scores + run metrics) are persisted to Postgres
(`aml.ml_customer_scores`, `aml.ml_model_runs`) so the dashboard can render
model quality without recomputing. See `src/ml/train.py`.
"""
