"""Train + score the AML risk models, then persist results to Postgres.

Run as a one-shot job (locally or via the `ml_score` Airflow DAG):

    python -m src.ml.train

Pipeline:
  1. Load the 30-day per-customer feature frame (src/ml/features.py).
  2. Fit an Isolation Forest -> normalized anomaly score for every customer.
  3. If enough labelled history exists, train a supervised triage classifier
     (Gradient Boosting) on the rule-engine alert label, evaluate it on a
     held-out split (ROC-AUC, PR-AUC, precision/recall/F1, curves, feature
     importances), then score every customer's alert probability.
  4. Write the current score snapshot to aml.ml_customer_scores and the run
     metrics to aml.ml_model_runs. Best-effort joblib artifact to MODEL_DIR.

Designed to be safe on sparse data: if there are too few samples or only one
class, it still writes anomaly scores and records why supervision was skipped.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, IsolationForest
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sqlalchemy import text

from src.ml.features import FEATURE_COLUMNS, build_matrix, get_engine, load_features

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
try:
    from src.common.event_log import install_pg_log_handler

    install_pg_log_handler("ml")
except Exception:  # pragma: no cover - log mirroring is best-effort
    pass
logger = logging.getLogger("ml.train")

RANDOM_STATE = 42
CONTAMINATION = float(os.getenv("ML_CONTAMINATION", "0.05"))
MIN_SAMPLES = int(os.getenv("ML_MIN_SAMPLES", "40"))
# Alert labels are scarce (rule alerts are budget-capped), so we keep the
# supervised bar low and evaluate with cross-validation rather than a tiny
# single holdout. Metrics still report the exact label counts for honesty.
MIN_PER_CLASS = int(os.getenv("ML_MIN_PER_CLASS", "10"))
MODEL_DIR = os.getenv("ML_MODEL_DIR", "/opt/airflow/models")
_MAX_CURVE_POINTS = 200


def _downsample(values: np.ndarray, n: int = _MAX_CURVE_POINTS) -> list[float]:
    """Evenly subsample a curve array to keep persisted JSON small."""
    arr = np.asarray(values, dtype="float64")
    if arr.size <= n:
        return [round(float(v), 5) for v in arr]
    idx = np.linspace(0, arr.size - 1, n).astype(int)
    return [round(float(v), 5) for v in arr[idx]]


def _normalize(scores: np.ndarray) -> np.ndarray:
    """Min-max normalize to 0..1 (higher = more anomalous)."""
    lo, hi = float(np.min(scores)), float(np.max(scores))
    if hi - lo < 1e-12:
        return np.zeros_like(scores)
    return (scores - lo) / (hi - lo)


def _train_supervised(x: np.ndarray, y: np.ndarray) -> dict:
    """Train + evaluate the supervised triage model with cross-validation.

    Alert labels are scarce and highly imbalanced, so instead of a single
    holdout (which would leave only a handful of positives in the test fold) we
    use stratified K-fold *out-of-fold* probabilities for every customer. This
    uses all positives for evaluation and yields stable ROC / PR estimates.
    Precision/recall/F1 are reported at the F1-optimal threshold rather than a
    fixed 0.5, which is the right call for an imbalanced base rate.
    """
    pos, neg = int(y.sum()), int((~y.astype(bool)).sum())
    if pos < MIN_PER_CLASS or neg < MIN_PER_CLASS:
        return {
            "supervised_trained": False,
            "notes": f"supervised skipped: positives={pos}, negatives={neg} "
            f"(need >= {MIN_PER_CLASS} each)",
        }

    n_splits = int(min(5, pos, neg))
    if n_splits < 2:
        return {
            "supervised_trained": False,
            "notes": f"supervised skipped: too few per class for CV (positives={pos})",
        }

    skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=RANDOM_STATE)
    base = GradientBoostingClassifier(random_state=RANDOM_STATE)
    oof = cross_val_predict(base, x, y, cv=skf, method="predict_proba", n_jobs=1)[:, 1]

    fpr, tpr, _ = roc_curve(y, oof)
    prec, rec, thr = precision_recall_curve(y, oof)
    f1s = 2 * prec * rec / (prec + rec + 1e-12)
    best = int(np.argmax(f1s))
    best_thr = float(thr[min(best, len(thr) - 1)]) if len(thr) else 0.5
    pred = (oof >= best_thr).astype(int)

    # Refit on the full set so every customer gets a triage score + stable importances.
    clf_full = GradientBoostingClassifier(random_state=RANDOM_STATE)
    clf_full.fit(x, y)
    importances = {
        feat: round(float(imp), 5)
        for feat, imp in zip(FEATURE_COLUMNS, clf_full.feature_importances_)
    }

    return {
        "supervised_trained": True,
        "model": clf_full,
        "roc_auc": round(float(roc_auc_score(y, oof)), 4),
        "pr_auc": round(float(average_precision_score(y, oof)), 4),
        "precision_score": round(float(precision_score(y, pred, zero_division=0)), 4),
        "recall_score": round(float(recall_score(y, pred, zero_division=0)), 4),
        "f1_score": round(float(f1_score(y, pred, zero_division=0)), 4),
        "roc_curve": {"fpr": _downsample(fpr), "tpr": _downsample(tpr)},
        "pr_curve": {"recall": _downsample(rec), "precision": _downsample(prec)},
        "feature_importance": importances,
        "notes": f"supervised: {n_splits}-fold CV on {len(y)} samples "
        f"(positives={pos}, negatives={neg}); thr*={best_thr:.3f}",
    }


def _persist(engine, version: str, scores_df: pd.DataFrame, run: dict) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO aml.ml_model_runs (
                    model_version, n_samples, n_features, n_anomalies, contamination,
                    supervised_trained, positive_rate, roc_auc, pr_auc,
                    precision_score, recall_score, f1_score,
                    roc_curve, pr_curve, feature_importance, notes
                ) VALUES (
                    :version, :n_samples, :n_features, :n_anomalies, :contamination,
                    :supervised, :positive_rate, :roc_auc, :pr_auc,
                    :precision, :recall, :f1,
                    CAST(:roc_curve AS jsonb), CAST(:pr_curve AS jsonb),
                    CAST(:feat_imp AS jsonb), :notes
                )
                """
            ),
            {
                "version": version,
                "n_samples": int(run["n_samples"]),
                "n_features": len(FEATURE_COLUMNS),
                "n_anomalies": int(run["n_anomalies"]),
                "contamination": CONTAMINATION,
                "supervised": bool(run.get("supervised_trained", False)),
                "positive_rate": run.get("positive_rate"),
                "roc_auc": run.get("roc_auc"),
                "pr_auc": run.get("pr_auc"),
                "precision": run.get("precision_score"),
                "recall": run.get("recall_score"),
                "f1": run.get("f1_score"),
                "roc_curve": json.dumps(run.get("roc_curve")) if run.get("roc_curve") else None,
                "pr_curve": json.dumps(run.get("pr_curve")) if run.get("pr_curve") else None,
                "feat_imp": json.dumps(run.get("feature_importance"))
                if run.get("feature_importance")
                else None,
                "notes": run.get("notes"),
            },
        )
        # Current snapshot only: keep aml.ml_customer_scores at one version.
        conn.execute(text("DELETE FROM aml.ml_customer_scores"))

    scores_df.to_sql(
        "ml_customer_scores",
        engine,
        schema="aml",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )


def _dump_artifact(version: str, iso: IsolationForest, run: dict) -> None:
    """Best-effort joblib artifact (never fails the job if the path is read-only)."""
    try:
        import joblib

        os.makedirs(MODEL_DIR, exist_ok=True)
        payload = {
            "version": version,
            "features": FEATURE_COLUMNS,
            "isolation_forest": iso,
            "triage_model": run.get("model"),
            "metrics": {k: run.get(k) for k in ("roc_auc", "pr_auc", "f1_score")},
        }
        path = os.path.join(MODEL_DIR, "aml_risk_models.joblib")
        joblib.dump(payload, path)
        logger.info("Saved model artifact to %s", path)
    except Exception as exc:  # noqa: BLE001 - artifact is optional
        logger.warning("Could not write joblib artifact: %s", exc)


def main() -> dict:
    engine = get_engine()
    df = load_features(engine)
    version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    if df.empty or len(df) < MIN_SAMPLES:
        n = 0 if df.empty else len(df)
        notes = f"insufficient data: {n} active customers (need >= {MIN_SAMPLES})"
        logger.warning(notes)
        _persist(
            engine,
            version,
            pd.DataFrame(
                columns=[
                    "customer_id", "model_version", "anomaly_score", "anomaly_rank",
                    "is_anomaly", "triage_score", "rule_flagged", "txn_count_30d",
                    "volume_30d", "distinct_receivers_30d", "max_txn_30d", "kyc_risk_score",
                ]
            ),
            {"n_samples": n, "n_anomalies": 0, "supervised_trained": False, "notes": notes},
        )
        return {"version": version, "n_samples": n, "status": "skipped"}

    x = build_matrix(df)
    y = df["rule_flagged"].astype(int).to_numpy()

    # 1. Unsupervised anomaly detection.
    iso = IsolationForest(
        n_estimators=200, contamination=CONTAMINATION, random_state=RANDOM_STATE, n_jobs=1
    )
    iso.fit(x)
    raw = -iso.decision_function(x)  # higher = more anomalous
    anomaly_score = _normalize(raw)
    is_anomaly = iso.predict(x) == -1

    # 2. Supervised triage (optional, depends on label balance).
    run = _train_supervised(x, y)
    run["n_samples"] = len(df)
    run["n_anomalies"] = int(is_anomaly.sum())
    run["positive_rate"] = round(float(y.mean()), 4)

    triage = None
    if run.get("supervised_trained") and run.get("model") is not None:
        triage = run["model"].predict_proba(x)[:, 1]

    # 3. Assemble the score snapshot.
    out = pd.DataFrame(
        {
            "customer_id": df["customer_id"].values,
            "model_version": version,
            "anomaly_score": np.round(anomaly_score, 5),
            "is_anomaly": is_anomaly,
            "triage_score": np.round(triage, 5) if triage is not None else None,
            "rule_flagged": df["rule_flagged"].values,
            "txn_count_30d": df["txn_count_30d"].astype(int).values,
            "volume_30d": df["volume_30d"].astype(float).values,
            "distinct_receivers_30d": df["distinct_receivers_30d"].astype(int).values,
            "max_txn_30d": df["max_txn_30d"].astype(float).values,
            "kyc_risk_score": df["kyc_risk_score"].astype(float).values,
        }
    )
    out = out.sort_values("anomaly_score", ascending=False).reset_index(drop=True)
    out["anomaly_rank"] = out.index + 1

    _persist(engine, version, out, run)
    _dump_artifact(version, iso, run)

    logger.info(
        "Run %s: %d customers, %d anomalies, supervised=%s, roc_auc=%s",
        version, run["n_samples"], run["n_anomalies"],
        run.get("supervised_trained"), run.get("roc_auc"),
    )
    return {
        "version": version,
        "n_samples": run["n_samples"],
        "n_anomalies": run["n_anomalies"],
        "supervised_trained": run.get("supervised_trained"),
        "roc_auc": run.get("roc_auc"),
        "status": "ok",
    }


if __name__ == "__main__":
    print(main())
