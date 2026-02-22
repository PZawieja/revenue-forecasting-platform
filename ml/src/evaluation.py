"""Evaluation metrics and JSON-serializable calibration curves for renewal model."""

from typing import Any, List, Tuple

import numpy as np
from sklearn.metrics import (
    brier_score_loss,
    precision_recall_curve,
    roc_auc_score,
)

try:
    from sklearn.calibration import calibration_curve
except ImportError:
    calibration_curve = None  # type: ignore


def roc_auc(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Compute ROC AUC; return 0.5 if only one class present."""
    try:
        return float(roc_auc_score(y_true, y_prob))
    except ValueError:
        return 0.5


def pr_auc(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Compute area under precision-recall curve (average precision)."""
    precision, recall, _ = precision_recall_curve(y_true, y_prob)
    # AUC of PR curve: integrate precision by recall
    return float(np.trapz(precision, recall))


def brier_score(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Brier score (lower is better)."""
    return float(brier_score_loss(y_true, y_prob))


def calibration_curve_buckets(
    y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10
) -> Tuple[List[float], List[float], List[float]]:
    """
    Return (mean_predicted_value, fraction_of_positives, count_per_bin) for JSON.
    If calibration_curve not available, return empty lists.
    """
    if calibration_curve is None:
        return [], [], []
    prob_true, prob_pred = calibration_curve(y_true, y_prob, n_bins=n_bins)
    # Counts per bin require binning manually
    bins = np.linspace(0, 1, n_bins + 1)
    counts = []
    for i in range(n_bins):
        mask = (y_prob >= bins[i]) & (y_prob < bins[i + 1])
        if i == n_bins - 1:
            mask = (y_prob >= bins[i]) & (y_prob <= bins[i + 1])
        counts.append(int(np.sum(mask)))
    return (
        prob_pred.tolist(),
        prob_true.tolist(),
        counts,
    )


def metrics_dict(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    n_calibration_bins: int = 10,
) -> dict:
    """Build a JSON-serializable dict of metrics and calibration curve."""
    cal_pred, cal_true, cal_counts = calibration_curve_buckets(
        y_true, y_prob, n_bins=n_calibration_bins
    )
    return {
        "roc_auc": roc_auc(y_true, y_prob),
        "pr_auc": pr_auc(y_true, y_prob),
        "brier_score": brier_score(y_true, y_prob),
        "calibration": {
            "mean_predicted_value": cal_pred,
            "fraction_of_positives": cal_true,
            "count_per_bin": cal_counts,
        },
    }


def ensure_serializable(obj: Any) -> Any:
    """Convert numpy types to native Python for JSON."""
    if isinstance(obj, (np.integer, np.floating)):
        return float(obj) if isinstance(obj, np.floating) else int(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: ensure_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [ensure_serializable(x) for x in obj]
    return obj
