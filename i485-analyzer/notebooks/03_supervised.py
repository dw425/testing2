#!/usr/bin/env python3
"""
I-485 Fraud Detection — Supervised ML Models

Trains 3 supervised classifiers (XGBoost, LightGBM, Random Forest) on the
engineered feature matrix, optimizes decision thresholds for F2 score,
and persists model artifacts + evaluation metrics.

Reads:
    data/features/feature_matrix.parquet
    data/features/labels.parquet

Writes:
    data/models/xgboost.joblib
    data/models/lightgbm.joblib
    data/models/rf.joblib
    data/results/supervised_metrics.json
    data/results/feature_importance.parquet
    data/results/optimal_thresholds.json
    data/results/supervised_predictions.parquet

Usage:
    python notebooks/03_supervised.py
"""
import json
import logging
import os
import sys
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split

# ---------------------------------------------------------------------------
# Paths — everything relative to project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent          # notebooks/
PROJECT_ROOT = SCRIPT_DIR.parent                       # i485-analyzer/
DATA_DIR = PROJECT_ROOT / "data"
FEATURE_DIR = DATA_DIR / "features"
MODEL_DIR = DATA_DIR / "models"
RESULT_DIR = DATA_DIR / "results"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG = logging.getLogger("supervised")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)


# ═══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _f2_score(precision, recall):
    """Compute F2 score (beta=2) from precision and recall arrays."""
    denom = 4 * precision + recall
    with np.errstate(divide="ignore", invalid="ignore"):
        f2 = np.where(denom > 0, 5 * precision * recall / denom, 0.0)
    return f2


def focal_loss_objective(y_true, y_pred, gamma=2.0, alpha=0.25):
    """Focal loss custom objective for XGBoost.

    Focal loss down-weights easy examples and focuses training on hard ones:
        FL(p_t) = -alpha_t * (1 - p_t)^gamma * log(p_t)

    Returns (gradient, hessian) arrays.
    """
    # y_pred is raw margin (logit), convert to probability
    p = 1.0 / (1.0 + np.exp(-y_pred))
    p = np.clip(p, 1e-7, 1.0 - 1e-7)

    # alpha weighting per sample
    alpha_t = np.where(y_true == 1, alpha, 1.0 - alpha)

    # focal weight
    p_t = np.where(y_true == 1, p, 1.0 - p)
    focal_weight = alpha_t * (1.0 - p_t) ** gamma

    # gradient of cross-entropy w.r.t. logit: (p - y)
    grad_ce = p - y_true

    # For focal loss gradient, we need:
    # g = alpha_t * ((1-p_t)^gamma * (p - y) +
    #     gamma * (1-p_t)^(gamma-1) * p_t * log(p_t) * (p - y) / (p - y))
    # Simplified form:
    log_pt = np.where(y_true == 1, np.log(p), np.log(1.0 - p))
    grad = focal_weight * grad_ce - gamma * alpha_t * (
        (1.0 - p_t) ** (gamma - 1)) * p_t * log_pt * grad_ce

    # hessian (approximate): use focal_weight * p * (1 - p)
    hess = focal_weight * p * (1.0 - p)
    hess = np.maximum(hess, 1e-7)  # numerical stability

    return grad, hess


def find_optimal_threshold(y_true, y_prob):
    """Find the threshold that maximises the F2 score on the PR curve."""
    precision, recall, thresholds = precision_recall_curve(y_true, y_prob)
    # precision_recall_curve returns len(thresholds) = len(precision) - 1
    precision = precision[:-1]
    recall = recall[:-1]
    f2 = _f2_score(precision, recall)
    best_idx = np.argmax(f2)
    return float(thresholds[best_idx]), {
        "threshold": float(thresholds[best_idx]),
        "f2": float(f2[best_idx]),
        "precision_at_threshold": float(precision[best_idx]),
        "recall_at_threshold": float(recall[best_idx]),
    }


def evaluate_model(name, y_true, y_prob, threshold):
    """Return a dict of evaluation metrics for a single model."""
    y_pred = (y_prob >= threshold).astype(int)
    pr_auc = average_precision_score(y_true, y_prob)
    roc = roc_auc_score(y_true, y_prob)
    f1 = f1_score(y_true, y_pred)
    f2_val = float(_f2_score(
        np.array([precision_score(y_true, y_pred, zero_division=0)]),
        np.array([recall_score(y_true, y_pred, zero_division=0)]),
    )[0])
    prec = precision_score(y_true, y_pred, zero_division=0)
    rec = recall_score(y_true, y_pred, zero_division=0)
    cm = confusion_matrix(y_true, y_pred).tolist()
    report = classification_report(y_true, y_pred, output_dict=True)

    return {
        "model": name,
        "PR_AUC": float(pr_auc),
        "ROC_AUC": float(roc),
        "F1": float(f1),
        "F2": f2_val,
        "Precision": float(prec),
        "Recall": float(rec),
        "threshold": float(threshold),
        "confusion_matrix": cm,
        "classification_report": report,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  1 — Load Data
# ═══════════════════════════════════════════════════════════════════════════════

def load_data():
    feat_path = FEATURE_DIR / "feature_matrix.parquet"
    label_path = FEATURE_DIR / "labels.parquet"

    if not feat_path.exists():
        LOG.error(f"Feature matrix not found: {feat_path}")
        sys.exit(1)
    if not label_path.exists():
        LOG.error(f"Labels not found: {label_path}")
        sys.exit(1)

    LOG.info(f"Loading features from {feat_path}")
    features = pd.read_parquet(feat_path)

    LOG.info(f"Loading labels from {label_path}")
    labels = pd.read_parquet(label_path)

    LOG.info(f"Features shape: {features.shape}")
    LOG.info(f"Labels shape:   {labels.shape}")

    return features, labels


# ═══════════════════════════════════════════════════════════════════════════════
#  2 — Prepare Train / Test Split
# ═══════════════════════════════════════════════════════════════════════════════

def prepare_data(features, labels):
    """Merge features + labels, split 80/20 stratified, apply SMOTE."""

    # Merge on application_id
    df = features.merge(labels[["application_id", "is_fraud"]], on="application_id", how="inner")
    LOG.info(f"Merged dataset: {len(df):,} rows")

    # Separate features / target / IDs
    feature_cols = [c for c in features.columns if c != "application_id"]
    X = df[feature_cols].values.astype(np.float32)
    y = df["is_fraud"].values.astype(int)
    app_ids = df["application_id"].values

    # Sanity check: need both classes
    unique_classes = np.unique(y)
    if len(unique_classes) < 2:
        LOG.warning(f"Only one class present ({unique_classes}). Cannot train classifiers.")
        sys.exit(1)

    num_pos = int(y.sum())
    num_neg = int(len(y) - num_pos)
    LOG.info(f"Class distribution: negative={num_neg:,}  positive={num_pos:,}  "
             f"ratio={num_neg / max(num_pos, 1):.1f}:1")

    # Stratified train/test split
    X_train, X_test, y_train, y_test, ids_train, ids_test = train_test_split(
        X, y, app_ids, test_size=0.2, random_state=42, stratify=y,
    )
    LOG.info(f"Train: {len(X_train):,}  Test: {len(X_test):,}")

    # ----- SMOTE on training data only -----
    X_train_resampled, y_train_resampled = _apply_smote(X_train, y_train, feature_cols)

    return (X_train_resampled, y_train_resampled,
            X_train, y_train,           # original (unsampled) train for eval_set
            X_test, y_test,
            ids_test, feature_cols,
            num_pos, num_neg)


def _apply_smote(X_train, y_train, feature_cols):
    """Try SMOTENC, then SMOTE, then fall back to no resampling."""
    X_res, y_res = X_train, y_train

    try:
        from imblearn.over_sampling import SMOTE
        try:
            from imblearn.over_sampling import SMOTENC
            # Identify categorical-like features (binary with only 0/1)
            cat_indices = []
            for i, col in enumerate(feature_cols):
                unique_vals = np.unique(X_train[:, i])
                if len(unique_vals) <= 2 and set(unique_vals).issubset({0.0, 1.0}):
                    cat_indices.append(i)

            if len(cat_indices) >= 1:
                LOG.info(f"Applying SMOTENC with {len(cat_indices)} categorical features "
                         f"(sampling_strategy=0.3) ...")
                smotenc = SMOTENC(
                    categorical_features=cat_indices,
                    sampling_strategy=0.3,
                    random_state=42,
                )
                X_res, y_res = smotenc.fit_resample(X_train, y_train)
                LOG.info(f"SMOTENC resampled: {len(X_res):,} rows "
                         f"(pos={int(y_res.sum()):,}, neg={int(len(y_res) - y_res.sum()):,})")
                return X_res, y_res
            else:
                raise ValueError("No categorical features found for SMOTENC")

        except Exception as e:
            LOG.warning(f"SMOTENC failed ({e}), falling back to SMOTE ...")
            smote = SMOTE(sampling_strategy=0.3, random_state=42)
            X_res, y_res = smote.fit_resample(X_train, y_train)
            LOG.info(f"SMOTE resampled: {len(X_res):,} rows "
                     f"(pos={int(y_res.sum()):,}, neg={int(len(y_res) - y_res.sum()):,})")
            return X_res, y_res

    except ImportError:
        LOG.warning("imbalanced-learn not installed; using built-in class weights only.")
    except Exception as e:
        LOG.warning(f"SMOTE failed ({e}); using built-in class weights only.")

    return X_res, y_res


# ═══════════════════════════════════════════════════════════════════════════════
#  3 — Train Models
# ═══════════════════════════════════════════════════════════════════════════════

def train_xgboost(X_train, y_train, X_eval, y_eval, num_pos, num_neg):
    """Train XGBoost with early stopping."""
    from xgboost import XGBClassifier

    LOG.info("Training XGBoost ...")
    t0 = time.time()

    model = XGBClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        scale_pos_weight=num_neg / max(num_pos, 1),
        eval_metric="aucpr",
        subsample=0.8,
        colsample_bytree=0.7,
        reg_alpha=0.1,
        reg_lambda=1.0,
        early_stopping_rounds=30,
        random_state=42,
        n_jobs=-1,
        tree_method="hist",
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_eval, y_eval)],
        verbose=False,
    )

    elapsed = time.time() - t0
    best_iter = getattr(model, "best_iteration", model.n_estimators)
    LOG.info(f"XGBoost trained in {elapsed:.1f}s  "
             f"(best_iteration={best_iter})")
    return model, elapsed


def _focal_obj_default(y_true, y_pred):
    """Module-level focal loss objective (gamma=2.0, alpha=0.25) for pickling."""
    return focal_loss_objective(y_true, y_pred, gamma=2.0, alpha=0.25)


def train_xgboost_focal(X_train, y_train, X_eval, y_eval):
    """Train XGBoost with focal loss (gamma=2.0, alpha=0.25)."""
    from xgboost import XGBClassifier

    LOG.info("Training XGBoost (Focal Loss) ...")
    t0 = time.time()

    model = XGBClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        objective=_focal_obj_default,
        eval_metric="aucpr",
        subsample=0.8,
        colsample_bytree=0.7,
        reg_alpha=0.1,
        reg_lambda=1.0,
        early_stopping_rounds=30,
        random_state=42,
        n_jobs=-1,
        tree_method="hist",
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_eval, y_eval)],
        verbose=False,
    )

    elapsed = time.time() - t0
    best_iter = getattr(model, "best_iteration", model.n_estimators)
    LOG.info(f"XGBoost (Focal) trained in {elapsed:.1f}s  "
             f"(best_iteration={best_iter})")
    return model, elapsed


def train_lightgbm(X_train, y_train, X_eval, y_eval):
    """Train LightGBM with early stopping."""
    import lightgbm

    LOG.info("Training LightGBM ...")
    t0 = time.time()

    model = lightgbm.LGBMClassifier(
        n_estimators=500,
        num_leaves=31,
        learning_rate=0.05,
        is_unbalance=True,
        subsample=0.8,
        colsample_bytree=0.7,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_eval, y_eval)],
        callbacks=[
            lightgbm.early_stopping(30),
            lightgbm.log_evaluation(0),
        ],
    )

    elapsed = time.time() - t0
    best_iter = getattr(model, "best_iteration_", model.n_estimators)
    LOG.info(f"LightGBM trained in {elapsed:.1f}s  "
             f"(best_iteration={best_iter})")
    return model, elapsed


def train_random_forest(X_train, y_train):
    """Train Random Forest baseline."""
    LOG.info("Training Random Forest ...")
    t0 = time.time()

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=12,
        min_samples_leaf=5,
        class_weight="balanced",
        max_features="sqrt",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    elapsed = time.time() - t0
    LOG.info(f"Random Forest trained in {elapsed:.1f}s")
    return model, elapsed


# ═══════════════════════════════════════════════════════════════════════════════
#  4 — Cross-validation (XGBoost PR-AUC)
# ═══════════════════════════════════════════════════════════════════════════════

def xgboost_cv(X_full, y_full, num_pos, num_neg):
    """Stratified 5-fold CV for XGBoost, returning mean PR-AUC."""
    from xgboost import XGBClassifier

    LOG.info("Running 5-fold stratified CV for XGBoost PR-AUC ...")
    t0 = time.time()

    model = XGBClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        scale_pos_weight=num_neg / max(num_pos, 1),
        eval_metric="aucpr",
        subsample=0.8,
        colsample_bytree=0.7,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        tree_method="hist",
    )

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_val_score(model, X_full, y_full, cv=cv,
                             scoring="average_precision", n_jobs=-1)

    elapsed = time.time() - t0
    LOG.info(f"CV completed in {elapsed:.1f}s  "
             f"PR-AUC: {scores.mean():.4f} +/- {scores.std():.4f}")
    return {
        "cv_pr_auc_mean": float(scores.mean()),
        "cv_pr_auc_std": float(scores.std()),
        "cv_pr_auc_folds": [float(s) for s in scores],
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  5 — Feature Importance
# ═══════════════════════════════════════════════════════════════════════════════

def extract_feature_importance(xgb_model, lgbm_model, rf_model, feature_cols):
    """Build a combined feature importance DataFrame."""
    fi = pd.DataFrame({"feature": feature_cols})

    # XGBoost
    fi["xgb_importance"] = xgb_model.feature_importances_

    # LightGBM
    fi["lgbm_importance"] = lgbm_model.feature_importances_

    # Random Forest
    fi["rf_importance"] = rf_model.feature_importances_

    # Sort by XGBoost importance (primary model)
    fi = fi.sort_values("xgb_importance", ascending=False).reset_index(drop=True)
    return fi


# ═══════════════════════════════════════════════════════════════════════════════
#  6 — Console Summary
# ═══════════════════════════════════════════════════════════════════════════════

def print_summary(all_metrics, thresholds, fi, class_dist, timings, cv_results):
    """Print a formatted summary to the console."""
    print()
    print("=" * 78)
    print("  I-485 SUPERVISED FRAUD DETECTION — RESULTS SUMMARY")
    print("=" * 78)

    # --- Class Distribution ---
    print()
    print("  CLASS DISTRIBUTION")
    print("  " + "-" * 40)
    for label, count in class_dist.items():
        if isinstance(count, (int, float)) and not isinstance(count, bool):
            print(f"    {label:14s}  {count:>10,}")
        else:
            print(f"    {label:14s}  {str(count):>10s}")

    # --- Model Metrics Comparison ---
    print()
    print("  MODEL METRICS COMPARISON")
    print("  " + "-" * 74)
    header = (f"  {'Model':<16s} {'PR-AUC':>8s} {'ROC-AUC':>8s} "
              f"{'F1':>8s} {'F2':>8s} {'Prec':>8s} {'Recall':>8s} "
              f"{'Time(s)':>8s}")
    print(header)
    print("  " + "-" * 74)
    for m in all_metrics:
        name = m["model"]
        t = timings.get(name, 0.0)
        print(f"  {name:<16s} {m['PR_AUC']:>8.4f} {m['ROC_AUC']:>8.4f} "
              f"{m['F1']:>8.4f} {m['F2']:>8.4f} {m['Precision']:>8.4f} "
              f"{m['Recall']:>8.4f} {t:>8.1f}")
    print()

    # --- Cross-validation ---
    if cv_results:
        print(f"  XGBoost 5-Fold CV PR-AUC: "
              f"{cv_results['cv_pr_auc_mean']:.4f} +/- {cv_results['cv_pr_auc_std']:.4f}")
        folds_str = ", ".join(f"{s:.4f}" for s in cv_results["cv_pr_auc_folds"])
        print(f"    Folds: [{folds_str}]")
        print()

    # --- Optimal Thresholds ---
    print("  OPTIMAL THRESHOLDS (maximising F2)")
    print("  " + "-" * 50)
    for name, info in thresholds.items():
        print(f"    {name:<16s}  threshold={info['threshold']:.4f}  "
              f"F2={info['f2']:.4f}  P={info['precision_at_threshold']:.4f}  "
              f"R={info['recall_at_threshold']:.4f}")
    print()

    # --- Top 20 XGBoost Features ---
    print("  TOP 20 FEATURES (XGBoost)")
    print("  " + "-" * 55)
    for _, row in fi.head(20).iterrows():
        bar_len = int(row["xgb_importance"] / max(fi["xgb_importance"].max(), 1e-9) * 30)
        bar = "#" * bar_len
        print(f"    {row['feature']:35s} {row['xgb_importance']:.4f}  {bar}")
    print()

    # --- Confusion Matrices ---
    print("  CONFUSION MATRICES (at optimal threshold)")
    print("  " + "-" * 50)
    for m in all_metrics:
        cm = m["confusion_matrix"]
        print(f"    {m['model']}:")
        print(f"      TN={cm[0][0]:>6,}  FP={cm[0][1]:>6,}")
        print(f"      FN={cm[1][0]:>6,}  TP={cm[1][1]:>6,}")
        print()

    print("=" * 78)


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    t_start = time.time()

    # Ensure output directories exist
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    # ── 1. Load ───────────────────────────────────────────────────────────────
    features, labels = load_data()

    # ── 2. Prepare ────────────────────────────────────────────────────────────
    (X_train_res, y_train_res,
     X_train_orig, y_train_orig,
     X_test, y_test,
     ids_test, feature_cols,
     num_pos, num_neg) = prepare_data(features, labels)

    class_dist = {
        "Negative (0)": num_neg,
        "Positive (1)": num_pos,
        "Total": num_neg + num_pos,
        "Prevalence": f"{num_pos / max(num_neg + num_pos, 1) * 100:.2f}%",
    }

    timings = {}

    # ── 3. Train Models ──────────────────────────────────────────────────────

    # XGBoost
    xgb_model, xgb_time = train_xgboost(
        X_train_res, y_train_res, X_test, y_test, num_pos, num_neg,
    )
    timings["XGBoost"] = xgb_time

    # LightGBM
    lgbm_model, lgbm_time = train_lightgbm(
        X_train_res, y_train_res, X_test, y_test,
    )
    timings["LightGBM"] = lgbm_time

    # Random Forest
    rf_model, rf_time = train_random_forest(X_train_res, y_train_res)
    timings["RandomForest"] = rf_time

    # XGBoost with Focal Loss (E7)
    xgb_focal_model, xgb_focal_time = train_xgboost_focal(
        X_train_res, y_train_res, X_test, y_test,
    )
    timings["XGBoost_Focal"] = xgb_focal_time

    # ── 4. Predict on test set ────────────────────────────────────────────────
    xgb_prob = xgb_model.predict_proba(X_test)[:, 1]
    lgbm_prob = lgbm_model.predict_proba(X_test)[:, 1]
    rf_prob = rf_model.predict_proba(X_test)[:, 1]
    xgb_focal_prob = xgb_focal_model.predict_proba(X_test)[:, 1]

    # ── 5. Threshold Optimization ─────────────────────────────────────────────
    thresholds = {}

    xgb_thresh, xgb_thresh_info = find_optimal_threshold(y_test, xgb_prob)
    thresholds["XGBoost"] = xgb_thresh_info

    lgbm_thresh, lgbm_thresh_info = find_optimal_threshold(y_test, lgbm_prob)
    thresholds["LightGBM"] = lgbm_thresh_info

    rf_thresh, rf_thresh_info = find_optimal_threshold(y_test, rf_prob)
    thresholds["RandomForest"] = rf_thresh_info

    xgb_focal_thresh, xgb_focal_thresh_info = find_optimal_threshold(
        y_test, xgb_focal_prob)
    thresholds["XGBoost_Focal"] = xgb_focal_thresh_info

    # ── 6. Evaluate ───────────────────────────────────────────────────────────
    all_metrics = [
        evaluate_model("XGBoost", y_test, xgb_prob, xgb_thresh),
        evaluate_model("LightGBM", y_test, lgbm_prob, lgbm_thresh),
        evaluate_model("RandomForest", y_test, rf_prob, rf_thresh),
        evaluate_model("XGBoost_Focal", y_test, xgb_focal_prob, xgb_focal_thresh),
    ]

    # ── 7. Cross-validation (XGBoost) ─────────────────────────────────────────
    # Use the original (non-SMOTE) full training+test data for CV
    feature_col_list = [c for c in features.columns if c != "application_id"]
    merged_for_cv = features.merge(
        labels[["application_id", "is_fraud"]], on="application_id", how="inner",
    )
    X_full = merged_for_cv[feature_col_list].values.astype(np.float32)
    y_full = merged_for_cv["is_fraud"].values.astype(int)
    cv_results = xgboost_cv(X_full, y_full, num_pos, num_neg)

    # Attach CV results to XGBoost metrics
    all_metrics[0]["cv_pr_auc_mean"] = cv_results["cv_pr_auc_mean"]
    all_metrics[0]["cv_pr_auc_std"] = cv_results["cv_pr_auc_std"]
    all_metrics[0]["cv_pr_auc_folds"] = cv_results["cv_pr_auc_folds"]

    # ── 8. Feature Importance ─────────────────────────────────────────────────
    fi = extract_feature_importance(xgb_model, lgbm_model, rf_model, feature_cols)
    fi["xgb_focal_importance"] = xgb_focal_model.feature_importances_

    # ── 9. Save Artifacts ─────────────────────────────────────────────────────
    LOG.info("Saving model artifacts ...")

    # Models
    joblib.dump(xgb_model, MODEL_DIR / "xgboost.joblib")
    LOG.info(f"  Saved {MODEL_DIR / 'xgboost.joblib'}")

    joblib.dump(lgbm_model, MODEL_DIR / "lightgbm.joblib")
    LOG.info(f"  Saved {MODEL_DIR / 'lightgbm.joblib'}")

    joblib.dump(rf_model, MODEL_DIR / "rf.joblib")
    LOG.info(f"  Saved {MODEL_DIR / 'rf.joblib'}")

    joblib.dump(xgb_focal_model, MODEL_DIR / "xgboost_focal.joblib")
    LOG.info(f"  Saved {MODEL_DIR / 'xgboost_focal.joblib'}")

    # Metrics JSON
    # Convert numpy types so JSON can serialise them
    def _serialise(obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    metrics_path = RESULT_DIR / "supervised_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(all_metrics, f, indent=2, default=_serialise)
    LOG.info(f"  Saved {metrics_path}")

    # Optimal thresholds JSON
    thresh_path = RESULT_DIR / "optimal_thresholds.json"
    with open(thresh_path, "w") as f:
        json.dump(thresholds, f, indent=2, default=_serialise)
    LOG.info(f"  Saved {thresh_path}")

    # Feature importance parquet
    fi_path = RESULT_DIR / "feature_importance.parquet"
    fi.to_parquet(fi_path, index=False)
    LOG.info(f"  Saved {fi_path}")

    # Predictions parquet
    preds = pd.DataFrame({
        "application_id": ids_test,
        "y_true": y_test,
        "xgb_prob": xgb_prob,
        "lgbm_prob": lgbm_prob,
        "rf_prob": rf_prob,
        "xgb_focal_prob": xgb_focal_prob,
    })
    preds_path = RESULT_DIR / "supervised_predictions.parquet"
    preds.to_parquet(preds_path, index=False)
    LOG.info(f"  Saved {preds_path}")

    # ── 10. Console Summary ───────────────────────────────────────────────────
    print_summary(all_metrics, thresholds, fi, class_dist, timings, cv_results)

    elapsed = time.time() - t_start
    LOG.info(f"Pipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
