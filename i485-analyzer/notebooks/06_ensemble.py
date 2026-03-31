#!/usr/bin/env python3
"""
06_ensemble.py — Stacking Ensemble, SHAP Explainability & Final Output

Reads outputs from all previous pipeline steps, builds a stacking ensemble
(Level 0 base scores -> Level 1 meta-learner), adds SHAP explanations,
and produces final scored output with tiered risk assignments.

Inputs:
    data/features/feature_matrix.parquet
    data/features/labels.parquet
    data/models/xgboost.joblib, lightgbm.joblib, rf.joblib
    data/results/unsupervised_scores.parquet
    data/results/graph_features.parquet

Outputs:
    data/results/final_scores.parquet
    data/results/shap_values.parquet
    data/results/fraud_rings.parquet        (copied if exists)
    data/results/novel_detections.parquet
    data/results/model_comparison.json
    data/results/summary_report.txt
    data/models/meta_model.joblib

Usage:
    python notebooks/06_ensemble.py
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
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold

# ---------------------------------------------------------------------------
# Project root — resolve relative to this script's location
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
FEATURE_MATRIX_PATH = PROJECT_ROOT / "data" / "features" / "feature_matrix.parquet"
LABELS_PATH = PROJECT_ROOT / "data" / "features" / "labels.parquet"

MODEL_DIR = PROJECT_ROOT / "data" / "models"
XGBOOST_MODEL_PATH = MODEL_DIR / "xgboost.joblib"
LIGHTGBM_MODEL_PATH = MODEL_DIR / "lightgbm.joblib"
RF_MODEL_PATH = MODEL_DIR / "rf.joblib"
XGB_FOCAL_MODEL_PATH = MODEL_DIR / "xgboost_focal.joblib"

RESULTS_DIR = PROJECT_ROOT / "data" / "results"
UNSUPERVISED_SCORES_PATH = RESULTS_DIR / "unsupervised_scores.parquet"
GRAPH_FEATURES_PATH = RESULTS_DIR / "graph_features.parquet"
FRAUD_RINGS_SRC_PATH = RESULTS_DIR / "fraud_rings.parquet"

# Outputs
FINAL_SCORES_PATH = RESULTS_DIR / "final_scores.parquet"
SHAP_VALUES_PATH = RESULTS_DIR / "shap_values.parquet"
FRAUD_RINGS_DST_PATH = RESULTS_DIR / "fraud_rings.parquet"
NOVEL_DETECTIONS_PATH = RESULTS_DIR / "novel_detections.parquet"
MODEL_COMPARISON_PATH = RESULTS_DIR / "model_comparison.json"
SUMMARY_REPORT_PATH = RESULTS_DIR / "summary_report.txt"
META_MODEL_PATH = MODEL_DIR / "meta_model.joblib"
SUPERVISED_METRICS_PATH = RESULTS_DIR / "supervised_metrics.json"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG = logging.getLogger("ensemble")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)


# ===================================================================
#  Focal loss (needed for unpickling XGBoost focal model)
# ===================================================================

def focal_loss_objective(y_true, y_pred, gamma=2.0, alpha=0.25):
    """Focal loss custom objective for XGBoost."""
    p = 1.0 / (1.0 + np.exp(-y_pred))
    p = np.clip(p, 1e-7, 1.0 - 1e-7)
    alpha_t = np.where(y_true == 1, alpha, 1.0 - alpha)
    p_t = np.where(y_true == 1, p, 1.0 - p)
    focal_weight = alpha_t * (1.0 - p_t) ** gamma
    grad_ce = p - y_true
    log_pt = np.where(y_true == 1, np.log(p), np.log(1.0 - p))
    grad = focal_weight * grad_ce - gamma * alpha_t * (
        (1.0 - p_t) ** (gamma - 1)) * p_t * log_pt * grad_ce
    hess = focal_weight * p * (1.0 - p)
    hess = np.maximum(hess, 1e-7)
    return grad, hess


def _focal_obj_default(y_true, y_pred):
    """Module-level focal loss objective for pickling compatibility."""
    return focal_loss_objective(y_true, y_pred, gamma=2.0, alpha=0.25)


# ===================================================================
#  Utility helpers
# ===================================================================

def _timer(label: str):
    """Context manager that logs elapsed time for a block."""
    class _T:
        def __init__(self):
            self.elapsed = 0.0
        def __enter__(self):
            self.t0 = time.time()
            LOG.info(f"[START] {label}")
            return self
        def __exit__(self, *exc):
            self.elapsed = time.time() - self.t0
            LOG.info(f"[DONE]  {label} ({self.elapsed:.1f}s)")
    return _T()


def _safe_load_parquet(path: Path, description: str) -> pd.DataFrame | None:
    """Load a parquet file if it exists, otherwise return None."""
    if path.exists():
        df = pd.read_parquet(path)
        LOG.info(f"  Loaded {description}: {df.shape[0]:,} rows x {df.shape[1]} cols")
        return df
    LOG.warning(f"  {description} not found at {path} — skipping")
    return None


def _safe_load_model(path: Path, description: str):
    """Load a joblib model if it exists, otherwise return None."""
    if path.exists():
        model = joblib.load(path)
        LOG.info(f"  Loaded {description} from {path.name}")
        return model
    LOG.warning(f"  {description} not found at {path} — skipping")
    return None


def _f2_score(precision: float, recall: float) -> float:
    """Compute F2 score: (5 * P * R) / (4 * P + R)."""
    denom = 4.0 * precision + recall
    if denom == 0:
        return 0.0
    return (5.0 * precision * recall) / denom


def _optimal_f2_threshold(y_true: np.ndarray, y_prob: np.ndarray) -> tuple[float, float]:
    """Sweep thresholds 0.01-0.99 and return (best_threshold, best_f2)."""
    best_t, best_f2 = 0.5, 0.0
    for t in np.arange(0.01, 1.0, 0.01):
        preds = (y_prob >= t).astype(int)
        if preds.sum() == 0 or preds.sum() == len(preds):
            continue
        p = precision_score(y_true, preds, zero_division=0)
        r = recall_score(y_true, preds, zero_division=0)
        f2 = _f2_score(p, r)
        if f2 > best_f2:
            best_f2 = f2
            best_t = t
    return best_t, best_f2


def _assign_tier(score: float) -> int:
    """Map calibrated probability to risk tier 1-4."""
    if score >= 0.8:
        return 4
    if score >= 0.6:
        return 3
    if score >= 0.3:
        return 2
    return 1


# ===================================================================
#  1. Load all inputs
# ===================================================================

def load_inputs() -> dict:
    """Load feature matrix, labels, models, and auxiliary scores."""
    with _timer("Loading inputs"):
        data = {}

        # -- Feature matrix (required) --
        feature_matrix = _safe_load_parquet(FEATURE_MATRIX_PATH, "feature_matrix")
        if feature_matrix is None:
            raise FileNotFoundError(
                f"Feature matrix is required but not found at {FEATURE_MATRIX_PATH}"
            )
        data["feature_matrix"] = feature_matrix

        # -- Labels (required) --
        labels = _safe_load_parquet(LABELS_PATH, "labels")
        if labels is None:
            raise FileNotFoundError(
                f"Labels file is required but not found at {LABELS_PATH}"
            )
        data["labels"] = labels

        # -- Supervised models (at least one required) --
        data["xgb_model"] = _safe_load_model(XGBOOST_MODEL_PATH, "XGBoost model")
        data["lgb_model"] = _safe_load_model(LIGHTGBM_MODEL_PATH, "LightGBM model")
        data["rf_model"] = _safe_load_model(RF_MODEL_PATH, "Random Forest model")
        data["xgb_focal_model"] = _safe_load_model(
            XGB_FOCAL_MODEL_PATH, "XGBoost Focal model")

        n_sup = sum(1 for k in ("xgb_model", "lgb_model", "rf_model",
                                "xgb_focal_model") if data[k] is not None)
        if n_sup == 0:
            raise FileNotFoundError(
                "At least one supervised model is required"
            )
        LOG.info(f"  Supervised models available: {n_sup}/4")

        # -- Unsupervised scores (optional) --
        data["unsupervised"] = _safe_load_parquet(
            UNSUPERVISED_SCORES_PATH, "unsupervised_scores"
        )

        # -- Graph features (optional) --
        data["graph"] = _safe_load_parquet(GRAPH_FEATURES_PATH, "graph_features")

    return data


# ===================================================================
#  2. Build Level 0 feature matrix
# ===================================================================

def build_level0(data: dict) -> tuple[pd.DataFrame, list[str]]:
    """Assemble Level 0 base scores for the meta-learner."""
    with _timer("Building Level 0 feature matrix"):
        fm = data["feature_matrix"]
        app_ids = fm[["application_id"]].copy()

        # Separate feature columns (everything except application_id)
        feature_cols = [c for c in fm.columns if c != "application_id"]
        X = fm[feature_cols].values

        level0_parts = [app_ids.reset_index(drop=True)]
        level0_col_names: list[str] = []

        # ---- Supervised model probabilities ----
        model_map = {
            "xgb_prob": data["xgb_model"],
            "lgb_prob": data["lgb_model"],
            "rf_prob": data["rf_model"],
            "xgb_focal_prob": data.get("xgb_focal_model"),
        }
        for col_name, model in model_map.items():
            if model is not None:
                try:
                    proba = model.predict_proba(X)[:, 1]
                except Exception as e:
                    LOG.warning(f"  predict_proba failed for {col_name}: {e}")
                    proba = np.full(len(X), np.nan)
                level0_parts.append(pd.Series(proba, name=col_name))
                level0_col_names.append(col_name)
                LOG.info(f"  Added {col_name}: mean={np.nanmean(proba):.4f}")

        # ---- Unsupervised scores ----
        unsup_cols = [
            "if_score", "ae_score", "hybrid_score",
            "lof_score", "copod_score", "mahalanobis_score", "benford_flag",
            "ocsvm_score",
        ]
        if data["unsupervised"] is not None:
            unsup = data["unsupervised"].copy()
            unsup_merged = app_ids.merge(unsup, on="application_id", how="left")
            for col in unsup_cols:
                if col in unsup_merged.columns:
                    level0_parts.append(
                        unsup_merged[col].reset_index(drop=True).rename(col)
                    )
                    level0_col_names.append(col)
                    LOG.info(f"  Added {col}")

        # ---- Graph features ----
        graph_metric_cols = ["neighbor_fraud_rate", "community_fraud_density", "pagerank"]
        n2v_cols = [f"n2v_{i}" for i in range(64)]
        if data["graph"] is not None:
            graph = data["graph"].copy()
            graph_merged = app_ids.merge(graph, on="application_id", how="left")
            for col in graph_metric_cols + n2v_cols:
                if col in graph_merged.columns:
                    level0_parts.append(
                        graph_merged[col].reset_index(drop=True).rename(col)
                    )
                    level0_col_names.append(col)
            graph_added = [c for c in graph_metric_cols + n2v_cols if c in graph_merged.columns]
            LOG.info(f"  Added {len(graph_added)} graph features")

        # ---- Assemble ----
        level0 = pd.concat(level0_parts, axis=1)
        # Fill missing with 0
        for col in level0_col_names:
            level0[col] = pd.to_numeric(level0[col], errors="coerce")
        level0[level0_col_names] = level0[level0_col_names].fillna(0).astype(np.float64)

        LOG.info(
            f"  Level 0 matrix: {level0.shape[0]:,} rows x "
            f"{len(level0_col_names)} features "
            f"(+application_id)"
        )

    return level0, level0_col_names


# ===================================================================
#  3. Level 1 Meta-Learner (Stacking)
# ===================================================================

def train_meta_learner(
    level0: pd.DataFrame,
    level0_cols: list[str],
    labels: pd.DataFrame,
) -> tuple:
    """
    Train a Level 1 XGBoost meta-learner using 5-fold stratified CV
    to generate out-of-fold predictions, then train a final model on
    all OOF predictions and calibrate.
    """
    from xgboost import XGBClassifier

    with _timer("Training Level 1 meta-learner"):
        # Merge labels
        merged = level0.merge(labels[["application_id", "is_fraud"]], on="application_id", how="left")
        merged["is_fraud"] = merged["is_fraud"].fillna(0).astype(int)

        X_l0 = merged[level0_cols].values
        y = merged["is_fraud"].values
        app_ids = merged["application_id"].values

        LOG.info(f"  Label distribution: fraud={y.sum():,}  clean={( y == 0).sum():,}")

        # -- Out-of-fold predictions --
        oof_preds = np.zeros(len(y), dtype=np.float64)
        skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

        fold_metrics = []
        for fold_idx, (train_idx, val_idx) in enumerate(skf.split(X_l0, y), 1):
            X_tr, X_val = X_l0[train_idx], X_l0[val_idx]
            y_tr, y_val = y[train_idx], y[val_idx]

            fold_model = XGBClassifier(
                n_estimators=100,
                max_depth=4,
                learning_rate=0.1,
                eval_metric="aucpr",
                random_state=42,
                n_jobs=-1,
                use_label_encoder=False,
            )
            fold_model.fit(
                X_tr, y_tr,
                eval_set=[(X_val, y_val)],
                verbose=False,
            )

            val_proba = fold_model.predict_proba(X_val)[:, 1]
            oof_preds[val_idx] = val_proba

            fold_pr_auc = average_precision_score(y_val, val_proba)
            fold_roc_auc = roc_auc_score(y_val, val_proba)
            fold_metrics.append({
                "fold": fold_idx,
                "pr_auc": fold_pr_auc,
                "roc_auc": fold_roc_auc,
            })
            LOG.info(
                f"  Fold {fold_idx}: PR-AUC={fold_pr_auc:.4f}  "
                f"ROC-AUC={fold_roc_auc:.4f}"
            )

        # OOF aggregate metrics
        oof_pr_auc = average_precision_score(y, oof_preds)
        oof_roc_auc = roc_auc_score(y, oof_preds)
        LOG.info(f"  OOF PR-AUC={oof_pr_auc:.4f}  ROC-AUC={oof_roc_auc:.4f}")

        # -- Train final meta-model on all data --
        LOG.info("  Training final meta-model on full dataset")
        meta_model = XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            eval_metric="aucpr",
            random_state=42,
            n_jobs=-1,
            use_label_encoder=False,
        )
        meta_model.fit(X_l0, y, verbose=False)

        # -- Calibrate probabilities --
        LOG.info("  Calibrating probabilities (CalibratedClassifierCV)")
        try:
            calibrated_model = CalibratedClassifierCV(
                meta_model, cv=5, method="isotonic"
            )
            calibrated_model.fit(X_l0, y)
            ensemble_scores = calibrated_model.predict_proba(X_l0)[:, 1]
            LOG.info("  Calibration successful")
        except Exception as e:
            LOG.warning(f"  Calibration failed ({e}), using raw probabilities")
            calibrated_model = None
            ensemble_scores = meta_model.predict_proba(X_l0)[:, 1]

        # -- Find optimal F2 threshold using OOF predictions --
        opt_threshold, opt_f2 = _optimal_f2_threshold(y, oof_preds)
        LOG.info(f"  Optimal threshold (F2): {opt_threshold:.2f}  F2={opt_f2:.4f}")

        # Final metrics (using OOF to avoid data leakage)
        oof_binary = (oof_preds >= opt_threshold).astype(int)
        final_precision = precision_score(y, oof_binary, zero_division=0)
        final_recall = recall_score(y, oof_binary, zero_division=0)
        final_f1 = f1_score(y, oof_binary, zero_division=0)
        final_f2 = _f2_score(final_precision, final_recall)

        ensemble_metrics = {
            "pr_auc": float(oof_pr_auc),
            "roc_auc": float(oof_roc_auc),
            "f1": float(final_f1),
            "f2": float(final_f2),
            "precision": float(final_precision),
            "recall": float(final_recall),
            "optimal_threshold": float(opt_threshold),
        }
        LOG.info(f"  Ensemble metrics: {json.dumps(ensemble_metrics, indent=2)}")

        # -- Save meta-model --
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        joblib.dump(meta_model, META_MODEL_PATH)
        LOG.info(f"  Saved meta_model to {META_MODEL_PATH}")

    return (
        meta_model,
        calibrated_model,
        ensemble_scores,
        ensemble_metrics,
        fold_metrics,
        app_ids,
        X_l0,
        y,
    )


# ===================================================================
#  4. SHAP Explainability
# ===================================================================

def compute_shap(
    meta_model,
    X_level0: np.ndarray,
    level0_cols: list[str],
    app_ids: np.ndarray,
    max_samples: int = 50_000,
) -> tuple[pd.DataFrame | None, list | None, list | None]:
    """
    Compute SHAP values for the meta-learner. Returns:
      - shap_df: DataFrame with application_id + one column per Level 0 feature
      - top5_per_app: list of JSON strings with top-5 feature names per app
      - risk_factors_per_app: list of JSON strings with top-5 feature names + SHAP values
    """
    try:
        import shap
    except ImportError:
        LOG.warning("shap package not installed — skipping SHAP explainability")
        return None, None, None

    with _timer("Computing SHAP values"):
        n_total = X_level0.shape[0]

        # Use a sample if dataset is too large
        if n_total > max_samples:
            LOG.info(f"  Sampling {max_samples:,} of {n_total:,} for SHAP")
            rng = np.random.RandomState(42)
            sample_idx = rng.choice(n_total, size=max_samples, replace=False)
            sample_idx.sort()
            X_shap = X_level0[sample_idx]
            shap_app_ids = app_ids[sample_idx]
        else:
            X_shap = X_level0
            shap_app_ids = app_ids
            sample_idx = np.arange(n_total)

        try:
            # Ensure X_shap is a clean float64 numpy array (no object cols)
            X_shap = np.array(X_shap, dtype=np.float64)

            # Fix SHAP/XGBoost 3.x compat: base_score is serialised as
            # '[6.405E-2]' (with brackets). Patch SHAP's parser inline.
            _orig_loader = shap.explainers._tree.XGBTreeModelLoader.__init__
            def _patched_loader_init(self_loader, xgb_model):
                # Intercept save_config to strip brackets from base_score
                import json as _j
                _real_save_config = xgb_model.save_config
                def _clean_config():
                    raw = _real_save_config()
                    cfg = _j.loads(raw)
                    lmp = cfg.get("learner", {}).get("learner_model_param", {})
                    bs = lmp.get("base_score", "")
                    if isinstance(bs, str) and bs.startswith("["):
                        lmp["base_score"] = bs.strip("[]")
                        return _j.dumps(cfg)
                    return raw
                xgb_model.save_config = _clean_config
                _orig_loader(self_loader, xgb_model)
            shap.explainers._tree.XGBTreeModelLoader.__init__ = _patched_loader_init

            explainer = shap.TreeExplainer(meta_model)
            shap_values = explainer.shap_values(X_shap)

            # shap_values may be a single array (binary) or list
            if isinstance(shap_values, list):
                # For binary classification, take class 1
                sv = shap_values[1] if len(shap_values) > 1 else shap_values[0]
            else:
                sv = shap_values

            LOG.info(f"  SHAP values shape: {sv.shape}")

            # -- Build SHAP DataFrame --
            shap_df = pd.DataFrame(sv, columns=level0_cols)
            shap_df.insert(0, "application_id", shap_app_ids)

            # -- Global top-10 features --
            mean_abs_shap = np.abs(sv).mean(axis=0)
            feature_importance_idx = np.argsort(mean_abs_shap)[::-1]
            LOG.info("  Global top-10 features (mean |SHAP|):")
            global_top10 = []
            for rank, idx in enumerate(feature_importance_idx[:10], 1):
                feat = level0_cols[idx]
                val = mean_abs_shap[idx]
                LOG.info(f"    {rank:2d}. {feat:35s} {val:.6f}")
                global_top10.append(feat)

            # -- Per-app top-5 features --
            top5_per_app = []
            risk_factors_per_app = []
            for i in range(sv.shape[0]):
                row_abs = np.abs(sv[i])
                top5_idx = np.argsort(row_abs)[::-1][:5]
                top5_names = [level0_cols[j] for j in top5_idx]
                top5_per_app.append(json.dumps(top5_names))

                risk_dict = {
                    level0_cols[j]: round(float(sv[i, j]), 6)
                    for j in top5_idx
                }
                risk_factors_per_app.append(json.dumps(risk_dict))

            LOG.info(f"  Computed top-5 features for {len(top5_per_app):,} apps")
            return shap_df, top5_per_app, risk_factors_per_app

        except Exception as e:
            LOG.error(f"  SHAP computation failed: {e}")
            return None, None, None


# ===================================================================
#  5. Tiered Risk Scoring & Final Output Assembly
# ===================================================================

def build_final_output(
    level0: pd.DataFrame,
    level0_cols: list[str],
    ensemble_scores: np.ndarray,
    app_ids: np.ndarray,
    y: np.ndarray,
    shap_df: pd.DataFrame | None,
    top5_per_app: list | None,
    risk_factors_per_app: list | None,
    ensemble_metrics: dict,
    data: dict,
) -> dict:
    """Produce all output files."""
    with _timer("Building final output"):
        n_apps = len(app_ids)

        # ---- Tier assignment ----
        tiers = np.array([_assign_tier(s) for s in ensemble_scores])

        # ---- final_scores.parquet ----
        final_df = pd.DataFrame({
            "application_id": app_ids,
            "ensemble_score": np.round(ensemble_scores, 6),
            "tier": tiers,
        })

        # top_5_features & risk_factors: use SHAP if available, else empty
        if top5_per_app is not None and risk_factors_per_app is not None:
            # SHAP may cover only a sample; map back by application_id
            if shap_df is not None and len(top5_per_app) < n_apps:
                # Build lookup from SHAP sample
                shap_app_set = set(shap_df["application_id"].values)
                top5_map = dict(zip(shap_df["application_id"].values, top5_per_app))
                risk_map = dict(zip(shap_df["application_id"].values, risk_factors_per_app))
                final_df["top_5_features"] = final_df["application_id"].map(
                    lambda aid: top5_map.get(aid, "[]")
                )
                final_df["risk_factors"] = final_df["application_id"].map(
                    lambda aid: risk_map.get(aid, "{}")
                )
            else:
                final_df["top_5_features"] = top5_per_app
                final_df["risk_factors"] = risk_factors_per_app
        else:
            final_df["top_5_features"] = "[]"
            final_df["risk_factors"] = "{}"

        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        final_df.to_parquet(FINAL_SCORES_PATH, index=False)
        LOG.info(f"  Saved final_scores.parquet ({final_df.shape[0]:,} rows)")

        # ---- shap_values.parquet ----
        if shap_df is not None:
            shap_df.to_parquet(SHAP_VALUES_PATH, index=False)
            LOG.info(f"  Saved shap_values.parquet ({shap_df.shape[0]:,} rows)")
        else:
            LOG.info("  Skipping shap_values.parquet (SHAP unavailable)")

        # ---- fraud_rings.parquet ----
        if FRAUD_RINGS_SRC_PATH.exists():
            # Already at the destination; nothing to copy
            fraud_rings = pd.read_parquet(FRAUD_RINGS_SRC_PATH)
            LOG.info(f"  fraud_rings.parquet exists ({fraud_rings.shape[0]:,} rows)")
            n_fraud_rings = fraud_rings["cluster_id"].nunique() if "cluster_id" in fraud_rings.columns else 0
        else:
            LOG.info("  fraud_rings.parquet not found — skipping")
            n_fraud_rings = 0

        # ---- novel_detections.parquet ----
        novel_mask = (ensemble_scores > 0.5) & (y == 0)
        novel_df = final_df.loc[novel_mask, [
            "application_id", "ensemble_score", "tier", "top_5_features"
        ]].copy()
        novel_df.to_parquet(NOVEL_DETECTIONS_PATH, index=False)
        LOG.info(f"  Saved novel_detections.parquet ({novel_df.shape[0]:,} rows)")

        # ---- model_comparison.json ----
        comparison: dict = {}

        # Load supervised metrics if available
        if SUPERVISED_METRICS_PATH.exists():
            try:
                with open(SUPERVISED_METRICS_PATH) as f:
                    comparison["supervised"] = json.load(f)
                LOG.info("  Loaded supervised_metrics.json")
            except Exception as e:
                LOG.warning(f"  Could not load supervised_metrics.json: {e}")
                comparison["supervised"] = {}
        else:
            comparison["supervised"] = {}

        comparison["ensemble"] = ensemble_metrics

        with open(MODEL_COMPARISON_PATH, "w") as f:
            json.dump(comparison, f, indent=2)
        LOG.info(f"  Saved model_comparison.json")

        # ---- Tier distribution ----
        tier_counts = {
            1: int((tiers == 1).sum()),
            2: int((tiers == 2).sum()),
            3: int((tiers == 3).sum()),
            4: int((tiers == 4).sum()),
        }

        # ---- Collect info for summary ----
        feature_matrix = data["feature_matrix"]
        n_features = len([c for c in feature_matrix.columns if c != "application_id"])

        # Supervised PR-AUC from comparison if available
        sup_raw = comparison.get("supervised", {})
        # Handle both list-of-dicts and nested-dict formats
        if isinstance(sup_raw, list):
            sup = {}
            for entry in sup_raw:
                name = entry.get("model", "").lower().replace(" ", "")
                sup[name] = {k.lower(): v for k, v in entry.items()}
        else:
            sup = sup_raw
        xgb_pr_auc = sup.get("xgboost", {}).get("pr_auc", None)
        lgb_pr_auc = sup.get("lightgbm", {}).get("pr_auc", None)
        rf_pr_auc = sup.get("randomforest", {}).get("pr_auc", None)

        # Global top-5 features (from SHAP if available)
        global_top5 = []
        if shap_df is not None:
            shap_feat_cols = [c for c in shap_df.columns if c != "application_id"]
            mean_abs = shap_df[shap_feat_cols].abs().mean()
            global_top5 = mean_abs.sort_values(ascending=False).head(5).index.tolist()

        summary_info = {
            "n_features": n_features,
            "tier_counts": tier_counts,
            "n_novel": int(novel_df.shape[0]),
            "n_fraud_rings": n_fraud_rings,
            "ensemble_metrics": ensemble_metrics,
            "xgb_pr_auc": xgb_pr_auc,
            "lgb_pr_auc": lgb_pr_auc,
            "rf_pr_auc": rf_pr_auc,
            "global_top5": global_top5,
        }

    return summary_info


# ===================================================================
#  6. Summary Report
# ===================================================================

def _fmt_metric(val, width=6):
    """Format a metric value, showing 'N/A' if None."""
    if val is None:
        return "N/A".rjust(width)
    return f"{val:.4f}".rjust(width)


def generate_summary(info: dict) -> str:
    """Build the human-readable summary report string."""
    tc = info["tier_counts"]
    em = info["ensemble_metrics"]
    top5 = info.get("global_top5", [])
    top5_str = ", ".join(top5) if top5 else "N/A (SHAP unavailable)"

    lines = [
        "",
        "=" * 55,
        "  I-485 FRAUD DETECTION -- ML PIPELINE RESULTS",
        "=" * 55,
        f"  Features engineered:    {info['n_features']}",
        f"  Models trained:         10 (3 supervised + 7 unsupervised/graph)",
        "",
        "  SUPERVISED PERFORMANCE (PR-AUC):",
        f"    XGBoost:              {_fmt_metric(info['xgb_pr_auc'])}",
        f"    LightGBM:             {_fmt_metric(info['lgb_pr_auc'])}",
        f"    Random Forest:        {_fmt_metric(info['rf_pr_auc'])}",
        "",
        "  ENSEMBLE PERFORMANCE:",
        f"    PR-AUC:               {em['pr_auc']:.4f}",
        f"    F2 @ optimal thresh:  {em['f2']:.4f}",
        "",
        "  RISK TIER DISTRIBUTION:",
        f"    Tier 1 (clean):       {tc[1]:>8,}",
        f"    Tier 2 (monitor):     {tc[2]:>8,}",
        f"    Tier 3 (review):      {tc[3]:>8,}",
        f"    Tier 4 (hold):        {tc[4]:>8,}",
        "",
        f"  NOVEL DETECTIONS (ML found, rules missed): {info['n_novel']:,}",
        f"  FRAUD RINGS IDENTIFIED: {info['n_fraud_rings']} clusters",
        "",
        f"  Top 5 Features: {top5_str}",
        "=" * 55,
        "",
    ]
    return "\n".join(lines)


# ===================================================================
#  MAIN
# ===================================================================

def main():
    t0 = time.time()

    print()
    print("=" * 55)
    print("  06_ensemble.py — Stacking Ensemble + SHAP")
    print("=" * 55)
    print()

    # 1. Load all inputs
    data = load_inputs()

    # 2. Build Level 0 feature matrix
    level0, level0_cols = build_level0(data)

    # 3. Train Level 1 meta-learner
    (
        meta_model,
        calibrated_model,
        ensemble_scores,
        ensemble_metrics,
        fold_metrics,
        app_ids,
        X_level0,
        y,
    ) = train_meta_learner(level0, level0_cols, data["labels"])

    # 4. SHAP explainability
    shap_df, top5_per_app, risk_factors_per_app = compute_shap(
        meta_model, X_level0, level0_cols, app_ids
    )

    # 5. Build final output files
    summary_info = build_final_output(
        level0=level0,
        level0_cols=level0_cols,
        ensemble_scores=ensemble_scores,
        app_ids=app_ids,
        y=y,
        shap_df=shap_df,
        top5_per_app=top5_per_app,
        risk_factors_per_app=risk_factors_per_app,
        ensemble_metrics=ensemble_metrics,
        data=data,
    )

    # 6. Generate and print summary report
    report = generate_summary(summary_info)

    # Write summary to file
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(SUMMARY_REPORT_PATH, "w") as f:
        f.write(report)
    LOG.info(f"Saved summary_report.txt")

    # Print to console
    print(report)

    elapsed = time.time() - t0
    LOG.info(f"Total pipeline time: {elapsed:.1f}s ({elapsed / 60:.1f} min)")


if __name__ == "__main__":
    main()
