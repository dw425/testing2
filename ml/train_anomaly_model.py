"""
Train a CNC tolerance anomaly detection model using GradientBoostingClassifier.

Reads training data from the silver layer, trains the model, logs metrics
and artifacts to MLflow, and computes SHAP feature importance.
"""

import logging
from typing import List

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import shap
from pyspark.sql import SparkSession
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

FEATURE_COLUMNS: List[str] = [
    "vibration_hz",
    "temp_c",
    "spindle_rpm",
    "tool_wear_index",
    "feed_rate",
]
TARGET_COLUMN: str = "is_anomalous"
SOURCE_TABLE: str = "manufacturing_iq.silver.cnc_anomalies"
EXPERIMENT_NAME: str = "/ManufacturingIQ/CNC_Tolerance_Anomaly"

MODEL_PARAMS = {
    "n_estimators": 300,
    "max_depth": 6,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "min_samples_split": 20,
    "min_samples_leaf": 10,
    "max_features": "sqrt",
    "random_state": 42,
}


def _load_training_data(spark: SparkSession) -> pd.DataFrame:
    """Read CNC anomaly data from the silver table and return as a pandas DataFrame."""
    logger.info("Reading training data from %s", SOURCE_TABLE)
    df_spark = spark.table(SOURCE_TABLE)
    required_cols = FEATURE_COLUMNS + [TARGET_COLUMN]
    missing = [c for c in required_cols if c not in df_spark.columns]
    if missing:
        raise ValueError(f"Missing columns in source table: {missing}")

    df = df_spark.select(*required_cols).dropna().toPandas()
    logger.info("Loaded %d rows for training", len(df))
    return df


def _train_model(
    X_train: np.ndarray,
    y_train: np.ndarray,
) -> GradientBoostingClassifier:
    """Fit a GradientBoostingClassifier on the training split."""
    logger.info("Training GradientBoostingClassifier with params: %s", MODEL_PARAMS)
    clf = GradientBoostingClassifier(**MODEL_PARAMS)
    clf.fit(X_train, y_train)
    return clf


def _evaluate_model(
    clf: GradientBoostingClassifier,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> dict:
    """Compute evaluation metrics on the test set."""
    y_pred = clf.predict(X_test)
    y_proba = clf.predict_proba(X_test)[:, 1]

    metrics = {
        "f1_score": f1_score(y_test, y_pred),
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "roc_auc": roc_auc_score(y_test, y_proba),
    }
    logger.info("Evaluation metrics: %s", metrics)
    return metrics


def _compute_shap_importance(
    clf: GradientBoostingClassifier,
    X_test: np.ndarray,
    feature_names: List[str],
) -> pd.DataFrame:
    """Compute SHAP values with TreeExplainer and return feature importance."""
    logger.info("Computing SHAP values with TreeExplainer")
    explainer = shap.TreeExplainer(clf)
    shap_values = explainer.shap_values(X_test)

    mean_abs_shap = np.abs(shap_values).mean(axis=0)
    importance_df = pd.DataFrame(
        {"feature": feature_names, "mean_abs_shap": mean_abs_shap}
    ).sort_values("mean_abs_shap", ascending=False)

    return importance_df


def main() -> None:
    """End-to-end training pipeline for the CNC anomaly detection model."""
    spark = SparkSession.builder.getOrCreate()

    mlflow.set_experiment(EXPERIMENT_NAME)

    df = _load_training_data(spark)
    X = df[FEATURE_COLUMNS].values
    y = df[TARGET_COLUMN].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    with mlflow.start_run(run_name="cnc_anomaly_gbc") as run:
        logger.info("MLflow run ID: %s", run.info.run_id)

        # Log parameters
        mlflow.log_params(MODEL_PARAMS)
        mlflow.log_param("feature_columns", FEATURE_COLUMNS)
        mlflow.log_param("source_table", SOURCE_TABLE)
        mlflow.log_param("train_rows", len(X_train))
        mlflow.log_param("test_rows", len(X_test))

        # Train
        clf = _train_model(X_train, y_train)

        # Evaluate
        metrics = _evaluate_model(clf, X_test, y_test)
        mlflow.log_metrics(metrics)

        # Log model
        mlflow.sklearn.log_model(
            sk_model=clf,
            artifact_path="cnc_anomaly_model",
            input_example=X_test[:5],
        )

        # SHAP feature importance
        importance_df = _compute_shap_importance(clf, X_test, FEATURE_COLUMNS)
        importance_path = "/tmp/shap_feature_importance.csv"
        importance_df.to_csv(importance_path, index=False)
        mlflow.log_artifact(importance_path, artifact_path="shap")

        logger.info(
            "Training complete. Run ID: %s | F1: %.4f | AUC: %.4f",
            run.info.run_id,
            metrics["f1_score"],
            metrics["roc_auc"],
        )


if __name__ == "__main__":
    main()
