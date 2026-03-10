"""
SHAP value computation and caching for the CNC anomaly detection model.

Loads the Production model from the MLflow registry, computes SHAP values
on the latest silver data, and writes aggregated feature importance metrics
to the gold layer for downstream dashboards and monitoring.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import mlflow
import numpy as np
import pandas as pd
import shap
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MODEL_NAME: str = "CNC_Tolerance_Anomaly"
MODEL_STAGE: str = "Production"

SOURCE_TABLE: str = "manufacturing_iq.silver.cnc_anomalies"
TARGET_TABLE: str = "manufacturing_iq.gold.model_health_metrics"

FEATURE_COLUMNS = [
    "vibration_hz",
    "temp_c",
    "spindle_rpm",
    "tool_wear_index",
    "feed_rate",
]

MAX_SAMPLE_SIZE: int = 50_000

HEALTH_SCHEMA = StructType(
    [
        StructField("model_name", StringType(), False),
        StructField("model_version", StringType(), False),
        StructField("feature_name", StringType(), False),
        StructField("mean_abs_shap", DoubleType(), False),
        StructField("median_abs_shap", DoubleType(), False),
        StructField("max_abs_shap", DoubleType(), False),
        StructField("importance_rank", DoubleType(), False),
        StructField("sample_size", DoubleType(), False),
        StructField("computed_at", TimestampType(), False),
    ]
)


def _load_production_model() -> tuple:
    """Load the Production-stage model from the MLflow registry.

    Returns:
        A tuple of (loaded_model, model_version_string).
    """
    model_uri = f"models:/{MODEL_NAME}/{MODEL_STAGE}"
    logger.info("Loading model from %s", model_uri)

    model = mlflow.sklearn.load_model(model_uri)

    client = mlflow.tracking.MlflowClient()
    latest_versions = client.get_latest_versions(MODEL_NAME, stages=[MODEL_STAGE])
    version_str = latest_versions[0].version if latest_versions else "unknown"

    logger.info("Loaded %s version %s", MODEL_NAME, version_str)
    return model, version_str


def _load_silver_data(
    spark: SparkSession, sample_size: Optional[int] = None
) -> pd.DataFrame:
    """Read the latest CNC anomaly data from the silver table.

    If the table has more rows than sample_size, a random sample is taken
    to keep SHAP computation tractable.
    """
    logger.info("Reading feature data from %s", SOURCE_TABLE)
    df_spark = spark.table(SOURCE_TABLE).select(*FEATURE_COLUMNS).dropna()

    total_rows = df_spark.count()
    effective_sample = sample_size or MAX_SAMPLE_SIZE

    if total_rows > effective_sample:
        fraction = effective_sample / total_rows
        logger.info(
            "Sampling %.2f%% of %d rows (target %d)",
            fraction * 100,
            total_rows,
            effective_sample,
        )
        df_spark = df_spark.sample(withReplacement=False, fraction=fraction, seed=42)

    pdf = df_spark.toPandas()
    logger.info("Loaded %d rows for SHAP computation", len(pdf))
    return pdf


def _compute_shap_values(model, X: np.ndarray) -> np.ndarray:
    """Compute SHAP values using TreeExplainer."""
    logger.info("Initializing TreeExplainer")
    explainer = shap.TreeExplainer(model)

    logger.info("Computing SHAP values for %d samples", X.shape[0])
    shap_values = explainer.shap_values(X)

    # For binary classifiers, shap_values may be a list of two arrays.
    # Use the positive-class SHAP values.
    if isinstance(shap_values, list):
        shap_values = shap_values[1]

    return shap_values


def _build_importance_dataframe(
    shap_values: np.ndarray,
    feature_names: list,
    model_version: str,
    sample_size: int,
) -> pd.DataFrame:
    """Aggregate SHAP values into per-feature importance metrics."""
    abs_shap = np.abs(shap_values)
    now = datetime.now(timezone.utc)

    records = []
    mean_abs = abs_shap.mean(axis=0)
    rank_order = np.argsort(-mean_abs)

    for rank_idx, feat_idx in enumerate(rank_order, start=1):
        records.append(
            {
                "model_name": MODEL_NAME,
                "model_version": str(model_version),
                "feature_name": feature_names[feat_idx],
                "mean_abs_shap": float(mean_abs[feat_idx]),
                "median_abs_shap": float(np.median(abs_shap[:, feat_idx])),
                "max_abs_shap": float(abs_shap[:, feat_idx].max()),
                "importance_rank": float(rank_idx),
                "sample_size": float(sample_size),
                "computed_at": now,
            }
        )

    return pd.DataFrame(records)


def compute_and_cache_shap(sample_size: Optional[int] = None) -> pd.DataFrame:
    """Compute SHAP values on the latest silver data and write to gold.

    This is the primary entry point for scheduled jobs and notebooks.

    Args:
        sample_size: Maximum number of rows to use for SHAP computation.
                     Defaults to MAX_SAMPLE_SIZE (50,000).

    Returns:
        A pandas DataFrame with the feature importance metrics that were written.
    """
    spark = SparkSession.builder.getOrCreate()

    model, model_version = _load_production_model()

    pdf = _load_silver_data(spark, sample_size=sample_size)
    X = pdf[FEATURE_COLUMNS].values

    shap_values = _compute_shap_values(model, X)

    importance_df = _build_importance_dataframe(
        shap_values, FEATURE_COLUMNS, model_version, len(pdf)
    )

    logger.info("Feature importance summary:\n%s", importance_df.to_string(index=False))

    # Write to gold table
    df_spark = spark.createDataFrame(importance_df, schema=HEALTH_SCHEMA)
    df_spark.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)

    logger.info("Wrote %d rows to %s", len(importance_df), TARGET_TABLE)

    return importance_df


if __name__ == "__main__":
    compute_and_cache_shap()
