"""
Train inventory demand forecast models per site/component using Prophet.

Reads enriched order data from the silver layer, trains a separate Prophet
model for each (site, component) combination, logs to MLflow, and writes
forecasts to the gold layer.
"""

import logging
from typing import Dict, Tuple

import mlflow
import mlflow.prophet
import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
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

SOURCE_TABLE: str = "manufacturing_iq.silver.enriched_orders"
TARGET_TABLE: str = "manufacturing_iq.gold.inventory_forecast"
EXPERIMENT_NAME: str = "/ManufacturingIQ/Inventory_Demand_Forecast"

FORECAST_HORIZON_DAYS: int = 90
PROPHET_PARAMS: Dict = {
    "changepoint_prior_scale": 0.05,
    "seasonality_prior_scale": 10.0,
    "seasonality_mode": "multiplicative",
    "yearly_seasonality": True,
    "weekly_seasonality": True,
    "daily_seasonality": False,
}

FORECAST_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), False),
        StructField("component_id", StringType(), False),
        StructField("ds", TimestampType(), False),
        StructField("yhat", DoubleType(), False),
        StructField("yhat_lower", DoubleType(), False),
        StructField("yhat_upper", DoubleType(), False),
    ]
)


def _load_order_data(spark: SparkSession) -> pd.DataFrame:
    """Read enriched orders and aggregate daily demand per site/component."""
    logger.info("Reading order data from %s", SOURCE_TABLE)
    df_spark = (
        spark.table(SOURCE_TABLE)
        .select("site_id", "component_id", "order_date", "quantity")
        .dropna()
    )

    df_agg = (
        df_spark.groupBy("site_id", "component_id", "order_date")
        .agg(F.sum("quantity").alias("y"))
        .withColumnRenamed("order_date", "ds")
        .orderBy("site_id", "component_id", "ds")
    )

    pdf = df_agg.toPandas()
    pdf["ds"] = pd.to_datetime(pdf["ds"])
    logger.info(
        "Loaded %d daily demand records across %d site/component groups",
        len(pdf),
        pdf.groupby(["site_id", "component_id"]).ngroups,
    )
    return pdf


def _train_prophet_model(
    group_df: pd.DataFrame,
    site_id: str,
    component_id: str,
) -> Tuple[Prophet, pd.DataFrame, Dict]:
    """Train a Prophet model for a single site/component and return the forecast."""
    prophet_df = group_df[["ds", "y"]].copy()
    prophet_df = prophet_df.sort_values("ds").reset_index(drop=True)

    model = Prophet(**PROPHET_PARAMS)
    model.fit(prophet_df)

    future = model.make_future_dataframe(periods=FORECAST_HORIZON_DAYS, freq="D")
    forecast = model.predict(future)

    # Cross-validation for metrics (only if sufficient history)
    history_days = (prophet_df["ds"].max() - prophet_df["ds"].min()).days
    metrics = {}
    if history_days >= 180:
        try:
            cv_results = cross_validation(
                model, initial="90 days", period="30 days", horizon="30 days"
            )
            perf = performance_metrics(cv_results)
            metrics = {
                "mae": perf["mae"].mean(),
                "rmse": perf["rmse"].mean(),
                "mape": perf["mape"].mean(),
            }
        except Exception as exc:
            logger.warning(
                "Cross-validation failed for %s/%s: %s", site_id, component_id, exc
            )
    else:
        logger.info(
            "Skipping cross-validation for %s/%s — only %d days of history",
            site_id,
            component_id,
            history_days,
        )

    return model, forecast, metrics


def _build_forecast_rows(
    forecast: pd.DataFrame,
    site_id: str,
    component_id: str,
) -> pd.DataFrame:
    """Extract the columns we need for the gold forecast table."""
    result = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    result["site_id"] = site_id
    result["component_id"] = component_id
    return result[["site_id", "component_id", "ds", "yhat", "yhat_lower", "yhat_upper"]]


def main() -> None:
    """Train Prophet models for every site/component and write forecasts to gold."""
    spark = SparkSession.builder.getOrCreate()
    mlflow.set_experiment(EXPERIMENT_NAME)

    pdf = _load_order_data(spark)
    groups = pdf.groupby(["site_id", "component_id"])

    all_forecasts = []
    total_groups = len(groups)
    logger.info("Training %d Prophet models", total_groups)

    for idx, ((site_id, component_id), group_df) in enumerate(groups, start=1):
        logger.info(
            "[%d/%d] Training model for site=%s component=%s (%d rows)",
            idx,
            total_groups,
            site_id,
            component_id,
            len(group_df),
        )

        with mlflow.start_run(
            run_name=f"forecast_{site_id}_{component_id}", nested=True
        ) as run:
            # Log parameters
            mlflow.log_params(PROPHET_PARAMS)
            mlflow.log_param("site_id", site_id)
            mlflow.log_param("component_id", component_id)
            mlflow.log_param("training_rows", len(group_df))
            mlflow.log_param("forecast_horizon_days", FORECAST_HORIZON_DAYS)

            model, forecast, metrics = _train_prophet_model(
                group_df, site_id, component_id
            )

            # Log metrics
            if metrics:
                mlflow.log_metrics(metrics)

            # Log model
            mlflow.prophet.log_model(
                pr_model=model,
                artifact_path="prophet_model",
            )

            forecast_rows = _build_forecast_rows(forecast, site_id, component_id)
            all_forecasts.append(forecast_rows)

            logger.info(
                "  Run ID: %s | Metrics: %s", run.info.run_id, metrics or "N/A"
            )

    # Combine all forecasts and write to gold table
    if all_forecasts:
        combined = pd.concat(all_forecasts, ignore_index=True)
        df_forecast = spark.createDataFrame(combined, schema=FORECAST_SCHEMA)

        df_forecast.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(TARGET_TABLE)

        logger.info(
            "Wrote %d forecast rows to %s", df_forecast.count(), TARGET_TABLE
        )
    else:
        logger.warning("No forecasts generated — no data groups found")


if __name__ == "__main__":
    main()
