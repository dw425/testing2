"""
Register trained MLflow models and transition them to the Production stage.

Retrieves the latest run IDs from the relevant MLflow experiments, registers
the models in the MLflow Model Registry, and promotes them to Production.
"""

import logging
import sys
from typing import Optional

import mlflow
from mlflow.tracking import MlflowClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ANOMALY_EXPERIMENT: str = "/ManufacturingIQ/CNC_Tolerance_Anomaly"
FORECAST_EXPERIMENT: str = "/ManufacturingIQ/Inventory_Demand_Forecast"

ANOMALY_MODEL_NAME: str = "CNC_Tolerance_Anomaly"
FORECAST_MODEL_NAME: str = "Inventory_Demand_Forecast"

ANOMALY_ARTIFACT_PATH: str = "cnc_anomaly_model"
FORECAST_ARTIFACT_PATH: str = "prophet_model"

PRODUCTION_STAGE: str = "Production"
ARCHIVE_STAGE: str = "Archived"


def _get_latest_run_id(client: MlflowClient, experiment_name: str) -> Optional[str]:
    """Retrieve the run ID of the most recent successful run for an experiment."""
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        logger.error("Experiment '%s' not found", experiment_name)
        return None

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string="status = 'FINISHED'",
        order_by=["start_time DESC"],
        max_results=1,
    )

    if not runs:
        logger.error("No finished runs found for experiment '%s'", experiment_name)
        return None

    run_id = runs[0].info.run_id
    logger.info(
        "Latest run for '%s': %s (started %s)",
        experiment_name,
        run_id,
        runs[0].info.start_time,
    )
    return run_id


def _register_and_promote(
    client: MlflowClient,
    run_id: str,
    artifact_path: str,
    model_name: str,
) -> int:
    """Register a model version from a run and transition it to Production.

    Any existing Production versions are archived first.

    Returns:
        The new model version number.
    """
    model_uri = f"runs:/{run_id}/{artifact_path}"
    logger.info("Registering model '%s' from URI: %s", model_name, model_uri)

    model_version = mlflow.register_model(model_uri=model_uri, name=model_name)
    new_version = int(model_version.version)
    logger.info("Registered '%s' version %d", model_name, new_version)

    # Archive any existing Production versions
    existing_versions = client.get_latest_versions(model_name, stages=[PRODUCTION_STAGE])
    for ev in existing_versions:
        if int(ev.version) != new_version:
            logger.info(
                "Archiving previous Production version %s of '%s'",
                ev.version,
                model_name,
            )
            client.transition_model_version_stage(
                name=model_name,
                version=ev.version,
                stage=ARCHIVE_STAGE,
            )

    # Promote the new version to Production
    client.transition_model_version_stage(
        name=model_name,
        version=str(new_version),
        stage=PRODUCTION_STAGE,
    )
    logger.info(
        "Transitioned '%s' version %d to %s", model_name, new_version, PRODUCTION_STAGE
    )

    return new_version


def main() -> None:
    """Register the anomaly and forecast models and promote to Production."""
    client = MlflowClient()

    # --- CNC Tolerance Anomaly model ---
    anomaly_run_id = _get_latest_run_id(client, ANOMALY_EXPERIMENT)
    if anomaly_run_id is None:
        logger.error("Cannot register anomaly model — no valid run found")
        sys.exit(1)

    anomaly_version = _register_and_promote(
        client, anomaly_run_id, ANOMALY_ARTIFACT_PATH, ANOMALY_MODEL_NAME
    )

    # --- Inventory Demand Forecast model ---
    forecast_run_id = _get_latest_run_id(client, FORECAST_EXPERIMENT)
    if forecast_run_id is None:
        logger.error("Cannot register forecast model — no valid run found")
        sys.exit(1)

    forecast_version = _register_and_promote(
        client, forecast_run_id, FORECAST_ARTIFACT_PATH, FORECAST_MODEL_NAME
    )

    logger.info(
        "Registration complete. Anomaly model v%d, Forecast model v%d — both in %s",
        anomaly_version,
        forecast_version,
        PRODUCTION_STAGE,
    )


if __name__ == "__main__":
    main()
