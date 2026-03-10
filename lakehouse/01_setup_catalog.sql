-- =============================================================================
-- ManufacturingIQ — Unity Catalog DDL
-- Creates catalog, schemas (bronze / silver / gold), and all 11 Delta tables.
-- Idempotent: safe to re-run.
-- =============================================================================

-- -------------------------------------------------------------------------
-- Catalog
-- -------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS manufacturing_iq;
USE CATALOG manufacturing_iq;

-- -------------------------------------------------------------------------
-- Schemas
-- -------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw ingestion layer — append-only, minimal transformation.';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Curated layer — cleaned, enriched, and joined datasets.';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Aggregation layer — KPIs, forecasts, and dashboard-ready tables.';

-- =========================================================================
-- BRONZE TABLES
-- =========================================================================

-- 1. raw_iot_telemetry
CREATE TABLE IF NOT EXISTS bronze.raw_iot_telemetry (
  event_id            STRING        COMMENT 'Unique telemetry event identifier',
  timestamp           TIMESTAMP     COMMENT 'Event timestamp from the edge device',
  asset_id            STRING        COMMENT 'CNC machine / asset identifier',
  site                STRING        COMMENT 'Manufacturing site code',
  machine_type        STRING        COMMENT 'Machine category (e.g. CNC-5Axis)',
  vibration_hz        DOUBLE        COMMENT 'Vibration frequency in Hz',
  temp_c              DOUBLE        COMMENT 'Spindle temperature in Celsius',
  spindle_rpm         DOUBLE        COMMENT 'Spindle speed in RPM',
  tool_wear_index     DOUBLE        COMMENT 'Normalised tool-wear score 0-1',
  feed_rate           DOUBLE        COMMENT 'Feed rate mm/min',
  _ingested_at        TIMESTAMP     DEFAULT current_timestamp()
                                    COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw IoT sensor telemetry streamed from edge gateways.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 2. raw_erp_orders
CREATE TABLE IF NOT EXISTS bronze.raw_erp_orders (
  order_id            STRING        COMMENT 'ERP purchase-order identifier',
  batch_id            STRING        COMMENT 'Production batch identifier',
  component           STRING        COMMENT 'Component / part name',
  site                STRING        COMMENT 'Manufacturing site code',
  quantity            INT           COMMENT 'Ordered quantity',
  supplier            STRING        COMMENT 'Supplier name',
  lead_time_days      INT           COMMENT 'Supplier-quoted lead time in days',
  order_date          DATE          COMMENT 'Date the order was placed',
  expected_delivery   DATE          COMMENT 'Expected delivery date',
  _ingested_at        TIMESTAMP     DEFAULT current_timestamp()
                                    COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw ERP purchase-order data.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 3. raw_inspections
CREATE TABLE IF NOT EXISTS bronze.raw_inspections (
  inspection_id       STRING        COMMENT 'Inspection record identifier',
  timestamp           TIMESTAMP     COMMENT 'Inspection timestamp',
  asset_id            STRING        COMMENT 'Asset that produced the part',
  site                STRING        COMMENT 'Manufacturing site code',
  measurement_um      DOUBLE        COMMENT 'Measured dimension in micrometres',
  spec_upper_um       DOUBLE        COMMENT 'Upper specification limit (um)',
  spec_lower_um       DOUBLE        COMMENT 'Lower specification limit (um)',
  pass_fail           STRING        COMMENT 'PASS or FAIL',
  inspection_method   STRING        COMMENT 'Inspection method (CMM, Vision, etc.)',
  _ingested_at        TIMESTAMP     DEFAULT current_timestamp()
                                    COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw quality-inspection measurements.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =========================================================================
-- SILVER TABLES
-- =========================================================================

-- 4. cnc_anomalies
CREATE TABLE IF NOT EXISTS silver.cnc_anomalies (
  event_id            STRING        COMMENT 'Source telemetry event id',
  timestamp           TIMESTAMP     COMMENT 'Event timestamp',
  asset_id            STRING        COMMENT 'CNC machine identifier',
  site                STRING        COMMENT 'Manufacturing site code',
  machine_type        STRING        COMMENT 'Machine category',
  vibration_hz        DOUBLE        COMMENT 'Vibration frequency in Hz',
  temp_c              DOUBLE        COMMENT 'Spindle temperature in Celsius',
  spindle_rpm         DOUBLE        COMMENT 'Spindle speed in RPM',
  tool_wear_index     DOUBLE        COMMENT 'Normalised tool-wear score 0-1',
  feed_rate           DOUBLE        COMMENT 'Feed rate mm/min',
  is_anomalous        BOOLEAN       COMMENT 'True if the model flags an anomaly',
  anomaly_confidence  DOUBLE        COMMENT 'Model confidence score 0-1',
  shap_top_driver     STRING        COMMENT 'Feature with highest SHAP value',
  shap_top_value      DOUBLE        COMMENT 'SHAP value for the top driver'
)
USING DELTA
COMMENT 'Telemetry enriched with anomaly-detection model predictions and SHAP explanations.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 5. enriched_orders
CREATE TABLE IF NOT EXISTS silver.enriched_orders (
  order_id            STRING        COMMENT 'ERP purchase-order identifier',
  batch_id            STRING        COMMENT 'Production batch identifier',
  component           STRING        COMMENT 'Component / part name',
  site                STRING        COMMENT 'Manufacturing site code',
  quantity            INT           COMMENT 'Ordered quantity',
  supplier            STRING        COMMENT 'Supplier name',
  lead_time_days      INT           COMMENT 'Supplier-quoted lead time in days',
  order_date          DATE          COMMENT 'Date the order was placed',
  expected_delivery   DATE          COMMENT 'Expected delivery date',
  current_stock       INT           COMMENT 'Current inventory on hand',
  days_of_stock       INT           COMMENT 'Estimated days of remaining stock',
  stock_status        STRING        COMMENT 'OK / LOW / CRITICAL'
)
USING DELTA
COMMENT 'ERP orders enriched with inventory levels and stock-status classification.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 6. tolerance_stats
CREATE TABLE IF NOT EXISTS silver.tolerance_stats (
  inspection_id       STRING        COMMENT 'Inspection record identifier',
  timestamp           TIMESTAMP     COMMENT 'Inspection timestamp',
  asset_id            STRING        COMMENT 'Asset that produced the part',
  site                STRING        COMMENT 'Manufacturing site code',
  deviation_um        DOUBLE        COMMENT 'Deviation from nominal in um',
  is_out_of_spec      BOOLEAN       COMMENT 'True if measurement is outside spec',
  inspection_method   STRING        COMMENT 'Inspection method used'
)
USING DELTA
COMMENT 'Inspection data with computed deviations and out-of-spec flags.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 7. build_tracking
CREATE TABLE IF NOT EXISTS silver.build_tracking (
  event_id            STRING        COMMENT 'Telemetry event identifier',
  timestamp           TIMESTAMP     COMMENT 'Event timestamp',
  batch_id            STRING        COMMENT 'Production batch identifier',
  station             STRING        COMMENT 'Manufacturing station',
  site                STRING        COMMENT 'Manufacturing site code',
  status              STRING        COMMENT 'Station status (IN_PROGRESS, DONE, ERROR)',
  defect_flag         STRING        COMMENT 'Defect category or NONE'
)
USING DELTA
COMMENT 'Build-tracking events derived from telemetry, one row per station pass.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =========================================================================
-- GOLD TABLES
-- =========================================================================

-- 8. production_kpis
CREATE TABLE IF NOT EXISTS gold.production_kpis (
  snapshot_time       TIMESTAMP     COMMENT 'KPI snapshot timestamp',
  model_f1_score      DOUBLE        COMMENT 'Latest anomaly-model F1 score',
  inference_latency_ms DOUBLE       COMMENT 'P95 inference latency in ms',
  data_drift_pct      DOUBLE        COMMENT 'Feature drift percentage',
  anomalies_1h        INT           COMMENT 'Anomaly count in last 1 hour',
  anomalies_high_conf INT           COMMENT 'High-confidence anomalies (>0.85)',
  model_version       STRING        COMMENT 'MLflow model version string'
)
USING DELTA
COMMENT 'Point-in-time production KPI snapshots for the ops dashboard.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 9. inventory_forecast
CREATE TABLE IF NOT EXISTS gold.inventory_forecast (
  forecast_date       DATE          COMMENT 'Forecast generation date',
  site                STRING        COMMENT 'Manufacturing site code',
  component           STRING        COMMENT 'Component / part name',
  current_stock       INT           COMMENT 'Current inventory on hand',
  predicted_shortage_date DATE      COMMENT 'Predicted date stock reaches zero',
  days_of_stock       INT           COMMENT 'Estimated days of remaining stock',
  stock_status        STRING        COMMENT 'OK / LOW / CRITICAL',
  ramp_impact         STRING        COMMENT 'Impact on production ramp plan'
)
USING DELTA
COMMENT 'Per-site, per-component inventory shortage forecasts.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 10. site_component_status
CREATE TABLE IF NOT EXISTS gold.site_component_status (
  site                STRING        COMMENT 'Manufacturing site code',
  component           STRING        COMMENT 'Component / part name',
  stock_status        STRING        COMMENT 'OK / LOW / CRITICAL',
  days_remaining      INT           COMMENT 'Estimated days of remaining stock',
  predicted_shortage  DATE          COMMENT 'Predicted shortage date',
  ramp_impact         STRING        COMMENT 'Impact on production ramp plan',
  last_updated        TIMESTAMP     COMMENT 'Row last-updated timestamp'
)
USING DELTA
COMMENT 'Current inventory status per site and component — dashboard-ready.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 11. model_health_metrics
CREATE TABLE IF NOT EXISTS gold.model_health_metrics (
  timestamp           TIMESTAMP     COMMENT 'Metric capture timestamp',
  model_name          STRING        COMMENT 'Registered model name',
  f1_score            DOUBLE        COMMENT 'F1 score',
  precision_val       DOUBLE        COMMENT 'Precision',
  recall              DOUBLE        COMMENT 'Recall',
  latency_ms          DOUBLE        COMMENT 'Inference latency in ms',
  drift_pct           DOUBLE        COMMENT 'Data-drift percentage',
  feature_importance  MAP<STRING, DOUBLE>
                                    COMMENT 'Feature-importance map from the model'
)
USING DELTA
COMMENT 'Model health metrics over time for monitoring and alerting.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =========================================================================
-- Done.  All 11 tables created across bronze / silver / gold schemas.
-- =========================================================================
