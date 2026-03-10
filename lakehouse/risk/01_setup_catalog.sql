-- =============================================================================
-- RiskIQ — Unity Catalog DDL
-- Creates catalog, schemas (bronze / silver / gold), and all 10 Delta tables.
-- Idempotent: safe to re-run.
-- =============================================================================

-- -------------------------------------------------------------------------
-- Catalog
-- -------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS risk_iq;
USE CATALOG risk_iq;

-- -------------------------------------------------------------------------
-- Schemas
-- -------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw ingestion layer — append-only, minimal transformation.';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Curated layer — cleaned, enriched, and scored datasets.';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Aggregation layer — KPIs, risk scores, and dashboard-ready tables.';

-- =========================================================================
-- BRONZE TABLES
-- =========================================================================

-- 1. raw_access_logs
CREATE TABLE IF NOT EXISTS bronze.raw_access_logs (
  event_id            STRING        COMMENT 'Unique access event identifier',
  timestamp           TIMESTAMP     COMMENT 'Time the access event occurred',
  user_id             STRING        COMMENT 'Unique user identifier',
  email               STRING        COMMENT 'User email address',
  asset_accessed      STRING        COMMENT 'Data asset or resource accessed (e.g. finance.payroll)',
  ip_address          STRING        COMMENT 'Source IP address',
  geo_location        STRING        COMMENT 'Geographic location derived from IP',
  device_type         STRING        COMMENT 'Device type (Desktop, Mobile, API, Service)',
  access_method       STRING        COMMENT 'Access method (UI, API, JDBC, Service Account)',
  session_duration_sec INT          COMMENT 'Session duration in seconds',
  _ingested_at        TIMESTAMP     DEFAULT current_timestamp()
                                    COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw user access events from RBAC audit logs.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 2. raw_compliance_scans
CREATE TABLE IF NOT EXISTS bronze.raw_compliance_scans (
  scan_id             STRING        COMMENT 'Unique scan record identifier',
  timestamp           TIMESTAMP     COMMENT 'Time the compliance scan was performed',
  framework           STRING        COMMENT 'Compliance framework (GDPR, CCPA, HIPAA, SOC2, PCI-DSS)',
  domain              STRING        COMMENT 'Data domain assessed (Finance, Healthcare, Customer, HR)',
  control_id          STRING        COMMENT 'Control identifier within the framework',
  control_name        STRING        COMMENT 'Human-readable control name',
  status              STRING        COMMENT 'Scan result (Pass, Fail, Warning, Not Applicable)',
  finding_detail      STRING        COMMENT 'Description of finding or observation',
  severity            STRING        COMMENT 'Finding severity (Critical, High, Medium, Low, Info)',
  assessor            STRING        COMMENT 'Assessor or scanning tool name',
  _ingested_at        TIMESTAMP     DEFAULT current_timestamp()
                                    COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw compliance scan results across all regulatory frameworks.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 3. raw_pii_detections
CREATE TABLE IF NOT EXISTS bronze.raw_pii_detections (
  detection_id        STRING        COMMENT 'Unique PII detection identifier',
  timestamp           TIMESTAMP     COMMENT 'Time the PII detection occurred',
  table_name          STRING        COMMENT 'Fully-qualified table where PII was detected',
  column_name         STRING        COMMENT 'Column containing PII',
  pii_type            STRING        COMMENT 'PII type (PHONE_NUMBER, EMAIL_ADDRESS, US_SSN, CREDIT_CARD)',
  confidence          DOUBLE        COMMENT 'Detection confidence score 0.0-1.0',
  record_count        INT           COMMENT 'Number of records containing PII in this column',
  masked              BOOLEAN       COMMENT 'True if the column is currently masked',
  pipeline_id         STRING        COMMENT 'Data pipeline identifier that produced this data',
  _ingested_at        TIMESTAMP     DEFAULT current_timestamp()
                                    COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw PII detection events from automated scanning pipelines.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =========================================================================
-- SILVER TABLES
-- =========================================================================

-- 4. access_anomalies
CREATE TABLE IF NOT EXISTS silver.access_anomalies (
  event_id            STRING        COMMENT 'Source access event identifier',
  timestamp           TIMESTAMP     COMMENT 'Access event timestamp',
  user_id             STRING        COMMENT 'User identifier',
  email               STRING        COMMENT 'User email address',
  asset_accessed      STRING        COMMENT 'Data asset accessed',
  ip_address          STRING        COMMENT 'Source IP address',
  geo_location        STRING        COMMENT 'Geographic location',
  access_frequency    INT           COMMENT 'Number of accesses by this user in the past 24h',
  data_sensitivity    STRING        COMMENT 'Sensitivity classification of the asset (High, Medium, Low)',
  role_match_score    DOUBLE        COMMENT 'Score 0-1 indicating how well access matches user role',
  is_anomalous        BOOLEAN       COMMENT 'True if the access is flagged as anomalous',
  anomaly_confidence  DOUBLE        COMMENT 'Anomaly confidence score 0.0-1.0',
  risk_level          STRING        COMMENT 'Computed risk level (Critical, High, Medium, Low)'
)
USING DELTA
COMMENT 'Access logs enriched with anomaly scores and risk classification.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 5. compliance_status
CREATE TABLE IF NOT EXISTS silver.compliance_status (
  scan_id             STRING        COMMENT 'Source scan record identifier',
  timestamp           TIMESTAMP     COMMENT 'Scan timestamp',
  framework           STRING        COMMENT 'Compliance framework',
  domain              STRING        COMMENT 'Data domain',
  control_id          STRING        COMMENT 'Control identifier',
  control_name        STRING        COMMENT 'Control name',
  status              STRING        COMMENT 'Scan result status',
  severity            STRING        COMMENT 'Finding severity',
  open_violations     INT           COMMENT 'Count of open violations for this control',
  days_since_last_audit INT         COMMENT 'Days since the most recent audit for this framework',
  trend               STRING        COMMENT 'Compliance trend (Improving, Stable, Degrading)'
)
USING DELTA
COMMENT 'Compliance scan data enriched with audit recency and violation trends.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 6. pii_exposure_map
CREATE TABLE IF NOT EXISTS silver.pii_exposure_map (
  detection_id        STRING        COMMENT 'Source detection identifier',
  timestamp           TIMESTAMP     COMMENT 'Detection timestamp',
  table_name          STRING        COMMENT 'Table where PII was detected',
  column_name         STRING        COMMENT 'Column containing PII',
  pii_type            STRING        COMMENT 'PII type detected',
  confidence          DOUBLE        COMMENT 'Detection confidence score',
  record_count        INT           COMMENT 'Number of affected records',
  masked              BOOLEAN       COMMENT 'True if column is masked',
  exposure_risk       STRING        COMMENT 'Exposure risk level (Critical, High, Medium, Low)',
  remediation_status  STRING        COMMENT 'Remediation status (Remediated, In Progress, Open, Acknowledged)'
)
USING DELTA
COMMENT 'PII detections enriched with exposure risk classification and remediation tracking.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =========================================================================
-- GOLD TABLES
-- =========================================================================

-- 7. risk_summary
CREATE TABLE IF NOT EXISTS gold.risk_summary (
  snapshot_time       TIMESTAMP     COMMENT 'KPI snapshot timestamp',
  total_financial_risk DOUBLE       COMMENT 'Total financial risk exposure in millions',
  compliance_risk_score INT         COMMENT 'Aggregate compliance risk score 0-100',
  active_alerts       INT           COMMENT 'Number of active high-severity alerts',
  risk_trend          STRING        COMMENT 'Overall risk trend (Increasing, Stable, Decreasing)'
)
USING DELTA
COMMENT 'Point-in-time risk KPI snapshots for the executive dashboard.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 8. compliance_scores
CREATE TABLE IF NOT EXISTS gold.compliance_scores (
  framework           STRING        COMMENT 'Compliance framework name',
  status              STRING        COMMENT 'Overall framework status (Compliant, Needs Review, At Risk)',
  last_audit_date     TIMESTAMP     COMMENT 'Timestamp of the most recent audit',
  open_violations     INT           COMMENT 'Total open violations for this framework',
  compliance_pct      DOUBLE        COMMENT 'Percentage of controls passing',
  risk_level          STRING        COMMENT 'Risk level (Critical, High, Medium, Low)',
  last_updated        TIMESTAMP     COMMENT 'Row last-updated timestamp'
)
USING DELTA
COMMENT 'Per-framework compliance scores for the compliance dashboard.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 9. active_alerts
CREATE TABLE IF NOT EXISTS gold.active_alerts (
  alert_id            STRING        COMMENT 'Unique alert identifier',
  timestamp           TIMESTAMP     COMMENT 'Alert creation timestamp',
  alert_type          STRING        COMMENT 'Alert type (Access Anomaly, PII Exposure, Compliance Violation)',
  severity            STRING        COMMENT 'Alert severity (Critical, High, Medium, Low)',
  description         STRING        COMMENT 'Human-readable alert description',
  source_table        STRING        COMMENT 'Silver table that originated the alert',
  affected_users      INT           COMMENT 'Number of affected users',
  status              STRING        COMMENT 'Alert status (Open, Investigating, Resolved)'
)
USING DELTA
COMMENT 'Active risk alerts aggregated from access anomalies and PII exposures.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 10. user_risk_profiles
CREATE TABLE IF NOT EXISTS gold.user_risk_profiles (
  user_id             STRING        COMMENT 'Unique user identifier',
  email               STRING        COMMENT 'User email address',
  risk_score          DOUBLE        COMMENT 'Composite user risk score 0.0-100.0',
  anomalous_access_count INT        COMMENT 'Total anomalous accesses in the observation window',
  last_anomaly_time   TIMESTAMP     COMMENT 'Timestamp of the most recent anomalous access',
  risk_level          STRING        COMMENT 'Risk level (Critical, High, Medium, Low)',
  last_updated        TIMESTAMP     COMMENT 'Row last-updated timestamp'
)
USING DELTA
COMMENT 'Per-user risk profiles computed from access anomalies and compliance data.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =========================================================================
-- Done.  All 10 tables created across bronze / silver / gold schemas.
-- =========================================================================
