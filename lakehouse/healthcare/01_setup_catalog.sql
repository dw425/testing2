-- =============================================================================
-- HealthcareIQ — Unity Catalog DDL
-- Creates catalog, schemas (bronze / silver / gold), and all 11 Delta tables.
-- Idempotent: safe to re-run.
-- =============================================================================

-- -------------------------------------------------------------------------
-- Catalog
-- -------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS healthcare_iq;
USE CATALOG healthcare_iq;

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

-- 1. raw_admissions
CREATE TABLE IF NOT EXISTS bronze.raw_admissions (
  admission_id          STRING        COMMENT 'Unique admission event identifier',
  timestamp             TIMESTAMP     COMMENT 'Admission event timestamp',
  patient_id            STRING        COMMENT 'De-identified patient identifier',
  facility              STRING        COMMENT 'Hospital or clinic facility name',
  department            STRING        COMMENT 'Clinical department (e.g. Emergency, ICU)',
  admission_type        STRING        COMMENT 'Admission type (e.g. Emergency, Elective, Urgent)',
  acuity_level          INT           COMMENT 'Patient acuity level 1 (lowest) to 5 (highest)',
  insurance_type        STRING        COMMENT 'Insurance category (e.g. Medicare, Private, Medicaid)',
  attending_physician   STRING        COMMENT 'Name of the attending physician',
  _ingested_at          TIMESTAMP     DEFAULT current_timestamp()
                                      COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw patient admission events ingested from ADT feeds.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 2. raw_vitals
CREATE TABLE IF NOT EXISTS bronze.raw_vitals (
  reading_id            STRING        COMMENT 'Unique vital-sign reading identifier',
  timestamp             TIMESTAMP     COMMENT 'Vital-sign measurement timestamp',
  patient_id            STRING        COMMENT 'De-identified patient identifier',
  facility              STRING        COMMENT 'Hospital or clinic facility name',
  heart_rate            INT           COMMENT 'Heart rate in beats per minute',
  blood_pressure_sys    INT           COMMENT 'Systolic blood pressure in mmHg',
  blood_pressure_dia    INT           COMMENT 'Diastolic blood pressure in mmHg',
  temp_f                DOUBLE        COMMENT 'Body temperature in Fahrenheit',
  spo2                  DOUBLE        COMMENT 'Peripheral oxygen saturation percentage',
  respiratory_rate      INT           COMMENT 'Respiratory rate in breaths per minute',
  _ingested_at          TIMESTAMP     DEFAULT current_timestamp()
                                      COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw patient vital-sign readings from bedside monitors and nursing assessments.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 3. raw_equipment_telemetry
CREATE TABLE IF NOT EXISTS bronze.raw_equipment_telemetry (
  event_id              STRING        COMMENT 'Unique equipment telemetry event identifier',
  timestamp             TIMESTAMP     COMMENT 'Event timestamp from the equipment sensor',
  asset_id              STRING        COMMENT 'Medical equipment asset identifier',
  facility              STRING        COMMENT 'Hospital or clinic facility name',
  equipment_type        STRING        COMMENT 'Equipment category (e.g. MRI, CT-Scanner, Ventilator)',
  usage_hours           DOUBLE        COMMENT 'Cumulative usage hours on the asset',
  temperature           DOUBLE        COMMENT 'Equipment operating temperature in Celsius',
  vibration             DOUBLE        COMMENT 'Vibration level reading',
  error_count           INT           COMMENT 'Number of logged errors since last reset',
  maintenance_due       BOOLEAN       COMMENT 'True if scheduled maintenance is overdue',
  _ingested_at          TIMESTAMP     DEFAULT current_timestamp()
                                      COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw medical equipment telemetry streamed from biomedical engineering sensors.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 4. raw_discharges
CREATE TABLE IF NOT EXISTS bronze.raw_discharges (
  discharge_id          STRING        COMMENT 'Unique discharge event identifier',
  timestamp             TIMESTAMP     COMMENT 'Discharge event timestamp',
  patient_id            STRING        COMMENT 'De-identified patient identifier',
  facility              STRING        COMMENT 'Hospital or clinic facility name',
  department            STRING        COMMENT 'Clinical department at discharge',
  los_days              DOUBLE        COMMENT 'Length of stay in days',
  diagnosis_code        STRING        COMMENT 'Primary ICD-10 diagnosis code',
  discharge_disposition STRING        COMMENT 'Disposition (e.g. Home, SNF, AMA, Expired)',
  readmission_risk_score DOUBLE       COMMENT 'Pre-computed readmission risk score 0-1',
  _ingested_at          TIMESTAMP     DEFAULT current_timestamp()
                                      COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw patient discharge events from ADT and case management systems.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =========================================================================
-- SILVER TABLES
-- =========================================================================

-- 5. patient_flow
CREATE TABLE IF NOT EXISTS silver.patient_flow (
  admission_id          STRING        COMMENT 'Source admission event identifier',
  patient_id            STRING        COMMENT 'De-identified patient identifier',
  facility              STRING        COMMENT 'Hospital or clinic facility name',
  department            STRING        COMMENT 'Clinical department',
  admission_type        STRING        COMMENT 'Admission type (Emergency, Elective, Urgent)',
  acuity_level          INT           COMMENT 'Patient acuity level 1-5',
  wait_time_min         INT           COMMENT 'Wait time from arrival to bed assignment in minutes',
  bed_assigned          STRING        COMMENT 'Assigned bed identifier',
  los_days              DOUBLE        COMMENT 'Length of stay in days (null if still admitted)',
  current_status        STRING        COMMENT 'Patient status (Admitted, In Treatment, Discharged, Transferred)'
)
USING DELTA
COMMENT 'Enriched patient flow records joining admissions with vitals and bed assignments.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 6. readmission_risk
CREATE TABLE IF NOT EXISTS silver.readmission_risk (
  patient_id            STRING        COMMENT 'De-identified patient identifier',
  facility              STRING        COMMENT 'Hospital or clinic facility name',
  department            STRING        COMMENT 'Clinical department at discharge',
  age                   INT           COMMENT 'Patient age in years',
  diagnosis_code        STRING        COMMENT 'Primary ICD-10 diagnosis code',
  los_days              DOUBLE        COMMENT 'Length of stay in days',
  prior_admissions_12m  INT           COMMENT 'Number of admissions in the prior 12 months',
  comorbidity_index     DOUBLE        COMMENT 'Charlson comorbidity index score',
  risk_score            DOUBLE        COMMENT 'Model-predicted readmission risk score 0-1',
  risk_level            STRING        COMMENT 'Risk classification (Low, Medium, High, Critical)'
)
USING DELTA
COMMENT 'Readmission risk predictions scored by the Readmission_Risk_Predictor model.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 7. equipment_health
CREATE TABLE IF NOT EXISTS silver.equipment_health (
  asset_id              STRING        COMMENT 'Medical equipment asset identifier',
  facility              STRING        COMMENT 'Hospital or clinic facility name',
  equipment_type        STRING        COMMENT 'Equipment category',
  health_score          DOUBLE        COMMENT 'Computed equipment health score 0-100',
  predicted_failure_date DATE         COMMENT 'Model-predicted failure date',
  maintenance_priority  STRING        COMMENT 'Maintenance priority (Routine, Elevated, Urgent, Critical)',
  alert_level           STRING        COMMENT 'Alert level (Normal, Warning, Critical)'
)
USING DELTA
COMMENT 'Equipment health scores and failure predictions from the Equipment_Failure_Predictor model.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =========================================================================
-- GOLD TABLES
-- =========================================================================

-- 8. operations_kpis
CREATE TABLE IF NOT EXISTS gold.operations_kpis (
  snapshot_time             TIMESTAMP   COMMENT 'Point-in-time snapshot timestamp',
  bed_utilization_pct       DOUBLE      COMMENT 'Overall bed utilization percentage',
  avg_ed_wait_min           INT         COMMENT 'Average ED wait time in minutes',
  avg_los_days              DOUBLE      COMMENT 'Average length of stay in days',
  readmission_rate_30d      DOUBLE      COMMENT '30-day readmission rate as a percentage',
  critical_equipment_alerts INT         COMMENT 'Count of critical equipment alerts'
)
USING DELTA
COMMENT 'Point-in-time operational KPI snapshot for the executive dashboard.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 9. facility_capacity
CREATE TABLE IF NOT EXISTS gold.facility_capacity (
  facility                      STRING    COMMENT 'Hospital or clinic facility name',
  department                    STRING    COMMENT 'Clinical department',
  total_beds                    INT       COMMENT 'Total licensed beds',
  occupied_beds                 INT       COMMENT 'Currently occupied beds',
  utilization_pct               DOUBLE    COMMENT 'Bed utilization percentage',
  predicted_capacity_breach_hours INT     COMMENT 'Hours until predicted capacity breach (null if none)',
  last_updated                  TIMESTAMP COMMENT 'Timestamp of last capacity computation'
)
USING DELTA
COMMENT 'Per-facility, per-department bed capacity and utilization with breach predictions.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 10. readmission_summary
CREATE TABLE IF NOT EXISTS gold.readmission_summary (
  facility              STRING        COMMENT 'Hospital or clinic facility name',
  department            STRING        COMMENT 'Clinical department',
  total_discharges      INT           COMMENT 'Total discharges in the reporting period',
  readmissions          INT           COMMENT 'Count of 30-day readmissions',
  readmission_rate      DOUBLE        COMMENT 'Readmission rate as a decimal (0-1)',
  avg_risk_score        DOUBLE        COMMENT 'Average predicted readmission risk score',
  top_diagnosis         STRING        COMMENT 'Most frequent primary diagnosis code for readmissions'
)
USING DELTA
COMMENT 'Readmission analytics aggregated by facility and department.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 11. equipment_status
CREATE TABLE IF NOT EXISTS gold.equipment_status (
  facility              STRING        COMMENT 'Hospital or clinic facility name',
  equipment_type        STRING        COMMENT 'Equipment category',
  total_assets          INT           COMMENT 'Total assets of this type at the facility',
  healthy               INT           COMMENT 'Count of assets with Normal health',
  maintenance_due       INT           COMMENT 'Count of assets with maintenance due',
  critical_alerts       INT           COMMENT 'Count of assets with Critical alert level',
  last_updated          TIMESTAMP     COMMENT 'Timestamp of last status computation'
)
USING DELTA
COMMENT 'Equipment fleet health status aggregated by facility and equipment type.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
