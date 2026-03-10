# Databricks notebook source
# MAGIC %md
# MAGIC # ManufacturingIQ - One-Click Demo Setup
# MAGIC
# MAGIC This notebook stands up the entire ManufacturingIQ demo:
# MAGIC 1. Creates Unity Catalog, schemas, and Delta tables
# MAGIC 2. Generates synthetic demo data
# MAGIC 3. Runs Silver + Gold transforms
# MAGIC 4. Trains and registers ML models
# MAGIC
# MAGIC **Prerequisites:** A Databricks workspace with Unity Catalog enabled.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog & Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run the catalog setup DDL
# MAGIC -- You can also run lakehouse/01_setup_catalog.sql directly in a SQL warehouse

# COMMAND ----------

import os
os.chdir("/Workspace/Repos/manufacturing-iq")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Synthetic Demo Data

# COMMAND ----------

# MAGIC %run ../lakehouse/05_seed_demo_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run Silver Transforms

# COMMAND ----------

# MAGIC %run ../lakehouse/03_silver_transform

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Run Gold Aggregations

# COMMAND ----------

# MAGIC %run ../lakehouse/04_gold_aggregate

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Train & Register ML Models

# COMMAND ----------

# MAGIC %run ../ml/train_anomaly_model

# COMMAND ----------

# MAGIC %run ../ml/register_models

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Setup

# COMMAND ----------

# Verify table row counts
tables = [
    "manufacturing_iq.bronze.raw_iot_telemetry",
    "manufacturing_iq.bronze.raw_erp_orders",
    "manufacturing_iq.bronze.raw_inspections",
    "manufacturing_iq.silver.cnc_anomalies",
    "manufacturing_iq.silver.enriched_orders",
    "manufacturing_iq.silver.tolerance_stats",
    "manufacturing_iq.silver.build_tracking",
    "manufacturing_iq.gold.production_kpis",
    "manufacturing_iq.gold.inventory_forecast",
    "manufacturing_iq.gold.site_component_status",
    "manufacturing_iq.gold.model_health_metrics",
]

for table in tables:
    try:
        count = spark.table(table).count()
        print(f"  {table}: {count:,} rows")
    except Exception as e:
        print(f"  {table}: ERROR - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC Deploy the Dash app:
# MAGIC ```bash
# MAGIC databricks apps create manufacturing-iq --manifest app.yaml
# MAGIC databricks apps deploy manufacturing-iq
# MAGIC ```
