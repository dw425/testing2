# Databricks notebook source
# MAGIC %md
# MAGIC # ManufacturingIQ - Interactive EDA
# MAGIC
# MAGIC Exploratory data analysis notebook for customer workshops and demos.
# MAGIC Use this to walk through the data layer and show how insights are derived.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Telemetry Overview

# COMMAND ----------

df_telemetry = spark.table("manufacturing_iq.bronze.raw_iot_telemetry")
print(f"Total telemetry records: {df_telemetry.count():,}")
display(df_telemetry.groupBy("site", "machine_type").count().orderBy("site"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Distribution

# COMMAND ----------

df_anomalies = spark.table("manufacturing_iq.silver.cnc_anomalies")
display(
    df_anomalies
    .groupBy("site", "is_anomalous")
    .count()
    .orderBy("site", "is_anomalous")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Inspections

# COMMAND ----------

df_quality = spark.table("manufacturing_iq.silver.tolerance_stats")
total = df_quality.count()
out_of_spec = df_quality.filter("is_out_of_spec = true").count()
print(f"Total inspections: {total:,}")
print(f"Out of spec: {out_of_spec}")
print(f"Rate: {out_of_spec/total*100:.6f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory Status

# COMMAND ----------

display(spark.table("manufacturing_iq.gold.site_component_status").orderBy("days_remaining"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Health

# COMMAND ----------

display(spark.table("manufacturing_iq.gold.production_kpis").orderBy("snapshot_time"))
