#!/usr/bin/env python3
"""
Pull real data from the blueprint_demos workspace and save as a snapshot JSON
that the app can load on startup. Runs locally using Dan's credentials.
"""

import json
import time
import requests
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient

PROFILE = "blueprint_demos"
WH_ID = "e3a0fc2c08db05bb"

w = WorkspaceClient(profile=PROFILE)
HOST = w.config.host.rstrip("/")
header = w.config.authenticate()
h = header() if callable(header) else header
TOKEN = h.get("Authorization", "").replace("Bearer ", "")

S = requests.Session()
S.headers["Authorization"] = f"Bearer {TOKEN}"
S.headers["Content-Type"] = "application/json"
S.headers["User-Agent"] = "LHO-Lite/1.0"


def get(path):
    try:
        r = S.get(f"{HOST}{path}", timeout=30)
        return r.json() if r.status_code == 200 else {}
    except:
        return {}


def sql(statement):
    r = S.post(f"{HOST}/api/2.0/sql/statements", json={
        "warehouse_id": WH_ID,
        "statement": statement,
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }, timeout=60)
    result = r.json()
    state = result.get("status", {}).get("state", "")
    if state == "SUCCEEDED":
        cols = [c["name"] for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = result.get("result", {}).get("data_array", [])
        return cols, rows
    if state in ("PENDING", "RUNNING"):
        sid = result.get("statement_id", "")
        for _ in range(24):
            time.sleep(5)
            poll = S.get(f"{HOST}/api/2.0/sql/statements/{sid}", timeout=30).json()
            if poll.get("status", {}).get("state") == "SUCCEEDED":
                cols = [c["name"] for c in poll.get("manifest", {}).get("schema", {}).get("columns", [])]
                rows = poll.get("result", {}).get("data_array", [])
                return cols, rows
    err = result.get("status", {}).get("error", {}).get("message", str(result)[:200])
    print(f"  SQL WARN: {err[:150]}")
    return [], []


t0 = time.time()

# =========================================================================
# Phase 1: Security data (REST APIs)
# =========================================================================
print("Phase 1: Collecting security data via REST APIs...")
sec_data = {}

endpoints = {
    "me": "/api/2.0/preview/scim/v2/Me",
    "users": "/api/2.0/preview/scim/v2/Users",
    "groups": "/api/2.0/preview/scim/v2/Groups",
    "sps": "/api/2.0/preview/scim/v2/ServicePrincipals",
    "clusters": "/api/2.0/clusters/list",
    "policies": "/api/2.0/policies/clusters/list",
    "warehouses": "/api/2.0/sql/warehouses",
    "jobs": "/api/2.1/jobs/list?limit=100",
    "ip_lists": "/api/2.0/ip-access-lists",
    "secrets": "/api/2.0/secrets/scopes/list",
    "tokens": "/api/2.0/token-management/tokens",
    "init_scripts": "/api/2.0/global-init-scripts",
    "workspace_conf": "/api/2.0/workspace-conf?keys=enableResultsDownloading,enableUploadDataUis,enableNotebookGitVersioning,enableDcs,enableExportNotebook,enableWebTerminal,enableNotebookTableClipboard,maxTokenLifetimeDays,enableTokensConfig,enableIpAccessLists,enableDbfsFileBrowser",
    "metastores": "/api/2.1/unity-catalog/metastores",
    "catalogs": "/api/2.1/unity-catalog/catalogs",
    "storage_creds": "/api/2.1/unity-catalog/storage-credentials",
    "ext_locations": "/api/2.1/unity-catalog/external-locations",
    "shares": "/api/2.1/unity-catalog/shares",
    "recipients": "/api/2.1/unity-catalog/recipients",
    "apps": "/api/2.0/apps",
    "serving": "/api/2.0/serving-endpoints",
}

for key, path in endpoints.items():
    print(f"  {key}...")
    sec_data[key] = get(path)

sec_data["dbfs"] = {}
sec_data["init_contents"] = {}
sec_data["all_schemas"] = {}

# Detect cloud
url = HOST.lower()
if ".azuredatabricks.net" in url or "adb-" in url:
    cloud = "AZURE"
elif ".gcp.databricks.com" in url:
    cloud = "GCP"
else:
    cloud = "AWS"

sec_data["_cloud"] = cloud
sec_data["_workspace_url"] = HOST.replace("https://", "")
sec_data["_govcloud"] = False

print(f"  Done. {len(sec_data)} datasets.")

# =========================================================================
# Phase 2: Usage data (SQL queries)
# =========================================================================
print("\nPhase 2: Collecting usage data via SQL...")
usage_data = {"warehouse_id": WH_ID}

print("  user_queries...")
cols, rows = sql(
    "SELECT executed_by, execution_status, COUNT(*) as cnt, "
    "ROUND(SUM(COALESCE(read_bytes,0))/1073741824, 4) as total_read_gb, "
    "SUM(COALESCE(read_rows,0)) as total_rows, "
    "ROUND(SUM(COALESCE(total_duration_ms,0))/1000/60, 2) as total_minutes "
    "FROM system.query.history WHERE start_time > date_sub(now(), 30) "
    "GROUP BY executed_by, execution_status ORDER BY cnt DESC"
)
usage_data["user_queries"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

print("  daily_queries...")
cols, rows = sql(
    "SELECT DATE(start_time) as query_date, COUNT(*) as total_queries, "
    "SUM(CASE WHEN execution_status='FINISHED' THEN 1 ELSE 0 END) as succeeded, "
    "SUM(CASE WHEN execution_status='FAILED' THEN 1 ELSE 0 END) as failed, "
    "ROUND(SUM(COALESCE(read_bytes,0))/1073741824, 4) as read_gb, "
    "SUM(COALESCE(read_rows,0)) as read_rows, "
    "ROUND(SUM(COALESCE(total_duration_ms,0))/1000/60, 2) as total_minutes "
    "FROM system.query.history WHERE start_time > date_sub(now(), 30) "
    "GROUP BY DATE(start_time) ORDER BY query_date"
)
usage_data["daily_queries"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

print("  warehouse_events...")
cols, rows = sql(
    "SELECT warehouse_id, event_type, event_time, cluster_count "
    "FROM system.compute.warehouse_events "
    "WHERE event_time > date_sub(now(), 30) ORDER BY event_time DESC LIMIT 100"
)
usage_data["warehouse_events"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

print("  list_prices...")
cols, rows = sql(
    "SELECT sku_name, pricing.default as price_usd, usage_unit "
    "FROM system.billing.list_prices "
    "WHERE currency_code = 'USD' AND price_end_time IS NULL "
    "ORDER BY sku_name"
)
usage_data["list_prices"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

print("  schema_overview...")
cols, rows = sql(
    "SELECT table_catalog, table_schema, COUNT(*) as table_count "
    "FROM system.information_schema.tables WHERE table_catalog NOT IN ('system') "
    "GROUP BY table_catalog, table_schema ORDER BY table_catalog, table_schema"
)
usage_data["schema_overview"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

# Table inventory per schema
usage_data["table_inventory"] = {}
user_catalogs = set()
for r in rows:
    if r[0] not in ("samples",):
        user_catalogs.add((r[0], r[1]))
for cat, schema in user_catalogs:
    if schema == "information_schema":
        continue
    key = f"{cat}.{schema}"
    print(f"  tables: {key}...")
    cols2, rows2 = sql(
        f"SELECT table_name, table_type, created, last_altered, comment "
        f"FROM system.information_schema.tables "
        f"WHERE table_catalog = '{cat}' AND table_schema = '{schema}' "
        f"ORDER BY table_name LIMIT 500"
    )
    usage_data["table_inventory"][key] = {"cols": cols2, "rows": rows2}

usage_data["table_sizes"] = {}

print("  monthly_cost_by_product...")
cols, rows = sql(
    "SELECT DATE_TRUNC('month', usage_date) as month, "
    "billing_origin_product, "
    "ROUND(SUM(usage_quantity), 2) as total_dbus "
    "FROM system.billing.usage "
    "WHERE usage_date >= date_sub(current_date(), 90) "
    "GROUP BY 1, 2 ORDER BY 1, 2"
)
usage_data["monthly_cost_by_product"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

print("  cost_by_product...")
cols, rows = sql(
    "SELECT billing_origin_product, "
    "ROUND(SUM(usage_quantity), 2) as total_dbus, "
    "COUNT(DISTINCT usage_date) as active_days "
    "FROM system.billing.usage "
    "WHERE usage_date >= date_sub(current_date(), 90) "
    "GROUP BY 1 ORDER BY 2 DESC"
)
usage_data["cost_by_product"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

print("  cost_by_tag...")
cols, rows = sql(
    "SELECT "
    "CASE WHEN size(custom_tags) > 0 THEN map_keys(custom_tags)[0] "
    "     ELSE 'Untagged' END as tag_key, "
    "CASE WHEN size(custom_tags) > 0 THEN map_values(custom_tags)[0] "
    "     ELSE 'Untagged' END as tag_value, "
    "billing_origin_product, "
    "ROUND(SUM(usage_quantity), 2) as total_dbus "
    "FROM system.billing.usage "
    "WHERE usage_date >= date_sub(current_date(), 90) "
    "GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 50"
)
usage_data["cost_by_tag"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

print("  job_runs...")
cols, rows = sql(
    "SELECT "
    "j.job_id, "
    "COALESCE(j.run_name, CAST(j.job_id AS STRING)) as job_name, "
    "COUNT(DISTINCT j.run_id) as total_runs, "
    "SUM(CASE WHEN j.result_state = 'SUCCESS' THEN 1 ELSE 0 END) as succeeded, "
    "SUM(CASE WHEN j.result_state IN ('FAILED','TIMEDOUT','INTERNAL_ERROR') THEN 1 ELSE 0 END) as failed, "
    "SUM(CASE WHEN j.result_state = 'CANCELED' THEN 1 ELSE 0 END) as canceled, "
    "ROUND(AVG(j.run_duration_seconds) / 60, 1) as avg_duration_min, "
    "ROUND(SUM(j.run_duration_seconds) / 60, 1) as total_duration_min "
    "FROM system.lakeflow.job_run_timeline j "
    "WHERE j.period_start_time >= date_sub(now(), 30) "
    "GROUP BY j.job_id, COALESCE(j.run_name, CAST(j.job_id AS STRING)) "
    "ORDER BY total_runs DESC LIMIT 100"
)
usage_data["job_runs"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

print("  job_billing...")
cols, rows = sql(
    "SELECT "
    "usage_metadata.job_id as job_id, "
    "usage_metadata.job_name as job_name, "
    "sku_name, "
    "ROUND(SUM(usage_quantity), 4) as total_dbus "
    "FROM system.billing.usage "
    "WHERE usage_metadata.job_id IS NOT NULL "
    "AND usage_date >= date_sub(current_date(), 30) "
    "GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 200"
)
usage_data["job_billing"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

print("  daily_cost...")
cols, rows = sql(
    "SELECT usage_date, "
    "ROUND(SUM(usage_quantity), 2) as total_dbus, "
    "ROUND(SUM(CASE WHEN billing_origin_product = 'SQL' THEN usage_quantity ELSE 0 END), 2) as sql_dbus, "
    "ROUND(SUM(CASE WHEN billing_origin_product = 'JOBS' THEN usage_quantity ELSE 0 END), 2) as jobs_dbus, "
    "ROUND(SUM(CASE WHEN billing_origin_product = 'ALL_PURPOSE' THEN usage_quantity ELSE 0 END), 2) as allpurpose_dbus, "
    "ROUND(SUM(CASE WHEN billing_origin_product = 'DLT' THEN usage_quantity ELSE 0 END), 2) as dlt_dbus, "
    "ROUND(SUM(CASE WHEN billing_origin_product NOT IN ('SQL','JOBS','ALL_PURPOSE','DLT') THEN usage_quantity ELSE 0 END), 2) as other_dbus "
    "FROM system.billing.usage "
    "WHERE usage_date >= date_sub(current_date(), 90) "
    "GROUP BY usage_date ORDER BY usage_date"
)
usage_data["daily_cost"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

print("  billing_line_items...")
cols, rows = sql(
    "SELECT "
    "billing_origin_product, "
    "sku_name, "
    "usage_metadata.warehouse_id as warehouse_id, "
    "usage_metadata.job_id as job_id, "
    "usage_metadata.notebook_id as notebook_id, "
    "ROUND(SUM(usage_quantity), 4) as total_dbus, "
    "COUNT(DISTINCT usage_date) as active_days, "
    "MIN(usage_date) as first_seen, "
    "MAX(usage_date) as last_seen "
    "FROM system.billing.usage "
    "WHERE usage_date >= date_sub(current_date(), 90) "
    "GROUP BY 1, 2, 3, 4, 5 ORDER BY 6 DESC LIMIT 500"
)
usage_data["billing_line_items"] = {"cols": cols, "rows": rows}
print(f"    {len(rows)} rows")

# =========================================================================
# Save snapshot
# =========================================================================
duration = round(time.time() - t0, 1)
snapshot = {"security": sec_data, "usage": usage_data}

out_path = "data/preloaded_snapshot.json"
import os
os.makedirs("data", exist_ok=True)
with open(out_path, "w") as f:
    json.dump({
        "data": snapshot,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "duration_sec": duration,
    }, f, default=str)

print(f"\nDone in {duration}s. Saved to {out_path}")
sz = os.path.getsize(out_path)
print(f"File size: {sz / 1024:.0f} KB")
