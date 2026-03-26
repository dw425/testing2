"""
Universal Databricks data collector for LHO Lite.

Supports AWS, Azure, and GCP Databricks workspaces.
Auth modes: PAT, Service Principal (OAuth2), or Databricks SDK auto-auth.
"""

import base64
import json
import logging
import re
import time
from datetime import datetime

import requests

log = logging.getLogger("lho.collector")

# ---------------------------------------------------------------------------
# Cloud detection
# ---------------------------------------------------------------------------

def detect_cloud(workspace_url: str) -> str:
    """Detect cloud provider from workspace URL."""
    url = workspace_url.lower()
    if ".azuredatabricks.net" in url or "adb-" in url:
        return "AZURE"
    if ".gcp.databricks.com" in url:
        return "GCP"
    # Default to AWS (covers .cloud.databricks.com and .databricks.us)
    return "AWS"


def detect_govcloud(workspace_url: str, region: str = "") -> bool:
    """Detect if workspace is in a GovCloud environment."""
    url = workspace_url.lower()
    region_lower = region.lower()
    # AWS GovCloud
    if ".databricks.us" in url or "gov" in region_lower:
        return True
    # Azure Government
    if ".databricks.azure.us" in url or "usgov" in region_lower or "usdod" in region_lower:
        return True
    return False


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class DatabricksCollector:
    """Universal Databricks API collector.

    Parameters
    ----------
    workspace_url : str
        Full URL, e.g. ``https://adb-123.4.azuredatabricks.net``
    auth_method : str
        ``"pat"`` | ``"sp"`` | ``"auto"``
    pat_token : str, optional
        Bearer token for PAT auth
    sp_client_id, sp_client_secret, sp_tenant_id : str, optional
        For service-principal OAuth2 (Azure & AWS)
    """

    def __init__(
        self,
        workspace_url: str,
        auth_method: str = "pat",
        pat_token: str = "",
        sp_client_id: str = "",
        sp_client_secret: str = "",
        sp_tenant_id: str = "",
    ):
        self.host = workspace_url.rstrip("/")
        self.cloud = detect_cloud(self.host)
        self.auth_method = auth_method
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"
        self.session.headers["User-Agent"] = "LHO-Lite/1.0"

        # Store SP credentials for token refresh
        self._sp_client_id = sp_client_id
        self._sp_client_secret = sp_client_secret
        self._sp_tenant_id = sp_tenant_id
        self._token_expires_at = 0

        # Auth setup
        if auth_method == "pat":
            self.session.headers["Authorization"] = f"Bearer {pat_token}"
        elif auth_method == "sp":
            self._refresh_sp_token()
        elif auth_method == "auto":
            self._setup_sdk_auth()

    # ---- Service Principal OAuth2 (works on AWS, Azure, GCP) ----

    def _refresh_sp_token(self):
        """Obtain (or refresh) an OAuth2 access token via client_credentials grant."""
        if self.cloud == "AZURE" and self._sp_tenant_id:
            # Azure AD — commercial and government
            is_gov = "databricks.azure.us" in self.host.lower()
            login_host = "login.microsoftonline.us" if is_gov else "login.microsoftonline.com"
            resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
            url = f"https://{login_host}/{self._sp_tenant_id}/oauth2/v2.0/token"
            data = {
                "grant_type": "client_credentials",
                "client_id": self._sp_client_id,
                "client_secret": self._sp_client_secret,
                "scope": f"{resource_id}/.default",
            }
        else:
            # AWS / GCP — use Databricks OIDC endpoint
            url = f"{self.host}/oidc/v1/token"
            data = {
                "grant_type": "client_credentials",
                "client_id": self._sp_client_id,
                "client_secret": self._sp_client_secret,
                "scope": "all-apis",
            }

        resp = requests.post(url, data=data, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        self.session.headers["Authorization"] = f"Bearer {result['access_token']}"
        # Token typically expires in 3600s; refresh at 80% lifetime
        expires_in = int(result.get("expires_in", 3600))
        self._token_expires_at = time.time() + (expires_in * 0.8)
        log.info("SP token obtained, expires in %ds", expires_in)

    def _ensure_token_valid(self):
        """Refresh SP/SDK token if near expiry."""
        if self.auth_method == "sp" and time.time() >= self._token_expires_at:
            log.info("SP token nearing expiry, refreshing...")
            self._refresh_sp_token()
        elif self.auth_method == "auto" and hasattr(self, "_sdk_client"):
            # Re-fetch token from SDK (handles refresh internally)
            try:
                header = self._sdk_client.config.authenticate()
                if callable(header):
                    h = header()
                    if "Authorization" in h:
                        self.session.headers["Authorization"] = h["Authorization"]
                elif isinstance(header, dict) and "Authorization" in header:
                    self.session.headers["Authorization"] = header["Authorization"]
            except Exception:
                pass

    def _setup_sdk_auth(self):
        """Use databricks-sdk default auth chain (for Databricks Apps runtime)."""
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            # Auto-detect workspace URL from SDK if not provided
            if not self.host and w.config.host:
                self.host = w.config.host.rstrip("/")
                log.info("Auto-detected workspace URL from SDK: %s", self.host)
            # Extract token from SDK's auth
            header = w.config.authenticate()
            if callable(header):
                h = header()
                if "Authorization" in h:
                    self.session.headers["Authorization"] = h["Authorization"]
            elif isinstance(header, dict) and "Authorization" in header:
                self.session.headers["Authorization"] = header["Authorization"]
            # Store SDK client for token refresh
            self._sdk_client = w
        except Exception as e:
            log.warning("SDK auto-auth failed: %s. Falling back to env vars.", e)
            # Fallback: check environment
            import os
            token = os.environ.get("DATABRICKS_TOKEN", "")
            if token:
                self.session.headers["Authorization"] = f"Bearer {token}"

    # ---- Low-level HTTP ----

    def get(self, path: str, params: dict = None, **kwargs) -> dict:
        self._ensure_token_valid()
        try:
            r = self.session.get(f"{self.host}{path}", params=params, timeout=30, **kwargs)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (401, 403):
                log.warning("%s returned %d (auth/permission issue)", path, r.status_code)
            return {}
        except Exception as e:
            log.warning("%s failed: %s", path, e)
            return {}

    def post(self, path: str, data: dict = None, **kwargs) -> dict:
        self._ensure_token_valid()
        try:
            r = self.session.post(f"{self.host}{path}", json=data, timeout=60, **kwargs)
            if r.status_code in (200, 201):
                return r.json()
            log.warning("POST %s returned %d: %s", path, r.status_code, r.text[:300])
            try:
                return r.json()
            except Exception:
                return {}
        except Exception as e:
            log.warning("%s failed: %s", path, e)
            return {}

    def sql(self, warehouse_id: str, statement: str, timeout: str = "50s"):
        """Execute SQL and return (cols, rows).  Works on all clouds."""
        result = self.post("/api/2.0/sql/statements", {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": timeout,
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        })
        if not result:
            log.warning("SQL empty response [%s]", statement[:60])
            return [], []
        state = result.get("status", {}).get("state", "")
        if state == "SUCCEEDED":
            cols = [c["name"] for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
            rows = result.get("result", {}).get("data_array", [])
            return cols, rows
        if state in ("PENDING", "RUNNING"):
            sid = result.get("statement_id", "")
            for _ in range(12):
                time.sleep(5)
                poll = self.get(f"/api/2.0/sql/statements/{sid}")
                if poll.get("status", {}).get("state") == "SUCCEEDED":
                    cols = [c["name"] for c in poll.get("manifest", {}).get("schema", {}).get("columns", [])]
                    rows = poll.get("result", {}).get("data_array", [])
                    return cols, rows
            log.warning("SQL timed out: %s...", statement[:60])
            return [], []
        # Log error with full context
        err_msg = result.get("status", {}).get("error", {}).get("message", "")
        err_code = result.get("error_code", result.get("message", ""))
        if not err_msg and not state:
            err_msg = str(result)[:300]
        log.warning("SQL error [%s] state=%s: %s %s", statement[:60], state, err_code, err_msg[:200])
        return [], []

    # ---- Connection test ----

    def test_connection(self) -> dict:
        """Test auth and return workspace info.  Returns dict with 'ok', 'user', 'cloud', etc."""
        me = self.get("/api/2.0/preview/scim/v2/Me")
        username = me.get("userName", "")
        if not username:
            return {"ok": False, "error": "Authentication failed. Check credentials."}

        return {
            "ok": True,
            "user": username,
            "cloud": self.cloud,
            "workspace_url": self.host,
        }

    # ---- Phase 1: Security data (21 REST APIs) ----

    def collect_security_data(self) -> dict:
        log.info("Phase 1: Collecting security & architecture data...")
        data = {}

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
            log.info("  Fetching %s ...", key)
            data[key] = self.get(path)

        # DBFS root listing (may not exist on all workspaces)
        data["dbfs"] = self.post("/api/2.0/dbfs/list", {"path": "/"})

        # Init script contents
        data["init_contents"] = {}
        for s in data.get("init_scripts", {}).get("scripts", []):
            sid = s.get("script_id", "")
            if not sid:
                continue
            detail = self.get(f"/api/2.0/global-init-scripts/{sid}")
            content = base64.b64decode(detail.get("script", "")).decode("utf-8", "replace")
            data["init_contents"][sid] = content

        # Schemas per catalog (skip system/samples)
        data["all_schemas"] = {}
        for c in data.get("catalogs", {}).get("catalogs", []):
            name = c.get("name", "")
            if name in ("system", "samples", "__databricks_internal"):
                continue
            schemas = self.get(f"/api/2.1/unity-catalog/schemas?catalog_name={name}")
            data["all_schemas"][name] = schemas.get("schemas", [])

        # Store cloud info
        data["_cloud"] = self.cloud
        data["_workspace_url"] = self.host
        data["_govcloud"] = detect_govcloud(
            self.host,
            data.get("metastores", {}).get("metastores", [{}])[0].get("region", "")
            if data.get("metastores", {}).get("metastores") else ""
        )

        log.info("  Done. Collected %d datasets.", len(data))
        return data

    # ---- Phase 2: Usage data (7 SQL queries) ----

    def collect_usage_data(self, warehouses: dict) -> dict:
        log.info("Phase 2: Collecting usage & cost data...")
        data = {}

        # Find a running warehouse or use first available
        wh_list = warehouses.get("warehouses", [])
        wh_id = None
        for w in wh_list:
            if w.get("state") == "RUNNING":
                wh_id = w["id"]
                break
        if not wh_id and wh_list:
            wh_id = wh_list[0]["id"]
            log.info("  Starting warehouse %s ...", wh_id)
            self.post(f"/api/2.0/sql/warehouses/{wh_id}/start")
            for _ in range(24):
                time.sleep(5)
                status = self.get(f"/api/2.0/sql/warehouses/{wh_id}")
                if status.get("state") == "RUNNING":
                    break
            else:
                log.warning("Warehouse did not start in time.")
                return data

        if not wh_id:
            log.warning("No SQL warehouse available. Skipping usage queries.")
            return data

        data["warehouse_id"] = wh_id
        log.info("  Using warehouse: %s", wh_id)

        # --- Cloud-adaptive billing query ---
        cloud = self.cloud

        # 1. User activity (30d)
        log.info("  Querying user activity...")
        cols, rows = self.sql(wh_id,
            "SELECT executed_by, execution_status, COUNT(*) as cnt, "
            "ROUND(SUM(COALESCE(read_bytes,0))/1073741824, 4) as total_read_gb, "
            "SUM(COALESCE(read_rows,0)) as total_rows, "
            "ROUND(SUM(COALESCE(total_duration_ms,0))/1000/60, 2) as total_minutes "
            "FROM system.query.history WHERE start_time > date_sub(now(), 30) "
            "GROUP BY executed_by, execution_status ORDER BY cnt DESC"
        )
        data["user_queries"] = {"cols": cols, "rows": rows}
        log.info("  → user_queries: %d rows", len(rows))

        # 2. Daily query trends (30d)
        log.info("  Querying daily trends...")
        cols, rows = self.sql(wh_id,
            "SELECT DATE(start_time) as query_date, COUNT(*) as total_queries, "
            "SUM(CASE WHEN execution_status='FINISHED' THEN 1 ELSE 0 END) as succeeded, "
            "SUM(CASE WHEN execution_status='FAILED' THEN 1 ELSE 0 END) as failed, "
            "ROUND(SUM(COALESCE(read_bytes,0))/1073741824, 4) as read_gb, "
            "SUM(COALESCE(read_rows,0)) as read_rows, "
            "ROUND(SUM(COALESCE(total_duration_ms,0))/1000/60, 2) as total_minutes "
            "FROM system.query.history WHERE start_time > date_sub(now(), 30) "
            "GROUP BY DATE(start_time) ORDER BY query_date"
        )
        data["daily_queries"] = {"cols": cols, "rows": rows}
        log.info("  → daily_queries: %d rows", len(rows))

        # 3. Warehouse events (30d)
        log.info("  Querying warehouse events...")
        cols, rows = self.sql(wh_id,
            "SELECT warehouse_id, event_type, event_time, cluster_count "
            "FROM system.compute.warehouse_events "
            "WHERE event_time > date_sub(now(), 30) ORDER BY event_time DESC LIMIT 100"
        )
        data["warehouse_events"] = {"cols": cols, "rows": rows}
        log.info("  → warehouse_events: %d rows", len(rows))

        # 4. DBU list prices — cloud-adaptive
        log.info("  Querying DBU list prices...")
        # The cloud column varies; try to auto-detect
        price_query = (
            "SELECT sku_name, pricing.default as price_usd, usage_unit "
            "FROM system.billing.list_prices "
            "WHERE currency_code = 'USD' AND price_end_time IS NULL "
            "ORDER BY sku_name"
        )
        cols, rows = self.sql(wh_id, price_query)
        if not rows:
            # Some workspaces restrict system.billing — try without filter
            cols, rows = self.sql(wh_id,
                "SELECT sku_name, pricing.default as price_usd, usage_unit "
                "FROM system.billing.list_prices "
                "WHERE price_end_time IS NULL ORDER BY sku_name LIMIT 200"
            )
        data["list_prices"] = {"cols": cols, "rows": rows}
        log.info("  → list_prices: %d rows", len(rows))

        # 5. Schema overview
        log.info("  Querying table inventory...")
        cols, rows = self.sql(wh_id,
            "SELECT table_catalog, table_schema, COUNT(*) as table_count "
            "FROM system.information_schema.tables WHERE table_catalog NOT IN ('system') "
            "GROUP BY table_catalog, table_schema ORDER BY table_catalog, table_schema"
        )
        data["schema_overview"] = {"cols": cols, "rows": rows}

        # 6. Per-schema table details
        user_catalogs = set()
        for r in rows:
            if r[0] not in ("samples",):
                user_catalogs.add((r[0], r[1]))

        data["table_inventory"] = {}
        for cat, schema in user_catalogs:
            if schema == "information_schema":
                continue
            key = f"{cat}.{schema}"
            log.info("  Fetching tables for %s ...", key)
            cols2, rows2 = self.sql(wh_id,
                f"SELECT table_name, table_type, created, last_altered, comment "
                f"FROM system.information_schema.tables "
                f"WHERE table_catalog = '{cat}' AND table_schema = '{schema}' "
                f"ORDER BY table_name LIMIT 500"
            )
            data["table_inventory"][key] = {"cols": cols2, "rows": rows2}

        # 7. Sample table sizes via DESCRIBE DETAIL
        log.info("  Sampling table sizes...")
        data["table_sizes"] = {}
        for key, inv in data["table_inventory"].items():
            sampled = 0
            for r in inv["rows"]:
                if sampled >= 15:
                    break
                tname = r[0]
                try:
                    c, rr = self.sql(wh_id, f"DESCRIBE DETAIL `{key}`.`{tname}`")
                    if rr:
                        col_map = dict(zip(c, rr[0]))
                        size = col_map.get("sizeInBytes")
                        if size:
                            data["table_sizes"][f"{key}.{tname}"] = int(size)
                            sampled += 1
                except Exception:
                    pass

        log.info("  Done. Collected usage data with %d datasets.", len(data))
        return data

    # ---- Full collection ----

    def collect_all(self) -> dict:
        """Run both phases and return combined result."""
        sec_data = self.collect_security_data()
        usage_data = self.collect_usage_data(sec_data.get("warehouses", {}))
        return {"security": sec_data, "usage": usage_data}
