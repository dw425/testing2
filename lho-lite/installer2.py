# Databricks notebook source
# MAGIC %md
# MAGIC # LHO Lite — One-Click Installer
# MAGIC
# MAGIC **Lakehouse Optimizer Lite** by Blueprint Technologies
# MAGIC
# MAGIC This notebook downloads and deploys the latest version of LHO Lite as a Databricks App in your workspace.
# MAGIC
# MAGIC ### Instructions
# MAGIC 1. Fill in your **License Key** below
# MAGIC 2. (Optional) Add a GitHub token if the repo is private
# MAGIC 3. Click **Run All**
# MAGIC 4. When complete, click the app link to open LHO Lite
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# App name in your workspace (lowercase, hyphens ok)
APP_NAME = "lho-litev2"

# License key for this deployment (get from Blueprint Technologies)
LICENSE_KEY = "LHO-DEMO-0001-BPTECH"

# GitHub repo coordinates
GITHUB_OWNER = "dw425"
GITHUB_REPO = "testing2"
GITHUB_BRANCH = "main"
GITHUB_PATH = "lho-lite"  # subdirectory in the repo

# Authentication for private repos (leave empty for public repos)
# Option 1: Paste a GitHub PAT here
GITHUB_TOKEN = ""

# Option 2: Use a Databricks secret (recommended for shared workspaces)
SECRET_SCOPE = ""   # e.g. "blueprint"
SECRET_KEY = ""     # e.g. "github-pat"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Download App Code from GitHub

# COMMAND ----------

import requests
import json
import base64
import os
import datetime

# Resolve GitHub token
_token = GITHUB_TOKEN
if SECRET_SCOPE and SECRET_KEY and not _token:
    try:
        _token = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
        print(f"✓ Using GitHub token from secret scope '{SECRET_SCOPE}'")
    except Exception as e:
        print(f"⚠ Could not read secret {SECRET_SCOPE}/{SECRET_KEY}: {e}")
        print("  Falling back to unauthenticated access (public repos only)")

_headers = {"Accept": "application/vnd.github.v3+json"}
if _token:
    _headers["Authorization"] = f"token {_token}"
    print("✓ GitHub authentication configured")
else:
    print("ℹ No GitHub token — using unauthenticated access (works for public repos)")

API_BASE = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}"

def github_get(path):
    """GET from GitHub API with auth headers."""
    r = requests.get(f"{API_BASE}/{path}", headers=_headers, timeout=30)
    r.raise_for_status()
    return r.json()

def get_file_content(sha):
    """Download file content by blob SHA."""
    r = requests.get(f"{API_BASE}/git/blobs/{sha}", headers=_headers, timeout=30)
    r.raise_for_status()
    blob = r.json()
    if blob.get("encoding") == "base64":
        return base64.b64decode(blob["content"])
    return blob["content"].encode()

# Get the commit tree for the branch
print(f"\nFetching latest code from {GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}/{GITHUB_PATH} ...")
branch_info = github_get(f"branches/{GITHUB_BRANCH}")
tree_sha = branch_info["commit"]["commit"]["tree"]["sha"]
commit_sha = branch_info["commit"]["sha"][:8]

# Get full tree and filter to our subdirectory
full_tree = requests.get(
    f"{API_BASE}/git/trees/{tree_sha}",
    headers=_headers,
    params={"recursive": "1"},
    timeout=30
).json()

# Files to skip (not needed for the running app)
SKIP_PREFIXES = (
    "data/", "__pycache__/", "app/__pycache__/",
    "seed_demo", "deploy.sh", "databricks.yml", "Dockerfile",
    "installer", ".gitignore", "README", "lite.md",
)

app_files = []
for item in full_tree.get("tree", []):
    if item["type"] == "blob" and item["path"].startswith(f"{GITHUB_PATH}/"):
        rel_path = item["path"][len(GITHUB_PATH) + 1:]
        if any(rel_path.startswith(p) for p in SKIP_PREFIXES):
            continue
        # Also skip __pycache__ anywhere in path
        if "/__pycache__/" in rel_path or rel_path.endswith(".pyc"):
            continue
        app_files.append({"path": rel_path, "sha": item["sha"], "size": item.get("size", 0)})

print(f"✓ Found {len(app_files)} app files (commit {commit_sha})")
for f in app_files:
    print(f"  • {f['path']} ({f['size']} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Write App Files to Workspace

# COMMAND ----------

WORKSPACE_APP_DIR = f"/Workspace/Applications/{APP_NAME}"

print(f"Writing app files to {WORKSPACE_APP_DIR} ...")

# Create directories
dirs_needed = set()
for f in app_files:
    parts = f["path"].split("/")
    for i in range(1, len(parts)):
        dirs_needed.add("/".join(parts[:i]))

for d in sorted(dirs_needed):
    os.makedirs(f"{WORKSPACE_APP_DIR}/{d}", exist_ok=True)

# Download and write each file
written = 0
for f in app_files:
    content = get_file_content(f["sha"])
    full_path = f"{WORKSPACE_APP_DIR}/{f['path']}"
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with open(full_path, "wb") as fh:
        fh.write(content)
    written += 1
    print(f"  ✓ {f['path']}")

# Create empty data directory
os.makedirs(f"{WORKSPACE_APP_DIR}/data", exist_ok=True)

# Ensure __init__.py exists
init_path = f"{WORKSPACE_APP_DIR}/app/__init__.py"
if not os.path.exists(init_path):
    with open(init_path, "w") as fh:
        fh.write("")

# Pre-seed config: license key + auto auth method
import sqlite3
db_path = f"{WORKSPACE_APP_DIR}/data/lho_lite.db"
db = sqlite3.connect(db_path)
db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL, encrypted INTEGER DEFAULT 0)")
if LICENSE_KEY:
    # Seed as pending — main.py promotes it on first startup
    db.execute("INSERT OR REPLACE INTO config (key, value, encrypted) VALUES (?, ?, 0)",
               ("license_key_pending", LICENSE_KEY))
    print(f"  ✓ License key pre-configured: {LICENSE_KEY[:12]}...")
# Pre-set auth to auto (SDK) so the admin page shows the right default
db.execute("INSERT OR REPLACE INTO config (key, value, encrypted) VALUES (?, ?, 0)",
           ("auth_method", "auto"))
db.commit()
db.close()

print(f"\n✓ {written} files written to {WORKSPACE_APP_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create & Deploy the Databricks App

# COMMAND ----------

import time as _time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Build auth headers for REST API calls (SDK Apps API not available on all runtimes)
_host = w.config.host.rstrip("/")
_auth_headers = {}
try:
    _auth_func = w.config.authenticate()
    if callable(_auth_func):
        _auth_headers = _auth_func()
    elif isinstance(_auth_func, dict):
        _auth_headers = _auth_func
except Exception:
    pass
if not _auth_headers:
    # Fallback: get token from SDK config
    try:
        _auth_headers = {"Authorization": f"Bearer {w.config.token}"}
    except Exception:
        pass

def _api(method, path, body=None):
    """Call Databricks REST API."""
    url = f"{_host}{path}"
    r = requests.request(method, url, headers=_auth_headers, json=body, timeout=60)
    return r.status_code, r.json() if r.text else {}

# Verify auth
try:
    me = w.current_user.me()
    print(f"✓ Authenticated as: {me.user_name}")
except Exception as e:
    print(f"⚠ Auth check failed: {e}")

print(f"Workspace: {_host}")
print(f"App source: {WORKSPACE_APP_DIR}")
print()

# --- Check if app already exists ---
app_exists = False
app_url = None
status_code, app_data = _api("GET", f"/api/2.0/apps/{APP_NAME}")
if status_code == 200:
    app_exists = True
    app_url = app_data.get("url")
    compute_state = (app_data.get("compute_status") or {}).get("state", "UNKNOWN")
    print(f"✓ App '{APP_NAME}' already exists (state: {compute_state})")

    # If app is being deleted, wait for deletion to complete
    if compute_state in ("DELETING", "STOPPING"):
        print(f"  App is being deleted, waiting...")
        for i in range(18):  # up to 3 minutes
            _time.sleep(10)
            sc, data = _api("GET", f"/api/2.0/apps/{APP_NAME}")
            if sc != 200:
                app_exists = False
                print(f"  ✓ App deletion complete")
                break
            compute_state = (data.get("compute_status") or {}).get("state", "UNKNOWN")
            print(f"  ... state: {compute_state}")
            if compute_state not in ("DELETING", "STOPPING"):
                break
else:
    err_msg = app_data.get("message", str(status_code))
    if status_code == 404 or "not exist" in err_msg.lower() or "deleted" in err_msg.lower():
        print(f"  App not found. Will create...")
    else:
        print(f"  Error checking app ({status_code}): {err_msg[:300]}")
        print(f"  Will try to create anyway...")

# --- Create app if needed ---
if not app_exists:
    print(f"\nCreating app '{APP_NAME}' ...")
    for _attempt in range(4):
        sc, data = _api("POST", "/api/2.0/apps", {
            "name": APP_NAME,
            "description": "LHO Lite — Lakehouse Optimizer by Blueprint Technologies",
        })
        if sc in (200, 201):
            app_url = data.get("url")
            print(f"  ✓ App create initiated!")
            if app_url:
                print(f"  URL: {app_url}")
            break
        err_msg = data.get("message", str(sc))
        if "already exists" in err_msg.lower():
            print(f"  ✓ App already exists — continuing")
            sc2, d2 = _api("GET", f"/api/2.0/apps/{APP_NAME}")
            if sc2 == 200:
                app_url = d2.get("url")
            break
        elif "deleted" in err_msg.lower() or "reserved" in err_msg.lower() or sc == 409:
            print(f"  Name still reserved from deletion. Waiting 30s... (attempt {_attempt+1}/4)")
            _time.sleep(30)
        else:
            print(f"  Create failed ({sc}): {err_msg[:400]}")
            if _attempt < 3:
                print(f"  Retrying in 15s... (attempt {_attempt+1}/4)")
                _time.sleep(15)
            else:
                print(f"\n  ⚠ Could not create app. Try manually: Compute → Apps → Create App")

# --- Wait for compute to be ACTIVE before deploying ---
print(f"\nWaiting for app compute to be ready...")
for i in range(30):  # up to 5 minutes
    sc, data = _api("GET", f"/api/2.0/apps/{APP_NAME}")
    if sc == 200:
        compute_state = (data.get("compute_status") or {}).get("state", "UNKNOWN")
        app_url = data.get("url") or app_url
        if compute_state == "ACTIVE":
            print(f"  ✓ Compute is ACTIVE")
            break
        print(f"  ... compute state: {compute_state} (waiting)")
    _time.sleep(10)
else:
    print(f"  ⚠ Compute not active after 5 minutes — attempting deploy anyway")

# --- Deploy ---
print(f"\nDeploying from {WORKSPACE_APP_DIR} ...")
sc, data = _api("POST", f"/api/2.0/apps/{APP_NAME}/deployments", {
    "source_code_path": WORKSPACE_APP_DIR,
})
if sc in (200, 201):
    deployment_id = data.get("deployment_id", "")
    print(f"  Deploy initiated (ID: {deployment_id})")

    # Poll for deployment completion
    for i in range(30):  # up to 5 minutes
        _time.sleep(10)
        sc2, d2 = _api("GET", f"/api/2.0/apps/{APP_NAME}/deployments/{deployment_id}")
        if sc2 == 200:
            state = (d2.get("status") or {}).get("state", "UNKNOWN")
            if state == "SUCCEEDED":
                print(f"  ✓ Deployment complete!")
                break
            elif state in ("FAILED", "CANCELLED"):
                msg = (d2.get("status") or {}).get("message", "")
                print(f"  ❌ Deployment {state}: {msg[:300]}")
                break
            print(f"  ... deploy state: {state}")
    else:
        print(f"  ⚠ Deployment still in progress — check Compute → Apps")

    # Get final app info
    sc3, d3 = _api("GET", f"/api/2.0/apps/{APP_NAME}")
    if sc3 == 200:
        app_url = d3.get("url") or app_url
        compute_state = (d3.get("compute_status") or {}).get("state", "UNKNOWN")
        print(f"  App URL: {app_url}")
        print(f"  Compute state: {compute_state}")
else:
    err_msg = data.get("message", str(sc))
    print(f"  ❌ Deploy failed ({sc}): {err_msg[:500]}")
    print(f"  Try redeploying from Compute → Apps → {APP_NAME}")

if not app_url:
    app_url = f"{_host}/compute/apps"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Grant System Table Access to App Service Principal
# MAGIC
# MAGIC LHO Lite needs access to `system.billing`, `system.query`, and `system.compute` schemas for usage analytics.
# MAGIC This step grants the app's service principal the required permissions.

# COMMAND ----------

# Get the app's service principal
sp_id = None
sp_display = None
sc, app_data = _api("GET", f"/api/2.0/apps/{APP_NAME}")
if sc == 200:
    sp_id = app_data.get("service_principal_client_id")
    sp_display = app_data.get("service_principal_name") or sp_id
    if sp_id:
        print(f"✓ App service principal: {sp_display} ({sp_id})")
    else:
        print("⚠ No service principal found on app — permissions must be granted manually")
else:
    print(f"⚠ Could not get app info ({sc}) — permissions must be granted manually")

# Grant system table access via Unity Catalog REST API (uses SP UUID which always works)
if sp_id:
    schemas_to_grant = ["system.billing", "system.query", "system.compute", "system.lakeflow"]
    print(f"\nGranting system table access to {sp_display} ({sp_id})...")
    print("  (requires metastore admin privileges)")

    for schema_path in schemas_to_grant:
        sc, data = _api("PATCH", f"/api/2.1/unity-catalog/permissions/schema/{schema_path}", {
            "changes": [{"principal": sp_id, "add": ["USE_SCHEMA", "SELECT"]}]
        })
        if sc == 200:
            print(f"  ✓ Granted USE_SCHEMA + SELECT on {schema_path}")
        else:
            err_msg = data.get("message", str(sc))
            print(f"  ⚠ {schema_path} — grant failed ({sc}): {err_msg[:200]}")
            print(f"    → Run manually: GRANT USE_SCHEMA, SELECT ON SCHEMA `{schema_path}` TO `{sp_display}`")

    # Also grant USE_CATALOG on system
    sc, data = _api("PATCH", "/api/2.1/unity-catalog/permissions/catalog/system", {
        "changes": [{"principal": sp_id, "add": ["USE_CATALOG"]}]
    })
    if sc == 200:
        print(f"  ✓ Granted USE_CATALOG on system")
    else:
        err_msg = data.get("message", str(sc))
        print(f"  ⚠ USE_CATALOG on system — grant failed ({sc}): {err_msg[:200]}")
        print(f"    → Run manually: GRANT USE_CATALOG ON CATALOG `system` TO `{sp_display}`")
else:
    print("\n⚠ Skipping system table grants — no service principal detected.")
    print("  You will need to grant these manually after the app starts:")
    print("  GRANT USE_CATALOG ON CATALOG `system` TO `<app-service-principal>`")
    print("  GRANT USE_SCHEMA, SELECT ON SCHEMA `system.billing` TO `<app-service-principal>`")
    print("  GRANT USE_SCHEMA, SELECT ON SCHEMA `system.query` TO `<app-service-principal>`")
    print("  GRANT USE_SCHEMA, SELECT ON SCHEMA `system.compute` TO `<app-service-principal>`")
    print("  GRANT USE_SCHEMA, SELECT ON SCHEMA `system.lakeflow` TO `<app-service-principal>`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Done! Open LHO Lite

# COMMAND ----------

# Get the app URL
if not app_url or "Compute" in str(app_url):
    sc, data = _api("GET", f"/api/2.0/apps/{APP_NAME}")
    if sc == 200:
        app_url = data.get("url") or f"{_host}/compute/apps"
    else:
        app_url = f"{_host}/compute/apps"

print("=" * 60)
print("  LHO Lite — Lakehouse Optimizer")
print("  by Blueprint Technologies")
print("=" * 60)
print()
print(f"  ✅ App deployed successfully!")
print()
print(f"  🔗 Open: {app_url}")
print()
print("  NEXT STEPS:")
print("  1. Click the link above to open LHO Lite")
if LICENSE_KEY:
    print(f"  2. License key is pre-loaded: {LICENSE_KEY[:12]}...")
    print("  3. Authentication is set to Auto (SDK) — no tokens needed")
    print("  4. Click Save → data collection starts (~2-4 min)")
else:
    print("  2. Enter your license key from Blueprint Technologies")
    print("  3. Authentication is set to Auto (SDK) — no tokens needed")
    print("  4. Click Save → data collection starts (~2-4 min)")
print()
print("  PERMISSIONS (handled in Step 4, verify if needed):")
print("  • App SP needs CAN_USE on a SQL warehouse")
print("  • System table grants: billing, query, compute, lakeflow")
print("  • If Step 4 grants failed, run them manually as a metastore admin")
print()
print("=" * 60)

# Display clickable link in notebook
displayHTML(f"""
<div style="background:#161B22;border:1px solid #272D3F;border-radius:12px;padding:32px;font-family:'DM Sans',sans-serif;max-width:600px;margin:20px auto">
  <h2 style="color:#E6EDF3;margin:0 0 8px">✅ LHO Lite Deployed</h2>
  <p style="color:#8B949E;margin:0 0 20px">Lakehouse Optimizer by Blueprint Technologies</p>
  <a href="{app_url}" target="_blank" style="display:inline-block;background:#4B7BF5;color:white;padding:12px 32px;border-radius:8px;text-decoration:none;font-weight:600;font-size:16px">
    Open LHO Lite →
  </a>
  <p style="color:#8B949E;margin:16px 0 0;font-size:13px">
    The admin setup page will appear on first visit.<br>
    Auth is set to <b>Auto (SDK)</b> — just click Save to start.
  </p>
</div>
""")
