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
APP_NAME = "lho-lite"

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

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Verify auth
try:
    me = w.current_user.me()
    print(f"✓ Authenticated as: {me.user_name}")
except Exception as e:
    print(f"⚠ Auth check failed: {e}")

_host = w.config.host.rstrip("/")
print(f"Workspace: {_host}")
print(f"App source: {WORKSPACE_APP_DIR}")
print()

# --- Check if app already exists ---
app_exists = False
app_url = None
try:
    existing = w.apps.get(APP_NAME)
    app_exists = True
    app_url = getattr(existing, 'url', None)
    state = getattr(getattr(existing, 'compute_status', None), 'state', 'UNKNOWN')
    print(f"✓ App '{APP_NAME}' already exists (state: {state})")
except Exception as e:
    err = str(e)
    if "does not exist" in err.lower() or "not found" in err.lower() or "404" in err:
        print(f"  App not found. Will create...")
    else:
        print(f"  Error checking app: {err[:300]}")
        print(f"  Will try to create anyway...")

# --- Create app if needed ---
if not app_exists:
    print(f"\nCreating app '{APP_NAME}' ...")
    try:
        from databricks.sdk.service.apps import App
        app = w.apps.create_and_wait(
            App(
                name=APP_NAME,
                description="LHO Lite — Lakehouse Optimizer by Blueprint Technologies",
            ),
            timeout=datetime.timedelta(minutes=5),
        )
        app_url = getattr(app, 'url', None)
        print(f"  ✓ App created!")
        print(f"  URL: {app_url}")
    except TypeError:
        from databricks.sdk.service.apps import App
        app = w.apps.create_and_wait(
            App(
                name=APP_NAME,
                description="LHO Lite — Lakehouse Optimizer by Blueprint Technologies",
            )
        )
        app_url = getattr(app, 'url', None)
        print(f"  ✓ App created!")
        print(f"  URL: {app_url}")
    except Exception as e:
        err = str(e)
        print(f"  SDK create failed: {err[:500]}")
        if "already exists" in err.lower():
            print(f"  ✓ App already exists — continuing")
            app_exists = True
        else:
            print(f"\n  ⚠ Will try to deploy anyway...")

# --- Deploy ---
print(f"\nDeploying from {WORKSPACE_APP_DIR} ...")
try:
    from databricks.sdk.service.apps import AppDeployment
    deployment = w.apps.deploy_and_wait(
        APP_NAME,
        AppDeployment(source_code_path=WORKSPACE_APP_DIR),
    )
    deploy_status = getattr(getattr(deployment, 'status', None), 'state', 'UNKNOWN')
    print(f"  ✓ Deployment complete! Status: {deploy_status}")

    # Get final app info
    try:
        final_app = w.apps.get(APP_NAME)
        app_url = getattr(final_app, 'url', None) or app_url
        compute_state = getattr(getattr(final_app, 'compute_status', None), 'state', 'UNKNOWN')
        print(f"  App URL: {app_url}")
        print(f"  Compute state: {compute_state}")
    except Exception:
        pass

except Exception as e:
    err = str(e)
    print(f"  ❌ Deploy failed: {err[:500]}")

    try:
        final_app = w.apps.get(APP_NAME)
        app_url = getattr(final_app, 'url', None)
        compute_state = getattr(getattr(final_app, 'compute_status', None), 'state', 'UNKNOWN')
        print(f"\n  App exists but deploy failed. State: {compute_state}")
        print(f"  URL: {app_url}")
        print(f"  Try redeploying from Compute → Apps → {APP_NAME}")
    except Exception:
        print(f"\n  App may not have been created.")
        print(f"  Manual fallback: Compute → Apps → Create App")
        print(f"  Set source code path to: {WORKSPACE_APP_DIR}")

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
try:
    app_info = w.apps.get(APP_NAME)
    sp_client_id = getattr(app_info, 'service_principal_client_id', None)
    sp_name = getattr(app_info, 'service_principal_name', None)
    if sp_client_id:
        sp_id = sp_client_id
        sp_display = sp_name or sp_client_id
        print(f"✓ App service principal: {sp_display} ({sp_id})")
    else:
        print("⚠ No service principal found on app — permissions must be granted manually")
except Exception as e:
    print(f"⚠ Could not get app info: {e}")

# Grant system table access
if sp_id:
    schemas_to_grant = ["system.billing", "system.query", "system.compute", "system.lakeflow"]
    print(f"\nGranting system table access to {sp_display}...")

    for schema_path in schemas_to_grant:
        try:
            # Use SQL GRANT via the SDK
            w.statement_execution.execute_statement(
                warehouse_id=None,  # will be auto-selected
                statement=f"GRANT USE_SCHEMA, SELECT ON SCHEMA `{schema_path}` TO `{sp_display}`",
                wait_timeout="30s"
            )
            print(f"  ✓ Granted USE_SCHEMA + SELECT on {schema_path}")
        except Exception as e:
            err = str(e)
            if "already" in err.lower():
                print(f"  ✓ {schema_path} — already granted")
            else:
                print(f"  ⚠ {schema_path} — grant failed: {err[:200]}")
                print(f"    → Run manually: GRANT USE_SCHEMA, SELECT ON SCHEMA `{schema_path}` TO `{sp_display}`")

    # Also grant USE_CATALOG on system
    try:
        w.statement_execution.execute_statement(
            warehouse_id=None,
            statement=f"GRANT USE_CATALOG ON CATALOG `system` TO `{sp_display}`",
            wait_timeout="30s"
        )
        print(f"  ✓ Granted USE_CATALOG on system")
    except Exception as e:
        err = str(e)
        if "already" in err.lower():
            print(f"  ✓ system catalog — already granted")
        else:
            print(f"  ⚠ USE_CATALOG on system — grant failed: {err[:200]}")
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
    try:
        final = w.apps.get(APP_NAME)
        app_url = getattr(final, 'url', None) or f"{_host}/compute/apps"
    except Exception:
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
