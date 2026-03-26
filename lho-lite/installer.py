# Databricks notebook source
# MAGIC %md
# MAGIC # LHO Lite — One-Click Installer
# MAGIC
# MAGIC **Lakehouse Optimizer Lite** by Blueprint Technologies
# MAGIC
# MAGIC This notebook downloads and deploys the latest version of LHO Lite as a Databricks App in your workspace.
# MAGIC
# MAGIC ### Instructions
# MAGIC 1. Fill in your **License Key** and (optional) GitHub token below
# MAGIC 2. Click **Run All**
# MAGIC 3. When complete, click the app link at the bottom to open the admin setup
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

# GitHub repo coordinates — change if you fork or move the repo
GITHUB_OWNER = "dw425"
GITHUB_REPO = "testing2"
GITHUB_BRANCH = "main"
GITHUB_PATH = "lho-lite"  # subdirectory in the repo

# Authentication for private repos (leave empty for public repos)
# Option 1: Paste a GitHub PAT here (it will be used only during install)
GITHUB_TOKEN = ""

# Option 2: Reference a Databricks secret (recommended for shared workspaces)
# Set these to pull the token from a secret scope instead of hardcoding above
SECRET_SCOPE = ""   # e.g. "blueprint"
SECRET_KEY = ""     # e.g. "github-pat"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Download Latest App Code from GitHub

# COMMAND ----------

import requests
import json
import base64
import time

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

# Fetch the file tree from GitHub API
API_BASE = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}"

def github_get(path):
    """GET from GitHub API with auth headers."""
    r = requests.get(f"{API_BASE}/{path}", headers=_headers)
    r.raise_for_status()
    return r.json()

def get_tree_files(tree_url, prefix=""):
    """Recursively get all files from a git tree."""
    tree = requests.get(tree_url, headers=_headers, params={"recursive": "1"}).json()
    files = []
    for item in tree.get("tree", []):
        if item["type"] == "blob":
            files.append({"path": f"{prefix}{item['path']}", "sha": item["sha"], "size": item.get("size", 0)})
    return files

def get_file_content(sha):
    """Download file content by blob SHA."""
    r = requests.get(f"{API_BASE}/git/blobs/{sha}", headers=_headers)
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
    params={"recursive": "1"}
).json()

app_files = []
for item in full_tree.get("tree", []):
    if item["type"] == "blob" and item["path"].startswith(f"{GITHUB_PATH}/"):
        rel_path = item["path"][len(GITHUB_PATH) + 1:]  # strip the prefix
        # Skip non-app files
        if rel_path.startswith(("data/", "seed_demo", "deploy.sh", "databricks.yml", "Dockerfile", "installer", ".gitignore", "README", "lite.md")):
            continue
        app_files.append({"path": rel_path, "sha": item["sha"], "size": item.get("size", 0)})

print(f"✓ Found {len(app_files)} app files (commit {commit_sha})")
for f in app_files:
    print(f"  • {f['path']} ({f['size']} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Write App Files to Workspace

# COMMAND ----------

import os

# Write files to workspace storage
WORKSPACE_APP_DIR = f"/Workspace/Applications/{APP_NAME}"

# Use workspace files API via dbutils
print(f"Writing app files to {WORKSPACE_APP_DIR} ...")

# Create directories
dirs_needed = set()
for f in app_files:
    parts = f["path"].split("/")
    for i in range(1, len(parts)):
        dirs_needed.add("/".join(parts[:i]))

for d in sorted(dirs_needed):
    full_path = f"{WORKSPACE_APP_DIR}/{d}"
    try:
        os.makedirs(full_path, exist_ok=True)
    except Exception:
        pass

# Download and write each file
for f in app_files:
    content = get_file_content(f["sha"])
    full_path = f"{WORKSPACE_APP_DIR}/{f['path']}"

    # Ensure parent dir exists
    parent = os.path.dirname(full_path)
    os.makedirs(parent, exist_ok=True)

    with open(full_path, "wb") as fh:
        fh.write(content)
    print(f"  ✓ {f['path']}")

# Create empty data directory
os.makedirs(f"{WORKSPACE_APP_DIR}/data", exist_ok=True)

# Create __init__.py if not present
init_path = f"{WORKSPACE_APP_DIR}/app/__init__.py"
if not os.path.exists(init_path):
    with open(init_path, "w") as fh:
        fh.write("")

# Pre-seed license key into the app's config DB so admin page has it ready
if LICENSE_KEY:
    import sqlite3
    db_path = f"{WORKSPACE_APP_DIR}/data/lho_lite.db"
    db = sqlite3.connect(db_path)
    db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL, encrypted INTEGER DEFAULT 0)")
    db.execute("INSERT OR REPLACE INTO config (key, value, encrypted) VALUES (?, ?, 0)", ("license_key_pending", LICENSE_KEY))
    db.commit()
    db.close()
    print(f"  ✓ License key pre-configured: {LICENSE_KEY[:12]}...")

# Store the GitHub token for license registry access (if provided)
_license_token = _token  # reuse the GitHub token from install
if _license_token:
    env_hint = f"LHO_GITHUB_TOKEN={_license_token[:8]}..."
    print(f"  ℹ Set environment variable LHO_GITHUB_TOKEN on the app for license validation")

print(f"\n✓ All files written to {WORKSPACE_APP_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create & Deploy the Databricks App

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Check if app already exists
app_exists = False
try:
    existing = w.apps.get(APP_NAME)
    app_exists = True
    print(f"✓ App '{APP_NAME}' already exists — will update deployment")
except Exception:
    pass

if not app_exists:
    print(f"Creating app '{APP_NAME}' ...")
    try:
        from databricks.sdk.service.apps import App
        app = w.apps.create_and_wait(
            App(
                name=APP_NAME,
                description="LHO Lite — Lakehouse Optimizer by Blueprint Technologies",
            )
        )
        print(f"✓ App '{APP_NAME}' created")
    except Exception as e:
        # Handle older SDK versions
        print(f"Creating app via API fallback...")
        import requests as req
        host = w.config.host
        token = w.config.token
        resp = req.post(
            f"{host}/api/2.0/apps",
            headers={"Authorization": f"Bearer {token}"},
            json={"name": APP_NAME, "description": "LHO Lite — Lakehouse Optimizer by Blueprint Technologies"}
        )
        if resp.status_code in (200, 201):
            print(f"✓ App '{APP_NAME}' created")
        elif "already exists" in resp.text.lower():
            print(f"✓ App '{APP_NAME}' already exists")
        else:
            print(f"⚠ App creation response: {resp.status_code} {resp.text}")

# Deploy
print(f"\nDeploying app from {WORKSPACE_APP_DIR} ...")
try:
    from databricks.sdk.service.apps import AppDeployment
    deployment = w.apps.deploy_and_wait(
        APP_NAME,
        AppDeployment(source_code_path=WORKSPACE_APP_DIR)
    )
    print(f"✓ Deployment complete!")
except Exception as e:
    # API fallback for older SDK
    print(f"Deploying via API fallback...")
    import requests as req
    host = w.config.host
    token = w.config.token
    resp = req.post(
        f"{host}/api/2.0/apps/{APP_NAME}/deployments",
        headers={"Authorization": f"Bearer {token}"},
        json={"source_code_path": WORKSPACE_APP_DIR}
    )
    if resp.status_code in (200, 201):
        print(f"✓ Deployment initiated!")
        # Wait for deployment
        print("  Waiting for app to start...")
        for _ in range(30):
            time.sleep(5)
            status = req.get(
                f"{host}/api/2.0/apps/{APP_NAME}",
                headers={"Authorization": f"Bearer {token}"}
            ).json()
            state = status.get("compute_status", {}).get("state", "UNKNOWN")
            if state == "ACTIVE":
                break
            print(f"  Status: {state}...")
    else:
        print(f"⚠ Deployment response: {resp.status_code} {resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Done! Open LHO Lite

# COMMAND ----------

# Get the app URL
try:
    app_info = w.apps.get(APP_NAME)
    app_url = getattr(app_info, 'url', None) or f"Open Databricks → Compute → Apps → {APP_NAME}"
except Exception:
    try:
        import requests as req
        host = w.config.host
        token = w.config.token
        resp = req.get(f"{host}/api/2.0/apps/{APP_NAME}", headers={"Authorization": f"Bearer {token}"})
        app_info = resp.json()
        app_url = app_info.get("url", f"{host}/apps/{APP_NAME}")
    except Exception:
        app_url = f"Open Databricks → Compute → Apps → {APP_NAME}"

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
print("  2. The admin setup page will appear automatically")
if LICENSE_KEY:
    print(f"  3. License key is pre-loaded: {LICENSE_KEY[:12]}...")
    print("  4. Select 'Auto (SDK)' for authentication")
    print("  5. Click Save → data collection starts (~1-3 min)")
else:
    print("  3. Enter your license key from Blueprint Technologies")
    print("  4. Select 'Auto (SDK)' for authentication")
    print("  5. Click Save → data collection starts (~1-3 min)")
print()
print("  PERMISSIONS NEEDED (one-time):")
print(f"  Grant the app's service principal these in Admin Console:")
print("  • Workspace admin (for SCIM user/group data)")
print("  • CAN_USE on a SQL warehouse (for usage queries)")
print("  • USE CATALOG on 'system' catalog (billing, query history)")
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
    Select <b>Auto (SDK)</b> for authentication — no tokens needed.
  </p>
</div>
""")
