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

# Verify auth works
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
            timeout=time.timedelta(minutes=5) if hasattr(time, 'timedelta') else None,
        )
        app_url = getattr(app, 'url', None)
        print(f"  ✓ App created via SDK!")
        print(f"  URL: {app_url}")
    except TypeError:
        # Some SDK versions don't accept timeout in create_and_wait
        from databricks.sdk.service.apps import App
        app = w.apps.create_and_wait(
            App(
                name=APP_NAME,
                description="LHO Lite — Lakehouse Optimizer by Blueprint Technologies",
            )
        )
        app_url = getattr(app, 'url', None)
        print(f"  ✓ App created via SDK!")
        print(f"  URL: {app_url}")
    except Exception as e:
        err = str(e)
        print(f"  SDK create failed: {err[:500]}")
        if "already exists" in err.lower():
            print(f"  ✓ App already exists — continuing")
            app_exists = True
        else:
            # Try listing all apps to debug
            print(f"\n  Listing all apps for debugging...")
            try:
                for a in w.apps.list():
                    print(f"    - {a.name} (url: {getattr(a, 'url', 'N/A')})")
            except Exception as le:
                print(f"    List apps failed: {le}")
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

    # If deploy failed, try to get app info anyway
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
    app_url = f"{_host}/compute/apps"  # link to apps list as fallback

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Done! Open LHO Lite

# COMMAND ----------

# Get the app URL (app_url was set in Step 3)
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
