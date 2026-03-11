# Blueprint IQ - Deployment Agent Guide

Standard CI/CD pathway for deploying the Blueprint AI Demo Hub to Databricks Apps.
Follow this process every time changes need to be deployed.

---

## Prerequisites

- Databricks CLI installed and authenticated (`databricks auth login`)
- Python 3.10+ with dependencies from `requirements.txt`
- Access to the target Databricks workspace
- Git repo pushed to origin (GitHub)

## Deployment Pipeline

### Step 1: Validate Locally

```bash
# Run all tests before deploying
python3 -m pytest tests/ -v

# Verify the app starts locally (optional)
python3 app/main.py
# App runs at http://localhost:8050
```

### Step 2: Commit & Push to GitHub

```bash
git add <changed-files>
git commit -m "Description of changes"
git push origin main
```

### Step 3: Sync Files to Databricks Workspace

```bash
# Sync the entire project to the Databricks workspace repo
databricks sync . /Workspace/Repos/blueprint-iq --watch
```

Or for a one-time sync (no watch):

```bash
databricks sync . /Workspace/Repos/blueprint-iq
```

### Step 4: Deploy the Databricks App

```bash
# Deploy (or redeploy) the app
databricks apps deploy blueprint-iq
```

If this is the **first deployment** (app doesn't exist yet):

```bash
databricks apps create blueprint-iq --manifest app.yaml
```

### Step 5: Verify Deployment

```bash
# Check app status
databricks apps get blueprint-iq

# Get the app URL
databricks apps get blueprint-iq --output json | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))"
```

Open the app URL in a browser and verify all 7 verticals load correctly.

---

## App Configuration

**Manifest file:** `app.yaml`

```yaml
command:
  - python
  - app/main.py
env:
  - name: USE_DEMO_DATA
    value: "true"
  - name: DATABRICKS_FM_ENDPOINT
    value: "databricks-claude-sonnet-4-6"
```

**Key environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_DEMO_DATA` | `true` | Set to `false` to use live Databricks SQL |
| `USE_CASE` | `gaming` | Default vertical on startup |
| `DATABRICKS_FM_ENDPOINT` | `databricks-claude-sonnet-4-6` | Foundation Model endpoint for Genie AI |

---

## Architecture

```
blueprint-iq/
  app.yaml              # Databricks Apps manifest
  requirements.txt      # Python dependencies
  app/
    main.py             # Dash entry point (port 8050)
    theme.py            # Color palette & CSS
    layout.py           # Sidebar + shell layout
    page_styles.py      # 6 shared layout builders
    data_access.py      # Config loader + SQL connector
    genie_backend.py    # Genie AI chat backend
    pages/              # 7 vertical page modules
      gaming.py
      telecom.py
      media.py
      financial_services.py
      hls.py
      manufacturing.py
      risk.py
  config/               # 7 vertical YAML configs
  lakehouse/            # Delta Lake pipeline scripts
  ml/                   # ML model training scripts
  notebooks/            # Databricks notebooks
  tests/                # pytest test suite
```

---

## Verticals (7 total)

| Vertical | Config | Pages | Genie Questions |
|----------|--------|-------|-----------------|
| GamingIQ | `config/gaming.yaml` | 7 | 100 |
| TelecomIQ | `config/telecom.yaml` | 7 | 100 |
| MediaIQ | `config/media.yaml` | 7 | 100 |
| Financial ServicesIQ | `config/financial_services.yaml` | 7 | 100 |
| HLS | `config/hls.yaml` | 7 | 100 |
| ManufacturingIQ | `config/manufacturing.yaml` | 7 | 100 |
| RiskIQ | `config/risk.yaml` | 7 | 100 |

---

## Rollback

If a deployment fails or introduces issues:

```bash
# Revert to previous commit
git revert HEAD
git push origin main

# Re-sync and redeploy
databricks sync . /Workspace/Repos/blueprint-iq
databricks apps deploy blueprint-iq
```

---

## Lakehouse Setup (First-Time Only)

If setting up a new workspace, run the lakehouse pipeline notebooks in order:

1. `lakehouse/01_setup_catalog.sql` - Create Unity Catalog & schemas
2. `lakehouse/02_bronze_ingest.py` - Raw data ingestion
3. `lakehouse/03_silver_transform.py` - Silver transforms
4. `lakehouse/04_gold_aggregate.py` - Gold KPI aggregates
5. `lakehouse/05_seed_demo_data.py` - Seed demo data

Or use the quickstart notebook: `notebooks/00_quickstart.py`

---

## Quick Deploy Command (Copy-Paste)

```bash
python3 -m pytest tests/ -v && git push origin main && databricks sync . /Workspace/Repos/blueprint-iq && databricks apps deploy blueprint-iq
```
