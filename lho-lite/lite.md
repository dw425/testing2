# LHO Lite — Lakehouse Optimizer Lite

## Overview

LHO Lite is a self-contained Databricks App that analyzes workspace security, usage, and cost across **any Databricks environment** (AWS, Azure, GCP). It collects data via 21 REST APIs + 7 SQL queries, runs 16 NIST-mapped security checks, and presents results in a live Blueprint-branded dark-theme dashboard with scheduled refresh and Excel export.

## Architecture

```
User (browser)
    |
    v
Flask (port 8050)
    +-- GET  /             -> Dashboard (Blueprint dark theme, 8 tabs, Chart.js + Mermaid)
    +-- GET  /admin        -> Admin setup page (auth config, permissions, schedule)
    +-- POST /admin/save   -> Save config to SQLite
    +-- POST /admin/test   -> Test Databricks connection (AJAX)
    +-- GET  /api/data     -> Return cached data as JSON
    +-- POST /api/refresh  -> Trigger manual data refresh
    +-- GET  /api/status   -> Last refresh time, next scheduled, errors
    +-- GET  /export/security -> Download FedRAMP Excel
    +-- GET  /export/usage    -> Download Usage/Cost Excel
    +-- GET  /health       -> Health check
    |
    v
APScheduler (background thread)
    -> collector -> analyzer -> save snapshot to SQLite
```

## File Structure

```
LHO-Lite/
+-- lite.md                     # This file
+-- app.yaml                    # Databricks App manifest
+-- requirements.txt            # Python dependencies
+-- app/
|   +-- __init__.py
|   +-- main.py                 # Flask app, all routes, startup
|   +-- config_store.py         # SQLite config + data cache persistence
|   +-- collector.py            # Databricks API collection (21 REST + 7 SQL)
|   +-- analyzer.py             # 16 security checks + NIST + Mermaid generation
|   +-- dashboard.py            # Blueprint dark-themed HTML generator (8 tabs)
|   +-- admin.py                # Admin setup page HTML generator
|   +-- excel_export.py         # Excel report generation (BytesIO)
|   +-- scheduler.py            # APScheduler background refresh
+-- data/                       # Created at runtime
    +-- lho_lite.db             # SQLite database
    +-- .fernet.key             # Encryption key (auto-generated)
```

## Cloud Support

LHO Lite auto-detects the cloud provider from the workspace URL:

| Cloud | URL Pattern | Auth Methods | Notes |
|-------|------------|-------------|-------|
| **AWS** | `*.cloud.databricks.com`, `*.databricks.us` | PAT, SP (OIDC), Auto | GovCloud detected from `.databricks.us` |
| **Azure** | `*.azuredatabricks.net`, `adb-*` | PAT, SP (Azure AD), Auto | Requires Tenant ID for SP auth |
| **GCP** | `*.gcp.databricks.com` | PAT, SP (OIDC), Auto | Some system tables may be limited |

### Authentication Modes

1. **PAT (Personal Access Token)** — Simplest. Works on all clouds. Token entered in admin page.
2. **Service Principal (OAuth2)** — Production-grade. Uses client_credentials grant.
   - AWS/GCP: OIDC endpoint at `{workspace}/oidc/v1/token`
   - Azure: Azure AD endpoint with Databricks resource ID
3. **Auto (SDK)** — For Databricks App runtime. Uses `databricks-sdk` default auth chain.

### Required Permissions

- Workspace admin OR account admin (for SCIM user/group data)
- `CAN_MANAGE` on clusters (for cluster listing)
- `CAN_USE` on at least one SQL warehouse (for SQL queries)
- `USE CATALOG` on `system` catalog (for billing, query history, warehouse events)
- Unity Catalog metastore admin (for full catalog/schema listing)

## Data Collection

### Phase 1: Security (21 REST APIs)

| # | Endpoint | Data |
|---|----------|------|
| 1 | `/api/2.0/preview/scim/v2/Me` | Current user |
| 2 | `/api/2.0/preview/scim/v2/Users` | All users |
| 3 | `/api/2.0/preview/scim/v2/Groups` | Groups |
| 4 | `/api/2.0/preview/scim/v2/ServicePrincipals` | Service principals |
| 5 | `/api/2.0/clusters/list` | Clusters |
| 6 | `/api/2.0/policies/clusters/list` | Cluster policies |
| 7 | `/api/2.0/sql/warehouses` | SQL warehouses |
| 8 | `/api/2.1/jobs/list?limit=100` | Jobs |
| 9 | `/api/2.0/ip-access-lists` | IP access lists |
| 10 | `/api/2.0/secrets/scopes/list` | Secret scopes |
| 11 | `/api/2.0/token-management/tokens` | Tokens |
| 12 | `/api/2.0/global-init-scripts` | Init scripts |
| 13 | `/api/2.0/workspace-conf?keys=...` | Workspace config flags |
| 14 | `/api/2.1/unity-catalog/metastores` | Metastores |
| 15 | `/api/2.1/unity-catalog/catalogs` | Catalogs |
| 16 | `/api/2.1/unity-catalog/storage-credentials` | Storage credentials |
| 17 | `/api/2.1/unity-catalog/external-locations` | External locations |
| 18 | `/api/2.1/unity-catalog/shares` | Delta Sharing shares |
| 19 | `/api/2.1/unity-catalog/recipients` | Delta Sharing recipients |
| 20 | `/api/2.0/apps` | Databricks Apps |
| 21 | `/api/2.0/serving-endpoints` | Model serving endpoints |

### Phase 2: Usage (7 SQL Queries)

| # | Query | Source |
|---|-------|--------|
| 1 | User activity (30d) | `system.query.history` |
| 2 | Daily query trends (30d) | `system.query.history` |
| 3 | Warehouse events (30d) | `system.compute.warehouse_events` |
| 4 | DBU list prices | `system.billing.list_prices` |
| 5 | Schema overview | `system.information_schema.tables` |
| 6 | Per-schema table details | `system.information_schema.tables` |
| 7 | Table sizes (sampled) | `DESCRIBE DETAIL` |

## 16 Security Checks

| ID | Category | NIST Controls | Severity |
|----|----------|---------------|----------|
| F1 | Credential Exposure | IA-5, SC-12 | CRITICAL |
| F2 | Access Control (admin ratio) | AC-2, AC-6 | CRITICAL |
| F3 | FedRAMP Environment | CA-3, SC-7 | CRITICAL |
| F4 | Network Security (IP lists) | SC-7, AC-17 | HIGH |
| F5 | Token Management | IA-5, AC-2(3) | HIGH |
| F6 | Encryption (disk) | SC-28, SC-13 | HIGH |
| F7 | Data Governance (security mode) | AC-3, AC-6 | HIGH |
| F8 | Session Management | AC-12, SC-10 | HIGH |
| F9 | External Access (domains) | AC-2(7), PS-7 | MEDIUM |
| F10 | Personal Accounts | IA-2, AC-2 | MEDIUM |
| F11 | Audit Logging | AU-2, AU-3, AU-6 | MEDIUM |
| F12 | Secret Management | SC-12, SC-28 | MEDIUM |
| F13 | Delta Sharing | AC-3, AC-21 | MEDIUM |
| F14 | Auto-termination | SC-10, AC-12 | MEDIUM |
| F15 | Identity Federation (SSO) | IA-2, IA-8 | LOW |
| F16 | Data Exfiltration | SC-28, AC-3 | LOW |

### Security Score

Weighted scoring: CRITICAL=25, HIGH=10, MEDIUM=3, LOW=1 penalty points.
Score = max(0, 100 - total_penalty). Grade: A (90+), B (75+), C (60+), D (40+), F (<40).

## Dashboard Tabs

1. **Executive Summary** — KPIs (queries, users, tables, security grade), daily volume chart, top users, findings donut
2. **Security & Compliance** — Security grade, findings table, 3 Mermaid architecture diagrams
3. **Apps & Models** — App cards, model pricing chart, endpoint table
4. **Table Inventory** — Storage distribution, searchable table list
5. **User Activity** — Query distribution donut, success/fail bar chart, detailed table
6. **Daily Trends** — Stacked bar, heatmap, dual-axis line chart, daily table
7. **Warehouse & Compute** — Cost estimation, warehouse events
8. **DBU Pricing** — Price by category chart, full pricing table

## Setup & Deployment

### Local Development

```bash
cd ~/Desktop/LHO-Lite
pip3 install -r requirements.txt

# With CLI args (auto-saves config)
python3 -m app.main --host https://your-workspace.cloud.databricks.com --token dapi1234...

# Or start and configure via admin UI
python3 -m app.main
# Opens browser -> redirected to /admin?setup=1
```

### Databricks App Deployment

1. Upload the `LHO-Lite/` directory to a Databricks workspace (via Repos or Files)
2. Create a new Databricks App pointing to the directory
3. The `app.yaml` manifest tells the runtime to execute `python3 app/main.py`
4. The app auto-detects `DATABRICKS_APP_PORT` and uses SDK auto-auth
5. On first visit, the admin setup page appears for configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABRICKS_APP_PORT` | `8050` | Port (set automatically in Databricks Apps) |
| `LHO_DATA_DIR` | `./data` | Directory for SQLite DB and encryption key |
| `DATABRICKS_HOST` | — | Workspace URL (for auto auth fallback) |
| `DATABRICKS_TOKEN` | — | PAT token (for auto auth fallback) |

## Data Persistence

- **Config**: Stored in SQLite `config` table. Sensitive values (tokens, secrets) encrypted with Fernet at rest.
- **Snapshots**: Collected data stored as JSON in SQLite `data_cache` table. Last 10 snapshots retained.
- **Encryption key**: Auto-generated on first run, stored at `data/.fernet.key` (chmod 600).

## Excel Export

Two downloadable reports available from the dashboard:

1. **Security Report** (`/export/security`) — Executive Summary, Security Findings, FedRAMP Roadmap
2. **Usage Report** (`/export/usage`) — Executive Summary, Apps & Models, Table Inventory, Query by User, Daily Trends, Warehouse Events, DBU Pricing

## Blueprint Design System

Dark theme tokens used throughout:

| Token | Value | Usage |
|-------|-------|-------|
| bg_base | `#0D1117` | Page canvas |
| surface | `#161B22` | Cards |
| surface_elevated | `#21262D` | Hover/modals |
| sidebar_bg | `#1A1F2E` | Sidebar |
| border | `#272D3F` | Borders |
| text_primary | `#E6EDF3` | Main text |
| text_secondary | `#8B949E` | Secondary |
| accent | `#4B7BF5` | Brand blue |
| green | `#34D399` | Success |
| yellow | `#FBBF24` | Warning |
| red | `#F87171` | Critical |
| font | DM Sans, Inter | Typography |

CDN dependencies: Chart.js 4.x, Mermaid 11, DM Sans (Google Fonts), Font Awesome 6.x.
