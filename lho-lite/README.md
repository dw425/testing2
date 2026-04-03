# LHO Lite - Lakehouse Optimizer Lite

**Real-time security auditing, cost analytics, and compliance reporting for Databricks workspaces.**

LHO Lite connects directly to your Databricks workspace via REST APIs and system tables to collect security configurations, usage metrics, and billing data — then renders a fully interactive dark-themed dashboard with 14 tabs covering everything from executive KPIs to granular cost line items.

---

## Features at a Glance

| Feature | Description |
|---------|-------------|
| **14-Tab Dashboard** | Executive summary, security compliance, cost analytics, infrastructure, workflows, data inventory, and more |
| **Real Data from System Tables** | Queries `system.query.history`, `system.billing.usage`, `system.lakeflow.job_run_timeline`, and 3 more system tables |
| **21+ REST API Endpoints** | Collects users, groups, clusters, warehouses, jobs, Unity Catalog objects, apps, serving endpoints, secrets, tokens, and workspace config |
| **16 Security Checks** | NIST-mapped findings across credential exposure, access control, network security, encryption, and data governance |
| **4 Compliance Frameworks** | HIPAA, FedRAMP, SOC 2 Type II, and RBAC assessment with per-control scoring |
| **Security Scoring** | Letter grade (A-F) based on weighted findings with drill-down to remediation steps |
| **Cost Analytics** | 90-day billing breakdown by product, tag, job, warehouse, and SKU with daily trend charts |
| **Workflow Analysis** | Job run success/failure rates, duration, and DBU cost attribution from billing data |
| **Excel Export** | Security & FedRAMP report + Usage & Cost report as formatted `.xlsx` workbooks |
| **3 Auth Methods** | PAT token, Service Principal (OAuth2), or Databricks SDK auto-auth |
| **3 Persistence Targets** | Local SQLite, Delta tables (Unity Catalog), or Lakebase (PostgreSQL) |
| **Multi-Cloud** | AWS, Azure, GCP — including GovCloud environments |
| **Scheduled Refresh** | Manual, hourly, daily, or weekly data collection with background scheduling |
| **License Gating** | GitHub-hosted license registry with remote validation, local caching, and 48-hour grace period |

---

## Architecture

```
Browser -> Flask (port 8050)
             |-- Dashboard   (Chart.js 4 + Mermaid 11, 14 tabs)
             |-- Admin UI    (auth config, data destinations, schedule)
             |-- API         (/api/data, /api/refresh, /api/status)
             |-- Export      (/export/security, /export/usage)
             |
             v
         APScheduler -> collector -> analyzer -> persistence
                                                   |-- SQLite (local)
                                                   |-- Delta tables (Unity Catalog)
                                                   |-- Lakebase (PostgreSQL)
```

## File Structure

```
lho-lite/
├── app.yaml              # Databricks App manifest
├── databricks.yml        # Databricks Asset Bundle config
├── requirements.txt      # Python dependencies
├── pull_snapshot.py      # Offline data collection script (uses local credentials)
├── load_to_lakebase.py   # Load snapshot data into Delta tables
├── app/
│   ├── __init__.py
│   ├── main.py           # Flask app, all routes, startup logic
│   ├── config_store.py   # SQLite config + data cache persistence (Fernet encryption)
│   ├── collector.py      # Databricks API collection (21 REST + 14 SQL)
│   ├── analyzer.py       # 16 security checks, scoring, compliance, Mermaid diagrams
│   ├── dashboard.py      # Blueprint dark-themed HTML generator (14 tabs)
│   ├── admin.py          # Admin setup/config page
│   ├── data_writer.py    # Delta table + Lakebase persistence writers
│   ├── excel_export.py   # Excel report generation (openpyxl)
│   ├── scheduler.py      # APScheduler background refresh
│   ├── license.py        # License validation, caching, and gating
│   └── preloaded_snapshot.json  # Pre-collected workspace data (bundled with deploy)
└── data/                 # Created at runtime (gitignored)
    └── lho_lite.db       # SQLite database
```

---

## Dashboard Tabs

### Overview

| Tab | KPIs | Charts & Tables |
|-----|------|-----------------|
| **Executive Summary** | Total Queries, Est. Cost (90d), DBUs Consumed (90d), Security Grade | Daily query volume bar chart, top users bar chart, findings by severity doughnut, resource inventory table |
| **Workspace Overview** | Cloud, Region, Tier, Users, Clusters, Warehouses | Workspace config settings, identity & access counts, data assets inventory, network & security summary |

### Security

| Tab | KPIs | Charts & Tables |
|-----|------|-----------------|
| **Compliance** | Security Grade (score/100), Critical/High/Medium/Low finding counts | 4 framework cards (HIPAA, FedRAMP, SOC 2, RBAC) with per-control breakdown, full findings table with NIST mappings and recommendations |
| **Architecture** | — | 3 auto-generated Mermaid diagrams: Workspace Architecture, Security Posture, Data Flow (Bronze/Silver/Gold) |

### Operations

| Tab | KPIs | Charts & Tables |
|-----|------|-----------------|
| **Infrastructure** | Total Clusters, Running, SQL Warehouses, Jobs | Cluster inventory (state, Spark version, encryption, auto-terminate), warehouse inventory (size, type, serverless) |
| **Spend Overview** | Total DBUs (90d), Avg Daily Cost, Top Category, Avg DBU Price | Monthly cost by category chart, cost by tag doughnut, cost breakdown table with % of total, daily cost trend (90d) |
| **Workflows** | Total Jobs, Total Runs, Total Job Cost, Total Duration | Sortable/searchable job runs table with success/fail/canceled breakdown, compute type, DBU cost per job |
| **Cost Explorer** | Line Items, Total Est. Cost, Total DBUs, Categories | Filterable billing line items with product/SKU/warehouse/job/notebook granularity, date ranges, % of total |
| **Cost Details** | Est. Total (90d), SQL Cost, App Cost, Storage Cost | Cost by user chart, cost by category chart, monthly breakdown (SQL, Apps, Storage, Foundation Models) |

### Data

| Tab | KPIs | Charts & Tables |
|-----|------|-----------------|
| **Apps & Models** | — | Databricks Apps grid (name, state, compute, creator), foundation model pricing chart (top 12), all endpoint pricing table |
| **Table Inventory** | Total Tables, Sampled Sizes, Est. Storage | Storage distribution chart (top 20 by size), searchable/filterable table list with schema, type, created date, byte sizes |

### Activity

| Tab | KPIs | Charts & Tables |
|-----|------|-----------------|
| **User Activity** | Total Queries, Data Read, DBUs Consumed | Query distribution doughnut, success vs. failed bar chart, per-user activity table (queries, GB read, rows, compute min) |
| **Daily Trends** | Total Queries, Peak Day, Avg/Day, Success Rate | Daily query volume stacked bar (succeeded/failed), activity heatmap (LOW/MEDIUM/HIGH), data read & compute dual-axis chart |
| **DBU Pricing** | — | Price by category chart, full SKU pricing table (SKU name, USD per DBU, unit) |

---

## Data Sources

### REST APIs (Phase 1 — Security & Architecture)

Collected via Databricks REST API calls. No SQL warehouse required.

| # | Key | API Endpoint | Data Collected |
|---|-----|-------------|----------------|
| 1 | `me` | `/api/2.0/preview/scim/v2/Me` | Current authenticated user identity |
| 2 | `users` | `/api/2.0/preview/scim/v2/Users` | All workspace users, roles, email domains |
| 3 | `groups` | `/api/2.0/preview/scim/v2/Groups` | Groups, memberships, admin group detection |
| 4 | `sps` | `/api/2.0/preview/scim/v2/ServicePrincipals` | Service principals inventory |
| 5 | `clusters` | `/api/2.0/clusters/list` | Cluster configs: encryption, auto-terminate, Spark versions, node types |
| 6 | `policies` | `/api/2.0/policies/clusters/list` | Cluster policies including `data_security_mode` |
| 7 | `warehouses` | `/api/2.0/sql/warehouses` | SQL warehouses: size, type, serverless, state, max clusters |
| 8 | `jobs` | `/api/2.1/jobs/list?limit=100` | Job definitions and counts |
| 9 | `ip_lists` | `/api/2.0/ip-access-lists` | IP access list configurations |
| 10 | `secrets` | `/api/2.0/secrets/scopes/list` | Secret scope inventory |
| 11 | `tokens` | `/api/2.0/token-management/tokens` | PAT token inventory and lifetimes |
| 12 | `init_scripts` | `/api/2.0/global-init-scripts` | Global init scripts + fetches each script's base64 content for credential scanning |
| 13 | `workspace_conf` | `/api/2.0/workspace-conf?keys=...` | 11 workspace flags: result downloading, data upload, notebook export, git versioning, web terminal, clipboard, token lifetime, token config, IP lists, DBFS browser |
| 14 | `metastores` | `/api/2.1/unity-catalog/metastores` | Unity Catalog metastores and region info |
| 15 | `catalogs` | `/api/2.1/unity-catalog/catalogs` | All catalogs in the metastore |
| 16 | `storage_creds` | `/api/2.1/unity-catalog/storage-credentials` | Cloud storage credentials |
| 17 | `ext_locations` | `/api/2.1/unity-catalog/external-locations` | External location mappings |
| 18 | `shares` | `/api/2.1/unity-catalog/shares` | Delta Sharing outbound shares |
| 19 | `recipients` | `/api/2.1/unity-catalog/recipients` | Delta Sharing recipients |
| 20 | `apps` | `/api/2.0/apps` | Databricks Apps: name, state, compute, creator, deployments |
| 21 | `serving` | `/api/2.0/serving-endpoints` | Model serving endpoints: pricing, capabilities, entities |
| 22 | `dbfs` | `/api/2.0/dbfs/list` (POST) | DBFS root directory listing |
| 23 | `all_schemas` | `/api/2.1/unity-catalog/schemas` | Schemas per user catalog (one call per catalog, skips system/samples) |

### System Table SQL Queries (Phase 2 — Usage & Cost)

Require a running SQL warehouse with `USE CATALOG` on the `system` catalog.

| # | Key | System Table | What It Queries | Period |
|---|-----|-------------|-----------------|--------|
| 1 | `user_queries` | `system.query.history` | Per-user query counts, data read (GB), rows read, compute minutes, grouped by execution status | 30d |
| 2 | `daily_queries` | `system.query.history` | Daily query volume, succeeded/failed counts, data read (GB), rows, compute minutes | 30d |
| 3 | `warehouse_events` | `system.compute.warehouse_events` | Warehouse scaling events: start, stop, scale up/down with cluster counts | 30d |
| 4 | `list_prices` | `system.billing.list_prices` | Current active SKU pricing in USD per DBU | Current |
| 5 | `schema_overview` | `system.information_schema.tables` | Table counts grouped by catalog and schema | Current |
| 6 | `table_inventory` | `system.information_schema.tables` | Table names, types, created/altered dates, comments per schema (limit 500/schema) | Current |
| 7 | `table_sizes` | `DESCRIBE DETAIL` per table | Byte sizes for sampled tables (5 per schema) | Current |
| 8 | `monthly_cost_by_product` | `system.billing.usage` | Monthly DBU totals grouped by `billing_origin_product` | 90d |
| 9 | `cost_by_product` | `system.billing.usage` | Total DBUs and active days by billing product category | 90d |
| 10 | `cost_by_tag` | `system.billing.usage` | DBU consumption by first custom tag key/value and product | 90d |
| 11 | `job_runs` | `system.lakeflow.job_run_timeline` | Job run counts, success/failed/canceled, avg and total duration per job | 30d |
| 12 | `job_billing` | `system.billing.usage` | DBU cost per job by SKU from `usage_metadata.job_id` | 30d |
| 13 | `daily_cost` | `system.billing.usage` | Daily DBUs broken down into SQL, Jobs, All-Purpose, DLT, Other | 90d |
| 14 | `billing_line_items` | `system.billing.usage` | Granular billing by product/SKU/warehouse/job/notebook with first/last seen dates (limit 500) | 90d |

### System Tables Summary

| System Table | # of Queries | Purpose |
|-------------|:---:|---------|
| `system.query.history` | 2 | User activity and daily query trends |
| `system.compute.warehouse_events` | 1 | Warehouse scaling patterns |
| `system.billing.list_prices` | 1 | SKU pricing for cost estimation |
| `system.billing.usage` | 6 | Billing analytics: monthly, daily, by product, tag, job, line items |
| `system.information_schema.tables` | 2+ | Table inventory and schema overview |
| `system.lakeflow.job_run_timeline` | 1 | Job execution history |

---

## Security Analysis

### 16 Security Checks

Each finding is mapped to NIST 800-53 controls and assigned a severity level.

| # | Severity | Category | Finding | What It Checks | NIST Controls |
|---|----------|----------|---------|----------------|---------------|
| 1 | **CRITICAL** | Credential Exposure | Hardcoded secrets in init scripts | Scans init script contents for AWS keys, passwords, Azure keys, GCP service account JSON | IA-5, SC-12 |
| 2 | **CRITICAL** | Access Control | Excessive admin access | Admin users exceed 20% of total workspace users | AC-2, AC-6 |
| 3 | **CRITICAL** | FedRAMP | Non-GovCloud environment | Workspace not deployed to AWS GovCloud or Azure Government | CA-3, SC-7 |
| 4 | HIGH | Network Security | No IP access lists | Workspace accessible from any IP address | SC-7, AC-17 |
| 5 | HIGH | Token Management | Long token lifetime | `maxTokenLifetimeDays` exceeds 90 days | IA-5, AC-2(3) |
| 6 | HIGH | Encryption | Unencrypted cluster disks | Clusters with `enable_local_disk_encryption` = false | SC-28, SC-13 |
| 7 | HIGH | Data Governance | Legacy security modes | Cluster policies with `data_security_mode` = NONE | AC-3, AC-6 |
| 8 | HIGH | Session Management | No session timeout | Idle session timeout not configured or not verifiable | AC-12, SC-10 |
| 9 | MEDIUM | External Access | External email domains | Non-organizational, non-personal email domains in user list | AC-2(7), PS-7 |
| 10 | MEDIUM | Personal Accounts | Personal email accounts | Gmail, Outlook, Yahoo, iCloud, or ProtonMail addresses | IA-2, AC-2 |
| 11 | MEDIUM | Audit Logging | Audit logging gaps | Verbose audit logs not confirmed as forwarded to SIEM | AU-2, AU-3, AU-6 |
| 12 | MEDIUM | Secret Management | Insufficient secret scopes | Fewer than 2 secret scopes configured | SC-12, SC-28 |
| 13 | MEDIUM | Delta Sharing | Delta Sharing active | Active shares/recipients — requires quarterly access review | AC-3, AC-21 |
| 14 | MEDIUM | Cost & Security | Auto-termination disabled | Clusters with `autotermination_minutes` = 0 | SC-10, AC-12 |
| 15 | LOW | Identity Federation | No SSO enforcement | SAML/OIDC SSO with MFA not verifiable from API data | IA-2, IA-8 |
| 16 | LOW | Data Exfiltration | Data export not restricted | `enableExportNotebook` or `enableResultsDownloading` not false | SC-28, AC-3 |

### Scoring System

| Grade | Score Range | Meaning |
|:-----:|:----------:|---------|
| **A** | 90 - 100 | Excellent — minimal risk |
| **B** | 75 - 89 | Good — low risk with minor gaps |
| **C** | 60 - 74 | Fair — moderate risk |
| **D** | 40 - 59 | Poor — significant risk |
| **F** | 0 - 39 | Critical — immediate action required |

**Calculation:** `score = max(0, 100 - total_penalty)`

Penalty weights per finding: **CRITICAL = 25**, HIGH = 10, MEDIUM = 3, LOW = 1. Findings of the same severity stack — two CRITICAL findings deduct 50 points.

### Compliance Frameworks

| Framework | Controls Assessed | What It Evaluates |
|-----------|:-----------------:|-------------------|
| **HIPAA** | 8 | Access controls, audit logging, encryption (at rest + in transit), Unity Catalog governance, secret management, network isolation, data export controls |
| **FedRAMP** | 9 | GovCloud environment, IP access lists, token lifetime (<=90d), disk encryption, audit logging, session management (<=30min), least privilege, data exfiltration, secret management |
| **SOC 2 Type II** | 6 | Logical access (CC6.1), system operations (CC7.1), change management (CC8.1), risk mitigation (CC9.1), monitoring (CC7.2), data classification |
| **RBAC** | 6 | Unity Catalog enabled, group-based access, least privilege (admin ratio), cluster policies, data security mode enforcement, secret scope isolation |

Each framework returns **COMPLIANT**, **PARTIAL**, or **NON-COMPLIANT** with a percentage score and per-control pass/fail detail.

---

## Data Persistence

### Local SQLite (Default)

- Stored at `data/lho_lite.db` (configurable via `LHO_DATA_DIR` env var)
- Two tables: `config` (key/value with Fernet encryption for secrets) and `data_cache` (timestamped JSON snapshots)
- Retains last 10 snapshots automatically

### Delta Tables (Unity Catalog)

Writes via the SQL Statement API to managed Delta tables. Requires a running SQL warehouse and `USE CATALOG`/`CREATE TABLE` permissions on the target catalog.

### Lakebase (PostgreSQL)

Connects via `pg8000` PostgreSQL wire protocol over SSL (port 443). Auth uses PAT token as password with `"token"` as username.

### 10 Tables Written Per Snapshot

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `{prefix}snapshots` | Snapshot metadata | snapshot_id, timestamp, duration, workspace_url, cloud, score, grade, finding counts |
| `{prefix}security_findings` | Individual security findings | severity, category, NIST controls, finding text, impact, recommendation |
| `{prefix}security_scores` | Aggregated scores | score (0-100), grade (A-F), total/critical/high/medium/low counts |
| `{prefix}compliance` | Framework assessments | framework name, status, score, controls total/passed, controls JSON detail |
| `{prefix}workspace_profile` | Workspace inventory | cloud, region, all resource counts (users, clusters, warehouses, catalogs, etc.) |
| `{prefix}user_activity` | Per-user query metrics | user, status, query count, data read GB, rows, compute minutes |
| `{prefix}daily_queries` | Daily query aggregates | date, total/succeeded/failed queries, read GB, rows, compute minutes |
| `{prefix}billing_daily` | Daily cost breakdown | date, total/sql/jobs/all-purpose/dlt/other DBUs |
| `{prefix}billing_by_product` | Cost by product category | product, total DBUs, active days |
| `{prefix}job_runs` | Job execution history | job ID/name, run counts, success/fail/canceled, avg/total duration |

Default table prefix: `lho_`

---

## Authentication

| Method | When to Use | Configuration |
|--------|------------|---------------|
| **PAT Token** | Local development, quick setup | Workspace URL + `dapi...` token |
| **Service Principal** | Production, automated pipelines | Client ID + Client Secret + Tenant ID (Azure) |
| **Auto (SDK)** | Databricks App runtime | No config needed — uses Databricks SDK default auth chain |

### Cloud Support

| Cloud | URL Pattern | Auth Methods |
|-------|------------|--------------|
| **AWS** | `*.cloud.databricks.com`, `*.databricks.us` | PAT, SP (OIDC), Auto |
| **Azure** | `*.azuredatabricks.net`, `adb-*` | PAT, SP (Azure AD), Auto |
| **GCP** | `*.gcp.databricks.com` | PAT, SP (OIDC), Auto |

### Required Permissions

- **Workspace admin** or **account admin** — for SCIM user/group data
- **CAN_MANAGE** on clusters
- **CAN_USE** on at least one SQL warehouse
- **USE CATALOG** on `system` catalog — for billing, query history, job run timeline
- **Unity Catalog metastore admin** — for catalog/schema/credential enumeration

---

## License System

### How It Works

LHO Lite uses a GitHub-hosted license registry for activation and validation.

**Registry location:**
```
https://raw.githubusercontent.com/dw425/testing2/main/licenses/licenses.json
```

**Registry format:**
```json
{
  "licenses": {
    "LHO-DEMO-0001-BPTECH": {
      "valid": true,
      "expires": "2026-12-31",
      "customer": "Blueprint Technologies",
      "message": "Licensed to Blueprint Technologies"
    }
  }
}
```

### Validation Flow

```
App Startup
  │
  ├─ LHO_SKIP_LICENSE=true? ──> Skip all checks (dev/app mode)
  │
  └─ Fetch registry from GitHub
       │
       ├─ Key found & valid ──> Store locally, allow access
       │    └─ Re-check every 30 days
       │
       ├─ Key found & invalid/expired ──> Block access, show license gate
       │
       └─ Registry unreachable
            ├─ Previously valid & not expired ──> Allow (offline mode)
            ├─ Previously valid & expired ──> 48-hour grace period
            └─ Never validated ──> Block access
```

### Key Properties

| Property | Value |
|----------|-------|
| Key format | Freeform string (e.g., `LHO-DEMO-0001-BPTECH`) |
| Storage | SQLite `config` table, **Fernet-encrypted** at rest |
| Remote check interval | Every 30 days |
| Grace period | 48 hours after expiration if registry unreachable |
| Workspace binding | SHA256 fingerprint of workspace URL (first 16 chars) — extensible |
| Key rotation | Registry can specify `new_key` field to auto-rotate to a replacement key |

### Bypass Mechanisms

| Method | How | Use Case |
|--------|-----|----------|
| Environment variable | `LHO_SKIP_LICENSE=true` | Databricks App deployment |
| CLI flag | `python3 app/main.py --dev-license` | Local development |
| Code bypass | `_check_license` returns `None` | Permanent disable |

When the license gate is active and the license is invalid, all routes redirect to a lock screen with the error message and a link to `/admin` to enter a valid key.

---

## Excel Reports

Two downloadable `.xlsx` reports available from the dashboard header.

### Security & FedRAMP Report (`/export/security`)

| Sheet | Contents |
|-------|----------|
| **Executive Summary** | Cloud, user/admin/cluster/warehouse counts, finding counts by severity |
| **Security Findings** | Full findings table with severity color-coding (CRITICAL=red, HIGH=pink, MEDIUM=yellow, LOW=green), NIST controls, impact, recommendations |
| **FedRAMP Roadmap** | Prioritized remediation: Phase (Week 1 / 1-2 / 2-4 / Month 2-3), Action, NIST control, Effort, Status |

### Usage & Cost Report (`/export/usage`)

| Sheet | Contents |
|-------|----------|
| **Executive Summary** | Cloud, app/model counts, total tables, query counts, success rate, data read, compute time |
| **Apps & Models** | App inventory + foundation model endpoint pricing (input/output DBU and USD per 1M tokens) |
| **Table Inventory** | All tables with schema, type, created date, description, formatted byte sizes |
| **Query Usage by User** | Per-user query counts, data read (GB), rows, duration (min) |
| **Daily Trends** | 30-day daily query volume with succeeded/failed breakdown |
| **Warehouse Events** | Warehouse scaling event log with timestamps and cluster counts |
| **DBU Pricing** | Full SKU pricing reference table |

---

## Deployment

### Local Development

```bash
cd lho-lite
pip3 install -r requirements.txt

# With CLI args (auto-saves config, opens browser)
python3 app/main.py --host https://your-workspace.azuredatabricks.net --token dapi... --dev-license

# Or start and configure via admin UI at http://localhost:8050
python3 app/main.py --dev-license
```

### Databricks App (via Asset Bundles)

```bash
# Deploy bundle to workspace
databricks bundle deploy -p <profile>

# Deploy app (restarts with new code)
databricks apps deploy lho-lite \
  --source-code-path /Workspace/Users/<user>/.bundle/lho-lite/dev/files \
  -p <profile>
```

The app auto-detects Databricks App runtime via `DATABRICKS_APP_PORT` and uses SDK auto-auth.

**`app.yaml`:**
```yaml
command:
  - python
  - app/main.py
  - "--no-browser"
env:
  - name: LHO_SKIP_LICENSE
    value: "true"
```

### Pre-Seeded Data

For environments where the app's service principal lacks SQL warehouse access, data can be pre-collected locally and bundled:

```bash
# Pull data using your own credentials (requires databricks-sdk profile)
python3 pull_snapshot.py

# Copy to app directory so it's included in the bundle
cp data/preloaded_snapshot.json app/preloaded_snapshot.json

# Deploy — app loads the snapshot on startup
databricks bundle deploy -p <profile>
```

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main dashboard (redirects to `/admin?setup=1` if not configured) |
| `/admin` | GET | Admin setup and configuration page |
| `/admin/save` | POST | Save configuration and trigger initial data collection |
| `/admin/test` | POST | AJAX connection test — returns username and cloud |
| `/admin/validate-license` | POST | AJAX license key validation |
| `/admin/reset` | GET | Wipe all configuration and start over |
| `/api/data` | GET | Full snapshot as JSON |
| `/api/refresh` | POST | Trigger manual data refresh |
| `/api/status` | GET | Scheduler status, last collection time, next scheduled run |
| `/export/security` | GET | Download Security & FedRAMP Excel report |
| `/export/usage` | GET | Download Usage & Cost Excel report |
| `/health` | GET | Health check — `{"status": "ok", "version": "1.0.0"}` |
| `/debug/data-check` | GET | Data state summary: row counts per dataset, config, timestamps |

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABRICKS_APP_PORT` | `8050` | Port (set automatically in Databricks Apps) |
| `LHO_DATA_DIR` | `./data` | Directory for SQLite DB and Fernet encryption key |
| `LHO_SKIP_LICENSE` | — | Set to `true` to bypass license checks |
| `LHO_GITHUB_TOKEN` | — | GitHub token for private license registry access |
| `DATABRICKS_HOST` | — | Workspace URL (for SDK auto-auth fallback) |
| `DATABRICKS_TOKEN` | — | PAT token (for SDK auto-auth fallback) |

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Backend | Python 3, Flask |
| Data collection | `requests` (REST APIs), Databricks SQL Statement API |
| Auth | Databricks SDK, OAuth2 client credentials, PAT |
| Local persistence | SQLite + Fernet encryption (`cryptography`) |
| External persistence | Delta tables (SQL API), Lakebase/PostgreSQL (`pg8000`) |
| Scheduling | APScheduler (BackgroundScheduler) |
| Charts | Chart.js 4.x |
| Diagrams | Mermaid 11 |
| Excel export | openpyxl |
| UI theme | Blueprint dark theme, DM Sans font, Font Awesome 6.5.1 |
