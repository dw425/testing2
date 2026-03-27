# LHO Lite — Technical Reference Document

**Lakehouse Optimizer Lite by Blueprint Technologies**
Version 1.0 | March 2026

---

## 1. Application Overview

LHO Lite is a self-contained Databricks App that provides comprehensive workspace analysis across security posture, cost optimization, compliance, and operational monitoring. It deploys as a single Flask application, queries Databricks system tables and REST APIs, and renders a 14-tab interactive dashboard.

### Architecture Pattern

```
┌─────────────────────────────────────────────────────────┐
│                  Databricks Workspace                    │
│                                                         │
│  ┌──────────────┐     ┌──────────────────────────────┐ │
│  │ Installer     │     │ LHO Lite App (Flask)          │ │
│  │ Notebook      │────▶│                               │ │
│  │ (One-Click)   │     │ ┌──────────┐  ┌───────────┐  │ │
│  └──────────────┘     │ │ Collector │  │ Dashboard  │  │ │
│                       │ │ (API+SQL) │  │ (HTML/JS)  │  │ │
│                       │ └─────┬─────┘  └─────┬──────┘  │ │
│                       │       │              │         │ │
│                       │  ┌────▼────┐   ┌─────▼──────┐ │ │
│                       │  │ Analyzer │   │ Excel      │ │ │
│                       │  │ (Scoring)│   │ Export     │ │ │
│                       │  └─────────┘   └────────────┘ │ │
│                       │                               │ │
│                       │  ┌────────────────────────┐   │ │
│                       │  │ SQLite (config + data)  │   │ │
│                       │  │ /data/lho_lite.db       │   │ │
│                       │  └────────────────────────┘   │ │
│                       └──────────────────────────────┘ │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │              System Tables (SQL)                   │  │
│  │  system.billing.usage                             │  │
│  │  system.billing.list_prices                       │  │
│  │  system.query.history                             │  │
│  │  system.compute.warehouse_events                  │  │
│  │  system.lakeflow.job_run_timeline                 │  │
│  │  system.information_schema.tables                 │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │              REST APIs                             │  │
│  │  SCIM (Users, Groups, Service Principals)         │  │
│  │  Clusters, SQL Warehouses, Jobs                   │  │
│  │  Unity Catalog (Catalogs, Metastores, Shares)     │  │
│  │  Apps, Serving Endpoints                          │  │
│  │  Workspace Config, IP Lists, Tokens, Secrets      │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### Tech Stack

| Component | Technology |
|-----------|-----------|
| Backend | Python 3.x, Flask |
| Frontend | HTML5, Chart.js 4.x, Mermaid 11 |
| Database | SQLite (ephemeral app storage) |
| Auth | Databricks SDK auto-auth, PAT, Service Principal |
| Scheduler | APScheduler (refresh scheduling) |
| Export | openpyxl (Excel generation) |
| Licensing | GitHub-hosted registry with 30-day recheck |

---

## 2. Deployment

### One-Click Installer (Databricks Notebook)

The installer notebook (`installer.py` / `installer2.py`) performs:

1. **Step 1 — Download Code**: Fetches all app files from GitHub via API
2. **Step 2 — Write Files**: Writes to `/Workspace/Applications/{APP_NAME}/`
3. **Step 3 — Create & Deploy App**: REST API calls to create Databricks App, wait for compute, deploy
4. **Step 4 — Grant Permissions**: UC REST API grants system table access to app's Service Principal

**Pre-seeded Config:**
- `license_key_pending` → promoted to `license_key` on first startup
- `auth_method=auto` → SDK auto-authentication in Databricks App runtime

### Requirements

```
flask>=3.0,<4.0
requests>=2.31,<3.0
openpyxl>=3.1,<4.0
cryptography>=41.0,<44.0
apscheduler>=3.10,<4.0
databricks-sdk>=0.20.0,<1.0
```

### Required Permissions

| Permission | Scope | Purpose |
|-----------|-------|---------|
| USE_CATALOG | `system` | Access system catalog |
| USE_SCHEMA + SELECT | `system.billing` | Billing/cost data |
| USE_SCHEMA + SELECT | `system.query` | Query history |
| USE_SCHEMA + SELECT | `system.compute` | Warehouse events |
| USE_SCHEMA + SELECT | `system.lakeflow` | Job run data |
| CAN_USE | At least 1 SQL warehouse | Execute SQL queries |
| Workspace admin (recommended) | Workspace | Full SCIM, config, token data |

---

## 3. Data Collection — System Tables

### SQL Queries (14 queries via SQL warehouse)

| # | System Table | Time Range | Data Collected | Dashboard Tabs |
|---|-------------|-----------|----------------|----------------|
| 1 | `system.query.history` | 30 days | User query stats: user, status, count, GB read, rows, duration | User Activity, Executive Summary |
| 2 | `system.query.history` | 30 days | Daily query trends: date, total, succeeded, failed, GB, minutes | Daily Trends, Executive Summary |
| 3 | `system.compute.warehouse_events` | 30 days | Warehouse events: warehouse_id, event_type, time, cluster_count | Infrastructure |
| 4 | `system.billing.list_prices` | Current | SKU pricing: sku_name, price_usd, usage_unit | DBU Pricing, all cost calculations |
| 5 | `system.information_schema.tables` | Current | Schema overview: catalog, schema, table_count | Table Inventory |
| 6 | `system.information_schema.tables` | Current | Table details per schema: name, type, created, altered, comment | Table Inventory |
| 7 | `DESCRIBE DETAIL` | Current | Table sizes in bytes (sampled, 5 per schema) | Table Inventory |
| 8 | `system.billing.usage` | 90 days | Monthly cost by product: month, product, total_dbus | Spend Overview |
| 9 | `system.billing.usage` | 90 days | Cost by product: product, total_dbus, active_days | Spend Overview, Executive Summary |
| 10 | `system.billing.usage` | 90 days | Cost by tag: tag_key, tag_value, product, total_dbus | Spend Overview, Cost Explorer |
| 11 | `system.lakeflow.job_run_timeline` | 30 days | Job runs: job_id, name, runs, succeeded, failed, canceled, duration | Workflows |
| 12 | `system.billing.usage` | 30 days | Job billing: job_id, job_name, sku, total_dbus | Workflows, Cost Details |
| 13 | `system.billing.usage` | 90 days | Daily cost: date, total_dbus, sql, jobs, all_purpose, dlt, other | Spend Overview |
| 14 | `system.billing.usage` | 90 days | Billing line items: product, sku, warehouse, job, notebook, dbus, days, period | Cost Explorer |

### REST API Endpoints (22 endpoints)

| # | Endpoint | Data Collected | Dashboard Tabs |
|---|---------|---------------|----------------|
| 1 | `GET /api/2.0/preview/scim/v2/Me` | Current user profile | Auth verification |
| 2 | `GET /api/2.0/preview/scim/v2/Users` | All users (email, roles, admin status) | Workspace Overview, Security |
| 3 | `GET /api/2.0/preview/scim/v2/Groups` | All groups (members, roles) | Workspace Overview, Compliance |
| 4 | `GET /api/2.0/preview/scim/v2/ServicePrincipals` | Service principals (name, ID, active) | Workspace Overview |
| 5 | `GET /api/2.0/clusters/list` | Cluster configs (encryption, auto-terminate) | Infrastructure, Architecture |
| 6 | `GET /api/2.0/policies/clusters/list` | Cluster policies (security modes) | Infrastructure |
| 7 | `GET /api/2.0/sql/warehouses` | SQL warehouses (state, size, type) | Infrastructure, Spend Overview |
| 8 | `GET /api/2.1/jobs/list` | Jobs (id, name, creator) | Workflows |
| 9 | `GET /api/2.0/ip-access-lists` | IP access lists (IPs, status) | Security, Compliance |
| 10 | `GET /api/2.0/secrets/scopes/list` | Secret scopes (name, backend) | Security |
| 11 | `GET /api/2.0/token-management/tokens` | PAT tokens (creation, expiry) | Security, Compliance |
| 12 | `GET /api/2.0/global-init-scripts` | Init scripts (name, enabled) | Security |
| 13 | `GET /api/2.0/global-init-scripts/{id}` | Script content (scanned for credentials) | Security Findings |
| 14 | `GET /api/2.0/workspace-conf` | 13 workspace config flags | Security, Compliance |
| 15 | `GET /api/2.1/unity-catalog/metastores` | Metastores (region, owner) | Workspace Overview |
| 16 | `GET /api/2.1/unity-catalog/catalogs` | Catalogs (name, owner, type) | Architecture |
| 17 | `GET /api/2.1/unity-catalog/schemas` | Schemas per catalog | Table Inventory |
| 18 | `GET /api/2.1/unity-catalog/storage-credentials` | Storage credentials | Workspace Overview |
| 19 | `GET /api/2.1/unity-catalog/external-locations` | External locations | Workspace Overview |
| 20 | `GET /api/2.1/unity-catalog/shares` | Delta Shares | Workspace Overview, Security |
| 21 | `GET /api/2.1/unity-catalog/recipients` | Share recipients | Workspace Overview |
| 22 | `GET /api/2.0/apps` | Databricks Apps (state, URL, SP) | Apps & Models |
| 23 | `GET /api/2.0/serving-endpoints` | Model serving endpoints (pricing, capabilities) | Apps & Models |

---

## 4. Security Analysis Engine

### 16 Security Findings (F1–F16)

| ID | Severity | Category | NIST Control | What It Checks |
|----|---------|----------|-------------|---------------|
| F1 | CRITICAL | Credential Exposure | SA-4 | Init scripts for hardcoded AWS keys, secrets, passwords, Azure keys, GCP service accounts |
| F2 | CRITICAL | Admin Privileges | AC-6 | Admin-to-user ratio >20% |
| F3 | CRITICAL | Cloud Compliance | CA-3 | Non-GovCloud environment |
| F4 | HIGH | Network Security | SC-7 | No IP access lists configured |
| F5 | HIGH | Token Management | IA-5 | Token lifetime >90 days |
| F6 | HIGH | Encryption | SC-28 | Cluster disk encryption disabled |
| F7 | HIGH | Data Security | AC-3 | Legacy security modes (data_security_mode=NONE) |
| F8 | HIGH | Session Mgmt | AC-12 | No session timeout configured |
| F9 | MEDIUM | Identity | IA-2 | External/non-org email domains |
| F10 | MEDIUM | Identity | IA-2 | Personal email accounts (gmail, outlook, etc.) |
| F11 | MEDIUM | Audit | AU-2 | Audit log SIEM forwarding not detected |
| F12 | MEDIUM | Secrets | SC-12 | Insufficient secret scopes (<=1) |
| F13 | MEDIUM | Data Sharing | AC-22 | Delta Sharing with external recipients |
| F14 | MEDIUM | Cost/Security | SI-5 | Clusters with no auto-termination |
| F15 | LOW | Authentication | IA-2 | SSO/SAML/OIDC recommendation |
| F16 | LOW | Data Exfiltration | SC-28 | Notebook export and result download enabled |

### Security Scoring

- **CRITICAL**: -25 points per finding
- **HIGH**: -10 points per finding
- **MEDIUM**: -3 points per finding
- **LOW**: -1 point per finding
- **Score**: max(0, 100 - total_penalty)
- **Grade**: A (90-100), B (75-89), C (60-74), D (40-59), F (<40)

### Compliance Frameworks (4)

| Framework | Controls Assessed | Key Requirements |
|-----------|------------------|-----------------|
| **HIPAA** | 8 controls | Access Controls, Audit Logging, Encryption (at-rest + in-transit), Unity Catalog, Secret Mgmt, Network Isolation, Export Controls |
| **FedRAMP** | 9 controls | GovCloud, IP Lists, Token Lifetime, Encryption, Audit, Sessions, Least Privilege, Exfiltration, Secrets |
| **SOC 2** | 6 controls | Logical Access, System Operations, Change Mgmt, Risk Mitigation, Monitoring, Data Classification |
| **RBAC** | 6 controls | Unity Catalog, Group-Based Access, Admin Ratio, Cluster Policies, Data Security Mode, Secret Isolation |

**Compliance Status**: COMPLIANT (>=80%), PARTIAL (50-79%), NON-COMPLIANT (<50%)

---

## 5. Dashboard Tabs — Detailed Reference

### Tab 1: Executive Summary
**Purpose**: At-a-glance workspace health and key metrics

| Element | Type | Data Source | Metric |
|---------|------|-----------|--------|
| Total Queries | KPI | system.query.history | Count of queries (30d) |
| Est. Total Cost (90d) | KPI | system.billing.usage | billing_dbus × avg_price + storage |
| DBUs Consumed (90d) | KPI | system.billing.usage | Sum of all product DBUs |
| Security Grade | KPI | Analyzer | A-F grade from findings |
| Daily Query Volume | Stacked Bar | system.query.history | Succeeded vs Failed by date |
| Top Users | Horiz. Bar | system.query.history | Top 6 users by query count |
| Findings by Severity | Doughnut | Analyzer | Critical/High/Medium/Low distribution |
| Resource Inventory | Table | REST APIs | Cloud, Apps, Endpoints, Data, DBUs, Findings |

### Tab 2: Workspace Overview
**Purpose**: Complete workspace configuration inventory

| Element | Type | Data Source |
|---------|------|-----------|
| Cloud / Region / Tier | KPIs | Workspace config |
| Total Users / Clusters / Warehouses | KPIs | SCIM + REST APIs |
| Configuration Flags | Table | workspace-conf API (13 flags) |
| Identity & Access | Table | SCIM (users, admins, groups, SPs) |
| Data Assets | Table | UC (catalogs, metastores, credentials, locations, shares) |
| Network & Security | Table | IP lists, secret scopes |

### Tab 3: Compliance
**Purpose**: Framework-specific compliance assessment

| Element | Type | Data Source |
|---------|------|-----------|
| Security Score (A-F) | Grade Circle | Analyzer scoring engine |
| Critical/High/Medium/Low counts | KPIs | Finding severity counts |
| HIPAA / FedRAMP / SOC 2 / RBAC | Framework Cards | Compliance assessor (score + controls) |
| Security Findings | Table | 16-finding checklist with NIST mapping |

### Tab 4: Architecture
**Purpose**: Visual topology of workspace components

| Element | Type | Data Source |
|---------|------|-----------|
| Workspace Architecture | Mermaid Diagram | Cloud → Workspace → Compute/Data/Apps |
| Security Posture | Mermaid Diagram | Identity → Network → Data Security |
| Data Flow | Mermaid Diagram | Bronze → Silver → Gold medallion |

### Tab 5: Infrastructure
**Purpose**: Compute resource inventory and configuration

| Element | Type | Data Source |
|---------|------|-----------|
| Total / Running Clusters | KPIs | /api/2.0/clusters/list |
| SQL Warehouses / Jobs | KPIs | REST APIs |
| Cluster Inventory | Table | Name, state, Spark version, encryption, auto-terminate |
| SQL Warehouses | Table | Name, state, size, type, serverless, max clusters |

### Tab 6: Spend Overview
**Purpose**: Cost analysis across 90 days of billing data

| Element | Type | Data Source |
|---------|------|-----------|
| Total DBUs (90d) | KPI | system.billing.usage |
| Avg. Daily Cost | KPI | total_cost / active_days |
| Top Category | KPI | Highest DBU product |
| Avg DBU Price | KPI | system.billing.list_prices |
| Monthly Cost by Category | Stacked Bar | Monthly product DBUs × price |
| Cost by Tag | Doughnut | Tag-based cost grouping (top 10) |
| Daily Cost Trend | Multi-line | Daily DBUs by product category |
| Cost Breakdown | Table | Category, DBUs, %, Est. Cost, Active Days, Avg/Day |

### Tab 7: Workflows
**Purpose**: Job run performance and cost attribution

| Element | Type | Data Source |
|---------|------|-----------|
| Total Jobs / Runs / Cost / Duration | KPIs | system.lakeflow + system.billing |
| Job Runs | Table | Name, compute type, cost, success/fail/cancel, duration |
| Sort & Search | Controls | Sort by cost/runs/duration; search by name |

### Tab 8: Cost Explorer
**Purpose**: Drill-down into billing line items

| Element | Type | Data Source |
|---------|------|-----------|
| Line Items / Total Cost / DBUs / Categories | KPIs | system.billing.usage |
| Billing Line Items | Table | Category, SKU, resource (WH/Job/NB), cost, DBUs, %, period |
| Category Filter / Sort / Search | Controls | Filter by product, sort by cost/DBUs, search by SKU |

### Tab 9: Cost Details
**Purpose**: Cost estimation breakdown and user attribution

| Element | Type | Data Source |
|---------|------|-----------|
| Est. Total / SQL / Apps / Storage | KPIs | Billing data + storage estimate |
| Cost by User | Horiz. Bar | Top 12 users by compute cost |
| Cost by Category | Doughnut | SQL / Apps / Storage / Models |
| Cost Breakdown | Line Items | SQL, Apps, Storage, Models, Total |

### Tab 10: Apps & Models
**Purpose**: Databricks Apps and model serving inventory

| Element | Type | Data Source |
|---------|------|-----------|
| App Cards | Grid | /api/2.0/apps (name, state, compute, creator, dates) |
| Foundation Model Pricing | Horiz. Bar | Top 12 models by output price |
| All Endpoints | Table | Model, input/output DBU and USD pricing, capabilities |

### Tab 11: Table Inventory
**Purpose**: Data asset discovery and storage analysis

| Element | Type | Data Source |
|---------|------|-----------|
| Total Tables / Sampled / Est. Storage | KPIs | information_schema + DESCRIBE DETAIL |
| Storage Distribution | Horiz. Bar | Top 16 tables by size |
| Table Inventory | Searchable Table | Name, schema, type, created, description, size |

### Tab 12: User Activity
**Purpose**: Per-user query analysis

| Element | Type | Data Source |
|---------|------|-----------|
| Total Queries / Data Read / DBUs | KPIs | system.query.history |
| Query Distribution | Doughnut | Queries per user |
| Success vs Failed | Stacked Bar | Per-user success/failure |
| Detailed Activity | Table | User, status, queries, GB, rows, duration |

### Tab 13: Daily Trends
**Purpose**: Time-series analysis of query patterns

| Element | Type | Data Source |
|---------|------|-----------|
| Total Queries / Peak Day / Avg/Day / Success Rate | KPIs | system.query.history |
| Daily Query Volume | Stacked Bar | Succeeded vs Failed by date |
| Activity Heatmap | Heatmap Grid | Color-coded cells by activity level |
| Data Read & Compute | Dual-Axis Line | GB read (left) vs Compute minutes (right) |
| Daily Table | Table | Date, total, OK, fail, GB, min, level |

### Tab 14: DBU Pricing
**Purpose**: Current Databricks pricing reference

| Element | Type | Data Source |
|---------|------|-----------|
| Price by Category | Grouped Bar | Avg and Max $/DBU per compute type |
| Pricing Reference | Table | SKU, Price USD, Unit |

---

## 6. API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/` | GET | Dashboard (redirects to /admin if setup incomplete) |
| `/admin` | GET | Admin/setup page |
| `/admin/save` | POST | Save configuration |
| `/admin/test` | POST | AJAX connection test |
| `/admin/validate-license` | POST | AJAX license validation |
| `/admin/reset` | GET | Reset all configuration |
| `/api/data` | GET | Full data snapshot (JSON) |
| `/api/refresh` | POST | Trigger data refresh |
| `/api/status` | GET | Refresh status + timestamps |
| `/export/security` | GET | Excel security report download |
| `/export/usage` | GET | Excel usage report download |
| `/health` | GET | Health check (always returns ok) |

---

## 7. License System

- **Registry**: GitHub-hosted `licenses/licenses.json` in private repo
- **Validation**: Checks key existence, `valid` flag, and `expires` date
- **Recheck interval**: Every 30 days against remote registry
- **Grace period**: 48 hours if registry is unreachable (was previously valid)
- **Gate**: Blocks all routes except `/admin`, `/health`, `/static/` when unlicensed
- **Key rotation**: Registry can issue `new_key` to transparently rotate

---

## 8. File Structure

```
lho-lite/
├── app/
│   ├── __init__.py
│   ├── main.py           # Flask app, routes, startup
│   ├── collector.py       # Data collection (API + SQL)
│   ├── analyzer.py        # Security analysis, scoring, compliance
│   ├── dashboard.py       # HTML dashboard renderer (14 tabs)
│   ├── admin.py           # Admin/setup page renderer
│   ├── config_store.py    # SQLite config persistence
│   ├── excel_export.py    # Excel report generation
│   ├── license.py         # License validation system
│   └── scheduler.py       # APScheduler refresh scheduling
├── static/
│   └── blueprint-logo.png
├── data/
│   └── lho_lite.db        # SQLite database (runtime)
├── installer.py           # One-click installer notebook
├── installer2.py          # Alternate installer (lho-litev2)
└── requirements.txt
```
