# LHO Lite — Lakehouse Optimizer Lite

A self-contained Databricks App that analyzes workspace **security**, **usage**, and **cost** across any Databricks environment (AWS, Azure, GCP).

## What It Does

- Collects data via **21 REST APIs + 7 SQL queries**
- Runs **16 NIST-mapped security checks** with weighted scoring
- Presents results in a live **Blueprint-branded dark-theme dashboard** (8 tabs)
- Supports **scheduled refresh** (APScheduler) and **Excel export**

## Architecture

```
Browser -> Flask (port 8050)
             |-- Dashboard   (Chart.js + Mermaid, 8 tabs)
             |-- Admin UI    (auth config, permissions, schedule)
             |-- API         (/api/data, /api/refresh, /api/status)
             |-- Export      (/export/security, /export/usage)
             |
             v
         APScheduler -> collector -> analyzer -> SQLite snapshot
```

## File Structure

```
lho-lite/
├── app.yaml              # Databricks App manifest
├── requirements.txt      # Python dependencies
├── lite.md               # Detailed technical spec
├── app/
│   ├── __init__.py
│   ├── main.py           # Flask app, all routes, startup
│   ├── config_store.py   # SQLite config + data cache persistence
│   ├── collector.py      # Databricks API collection (21 REST + 7 SQL)
│   ├── analyzer.py       # 16 security checks + NIST + Mermaid generation
│   ├── dashboard.py      # Blueprint dark-themed HTML generator (8 tabs)
│   ├── admin.py          # Admin setup page HTML generator
│   ├── excel_export.py   # Excel report generation (BytesIO)
│   └── scheduler.py      # APScheduler background refresh
└── data/                 # Created at runtime (gitignored)
```

## Quick Start

### Local Development

```bash
cd lho-lite
pip3 install -r requirements.txt

# With CLI args (auto-saves config)
python3 -m app.main --host https://your-workspace.cloud.databricks.com --token dapi1234...

# Or start and configure via admin UI
python3 -m app.main
# Opens browser -> redirected to /admin?setup=1
```

### Databricks App Deployment

1. Upload `lho-lite/` to a Databricks workspace (via Repos or Files)
2. Create a new Databricks App pointing to the directory
3. `app.yaml` tells the runtime to execute `python3 app/main.py`
4. Auto-detects `DATABRICKS_APP_PORT` and uses SDK auto-auth
5. First visit shows admin setup page for configuration

## Cloud Support

| Cloud     | URL Pattern                        | Auth Methods          |
|-----------|------------------------------------|-----------------------|
| **AWS**   | `*.cloud.databricks.com`, `*.databricks.us` | PAT, SP (OIDC), Auto |
| **Azure** | `*.azuredatabricks.net`, `adb-*`   | PAT, SP (Azure AD), Auto |
| **GCP**   | `*.gcp.databricks.com`             | PAT, SP (OIDC), Auto |

## Dashboard Tabs

1. **Executive Summary** — KPIs, daily volume chart, top users, findings donut
2. **Security & Compliance** — Security grade, findings table, Mermaid architecture diagrams
3. **Apps & Models** — App cards, model pricing chart, endpoint table
4. **Table Inventory** — Storage distribution, searchable table list
5. **User Activity** — Query distribution donut, success/fail bar chart
6. **Daily Trends** — Stacked bar, heatmap, dual-axis line chart
7. **Warehouse & Compute** — Cost estimation, warehouse events
8. **DBU Pricing** — Price by category chart, full pricing table

## Security Checks (16)

Weighted scoring: CRITICAL=25, HIGH=10, MEDIUM=3, LOW=1 penalty points.
Score = max(0, 100 - total_penalty). Grade: A (90+), B (75+), C (60+), D (40+), F (<40).

See [lite.md](lite.md) for the full technical specification.

## Environment Variables

| Variable              | Default  | Description                                    |
|-----------------------|----------|------------------------------------------------|
| `DATABRICKS_APP_PORT` | `8050`   | Port (set automatically in Databricks Apps)    |
| `LHO_DATA_DIR`        | `./data` | Directory for SQLite DB and encryption key     |
| `DATABRICKS_HOST`     | —        | Workspace URL (for auto auth fallback)         |
| `DATABRICKS_TOKEN`    | —        | PAT token (for auto auth fallback)             |
