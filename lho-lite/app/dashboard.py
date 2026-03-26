"""
Blueprint dark-themed HTML dashboard generator for LHO Lite.

14-tab layout with sidebar groups:
  OVERVIEW:  1. Executive Summary  2. Workspace Overview
  SECURITY:  3. Compliance  4. Architecture
  OPERATIONS:  5. Infrastructure  6. Spend Overview  7. Workflows  8. Cost Explorer  9. Cost Details
  DATA:  10. Apps & Models  11. Table Inventory
  ACTIVITY:  12. User Activity  13. Daily Trends  14. DBU Pricing

CDN: Chart.js 4.x, Mermaid 11, DM Sans, Font Awesome 6.x
"""

import json
import re

ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')
ILLEGAL_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]')


def _clean(v):
    if isinstance(v, str):
        v = ANSI_RE.sub('', v)
        v = ILLEGAL_RE.sub('', v)
    return v


def _fmt_bytes(b):
    if not b:
        return "\u2014"
    b = int(b)
    if b < 1024:
        return f"{b} B"
    if b < 1048576:
        return f"{b / 1024:.1f} KB"
    if b < 1073741824:
        return f"{b / 1048576:.2f} MB"
    return f"{b / 1073741824:.2f} GB"


# Blueprint Logo SVG
LOGO_SVG = '<img src="/static/blueprint-logo.png" alt="Blueprint" style="height:26px">'


def render_dashboard(snapshot: dict, findings=None, security_score=None,
                     mermaid_diagrams=None, compliance=None,
                     workspace_profile=None, last_refresh: str = "") -> str:
    """Build the full self-contained HTML dashboard."""

    sec_data = snapshot.get("security", {})
    usage_data = snapshot.get("usage", {})
    findings = findings or []
    security_score = security_score or {}
    mermaid_diagrams = mermaid_diagrams or {}
    compliance = compliance or {}
    workspace_profile = workspace_profile or {}

    cloud = sec_data.get("_cloud", "Unknown")
    workspace_url = sec_data.get("_workspace_url", "")

    # ---- Build JSON data for client-side rendering ----
    apps = sec_data.get("apps", {}).get("apps", [])
    serving = sec_data.get("serving", {}).get("endpoints", [])
    daily_rows = usage_data.get("daily_queries", {}).get("rows", [])
    user_rows = usage_data.get("user_queries", {}).get("rows", [])
    wh_rows = usage_data.get("warehouse_events", {}).get("rows", [])
    price_rows = usage_data.get("list_prices", {}).get("rows", [])

    # SP display name mapping
    sp_map = {}
    for app in apps:
        sp_id = app.get("service_principal_client_id", "")
        if sp_id:
            sp_map[sp_id] = f"{app['name']} (SP)"

    app_list = [{
        "name": a.get("name", ""),
        "desc": _clean(a.get("description", "")),
        "state": a.get("compute_status", {}).get("state", "") if isinstance(a.get("compute_status"), dict) else "",
        "compute": a.get("compute_size", ""),
        "url": a.get("url", ""),
        "creator": a.get("creator", ""),
        "created": (a.get("create_time", "") or "")[:10],
        "lastDeploy": (a.get("active_deployment", {}).get("create_time", "") or "")[:10] if a.get("active_deployment") else "",
    } for a in apps]

    model_list = []
    for ep in serving:
        entities = ep.get("config", {}).get("served_entities", [{}])
        fm = entities[0].get("foundation_model", {}) if entities else {}
        caps = [k for k, v in ep.get("capabilities", {}).items() if v] if isinstance(ep.get("capabilities"), dict) else []
        inp = float(fm.get("input_price", "0") or "0")
        out = float(fm.get("price", "0") or "0")
        model_list.append({
            "name": ep.get("name", ""),
            "display": fm.get("display_name", ep.get("name", "")),
            "inputDbu": inp,
            "outputDbu": out,
            "inputUsd": round(inp * 0.07, 4),
            "outputUsd": round(out * 0.07, 4),
            "pricing": fm.get("pricing_model", ""),
            "capabilities": caps,
        })

    def _int(v):
        try: return int(v)
        except (TypeError, ValueError): return 0

    def _float(v):
        try: return float(v)
        except (TypeError, ValueError): return 0.0

    user_list = [{
        "user": sp_map.get(r[0], r[0]),
        "status": r[1],
        "queries": _int(r[2]),
        "dataGb": _float(r[3]),
        "rowsRead": _int(r[4]),
        "durationMin": _float(r[5]),
    } for r in user_rows]

    daily_list = []
    for r in daily_rows:
        total = _int(r[1])
        daily_list.append({
            "date": r[0],
            "total": total,
            "succeeded": _int(r[2]),
            "failed": _int(r[3]),
            "dataGb": _float(r[4]),
            "rowsRead": _int(r[5]),
            "computeMin": _float(r[6]),
            "level": "HIGH" if total > 500 else ("MEDIUM" if total > 50 else "LOW"),
        })

    wh_ids = {}
    wh_list = []
    for r in wh_rows:
        wh_id = r[0] or ""
        if wh_id not in wh_ids:
            wh_ids[wh_id] = f"WH-{len(wh_ids) + 1} ({wh_id[:8]})"
        wh_list.append({"wh": wh_ids[wh_id], "whId": wh_id, "event": r[1], "time": r[2] or "", "clusters": _int(r[3])})

    table_list = []
    sized_tables = []
    for key, inv in usage_data.get("table_inventory", {}).items():
        schema = key.split(".")[-1]
        for row in inv.get("rows", []):
            fqn = f"{key}.{row[0]}"
            size = usage_data.get("table_sizes", {}).get(fqn)
            table_list.append({
                "name": row[0],
                "schema": schema,
                "type": row[1],
                "created": (row[2] or "")[:10],
                "altered": (row[3] or "")[:10] if len(row) > 3 else "",
                "desc": _clean(row[4] or "") if len(row) > 4 else "",
                "size": size,
            })
            if size:
                sized_tables.append({"name": row[0], "size": size})
    sized_tables.sort(key=lambda x: x["size"], reverse=True)

    relevant_skus = [
        "ENHANCED_SECURITY", "ENTERPRISE_ALL_PURPOSE_COMPUTE", "ENTERPRISE_SQL",
        "ENTERPRISE_JOBS", "ENTERPRISE_DLT", "ENTERPRISE_MODEL", "ENTERPRISE_ANTHROPIC",
        "ENTERPRISE_SERVERLESS", "ENTERPRISE_APPS", "STANDARD", "PREMIUM",
    ]
    pricing_list = [{"sku": r[0], "price": float(r[1]), "unit": r[2]}
                    for r in price_rows if any(r[0].upper().startswith(p) for p in relevant_skus)]

    # Findings for JS
    findings_js = [{"sev": f[0], "cat": f[1], "nist": f[2], "finding": f[3], "impact": f[4], "rec": f[5]}
                   for f in findings]

    # Infrastructure data
    cluster_raw = sec_data.get("clusters", {}).get("clusters", [])
    warehouse_raw = sec_data.get("warehouses", {}).get("warehouses", [])
    jobs_raw = sec_data.get("jobs", {}).get("jobs", [])

    cluster_js = [{
        "name": c.get("cluster_name", ""),
        "state": c.get("state", ""),
        "sparkVersion": c.get("spark_version", ""),
        "nodeType": c.get("node_type_id", c.get("driver_node_type_id", "")),
        "autoTerminate": c.get("autotermination_minutes", 0),
        "encryption": c.get("enable_local_disk_encryption", False),
    } for c in cluster_raw]

    warehouse_js = [{
        "name": w.get("name", ""),
        "state": w.get("state", ""),
        "size": w.get("cluster_size", ""),
        "type": w.get("warehouse_type", ""),
        "serverless": w.get("enable_serverless_compute", False),
        "maxClusters": w.get("max_num_clusters", 1),
    } for w in warehouse_raw]

    # ---- Billing / cost data for new tabs ----
    cost_by_product_rows = usage_data.get("cost_by_product", {}).get("rows", [])
    monthly_cost_rows = usage_data.get("monthly_cost_by_product", {}).get("rows", [])
    daily_cost_rows = usage_data.get("daily_cost", {}).get("rows", [])
    cost_by_tag_rows = usage_data.get("cost_by_tag", {}).get("rows", [])
    job_runs_rows = usage_data.get("job_runs", {}).get("rows", [])
    job_billing_rows = usage_data.get("job_billing", {}).get("rows", [])

    cost_by_product_js = [{"product": r[0] or "Other", "dbus": _float(r[1]), "days": _int(r[2])}
                          for r in cost_by_product_rows]

    monthly_cost_js = [{"month": (r[0] or "")[:10], "product": r[1] or "Other", "dbus": _float(r[2])}
                       for r in monthly_cost_rows]

    daily_cost_js = [{"date": str(r[0] or ""), "total": _float(r[1]), "sql": _float(r[2]),
                      "jobs": _float(r[3]), "allPurpose": _float(r[4]),
                      "dlt": _float(r[5]), "other": _float(r[6])}
                     for r in daily_cost_rows]

    cost_by_tag_js = [{"tagKey": r[0] or "Untagged", "tagValue": r[1] or "Untagged",
                       "product": r[2] or "Other", "dbus": _float(r[3])}
                      for r in cost_by_tag_rows]

    # Billing line items for Cost Explorer
    billing_line_rows = usage_data.get("billing_line_items", {}).get("rows", [])
    billing_lines_js = [{"product": r[0] or "Other", "sku": r[1] or "", "warehouseId": r[2] or "",
                         "jobId": str(r[3] or ""), "notebookId": str(r[4] or ""),
                         "dbus": _float(r[5]), "activeDays": _int(r[6]),
                         "firstSeen": str(r[7] or "")[:10], "lastSeen": str(r[8] or "")[:10]}
                        for r in billing_line_rows]

    # Build price lookup: sku_name -> usd_per_dbu
    price_map = {}
    for r in price_rows:
        sku = r[0]
        price = _float(r[1])
        if price > 0:
            price_map[sku] = price
    avg_dbu_price = round(sum(price_map.values()) / max(len(price_map), 1), 4) if price_map else 0.07

    # Merge job run data with billing data
    job_cost_map = {}  # job_id -> {dbus, sku, name}
    for r in job_billing_rows:
        jid = str(r[0] or "")
        if jid not in job_cost_map:
            job_cost_map[jid] = {"dbus": 0, "sku": r[2] or "", "name": r[1] or ""}
        job_cost_map[jid]["dbus"] += _float(r[3])

    workflow_js = []
    for r in job_runs_rows:
        jid = str(r[0] or "")
        billing = job_cost_map.get(jid, {})
        total_dbus = billing.get("dbus", 0)
        sku = billing.get("sku", "")
        compute_type = "Job Compute"
        if "ALL_PURPOSE" in sku.upper():
            compute_type = "All-Purpose"
        elif "SERVERLESS" in sku.upper():
            compute_type = "Serverless"
        elif "DLT" in sku.upper():
            compute_type = "DLT"
        workflow_js.append({
            "jobId": jid,
            "name": r[1] or jid,
            "computeType": compute_type,
            "totalRuns": _int(r[2]),
            "succeeded": _int(r[3]),
            "failed": _int(r[4]),
            "canceled": _int(r[5]),
            "avgDurationMin": _float(r[6]),
            "totalDurationMin": _float(r[7]),
            "totalDbus": round(total_dbus, 4),
            "estCost": round(total_dbus * avg_dbu_price, 2),
        })
    workflow_js.sort(key=lambda w: w["estCost"], reverse=True)

    # Totals from billing
    billing_total_dbus = sum(c["dbus"] for c in cost_by_product_js)
    billing_total_cost = round(billing_total_dbus * avg_dbu_price, 2)

    DATA = {
        "meta": {"workspace": workspace_url, "cloud": cloud, "period": "Last 30 Days"},
        "apps": app_list,
        "models": model_list,
        "users": user_list,
        "daily": daily_list,
        "whEvents": wh_list,
        "tables": table_list,
        "sizedTables": sized_tables[:20],
        "pricing": pricing_list,
        "findings": findings_js,
        "score": security_score,
        "compliance": compliance,
        "workspaceProfile": workspace_profile,
        "clusters": cluster_js,
        "warehouses": warehouse_js,
        "jobs": len(jobs_raw),
        "costByProduct": cost_by_product_js,
        "monthlyCost": monthly_cost_js,
        "dailyCost": daily_cost_js,
        "costByTag": cost_by_tag_js,
        "workflows": workflow_js,
        "billingLines": billing_lines_js,
        "avgDbuPrice": avg_dbu_price,
    }

    data_json = json.dumps(DATA, separators=(",", ":"), default=str)

    # Pre-compute KPIs
    total_queries = sum(d["total"] for d in daily_list) if daily_list else 0
    total_succeeded = sum(d["succeeded"] for d in daily_list) if daily_list else 0
    total_failed = sum(d["failed"] for d in daily_list) if daily_list else 0
    total_gb = round(sum(d["dataGb"] for d in daily_list), 2) if daily_list else 0
    total_min = round(sum(d["computeMin"] for d in daily_list), 1) if daily_list else 0
    success_pct = round(total_succeeded / max(total_queries, 1) * 100, 1)
    peak_day = max(daily_list, key=lambda x: x["total"]) if daily_list else {"total": 0, "date": "N/A"}
    total_tables = len(table_list)
    total_sized = sum(t["size"] for t in sized_tables)
    est_mb = round(total_sized / max(len(sized_tables), 1) * total_tables / 1048576, 1) if sized_tables else 0
    active_users = len(set(u["user"] for u in user_list))
    grade = security_score.get("grade", "?")
    score_val = security_score.get("score", 0)
    grade_color = {"A": "#34D399", "B": "#34D399", "C": "#FBBF24", "D": "#F87171", "F": "#F87171"}.get(grade, "#8B949E")

    # Infrastructure KPIs
    running_clusters = sum(1 for c in cluster_raw if c.get("state") == "RUNNING")

    # Cost estimates — compute minutes -> DBUs (1 DBU ≈ 1 compute-minute for serverless SQL)
    total_dbu = round(total_min, 1)
    sql_cost = round(total_dbu * 0.07, 2)
    app_cost_est = len(app_list) * 15  # rough per-app estimate
    storage_cost = round(est_mb * 0.023 / 1024, 2) if est_mb else 5
    total_cost_est = round(sql_cost + app_cost_est + storage_cost, 2)

    # Mermaid
    arch_mermaid = mermaid_diagrams.get("architecture", "").replace("\n", "\\n")
    sec_mermaid = mermaid_diagrams.get("security", "").replace("\n", "\\n")
    flow_mermaid = mermaid_diagrams.get("dataflow", "").replace("\n", "\\n")

    # Workspace profile KPIs
    wp = workspace_profile
    wp_cloud = wp.get("cloud", cloud)
    wp_region = wp.get("region", "unknown")
    wp_tier = wp.get("tier", "Premium")
    wp_users = wp.get("total_users", 0)
    wp_clusters = wp.get("cluster_count", len(cluster_raw))
    wp_warehouses = wp.get("warehouse_count", len(warehouse_raw))

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LHO Lite \u2014 Lakehouse Optimizer</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<style>
:root{{
  --bg:#0D1117;--surface:#161B22;--elevated:#21262D;--sidebar:#1A1F2E;
  --border:#272D3F;--text:#E6EDF3;--text2:#8B949E;--text3:#484F58;
  --accent:#4B7BF5;--green:#34D399;--yellow:#FBBF24;--red:#F87171;--purple:#A78BFA;--cyan:#00E5FF;
  --font:'DM Sans','Inter',system-ui,sans-serif;
}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--text);font-family:var(--font);font-size:14px;line-height:1.5;-webkit-font-smoothing:antialiased}}
::-webkit-scrollbar{{width:6px;height:6px}}
::-webkit-scrollbar-track{{background:var(--bg)}}
::-webkit-scrollbar-thumb{{background:var(--border);border-radius:3px}}

/* Layout */
.layout{{display:flex;height:100vh;overflow:hidden}}
.sidebar{{width:220px;min-width:220px;background:var(--sidebar);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow-y:auto}}
.main-area{{flex:1;display:flex;flex-direction:column;overflow:hidden}}
.header{{height:52px;background:var(--surface);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 24px;flex-shrink:0}}
.content{{flex:1;overflow-y:auto;padding:24px;scroll-behavior:smooth}}

/* Sidebar */
.sb-brand{{padding:16px 20px;border-bottom:1px solid var(--border)}}
.sb-brand .logo{{margin-bottom:4px}}
.sb-brand small{{font-size:12px;color:var(--text2)}}
.sb-nav{{padding:8px 0;flex:1}}
.sidebar-group{{font-size:9px;text-transform:uppercase;letter-spacing:1.2px;color:var(--text3);padding:16px 20px 4px;font-weight:700}}
.nav-item{{display:flex;align-items:center;gap:10px;padding:10px 20px;cursor:pointer;font-size:13px;font-weight:500;color:var(--text2);transition:all .15s;position:relative;border:none;background:none;width:100%;text-align:left;font-family:var(--font)}}
.nav-item:hover{{background:rgba(75,123,245,.08);color:var(--text)}}
.nav-item.active{{background:rgba(75,123,245,.15);color:var(--accent)}}
.nav-item.active::before{{content:'';position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--accent);border-radius:0 2px 2px 0}}
.nav-item i{{width:18px;text-align:center;font-size:14px}}
.sb-footer{{padding:16px 20px;border-top:1px solid var(--border)}}
.sb-footer .powered{{font-size:16px;font-weight:700;color:#fff;font-family:var(--font)}}
.sb-footer .powered-sub{{font-size:10px;color:var(--text3);margin-top:2px}}

/* Header */
.h-title{{font-size:14px;font-weight:600}}
.h-meta{{display:flex;align-items:center;gap:16px;font-size:12px;color:var(--text2)}}
.h-meta .cloud-badge{{background:rgba(75,123,245,.12);color:var(--accent);padding:3px 10px;border-radius:12px;font-weight:600;font-size:11px}}
.h-meta select{{background:var(--elevated);color:var(--text);border:1px solid var(--border);padding:5px 10px;border-radius:6px;font-size:12px;font-family:var(--font);cursor:pointer;outline:none}}
.h-meta select:focus{{border-color:var(--accent)}}
.btn-refresh{{background:var(--accent);color:#fff;border:none;padding:7px 16px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;font-family:var(--font);transition:background .15s}}
.btn-refresh:hover{{background:#3D6BE0}}
.btn-refresh:disabled{{opacity:.5;cursor:not-allowed}}

/* Tabs */
.tab{{display:none}}.tab.active{{display:block}}

/* Cards */
.card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;margin-bottom:20px;transition:transform .2s,box-shadow .2s}}
.card:hover{{transform:translateY(-1px);box-shadow:0 4px 16px rgba(0,0,0,.3)}}
.card-hdr{{padding:14px 20px;font-size:12px;font-weight:600;color:var(--text2);text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--border)}}
.card-body{{padding:20px}}

/* KPI */
.kpi-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}}
.kpi{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px;position:relative;overflow:hidden;transition:border-color .15s,transform .15s}}
.kpi[data-goto]{{cursor:pointer}}.kpi[data-goto]:hover{{border-color:var(--accent);transform:translateY(-2px)}}
.clickable-row{{cursor:pointer;transition:background .15s}}.clickable-row:hover{{background:rgba(75,123,245,.08)}}
.kpi::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px}}
.kpi.blue::before{{background:var(--accent)}}.kpi.green::before{{background:var(--green)}}.kpi.purple::before{{background:var(--purple)}}.kpi.red::before{{background:var(--red)}}.kpi.yellow::before{{background:var(--yellow)}}.kpi.cyan::before{{background:var(--cyan)}}
.kpi-label{{font-size:11px;color:var(--text2);font-weight:600;text-transform:uppercase;letter-spacing:.5px}}
.kpi-value{{font-size:28px;font-weight:700;margin:6px 0 2px}}
.kpi-sub{{font-size:11px;color:var(--text3)}}
.kpi-icon{{position:absolute;top:16px;right:18px;font-size:24px;opacity:.15}}

/* Grid */
.grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px}}
.grid-3{{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:24px}}
.chart-box{{position:relative;width:100%}}.h300{{height:300px}}.h250{{height:250px}}.h400{{height:400px}}

/* Tables */
.dtable{{width:100%;border-collapse:collapse;font-size:13px}}
.dtable th{{background:var(--elevated);padding:10px 14px;text-align:left;font-weight:600;color:var(--text2);font-size:11px;text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--border)}}
.dtable td{{padding:9px 14px;border-bottom:1px solid var(--border);color:var(--text)}}
.dtable tbody tr:hover{{background:var(--elevated)}}
.dtable .num{{text-align:right;font-variant-numeric:tabular-nums}}

/* Badges */
.badge{{display:inline-block;padding:2px 10px;border-radius:20px;font-size:11px;font-weight:600}}
.badge-crit{{background:rgba(248,113,113,.15);color:var(--red)}}.badge-high{{background:rgba(251,191,36,.15);color:var(--yellow)}}
.badge-med{{background:rgba(75,123,245,.15);color:var(--accent)}}.badge-low{{background:rgba(52,211,153,.15);color:var(--green)}}
.badge-green{{background:rgba(52,211,153,.12);color:var(--green)}}.badge-red{{background:rgba(248,113,113,.12);color:var(--red)}}
.badge-amber{{background:rgba(251,191,36,.12);color:var(--yellow)}}.badge-blue{{background:rgba(75,123,245,.12);color:var(--accent)}}
.badge-purple{{background:rgba(167,139,250,.12);color:var(--purple)}}.badge-gray{{background:rgba(255,255,255,.06);color:var(--text2)}}

/* Compliance */
.framework-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}}
.compliance-card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:24px;text-align:center}}
.compliance-card h4{{font-size:14px;font-weight:700;margin-bottom:12px;text-transform:uppercase;letter-spacing:.5px}}
.compliance-card .score-circle{{width:72px;height:72px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:24px;font-weight:700;margin:0 auto 12px;border:3px solid}}
.compliance-card .status-badge{{display:inline-block;padding:4px 12px;border-radius:20px;font-size:11px;font-weight:700}}
.ctrl-pass{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:600;background:rgba(52,211,153,.15);color:var(--green)}}
.ctrl-fail{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:600;background:rgba(248,113,113,.15);color:var(--red)}}

/* Misc */
.section-title{{font-size:18px;font-weight:700;margin-bottom:20px}}
.search-bar{{padding:10px 14px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:13px;width:300px;outline:none;font-family:var(--font)}}
.search-bar:focus{{border-color:var(--accent)}}
.search-bar::placeholder{{color:var(--text3)}}
.app-card{{border:1px solid var(--border);border-radius:12px;padding:20px;background:var(--surface)}}
.app-card h3{{font-size:15px;margin-bottom:4px;color:var(--text)}}.app-card p{{font-size:12px;color:var(--text2);margin-bottom:12px}}
.app-meta{{display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:12px}}.app-meta dt{{color:var(--text3);font-weight:600;text-transform:uppercase;letter-spacing:.3px;font-size:10px}}.app-meta dd{{color:var(--text);margin-bottom:6px}}.app-meta a{{color:var(--accent);text-decoration:none}}
.cost-row{{display:flex;align-items:center;padding:12px 0;border-bottom:1px solid var(--border)}}.cost-row:last-child{{border-bottom:none}}
.cost-comp{{flex:1;font-weight:500}}.cost-est{{width:140px;font-weight:700;text-align:right}}.cost-basis{{width:260px;color:var(--text2);font-size:12px;text-align:right}}
.cost-total{{background:rgba(75,123,245,.06);border:1px solid rgba(75,123,245,.2);border-radius:8px;padding:14px 20px;margin-top:12px}}
.cost-total .cost-comp{{color:var(--accent);font-size:15px}}.cost-total .cost-est{{color:var(--accent);font-size:17px}}
.heatmap{{display:flex;gap:3px;flex-wrap:wrap;margin:16px 0}}
.hm-cell{{width:28px;height:28px;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:600;cursor:pointer;position:relative}}
.hm-cell.low{{background:rgba(52,211,153,.2);color:var(--green)}}.hm-cell.medium{{background:rgba(251,191,36,.2);color:var(--yellow)}}.hm-cell.high{{background:rgba(248,113,113,.2);color:var(--red)}}
.hm-cell:hover::after{{content:attr(data-tip);position:absolute;bottom:110%;left:50%;transform:translateX(-50%);background:var(--elevated);color:var(--text);padding:4px 8px;border-radius:4px;font-size:10px;white-space:nowrap;z-index:10;border:1px solid var(--border)}}
.pill{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:600;margin:1px 2px;background:rgba(255,255,255,.06);color:var(--text2)}}
details summary{{cursor:pointer;font-weight:600;font-size:13px;color:var(--text2);padding:8px 0}}
.mermaid-box{{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:20px;margin-bottom:16px;overflow-x:auto}}

/* Grade circle */
.grade-circle{{width:80px;height:80px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:32px;font-weight:700;margin:0 auto 12px}}

@keyframes fadeInUp{{from{{opacity:0;transform:translateY(12px)}}to{{opacity:1;transform:translateY(0)}}}}
.content .card{{animation:fadeInUp .35s ease-out both}}
.content .card:nth-child(2){{animation-delay:.05s}}.content .card:nth-child(3){{animation-delay:.1s}}

@media(max-width:1200px){{.kpi-grid{{grid-template-columns:repeat(2,1fr)}}.grid-2{{grid-template-columns:1fr}}.framework-grid{{grid-template-columns:repeat(2,1fr)}}}}
@media(max-width:768px){{.sidebar{{display:none}}.kpi-grid{{grid-template-columns:1fr}}.grid-2,.grid-3{{grid-template-columns:1fr}}.framework-grid{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="layout">
  <!-- Sidebar -->
  <nav class="sidebar">
    <div class="sb-brand">
      <div class="logo">{LOGO_SVG}</div>
      <small>Lakehouse Optimizer Lite</small>
    </div>
    <div class="sb-nav">
      <div class="sidebar-group">Overview</div>
      <button class="nav-item active" data-tab="executive"><i class="fas fa-chart-line"></i> Executive Summary</button>
      <button class="nav-item" data-tab="workspace"><i class="fas fa-building"></i> Workspace</button>
      <div class="sidebar-group">Security</div>
      <button class="nav-item" data-tab="compliance"><i class="fas fa-shield-halved"></i> Compliance</button>
      <button class="nav-item" data-tab="architecture"><i class="fas fa-sitemap"></i> Architecture</button>
      <div class="sidebar-group">Operations</div>
      <button class="nav-item" data-tab="infrastructure"><i class="fas fa-server"></i> Infrastructure</button>
      <button class="nav-item" data-tab="spend"><i class="fas fa-dollar-sign"></i> Spend Overview</button>
      <button class="nav-item" data-tab="workflows"><i class="fas fa-cogs"></i> Workflows</button>
      <button class="nav-item" data-tab="costexplorer"><i class="fas fa-magnifying-glass-dollar"></i> Cost Explorer</button>
      <button class="nav-item" data-tab="cost"><i class="fas fa-calculator"></i> Cost Details</button>
      <div class="sidebar-group">Data</div>
      <button class="nav-item" data-tab="apps"><i class="fas fa-rocket"></i> Apps & Models</button>
      <button class="nav-item" data-tab="tables"><i class="fas fa-database"></i> Table Inventory</button>
      <div class="sidebar-group">Activity</div>
      <button class="nav-item" data-tab="users"><i class="fas fa-users"></i> User Activity</button>
      <button class="nav-item" data-tab="daily"><i class="fas fa-chart-bar"></i> Daily Trends</button>
      <button class="nav-item" data-tab="pricing"><i class="fas fa-tags"></i> DBU Pricing</button>
    </div>
    <div class="sb-footer">
      <div>Powered by <span class="powered">Blueprint</span></div>
      <div class="powered-sub">Databricks Platform</div>
    </div>
  </nav>

  <div class="main-area">
    <!-- Header -->
    <header class="header">
      <div class="h-title">Workspace Analysis</div>
      <div class="h-meta">
        <span class="cloud-badge"><i class="fas fa-cloud"></i> {cloud}</span>
        <select id="date-range">
          <option value="7">Last 7 days</option>
          <option value="14">Last 14 days</option>
          <option value="30" selected>Last 30 days</option>
          <option value="90">Last 90 days</option>
          <option value="365">Last 1 year</option>
          <option value="0">All time</option>
        </select>
        <span>{last_refresh}</span>
        <button class="btn-refresh" id="refresh-btn" onclick="triggerRefresh()"><i class="fas fa-sync-alt"></i> Refresh</button>
      </div>
    </header>

    <!-- Content -->
    <div class="content">

<!-- ============ TAB 1: EXECUTIVE ============ -->
<section class="tab active" id="tab-executive">
  <div class="section-title">Executive Summary</div>
  <div class="kpi-grid">
    <div class="kpi blue" data-goto="daily"><div class="kpi-icon"><i class="fas fa-search"></i></div><div class="kpi-label">Total Queries</div><div class="kpi-value" id="kpi-exec-queries">{total_queries:,}</div><div class="kpi-sub">{success_pct}% success rate</div></div>
    <div class="kpi green" data-goto="cost"><div class="kpi-icon"><i class="fas fa-dollar-sign"></i></div><div class="kpi-label">Est. Monthly Cost</div><div class="kpi-value">${total_cost_est:,.2f}</div><div class="kpi-sub">SQL ${sql_cost:,.0f} &bull; Apps ${app_cost_est:,.0f} &bull; Storage ${storage_cost:,.0f}</div></div>
    <div class="kpi purple" data-goto="cost"><div class="kpi-icon"><i class="fas fa-bolt"></i></div><div class="kpi-label">DBUs Consumed (30d)</div><div class="kpi-value">{total_dbu:,.0f}</div><div class="kpi-sub">{total_gb} GB read &bull; {active_users} users</div></div>
    <div class="kpi cyan" data-goto="compliance"><div class="kpi-icon"><i class="fas fa-shield-halved"></i></div><div class="kpi-label">Security Grade</div><div class="kpi-value" style="color:{grade_color}">{grade}</div><div class="kpi-sub">{score_val}/100 &bull; {security_score.get("total_findings",0)} findings</div></div>
  </div>
  <div class="grid-2">
    <div class="card"><div class="card-hdr">Daily Query Volume</div><div class="card-body"><div class="chart-box h300"><canvas id="exec-daily"></canvas></div></div></div>
    <div class="card"><div class="card-hdr">Resource Inventory</div><div class="card-body">
      <table class="dtable">
        <tr><td style="font-weight:600">Cloud</td><td>{cloud}</td></tr>
        <tr class="clickable-row" data-goto="apps"><td style="font-weight:600">Active Apps</td><td>{len(app_list)} <i class="fas fa-arrow-right" style="font-size:9px;opacity:.4;margin-left:4px"></i></td></tr>
        <tr class="clickable-row" data-goto="apps"><td style="font-weight:600">Model Endpoints</td><td>{len(model_list)} <i class="fas fa-arrow-right" style="font-size:9px;opacity:.4;margin-left:4px"></i></td></tr>
        <tr class="clickable-row" data-goto="daily"><td style="font-weight:600">Data Read (30d)</td><td>{total_gb} GB <i class="fas fa-arrow-right" style="font-size:9px;opacity:.4;margin-left:4px"></i></td></tr>
        <tr class="clickable-row" data-goto="cost"><td style="font-weight:600">DBUs Consumed (30d)</td><td>{total_dbu:,.0f} DBU <i class="fas fa-arrow-right" style="font-size:9px;opacity:.4;margin-left:4px"></i></td></tr>
        <tr class="clickable-row" data-goto="compliance"><td style="font-weight:600">Security Findings</td><td>{security_score.get("critical",0)} critical, {security_score.get("high",0)} high <i class="fas fa-arrow-right" style="font-size:9px;opacity:.4;margin-left:4px"></i></td></tr>
      </table>
    </div></div>
  </div>
  <div class="grid-2">
    <div class="card"><div class="card-hdr">Top Users by Query Count</div><div class="card-body"><div class="chart-box h250"><canvas id="exec-users"></canvas></div></div></div>
    <div class="card"><div class="card-hdr">Findings by Severity</div><div class="card-body"><div class="chart-box h250"><canvas id="exec-findings"></canvas></div></div></div>
  </div>
</section>

<!-- ============ TAB 2: WORKSPACE ============ -->
<section class="tab" id="tab-workspace">
  <div class="section-title">Workspace Overview</div>
  <div class="kpi-grid" style="grid-template-columns:repeat(6,1fr)">
    <div class="kpi blue"><div class="kpi-label">Cloud</div><div class="kpi-value" style="font-size:20px">{wp_cloud}</div></div>
    <div class="kpi green"><div class="kpi-label">Region</div><div class="kpi-value" style="font-size:16px">{wp_region}</div></div>
    <div class="kpi purple"><div class="kpi-label">Tier</div><div class="kpi-value" style="font-size:18px">{wp_tier}</div></div>
    <div class="kpi cyan" data-goto="users"><div class="kpi-label">Total Users</div><div class="kpi-value">{wp_users}</div></div>
    <div class="kpi yellow" data-goto="infrastructure"><div class="kpi-label">Clusters</div><div class="kpi-value">{wp_clusters}</div></div>
    <div class="kpi red" data-goto="infrastructure"><div class="kpi-label">Warehouses</div><div class="kpi-value">{wp_warehouses}</div></div>
  </div>
  <div class="grid-2">
    <div class="card"><div class="card-hdr">Workspace Configuration</div><div class="card-body"><div style="max-height:400px;overflow-y:auto"><table class="dtable" id="ws-config-table"><thead><tr><th>Setting</th><th>Value</th></tr></thead><tbody></tbody></table></div></div></div>
    <div class="card"><div class="card-hdr">Identity & Access</div><div class="card-body"><table class="dtable" id="ws-identity-table"><tbody></tbody></table></div></div>
  </div>
  <div class="grid-2">
    <div class="card"><div class="card-hdr">Data Assets</div><div class="card-body"><table class="dtable" id="ws-data-table"><tbody></tbody></table></div></div>
    <div class="card"><div class="card-hdr">Network & Security</div><div class="card-body"><table class="dtable" id="ws-network-table"><tbody></tbody></table></div></div>
  </div>
</section>

<!-- ============ TAB 3: COMPLIANCE ============ -->
<section class="tab" id="tab-compliance">
  <div class="section-title">Security & Compliance</div>
  <div class="kpi-grid" style="grid-template-columns:repeat(5,1fr)">
    <div class="kpi blue">
      <div class="kpi-label">Security Score</div>
      <div class="grade-circle" style="border:3px solid {grade_color};color:{grade_color}">{grade}</div>
      <div class="kpi-sub" style="text-align:center">{score_val}/100</div>
    </div>
    <div class="kpi red" style="cursor:pointer" onclick="document.getElementById('findings-table').scrollIntoView({{behavior:'smooth'}})"><div class="kpi-label">Critical</div><div class="kpi-value" style="color:var(--red)">{security_score.get("critical",0)}</div></div>
    <div class="kpi yellow" style="cursor:pointer" onclick="document.getElementById('findings-table').scrollIntoView({{behavior:'smooth'}})"><div class="kpi-label">High</div><div class="kpi-value" style="color:var(--yellow)">{security_score.get("high",0)}</div></div>
    <div class="kpi blue" style="cursor:pointer" onclick="document.getElementById('findings-table').scrollIntoView({{behavior:'smooth'}})"><div class="kpi-label">Medium</div><div class="kpi-value" style="color:var(--accent)">{security_score.get("medium",0)}</div></div>
    <div class="kpi green" style="cursor:pointer" onclick="document.getElementById('findings-table').scrollIntoView({{behavior:'smooth'}})"><div class="kpi-label">Low</div><div class="kpi-value" style="color:var(--green)">{security_score.get("low",0)}</div></div>
  </div>
  <div class="framework-grid" id="framework-grid"></div>
  <div id="framework-details"></div>
  <div class="card"><div class="card-hdr">Security Findings</div><div class="card-body" style="overflow-x:auto;max-height:500px;overflow-y:auto">
    <table class="dtable" id="findings-table"><thead><tr><th>ID</th><th>Severity</th><th>Category</th><th>NIST</th><th>Finding</th><th>Recommendation</th></tr></thead><tbody></tbody></table>
  </div></div>
</section>

<!-- ============ TAB 4: ARCHITECTURE ============ -->
<section class="tab" id="tab-architecture">
  <div class="section-title">Architecture Diagrams</div>
  <div class="card"><div class="card-hdr">Workspace Architecture</div><div class="card-body"><div class="mermaid-box" id="mermaid-arch"></div></div></div>
  <div class="card"><div class="card-hdr">Security Posture</div><div class="card-body"><div class="mermaid-box" id="mermaid-sec"></div></div></div>
  <div class="card"><div class="card-hdr">Data Flow</div><div class="card-body"><div class="mermaid-box" id="mermaid-flow"></div></div></div>
</section>

<!-- ============ TAB 5: INFRASTRUCTURE ============ -->
<section class="tab" id="tab-infrastructure">
  <div class="section-title">Infrastructure</div>
  <div class="kpi-grid">
    <div class="kpi blue"><div class="kpi-icon"><i class="fas fa-server"></i></div><div class="kpi-label">Total Clusters</div><div class="kpi-value">{len(cluster_raw)}</div></div>
    <div class="kpi green"><div class="kpi-icon"><i class="fas fa-play-circle"></i></div><div class="kpi-label">Running Clusters</div><div class="kpi-value">{running_clusters}</div></div>
    <div class="kpi purple"><div class="kpi-icon"><i class="fas fa-warehouse"></i></div><div class="kpi-label">SQL Warehouses</div><div class="kpi-value">{len(warehouse_raw)}</div></div>
    <div class="kpi yellow"><div class="kpi-icon"><i class="fas fa-cogs"></i></div><div class="kpi-label">Jobs</div><div class="kpi-value">{len(jobs_raw)}</div></div>
  </div>
  <div class="card"><div class="card-hdr">Cluster Inventory</div><div class="card-body" style="overflow-x:auto;max-height:500px;overflow-y:auto">
    <table class="dtable" id="cluster-table"><thead><tr><th>Name</th><th>State</th><th>Spark Version</th><th>Node Type</th><th class="num">Auto-terminate (min)</th><th>Encryption</th></tr></thead><tbody></tbody></table>
  </div></div>
  <div class="card"><div class="card-hdr">SQL Warehouses</div><div class="card-body" style="overflow-x:auto;max-height:500px;overflow-y:auto">
    <table class="dtable" id="warehouse-table"><thead><tr><th>Name</th><th>State</th><th>Size</th><th>Type</th><th>Serverless</th><th class="num">Max Clusters</th></tr></thead><tbody></tbody></table>
  </div></div>
</section>

<!-- ============ TAB 6: COST ANALYSIS ============ -->
<!-- ============ TAB: SPEND OVERVIEW ============ -->
<section class="tab" id="tab-spend">
  <div class="section-title">Spend Overview <span style="font-size:12px;color:var(--text2);font-weight:400">(Last 90 Days from system.billing.usage)</span></div>
  <div class="kpi-grid">
    <div class="kpi blue"><div class="kpi-icon"><i class="fas fa-dollar-sign"></i></div><div class="kpi-label">Total DBUs (90d)</div><div class="kpi-value" id="spend-total-dbus">—</div><div class="kpi-sub" id="spend-total-cost"></div></div>
    <div class="kpi green" data-goto="workflows"><div class="kpi-icon"><i class="fas fa-cogs"></i></div><div class="kpi-label">Avg. Daily Cost</div><div class="kpi-value" id="spend-daily-avg">—</div><div class="kpi-sub">per day</div></div>
    <div class="kpi purple"><div class="kpi-icon"><i class="fas fa-chart-pie"></i></div><div class="kpi-label">Top Category</div><div class="kpi-value" id="spend-top-cat">—</div><div class="kpi-sub" id="spend-top-cat-pct"></div></div>
    <div class="kpi yellow"><div class="kpi-icon"><i class="fas fa-tags"></i></div><div class="kpi-label">Avg DBU Price</div><div class="kpi-value">${avg_dbu_price:.4f}</div><div class="kpi-sub">per DBU</div></div>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1.2fr;gap:20px;margin-bottom:24px">
    <div class="card"><div class="card-hdr">Monthly Cost by Category</div><div class="card-body"><div class="chart-box h400"><canvas id="spend-monthly-chart"></canvas></div></div></div>
    <div class="card"><div class="card-hdr">Cost by Tag <span style="font-size:11px;font-weight:400;color:var(--text3);text-transform:none;letter-spacing:0;cursor:pointer" onclick="costExplorerCategory='';rendered['costexplorer']=false;goTab('costexplorer')">→ View Details</span></div><div class="card-body"><div class="chart-box" style="height:420px"><canvas id="spend-tag-chart"></canvas></div></div></div>
  </div>
  <div class="card"><div class="card-hdr">Cost Breakdown by Category (90 Days)</div><div class="card-body" style="overflow-x:auto">
    <table class="dtable" id="spend-breakdown-table">
      <thead><tr><th></th><th>Category</th><th class="num">Total DBUs</th><th class="num">% of Total</th><th class="num">Est. Cost</th><th class="num">Active Days</th><th class="num">Avg DBU/Day</th></tr></thead>
      <tbody></tbody>
    </table>
  </div></div>
  <div class="card"><div class="card-hdr">Daily Cost Trend (90 Days)</div><div class="card-body"><div class="chart-box h300"><canvas id="spend-daily-chart"></canvas></div></div></div>
</section>

<!-- ============ TAB: WORKFLOWS ============ -->
<section class="tab" id="tab-workflows">
  <div class="section-title">Workflows <span style="font-size:12px;color:var(--text2);font-weight:400">(Last 30 Days)</span></div>
  <div class="kpi-grid">
    <div class="kpi blue"><div class="kpi-icon"><i class="fas fa-cogs"></i></div><div class="kpi-label">Total Jobs</div><div class="kpi-value" id="wf-total-jobs">—</div></div>
    <div class="kpi green"><div class="kpi-icon"><i class="fas fa-play-circle"></i></div><div class="kpi-label">Total Runs</div><div class="kpi-value" id="wf-total-runs">—</div></div>
    <div class="kpi purple"><div class="kpi-icon"><i class="fas fa-dollar-sign"></i></div><div class="kpi-label">Total Job Cost</div><div class="kpi-value" id="wf-total-cost">—</div></div>
    <div class="kpi yellow"><div class="kpi-icon"><i class="fas fa-clock"></i></div><div class="kpi-label">Total Duration</div><div class="kpi-value" id="wf-total-duration">—</div></div>
  </div>
  <div class="card"><div class="card-hdr">
    <div style="display:flex;justify-content:space-between;align-items:center;width:100%">
      <span>Job Runs</span>
      <div style="display:flex;gap:8px;align-items:center">
        <select id="wf-sort" style="background:var(--elevated);color:var(--text);border:1px solid var(--border);padding:4px 8px;border-radius:4px;font-size:11px;font-family:var(--font)">
          <option value="cost">Sort: Cost</option><option value="runs">Sort: Runs</option><option value="duration">Sort: Duration</option><option value="name">Sort: Name</option>
        </select>
        <input type="text" id="wf-search" placeholder="Search jobs..." style="background:var(--elevated);color:var(--text);border:1px solid var(--border);padding:4px 10px;border-radius:4px;font-size:12px;font-family:var(--font);width:160px">
      </div>
    </div>
  </div><div class="card-body" style="overflow-x:auto">
    <table class="dtable" id="wf-table">
      <thead><tr>
        <th>Name</th><th>Compute Type</th><th class="num">Est. Cost</th><th>Run Statuses</th><th class="num">Duration</th><th class="num">Runs</th>
      </tr></thead>
      <tbody></tbody>
    </table>
  </div></div>
</section>

<!-- ============ TAB: COST EXPLORER ============ -->
<section class="tab" id="tab-costexplorer">
  <div class="section-title">Cost Explorer <span style="font-size:12px;color:var(--text2);font-weight:400">(Last 90 Days — Billing Line Items)</span></div>
  <div class="kpi-grid">
    <div class="kpi blue"><div class="kpi-icon"><i class="fas fa-receipt"></i></div><div class="kpi-label">Line Items</div><div class="kpi-value" id="ce-total-lines">—</div></div>
    <div class="kpi green"><div class="kpi-icon"><i class="fas fa-dollar-sign"></i></div><div class="kpi-label">Total Est. Cost</div><div class="kpi-value" id="ce-total-cost">—</div></div>
    <div class="kpi purple"><div class="kpi-icon"><i class="fas fa-bolt"></i></div><div class="kpi-label">Total DBUs</div><div class="kpi-value" id="ce-total-dbus">—</div></div>
    <div class="kpi yellow"><div class="kpi-icon"><i class="fas fa-layer-group"></i></div><div class="kpi-label">Categories</div><div class="kpi-value" id="ce-total-cats">—</div></div>
  </div>
  <div class="card"><div class="card-hdr">
    <div style="display:flex;justify-content:space-between;align-items:center;width:100%">
      <span>Billing Line Items</span>
      <div style="display:flex;gap:8px;align-items:center">
        <select id="ce-filter-cat" style="background:var(--elevated);color:var(--text);border:1px solid var(--border);padding:4px 8px;border-radius:4px;font-size:11px;font-family:var(--font)">
          <option value="">All Categories</option>
        </select>
        <select id="ce-sort" style="background:var(--elevated);color:var(--text);border:1px solid var(--border);padding:4px 8px;border-radius:4px;font-size:11px;font-family:var(--font)">
          <option value="cost">Sort: Cost</option><option value="dbus">Sort: DBUs</option><option value="days">Sort: Active Days</option><option value="sku">Sort: SKU</option>
        </select>
        <input type="text" id="ce-search" placeholder="Search SKU, job, warehouse..." style="background:var(--elevated);color:var(--text);border:1px solid var(--border);padding:4px 10px;border-radius:4px;font-size:12px;font-family:var(--font);width:200px">
      </div>
    </div>
  </div><div class="card-body" style="overflow-x:auto;max-height:600px;overflow-y:auto">
    <table class="dtable" id="ce-table">
      <thead><tr>
        <th>Category</th><th>SKU</th><th>Resource</th><th class="num">Est. Cost</th><th class="num">DBUs</th><th class="num">% of Total</th><th class="num">Active Days</th><th>Period</th>
      </tr></thead>
      <tbody></tbody>
    </table>
  </div></div>
</section>

<!-- ============ TAB: COST DETAILS ============ -->
<section class="tab" id="tab-cost">
  <div class="section-title">Cost Details</div>
  <div class="kpi-grid">
    <div class="kpi blue"><div class="kpi-icon"><i class="fas fa-dollar-sign"></i></div><div class="kpi-label">Est. Monthly Total</div><div class="kpi-value">${total_cost_est:,.2f}</div></div>
    <div class="kpi green"><div class="kpi-icon"><i class="fas fa-database"></i></div><div class="kpi-label">SQL Compute Cost</div><div class="kpi-value">${sql_cost:,.2f}</div></div>
    <div class="kpi purple"><div class="kpi-icon"><i class="fas fa-rocket"></i></div><div class="kpi-label">App Compute Cost</div><div class="kpi-value">${app_cost_est:,.2f}</div></div>
    <div class="kpi yellow"><div class="kpi-icon"><i class="fas fa-hdd"></i></div><div class="kpi-label">Storage Cost</div><div class="kpi-value">${storage_cost:,.2f}</div></div>
  </div>
  <div class="grid-2">
    <div class="card"><div class="card-hdr">Cost by User (Est. Compute)</div><div class="card-body"><div class="chart-box h400"><canvas id="cost-by-user"></canvas></div></div></div>
    <div class="card"><div class="card-hdr">Cost by Category</div><div class="card-body"><div class="chart-box h400"><canvas id="cost-category"></canvas></div></div></div>
  </div>
  <div class="card"><div class="card-hdr">Cost Breakdown (Monthly)</div><div class="card-body">
    <div class="cost-row"><span class="cost-comp">Serverless SQL Compute</span><span class="cost-est">${sql_cost:,.2f}</span><span class="cost-basis">~{total_dbu:,.0f} DBU * $0.07/DBU</span></div>
    <div class="cost-row"><span class="cost-comp">Apps ({len(app_list)}x)</span><span class="cost-est">${app_cost_est:,.2f}</span><span class="cost-basis">~$15/app estimated</span></div>
    <div class="cost-row"><span class="cost-comp">Managed Storage</span><span class="cost-est">${storage_cost:,.2f}</span><span class="cost-basis">~{est_mb} MB across {total_tables} tables</span></div>
    <div class="cost-row"><span class="cost-comp">Foundation Models</span><span class="cost-est">Pay-per-token</span><span class="cost-basis">{len(model_list)} endpoints</span></div>
    <div class="cost-total"><div class="cost-row"><span class="cost-comp">TOTAL ESTIMATED</span><span class="cost-est">${total_cost_est:,.2f}</span><span class="cost-basis">Cloud: {cloud}</span></div></div>
  </div></div>
</section>

<!-- ============ TAB 7: APPS ============ -->
<section class="tab" id="tab-apps">
  <div class="section-title">Apps & Model Endpoints</div>
  <div style="font-size:14px;font-weight:600;color:var(--text2);margin-bottom:16px">Databricks Apps ({len(app_list)})</div>
  <div class="grid-2" id="apps-cards"></div>
  <div style="font-size:14px;font-weight:600;color:var(--text2);margin:24px 0 16px">Foundation Model Pricing (Top 12)</div>
  <div class="card" style="margin-bottom:24px"><div class="card-body"><div class="chart-box h400"><canvas id="model-price"></canvas></div></div></div>
  <div class="card"><div class="card-hdr">All Endpoints ({len(model_list)})</div><div class="card-body" style="overflow-x:auto"><table class="dtable" id="models-table"><thead><tr><th>Model</th><th class="num">Input DBU/1M</th><th class="num">Output DBU/1M</th><th class="num">Input $/1M</th><th class="num">Output $/1M</th><th>Capabilities</th></tr></thead><tbody></tbody></table></div></div>
</section>

<!-- ============ TAB 8: TABLES ============ -->
<section class="tab" id="tab-tables">
  <div class="section-title">Table Inventory</div>
  <div class="kpi-grid" style="grid-template-columns:repeat(3,1fr)">
    <div class="kpi green"><div class="kpi-label">Total Tables</div><div class="kpi-value">{total_tables}</div></div>
    <div class="kpi blue"><div class="kpi-label">Sampled Sizes</div><div class="kpi-value">{len(sized_tables)}</div></div>
    <div class="kpi purple"><div class="kpi-label">Est. Storage</div><div class="kpi-value">~{est_mb} MB</div></div>
  </div>
  <div class="card" style="margin-bottom:24px"><div class="card-hdr">Storage Distribution</div><div class="card-body"><div class="chart-box h300"><canvas id="storage-chart"></canvas></div></div></div>
  <div class="card"><div class="card-body">
    <div style="margin-bottom:16px;display:flex;align-items:center;gap:16px"><input type="text" class="search-bar" id="table-search" placeholder="Search tables..."><span style="font-size:12px;color:var(--text2)" id="table-count">Showing {total_tables} tables</span></div>
    <div style="overflow-x:auto;max-height:600px;overflow-y:auto"><table class="dtable" id="tables-table"><thead><tr><th>Table</th><th>Schema</th><th>Type</th><th>Created</th><th>Description</th><th class="num">Size</th></tr></thead><tbody></tbody></table></div>
  </div></div>
</section>

<!-- ============ TAB 9: USERS ============ -->
<section class="tab" id="tab-users">
  <div class="section-title">User Activity</div>
  <div class="kpi-grid" style="grid-template-columns:repeat(3,1fr)">
    <div class="kpi blue"><div class="kpi-label">Total Queries</div><div class="kpi-value">{total_queries:,}</div></div>
    <div class="kpi green"><div class="kpi-label">Data Read</div><div class="kpi-value">{total_gb} GB</div></div>
    <div class="kpi yellow"><div class="kpi-label">DBUs Consumed</div><div class="kpi-value">{total_dbu:,.0f}</div></div>
  </div>
  <div class="grid-2">
    <div class="card"><div class="card-hdr">Query Distribution</div><div class="card-body"><div class="chart-box h300"><canvas id="user-donut"></canvas></div></div></div>
    <div class="card"><div class="card-hdr">Success vs Failed</div><div class="card-body"><div class="chart-box h300"><canvas id="user-bar"></canvas></div></div></div>
  </div>
  <div class="card"><div class="card-hdr">Detailed Activity</div><div class="card-body" style="overflow-x:auto"><table class="dtable" id="users-table"><thead><tr><th>User</th><th>Status</th><th class="num">Queries</th><th class="num">Data (GB)</th><th class="num">Rows</th><th class="num">Duration (min)</th></tr></thead><tbody></tbody></table></div></div>
</section>

<!-- ============ TAB 10: DAILY ============ -->
<section class="tab" id="tab-daily">
  <div class="section-title">Daily Activity Trends</div>
  <div class="kpi-grid">
    <div class="kpi blue"><div class="kpi-label">Total Queries</div><div class="kpi-value">{total_queries:,}</div></div>
    <div class="kpi red"><div class="kpi-label">Peak Day</div><div class="kpi-value">{peak_day["total"]:,}</div><div class="kpi-sub">{peak_day["date"]}</div></div>
    <div class="kpi green"><div class="kpi-label">Avg/Day</div><div class="kpi-value">{round(total_queries / max(len(daily_list), 1))}</div></div>
    <div class="kpi yellow"><div class="kpi-label">Success Rate</div><div class="kpi-value">{success_pct}%</div></div>
  </div>
  <div class="card" style="margin-bottom:24px"><div class="card-hdr">Daily Query Volume</div><div class="card-body"><div class="chart-box h300"><canvas id="daily-stacked"></canvas></div></div></div>
  <div style="font-size:14px;font-weight:600;color:var(--text2);margin-bottom:8px">Activity Heatmap</div>
  <div class="heatmap" id="heatmap"></div>
  <div class="grid-2" style="margin-top:24px">
    <div class="card"><div class="card-hdr">Data Read & Compute</div><div class="card-body"><div class="chart-box h250"><canvas id="daily-dual"></canvas></div></div></div>
    <div class="card"><div class="card-hdr">Daily Table</div><div class="card-body" style="max-height:300px;overflow-y:auto"><table class="dtable" id="daily-table"><thead><tr><th>Date</th><th class="num">Total</th><th class="num">OK</th><th class="num">Fail</th><th class="num">GB</th><th class="num">Min</th><th>Level</th></tr></thead><tbody></tbody></table></div></div>
  </div>
</section>

<!-- ============ TAB 11: PRICING ============ -->
<section class="tab" id="tab-pricing">
  <div class="section-title">DBU Pricing Reference</div>
  <div class="card" style="margin-bottom:24px"><div class="card-hdr">Price by Category</div><div class="card-body"><div class="chart-box h300"><canvas id="pricing-chart"></canvas></div></div></div>
  <div class="card"><div class="card-body" style="overflow-x:auto"><table class="dtable" id="pricing-table"><thead><tr><th>SKU</th><th class="num">Price (USD)</th><th>Unit</th></tr></thead><tbody></tbody></table></div></div>
</section>

    </div><!-- content -->
  </div><!-- main-area -->
</div><!-- layout -->

<script>
const D={data_json};
const COLORS=['#4B7BF5','#34D399','#FBBF24','#A78BFA','#F87171','#00E5FF','#ec4899','#14b8a6','#f97316','#6366f1'];
const chartDef={{responsive:true,maintainAspectRatio:false}};
function fmt(n){{return n!=null?n.toLocaleString():'\u2014';}}
function fmtN(n){{if(n==null)return'\u2014';if(Math.abs(n)>=1e6)return(n/1e6).toFixed(1)+'M';if(Math.abs(n)>=1e3)return(n/1e3).toFixed(1)+'k';return n<10?n.toFixed(2):Math.round(n).toLocaleString();}}
function fmtB(b){{if(!b)return'\u2014';if(b<1024)return b+' B';if(b<1048576)return(b/1024).toFixed(1)+' KB';if(b<1073741824)return(b/1048576).toFixed(2)+' MB';return(b/1073741824).toFixed(2)+' GB';}}
function shortN(s){{return s&&s.length>22?s.slice(0,20)+'...':s;}}

// Filtered data references (date range)
D._filteredDaily = D.daily;
D._filteredUsers = D.users;

// Date range filter
function applyDateFilter(){{
  const range = parseInt(document.getElementById('date-range').value);
  if(range === 0){{
    D._filteredDaily = D.daily;
  }} else {{
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - range);
    const cutoffStr = cutoff.toISOString().slice(0, 10);
    D._filteredDaily = D.daily.filter(d => d.date >= cutoffStr);
  }}
  D._filteredUsers = D.users;
  // Destroy existing charts for affected tabs before re-render
  costExplorerCategory='';
  ['exec-daily','exec-users','exec-findings','user-donut','user-bar','daily-stacked','daily-dual','cost-by-user','cost-category','spend-monthly-chart','spend-tag-chart','spend-daily-chart'].forEach(id=>{{
    const el=document.getElementById(id);
    if(el){{const ch=Chart.getChart(el);if(ch)ch.destroy();}}
  }});
  rendered = {{}};
  const activeTab = document.querySelector('.nav-item.active');
  if(activeTab) renderTab(activeTab.dataset.tab);
}}
document.getElementById('date-range').addEventListener('change', applyDateFilter);

// Tab navigation
let costExplorerCategory='';
const tabs=document.querySelectorAll('.nav-item'),secs=document.querySelectorAll('.tab');
let rendered={{}};
tabs.forEach(t=>t.addEventListener('click',()=>{{
  tabs.forEach(n=>n.classList.remove('active'));secs.forEach(s=>s.classList.remove('active'));
  t.classList.add('active');document.getElementById('tab-'+t.dataset.tab).classList.add('active');
  renderTab(t.dataset.tab);
}}));

// ---- goTab: programmatic tab switch ----
function goTab(tabId){{
  const target=document.querySelector(`.nav-item[data-tab="${{tabId}}"]`);
  if(target)target.click();
}}

// KPI cards & clickable rows -> goTab on click
document.querySelectorAll('[data-goto]').forEach(el=>{{
  el.addEventListener('click',()=>goTab(el.dataset.goto));
}});

function renderTab(id){{if(rendered[id])return;rendered[id]=true;
  if(id==='executive')renderExec();
  else if(id==='workspace')renderWorkspace();
  else if(id==='compliance')renderCompliance();
  else if(id==='architecture')renderArchitecture();
  else if(id==='infrastructure')renderInfra();
  else if(id==='spend')renderSpend();
  else if(id==='workflows')renderWorkflows();
  else if(id==='costexplorer')renderCostExplorer();
  else if(id==='cost')renderCost();
  else if(id==='apps')renderApps();
  else if(id==='tables')renderTables();
  else if(id==='users')renderUsers();
  else if(id==='daily')renderDaily();
  else if(id==='pricing')renderPricing();
}}

// Chart.js defaults
Chart.defaults.color='#8B949E';
Chart.defaults.borderColor='#272D3F';
Chart.defaults.font.family="'DM Sans','Inter',system-ui,sans-serif";

function renderExec(){{
  const daily = D._filteredDaily;
  const labels=daily.map(d=>d.date.slice(5));
  new Chart(document.getElementById('exec-daily'),{{type:'bar',data:{{labels,datasets:[
    {{label:'Succeeded',data:daily.map(d=>d.succeeded),backgroundColor:'rgba(52,211,153,.7)',borderRadius:3}},
    {{label:'Failed',data:daily.map(d=>d.failed),backgroundColor:'rgba(248,113,113,.7)',borderRadius:3}}
  ]}},options:{{...chartDef,plugins:{{legend:{{position:'top',labels:{{boxWidth:12}}}}}},scales:{{x:{{stacked:true,ticks:{{font:{{size:10}},maxRotation:45}}}},y:{{stacked:true,beginAtZero:true}}}}}}}});

  const uAgg={{}};D._filteredUsers.forEach(u=>{{uAgg[u.user]=(uAgg[u.user]||0)+u.queries;}});
  const sorted=Object.entries(uAgg).sort((a,b)=>b[1]-a[1]).slice(0,6);
  new Chart(document.getElementById('exec-users'),{{type:'bar',data:{{labels:sorted.map(s=>shortN(s[0])),datasets:[{{data:sorted.map(s=>s[1]),backgroundColor:COLORS.slice(0,sorted.length),borderRadius:4}}]}},options:{{...chartDef,indexAxis:'y',plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true}}}}}}}});

  const sc=D.score;
  const findingsChart=new Chart(document.getElementById('exec-findings'),{{type:'doughnut',data:{{labels:['Critical','High','Medium','Low'],datasets:[{{data:[sc.critical||0,sc.high||0,sc.medium||0,sc.low||0],backgroundColor:['#F87171','#FBBF24','#4B7BF5','#34D399']}}]}},options:{{...chartDef,cutout:'60%',plugins:{{legend:{{position:'right',labels:{{boxWidth:10,font:{{size:11}}}}}}}}}}}});

  // Click handlers: charts drill down to detail tabs
  document.getElementById('exec-daily').onclick=()=>goTab('daily');
  document.getElementById('exec-users').onclick=()=>goTab('users');
  document.getElementById('exec-findings').addEventListener('click',(e)=>{{
    const pts=findingsChart.getElementsAtEventForMode(e,'nearest',{{intersect:true}},false);
    goTab('compliance');
  }});
}}

function renderWorkspace(){{
  const wp = D.workspaceProfile;
  if(!wp || !Object.keys(wp).length) return;

  // Config flags
  const confTb = document.querySelector('#ws-config-table tbody');
  const flags = wp.config_flags || {{}};
  Object.entries(flags).forEach(([k,v])=>{{
    confTb.innerHTML += `<tr><td style="font-weight:500">${{k}}</td><td>${{v}}</td></tr>`;
  }});
  if(!Object.keys(flags).length) confTb.innerHTML = '<tr><td colspan="2" style="color:var(--text2)">No configuration flags available</td></tr>';

  // Identity & Access
  const idTb = document.querySelector('#ws-identity-table tbody');
  idTb.innerHTML = `
    <tr><td style="font-weight:600">Total Users</td><td>${{wp.total_users||0}}</td></tr>
    <tr><td style="font-weight:600">Admin Users</td><td>${{wp.admin_count||0}}</td></tr>
    <tr><td style="font-weight:600">Groups</td><td>${{wp.group_count||0}}</td></tr>
    <tr><td style="font-weight:600">Service Principals</td><td>${{wp.sp_count||0}}</td></tr>
  `;

  // Data Assets
  const daTb = document.querySelector('#ws-data-table tbody');
  daTb.innerHTML = `
    <tr><td style="font-weight:600">Catalogs</td><td>${{wp.catalog_count||0}}</td></tr>
    <tr><td style="font-weight:600">Metastores</td><td>${{wp.metastore_count||0}}</td></tr>
    <tr><td style="font-weight:600">Storage Credentials</td><td>${{wp.storage_cred_count||0}}</td></tr>
    <tr><td style="font-weight:600">External Locations</td><td>${{wp.ext_location_count||0}}</td></tr>
    <tr><td style="font-weight:600">Delta Shares</td><td>${{wp.share_count||0}}</td></tr>
  `;

  // Network & Security
  const nsTb = document.querySelector('#ws-network-table tbody');
  nsTb.innerHTML = `
    <tr><td style="font-weight:600">IP Access Lists</td><td>${{wp.ip_list_count||0}}</td></tr>
    <tr><td style="font-weight:600">Secret Scopes</td><td>${{wp.secret_scope_count||0}}</td></tr>
  `;
}}

function renderCompliance(){{
  // Framework cards
  const grid = document.getElementById('framework-grid');
  const details = document.getElementById('framework-details');
  const comp = D.compliance || {{}};
  const frameworks = [
    {{key:'hipaa', label:'HIPAA', color:'#4B7BF5'}},
    {{key:'fedramp', label:'FedRAMP', color:'#F87171'}},
    {{key:'soc2', label:'SOC 2', color:'#34D399'}},
    {{key:'rbac', label:'RBAC', color:'#A78BFA'}}
  ];

  frameworks.forEach(fw => {{
    const data = comp[fw.key] || {{}};
    const score = data.score || 0;
    const status = data.status || 'N/A';
    const controls = data.controls || [];

    // Status badge color
    let statusBg, statusColor;
    if(status === 'COMPLIANT') {{ statusBg = 'rgba(52,211,153,.15)'; statusColor = '#34D399'; }}
    else if(status === 'PARTIAL') {{ statusBg = 'rgba(251,191,36,.15)'; statusColor = '#FBBF24'; }}
    else {{ statusBg = 'rgba(248,113,113,.15)'; statusColor = '#F87171'; }}

    // Score circle border color
    let circleColor = score >= 80 ? '#34D399' : score >= 50 ? '#FBBF24' : '#F87171';

    grid.innerHTML += `<div class="compliance-card">
      <h4>${{fw.label}}</h4>
      <div class="score-circle" style="border-color:${{circleColor}};color:${{circleColor}}">${{score}}</div>
      <div class="status-badge" style="background:${{statusBg}};color:${{statusColor}}">${{status}}</div>
    </div>`;

    // Expandable controls
    if(controls.length) {{
      let rows = controls.map(c => {{
        const badge = c.status === 'PASS' ? '<span class="ctrl-pass">PASS</span>' : '<span class="ctrl-fail">FAIL</span>';
        return `<tr><td style="font-weight:500">${{c.name}}</td><td>${{badge}}</td><td style="font-size:12px;color:var(--text2)">${{c.detail}}</td></tr>`;
      }}).join('');
      details.innerHTML += `<div class="card"><div class="card-body"><details><summary>${{fw.label}} Controls (${{controls.filter(c=>c.status==='PASS').length}}/${{controls.length}} passed)</summary>
        <table class="dtable" style="margin-top:12px"><thead><tr><th>Control</th><th>Status</th><th>Detail</th></tr></thead><tbody>${{rows}}</tbody></table>
      </details></div></div>`;
    }}
  }});

  // Findings table
  const tb=document.querySelector('#findings-table tbody');
  D.findings.forEach((f,i)=>{{
    const bc=f.sev==='CRITICAL'?'badge-crit':f.sev==='HIGH'?'badge-high':f.sev==='MEDIUM'?'badge-med':'badge-low';
    tb.innerHTML+=`<tr><td>SEC-${{String(i+1).padStart(3,'0')}}</td><td><span class="badge ${{bc}}">${{f.sev}}</span></td><td>${{f.cat}}</td><td style="font-size:11px;color:var(--text2)">${{f.nist}}</td><td style="max-width:300px">${{f.finding}}</td><td style="max-width:250px;font-size:12px;color:var(--text2)">${{f.rec}}</td></tr>`;
  }});
}}

function renderArchitecture(){{
  const archDef = `{arch_mermaid}`;
  const secDef = `{sec_mermaid}`;
  const flowDef = `{flow_mermaid}`;
  mermaid.initialize({{startOnLoad:false,theme:'dark',themeVariables:{{primaryColor:'#4B7BF5',primaryTextColor:'#E6EDF3',primaryBorderColor:'#272D3F',lineColor:'#4B7BF5',secondaryColor:'#161B22',tertiaryColor:'#21262D'}}}});
  async function renderMM(id, def) {{
    try {{
      const {{svg}} = await mermaid.render(id+'-svg', def);
      document.getElementById(id).innerHTML = svg;
    }} catch(e) {{
      document.getElementById(id).innerHTML = '<pre style="color:var(--text2);font-size:12px">'+def+'</pre>';
    }}
  }}
  renderMM('mermaid-arch', archDef);
  renderMM('mermaid-sec', secDef);
  renderMM('mermaid-flow', flowDef);
}}

function renderInfra(){{
  // Clusters table
  const cTb = document.querySelector('#cluster-table tbody');
  D.clusters.forEach(c => {{
    const stateBadge = c.state === 'RUNNING' ? 'badge-green' : c.state === 'TERMINATED' ? 'badge-red' : 'badge-amber';
    const encBadge = c.encryption ? '<span class="badge badge-green">Enabled</span>' : '<span class="badge badge-red">Disabled</span>';
    cTb.innerHTML += `<tr><td style="font-weight:500">${{c.name}}</td><td><span class="badge ${{stateBadge}}">${{c.state}}</span></td><td style="font-size:12px">${{c.sparkVersion}}</td><td style="font-size:12px">${{c.nodeType}}</td><td class="num">${{c.autoTerminate || '\u2014'}}</td><td>${{encBadge}}</td></tr>`;
  }});
  if(!D.clusters.length) cTb.innerHTML = '<tr><td colspan="6" style="color:var(--text2)">No clusters found</td></tr>';

  // Warehouses table
  const wTb = document.querySelector('#warehouse-table tbody');
  D.warehouses.forEach(w => {{
    const stateBadge = w.state === 'RUNNING' ? 'badge-green' : w.state === 'STOPPED' ? 'badge-red' : 'badge-amber';
    const srvBadge = w.serverless ? '<span class="badge badge-green">Yes</span>' : '<span class="badge badge-gray">No</span>';
    wTb.innerHTML += `<tr><td style="font-weight:500">${{w.name}}</td><td><span class="badge ${{stateBadge}}">${{w.state}}</span></td><td>${{w.size}}</td><td>${{w.type}}</td><td>${{srvBadge}}</td><td class="num">${{w.maxClusters}}</td></tr>`;
  }});
  if(!D.warehouses.length) wTb.innerHTML = '<tr><td colspan="6" style="color:var(--text2)">No SQL warehouses found</td></tr>';
}}

// ---- Spend Overview ----
function renderSpend(){{
  const cbp = D.costByProduct || [];
  const mc = D.monthlyCost || [];
  const dc = D.dailyCost || [];
  const cbt = D.costByTag || [];
  const price = D.avgDbuPrice || 0.07;

  // KPIs
  const totalDbus = cbp.reduce((s,c)=>s+c.dbus,0);
  const totalCost = totalDbus * price;
  document.getElementById('spend-total-dbus').textContent = fmtN(totalDbus) + ' DBU';
  document.getElementById('spend-total-cost').textContent = '$' + fmtN(totalCost);
  const activeDays = dc.length || 1;
  document.getElementById('spend-daily-avg').textContent = '$' + fmtN(totalCost / activeDays);
  if(cbp.length){{
    const top = cbp[0];
    document.getElementById('spend-top-cat').textContent = top.product;
    const pct = totalDbus > 0 ? Math.round(top.dbus / totalDbus * 100) : 0;
    document.getElementById('spend-top-cat-pct').textContent = pct + '% of total spend';
  }}

  // Monthly stacked bar chart
  if(mc.length){{
    const months = [...new Set(mc.map(r=>r.month))].sort();
    const products = [...new Set(mc.map(r=>r.product))];
    const prodColors = {{'SQL':'#4B7BF5','JOBS':'#34D399','ALL_PURPOSE':'#FBBF24','DLT':'#A78BFA','APPS':'#00E5FF','INTERACTIVE':'#F87171','LAKEBASE':'#C084FC','SERVERLESS_COMPUTE':'#38BDF8','PREDICTIVE_OPTIMIZATION':'#FB923C','DATABASE':'#818CF8','NETWORKING':'#94A3B8'}};
    const datasets = products.map(p=>{{
      const color = prodColors[p] || COLORS[products.indexOf(p) % COLORS.length];
      return {{
        label: p,
        data: months.map(m=>{{
          const row = mc.find(r=>r.month===m && r.product===p);
          return row ? Math.round(row.dbus * price * 100) / 100 : 0;
        }}),
        backgroundColor: color,
        borderRadius: 2
      }};
    }});
    new Chart(document.getElementById('spend-monthly-chart'),{{
      type:'bar',
      data:{{ labels: months.map(m=>m.substring(0,7)), datasets }},
      options:{{...chartDef, plugins:{{legend:{{position:'bottom',labels:{{boxWidth:10,font:{{size:10}}}}}}}}, scales:{{x:{{stacked:true}},y:{{stacked:true,title:{{display:true,text:'Est. Cost ($)',color:'#8B949E'}}}}}}}}
    }});
  }}

  // Cost by Tag donut
  if(cbt.length){{
    // Group by product for the donut (since tags are mostly empty)
    const tagGroups = {{}};
    cbt.forEach(t=>{{
      const key = t.tagKey === 'Untagged' ? t.product : t.tagValue;
      tagGroups[key] = (tagGroups[key] || 0) + t.dbus;
    }});
    const entries = Object.entries(tagGroups).sort((a,b)=>b[1]-a[1]).slice(0,10);
    const tagChart = new Chart(document.getElementById('spend-tag-chart'),{{
      type:'doughnut',
      data:{{
        labels: entries.map(e=>e[0]),
        datasets:[{{data: entries.map(e=>Math.round(e[1]*price*100)/100), backgroundColor: COLORS.slice(0,entries.length)}}]
      }},
      options:{{...chartDef, cutout:'55%', plugins:{{
        legend:{{position:'right',labels:{{boxWidth:14,padding:14,font:{{size:13,weight:'500'}},
          generateLabels:function(chart){{
            const data=chart.data;
            const total=data.datasets[0].data.reduce((a,b)=>a+b,0);
            return data.labels.map((label,i)=>{{
              const val=data.datasets[0].data[i];
              const pct=total>0?Math.round(val/total*100):0;
              return {{text:label+' — $'+fmtN(val)+' ('+pct+'%)',fillStyle:data.datasets[0].backgroundColor[i],strokeStyle:data.datasets[0].backgroundColor[i],lineWidth:0,index:i}};
            }});
          }}
        }}}},
        tooltip:{{callbacks:{{label:function(ctx){{
          const total=ctx.dataset.data.reduce((a,b)=>a+b,0);
          const pct=total>0?Math.round(ctx.raw/total*100):0;
          return ctx.label+': $'+ctx.raw.toFixed(2)+' ('+pct+'%)';
        }}}}}}
      }}}}
    }});
    // Click donut segment -> Cost Explorer with filter
    document.getElementById('spend-tag-chart').addEventListener('click',(e)=>{{
      const pts=tagChart.getElementsAtEventForMode(e,'nearest',{{intersect:true}},false);
      if(pts.length){{
        const idx=pts[0].index;
        costExplorerCategory=entries[idx][0];
      }} else {{
        costExplorerCategory='';
      }}
      rendered['costexplorer']=false;
      goTab('costexplorer');
    }});
  }}

  // Breakdown table
  const tbody = document.querySelector('#spend-breakdown-table tbody');
  const catColors = {{'SQL':'#4B7BF5','JOBS':'#34D399','ALL_PURPOSE':'#FBBF24','DLT':'#A78BFA','APPS':'#00E5FF','INTERACTIVE':'#F87171','LAKEBASE':'#C084FC','PREDICTIVE_OPTIMIZATION':'#FB923C','DATABASE':'#818CF8','NETWORKING':'#94A3B8'}};
  cbp.forEach(c=>{{
    const pct = totalDbus > 0 ? Math.round(c.dbus / totalDbus * 100) : 0;
    const color = catColors[c.product] || '#8B949E';
    const cost = (c.dbus * price);
    const avgPerDay = c.days > 0 ? (c.dbus / c.days) : 0;
    tbody.innerHTML += `<tr>
      <td><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:${{color}}"></span></td>
      <td style="font-weight:600">${{c.product}}</td>
      <td class="num">${{fmtN(c.dbus)}}</td>
      <td class="num">${{pct}}%</td>
      <td class="num">$${{fmtN(cost)}}</td>
      <td class="num">${{c.days}}</td>
      <td class="num">${{fmtN(avgPerDay)}}</td>
    </tr>`;
  }});
  // Total row
  tbody.innerHTML += `<tr style="font-weight:700;border-top:2px solid var(--border)"><td></td><td>Total</td><td class="num">${{fmtN(totalDbus)}}</td><td class="num">100%</td><td class="num">$${{fmtN(totalCost)}}</td><td class="num">${{activeDays}}</td><td class="num">${{fmtN(totalDbus/activeDays)}}</td></tr>`;

  // Daily cost trend line
  if(dc.length){{
    new Chart(document.getElementById('spend-daily-chart'),{{
      type:'line',
      data:{{
        labels: dc.map(d=>d.date.substring(5)),
        datasets:[
          {{label:'SQL',data:dc.map(d=>Math.round(d.sql*price*100)/100),borderColor:'#4B7BF5',backgroundColor:'rgba(75,123,245,0.1)',fill:true,tension:.3,pointRadius:0}},
          {{label:'Jobs',data:dc.map(d=>Math.round(d.jobs*price*100)/100),borderColor:'#34D399',backgroundColor:'rgba(52,211,153,0.1)',fill:true,tension:.3,pointRadius:0}},
          {{label:'All-Purpose',data:dc.map(d=>Math.round(d.allPurpose*price*100)/100),borderColor:'#FBBF24',backgroundColor:'rgba(251,191,36,0.1)',fill:true,tension:.3,pointRadius:0}},
          {{label:'DLT',data:dc.map(d=>Math.round(d.dlt*price*100)/100),borderColor:'#A78BFA',backgroundColor:'rgba(167,139,250,0.1)',fill:true,tension:.3,pointRadius:0}},
          {{label:'Other',data:dc.map(d=>Math.round(d.other*price*100)/100),borderColor:'#8B949E',backgroundColor:'rgba(139,148,158,0.1)',fill:true,tension:.3,pointRadius:0}}
        ]
      }},
      options:{{...chartDef, plugins:{{legend:{{position:'bottom',labels:{{boxWidth:10,font:{{size:10}}}}}}}}, scales:{{y:{{title:{{display:true,text:'Est. Cost ($)',color:'#8B949E'}}}}}}}}
    }});
  }}
}}

// ---- Workflows ----
function renderWorkflows(){{
  const wf = D.workflows || [];
  const totalRuns = wf.reduce((s,w)=>s+w.totalRuns,0);
  const totalCost = wf.reduce((s,w)=>s+w.estCost,0);
  const totalDur = wf.reduce((s,w)=>s+w.totalDurationMin,0);
  document.getElementById('wf-total-jobs').textContent = wf.length;
  document.getElementById('wf-total-runs').textContent = fmtN(totalRuns);
  document.getElementById('wf-total-cost').textContent = '$' + fmtN(totalCost);
  const hrs = Math.floor(totalDur/60); const mins = Math.round(totalDur%60);
  document.getElementById('wf-total-duration').textContent = hrs + 'h ' + mins + 'm';

  function renderWfTable(data){{
    const tbody = document.querySelector('#wf-table tbody');
    tbody.innerHTML = '';
    data.forEach(w=>{{
      // Compute type badge
      const ctColors = {{'Job Compute':'#34D399','All-Purpose':'#FBBF24','Serverless':'#4B7BF5','DLT':'#A78BFA'}};
      const ctColor = ctColors[w.computeType] || '#8B949E';

      // Run status dots
      let statusHtml = '';
      for(let i=0;i<Math.min(w.succeeded,8);i++) statusHtml += '<span style="color:#34D399" title="Success">●</span>';
      for(let i=0;i<Math.min(w.failed,8);i++) statusHtml += '<span style="color:#F87171" title="Failed">●</span>';
      for(let i=0;i<Math.min(w.canceled,4);i++) statusHtml += '<span style="color:#FBBF24" title="Canceled">●</span>';

      // Duration formatting
      const durH = Math.floor(w.totalDurationMin/60);
      const durM = Math.round(w.totalDurationMin%60);
      const durStr = durH > 0 ? durH + 'h ' + durM + 'm' : durM + 'm';

      // Cost bar
      const maxCost = data.length ? data[0].estCost : 1;
      const costPct = maxCost > 0 ? Math.round(w.estCost / maxCost * 100) : 0;

      tbody.innerHTML += `<tr>
        <td style="font-weight:500;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{w.name}}">${{w.name}}</td>
        <td><span style="background:${{ctColor}}22;color:${{ctColor}};padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600">${{w.computeType}}</span></td>
        <td class="num">
          <div style="display:flex;align-items:center;gap:8px;justify-content:flex-end">
            <div style="width:80px;height:6px;background:var(--border);border-radius:3px;overflow:hidden"><div style="width:${{costPct}}%;height:100%;background:${{ctColor}};border-radius:3px"></div></div>
            <span>$${{w.estCost < 0.01 && w.estCost > 0 ? '<0.01' : w.estCost.toFixed(2)}}</span>
          </div>
        </td>
        <td style="font-size:16px;letter-spacing:2px">${{statusHtml}}</td>
        <td class="num">${{durStr}}</td>
        <td class="num">${{w.totalRuns}}</td>
      </tr>`;
    }});
    if(!data.length) tbody.innerHTML = '<tr><td colspan="6" style="color:var(--text2);text-align:center;padding:24px">No job run data available</td></tr>';
  }}

  renderWfTable(wf);

  // Sort
  document.getElementById('wf-sort').addEventListener('change',(e)=>{{
    const v = e.target.value;
    const sorted = [...wf];
    if(v==='cost') sorted.sort((a,b)=>b.estCost-a.estCost);
    else if(v==='runs') sorted.sort((a,b)=>b.totalRuns-a.totalRuns);
    else if(v==='duration') sorted.sort((a,b)=>b.totalDurationMin-a.totalDurationMin);
    else if(v==='name') sorted.sort((a,b)=>a.name.localeCompare(b.name));
    renderWfTable(sorted);
  }});

  // Search
  document.getElementById('wf-search').addEventListener('input',(e)=>{{
    const q = e.target.value.toLowerCase();
    const filtered = wf.filter(w=>w.name.toLowerCase().includes(q) || w.computeType.toLowerCase().includes(q));
    renderWfTable(filtered);
  }});
}}

// ---- Cost Explorer ----
function renderCostExplorer(preFilterCat){{
  const lines = D.billingLines || [];
  const price = D.avgDbuPrice || 0.07;
  const totalDbus = lines.reduce((s,l)=>s+l.dbus,0);
  const totalCost = totalDbus * price;
  const categories = [...new Set(lines.map(l=>l.product))].sort();

  // KPIs
  document.getElementById('ce-total-lines').textContent = lines.length;
  document.getElementById('ce-total-cost').textContent = '$' + fmtN(totalCost);
  document.getElementById('ce-total-dbus').textContent = fmtN(totalDbus);
  document.getElementById('ce-total-cats').textContent = categories.length;

  // Populate category filter dropdown
  const catSel = document.getElementById('ce-filter-cat');
  catSel.innerHTML = '<option value="">All Categories</option>';
  categories.forEach(c=>{{
    catSel.innerHTML += `<option value="${{c}}">${{c}}</option>`;
  }});

  // Pre-filter from donut click
  const initCat = costExplorerCategory || '';
  if(initCat){{
    // Match category: could be a product name or tag value
    const matchProduct = categories.find(c=>c===initCat);
    if(matchProduct){{
      catSel.value = matchProduct;
    }}
  }}

  const catColors = {{'SQL':'#4B7BF5','JOBS':'#34D399','ALL_PURPOSE':'#FBBF24','DLT':'#A78BFA','APPS':'#00E5FF','INTERACTIVE':'#F87171','LAKEBASE':'#C084FC','PREDICTIVE_OPTIMIZATION':'#FB923C','DATABASE':'#818CF8','NETWORKING':'#94A3B8','SERVERLESS_COMPUTE':'#38BDF8'}};

  function getResource(l){{
    if(l.jobId && l.jobId !== '0' && l.jobId !== '') return 'Job: ' + l.jobId;
    if(l.warehouseId) return 'WH: ' + l.warehouseId.substring(0,12);
    if(l.notebookId && l.notebookId !== '0' && l.notebookId !== '') return 'NB: ' + l.notebookId;
    return '\u2014';
  }}

  function renderCeTable(data){{
    const tbody = document.querySelector('#ce-table tbody');
    tbody.innerHTML = '';
    const maxCost = data.length ? data[0].dbus * price : 1;
    data.forEach(l=>{{
      const cost = l.dbus * price;
      const pct = totalDbus > 0 ? (l.dbus / totalDbus * 100).toFixed(1) : '0.0';
      const color = catColors[l.product] || '#8B949E';
      const costPct = maxCost > 0 ? Math.round(cost / maxCost * 100) : 0;
      const resource = getResource(l);
      tbody.innerHTML += `<tr>
        <td><span style="background:${{color}}22;color:${{color}};padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600">${{l.product}}</span></td>
        <td style="font-size:12px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{l.sku}}">${{l.sku}}</td>
        <td style="font-size:12px;color:var(--text2)">${{resource}}</td>
        <td class="num">
          <div style="display:flex;align-items:center;gap:8px;justify-content:flex-end">
            <div style="width:60px;height:5px;background:var(--border);border-radius:3px;overflow:hidden"><div style="width:${{costPct}}%;height:100%;background:${{color}};border-radius:3px"></div></div>
            <span>$${{cost<0.01&&cost>0?'<0.01':cost.toFixed(2)}}</span>
          </div>
        </td>
        <td class="num">${{fmtN(l.dbus)}}</td>
        <td class="num">${{pct}}%</td>
        <td class="num">${{l.activeDays}}</td>
        <td style="font-size:11px;color:var(--text2)">${{l.firstSeen}} → ${{l.lastSeen}}</td>
      </tr>`;
    }});
    if(!data.length) tbody.innerHTML = '<tr><td colspan="8" style="color:var(--text2);text-align:center;padding:24px">No billing line items available</td></tr>';
  }}

  function applyFilters(){{
    let data = [...lines];
    const cat = catSel.value;
    const q = document.getElementById('ce-search').value.toLowerCase();
    const sortBy = document.getElementById('ce-sort').value;
    if(cat) data = data.filter(l=>l.product===cat);
    if(q) data = data.filter(l=>l.sku.toLowerCase().includes(q) || l.product.toLowerCase().includes(q) || getResource(l).toLowerCase().includes(q));
    if(sortBy==='cost'||sortBy==='dbus') data.sort((a,b)=>b.dbus-a.dbus);
    else if(sortBy==='days') data.sort((a,b)=>b.activeDays-a.activeDays);
    else if(sortBy==='sku') data.sort((a,b)=>a.sku.localeCompare(b.sku));
    renderCeTable(data);
  }}

  applyFilters();
  catSel.addEventListener('change', applyFilters);
  document.getElementById('ce-sort').addEventListener('change', applyFilters);
  document.getElementById('ce-search').addEventListener('input', applyFilters);
}}

function renderCost(){{
  // Cost by User - horizontal bar
  const userCost = {{}};
  D._filteredUsers.forEach(u => {{
    const cost = u.durationMin * 0.07;
    userCost[u.user] = (userCost[u.user] || 0) + cost;
  }});
  const costEntries = Object.entries(userCost).sort((a,b) => b[1] - a[1]).slice(0, 12);
  if(costEntries.length) {{
    new Chart(document.getElementById('cost-by-user'), {{
      type: 'bar',
      data: {{
        labels: costEntries.map(e => shortN(e[0])),
        datasets: [{{
          label: 'Est. Cost ($)',
          data: costEntries.map(e => parseFloat(e[1].toFixed(2))),
          backgroundColor: COLORS.slice(0, costEntries.length),
          borderRadius: 4
        }}]
      }},
      options: {{
        ...chartDef,
        indexAxis: 'y',
        plugins: {{ legend: {{ display: false }} }},
        scales: {{ x: {{ beginAtZero: true, title: {{ display: true, text: 'Estimated Cost ($)', color: '#8B949E' }} }} }}
      }}
    }});
  }}

  // Click cost-by-user chart -> users tab
  document.getElementById('cost-by-user').onclick=()=>goTab('users');

  // Cost by Category - doughnut
  const sqlCost = {sql_cost};
  const appCost = {app_cost_est};
  const storageCost = {storage_cost};
  const modelCost = D.models.length * 2; // rough estimate
  const costCatChart=new Chart(document.getElementById('cost-category'), {{
    type: 'doughnut',
    data: {{
      labels: ['SQL Compute', 'Apps', 'Storage', 'Models'],
      datasets: [{{
        data: [sqlCost, appCost, storageCost, modelCost],
        backgroundColor: ['#4B7BF5', '#34D399', '#FBBF24', '#A78BFA']
      }}]
    }},
    options: {{
      ...chartDef,
      cutout: '55%',
      plugins: {{
        legend: {{ position: 'right', labels: {{ boxWidth: 10, font: {{ size: 11 }} }} }}
      }}
    }}
  }});
  // Click cost category segments -> relevant tab
  const costCatTabs=['infrastructure','apps','tables','apps'];
  document.getElementById('cost-category').addEventListener('click',(e)=>{{
    const pts=costCatChart.getElementsAtEventForMode(e,'nearest',{{intersect:true}},false);
    if(pts.length)goTab(costCatTabs[pts[0].index]);
  }});
}}

function renderApps(){{
  const c=document.getElementById('apps-cards');
  D.apps.forEach(a=>{{
    const sc=a.state==='ACTIVE'?'badge-green':'badge-gray';
    c.innerHTML+=`<div class="app-card"><h3>${{a.name}} <span class="badge ${{sc}}">${{a.state}}</span></h3><p>${{a.desc}}</p><dl class="app-meta"><div><dt>Compute</dt><dd>${{a.compute}}</dd></div><div><dt>Creator</dt><dd>${{a.creator}}</dd></div><div><dt>Created</dt><dd>${{a.created}}</dd></div><div><dt>Last Deploy</dt><dd>${{a.lastDeploy}}</dd></div></dl></div>`;
  }});
  if(!D.apps.length) c.innerHTML='<div style="color:var(--text2)">No apps found in this workspace.</div>';

  const top=[...D.models].sort((a,b)=>b.outputUsd-a.outputUsd).slice(0,12);
  if(top.length){{
    new Chart(document.getElementById('model-price'),{{type:'bar',data:{{labels:top.map(m=>m.display),datasets:[
      {{label:'Input ($/1M)',data:top.map(m=>m.inputUsd),backgroundColor:'#4B7BF5',borderRadius:3}},
      {{label:'Output ($/1M)',data:top.map(m=>m.outputUsd),backgroundColor:'#F87171',borderRadius:3}}
    ]}},options:{{...chartDef,indexAxis:'y',plugins:{{legend:{{position:'top',labels:{{boxWidth:12}}}}}},scales:{{x:{{beginAtZero:true,title:{{display:true,text:'$/1M tokens',color:'#8B949E'}}}}}}}}}});
  }}
  const tbody=document.querySelector('#models-table tbody');
  D.models.sort((a,b)=>b.outputDbu-a.outputDbu).forEach(m=>{{
    const caps=m.capabilities.map(c=>`<span class="pill">${{c.replace(/_/g,' ')}}</span>`).join('');
    tbody.innerHTML+=`<tr><td>${{m.display}}</td><td class="num">${{m.inputDbu.toFixed(2)}}</td><td class="num">${{m.outputDbu.toFixed(2)}}</td><td class="num">$${{m.inputUsd.toFixed(3)}}</td><td class="num">$${{m.outputUsd.toFixed(3)}}</td><td>${{caps}}</td></tr>`;
  }});
}}

function renderTables(){{
  const sized=D.sizedTables.slice(0,16);
  if(sized.length){{
    new Chart(document.getElementById('storage-chart'),{{type:'bar',data:{{labels:sized.map(t=>t.name.replace(/_/g,' ')),datasets:[{{label:'Size (MB)',data:sized.map(t=>(t.size/1048576).toFixed(2)),backgroundColor:sized.map((_,i)=>COLORS[i%COLORS.length]),borderRadius:4}}]}},options:{{...chartDef,indexAxis:'y',plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true,title:{{display:true,text:'MB',color:'#8B949E'}}}}}}}}}});
  }}
  // Click storage bar -> scroll to table list
  document.getElementById('storage-chart').onclick=()=>document.getElementById('tables-table').scrollIntoView({{behavior:'smooth'}});
  renderTableList('');
  document.getElementById('table-search').addEventListener('input',e=>renderTableList(e.target.value.toLowerCase()));
}}

function renderTableList(f){{
  const filtered=D.tables.filter(t=>!f||t.name.toLowerCase().includes(f)||(t.desc&&t.desc.toLowerCase().includes(f))||(t.schema&&t.schema.toLowerCase().includes(f)));
  document.getElementById('table-count').textContent=`Showing ${{filtered.length}} tables`;
  const tbody=document.querySelector('#tables-table tbody');
  tbody.innerHTML=filtered.map(t=>`<tr><td style="font-weight:500">${{t.name}}</td><td><span class="badge badge-blue">${{t.schema}}</span></td><td>${{t.type}}</td><td>${{t.created}}</td><td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{(t.desc||'').replace(/"/g,'&quot;')}}">${{t.desc||''}}</td><td class="num">${{fmtB(t.size)}}</td></tr>`).join('');
}}

function renderUsers(){{
  const users = D._filteredUsers;
  const agg={{}};users.forEach(u=>{{agg[u.user]=(agg[u.user]||0)+u.queries;}});
  const entries=Object.entries(agg).sort((a,b)=>b[1]-a[1]);
  new Chart(document.getElementById('user-donut'),{{type:'doughnut',data:{{labels:entries.map(e=>shortN(e[0])),datasets:[{{data:entries.map(e=>e[1]),backgroundColor:COLORS.slice(0,entries.length)}}]}},options:{{...chartDef,cutout:'55%',plugins:{{legend:{{position:'right',labels:{{boxWidth:10,font:{{size:10}}}}}}}}}}}});

  // Click donut -> scroll to user detail table
  document.getElementById('user-donut').onclick=()=>document.getElementById('users-table').scrollIntoView({{behavior:'smooth'}});

  const unames=[...new Set(users.map(u=>u.user))];
  const fin=unames.map(u=>(users.find(x=>x.user===u&&x.status==='FINISHED')||{{}}).queries||0);
  const fail=unames.map(u=>(users.find(x=>x.user===u&&x.status==='FAILED')||{{}}).queries||0);
  new Chart(document.getElementById('user-bar'),{{type:'bar',data:{{labels:unames.map(shortN),datasets:[
    {{label:'Finished',data:fin,backgroundColor:'rgba(52,211,153,.7)',borderRadius:3}},
    {{label:'Failed',data:fail,backgroundColor:'rgba(248,113,113,.7)',borderRadius:3}}
  ]}},options:{{...chartDef,indexAxis:'y',plugins:{{legend:{{position:'top',labels:{{boxWidth:12}}}}}},scales:{{x:{{stacked:true,beginAtZero:true}},y:{{stacked:true}}}}}}}});

  // Click user bar chart -> scroll to detail table
  document.getElementById('user-bar').onclick=()=>document.getElementById('users-table').scrollIntoView({{behavior:'smooth'}});

  const tbody=document.querySelector('#users-table tbody');
  tbody.innerHTML = '';
  users.forEach(u=>{{
    const sc=u.status==='FINISHED'?'badge-green':u.status==='FAILED'?'badge-red':'badge-amber';
    tbody.innerHTML+=`<tr><td style="font-weight:500">${{u.user}}</td><td><span class="badge ${{sc}}">${{u.status}}</span></td><td class="num">${{fmt(u.queries)}}</td><td class="num">${{u.dataGb.toFixed(4)}}</td><td class="num">${{fmt(u.rowsRead)}}</td><td class="num">${{u.durationMin.toFixed(2)}}</td></tr>`;
  }});
}}

function renderDaily(){{
  const daily = D._filteredDaily;
  const labels=daily.map(d=>d.date.slice(5));
  new Chart(document.getElementById('daily-stacked'),{{type:'bar',data:{{labels,datasets:[
    {{label:'Succeeded',data:daily.map(d=>d.succeeded),backgroundColor:'rgba(52,211,153,.7)',borderRadius:2}},
    {{label:'Failed',data:daily.map(d=>d.failed),backgroundColor:'rgba(248,113,113,.7)',borderRadius:2}}
  ]}},options:{{...chartDef,plugins:{{legend:{{position:'top',labels:{{boxWidth:12}}}}}},scales:{{x:{{stacked:true}},y:{{stacked:true,beginAtZero:true}}}}}}}});

  new Chart(document.getElementById('daily-dual'),{{type:'line',data:{{labels,datasets:[
    {{label:'Data Read (GB)',data:daily.map(d=>d.dataGb),borderColor:'#4B7BF5',backgroundColor:'rgba(75,123,245,.1)',fill:true,yAxisID:'y',tension:.3,pointRadius:3}},
    {{label:'Compute (min)',data:daily.map(d=>d.computeMin),borderColor:'#FBBF24',backgroundColor:'rgba(251,191,36,.1)',fill:true,yAxisID:'y1',tension:.3,pointRadius:3}}
  ]}},options:{{...chartDef,plugins:{{legend:{{position:'top',labels:{{boxWidth:12}}}}}},scales:{{y:{{type:'linear',position:'left',beginAtZero:true,title:{{display:true,text:'GB',color:'#8B949E'}}}},y1:{{type:'linear',position:'right',beginAtZero:true,title:{{display:true,text:'Minutes',color:'#8B949E'}},grid:{{drawOnChartArea:false}}}}}}}}}});

  // Click stacked/dual charts -> scroll to daily detail table
  document.getElementById('daily-stacked').onclick=()=>document.getElementById('daily-table').scrollIntoView({{behavior:'smooth'}});
  document.getElementById('daily-dual').onclick=()=>document.getElementById('daily-table').scrollIntoView({{behavior:'smooth'}});

  const hm=document.getElementById('heatmap');
  hm.innerHTML = '';
  daily.forEach(d=>{{hm.innerHTML+=`<div class="hm-cell ${{d.level.toLowerCase()}}" data-tip="${{d.date}}: ${{d.total}} queries">${{d.date.slice(8)}}</div>`;}});

  const tbody=document.querySelector('#daily-table tbody');
  tbody.innerHTML = '';
  daily.forEach(d=>{{
    const lc=d.level==='HIGH'?'badge-red':d.level==='MEDIUM'?'badge-amber':'badge-green';
    tbody.innerHTML+=`<tr><td>${{d.date}}</td><td class="num">${{fmt(d.total)}}</td><td class="num">${{fmt(d.succeeded)}}</td><td class="num">${{fmt(d.failed)}}</td><td class="num">${{d.dataGb.toFixed(4)}}</td><td class="num">${{d.computeMin.toFixed(2)}}</td><td><span class="badge ${{lc}}">${{d.level}}</span></td></tr>`;
  }});
}}

function renderPricing(){{
  const cats={{}};D.pricing.forEach(p=>{{
    let cat='Other';
    const s=p.sku.toUpperCase();
    if(s.includes('SQL'))cat='SQL';else if(s.includes('JOBS'))cat='Jobs';
    else if(s.includes('ALL_PURPOSE')&&!s.includes('SERVERLESS'))cat='All-Purpose';
    else if(s.includes('SERVERLESS'))cat='Serverless';else if(s.includes('DLT'))cat='DLT';
    else if(s.includes('MODEL')||s.includes('ANTHROPIC'))cat='Model Serving';
    else if(s.includes('SECURITY'))cat='Security';else if(s.includes('APPS'))cat='Apps';
    if(!cats[cat])cats[cat]=[];if(p.unit==='DBU')cats[cat].push(p.price);
  }});
  const cn=Object.keys(cats).filter(c=>cats[c].length>0);
  const ca=cn.map(c=>(cats[c].reduce((a,b)=>a+b,0)/cats[c].length).toFixed(3));
  const cm=cn.map(c=>Math.max(...cats[c]).toFixed(3));
  if(cn.length){{
    new Chart(document.getElementById('pricing-chart'),{{type:'bar',data:{{labels:cn,datasets:[
      {{label:'Avg $/DBU',data:ca,backgroundColor:'#4B7BF5',borderRadius:4}},
      {{label:'Max $/DBU',data:cm,backgroundColor:'rgba(75,123,245,.3)',borderRadius:4}}
    ]}},options:{{...chartDef,plugins:{{legend:{{position:'top',labels:{{boxWidth:12}}}}}},scales:{{y:{{beginAtZero:true,title:{{display:true,text:'$/DBU',color:'#8B949E'}}}}}}}}}});
  }}
  const tbody=document.querySelector('#pricing-table tbody');
  D.pricing.forEach(p=>{{tbody.innerHTML+=`<tr><td>${{p.sku.replace(/_/g,' ')}}</td><td class="num">$${{p.price.toFixed(3)}}</td><td>${{p.unit}}</td></tr>`;}});
}}

// Render first tab
renderTab('executive');

// Refresh button — polls /api/status until refresh completes
async function triggerRefresh(){{
  const btn=document.getElementById('refresh-btn');
  btn.disabled=true;btn.innerHTML='<i class="fas fa-spinner fa-spin"></i> Collecting data...';
  try{{
    const r=await fetch('/api/refresh',{{method:'POST'}});
    const d=await r.json();
    if(d.ok){{
      // Poll until refresh finishes
      const poll=async()=>{{
        try{{
          const s=await fetch('/api/status');
          const st=await s.json();
          if(st.is_refreshing){{
            btn.innerHTML='<i class="fas fa-spinner fa-spin"></i> Collecting data...';
            setTimeout(poll,3000);
          }}else if(st.last_error){{
            btn.innerHTML='<i class="fas fa-times"></i> Error';
            setTimeout(()=>{{btn.disabled=false;btn.innerHTML='<i class="fas fa-sync-alt"></i> Refresh';}},4000);
          }}else{{
            btn.innerHTML='<i class="fas fa-check"></i> Done! Reloading...';
            setTimeout(()=>location.reload(),500);
          }}
        }}catch(e){{setTimeout(poll,3000);}}
      }};
      setTimeout(poll,2000);
    }}else{{
      btn.innerHTML='<i class="fas fa-clock"></i> Already running...';
      setTimeout(()=>{{btn.disabled=false;btn.innerHTML='<i class="fas fa-sync-alt"></i> Refresh';}},3000);
    }}
  }}catch(e){{btn.innerHTML='<i class="fas fa-times"></i> Error';setTimeout(()=>{{btn.disabled=false;btn.innerHTML='<i class="fas fa-sync-alt"></i> Refresh';}},3000);}}
}}
</script>
</body>
</html>'''
