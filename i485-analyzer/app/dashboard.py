"""
I-485 Form Analyzer — Dashboard Renderer
Generates complete HTML with embedded CSS/JS following the Blueprint App Template.
"""
import json


def _rows(result):
    """Extract rows from a SQL result dict, defaulting to empty list."""
    if not result or not isinstance(result, dict):
        return []
    return result.get("rows", [])


def _val(result, r=0, c=0, default="0"):
    """Extract a single value from SQL result."""
    rows = _rows(result)
    if rows and len(rows) > r and len(rows[r]) > c:
        return rows[r][c]
    return default


def render_dashboard(data):
    """Return full HTML page for the I-485 Analyzer dashboard."""

    # ── Prep data for JS injection ────────────────────────────────────────
    def js_safe(obj):
        return json.dumps(obj, separators=(",", ":"), default=str)

    meta = data.get("_meta", {})

    def safe_int(v, default=0):
        try:
            return int(v) if v is not None else default
        except (ValueError, TypeError):
            return default

    # Status summary → [{status, count}]
    status_rows = [{"status": r[0] or "UNKNOWN", "count": safe_int(r[1])} for r in _rows(data.get("status_summary"))]
    total_apps = safe_int(_val(data.get("app_count"), default="0"))

    # Monthly → [{month, count}]
    monthly = [{"month": r[0], "count": safe_int(r[1])} for r in _rows(data.get("monthly"))]

    # Categories → [{group, count}]
    categories = [{"group": r[0] or "Unknown", "count": safe_int(r[1])} for r in _rows(data.get("categories"))]

    # States → [{state, count}]
    states = [{"state": r[0] or "?", "count": safe_int(r[1])} for r in _rows(data.get("states"))]

    # Countries → [{country, count}]
    countries = [{"country": r[0] or "?", "count": safe_int(r[1])} for r in _rows(data.get("countries"))]

    # Applications table
    app_cols = data.get("applications", {}).get("columns", [])
    app_rows = _rows(data.get("applications"))
    applications = [dict(zip(app_cols, r)) for r in app_rows] if app_cols else []

    # Table counts → {table: count}
    table_counts = {r[0]: safe_int(r[1]) for r in _rows(data.get("table_counts"))}

    # Missing fields → [{table, field, missing, total}]
    missing = [{"table": r[0], "field": r[1], "missing": safe_int(r[2]), "total": safe_int(r[3])}
               for r in _rows(data.get("missing_fields"))]

    # Duplicate A-numbers
    dup_a = [{"a_number": r[0], "count": safe_int(r[1])} for r in _rows(data.get("dup_anumbers"))]

    # Date errors
    date_err_cols = data.get("date_errors", {}).get("columns", [])
    date_errs = [dict(zip(date_err_cols, r)) for r in _rows(data.get("date_errors"))] if date_err_cols else []

    # Fraud: alert summary → [{risk_level, cnt, flags}]
    fraud_alert_summary = [{"level": r[0], "count": safe_int(r[1]), "flags": safe_int(r[2])}
                           for r in _rows(data.get("fraud_alert_summary"))]

    # Fraud: flag breakdown → [{rule_code, severity, cnt}]
    fraud_flag_summary = [{"rule": r[0], "severity": r[1], "count": safe_int(r[2])}
                          for r in _rows(data.get("fraud_flag_summary"))]

    # Fraud: top flagged applications
    elig_cols = data.get("eligibility_flags", {}).get("columns", [])
    elig_flags = [dict(zip(elig_cols, r)) for r in _rows(data.get("eligibility_flags"))] if elig_cols else []

    # Fraud: duplicate SSNs
    dup_ssn = [{"ssn": r[0], "count": safe_int(r[1])} for r in _rows(data.get("dup_ssns"))]

    # Fraud: duplicate identities
    dup_id_cols = data.get("dup_identity", {}).get("columns", [])
    dup_identity = [dict(zip(dup_id_cols, r)) for r in _rows(data.get("dup_identity"))] if dup_id_cols else []

    # Fraud: eligibility risk distribution
    elig_risk_dist = [{"tier": r[0], "count": safe_int(r[1])} for r in _rows(data.get("elig_risk_dist"))]

    # Fraud: financial anomalies
    fin_cols = data.get("financial_anomalies", {}).get("columns", [])
    fin_anomalies = [dict(zip(fin_cols, r)) for r in _rows(data.get("financial_anomalies"))] if fin_cols else []

    # Fraud: filing patterns
    filing_cols = data.get("filing_patterns", {}).get("columns", [])
    filing_patterns = [dict(zip(filing_cols, r)) for r in _rows(data.get("filing_patterns"))] if filing_cols else []

    # Fraud: same-day filings
    same_day = [{"date": r[0] or "?", "count": safe_int(r[1]), "detail": r[2] or ""} for r in _rows(data.get("same_day"))]

    # Fraud: address clusters
    addr_cl = [{"address": r[0] or "", "count": safe_int(r[1])} for r in _rows(data.get("addr_clusters"))]

    # Fraud: preparer concentration
    prep_c = [{"name": f"{r[0] or ''}, {r[1] or ''}", "count": safe_int(r[2])} for r in _rows(data.get("preparer_conc"))]

    # Compute derived KPIs
    approved = sum(s["count"] for s in status_rows if s["status"] == "APPROVED")
    denied = sum(s["count"] for s in status_rows if s["status"] == "DENIED")
    pending = sum(s["count"] for s in status_rows if s["status"] == "PENDING")
    received = sum(s["count"] for s in status_rows if s["status"] == "RECEIVED")
    approval_rate = round(approved / total_apps * 100, 1) if total_apps > 0 else 0

    # Data quality KPIs
    total_missing = sum(m["missing"] for m in missing)
    total_dups = len(dup_a)
    total_date_errs = len(date_errs)
    quality_issues = total_missing + total_dups + total_date_errs
    total_records = sum(table_counts.values())

    # Fraud KPIs (from pre-computed alerts)
    total_flagged = sum(a["count"] for a in fraud_alert_summary)
    critical_apps = sum(a["count"] for a in fraud_alert_summary if a["level"] == "CRITICAL")
    high_apps = sum(a["count"] for a in fraud_alert_summary if a["level"] == "HIGH")
    total_fraud_flags = sum(a["flags"] for a in fraud_alert_summary)
    addr_alerts = len(addr_cl)
    prep_alerts = len(prep_c)

    # Build JS data blob
    js_data = js_safe({
        "meta": meta,
        "totalApps": total_apps,
        "approvalRate": approval_rate,
        "approved": approved,
        "denied": denied,
        "pending": pending,
        "received": received,
        "statusRows": status_rows,
        "monthly": monthly,
        "categories": categories,
        "states": states,
        "countries": countries,
        "applications": applications,
        "tableCounts": table_counts,
        "missing": missing,
        "dupA": dup_a,
        "dateErrors": date_errs,
        "qualityIssues": quality_issues,
        "totalMissing": total_missing,
        "totalRecords": total_records,
        "addrClusters": addr_cl,
        "preparerConc": prep_c,
        "eligFlags": elig_flags,
        "dupSsn": dup_ssn,
        "sameDay": same_day,
        "totalFlagged": total_flagged,
        "criticalApps": critical_apps,
        "highApps": high_apps,
        "totalFraudFlags": total_fraud_flags,
        "addrAlerts": addr_alerts,
        "prepAlerts": prep_alerts,
        "fraudAlertSummary": fraud_alert_summary,
        "fraudFlagSummary": fraud_flag_summary,
        "dupIdentity": dup_identity,
        "eligRiskDist": elig_risk_dist,
        "finAnomalies": fin_anomalies,
        "filingPatterns": filing_patterns,
    })

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>I-485 Form Analyzer</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
:root{{
  --bg:#0D1117;--surface:#161B22;--elevated:#21262D;--sidebar:#1A1F2E;
  --border:#272D3F;--text:#E6EDF3;--text2:#8B949E;--text3:#484F58;
  --accent:#4B7BF5;--green:#34D399;--yellow:#FBBF24;--red:#F87171;
  --purple:#A78BFA;--cyan:#00E5FF;--orange:#FB923C;--pink:#F472B6;
}}
html,body{{height:100%;overflow:hidden;background:var(--bg);color:var(--text);
  font-family:'DM Sans','Inter',system-ui,sans-serif;font-size:14px;line-height:1.5;
  -webkit-font-smoothing:antialiased}}

/* ── Layout ── */
.layout{{display:flex;height:100vh}}
.sidebar{{width:220px;min-width:220px;background:var(--sidebar);border-right:1px solid var(--border);
  display:flex;flex-direction:column;overflow-y:auto}}
.sb-brand{{padding:16px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px}}
.sb-brand svg{{width:26px;height:26px;flex-shrink:0}}
.sb-brand span{{font-size:14px;font-weight:700;color:var(--text)}}
.sb-nav{{flex:1;padding:12px 0}}
.sb-group{{padding:16px 20px 6px;font-size:9px;font-weight:700;text-transform:uppercase;
  letter-spacing:1.2px;color:var(--text3)}}
.nav-item{{display:flex;align-items:center;gap:10px;width:100%;padding:10px 20px;border:none;
  background:none;color:var(--text2);font-size:13px;font-weight:500;cursor:pointer;
  text-align:left;transition:.15s;position:relative}}
.nav-item:hover{{color:var(--text);background:rgba(75,123,245,.08)}}
.nav-item.active{{color:var(--accent);background:rgba(75,123,245,.15)}}
.nav-item.active::before{{content:'';position:absolute;left:0;top:6px;bottom:6px;width:3px;
  background:var(--accent);border-radius:0 3px 3px 0}}
.nav-item i{{width:18px;text-align:center;font-size:14px}}
.sb-footer{{padding:16px 20px;border-top:1px solid var(--border);text-align:center}}
.sb-footer .brand{{font-size:16px;font-weight:700;color:#fff}}
.sb-footer .sub{{font-size:10px;color:var(--text3);margin-top:2px}}

.main-area{{flex:1;display:flex;flex-direction:column;overflow:hidden}}
.header{{height:52px;min-height:52px;background:var(--surface);border-bottom:1px solid var(--border);
  display:flex;align-items:center;justify-content:space-between;padding:0 24px}}
.header-left{{font-size:14px;font-weight:600}}
.header-right{{display:flex;align-items:center;gap:16px}}
.header-right .meta{{font-size:11px;color:var(--text3)}}
.btn{{padding:6px 14px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;
  border:1px solid var(--border);background:transparent;color:var(--text);transition:.15s}}
.btn:hover{{border-color:var(--accent);color:var(--accent)}}
.btn-primary{{background:var(--accent);border-color:var(--accent);color:#fff}}
.btn-primary:hover{{background:#3D6BE0}}

.content{{flex:1;overflow-y:auto;padding:24px}}
.tab{{display:none}}
.tab.active{{display:block}}

/* ── Cards & KPIs ── */
.section-title{{font-size:18px;font-weight:700;margin-bottom:16px}}
.kpi-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}}
.kpi{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px;
  position:relative;overflow:hidden;transition:.15s}}
.kpi::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px}}
.kpi.blue::before{{background:var(--accent)}}
.kpi.green::before{{background:var(--green)}}
.kpi.yellow::before{{background:var(--yellow)}}
.kpi.red::before{{background:var(--red)}}
.kpi.purple::before{{background:var(--purple)}}
.kpi.cyan::before{{background:var(--cyan)}}
.kpi.orange::before{{background:var(--orange)}}
.kpi:hover{{transform:translateY(-2px);border-color:var(--accent);box-shadow:0 4px 16px rgba(0,0,0,.3)}}
.kpi-val{{font-size:28px;font-weight:700;font-variant-numeric:tabular-nums}}
.kpi-label{{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;color:var(--text2);margin-top:4px}}

.card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;margin-bottom:24px}}
.card-header{{padding:14px 20px;border-bottom:1px solid var(--border);font-size:12px;font-weight:600;
  text-transform:uppercase;letter-spacing:.5px;color:var(--text2);display:flex;align-items:center;justify-content:space-between}}
.card-body{{padding:20px}}

.grid-2{{display:grid;grid-template-columns:repeat(2,1fr);gap:20px;margin-bottom:24px}}
.grid-3{{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:24px}}
.chart-wrap{{position:relative;height:280px}}

/* ── Tables ── */
.dtable{{width:100%;border-collapse:collapse}}
.dtable th{{padding:10px 14px;text-align:left;background:var(--elevated);font-size:11px;font-weight:600;
  text-transform:uppercase;letter-spacing:.5px;color:var(--text2);white-space:nowrap;
  border-bottom:1px solid var(--border)}}
.dtable td{{padding:9px 14px;font-size:13px;border-bottom:1px solid var(--border);white-space:nowrap}}
.dtable tr:hover td{{background:rgba(75,123,245,.08)}}
.dtable .num{{text-align:right;font-variant-numeric:tabular-nums}}

/* ── Badges ── */
.badge{{display:inline-block;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:600}}
.badge.green{{background:rgba(52,211,153,.12);color:var(--green)}}
.badge.red{{background:rgba(248,113,113,.12);color:var(--red)}}
.badge.yellow{{background:rgba(251,191,36,.12);color:var(--yellow)}}
.badge.blue{{background:rgba(75,123,245,.12);color:var(--accent)}}
.badge.purple{{background:rgba(167,139,250,.12);color:var(--purple)}}
.badge.cyan{{background:rgba(0,229,255,.12);color:var(--cyan)}}
.badge.orange{{background:rgba(251,146,60,.12);color:var(--orange)}}

/* ── Search ── */
.search-bar{{margin-bottom:16px;display:flex;gap:12px}}
.search-bar input{{flex:1;padding:10px 14px;background:var(--bg);border:1px solid var(--border);
  border-radius:8px;color:var(--text);font-size:13px;font-family:inherit}}
.search-bar input:focus{{outline:none;border-color:var(--accent)}}
.search-bar input::placeholder{{color:var(--text3)}}
.search-bar select{{padding:10px 14px;background:var(--bg);border:1px solid var(--border);
  border-radius:8px;color:var(--text);font-size:13px;font-family:inherit;cursor:pointer}}

/* ── Empty state ── */
.empty{{text-align:center;padding:60px 20px;color:var(--text3)}}
.empty i{{font-size:48px;margin-bottom:16px;display:block}}
.empty p{{font-size:14px;margin-top:8px}}

/* ── Fraud severity indicators ── */
.risk-high{{color:var(--red);font-weight:700}}
.risk-med{{color:var(--yellow);font-weight:600}}
.risk-low{{color:var(--green);font-weight:500}}

/* ── Scrollbar ── */
::-webkit-scrollbar{{width:6px;height:6px}}
::-webkit-scrollbar-track{{background:var(--bg)}}
::-webkit-scrollbar-thumb{{background:var(--border);border-radius:3px}}

/* ── Animations ── */
@keyframes fadeInUp{{from{{opacity:0;transform:translateY(12px)}}to{{opacity:1;transform:translateY(0)}}}}
.kpi,.card{{animation:fadeInUp .35s ease-out both}}
.kpi:nth-child(2),.card:nth-child(2){{animation-delay:.05s}}
.kpi:nth-child(3),.card:nth-child(3){{animation-delay:.1s}}
.kpi:nth-child(4),.card:nth-child(4){{animation-delay:.15s}}

/* ── Responsive ── */
@media(max-width:1200px){{
  .kpi-grid{{grid-template-columns:repeat(2,1fr)}}
  .grid-2{{grid-template-columns:1fr}}
}}
@media(max-width:768px){{
  .sidebar{{display:none}}
  .kpi-grid{{grid-template-columns:1fr}}
  .grid-2,.grid-3{{grid-template-columns:1fr}}
}}
</style>
</head>
<body>
<div class="layout">

<!-- ── Sidebar ── -->
<aside class="sidebar">
  <div class="sb-brand">
    <svg viewBox="0 0 32 32" fill="none"><rect width="32" height="32" rx="8" fill="#4B7BF5"/>
    <path d="M8 16h5l3-6 4 12 3-6h5" stroke="#fff" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
    <span>I-485 Analyzer</span>
  </div>
  <nav class="sb-nav">
    <div class="sb-group">Overview</div>
    <button class="nav-item active" data-tab="exec"><i class="fas fa-chart-line"></i> Executive Dashboard</button>
    <button class="nav-item" data-tab="detail"><i class="fas fa-table"></i> Application Explorer</button>
    <div class="sb-group">Analysis</div>
    <button class="nav-item" data-tab="quality"><i class="fas fa-exclamation-triangle"></i> Data Quality</button>
    <button class="nav-item" data-tab="fraud"><i class="fas fa-shield-halved"></i> Fraud &amp; Patterns</button>
  </nav>
  <div class="sb-footer">
    <div class="brand">Blueprint</div>
    <div class="sub">I-485 Form Analytics</div>
  </div>
</aside>

<!-- ── Main Area ── -->
<div class="main-area">
  <header class="header">
    <div class="header-left">I-485 Form Analyzer</div>
    <div class="header-right">
      <span class="meta"><i class="fas fa-database"></i> {meta.get("schema","")}</span>
      <span class="meta" id="loaded-at"></span>
      <button class="btn" onclick="refreshData()"><i class="fas fa-sync-alt"></i> Refresh</button>
    </div>
  </header>

  <div class="content">

    <!-- ════════════════════════════════════════════════════════════════════ -->
    <!-- TAB: Executive Dashboard                                           -->
    <!-- ════════════════════════════════════════════════════════════════════ -->
    <section class="tab active" id="tab-exec">
      <div class="section-title">Executive Dashboard</div>

      <div class="kpi-grid">
        <div class="kpi blue">
          <div class="kpi-val" id="kpi-total">0</div>
          <div class="kpi-label">Total Applications</div>
        </div>
        <div class="kpi green">
          <div class="kpi-val" id="kpi-approved">0</div>
          <div class="kpi-label">Approved</div>
        </div>
        <div class="kpi yellow">
          <div class="kpi-val" id="kpi-pending">0</div>
          <div class="kpi-label">Pending Review</div>
        </div>
        <div class="kpi red">
          <div class="kpi-val" id="kpi-denied">0</div>
          <div class="kpi-label">Denied</div>
        </div>
      </div>

      <div class="kpi-grid">
        <div class="kpi cyan">
          <div class="kpi-val" id="kpi-rate">0%</div>
          <div class="kpi-label">Approval Rate</div>
        </div>
        <div class="kpi purple">
          <div class="kpi-val" id="kpi-categories">0</div>
          <div class="kpi-label">Active Categories</div>
        </div>
        <div class="kpi orange">
          <div class="kpi-val" id="kpi-states">0</div>
          <div class="kpi-label">States Represented</div>
        </div>
        <div class="kpi blue">
          <div class="kpi-val" id="kpi-received">0</div>
          <div class="kpi-label">Received / In Queue</div>
        </div>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-header">Application Status Distribution</div>
          <div class="card-body"><div class="chart-wrap"><canvas id="chart-status"></canvas></div></div>
        </div>
        <div class="card">
          <div class="card-header">Monthly Filing Volume</div>
          <div class="card-body"><div class="chart-wrap"><canvas id="chart-monthly"></canvas></div></div>
        </div>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-header">Filing Category Groups</div>
          <div class="card-body"><div class="chart-wrap"><canvas id="chart-categories"></canvas></div></div>
        </div>
        <div class="card">
          <div class="card-header">Top States by Application Volume</div>
          <div class="card-body"><div class="chart-wrap"><canvas id="chart-states"></canvas></div></div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">Top Countries of Citizenship</div>
        <div class="card-body"><div class="chart-wrap" style="height:300px"><canvas id="chart-countries"></canvas></div></div>
      </div>
    </section>

    <!-- ════════════════════════════════════════════════════════════════════ -->
    <!-- TAB: Application Explorer                                          -->
    <!-- ════════════════════════════════════════════════════════════════════ -->
    <section class="tab" id="tab-detail">
      <div class="section-title">Application Explorer</div>

      <div class="search-bar">
        <input type="text" id="app-search" placeholder="Search by name, A-number, receipt number…">
        <select id="app-status-filter">
          <option value="">All Statuses</option>
          <option value="RECEIVED">Received</option>
          <option value="PENDING">Pending</option>
          <option value="APPROVED">Approved</option>
          <option value="DENIED">Denied</option>
          <option value="WITHDRAWN">Withdrawn</option>
        </select>
        <select id="app-cat-filter">
          <option value="">All Categories</option>
        </select>
      </div>

      <div class="card">
        <div class="card-header">
          <span>Applications</span>
          <span id="app-count-label" style="color:var(--text3)">0 records</span>
        </div>
        <div class="card-body" style="padding:0;overflow-x:auto">
          <table class="dtable" id="app-table">
            <thead><tr>
              <th>ID</th><th>A-Number</th><th>Receipt #</th><th>Name</th>
              <th>Status</th><th>Category</th><th>Filed</th>
              <th>Citizenship</th><th>State</th><th>City</th>
            </tr></thead>
            <tbody id="app-tbody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <!-- ════════════════════════════════════════════════════════════════════ -->
    <!-- TAB: Data Quality                                                  -->
    <!-- ════════════════════════════════════════════════════════════════════ -->
    <section class="tab" id="tab-quality">
      <div class="section-title">Data Quality &amp; Anomalies</div>

      <div class="kpi-grid">
        <div class="kpi red">
          <div class="kpi-val" id="kpi-q-issues">0</div>
          <div class="kpi-label">Total Issues</div>
        </div>
        <div class="kpi yellow">
          <div class="kpi-val" id="kpi-q-missing">0</div>
          <div class="kpi-label">Missing Required Fields</div>
        </div>
        <div class="kpi orange">
          <div class="kpi-val" id="kpi-q-dups">0</div>
          <div class="kpi-label">Duplicate A-Numbers</div>
        </div>
        <div class="kpi purple">
          <div class="kpi-val" id="kpi-q-dates">0</div>
          <div class="kpi-label">Date Validation Errors</div>
        </div>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-header">Table Record Counts</div>
          <div class="card-body"><div class="chart-wrap"><canvas id="chart-tables"></canvas></div></div>
        </div>
        <div class="card">
          <div class="card-header">Missing Required Fields</div>
          <div class="card-body"><div class="chart-wrap"><canvas id="chart-missing"></canvas></div></div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">Missing Field Details</div>
        <div class="card-body" style="padding:0;overflow-x:auto">
          <table class="dtable">
            <thead><tr><th>Table</th><th>Field</th><th class="num">Missing</th><th class="num">Total</th><th class="num">% Missing</th></tr></thead>
            <tbody id="missing-tbody"></tbody>
          </table>
        </div>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-header">Duplicate A-Numbers</div>
          <div class="card-body" style="padding:0;overflow-x:auto">
            <table class="dtable">
              <thead><tr><th>A-Number</th><th class="num">Occurrences</th></tr></thead>
              <tbody id="dups-tbody"></tbody>
            </table>
          </div>
        </div>
        <div class="card">
          <div class="card-header">Date Validation Errors</div>
          <div class="card-body" style="padding:0;overflow-x:auto">
            <table class="dtable">
              <thead><tr><th>App ID</th><th>Name</th><th>Issue</th></tr></thead>
              <tbody id="dates-tbody"></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <!-- ════════════════════════════════════════════════════════════════════ -->
    <!-- TAB: Fraud & Patterns                                              -->
    <!-- ════════════════════════════════════════════════════════════════════ -->
    <section class="tab" id="tab-fraud">
      <div class="section-title">Fraud Detection &amp; Pattern Analysis</div>

      <div class="kpi-grid">
        <div class="kpi red">
          <div class="kpi-val" id="kpi-f-flagged">0</div>
          <div class="kpi-label">Flagged Applications</div>
        </div>
        <div class="kpi orange">
          <div class="kpi-val" id="kpi-f-critical">0</div>
          <div class="kpi-label">Critical Risk</div>
        </div>
        <div class="kpi yellow">
          <div class="kpi-val" id="kpi-f-high">0</div>
          <div class="kpi-label">High Risk</div>
        </div>
        <div class="kpi purple">
          <div class="kpi-val" id="kpi-f-flags">0</div>
          <div class="kpi-label">Total Fraud Flags</div>
        </div>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-header"><i class="fas fa-chart-pie" style="color:var(--red);margin-right:8px"></i> Risk Level Distribution</div>
          <div class="card-body"><div class="chart-wrap"><canvas id="chart-risk-dist"></canvas></div></div>
        </div>
        <div class="card">
          <div class="card-header"><i class="fas fa-chart-bar" style="color:var(--orange);margin-right:8px"></i> Flag Breakdown by Rule</div>
          <div class="card-body"><div class="chart-wrap"><canvas id="chart-flag-rules"></canvas></div></div>
        </div>
      </div>

      <div class="card">
        <div class="card-header"><i class="fas fa-exclamation-circle" style="color:var(--red);margin-right:8px"></i> Top Flagged Applications
          <span id="elig-count-label" style="color:var(--text3);font-size:11px"></span></div>
        <div class="card-body" style="padding:0;overflow-x:auto">
          <table class="dtable">
            <thead><tr><th>App ID</th><th>Name</th><th class="num">Risk Score</th><th>Risk Level</th><th class="num">Total Flags</th><th class="num">Critical</th><th class="num">Warnings</th><th>Flag Categories</th></tr></thead>
            <tbody id="elig-tbody"></tbody>
          </table>
        </div>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-header"><i class="fas fa-fingerprint" style="color:var(--cyan);margin-right:8px"></i> Duplicate Identity Matches</div>
          <div class="card-body" style="padding:0;overflow-x:auto">
            <table class="dtable">
              <thead><tr><th>Match Type</th><th>Matched Value</th><th>Confidence</th><th>App ID 1</th><th>App ID 2</th></tr></thead>
              <tbody id="dupid-tbody"></tbody>
            </table>
          </div>
        </div>
        <div class="card">
          <div class="card-header"><i class="fas fa-dollar-sign" style="color:var(--green);margin-right:8px"></i> Financial Anomalies</div>
          <div class="card-body" style="padding:0;overflow-x:auto">
            <table class="dtable">
              <thead><tr><th>App ID</th><th>Name</th><th>Anomaly</th><th class="num">Income</th><th class="num">Assets</th><th class="num">Liabilities</th></tr></thead>
              <tbody id="fin-tbody"></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-header"><i class="fas fa-id-card" style="color:var(--orange);margin-right:8px"></i> Duplicate SSNs</div>
          <div class="card-body" style="padding:0;overflow-x:auto">
            <table class="dtable">
              <thead><tr><th>SSN (Masked)</th><th class="num">Occurrences</th></tr></thead>
              <tbody id="ssn-tbody"></tbody>
            </table>
          </div>
        </div>
        <div class="card">
          <div class="card-header"><i class="fas fa-calendar-day" style="color:var(--cyan);margin-right:8px"></i> Same-Day Filing Bursts</div>
          <div class="card-body" style="padding:0;overflow-x:auto">
            <table class="dtable">
              <thead><tr><th>Filing Date</th><th class="num">Applications</th><th>Detail</th></tr></thead>
              <tbody id="sameday-tbody"></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-header"><i class="fas fa-map-marker-alt" style="color:var(--yellow);margin-right:8px"></i> Address Clustering</div>
          <div class="card-body" style="padding:0;overflow-x:auto">
            <table class="dtable">
              <thead><tr><th>Address</th><th class="num">Applications</th></tr></thead>
              <tbody id="addr-tbody"></tbody>
            </table>
          </div>
        </div>
        <div class="card">
          <div class="card-header"><i class="fas fa-user-tie" style="color:var(--purple);margin-right:8px"></i> Preparer Concentration (4+ Apps)</div>
          <div class="card-body" style="padding:0;overflow-x:auto">
            <table class="dtable">
              <thead><tr><th>Preparer</th><th class="num">Applications</th></tr></thead>
              <tbody id="prep-tbody"></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-header"><i class="fas fa-project-diagram" style="color:var(--pink);margin-right:8px"></i> Filing Patterns</div>
          <div class="card-body" style="padding:0;overflow-x:auto">
            <table class="dtable">
              <thead><tr><th>Pattern Type</th><th class="num">App Count</th><th>Detail</th><th>Filing Date</th></tr></thead>
              <tbody id="pattern-tbody"></tbody>
            </table>
          </div>
        </div>
        <div class="card">
          <div class="card-header"><i class="fas fa-shield-halved" style="color:var(--accent);margin-right:8px"></i> Eligibility Risk Distribution</div>
          <div class="card-body"><div class="chart-wrap"><canvas id="chart-elig-risk"></canvas></div></div>
        </div>
      </div>
    </section>

  </div><!-- /content -->
</div><!-- /main-area -->
</div><!-- /layout -->

<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script>
// ── Data ─────────────────────────────────────────────────────────────────────
const D = {js_data};

// ── Chart defaults ───────────────────────────────────────────────────────────
Chart.defaults.color = '#8B949E';
Chart.defaults.borderColor = '#272D3F';
Chart.defaults.font.family = "'DM Sans','Inter',system-ui,sans-serif";
const COLORS = ['#4B7BF5','#34D399','#FBBF24','#A78BFA','#F87171','#00E5FF','#F472B6','#2DD4BF','#FB923C','#818CF8'];

// ── Helpers ──────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const fmt = n => n === null || n === undefined ? '—' : Number(n).toLocaleString();
const pct = (a, b) => b > 0 ? (a / b * 100).toFixed(1) + '%' : '0%';

function statusBadge(s) {{
  const m = {{APPROVED:'green',DENIED:'red',PENDING:'yellow',RECEIVED:'blue',WITHDRAWN:'purple'}};
  return `<span class="badge ${{m[s]||'blue'}}">${{s||'—'}}</span>`;
}}
function riskBadge(crit, warn) {{
  if (crit > 2) return '<span class="badge red">Critical</span>';
  if (crit > 0) return '<span class="badge orange">High</span>';
  if (warn > 2) return '<span class="badge yellow">Medium</span>';
  return '<span class="badge blue">Low</span>';
}}
function emptyRow(cols, msg) {{
  return `<tr><td colspan="${{cols}}" style="text-align:center;color:var(--text3);padding:40px">${{msg||'No data available'}}</td></tr>`;
}}

// ── Tab Management ───────────────────────────────────────────────────────────
const rendered = {{}};
let activeTab = 'exec';

document.querySelectorAll('.nav-item').forEach(btn => {{
  btn.addEventListener('click', () => {{
    document.querySelectorAll('.nav-item').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const tab = btn.dataset.tab;
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    $('tab-' + tab).classList.add('active');
    activeTab = tab;
    renderTab(tab);
  }});
}});

function renderTab(id) {{
  if (id === 'exec') renderExec();
  else if (id === 'detail') renderDetail();
  else if (id === 'quality') renderQuality();
  else if (id === 'fraud') renderFraud();
}}

// ── Executive Dashboard ──────────────────────────────────────────────────────
function renderExec() {{
  if (rendered.exec) return;
  rendered.exec = true;

  $('kpi-total').textContent = fmt(D.totalApps);
  $('kpi-approved').textContent = fmt(D.approved);
  $('kpi-pending').textContent = fmt(D.pending);
  $('kpi-denied').textContent = fmt(D.denied);
  $('kpi-rate').textContent = D.approvalRate + '%';
  $('kpi-categories').textContent = fmt(D.categories.length);
  $('kpi-states').textContent = fmt(D.states.length);
  $('kpi-received').textContent = fmt(D.received);

  // Status doughnut
  if (D.statusRows.length) {{
    new Chart($('chart-status'), {{
      type: 'doughnut',
      data: {{
        labels: D.statusRows.map(r => r.status),
        datasets: [{{data: D.statusRows.map(r => r.count), backgroundColor: COLORS, borderWidth: 0}}]
      }},
      options: {{responsive:true, maintainAspectRatio:false,
        plugins:{{legend:{{position:'right',labels:{{color:'#E6EDF3',padding:12}}}}}},
        cutout:'60%'}}
    }});
  }}

  // Monthly bar
  if (D.monthly.length) {{
    new Chart($('chart-monthly'), {{
      type: 'bar',
      data: {{
        labels: D.monthly.map(r => r.month),
        datasets: [{{label:'Applications', data: D.monthly.map(r => r.count),
          backgroundColor:'#4B7BF5', borderRadius:3}}]
      }},
      options: {{responsive:true, maintainAspectRatio:false,
        plugins:{{legend:{{display:false}}}},
        scales:{{y:{{beginAtZero:true,grid:{{color:'#272D3F'}}}},x:{{grid:{{display:false}}}}}}}}
    }});
  }}

  // Category groups
  if (D.categories.length) {{
    new Chart($('chart-categories'), {{
      type: 'doughnut',
      data: {{
        labels: D.categories.map(r => r.group),
        datasets: [{{data: D.categories.map(r => r.count), backgroundColor: COLORS, borderWidth:0}}]
      }},
      options: {{responsive:true, maintainAspectRatio:false,
        plugins:{{legend:{{position:'right',labels:{{color:'#E6EDF3',padding:10}}}}}},
        cutout:'60%'}}
    }});
  }}

  // Top states
  if (D.states.length) {{
    new Chart($('chart-states'), {{
      type: 'bar',
      data: {{
        labels: D.states.map(r => r.state),
        datasets: [{{label:'Applications', data: D.states.map(r => r.count),
          backgroundColor:'#34D399', borderRadius:3}}]
      }},
      options: {{responsive:true, maintainAspectRatio:false, indexAxis:'y',
        plugins:{{legend:{{display:false}}}},
        scales:{{x:{{beginAtZero:true,grid:{{color:'#272D3F'}}}},y:{{grid:{{display:false}}}}}}}}
    }});
  }}

  // Countries
  if (D.countries.length) {{
    new Chart($('chart-countries'), {{
      type: 'bar',
      data: {{
        labels: D.countries.map(r => r.country),
        datasets: [{{label:'Applicants', data: D.countries.map(r => r.count),
          backgroundColor:'#A78BFA', borderRadius:3}}]
      }},
      options: {{responsive:true, maintainAspectRatio:false, indexAxis:'y',
        plugins:{{legend:{{display:false}}}},
        scales:{{x:{{beginAtZero:true,grid:{{color:'#272D3F'}}}},y:{{grid:{{display:false}}}}}}}}
    }});
  }}
}}

// ── Application Explorer ─────────────────────────────────────────────────────
let appData = [];
function renderDetail() {{
  if (rendered.detail) return;
  rendered.detail = true;
  appData = D.applications;

  // Populate category filter
  const cats = [...new Set(D.applications.map(a => a.category_group).filter(Boolean))].sort();
  const sel = $('app-cat-filter');
  cats.forEach(c => {{ const o = document.createElement('option'); o.value = c; o.textContent = c; sel.appendChild(o); }});

  renderAppTable(appData);

  $('app-search').addEventListener('input', filterApps);
  $('app-status-filter').addEventListener('change', filterApps);
  $('app-cat-filter').addEventListener('change', filterApps);
}}

function filterApps() {{
  const q = $('app-search').value.toLowerCase();
  const st = $('app-status-filter').value;
  const cat = $('app-cat-filter').value;
  let filtered = D.applications;
  if (q) filtered = filtered.filter(a =>
    (a.family_name||'').toLowerCase().includes(q) ||
    (a.given_name||'').toLowerCase().includes(q) ||
    (a.a_number||'').toLowerCase().includes(q) ||
    (a.receipt_number||'').toLowerCase().includes(q) ||
    String(a.application_id).includes(q)
  );
  if (st) filtered = filtered.filter(a => a.status === st);
  if (cat) filtered = filtered.filter(a => a.category_group === cat);
  renderAppTable(filtered);
}}

function renderAppTable(rows) {{
  const tb = $('app-tbody');
  $('app-count-label').textContent = rows.length + ' records';
  if (!rows.length) {{ tb.innerHTML = emptyRow(10, 'No applications found'); return; }}
  tb.innerHTML = rows.slice(0, 200).map(a => `<tr>
    <td>${{a.application_id||'—'}}</td>
    <td>${{a.a_number||'—'}}</td>
    <td>${{a.receipt_number||'—'}}</td>
    <td>${{(a.family_name||'')}}, ${{(a.given_name||'')}}</td>
    <td>${{statusBadge(a.status)}}</td>
    <td><span class="badge blue">${{a.category_code||a.category_group||'—'}}</span></td>
    <td>${{a.filing_date||'—'}}</td>
    <td>${{a.country_of_citizenship||'—'}}</td>
    <td>${{a.res_state||'—'}}</td>
    <td>${{a.res_city||'—'}}</td>
  </tr>`).join('');
}}

// ── Data Quality ─────────────────────────────────────────────────────────────
function renderQuality() {{
  if (rendered.quality) return;
  rendered.quality = true;

  $('kpi-q-issues').textContent = fmt(D.qualityIssues);
  $('kpi-q-missing').textContent = fmt(D.totalMissing);
  $('kpi-q-dups').textContent = fmt(D.dupA.length);
  $('kpi-q-dates').textContent = fmt(D.dateErrors.length);

  // Table counts chart
  const tc = Object.entries(D.tableCounts).sort((a,b) => b[1]-a[1]);
  if (tc.length) {{
    new Chart($('chart-tables'), {{
      type: 'bar',
      data: {{
        labels: tc.map(t => t[0]),
        datasets: [{{label:'Rows', data: tc.map(t => t[1]), backgroundColor:'#4B7BF5', borderRadius:3}}]
      }},
      options: {{responsive:true, maintainAspectRatio:false, indexAxis:'y',
        plugins:{{legend:{{display:false}}}},
        scales:{{x:{{beginAtZero:true,grid:{{color:'#272D3F'}}}},y:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}}}}}}
    }});
  }}

  // Missing fields chart
  const mf = D.missing.filter(m => m.missing > 0);
  if (mf.length) {{
    new Chart($('chart-missing'), {{
      type: 'bar',
      data: {{
        labels: mf.map(m => m.table + '.' + m.field),
        datasets: [{{label:'Missing', data: mf.map(m => m.missing), backgroundColor:'#F87171', borderRadius:3}}]
      }},
      options: {{responsive:true, maintainAspectRatio:false, indexAxis:'y',
        plugins:{{legend:{{display:false}}}},
        scales:{{x:{{beginAtZero:true,grid:{{color:'#272D3F'}}}},y:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}}}}}}
    }});
  }}

  // Missing fields table
  const mtb = $('missing-tbody');
  if (!D.missing.length) {{ mtb.innerHTML = emptyRow(5); }}
  else {{ mtb.innerHTML = D.missing.map(m => `<tr>
    <td>${{m.table}}</td><td>${{m.field}}</td>
    <td class="num">${{fmt(m.missing)}}</td><td class="num">${{fmt(m.total)}}</td>
    <td class="num">${{m.total > 0 ? (m.missing/m.total*100).toFixed(1)+'%' : '—'}}</td>
  </tr>`).join(''); }}

  // Duplicates table
  const dtb = $('dups-tbody');
  if (!D.dupA.length) {{ dtb.innerHTML = emptyRow(2, 'No duplicate A-numbers found'); }}
  else {{ dtb.innerHTML = D.dupA.map(d => `<tr>
    <td>${{d.a_number}}</td><td class="num"><span class="badge red">${{d.count}}</span></td>
  </tr>`).join(''); }}

  // Date errors table
  const detb = $('dates-tbody');
  if (!D.dateErrors.length) {{ detb.innerHTML = emptyRow(3, 'No date errors found'); }}
  else {{ detb.innerHTML = D.dateErrors.map(d => `<tr>
    <td>${{d.application_id}}</td><td>${{(d.family_name||'')}}, ${{(d.given_name||'')}}</td>
    <td><span class="badge orange">${{d.issue}}</span></td>
  </tr>`).join(''); }}
}}

// ── Fraud & Patterns ─────────────────────────────────────────────────────────
const RISK_COLORS = {{'CRITICAL':'#F87171','HIGH':'#FB923C','MEDIUM':'#FBBF24','LOW':'#34D399'}};
const SEV_COLORS = {{'CRITICAL':'#F87171','HIGH':'#FB923C','MEDIUM':'#FBBF24','LOW':'#34D399','INFO':'#4B7BF5'}};

function riskLevelBadge(level) {{
  const m = {{'CRITICAL':'red','HIGH':'orange','MEDIUM':'yellow','LOW':'green'}};
  return `<span class="badge ${{m[level]||'blue'}}">${{level||'—'}}</span>`;
}}

function renderFraud() {{
  if (rendered.fraud) return;
  rendered.fraud = true;

  // KPIs
  $('kpi-f-flagged').textContent = fmt(D.totalFlagged);
  $('kpi-f-critical').textContent = fmt(D.criticalApps);
  $('kpi-f-high').textContent = fmt(D.highApps);
  $('kpi-f-flags').textContent = fmt(D.totalFraudFlags);

  // ── Risk Level Distribution (doughnut) ──
  if (D.fraudAlertSummary.length) {{
    new Chart($('chart-risk-dist'), {{
      type: 'doughnut',
      data: {{
        labels: D.fraudAlertSummary.map(r => r.level),
        datasets: [{{
          data: D.fraudAlertSummary.map(r => r.count),
          backgroundColor: D.fraudAlertSummary.map(r => RISK_COLORS[r.level] || '#4B7BF5'),
          borderWidth: 0
        }}]
      }},
      options: {{responsive:true, maintainAspectRatio:false,
        plugins:{{legend:{{position:'right',labels:{{color:'#E6EDF3',padding:12}}}}}},
        cutout:'60%'}}
    }});
  }}

  // ── Flag Breakdown by Rule (horizontal bar) ──
  if (D.fraudFlagSummary.length) {{
    new Chart($('chart-flag-rules'), {{
      type: 'bar',
      data: {{
        labels: D.fraudFlagSummary.map(r => r.rule),
        datasets: [{{
          label: 'Flags',
          data: D.fraudFlagSummary.map(r => r.count),
          backgroundColor: D.fraudFlagSummary.map(r => SEV_COLORS[r.severity] || '#4B7BF5'),
          borderRadius: 3
        }}]
      }},
      options: {{responsive:true, maintainAspectRatio:false, indexAxis:'y',
        plugins:{{legend:{{display:false}}}},
        scales:{{x:{{beginAtZero:true,grid:{{color:'#272D3F'}}}},y:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}}}}}}
    }});
  }}

  // ── Top Flagged Applications table ──
  const etb = $('elig-tbody');
  if (!D.eligFlags.length) {{ etb.innerHTML = emptyRow(8, 'No flagged applications — run the fraud detection notebook'); }}
  else {{
    $('elig-count-label').textContent = D.eligFlags.length + ' applications';
    etb.innerHTML = D.eligFlags.slice(0, 100).map(e => `<tr>
      <td>${{e.application_id}}</td>
      <td>${{(e.family_name||'')}}, ${{(e.given_name||'')}}</td>
      <td class="num" style="font-weight:700;color:${{Number(e.risk_score)>=70?'var(--red)':Number(e.risk_score)>=40?'var(--orange)':'var(--yellow)'}}">${{e.risk_score}}</td>
      <td>${{riskLevelBadge(e.risk_level)}}</td>
      <td class="num">${{e.total_flags||0}}</td>
      <td class="num">${{Number(e.critical_flags)>0?'<span class="risk-high">'+e.critical_flags+'</span>':'0'}}</td>
      <td class="num">${{Number(e.warning_flags)>0?'<span class="risk-med">'+e.warning_flags+'</span>':'0'}}</td>
      <td style="font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis">${{e.flag_categories||'—'}}</td>
    </tr>`).join('');
  }}

  // ── Duplicate Identity Matches ──
  const ditb = $('dupid-tbody');
  if (!D.dupIdentity.length) {{ ditb.innerHTML = emptyRow(5, 'No duplicate identity matches'); }}
  else {{ ditb.innerHTML = D.dupIdentity.slice(0, 100).map(d => `<tr>
    <td><span class="badge ${{d.match_type==='SSN'?'red':d.match_type==='A_NUMBER'?'orange':'yellow'}}">${{d.match_type}}</span></td>
    <td>${{d.matched_value||'—'}}</td>
    <td>${{d.confidence||'—'}}</td>
    <td>${{d.application_id_1||'—'}}</td>
    <td>${{d.application_id_2||'—'}}</td>
  </tr>`).join(''); }}

  // ── Financial Anomalies ──
  const ftb = $('fin-tbody');
  if (!D.finAnomalies.length) {{ ftb.innerHTML = emptyRow(6, 'No financial anomalies detected'); }}
  else {{ ftb.innerHTML = D.finAnomalies.slice(0, 100).map(f => `<tr>
    <td>${{f.application_id}}</td>
    <td>${{(f.family_name||'')}}, ${{(f.given_name||'')}}</td>
    <td><span class="badge yellow">${{f.anomaly_type||'—'}}</span></td>
    <td class="num">${{f.household_income ? '$'+fmt(f.household_income) : '—'}}</td>
    <td class="num">${{f.household_assets ? '$'+fmt(f.household_assets) : '—'}}</td>
    <td class="num">${{f.household_liabilities ? '$'+fmt(f.household_liabilities) : '—'}}</td>
  </tr>`).join(''); }}

  // ── Duplicate SSNs ──
  const stb = $('ssn-tbody');
  if (!D.dupSsn.length) {{ stb.innerHTML = emptyRow(2, 'No duplicate SSNs detected'); }}
  else {{ stb.innerHTML = D.dupSsn.map(s => `<tr>
    <td>${{s.ssn}}</td><td class="num"><span class="badge red">${{s.count}}</span></td>
  </tr>`).join(''); }}

  // ── Same-Day Filing Bursts ──
  const sdtb = $('sameday-tbody');
  if (!D.sameDay.length) {{ sdtb.innerHTML = emptyRow(3, 'No same-day filing bursts'); }}
  else {{ sdtb.innerHTML = D.sameDay.map(d => `<tr>
    <td>${{d.date}}</td>
    <td class="num"><span class="badge cyan">${{d.count}}</span></td>
    <td style="font-size:11px;max-width:300px;overflow:hidden;text-overflow:ellipsis">${{d.detail||'—'}}</td>
  </tr>`).join(''); }}

  // ── Address Clusters ──
  const atb = $('addr-tbody');
  if (!D.addrClusters.length) {{ atb.innerHTML = emptyRow(2, 'No address clusters detected'); }}
  else {{ atb.innerHTML = D.addrClusters.map(a => `<tr>
    <td>${{a.address}}</td><td class="num"><span class="badge yellow">${{a.count}}</span></td>
  </tr>`).join(''); }}

  // ── Preparer Concentration ──
  const ptb = $('prep-tbody');
  if (!D.preparerConc.length) {{ ptb.innerHTML = emptyRow(2, 'No preparer concentration alerts'); }}
  else {{ ptb.innerHTML = D.preparerConc.map(p => `<tr>
    <td>${{p.name}}</td><td class="num"><span class="badge purple">${{p.count}}</span></td>
  </tr>`).join(''); }}

  // ── Filing Patterns ──
  const pttb = $('pattern-tbody');
  if (!D.filingPatterns.length) {{ pttb.innerHTML = emptyRow(4, 'No filing patterns detected'); }}
  else {{ pttb.innerHTML = D.filingPatterns.slice(0, 50).map(p => `<tr>
    <td><span class="badge ${{p.pattern_type==='SAME_DAY_BURST'?'cyan':'yellow'}}">${{p.pattern_type}}</span></td>
    <td class="num">${{fmt(p.app_count)}}</td>
    <td style="font-size:11px;max-width:300px;overflow:hidden;text-overflow:ellipsis">${{p.detail||'—'}}</td>
    <td>${{p.filing_date||'—'}}</td>
  </tr>`).join(''); }}

  // ── Eligibility Risk Distribution (bar) ──
  if (D.eligRiskDist.length) {{
    const tierColors = {{'CRITICAL':'#F87171','HIGH':'#FB923C','MEDIUM':'#FBBF24','LOW':'#34D399','NONE':'#4B7BF5'}};
    new Chart($('chart-elig-risk'), {{
      type: 'bar',
      data: {{
        labels: D.eligRiskDist.map(r => r.tier),
        datasets: [{{
          label: 'Applications',
          data: D.eligRiskDist.map(r => r.count),
          backgroundColor: D.eligRiskDist.map(r => tierColors[r.tier] || '#4B7BF5'),
          borderRadius: 3
        }}]
      }},
      options: {{responsive:true, maintainAspectRatio:false,
        plugins:{{legend:{{display:false}}}},
        scales:{{y:{{beginAtZero:true,grid:{{color:'#272D3F'}}}},x:{{grid:{{display:false}}}}}}}}
    }});
  }}
}}

// ── Refresh ──────────────────────────────────────────────────────────────────
async function refreshData() {{
  const btn = document.querySelector('.btn');
  btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Loading…';
  btn.disabled = true;
  try {{
    const r = await fetch('/api/refresh', {{method:'POST'}});
    if (r.ok) location.reload();
    else alert('Refresh failed');
  }} catch(e) {{ alert('Error: ' + e.message); }}
  finally {{ btn.innerHTML = '<i class="fas fa-sync-alt"></i> Refresh'; btn.disabled = false; }}
}}

// ── Init ─────────────────────────────────────────────────────────────────────
(function init() {{
  if (D.meta && D.meta.loaded_at) {{
    const d = new Date(D.meta.loaded_at);
    $('loaded-at').innerHTML = '<i class="fas fa-clock"></i> ' + d.toLocaleString();
  }}
  renderExec();
}})();
</script>
</body>
</html>'''
