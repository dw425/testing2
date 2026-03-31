#!/usr/bin/env python3
"""
I-485 Form Analyzer — Blueprint Databricks App
Sits on lho_ucm.i485_form schema to visualize, detect anomalies, and flag fraud.
"""
import os, json, time, logging, subprocess
from flask import Flask, jsonify

LOG = logging.getLogger("i485")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

# ── Config ────────────────────────────────────────────────────────────────────
HOST    = os.environ.get("DATABRICKS_HOST", "https://dbc-1459cbde-0cff.cloud.databricks.com")
WH      = os.environ.get("DATABRICKS_WAREHOUSE_ID", "0c5bd90f54a5bd8b")
SCH     = os.environ.get("I485_SCHEMA", "lho_ucm.i485_form")
PORT    = int(os.environ.get("DATABRICKS_APP_PORT", os.environ.get("PORT", "8060")))
PROFILE = os.environ.get("DATABRICKS_PROFILE", "planxs")

# ── Auth ──────────────────────────────────────────────────────────────────────
IS_DBAPP = os.environ.get("DATABRICKS_APP_PORT") is not None
_tok = {"v": None, "ts": 0}

def _get_token():
    # 1. Explicit token env var
    t = os.environ.get("DATABRICKS_TOKEN", "")
    if t:
        return t
    # 2. Databricks SDK auto-auth (works inside Databricks Apps)
    now = time.time()
    if _tok["v"] and now - _tok["ts"] < 3000:
        return _tok["v"]
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        hdr = w.config.authenticate()
        token = hdr.get("Authorization", "").replace("Bearer ", "")
        if token:
            _tok["v"] = token
            _tok["ts"] = now
            LOG.info("Auth via Databricks SDK")
            return token
    except Exception:
        pass
    # 3. CLI profile (local dev)
    try:
        r = subprocess.run(
            ["databricks", "auth", "token", "-p", PROFILE, "--host", HOST],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0:
            d = json.loads(r.stdout)
            _tok["v"] = d.get("access_token") or d.get("token_value", "")
            _tok["ts"] = now
            return _tok["v"]
    except Exception as e:
        LOG.warning(f"CLI token error: {e}")
    return ""


def sql(stmt):
    """Execute SQL via Databricks SQL Statements API."""
    import requests as req

    token = _get_token()
    if not token:
        return {"columns": [], "rows": [], "error": "No auth token"}
    url = f"{HOST}/api/2.0/sql/statements"
    hdr = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "warehouse_id": WH,
        "statement": stmt,
        "wait_timeout": "30s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }
    try:
        r = req.post(url, json=body, headers=hdr, timeout=60)
        d = r.json()
        state = d.get("status", {}).get("state", "?")
        if state == "FAILED":
            return {"columns": [], "rows": [], "error": d["status"]["error"]["message"][:200]}
        # poll if still running
        if state in ("PENDING", "RUNNING"):
            sid = d.get("statement_id")
            for _ in range(12):
                time.sleep(5)
                r2 = req.get(f"{url}/{sid}", headers=hdr, timeout=30)
                d = r2.json()
                if d.get("status", {}).get("state") in ("SUCCEEDED", "FAILED"):
                    break
            if d.get("status", {}).get("state") == "FAILED":
                return {"columns": [], "rows": [], "error": d["status"]["error"]["message"][:200]}
        cols = [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = d.get("result", {}).get("data_array", [])
        return {"columns": cols, "rows": rows}
    except Exception as e:
        LOG.error(f"SQL error: {e}")
        return {"columns": [], "rows": [], "error": str(e)}


# ── Data Loading ──────────────────────────────────────────────────────────────
_cache = {"data": None, "ts": 0}
CACHE_TTL = 300

S = SCH  # shorthand for queries


def load_data():
    now = time.time()
    if _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["data"]

    LOG.info("Loading I-485 data …")
    t0 = time.time()
    data = {}

    # ── Executive Overview ────────────────────────────────────────────────
    data["status_summary"] = sql(
        f"SELECT COALESCE(status,'UNKNOWN') as status, COUNT(*) as cnt "
        f"FROM {S}.application GROUP BY 1 ORDER BY 2 DESC"
    )

    data["app_count"] = sql(f"SELECT COUNT(*) as total FROM {S}.application")

    data["monthly"] = sql(
        f"SELECT DATE_FORMAT(filing_date,'yyyy-MM') as month, COUNT(*) as cnt "
        f"FROM {S}.application WHERE filing_date IS NOT NULL GROUP BY 1 ORDER BY 1"
    )

    data["categories"] = sql(
        f"SELECT COALESCE(fc.category_group,'Uncategorized') as grp, COUNT(*) as cnt "
        f"FROM {S}.application a "
        f"LEFT JOIN {S}.filing_category fc ON a.application_id=fc.application_id "
        f"GROUP BY 1 ORDER BY 2 DESC"
    )

    data["states"] = sql(
        f"SELECT state, COUNT(DISTINCT application_id) as cnt "
        f"FROM {S}.addresses WHERE address_type='current_physical' AND state IS NOT NULL "
        f"GROUP BY 1 ORDER BY 2 DESC LIMIT 15"
    )

    data["countries"] = sql(
        f"SELECT country_of_citizenship as country, COUNT(*) as cnt "
        f"FROM {S}.applicant_info WHERE country_of_citizenship IS NOT NULL "
        f"GROUP BY 1 ORDER BY 2 DESC LIMIT 20"
    )

    # ── Application Explorer ──────────────────────────────────────────────
    data["applications"] = sql(f"""
        SELECT a.application_id, a.a_number, a.receipt_number, a.filing_date,
               a.status, a.created_at,
               ai.family_name, ai.given_name, ai.country_of_citizenship,
               ai.date_of_birth,
               fc.category_code, fc.category_group,
               addr.state as res_state, addr.city as res_city
        FROM {S}.application a
        LEFT JOIN {S}.applicant_info ai ON a.application_id=ai.application_id
        LEFT JOIN {S}.filing_category fc ON a.application_id=fc.application_id
        LEFT JOIN (SELECT application_id, state, city FROM {S}.addresses
                   WHERE address_type='current_physical') addr
          ON a.application_id=addr.application_id
        ORDER BY a.application_id DESC LIMIT 1000
    """)

    # ── Data Quality ──────────────────────────────────────────────────────
    data["table_counts"] = sql(f"""
        SELECT 'application' as tbl, COUNT(*) as cnt FROM {S}.application
        UNION ALL SELECT 'applicant_info', COUNT(*) FROM {S}.applicant_info
        UNION ALL SELECT 'addresses', COUNT(*) FROM {S}.addresses
        UNION ALL SELECT 'filing_category', COUNT(*) FROM {S}.filing_category
        UNION ALL SELECT 'employment_history', COUNT(*) FROM {S}.employment_history
        UNION ALL SELECT 'marital_history', COUNT(*) FROM {S}.marital_history
        UNION ALL SELECT 'children', COUNT(*) FROM {S}.children
        UNION ALL SELECT 'parents', COUNT(*) FROM {S}.parents
        UNION ALL SELECT 'biographic_info', COUNT(*) FROM {S}.biographic_info
        UNION ALL SELECT 'eligibility_responses', COUNT(*) FROM {S}.eligibility_responses
        UNION ALL SELECT 'public_charge', COUNT(*) FROM {S}.public_charge
        UNION ALL SELECT 'organizations', COUNT(*) FROM {S}.organizations
        UNION ALL SELECT 'contacts_signatures', COUNT(*) FROM {S}.contacts_signatures
        UNION ALL SELECT 'benefits_received', COUNT(*) FROM {S}.benefits_received
        UNION ALL SELECT 'institutionalization', COUNT(*) FROM {S}.institutionalization
        UNION ALL SELECT 'other_names', COUNT(*) FROM {S}.other_names
        UNION ALL SELECT 'additional_info', COUNT(*) FROM {S}.additional_info
        UNION ALL SELECT 'affidavit_exemption', COUNT(*) FROM {S}.affidavit_exemption
        UNION ALL SELECT 'interview_signature', COUNT(*) FROM {S}.interview_signature
        UNION ALL SELECT 'additional_information', COUNT(*) FROM {S}.additional_information
    """)

    data["missing_fields"] = sql(f"""
        SELECT 'applicant_info' as tbl, 'family_name' as fld,
               SUM(CASE WHEN family_name IS NULL THEN 1 ELSE 0 END) as missing, COUNT(*) as total
        FROM {S}.applicant_info
        UNION ALL SELECT 'applicant_info','given_name',
               SUM(CASE WHEN given_name IS NULL THEN 1 ELSE 0 END),COUNT(*) FROM {S}.applicant_info
        UNION ALL SELECT 'applicant_info','date_of_birth',
               SUM(CASE WHEN date_of_birth IS NULL THEN 1 ELSE 0 END),COUNT(*) FROM {S}.applicant_info
        UNION ALL SELECT 'applicant_info','sex',
               SUM(CASE WHEN sex IS NULL THEN 1 ELSE 0 END),COUNT(*) FROM {S}.applicant_info
        UNION ALL SELECT 'applicant_info','country_of_birth',
               SUM(CASE WHEN country_of_birth IS NULL THEN 1 ELSE 0 END),COUNT(*) FROM {S}.applicant_info
        UNION ALL SELECT 'application','filing_date',
               SUM(CASE WHEN filing_date IS NULL THEN 1 ELSE 0 END),COUNT(*) FROM {S}.application
        UNION ALL SELECT 'application','status',
               SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END),COUNT(*) FROM {S}.application
        UNION ALL SELECT 'application','a_number',
               SUM(CASE WHEN a_number IS NULL THEN 1 ELSE 0 END),COUNT(*) FROM {S}.application
        UNION ALL SELECT 'filing_category','category_code',
               SUM(CASE WHEN category_code IS NULL THEN 1 ELSE 0 END),COUNT(*) FROM {S}.filing_category
        UNION ALL SELECT 'biographic_info','ethnicity',
               SUM(CASE WHEN ethnicity IS NULL THEN 1 ELSE 0 END),COUNT(*) FROM {S}.biographic_info
    """)

    data["dup_anumbers"] = sql(
        f"SELECT a_number, COUNT(*) as cnt FROM {S}.application "
        f"WHERE a_number IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1 ORDER BY 2 DESC LIMIT 50"
    )

    data["date_errors"] = sql(f"""
        SELECT a.application_id, ai.family_name, ai.given_name,
          CASE
            WHEN ai.date_of_birth > CURRENT_DATE() THEN 'Future date of birth'
            WHEN a.filing_date > CURRENT_DATE() THEN 'Future filing date'
            WHEN ai.date_of_birth IS NOT NULL AND a.filing_date IS NOT NULL
                 AND ai.date_of_birth > a.filing_date THEN 'DOB after filing date'
            WHEN ai.date_of_birth IS NOT NULL AND a.filing_date IS NOT NULL
                 AND DATEDIFF(a.filing_date,ai.date_of_birth)/365>120 THEN 'Age over 120'
          END as issue
        FROM {S}.application a
        JOIN {S}.applicant_info ai ON a.application_id=ai.application_id
        WHERE ai.date_of_birth > CURRENT_DATE()
           OR a.filing_date > CURRENT_DATE()
           OR (ai.date_of_birth IS NOT NULL AND a.filing_date IS NOT NULL AND ai.date_of_birth > a.filing_date)
           OR (ai.date_of_birth IS NOT NULL AND a.filing_date IS NOT NULL AND DATEDIFF(a.filing_date,ai.date_of_birth)/365>120)
        LIMIT 100
    """)

    # ── Fraud & Patterns (from pre-computed fraud tables) ────────────────
    data["fraud_alert_summary"] = sql(
        f"SELECT risk_level, COUNT(*) as cnt, SUM(total_flags) as flags "
        f"FROM {S}.fraud_alerts GROUP BY 1 "
        f"ORDER BY CASE risk_level WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END"
    )

    data["fraud_flag_summary"] = sql(
        f"SELECT rule_code, severity, COUNT(*) as cnt "
        f"FROM {S}.fraud_flags GROUP BY 1,2 "
        f"ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END, cnt DESC"
    )

    data["eligibility_flags"] = sql(f"""
        SELECT fa.application_id, ai.family_name, ai.given_name,
               fa.risk_score, fa.risk_level, fa.total_flags, fa.critical_flags, fa.warning_flags,
               fa.flag_categories
        FROM {S}.fraud_alerts fa
        JOIN {S}.applicant_info ai ON fa.application_id=ai.application_id
        WHERE fa.risk_level IN ('CRITICAL','HIGH')
        ORDER BY fa.risk_score DESC LIMIT 200
    """)

    data["dup_ssns"] = sql(f"""
        SELECT matched_value as ssn_masked, COUNT(*) as cnt
        FROM {S}.fraud_dup_identity WHERE match_type='SSN'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 50
    """)

    data["dup_identity"] = sql(f"""
        SELECT match_type, matched_value, confidence,
               application_id_1, application_id_2
        FROM {S}.fraud_dup_identity ORDER BY
        CASE match_type WHEN 'SSN' THEN 1 WHEN 'A_NUMBER' THEN 2 ELSE 3 END LIMIT 100
    """)

    data["elig_risk_dist"] = sql(f"""
        SELECT risk_tier, COUNT(*) as cnt
        FROM {S}.fraud_eligibility_risk GROUP BY 1
        ORDER BY CASE risk_tier WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 WHEN 'LOW' THEN 4 ELSE 5 END
    """)

    data["financial_anomalies"] = sql(f"""
        SELECT fa.application_id, ai.family_name, ai.given_name,
               fa.anomaly_type, fa.household_income, fa.household_assets, fa.household_liabilities
        FROM {S}.fraud_financial_anomalies fa
        JOIN {S}.applicant_info ai ON fa.application_id=ai.application_id
        ORDER BY fa.anomaly_type, ABS(COALESCE(fa.household_assets,0)-COALESCE(fa.household_liabilities,0)) DESC
        LIMIT 100
    """)

    data["filing_patterns"] = sql(f"""
        SELECT pattern_type, app_count, detail, filing_date
        FROM {S}.fraud_filing_patterns ORDER BY app_count DESC LIMIT 50
    """)

    data["same_day"] = sql(f"""
        SELECT filing_date, app_count as cnt, detail
        FROM {S}.fraud_filing_patterns WHERE pattern_type='SAME_DAY_BURST'
        ORDER BY app_count DESC LIMIT 30
    """)

    data["addr_clusters"] = sql(f"""
        SELECT address_key as full_addr, app_count
        FROM {S}.fraud_address_anomalies WHERE anomaly_type='CLUSTER'
        ORDER BY app_count DESC LIMIT 50
    """)

    data["preparer_conc"] = sql(f"""
        SELECT family_name, given_name, COUNT(DISTINCT application_id) as app_count
        FROM {S}.contacts_signatures
        WHERE contact_type='preparer' AND family_name IS NOT NULL
        GROUP BY 1,2 HAVING COUNT(DISTINCT application_id)>3 ORDER BY 3 DESC LIMIT 50
    """)

    dur = time.time() - t0
    data["_meta"] = {
        "loaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_sec": round(dur, 1),
        "schema": SCH,
    }
    LOG.info(f"Data loaded in {dur:.1f}s")
    _cache["data"] = data
    _cache["ts"] = now
    return data


# ── Flask ─────────────────────────────────────────────────────────────────────
app = Flask(__name__)


@app.route("/")
def index():
    from dashboard import render_dashboard

    data = load_data()
    return render_dashboard(data)


@app.route("/api/refresh", methods=["POST"])
def refresh():
    _cache["data"] = None
    _cache["ts"] = 0
    data = load_data()
    return jsonify({"status": "ok", "loaded_at": data["_meta"]["loaded_at"]})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "schema": SCH})


if __name__ == "__main__":
    LOG.info(f"I-485 Analyzer | port {PORT} | schema {SCH}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
