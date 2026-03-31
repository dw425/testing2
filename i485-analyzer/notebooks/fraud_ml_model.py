#!/usr/bin/env python3
"""
I-485 Fraud Detection — ML Models (Local)

Pulls data from Databricks SQL, builds ML models locally with pandas/sklearn,
writes predictions back to Delta tables via SQL API.

Usage:
    python notebooks/fraud_ml_model.py
"""
import os, sys, json, time, logging, subprocess
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    roc_auc_score, average_precision_score, accuracy_score,
    f1_score, precision_score, recall_score, classification_report,
    confusion_matrix,
)

LOG = logging.getLogger("fraud_ml")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)

# ── Config ────────────────────────────────────────────────────────────────────
HOST    = os.environ.get("DATABRICKS_HOST", "https://dbc-1459cbde-0cff.cloud.databricks.com")
WH      = os.environ.get("DATABRICKS_WAREHOUSE_ID", "0c5bd90f54a5bd8b")
SCH     = os.environ.get("I485_SCHEMA", "lho_ucm.i485_form")
PROFILE = os.environ.get("DATABRICKS_PROFILE", "planxs")

# ── Auth (reuse same logic as app) ────────────────────────────────────────────
_tok = {"v": None, "ts": 0}

def _get_token():
    t = os.environ.get("DATABRICKS_TOKEN", "")
    if t:
        return t
    now = time.time()
    if _tok["v"] and now - _tok["ts"] < 3000:
        return _tok["v"]
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


def sql(stmt, wait_timeout="50s"):
    """Execute SQL via Databricks SQL Statements API, return {columns, rows}.
    Handles chunked pagination for large result sets."""
    import requests as req
    token = _get_token()
    if not token:
        raise RuntimeError("No auth token available")
    url = f"{HOST}/api/2.0/sql/statements"
    hdr = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "warehouse_id": WH,
        "statement": stmt,
        "wait_timeout": wait_timeout,
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }
    r = req.post(url, json=body, headers=hdr, timeout=180)
    d = r.json()
    state = d.get("status", {}).get("state", "?")
    if state == "FAILED":
        raise RuntimeError(d["status"]["error"]["message"][:300])
    if state in ("PENDING", "RUNNING"):
        sid = d.get("statement_id")
        for _ in range(30):
            time.sleep(5)
            r2 = req.get(f"{url}/{sid}", headers=hdr, timeout=60)
            d = r2.json()
            st = d.get("status", {}).get("state")
            if st == "SUCCEEDED":
                break
            if st == "FAILED":
                raise RuntimeError(d["status"]["error"]["message"][:300])
    cols = [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = d.get("result", {}).get("data_array", [])

    # Follow pagination chunks
    chunk_link = d.get("result", {}).get("next_chunk_internal_link")
    while chunk_link:
        chunk_url = f"{HOST}{chunk_link}"
        cr = req.get(chunk_url, headers=hdr, timeout=120)
        cd = cr.json()
        rows.extend(cd.get("data_array", []))
        chunk_link = cd.get("next_chunk_internal_link")

    return {"columns": cols, "rows": rows}


def sql_df(stmt, wait_timeout="50s"):
    """Run SQL, return pandas DataFrame."""
    r = sql(stmt, wait_timeout)
    if not r["columns"]:
        return pd.DataFrame()
    return pd.DataFrame(r["rows"], columns=r["columns"])


def sql_exec(stmt):
    """Execute DDL/DML without caring about results."""
    try:
        sql(stmt)
    except Exception as e:
        LOG.warning(f"SQL exec warning: {e}")


# ── Helpers ───────────────────────────────────────────────────────────────────
S = SCH

def safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def safe_int(v, default=0):
    try:
        return int(v) if v is not None else default
    except (ValueError, TypeError):
        return default


# ═══════════════════════════════════════════════════════════════════════════════
#  1 — DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def load_all_data():
    """Pull all needed tables from Databricks into pandas DataFrames."""
    LOG.info("Loading data from Databricks SQL …")
    t0 = time.time()

    dfs = {}

    queries = {
        "application": f"""
            SELECT application_id, a_number, receipt_number,
                   filing_date, status
            FROM {S}.application
        """,
        "applicant_info": f"""
            SELECT application_id, family_name, given_name,
                   date_of_birth, sex, country_of_citizenship,
                   country_of_birth, ssn
            FROM {S}.applicant_info
        """,
        "filing_category": f"""
            SELECT application_id, category_code, category_group
            FROM {S}.filing_category
        """,
        "public_charge": f"""
            SELECT application_id, household_income, household_assets,
                   household_liabilities
            FROM {S}.public_charge
        """,
        "eligibility": f"""
            SELECT * FROM {S}.eligibility_responses
        """,
        "addresses": f"""
            SELECT application_id, address_type, state, city
            FROM {S}.addresses
            WHERE address_type = 'current_physical'
        """,
        "contacts": f"""
            SELECT application_id, contact_type, family_name, given_name
            FROM {S}.contacts_signatures
            WHERE contact_type = 'preparer'
        """,
        "children": f"""
            SELECT application_id, COUNT(*) as cnt
            FROM {S}.children GROUP BY 1
        """,
        "employment": f"""
            SELECT application_id, COUNT(*) as cnt
            FROM {S}.employment_history GROUP BY 1
        """,
        "marital": f"""
            SELECT application_id, COUNT(*) as cnt
            FROM {S}.marital_history GROUP BY 1
        """,
        "other_names": f"""
            SELECT application_id, COUNT(*) as cnt
            FROM {S}.other_names GROUP BY 1
        """,
        "organizations": f"""
            SELECT application_id, COUNT(*) as cnt
            FROM {S}.organizations GROUP BY 1
        """,
        "fraud_alerts": f"""
            SELECT application_id, risk_score, risk_level, total_flags
            FROM {S}.fraud_alerts
        """,
    }

    for name, q in queries.items():
        LOG.info(f"  Loading {name} …")
        dfs[name] = sql_df(q)
        LOG.info(f"    → {len(dfs[name]):,} rows")

    LOG.info(f"Data loaded in {time.time()-t0:.1f}s")
    return dfs


# ═══════════════════════════════════════════════════════════════════════════════
#  2 — FEATURE ENGINEERING
# ═══════════════════════════════════════════════════════════════════════════════

def build_features(dfs):
    LOG.info("Building feature matrix …")

    app = dfs["application"].copy()
    info = dfs["applicant_info"].copy()
    base = app[["application_id"]].copy()

    # ── 2a: Demographics ─────────────────────────────────────────────────────
    info["dob"] = pd.to_datetime(info["date_of_birth"], errors="coerce")
    app["fdate"] = pd.to_datetime(app["filing_date"], errors="coerce")

    merged = base.merge(info[["application_id", "dob", "sex", "ssn", "family_name", "given_name"]], on="application_id", how="left")
    merged = merged.merge(app[["application_id", "fdate"]], on="application_id", how="left")

    merged["age_at_filing"] = ((merged["fdate"] - merged["dob"]).dt.days / 365.25).round(1)
    merged["sex_encoded"] = merged["sex"].map({"Male": 1, "Female": 0}).fillna(-1)
    merged["missing_dob"] = merged["dob"].isna().astype(int)
    merged["missing_ssn"] = merged["ssn"].isna().astype(int)
    merged["missing_name"] = merged["family_name"].isna().astype(int)
    merged["missing_filing_date"] = merged["fdate"].isna().astype(int)

    features = merged[["application_id", "age_at_filing", "sex_encoded",
                        "missing_dob", "missing_ssn", "missing_name", "missing_filing_date"]].copy()

    # ── 2b: Filing Category ──────────────────────────────────────────────────
    fc = dfs["filing_category"].copy()
    fc["wrong_category"] = (fc["category_group"] == "WRONG_GROUP").astype(int)
    fc["missing_category"] = fc["category_group"].isna().astype(int)
    features = features.merge(fc[["application_id", "wrong_category", "missing_category"]],
                               on="application_id", how="left")

    # ── 2c: Financial ────────────────────────────────────────────────────────
    pc = dfs["public_charge"].copy()
    for c in ["household_income", "household_assets", "household_liabilities"]:
        pc[c] = pd.to_numeric(pc[c], errors="coerce").fillna(0)
    pc.rename(columns={"household_income": "income", "household_assets": "assets",
                        "household_liabilities": "liabilities"}, inplace=True)
    pc["net_worth"] = pc["assets"] - pc["liabilities"]
    pc["negative_networth"] = (pc["net_worth"] < -50000).astype(int)
    pc["zero_income_high_assets"] = ((pc["income"] == 0) & (pc["assets"] > 500000)).astype(int)
    pc["missing_income"] = (pc["income"] == 0).astype(int)
    pc["debt_to_income"] = np.where(pc["income"] > 0, pc["liabilities"] / pc["income"], 0)
    pc["assets_to_income"] = np.where(pc["income"] > 0, pc["assets"] / pc["income"], 0)
    features = features.merge(
        pc[["application_id", "income", "assets", "liabilities", "net_worth",
            "negative_networth", "zero_income_high_assets", "missing_income",
            "debt_to_income", "assets_to_income"]],
        on="application_id", how="left")

    # ── 2d: Eligibility Responses ────────────────────────────────────────────
    elig = dfs["eligibility"].copy()
    q_cols = [c for c in elig.columns if c.startswith("q")]

    CRITICAL_QS = ["q26", "q27", "q36", "q37", "q41", "q42", "q53"]
    HIGH_QS = ["q22", "q24", "q30", "q70", "q74"]
    CRIMINAL_QS = [f"q{i}" for i in range(22, 42)]
    SECURITY_QS = [f"q{i}" for i in range(42, 56)]
    IMMIGRATION_QS = [f"q{i}" for i in range(10, 22)] + [f"q{i}" for i in range(67, 87)]

    def count_yes(row, cols):
        valid = [c for c in cols if c in q_cols]
        return sum(1 for c in valid if str(row.get(c, "")).upper() == "YES")

    elig["total_yes_count"] = elig.apply(lambda r: count_yes(r, q_cols), axis=1)
    elig["critical_yes_count"] = elig.apply(lambda r: count_yes(r, CRITICAL_QS), axis=1)
    elig["high_yes_count"] = elig.apply(lambda r: count_yes(r, HIGH_QS), axis=1)
    elig["criminal_yes_count"] = elig.apply(lambda r: count_yes(r, CRIMINAL_QS), axis=1)
    elig["security_yes_count"] = elig.apply(lambda r: count_yes(r, SECURITY_QS), axis=1)
    elig["immigration_yes_count"] = elig.apply(lambda r: count_yes(r, IMMIGRATION_QS), axis=1)
    elig["has_critical_elig"] = (elig["critical_yes_count"] > 0).astype(int)
    elig["has_high_elig"] = (elig["high_yes_count"] > 0).astype(int)

    elig_feat_cols = ["application_id", "total_yes_count", "critical_yes_count",
                      "high_yes_count", "criminal_yes_count", "security_yes_count",
                      "immigration_yes_count", "has_critical_elig", "has_high_elig"]
    features = features.merge(elig[elig_feat_cols], on="application_id", how="left")

    # ── 2e: Identity Network ─────────────────────────────────────────────────
    # SSN cluster
    ssn_valid = info[info["ssn"].notna()][["application_id", "ssn"]]
    ssn_counts = ssn_valid.groupby("ssn")["application_id"].count().reset_index()
    ssn_counts.columns = ["ssn", "ssn_cluster_size"]
    ssn_feat = ssn_valid.merge(ssn_counts, on="ssn", how="left")[["application_id", "ssn_cluster_size"]]
    ssn_feat["dup_ssn_flag"] = (ssn_feat["ssn_cluster_size"] > 1).astype(int)
    features = features.merge(ssn_feat, on="application_id", how="left")

    # A-number cluster
    anum_valid = app[app["a_number"].notna()][["application_id", "a_number"]]
    anum_counts = anum_valid.groupby("a_number")["application_id"].count().reset_index()
    anum_counts.columns = ["a_number", "anum_cluster_size"]
    anum_feat = anum_valid.merge(anum_counts, on="a_number", how="left")[["application_id", "anum_cluster_size"]]
    anum_feat["dup_anum_flag"] = (anum_feat["anum_cluster_size"] > 1).astype(int)
    features = features.merge(anum_feat, on="application_id", how="left")

    # Name+DOB cluster
    nd = info[info["family_name"].notna() & info["given_name"].notna() & info["dob"].notna()].copy()
    nd["nd_key"] = nd["family_name"].str.upper() + "|" + nd["given_name"].str.upper() + "|" + nd["dob"].astype(str)
    nd_counts = nd.groupby("nd_key")["application_id"].count().reset_index()
    nd_counts.columns = ["nd_key", "name_dob_cluster_size"]
    nd_feat = nd.merge(nd_counts, on="nd_key", how="left")[["application_id", "name_dob_cluster_size"]]
    nd_feat["dup_name_dob_flag"] = (nd_feat["name_dob_cluster_size"] > 1).astype(int)
    features = features.merge(nd_feat, on="application_id", how="left")

    # ── 2f: Address Clustering ───────────────────────────────────────────────
    addr = dfs["addresses"].copy()
    addr["addr_key"] = addr["state"].fillna("") + "|" + addr["city"].fillna("")
    addr_counts = addr.groupby("addr_key")["application_id"].count().reset_index()
    addr_counts.columns = ["addr_key", "address_cluster_size"]
    addr_feat = addr.merge(addr_counts, on="addr_key", how="left")[["application_id", "address_cluster_size"]]
    addr_feat["missing_state"] = (addr["state"].isna()).astype(int).values
    addr_feat = addr_feat.drop_duplicates("application_id")
    features = features.merge(addr_feat, on="application_id", how="left")

    # ── 2g: Preparer Concentration ───────────────────────────────────────────
    prep = dfs["contacts"].copy()
    prep["prep_key"] = prep["family_name"].fillna("") + "|" + prep["given_name"].fillna("")
    prep_counts = prep.groupby("prep_key")["application_id"].nunique().reset_index()
    prep_counts.columns = ["prep_key", "preparer_app_count"]
    prep_feat = prep.merge(prep_counts, on="prep_key", how="left")[["application_id", "preparer_app_count"]]
    prep_feat["high_vol_preparer"] = (prep_feat["preparer_app_count"] > 3).astype(int)
    prep_feat = prep_feat.drop_duplicates("application_id")
    features = features.merge(prep_feat, on="application_id", how="left")

    # ── 2h: Filing Patterns ──────────────────────────────────────────────────
    daily = app[app["fdate"].notna()].groupby("fdate")["application_id"].count().reset_index()
    daily.columns = ["fdate", "daily_volume"]
    mean_vol = daily["daily_volume"].mean()
    std_vol = daily["daily_volume"].std() or 1.0

    filing_feat = app[["application_id", "fdate"]].merge(daily, on="fdate", how="left")
    filing_feat["daily_filing_volume"] = filing_feat["daily_volume"].fillna(0)
    filing_feat["filing_volume_zscore"] = (filing_feat["daily_filing_volume"] - mean_vol) / std_vol
    filing_feat["same_day_burst_flag"] = (filing_feat["filing_volume_zscore"] > 3.0).astype(int)
    filing_feat["filing_dow"] = filing_feat["fdate"].dt.dayofweek.fillna(-1).astype(int)
    filing_feat["filing_month"] = filing_feat["fdate"].dt.month.fillna(0).astype(int)
    features = features.merge(
        filing_feat[["application_id", "daily_filing_volume", "filing_volume_zscore",
                      "same_day_burst_flag", "filing_dow", "filing_month"]],
        on="application_id", how="left")

    # ── 2i: Family / History Counts ──────────────────────────────────────────
    for name, col_name in [("children", "num_children"), ("employment", "num_employment"),
                            ("marital", "num_marital"), ("other_names", "num_other_names"),
                            ("organizations", "num_organizations")]:
        df = dfs[name].copy()
        df.columns = ["application_id", col_name]
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce").fillna(0).astype(int)
        features = features.merge(df, on="application_id", how="left")

    # ── Fill all NaN with 0 ──────────────────────────────────────────────────
    feature_cols = [c for c in features.columns if c != "application_id"]
    features[feature_cols] = features[feature_cols].fillna(0).astype(float)

    # Cap extreme ratios to prevent overflow
    for c in ["debt_to_income", "assets_to_income"]:
        if c in features.columns:
            features[c] = features[c].clip(-1000, 1000)

    LOG.info(f"Feature matrix: {len(features):,} rows × {len(feature_cols)} features")
    return features, feature_cols


# ═══════════════════════════════════════════════════════════════════════════════
#  3 — LABELS
# ═══════════════════════════════════════════════════════════════════════════════

def build_labels(dfs, features):
    labels = dfs["fraud_alerts"].copy()
    labels["risk_score"] = pd.to_numeric(labels["risk_score"], errors="coerce").fillna(0)
    labels["total_flags"] = pd.to_numeric(labels["total_flags"], errors="coerce").fillna(0)
    labels["is_fraud"] = labels["risk_level"].isin(["CRITICAL", "HIGH"]).astype(int)
    labels["risk_class"] = labels["risk_level"].map(
        {"CRITICAL": 3, "HIGH": 2, "MEDIUM": 1, "LOW": 0}
    ).fillna(0).astype(int)

    ml_df = features.merge(
        labels[["application_id", "risk_score", "total_flags", "is_fraud", "risk_class"]],
        on="application_id", how="left"
    )
    ml_df[["risk_score", "total_flags", "is_fraud", "risk_class"]] = \
        ml_df[["risk_score", "total_flags", "is_fraud", "risk_class"]].fillna(0)
    ml_df["is_fraud"] = ml_df["is_fraud"].astype(int)
    ml_df["risk_class"] = ml_df["risk_class"].astype(int)

    LOG.info(f"Label distribution:\n{ml_df['is_fraud'].value_counts().to_string()}")
    return ml_df


# ═══════════════════════════════════════════════════════════════════════════════
#  4 — MODEL TRAINING
# ═══════════════════════════════════════════════════════════════════════════════

def train_models(ml_df, feature_cols):
    X = ml_df[feature_cols].values
    y_binary = ml_df["is_fraud"].values
    y_multi = ml_df["risk_class"].values

    X_train, X_test, y_train, y_test, ym_train, ym_test = train_test_split(
        X, y_binary, y_multi, test_size=0.2, random_state=42, stratify=y_binary
    )

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)
    X_all_s = scaler.transform(X)

    results = {"scaler": scaler, "feature_cols": feature_cols}

    # ── 4a: Random Forest (Binary) ───────────────────────────────────────────
    LOG.info("Training Random Forest (binary) …")
    rf = RandomForestClassifier(
        n_estimators=200, max_depth=12, min_samples_leaf=5,
        max_features="sqrt", random_state=42, n_jobs=-1,
    )
    rf.fit(X_train_s, y_train)
    rf_proba = rf.predict_proba(X_test_s)[:, 1]
    rf_pred = rf.predict(X_test_s)

    rf_auc = roc_auc_score(y_test, rf_proba)
    rf_pr = average_precision_score(y_test, rf_proba)
    rf_acc = accuracy_score(y_test, rf_pred)
    rf_f1 = f1_score(y_test, rf_pred, average="weighted")
    rf_prec = precision_score(y_test, rf_pred, average="weighted")
    rf_rec = recall_score(y_test, rf_pred, average="weighted")

    print("\n" + "═" * 55)
    print("  RANDOM FOREST — Binary Fraud Classification")
    print("═" * 55)
    print(f"  AUC-ROC:    {rf_auc:.4f}")
    print(f"  AUC-PR:     {rf_pr:.4f}")
    print(f"  Accuracy:   {rf_acc:.4f}")
    print(f"  F1 Score:   {rf_f1:.4f}")
    print(f"  Precision:  {rf_prec:.4f}")
    print(f"  Recall:     {rf_rec:.4f}")

    results["rf"] = rf
    results["rf_metrics"] = {
        "AUC_ROC": rf_auc, "AUC_PR": rf_pr, "Accuracy": rf_acc,
        "F1": rf_f1, "Precision": rf_prec, "Recall": rf_rec,
    }

    # Feature importance
    fi = pd.DataFrame({"feature": feature_cols, "importance": rf.feature_importances_})
    fi = fi.sort_values("importance", ascending=False)
    print("\n  Top 20 Features (Random Forest):")
    print("  " + "─" * 50)
    for _, row in fi.head(20).iterrows():
        bar = "█" * int(row["importance"] * 120)
        print(f"    {row['feature']:30s} {row['importance']:.4f}  {bar}")
    results["feature_importance"] = fi

    # ── 4b: Gradient Boosted Trees (Binary) ──────────────────────────────────
    LOG.info("Training Gradient Boosted Trees (binary) …")
    gbt = GradientBoostingClassifier(
        n_estimators=150, max_depth=6, learning_rate=0.1,
        subsample=0.8, random_state=42,
    )
    gbt.fit(X_train_s, y_train)
    gbt_proba = gbt.predict_proba(X_test_s)[:, 1]
    gbt_pred = gbt.predict(X_test_s)

    gbt_auc = roc_auc_score(y_test, gbt_proba)
    gbt_pr = average_precision_score(y_test, gbt_proba)
    gbt_acc = accuracy_score(y_test, gbt_pred)
    gbt_f1 = f1_score(y_test, gbt_pred, average="weighted")
    gbt_prec = precision_score(y_test, gbt_pred, average="weighted")
    gbt_rec = recall_score(y_test, gbt_pred, average="weighted")

    print("\n" + "═" * 55)
    print("  GRADIENT BOOSTED TREES — Binary Fraud Classification")
    print("═" * 55)
    print(f"  AUC-ROC:    {gbt_auc:.4f}")
    print(f"  AUC-PR:     {gbt_pr:.4f}")
    print(f"  Accuracy:   {gbt_acc:.4f}")
    print(f"  F1 Score:   {gbt_f1:.4f}")
    print(f"  Precision:  {gbt_prec:.4f}")
    print(f"  Recall:     {gbt_rec:.4f}")

    results["gbt"] = gbt
    results["gbt_metrics"] = {
        "AUC_ROC": gbt_auc, "AUC_PR": gbt_pr, "Accuracy": gbt_acc,
        "F1": gbt_f1, "Precision": gbt_prec, "Recall": gbt_rec,
    }

    # GBT feature importance
    gbt_fi = pd.DataFrame({"feature": feature_cols, "importance": gbt.feature_importances_})
    gbt_fi = gbt_fi.sort_values("importance", ascending=False)
    print("\n  Top 20 Features (GBT):")
    print("  " + "─" * 50)
    for _, row in gbt_fi.head(20).iterrows():
        bar = "█" * int(row["importance"] * 120)
        print(f"    {row['feature']:30s} {row['importance']:.4f}  {bar}")

    # ── 4c: Random Forest Multi-Class (Risk Level) ───────────────────────────
    LOG.info("Training Random Forest (multi-class) …")
    rf_mc = RandomForestClassifier(
        n_estimators=200, max_depth=12, random_state=42, n_jobs=-1,
    )
    rf_mc.fit(X_train_s, ym_train)
    rfmc_pred = rf_mc.predict(X_test_s)
    rfmc_acc = accuracy_score(ym_test, rfmc_pred)
    rfmc_f1 = f1_score(ym_test, rfmc_pred, average="weighted")

    print("\n" + "═" * 55)
    print("  RANDOM FOREST — Multi-Class Risk Level (0/1/2/3)")
    print("═" * 55)
    print(f"  Accuracy:   {rfmc_acc:.4f}")
    print(f"  F1 Score:   {rfmc_f1:.4f}")
    print(f"\n  Classification Report:")
    print(classification_report(ym_test, rfmc_pred,
          target_names=["LOW", "MEDIUM", "HIGH", "CRITICAL"]))

    results["rf_mc"] = rf_mc
    results["rf_mc_metrics"] = {"Accuracy": rfmc_acc, "F1": rfmc_f1}

    # ── 4d: Isolation Forest (Unsupervised Anomaly Detection) ────────────────
    LOG.info("Training Isolation Forest (unsupervised) …")
    iso = IsolationForest(
        n_estimators=300, max_samples=0.5, contamination=0.15,
        random_state=42, n_jobs=-1,
    )
    iso.fit(X_all_s)

    iso_scores_raw = iso.decision_function(X_all_s)
    iso_labels = iso.predict(X_all_s)  # 1=normal, -1=anomaly

    # Normalize to 0-100 (higher = more anomalous)
    s_min, s_max = iso_scores_raw.min(), iso_scores_raw.max()
    iso_scores = ((s_max - iso_scores_raw) / (s_max - s_min) * 100).clip(0, 100)

    n_anom = (iso_labels == -1).sum()
    print("\n" + "═" * 55)
    print("  ISOLATION FOREST — Unsupervised Anomaly Detection")
    print("═" * 55)
    print(f"  Anomalies detected: {n_anom:,} ({n_anom/len(iso_labels)*100:.1f}%)")

    # Compare with rule-based
    is_fraud_all = ml_df["is_fraud"].values
    is_anom = (iso_labels == -1).astype(int)
    both = ((is_fraud_all == 1) & (is_anom == 1)).sum()
    novel = ((is_fraud_all == 0) & (is_anom == 1)).sum()
    print(f"  Agreement (both flag fraud): {both:,}")
    print(f"  Novel anomalies (IF only):   {novel:,}")

    results["iso"] = iso
    results["iso_scores"] = iso_scores
    results["iso_labels"] = iso_labels
    results["iso_metrics"] = {"Anomalies": n_anom, "Novel": novel}

    # ── 4e: Ensemble Scoring ─────────────────────────────────────────────────
    LOG.info("Computing ensemble scores …")

    rf_all_proba = rf.predict_proba(X_all_s)[:, 1]
    gbt_all_proba = gbt.predict_proba(X_all_s)[:, 1]

    # Weighted: 40% RF + 25% GBT + 35% Isolation Forest
    raw_ensemble = (rf_all_proba * 40 + gbt_all_proba * 25 + (iso_scores / 100.0) * 35)
    # Normalize to 0-100
    e_min, e_max = raw_ensemble.min(), raw_ensemble.max()
    e_range = e_max - e_min if e_max != e_min else 1.0
    ml_score = ((raw_ensemble - e_min) / e_range * 100).round(1)

    ml_risk = np.where(ml_score >= 75, "CRITICAL",
              np.where(ml_score >= 50, "HIGH",
              np.where(ml_score >= 25, "MEDIUM", "LOW")))

    results["ml_scores"] = ml_score
    results["ml_risk"] = ml_risk
    results["rf_proba"] = rf_all_proba
    results["gbt_proba"] = gbt_all_proba

    print("\n  ML Risk Distribution:")
    for level in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        cnt = (ml_risk == level).sum()
        print(f"    {level:10s}  {cnt:>8,}")

    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  5 — WRITE RESULTS BACK TO DATABRICKS
# ═══════════════════════════════════════════════════════════════════════════════

def write_results(ml_df, results):
    LOG.info("Writing results to Databricks …")

    # ── 5a: ml_fraud_scores ──────────────────────────────────────────────────
    LOG.info("  Creating ml_fraud_scores table …")
    sql_exec(f"DROP TABLE IF EXISTS {S}.ml_fraud_scores")
    sql_exec(f"""
        CREATE TABLE {S}.ml_fraud_scores (
            application_id BIGINT,
            rule_based_score DOUBLE,
            rf_probability DOUBLE,
            gbt_probability DOUBLE,
            isolation_forest_score DOUBLE,
            isolation_forest_anomaly INT,
            ml_fraud_score DOUBLE,
            ml_risk_level STRING,
            scored_at TIMESTAMP
        )
    """)

    app_ids = ml_df["application_id"].values
    rf_proba = results["rf_proba"]
    gbt_proba = results["gbt_proba"]
    iso_scores = results["iso_scores"]
    iso_labels = results["iso_labels"]
    ml_scores = results["ml_scores"]
    ml_risk = results["ml_risk"]
    rule_scores = ml_df["risk_score"].values

    # Insert in batches
    BATCH = 500
    total = len(app_ids)
    for i in range(0, total, BATCH):
        batch_end = min(i + BATCH, total)
        values = []
        for j in range(i, batch_end):
            iso_anom = 1 if iso_labels[j] == -1 else 0
            values.append(
                f"({int(app_ids[j])}, {float(rule_scores[j]):.2f}, "
                f"{float(rf_proba[j]):.4f}, {float(gbt_proba[j]):.4f}, "
                f"{float(iso_scores[j]):.2f}, {iso_anom}, "
                f"{float(ml_scores[j]):.1f}, '{ml_risk[j]}', CURRENT_TIMESTAMP())"
            )
        sql_exec(f"INSERT INTO {S}.ml_fraud_scores VALUES {','.join(values)}")
        if (i // BATCH) % 20 == 0:
            LOG.info(f"    {batch_end:,} / {total:,} …")

    cnt = safe_int(sql_df(f"SELECT COUNT(*) FROM {S}.ml_fraud_scores").iloc[0, 0])
    LOG.info(f"  ✓ ml_fraud_scores: {cnt:,} rows")

    # ── 5b: ml_feature_importance ────────────────────────────────────────────
    LOG.info("  Creating ml_feature_importance table …")
    sql_exec(f"DROP TABLE IF EXISTS {S}.ml_feature_importance")
    sql_exec(f"""
        CREATE TABLE {S}.ml_feature_importance (
            feature STRING,
            importance DOUBLE
        )
    """)
    fi = results["feature_importance"]
    vals = ", ".join(
        f"('{row.feature}', {row.importance:.6f})" for _, row in fi.iterrows()
    )
    sql_exec(f"INSERT INTO {S}.ml_feature_importance VALUES {vals}")
    LOG.info(f"  ✓ ml_feature_importance: {len(fi)} rows")

    # ── 5c: ml_model_metrics ─────────────────────────────────────────────────
    LOG.info("  Creating ml_model_metrics table …")
    sql_exec(f"DROP TABLE IF EXISTS {S}.ml_model_metrics")
    sql_exec(f"""
        CREATE TABLE {S}.ml_model_metrics (
            model_name STRING,
            metric_name STRING,
            metric_value DOUBLE
        )
    """)
    rows = []
    for m_name, m_dict in [
        ("RandomForest_Binary", results["rf_metrics"]),
        ("GBT_Binary", results["gbt_metrics"]),
        ("RandomForest_MultiClass", results["rf_mc_metrics"]),
    ]:
        for k, v in m_dict.items():
            rows.append(f"('{m_name}', '{k}', {float(v):.6f})")

    iso_m = results["iso_metrics"]
    rows.append(f"('IsolationForest', 'Anomalies_Detected', {float(iso_m['Anomalies'])})")
    rows.append(f"('IsolationForest', 'Novel_Detections', {float(iso_m['Novel'])})")

    sql_exec(f"INSERT INTO {S}.ml_model_metrics VALUES {','.join(rows)}")
    LOG.info(f"  ✓ ml_model_metrics: {len(rows)} rows")

    # ── 5d: ml_novel_anomalies ───────────────────────────────────────────────
    LOG.info("  Creating ml_novel_anomalies table …")
    sql_exec(f"DROP TABLE IF EXISTS {S}.ml_novel_anomalies")
    sql_exec(f"""
        CREATE TABLE {S}.ml_novel_anomalies (
            application_id BIGINT,
            isolation_forest_score DOUBLE,
            ml_fraud_score DOUBLE,
            ml_risk_level STRING,
            detected_at TIMESTAMP
        )
    """)

    is_fraud = ml_df["is_fraud"].values
    novel_mask = (iso_labels == -1) & (is_fraud == 0)
    novel_ids = app_ids[novel_mask]
    novel_if_scores = iso_scores[novel_mask]
    novel_ml_scores = ml_scores[novel_mask]
    novel_ml_risk = ml_risk[novel_mask]

    for i in range(0, len(novel_ids), BATCH):
        batch_end = min(i + BATCH, len(novel_ids))
        values = []
        for j in range(i, batch_end):
            values.append(
                f"({int(novel_ids[j])}, {float(novel_if_scores[j]):.2f}, "
                f"{float(novel_ml_scores[j]):.1f}, '{novel_ml_risk[j]}', CURRENT_TIMESTAMP())"
            )
        sql_exec(f"INSERT INTO {S}.ml_novel_anomalies VALUES {','.join(values)}")

    cnt2 = safe_int(sql_df(f"SELECT COUNT(*) FROM {S}.ml_novel_anomalies").iloc[0, 0])
    LOG.info(f"  ✓ ml_novel_anomalies: {cnt2:,} rows")

    return cnt, cnt2


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    t0 = time.time()
    print("=" * 60)
    print("  I-485 FRAUD DETECTION — ML MODEL PIPELINE")
    print("  Running locally, data from Databricks SQL")
    print("=" * 60)

    # 1. Load data
    dfs = load_all_data()

    # 2. Feature engineering
    features, feature_cols = build_features(dfs)

    # 3. Build labels
    ml_df = build_labels(dfs, features)

    # 4. Train models
    results = train_models(ml_df, feature_cols)

    # 5. Write to Databricks
    cnt, cnt2 = write_results(ml_df, results)

    dur = time.time() - t0
    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Duration:           {dur/60:.1f} minutes")
    print(f"  Applications scored: {cnt:,}")
    print(f"  Novel anomalies:    {cnt2:,}")
    print(f"  Features used:      {len(feature_cols)}")
    fi = results["feature_importance"]
    print(f"  Top 5 features:     {', '.join(fi['feature'].head(5).tolist())}")
    print()
    print(f"  Tables written to {S}:")
    print(f"    ml_fraud_scores        — Composite ML scores for all apps")
    print(f"    ml_novel_anomalies     — Novel ML-only detections")
    print(f"    ml_feature_importance  — Feature importance rankings")
    print(f"    ml_model_metrics       — Model performance metrics")
    print()
    print("  ✓ Done")


if __name__ == "__main__":
    main()
