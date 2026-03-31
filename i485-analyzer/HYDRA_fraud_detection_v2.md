# HYDRA v2 — Hybrid Detection & Risk Analysis

## I-485 Fraud Detection: Full-Stack ML Pipeline + Databricks Application

**HYDRA** = **H**ybrid **D**etection & **R**isk **A**nalysis
A multi-headed fraud detection system: every head attacks the problem from a different angle.

---

## Table of Contents

1. [Phase 1: Enhancement Builds (E1–E9)](#phase-1-enhancement-builds)
2. [Phase 2: Model Research & Expansion](#phase-2-model-research--expansion)
3. [Phase 3: Improvement Cycle — Gap Audit & Full Builds](#phase-3-improvement-cycle)
4. [Phase 4: Problem Analysis & Challenge Resolution](#phase-4-problem-analysis)
5. [Phase 5: Hardening & Output Package](#phase-5-hardening)
6. [Phase 6: Databricks App — HYDRA Dashboard](#phase-6-databricks-app)
7. [Execution Sequence](#execution-sequence)

---

## Phase 1: Enhancement Builds

### E1 — Marriage Fraud Indicators (7 features)

**File:** `02_features.py`
**Function:** `build_1m_marriage_fraud(app_df, info_df, marital_df, fc_df, feat)`
**Called after:** `build_1l_temporal` in `main()` (line 1406)

#### Features

| # | Feature | Data Source | Logic |
|---|---------|------------|-------|
| 1 | `marriage_to_filing_days` | `application.filing_date` + `marital_history.marriage_date` | For the most recent marriage (max `sort_order`), compute `filing_date - marriage_date` in days. Negative if married after filing. |
| 2 | `marriage_to_filing_suspicious` | derived | `marriage_to_filing_days >= 0 AND marriage_to_filing_days < 90` → 1 |
| 3 | `serial_petitioner_count` | `filing_category.principal_a_number` | Group by `principal_a_number`, count distinct `application_id`. Map back to each app that shares that principal. Flag if count > 2. |
| 4 | `multiple_marriage_based` | `filing_category` | Same `principal_a_number` appearing with `category_group` containing 'FAMILY' or 'IMMEDIATE'. Flag if that principal sponsors > 2 apps. |
| 5 | `rapid_remarriage` | `marital_history` | Sort by `sort_order`. Compute `marriage_date[i] - marriage_end_date[i-1]` in days. If < 90 → flag. |
| 6 | `spouse_country_mismatch` | `marital_history.spouse_country_of_birth` + `applicant_info.country_of_citizenship` | Flag if spouse country is on a different continent than applicant citizenship. Use a simple continent mapping dict (6 continent buckets). |
| 7 | `marriage_after_filing` | `application.filing_date` + `marital_history.marriage_date` | `marriage_date > filing_date` for any marital record → 1 |

#### Implementation

```python
def build_1m_marriage_fraud(app_df, info_df, marital_df, fc_df, feat):
    """1M. Marriage Fraud Indicators (7 features)."""
    log.info("Building 1M: Marriage Fraud Indicators ...")
    t0 = time.time()
    aids = feat["application_id"]

    # ── marriage_to_filing_days ──
    if (len(marital_df) > 0 and "marriage_date" in marital_df.columns
            and "filing_date" in app_df.columns):
        # Get most recent marriage per app (highest sort_order)
        mh = marital_df[["application_id", "sort_order", "marriage_date",
                          "marriage_end_date", "spouse_country_of_birth"]].copy()
        mh["sort_order"] = pd.to_numeric(mh["sort_order"], errors="coerce").fillna(0)
        latest = mh.sort_values("sort_order").groupby("application_id").last()
        latest["m_date"] = _safe_date(latest["marriage_date"])

        fd_lookup = app_df.set_index("application_id")["filing_date"]
        fd_mapped = _safe_date(aids.map(fd_lookup))
        m_mapped = aids.map(latest["m_date"])

        diff = (fd_mapped - m_mapped).dt.days
        feat["marriage_to_filing_days"] = diff.fillna(-9999).astype(int)
        feat["marriage_to_filing_suspicious"] = (
            (diff >= 0) & (diff < 90) & diff.notna()
        ).astype(int)
        feat["marriage_after_filing"] = (
            (m_mapped > fd_mapped) & m_mapped.notna() & fd_mapped.notna()
        ).astype(int)
    else:
        feat["marriage_to_filing_days"] = -9999
        feat["marriage_to_filing_suspicious"] = 0
        feat["marriage_after_filing"] = 0

    # ── serial_petitioner_count ──
    if (len(fc_df) > 0 and "principal_a_number" in fc_df.columns):
        pa = fc_df[["application_id", "principal_a_number"]].copy()
        pa = pa.dropna(subset=["principal_a_number"])
        pa = pa[pa["principal_a_number"].astype(str).str.strip().ne("")]
        pa = pa[pa["principal_a_number"].astype(str).ne("None")]
        if len(pa) > 0:
            pa_counts = pa.groupby("principal_a_number")["application_id"].transform("count")
            pa_map = pd.Series(pa_counts.values, index=pa["application_id"].values)
            feat["serial_petitioner_count"] = aids.map(pa_map).fillna(0).astype(int)
        else:
            feat["serial_petitioner_count"] = 0
    else:
        feat["serial_petitioner_count"] = 0
    feat["multiple_marriage_based"] = (feat["serial_petitioner_count"] > 2).astype(int)

    # ── rapid_remarriage ──
    if (len(marital_df) > 0 and "marriage_date" in marital_df.columns
            and "marriage_end_date" in marital_df.columns):
        mh2 = marital_df[["application_id", "sort_order",
                           "marriage_date", "marriage_end_date"]].copy()
        mh2["sort_order"] = pd.to_numeric(mh2["sort_order"], errors="coerce").fillna(0)
        mh2 = mh2.sort_values(["application_id", "sort_order"])
        mh2["m_date"] = _safe_date(mh2["marriage_date"])
        mh2["end_date"] = _safe_date(mh2["marriage_end_date"])
        mh2["prev_end"] = mh2.groupby("application_id")["end_date"].shift(1)
        mh2["gap_days"] = (mh2["m_date"] - mh2["prev_end"]).dt.days
        rapid = mh2[mh2["gap_days"].between(0, 90)]
        rapid_ids = set(rapid["application_id"].unique())
        feat["rapid_remarriage"] = aids.isin(rapid_ids).astype(int)
    else:
        feat["rapid_remarriage"] = 0

    # ── spouse_country_mismatch ──
    CONTINENT = {
        "UNITED STATES": "NA", "CANADA": "NA", "MEXICO": "NA",
        "BRAZIL": "SA", "COLOMBIA": "SA", "ARGENTINA": "SA", "PERU": "SA",
        "VENEZUELA": "SA", "CHILE": "SA", "ECUADOR": "SA",
        "UNITED KINGDOM": "EU", "FRANCE": "EU", "GERMANY": "EU",
        "ITALY": "EU", "SPAIN": "EU", "POLAND": "EU", "UKRAINE": "EU",
        "RUSSIA": "EU", "ROMANIA": "EU", "NETHERLANDS": "EU",
        "CHINA": "AS", "INDIA": "AS", "JAPAN": "AS", "SOUTH KOREA": "AS",
        "PHILIPPINES": "AS", "VIETNAM": "AS", "PAKISTAN": "AS",
        "BANGLADESH": "AS", "IRAN": "AS", "IRAQ": "AS", "THAILAND": "AS",
        "INDONESIA": "AS", "TAIWAN": "AS", "NEPAL": "AS",
        "NIGERIA": "AF", "ETHIOPIA": "AF", "GHANA": "AF", "KENYA": "AF",
        "SOUTH AFRICA": "AF", "EGYPT": "AF", "CAMEROON": "AF",
        "AUSTRALIA": "OC", "NEW ZEALAND": "OC",
        "EL SALVADOR": "NA", "GUATEMALA": "NA", "HONDURAS": "NA",
        "CUBA": "NA", "HAITI": "NA", "JAMAICA": "NA",
        "DOMINICAN REPUBLIC": "NA", "TRINIDAD AND TOBAGO": "NA",
    }
    if (len(marital_df) > 0 and "spouse_country_of_birth" in marital_df.columns
            and "country_of_citizenship" in info_df.columns):
        latest_spouse = marital_df.sort_values("sort_order").groupby(
            "application_id")["spouse_country_of_birth"].last()
        sp_country = aids.map(latest_spouse).astype(str).str.upper().str.strip()
        sp_cont = sp_country.map(CONTINENT)

        info_lookup = info_df.set_index("application_id")
        app_country = aids.map(
            info_lookup["country_of_citizenship"]).astype(str).str.upper().str.strip()
        app_cont = app_country.map(CONTINENT)

        feat["spouse_country_mismatch"] = (
            (sp_cont != app_cont) & sp_cont.notna() & app_cont.notna()
        ).astype(int)
    else:
        feat["spouse_country_mismatch"] = 0

    log.info("  1M complete: 7 features (%.1fs)", time.time() - t0)
    return feat
```

---

### E2 — Temporal Velocity Features (9 features)

**File:** `02_features.py`
**Function:** `build_1n_temporal_velocity(app_df, contacts_df, addr_df, feat)`

#### Features

| # | Feature | Logic |
|---|---------|-------|
| 1 | `atty_7d_rolling_count` | Per attorney: apps filed within ±7 days of this app |
| 2 | `atty_30d_rolling_count` | Per attorney: apps filed within ±30 days |
| 3 | `atty_90d_rolling_count` | Per attorney: apps filed within ±90 days |
| 4 | `preparer_7d_rolling_count` | Per preparer: apps within ±7 days |
| 5 | `preparer_30d_rolling_count` | Per preparer: apps within ±30 days |
| 6 | `address_30d_filing_count` | Same address + within ±30 days |
| 7 | `atty_filing_acceleration` | `(atty_30d / atty_90d) * 3`. Values > 1.5 = accelerating |
| 8 | `filing_hour_suspicious` | If signature_date has time: flag outside 7am-8pm or weekends |
| 9 | `volume_anomaly_zscore` | Weekly filing volume z-score (rolling 4-week mean/std) |

#### Implementation Strategy

Rolling window counts on 200K rows need to be efficient. Strategy:

```python
def _rolling_count(df, group_col, date_col, window_days):
    """Vectorized rolling window count per group."""
    # Sort by group + date
    df = df.sort_values([group_col, date_col])
    # For each row, count rows in same group within ±window_days
    # Use merge_asof approach: self-join with tolerance
    # Actually: for each group, build sorted date array, use searchsorted
    results = np.zeros(len(df), dtype=int)
    for _, grp in df.groupby(group_col):
        dates = grp[date_col].values.astype("datetime64[D]")
        idx = grp.index.values
        for i, d in enumerate(dates):
            lo = np.searchsorted(dates, d - np.timedelta64(window_days, "D"))
            hi = np.searchsorted(dates, d + np.timedelta64(window_days, "D"), side="right")
            results[idx[i]] = hi - lo  # includes self
    return results
```

For 200K rows with ~50K unique attorneys, this is feasible (<30s).
For 200K rows with preparer groups — similar.

The `volume_anomaly_zscore` uses per-week filing counts:

```python
# Compute weekly filing volume
app_df["week"] = app_df["filing_date"].dt.isocalendar().week
weekly_counts = app_df.groupby("week").size()
# Rolling 4-week mean/std
rolling_mean = weekly_counts.rolling(4, min_periods=1).mean()
rolling_std = weekly_counts.rolling(4, min_periods=1).std().fillna(1)
zscore = (weekly_counts - rolling_mean) / rolling_std
# Map back to each app via its week
```

---

### E3 — NLP Text Similarity (5 features)

**File:** `02_features.py`
**Function:** `build_1o_nlp_similarity(addl_text_df, app_df, contacts_df, feat)`
**Dependencies:** `sklearn.feature_extraction.text.TfidfVectorizer`, `sklearn.metrics.pairwise.cosine_similarity`

#### Features

| # | Feature | Logic |
|---|---------|-------|
| 1 | `has_additional_text` | Binary: app has ≥1 additional_information record |
| 2 | `additional_text_length` | Total char count of concatenated text per app |
| 3 | `text_similarity_max_same_atty` | Max cosine similarity to any other app with same attorney |
| 4 | `text_similarity_above_085` | Binary: max sim ≥ 0.85 |
| 5 | `text_duplicate_cluster_size` | Number of apps in same-atty group with pairwise sim > 0.85 |

#### Implementation

```python
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def build_1o_nlp_similarity(addl_text_df, app_df, contacts_df, feat):
    log.info("Building 1O: NLP Text Similarity ...")
    t0 = time.time()
    aids = feat["application_id"]

    feat["has_additional_text"] = 0
    feat["additional_text_length"] = 0
    feat["text_similarity_max_same_atty"] = 0.0
    feat["text_similarity_above_085"] = 0
    feat["text_duplicate_cluster_size"] = 0

    if len(addl_text_df) == 0 or "additional_text" not in addl_text_df.columns:
        log.info("  1O complete: 5 features (no text data) (%.1fs)", time.time() - t0)
        return feat

    # Concatenate text per app
    text_agg = addl_text_df.groupby("application_id")["additional_text"].apply(
        lambda x: " ".join(str(v) for v in x if pd.notna(v))
    ).reset_index()
    text_agg.columns = ["application_id", "full_text"]
    text_agg = text_agg[text_agg["full_text"].str.strip().ne("")]

    has_text_ids = set(text_agg["application_id"])
    feat["has_additional_text"] = aids.isin(has_text_ids).astype(int)

    len_map = text_agg.set_index("application_id")["full_text"].str.len()
    feat["additional_text_length"] = aids.map(len_map).fillna(0).astype(int)

    # Merge with attorney
    if "atty_state_bar_number" not in app_df.columns or len(text_agg) == 0:
        log.info("  1O complete: 5 features (no attorney for NLP) (%.1fs)", time.time() - t0)
        return feat

    text_atty = text_agg.merge(
        app_df[["application_id", "atty_state_bar_number"]],
        on="application_id", how="left"
    )
    text_atty = text_atty.dropna(subset=["atty_state_bar_number"])
    text_atty = text_atty[text_atty["atty_state_bar_number"].astype(str).str.strip().ne("")]

    # Per-attorney TF-IDF + cosine similarity
    max_sim_map = {}
    cluster_size_map = {}
    MAX_GROUP = 500  # cap per attorney to avoid O(n^2) explosion

    for atty, grp in text_atty.groupby("atty_state_bar_number"):
        if len(grp) < 2:
            continue
        g = grp.head(MAX_GROUP)  # cap
        try:
            tfidf = TfidfVectorizer(max_features=5000, stop_words="english",
                                     min_df=1, max_df=0.95)
            X = tfidf.fit_transform(g["full_text"])
            sim = cosine_similarity(X)
            np.fill_diagonal(sim, 0)

            app_ids_in_grp = g["application_id"].values
            for i in range(len(app_ids_in_grp)):
                aid = app_ids_in_grp[i]
                ms = float(sim[i].max())
                max_sim_map[aid] = ms
                cluster_size_map[aid] = int((sim[i] >= 0.85).sum()) + 1  # +1 for self
        except Exception:
            continue

    if max_sim_map:
        sim_series = pd.Series(max_sim_map)
        feat["text_similarity_max_same_atty"] = aids.map(sim_series).fillna(0.0).round(4)
        feat["text_similarity_above_085"] = (feat["text_similarity_max_same_atty"] >= 0.85).astype(int)
        clust_series = pd.Series(cluster_size_map)
        feat["text_duplicate_cluster_size"] = aids.map(clust_series).fillna(0).astype(int)

    log.info("  1O complete: 5 features (%.1fs)", time.time() - t0)
    return feat
```

---

### E4 — Employment & Shell Company Detection (7 features)

**File:** `02_features.py`
**Function:** `build_1p_employment_enhanced(emp_df, pc_df, app_df, addr_df, feat)`

#### Features

| # | Feature | Logic |
|---|---------|-------|
| 1 | `employer_app_count` | Count of I-485 apps sharing same current employer_name (normalized uppercase) |
| 2 | `employer_high_volume` | employer_app_count > 20 |
| 3 | `employer_extreme_volume` | employer_app_count > 100 |
| 4 | `employer_atty_concentration` | employer_app_count / distinct attorneys for that employer. High ratio = suspicious |
| 5 | `salary_occupation_zscore` | Z-score of household_income within same occupation group (min 10 apps per occupation) |
| 6 | `employment_gap_max_days` | Max gap between consecutive employment records (end_date[i] to start_date[i+1]) |
| 7 | `employment_state_address_mismatch` | Current employer_state != current address state |

#### Implementation Notes

- `employer_name` normalization: `.str.upper().str.strip().str.replace(r'\s+', ' ', regex=True)`
- `employment_gap_max_days` **replaces** existing placeholder `employment_gap_max_months` and `employment_gap_flag`
- `employment_state_address_mismatch` **replaces** existing placeholder `address_employment_mismatch`
- For `salary_occupation_zscore`: group by `occupation`, compute mean/std. For occupations with < 10 apps → zscore = 0

---

### E5 — Geographic Features (6 features)

**File:** `02_features.py`
**Function:** `build_1q_geographic(addr_df, info_df, labels_df, feat)`

#### Features

| # | Feature | Logic |
|---|---------|-------|
| 1 | `zip_fraud_rate` | Bayesian target-encoded fraud rate per ZIP: `(fraud_count + 1) / (total_count + 10)` |
| 2 | `po_box_address` | Current address street contains "PO BOX" / "P.O." / "P O BOX" |
| 3 | `zip_density_apps` | Count of apps per current address ZIP code |
| 4 | `foreign_address_domestic_filing` | Current address country not in ('US','USA','UNITED STATES') and not null. **Replaces placeholder.** |
| 5 | `address_unrelated_cluster_real` | At same address key, count distinct family names. If > 5 → flag. **Replaces placeholder.** |
| 6 | `multi_state_employment` | Count of distinct employer_state values per app |

#### Implementation Notes

- `zip_fraud_rate` requires labels. Pass `labels_df` to this function. Use Bayesian smoothing to prevent leakage.
- `address_unrelated_cluster_real`: join `addresses` with `applicant_info` on `application_id`, group by address key, count unique `family_name`.

---

### E6 — Fill Existing Placeholders (10 upgrades)

**File:** `02_features.py` — modify existing functions in-place.

| Placeholder | Line | Real Logic |
|-------------|------|-----------|
| `passport_country_mismatch` | 406 | `passport_country != country_of_citizenship` (both in `applicant_info`) |
| `passport_expired_before_filing` | 418 | `passport_expiration < filing_date` (applicant_info + application) |
| `circular_sponsorship` | 525 | A sponsors B and B sponsors A via `principal_a_number` cross-reference |
| `same_country_burst` | 1059 | Same `filing_date` + same `country_of_citizenship` > 10 apps |
| `coordinated_filing` | 1060 | Same attorney + same day + same address |
| `education_income_mismatch` | 1236 | `education_level_encoded <= 1 AND income > 200000` |
| `days_since_arrival` | 1259 | `filing_date - arrival_date` in days |
| `employment_gap_max_months` | 1203 | Replaced by E4's `employment_gap_max_days` (compute in new func, delete placeholder) |
| `employment_gap_flag` | 1204 | `employment_gap_max_days > 365` |
| `address_employment_mismatch` | 942 | Replaced by E4's `employment_state_address_mismatch` |

#### Circular Sponsorship Logic

```python
# filing_category has principal_a_number (who sponsors this app)
# applicant_info has a_number (this app's A-number)
# Circular: app A's a_number == app B's principal_a_number
#       AND app B's a_number == app A's principal_a_number
fc = tables["filing_category"]
info = tables["applicant_info"]

# Build mapping: application_id → own a_number
own_anum = info.set_index("application_id")["a_number"].dropna()
# Build mapping: application_id → principal_a_number (sponsor)
sponsor = fc.set_index("application_id")["principal_a_number"].dropna()

# For each app that has both: check if sponsor's app has this app as sponsor
# Efficient: build dict sponsor_anum → set of sponsored app_ids
# Then for each app: is my a_number in sponsor's principal_a_number?
```

---

### E7 — Focal Loss for XGBoost

**File:** `03_supervised.py`
**Modify:** training section, add parallel focal-loss XGBoost

#### Implementation

```python
def _focal_loss_obj(y_pred, dtrain, gamma=2.0, alpha=0.25):
    """Focal loss objective for XGBoost. Focuses on hard examples."""
    y = dtrain.get_label()
    p = 1.0 / (1.0 + np.exp(-y_pred))
    pt = np.where(y == 1, p, 1 - p)
    alpha_t = np.where(y == 1, alpha, 1 - alpha)
    focal_weight = alpha_t * (1 - pt) ** gamma
    grad = focal_weight * (p - y)
    hess = focal_weight * p * (1 - p)
    hess = np.maximum(hess, 1e-7)
    return grad, hess

# Train with focal loss
xgb_focal = XGBClassifier(
    n_estimators=500, max_depth=6, learning_rate=0.05,
    objective=_focal_loss_obj,  # custom
    eval_metric="aucpr",
    subsample=0.8, colsample_bytree=0.7,
    reg_alpha=0.1, reg_lambda=1.0,
    random_state=42, n_jobs=-1, tree_method="hist",
)
```

- Train both standard XGBoost (scale_pos_weight) and focal-loss XGBoost
- Compare PR-AUC on test set, save the better one as `xgboost.joblib`
- Save the other as `xgboost_focal.joblib` for the ensemble to optionally use

---

### E8 — One-Class SVM

**File:** `04_unsupervised.py`
**Function:** `run_ocsvm(X_scaled, is_fraud)`

```python
from sklearn.svm import OneClassSVM

def run_ocsvm(X_scaled, is_fraud, max_train=50000):
    """3H. One-Class SVM — trained on clean apps only."""
    t0 = time.time()
    clean_idx = np.where(is_fraud == 0)[0]
    if len(clean_idx) > max_train:
        rng = np.random.RandomState(42)
        train_idx = rng.choice(clean_idx, max_train, replace=False)
    else:
        train_idx = clean_idx

    ocsvm = OneClassSVM(kernel="rbf", gamma="scale", nu=0.03)
    ocsvm.fit(X_scaled[train_idx])

    raw = ocsvm.decision_function(X_scaled)
    scores = -raw  # flip: more negative = more anomalous → higher score
    lo, hi = scores.min(), scores.max()
    scores = (scores - lo) / (hi - lo + 1e-10)

    LOG.info(f"  One-Class SVM trained in {time.time() - t0:.1f}s")
    joblib.dump(ocsvm, MODELS_DIR / "ocsvm.joblib")
    return scores
```

**Add to output assembly:** `"ocsvm_score": ocsvm_scores` in `scores_df` dict.
**Add to summary:** `model_names.append("ocsvm_score")`, `model_labels.append("One-Class SVM")`.

---

### E9 — Ensemble Plumbing

**File:** `06_ensemble.py`
**Change:** Add `"ocsvm_score"` to `unsup_cols` list (line ~251).

```python
unsup_cols = [
    "if_score", "ae_score", "hybrid_score",
    "lof_score", "copod_score", "mahalanobis_score", "benford_flag",
    "ocsvm_score",  # E8
]
```

---

## Phase 2: Model Research & Expansion

### 2A — CatBoost (New Supervised Model)

**Why:** CatBoost handles categorical features natively — no encoding needed. Our feature matrix has many binary flags and ordinal features that CatBoost processes more naturally than XGBoost.

**File:** `03_supervised.py`
**Add:** 4th supervised model alongside XGBoost, LightGBM, RF.

```python
from catboost import CatBoostClassifier

cat_model = CatBoostClassifier(
    iterations=500, depth=6, learning_rate=0.05,
    auto_class_weights="Balanced",
    eval_metric="PRAUC",
    early_stopping_rounds=30,
    random_seed=42, thread_count=-1,
    verbose=0,
)
```

**Impact:** Adds `cat_prob` to ensemble Level 0. Typically 1-3% PR-AUC improvement over XGBoost on tabular data with many categorical features.

**Dependency:** `pip install catboost`

---

### 2B — TabNet (Deep Learning for Tabular Data)

**Why:** TabNet uses attention mechanisms to select features per sample — it automatically learns which features matter for each application, providing built-in interpretability.

**File:** New `03b_tabnet.py` or add to `03_supervised.py`

```python
from pytorch_tabnet.tab_model import TabNetClassifier

tabnet = TabNetClassifier(
    n_d=32, n_a=32, n_steps=5,
    gamma=1.5, lambda_sparse=1e-4,
    optimizer_fn=torch.optim.Adam,
    optimizer_params=dict(lr=2e-2),
    scheduler_params={"step_size": 10, "gamma": 0.9},
    scheduler_fn=torch.optim.lr_scheduler.StepLR,
    mask_type="entmax",
    verbose=0,
)
```

**Impact:** Alternative deep learning approach. Feature attention masks provide per-sample explainability complementary to SHAP.

**Dependency:** `pip install pytorch-tabnet`

---

### 2C — Variational Autoencoder (VAE)

**Why:** VAE learns a probabilistic latent space rather than deterministic. Reconstruction probability (not just error) is a better anomaly score. Research shows VAE outperforms standard AE for fraud detection.

**File:** `04_unsupervised.py`
**Add:** `run_vae()` alongside existing autoencoder.

```python
class VAE(nn.Module):
    def __init__(self, input_dim, latent_dim=16):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 64), nn.ReLU(),
            nn.Linear(64, 32), nn.ReLU(),
        )
        self.mu_layer = nn.Linear(32, latent_dim)
        self.logvar_layer = nn.Linear(32, latent_dim)
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, 32), nn.ReLU(),
            nn.Linear(32, 64), nn.ReLU(),
            nn.Linear(64, input_dim),
        )

    def encode(self, x):
        h = self.encoder(x)
        return self.mu_layer(h), self.logvar_layer(h)

    def reparameterize(self, mu, logvar):
        std = torch.exp(0.5 * logvar)
        eps = torch.randn_like(std)
        return mu + eps * std

    def decode(self, z):
        return self.decoder(z)

    def forward(self, x):
        mu, logvar = self.encode(x)
        z = self.reparameterize(mu, logvar)
        return self.decode(z), mu, logvar
```

**Output:** `vae_score` in `unsupervised_scores.parquet`
**Dependency:** Already have `torch`.

---

### 2D — Deep SVDD (Deep Support Vector Data Description)

**Why:** Neural network version of OC-SVM. Learns a hypersphere in learned feature space that encloses normal data. Points outside the sphere are anomalies. More powerful than linear OC-SVM.

**File:** `04_unsupervised.py`
**Add:** `run_deep_svdd()`

```python
class DeepSVDD(nn.Module):
    def __init__(self, input_dim, rep_dim=32):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 128), nn.ReLU(),
            nn.Linear(128, 64), nn.ReLU(),
            nn.Linear(64, rep_dim),
        )

    def forward(self, x):
        return self.net(x)

# Training: minimize distance to center c
# c = mean of all clean representations
# Loss: (1/n) * sum(||net(x) - c||^2)
```

**Output:** `deep_svdd_score` in `unsupervised_scores.parquet`

---

### 2E — Temporal Pattern Detection (LSTM)

**Why:** Filing sequences have temporal patterns. An LSTM can detect unusual filing velocity patterns, seasonal anomalies, and coordinated timing that static features miss.

**File:** New `03c_temporal.py`

**Approach:**
- For each attorney/preparer, build a time series of daily filing counts (365-day window)
- Train LSTM on "normal" attorney filing patterns
- Anomaly score = prediction error for each attorney's pattern
- Map score back to each application via its attorney

```python
class FilingLSTM(nn.Module):
    def __init__(self, input_size=1, hidden_size=32, num_layers=2):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        out, _ = self.lstm(x)
        return self.fc(out[:, -1, :])
```

**Output:** `temporal_anomaly_score` per application
**Dependency:** Already have `torch`.

---

### 2F — GraphSAGE (Inductive GNN)

**Why:** Node2Vec generates static embeddings — new nodes require retraining. GraphSAGE generates embeddings by sampling and aggregating neighbor features, making it inductive (works on unseen nodes). Research shows GNNs achieve 0.96+ AUC on fraud ring detection.

**File:** `05_graph.py` — add alongside Node2Vec

**Dependency:** `pip install torch-geometric` (or implement from scratch with PyTorch)

**Approach:**
- Use existing NetworkX graph from `05_graph.py`
- Convert to PyG format
- Train 2-layer GraphSAGE with mean aggregation
- Use fraud labels for semi-supervised training
- Output: 64-dim embeddings per application node

```python
from torch_geometric.nn import SAGEConv

class FraudGraphSAGE(nn.Module):
    def __init__(self, in_channels, hidden_channels=64, out_channels=64):
        super().__init__()
        self.conv1 = SAGEConv(in_channels, hidden_channels)
        self.conv2 = SAGEConv(hidden_channels, out_channels)

    def forward(self, x, edge_index):
        x = self.conv1(x, edge_index).relu()
        x = F.dropout(x, p=0.3, training=self.training)
        x = self.conv2(x, edge_index)
        return x
```

**Impact:** Replaces or augments Node2Vec embeddings in graph_features.parquet. Expected +2-5% improvement in graph-based fraud ring detection.

**Note:** `torch-geometric` can be heavy to install. Fallback: keep Node2Vec, add GraphSAGE as optional.

---

### 2G — Contrastive Learning Embeddings

**Why:** Train embeddings where fraud apps are pulled apart from clean apps in embedding space. SimCLR-style contrastive loss. These embeddings become powerful features for the ensemble.

**File:** New `03d_contrastive.py`

**Approach:**
- Project each app's feature vector through a small neural network
- Positive pairs: (app, augmented version of same app)
- Negative pairs: (fraud app, clean app)
- NT-Xent contrastive loss
- Output: 32-dim contrastive embeddings as features

---

### 2H — Multi-Task Learning

**Why:** Instead of just binary fraud/clean, predict fraud type simultaneously. The model learns shared representations that improve all tasks.

**File:** Modify `03_supervised.py` or new script

**Approach:**
- Head 1: Binary fraud/clean (primary)
- Head 2: Fraud pattern type (10-class: SSN ring, address fraud, etc.)
- Head 3: Anomaly severity (regression: number of anomaly flags)
- Shared trunk: first 3 layers of a neural network

---

### Phase 2 Summary — New Models

| Model | Type | File | Output | Dependency |
|-------|------|------|--------|------------|
| CatBoost | Supervised | `03_supervised.py` | `cat_prob` | catboost |
| TabNet | Supervised/DL | `03_supervised.py` | `tabnet_prob` | pytorch-tabnet |
| VAE | Unsupervised | `04_unsupervised.py` | `vae_score` | torch (already installed) |
| Deep SVDD | Unsupervised | `04_unsupervised.py` | `deep_svdd_score` | torch |
| LSTM temporal | Temporal | `03c_temporal.py` | `temporal_anomaly_score` | torch |
| GraphSAGE | Graph/GNN | `05_graph.py` | `sage_*` embeddings | torch-geometric (optional) |
| Contrastive | Embedding | `03d_contrastive.py` | 32-dim embeddings | torch |
| Multi-task | Supervised | `03_supervised.py` | shared representations | torch |

---

## Phase 3: Improvement Cycle — Gap Audit & Full Builds

### 3A — Audit All Existing Placeholders & Stubs

Systematic scan of every `= 0  # placeholder` in the codebase:

```
02_features.py:
  Line 406: passport_country_mismatch = 0          → FIXED in E6
  Line 418: passport_expired_before_filing = 0      → FIXED in E6
  Line 525: circular_sponsorship = 0               → FIXED in E6
  Line 823: attorney_elig_similarity = 0           → BUILD (3B)
  Line 824: attorney_financial_similarity = 0      → BUILD (3B)
  Line 825: attorney_employment_cookie_cutter = 0  → BUILD (3B)
  Line 861: preparer_client_diversity_low = 0      → BUILD (3C)
  Line 942: address_employment_mismatch = 0        → FIXED in E4
  Line 952: address_unrelated_cluster = 0          → FIXED in E5
  Line 953: foreign_address_domestic_filing = 0    → FIXED in E5
  Line 1059: same_country_burst = 0                → FIXED in E6
  Line 1060: coordinated_filing = 0                → FIXED in E6
  Line 1203: employment_gap_max_months = 0         → FIXED in E4
  Line 1204: employment_gap_flag = 0               → FIXED in E4
  Line 1216: overlapping_employment = 0            → BUILD (3D)
  Line 1217: employment_before_14 = 0              → BUILD (3D)
  Line 1236: education_income_mismatch = 0         → FIXED in E6
  Line 1259: days_since_arrival = 0                → FIXED in E6
  Line 1284: interview_waiver = 0                  → BUILD (3E)
  Line 748: elig_block_contradiction = 0           → BUILD (3F)
  Line 633: mahalanobis_financial = 0              → BUILD (3G)

04_unsupervised.py:
  Autoencoder "Anomalies (>=0.00): 1" — threshold too tight → FIX (3H)
  Hybrid score nearly all zeros — miscalibrated → FIX (3H)
```

### 3B — Attorney Pattern Features (fill 3 placeholders)

**`attorney_elig_similarity`:**
```python
# For each attorney, compute cosine similarity of eligibility response vectors
# across that attorney's clients. Average pairwise cosine sim.
# High similarity = cookie-cutter applications
elig_vectors = elig_bool[q_cols].values  # per app
# Group by attorney, compute mean pairwise cosine sim within group
```

**`attorney_financial_similarity`:**
```python
# For each attorney's clients, compute std of (income, assets, liabilities)
# Low std across many clients = suspicious cookie-cutter financials
# Score = 1 / (1 + std_income + std_assets + std_liabilities) per attorney
```

**`attorney_employment_cookie_cutter`:**
```python
# For each attorney's clients, count how many share the same employer_name
# If > 50% of an attorney's clients list the same employer → flag
```

### 3C — Preparer Diversity

**`preparer_client_diversity_low`:**
```python
# For each preparer, count distinct country_of_citizenship among clients
# If diversity < 3 countries AND client count > 10 → flag
```

### 3D — Employment Temporal Checks

**`overlapping_employment`:**
```python
# Per app: sort employment records by start_date
# If any end_date[i] > start_date[i+1] → overlapping employment → flag
```

**`employment_before_14`:**
```python
# If earliest employment start_date - applicant DOB < 14 years → flag
```

### 3E — Interview Waiver

**`interview_waiver`:**
```python
# From application table: uscis_interview_waived column
feat["interview_waiver"] = _to_bool_series(
    aids.map(app_lookup["uscis_interview_waived"])
).fillna(0).astype(int)
```

### 3F — Eligibility Block Contradiction

**`elig_block_contradiction`:**
```python
# Immigration questions (q10-q21) and security questions (q42-q55)
# Contradiction: claims no immigration violations (q10-q21 all NO)
# but admits security concerns (q42+ YES) → flag
```

### 3G — Mahalanobis Financial (in features, not just unsupervised)

**`mahalanobis_financial`:**
```python
# Compute Mahalanobis distance on (income, assets, liabilities, net_worth)
# Using robust covariance (MinCovDet)
from sklearn.covariance import MinCovDet
fin_cols = ["income", "assets", "liabilities", "net_worth"]
X_fin = feat[fin_cols].values
mcd = MinCovDet(support_fraction=0.75).fit(X_fin)
feat["mahalanobis_financial"] = mcd.mahalanobis(X_fin)
```

### 3H — Fix Autoencoder & Hybrid Calibration

**Problem:** AE threshold is 0.0001 (95th pct), causing only 1 anomaly above 0.5 for hybrid score.

**Fix:**
- Normalize AE scores to 0-1 range using percentile-based normalization
- Hybrid: use proper score combination with calibrated weights

```python
# In run_autoencoder: normalize reconstruction error
ae_raw = reconstruction_errors
ae_scores = (ae_raw - ae_raw.min()) / (np.percentile(ae_raw, 99) - ae_raw.min() + 1e-10)
ae_scores = np.clip(ae_scores, 0, 1)
```

---

## Phase 4: Problem Analysis & Challenge Resolution

### 4A — Known Issues from v1 Run

| Issue | Impact | Root Cause | Fix |
|-------|--------|-----------|-----|
| SMOTENC failed, fell back to class weights | Moderate | `n_jobs` param not in SMOTENC version | Remove `n_jobs`, use default |
| LightGBM warning "X does not have valid feature names" | Cosmetic | Feature matrix passed as numpy array not DataFrame | Pass DataFrame with column names |
| XGBoost `use_label_encoder` warning | Cosmetic | Deprecated param in XGBoost 2+ | Remove `use_label_encoder=False` from params |
| AE scores near-zero (threshold 0.0001) | Significant | Scores not normalized to 0-1 | Percentile normalization (3H) |
| Hybrid score only 1 anomaly | Significant | AE scores too small, hybrid threshold 0.5 too high | Calibrate both components (3H) |
| Benford flags 94.6% of apps | Moderate | Over-sensitive flagging | Increase deviation threshold |
| SHAP requires monkey-patch for XGBoost 3.x | Fragile | SHAP/XGBoost version incompatibility | Pin versions or keep patch |

### 4B — Data Quality Challenges

| Challenge | Detection | Mitigation |
|-----------|-----------|-----------|
| Synthetic data may not reflect real fraud distributions | Compare feature distributions to literature baselines | Document expected vs actual in metrics |
| Class imbalance (6.4% fraud) | Handled by SMOTE/scale_pos_weight | Add focal loss (E7), threshold optimization |
| Label noise (anomaly ≠ fraud) | Some anomaly records may be mislabeled | Use fraud_manifest only, not anomaly_manifest, for supervised labels |
| Feature leakage from zip_fraud_rate | Target encoding leaks | Bayesian smoothing + out-of-fold encoding |
| High cardinality categoricals (employer names) | Too many unique values for one-hot | Frequency encoding + target encoding |

### 4C — Performance Challenges

| Challenge | Current | Target | Fix |
|-----------|---------|--------|-----|
| NLP TF-IDF on large attorney groups | Untested | < 60s total | Cap at 500 apps per group |
| OC-SVM O(n²) on full data | N/A | < 120s | Subsample to 50K clean apps |
| Node2Vec 10+ minutes | 10 min | Keep | Already optimized, parallelize walks |
| Full pipeline re-run time | 25 min | < 30 min | Acceptable |
| Feature matrix size | ~50MB | ~80MB | Acceptable for local + Databricks |

### 4D — Model Robustness Challenges

| Challenge | Risk | Mitigation |
|-----------|------|-----------|
| Concept drift (fraud patterns change) | High in production | Implement monitoring, A/B champion/challenger |
| Adversarial attacks (fraudsters adapt) | Medium | Ensemble diversity, unsupervised models catch novel patterns |
| Model staleness | High | Scheduled retraining, drift detection triggers |
| Overfit to synthetic data patterns | Medium | Cross-validation, holdout test set evaluation |

---

## Phase 5: Hardening & Output Package

### 5A — Error Handling & Resilience

Every pipeline script gets:
```python
# Graceful degradation for every optional component
try:
    result = expensive_operation()
except Exception as e:
    LOG.warning(f"Component failed: {e} — using fallback")
    result = safe_default_value
```

- All model training wrapped in try/except with fallback to simpler model
- All file I/O validates paths before read/write
- All feature engineering functions validate input DataFrames
- Pipeline continues even if individual components fail

### 5B — Comprehensive Output Package

Every pipeline run produces these measurable outputs:

#### Feature Engineering (`02_features.py`)
| Output File | Contents |
|-------------|----------|
| `data/features/feature_matrix.parquet` | 200K × ~195 features |
| `data/features/labels.parquet` | 200K × 3 (app_id, is_fraud, fraud_type) |
| `data/features/feature_report.json` | Per-feature stats: mean, std, non-zero count, NaN count, correlation with label |

#### Supervised Models (`03_supervised.py`)
| Output File | Contents |
|-------------|----------|
| `data/models/xgboost.joblib` | Trained XGBoost model |
| `data/models/xgboost_focal.joblib` | Focal loss XGBoost |
| `data/models/lightgbm.joblib` | Trained LightGBM |
| `data/models/rf.joblib` | Trained Random Forest |
| `data/models/catboost.joblib` | Trained CatBoost |
| `data/results/supervised_metrics.json` | PR-AUC, ROC-AUC, F1, F2, precision, recall per model |
| `data/results/feature_importance.parquet` | Feature importance for all models |
| `data/results/optimal_thresholds.json` | F2-optimal thresholds per model |
| `data/results/supervised_predictions.parquet` | Per-app predictions from each model |
| `data/results/confusion_matrices.json` | Per-model confusion matrices |
| `data/results/cv_fold_metrics.json` | 5-fold CV results per model |

#### Unsupervised Models (`04_unsupervised.py`)
| Output File | Contents |
|-------------|----------|
| `data/models/iso_forest.joblib` | Isolation Forest |
| `data/models/autoencoder.pt` | Autoencoder weights |
| `data/models/vae.pt` | VAE weights |
| `data/models/lof.joblib` | Local Outlier Factor |
| `data/models/copod.joblib` | COPOD |
| `data/models/ocsvm.joblib` | One-Class SVM |
| `data/models/deep_svdd.pt` | Deep SVDD |
| `data/results/unsupervised_scores.parquet` | Per-app anomaly scores (9 columns) |
| `data/results/unsupervised_metrics.json` | Per-model overlap with known fraud |
| `data/results/novel_anomalies.parquet` | ML-detected, rules-missed anomalies |
| `data/results/score_distributions.json` | Percentile stats for each score |

#### Graph Analytics (`05_graph.py`)
| Output File | Contents |
|-------------|----------|
| `data/results/graph_features.parquet` | Node metrics + embeddings per app |
| `data/results/fraud_rings.parquet` | Cluster ID, size, fraud density, members |
| `data/results/graph_summary.json` | Node/edge counts, component stats, community stats |
| `data/models/node2vec.model` | Trained Node2Vec |

#### Ensemble (`06_ensemble.py`)
| Output File | Contents |
|-------------|----------|
| `data/models/meta_model.joblib` | Stacking meta-learner |
| `data/results/final_scores.parquet` | 200K rows: ensemble_score, tier, top_5_features, risk_factors |
| `data/results/shap_values.parquet` | SHAP values for 50K sampled apps |
| `data/results/model_comparison.json` | All model metrics side-by-side |
| `data/results/tier_distribution.json` | Counts per risk tier |
| `data/results/novel_detections.parquet` | ML-found, rules-missed detections |
| `data/results/summary_report.txt` | Human-readable full summary |
| `data/results/ensemble_feature_importance.json` | Meta-learner feature weights |

#### Total: **~30 output files** with measurable, auditable content.

### 5C — Validation & Sanity Checks

Built-in validation after each pipeline step:

```python
def validate_feature_matrix(feat_df, min_features=180):
    """Post-build validation."""
    checks = []
    cols = [c for c in feat_df.columns if c != "application_id"]
    checks.append(("feature_count", len(cols) >= min_features, len(cols)))
    checks.append(("no_nan", feat_df[cols].isna().sum().sum() == 0, int(feat_df[cols].isna().sum().sum())))
    checks.append(("row_count", len(feat_df) == 200000, len(feat_df)))
    checks.append(("no_inf", np.isinf(feat_df[cols].select_dtypes(include=[np.number])).sum().sum() == 0, None))
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        log.info(f"  Validation {status}: {name} ({detail})")
    return all(p for _, p, _ in checks)
```

### 5D — Reproducibility

- All random seeds fixed: `42` everywhere
- Model hyperparameters logged to JSON
- Feature engineering deterministic (no random sampling in features)
- Git hash recorded in output metadata
- Pipeline version stamped in all outputs

---

## Phase 6: Databricks App — HYDRA Dashboard

### Architecture

Follows existing app pattern: Flask backend + HTML/CSS/JS frontend (single-page with tab navigation). SQL Statements API for Databricks connectivity. Can also run locally against Parquet files.

```
i485-analyzer/
  app/
    __init__.py
    main.py              ← Flask app, API routes, dual-mode (local/Databricks)
    dashboard.py          ← v1 dashboard (existing)
    hydra_dashboard.py    ← v2 HYDRA dashboard (NEW)
    hydra_pages/
      home.py             ← Home page renderer
      results_supervised.py
      results_unsupervised.py
      results_graph.py
      results_ensemble.py
      training.py
      augmentation.py
      overview.py
```

### Dual-Mode Operation

```python
# In main.py:
MODE = os.environ.get("HYDRA_MODE", "local")  # "local" or "databricks"

if MODE == "local":
    # Load from local Parquet files
    data = load_local_data()
elif MODE == "databricks":
    # Load via SQL Statements API
    data = load_databricks_data()
```

### Page Specifications

#### Page 1: Home — Pipeline Control Center

**URL:** `/` or `/home`

**Layout:**
- Top banner: HYDRA logo, pipeline status indicator (green/yellow/red)
- KPI cards row: Total Applications | Fraud Rate | Models Trained | Ensemble PR-AUC
- **Run Buttons** section (centered, prominent):

| Button | Action | Endpoint |
|--------|--------|----------|
| Run Feature Engineering | Execute `02_features.py` | `POST /api/run/features` |
| Run Supervised Models | Execute `03_supervised.py` | `POST /api/run/supervised` |
| Run Unsupervised Models | Execute `04_unsupervised.py` | `POST /api/run/unsupervised` |
| Run Graph Analytics | Execute `05_graph.py` | `POST /api/run/graph` |
| Run Ensemble + SHAP | Execute `06_ensemble.py` | `POST /api/run/ensemble` |
| Run Full Pipeline | Execute all in sequence | `POST /api/run/full` |

- Each button shows: last run time, duration, status (success/fail)
- Progress indicator during runs (WebSocket or polling)
- Pipeline DAG visualization showing dependencies

**API Implementation:**
```python
import subprocess, threading

_run_status = {}

@app.route("/api/run/<step>", methods=["POST"])
def run_step(step):
    scripts = {
        "features": "notebooks/02_features.py",
        "supervised": "notebooks/03_supervised.py",
        "unsupervised": "notebooks/04_unsupervised.py",
        "graph": "notebooks/05_graph.py",
        "ensemble": "notebooks/06_ensemble.py",
    }
    if step == "full":
        # Run all in sequence
        def _run_all():
            for s in ["features", "supervised", "unsupervised", "graph", "ensemble"]:
                _run_status[s] = {"status": "running", "start": time.time()}
                result = subprocess.run(
                    ["python", scripts[s]], capture_output=True, text=True,
                    cwd=str(PROJECT_ROOT), timeout=1800
                )
                _run_status[s] = {
                    "status": "success" if result.returncode == 0 else "failed",
                    "duration": time.time() - _run_status[s]["start"],
                    "output": result.stdout[-2000:],
                    "error": result.stderr[-1000:] if result.returncode != 0 else "",
                }
        threading.Thread(target=_run_all, daemon=True).start()
        return jsonify({"status": "started"})
    # ... single step execution
```

---

#### Page 2: Results — Supervised Models

**URL:** `/results/supervised`

**Layout:**
- **Model Comparison Table:**
  | Model | PR-AUC | ROC-AUC | F1 | F2 | Precision | Recall | Threshold | Train Time |
  |-------|--------|---------|----|----|-----------|--------|-----------|------------|
  (populated from `supervised_metrics.json`)

- **Feature Importance Chart:** Top 30 features, horizontal bar chart (from `feature_importance.parquet`)
- **Confusion Matrices:** Side-by-side heatmaps for each model
- **PR Curves:** Overlaid precision-recall curves for all models
- **Cross-Validation Results:** Fold-by-fold metrics table
- **Prediction Distribution:** Histogram of predicted probabilities per model

**Data Source:** `data/results/supervised_metrics.json`, `data/results/feature_importance.parquet`, `data/results/supervised_predictions.parquet`

---

#### Page 3: Results — Unsupervised Models

**URL:** `/results/unsupervised`

**Layout:**
- **Model Summary Table:**
  | Model | Anomalies Flagged | Fraud Overlap | Precision | Recall | Score Mean | Score P95 |
  (populated from `unsupervised_scores.parquet` + labels)

- **Score Distribution Charts:** One histogram per model (9 charts in 3×3 grid)
- **Novel Anomaly Analysis:**
  - Count by detection method combination
  - Top 20 novel anomalies with detail columns
  - Venn diagram (approximate) showing overlap between top 3 methods

- **Autoencoder Training Curve:** Loss vs epoch (if logged)
- **Benford's Law Chart:** Expected vs observed first-digit distribution

**Data Source:** `data/results/unsupervised_scores.parquet`, `data/results/novel_anomalies.parquet`, `data/results/unsupervised_metrics.json`

---

#### Page 4: Results — Graph Analytics

**URL:** `/results/graph`

**Layout:**
- **Graph Statistics:**
  | Metric | Value |
  |--------|-------|
  | Total nodes | 771K |
  | Total edges | 658K |
  | Connected components | N |
  | Communities (Louvain) | N |
  | Fraud rings (HDBSCAN) | 554 |

- **Fraud Ring Table:** Top 50 fraud rings by size and fraud density
  | Ring ID | Size | Fraud Density | Members (app IDs) | Common Pattern |
- **Community Fraud Density Distribution:** Histogram
- **Node2Vec Embedding Visualization:** 2D t-SNE projection, colored by fraud/clean
- **Network Centrality Distributions:** Degree, betweenness, PageRank histograms

**Data Source:** `data/results/graph_features.parquet`, `data/results/fraud_rings.parquet`, `data/results/graph_summary.json`

---

#### Page 5: Results — Ensemble & SHAP

**URL:** `/results/ensemble`

**Layout:**
- **Ensemble Performance:**
  - PR-AUC, ROC-AUC, F1, F2 (large KPI cards)
  - Comparison: ensemble vs best individual model

- **Risk Tier Distribution:**
  | Tier | Label | Score Range | Count | % |
  |------|-------|------------|-------|---|
  | 1 | Clean | 0.0–0.3 | 187,179 | 93.6% |
  | 2 | Monitor | 0.3–0.6 | 1,594 | 0.8% |
  | 3 | Review | 0.6–0.8 | 1,149 | 0.6% |
  | 4 | Hold | 0.8–1.0 | 10,078 | 5.0% |

- **SHAP Global Feature Importance:** Top 20 features by mean |SHAP|, bar chart
- **SHAP Waterfall Charts:** Interactive — select an application ID, see its SHAP waterfall
- **Model Contribution Analysis:** How much does each Level 0 feature contribute to ensemble
- **Novel Detections Table:** 576 ML-found, rules-missed cases with top risk factors

**Data Source:** `data/results/final_scores.parquet`, `data/results/shap_values.parquet`, `data/results/model_comparison.json`

---

#### Page 6: Training — Model Configuration & Retraining

**URL:** `/training`

**Layout:**
- **Current Model Configuration:**
  - Expandable panels for each model showing hyperparameters
  - JSON editor for modifying hyperparameters

- **Retrain Controls:**
  | Model | Last Trained | Performance | Action |
  |-------|-------------|-------------|--------|
  | XGBoost | 2026-03-31 | PR-AUC 0.79 | [Retrain] |
  | LightGBM | 2026-03-31 | PR-AUC 0.78 | [Retrain] |
  | ... | | | |

- **Hyperparameter Tuning:**
  - Select model → set search ranges → run Optuna/grid search
  - Results table showing parameter combinations and scores

- **Training History:**
  - Timeline of all training runs with metrics
  - Compare any two runs side-by-side

- **Label Management:**
  - Upload corrected labels (CSV with application_id, is_fraud)
  - View label distribution before/after correction
  - Trigger retrain with new labels

**API Endpoints:**
```
POST /api/train/retrain/<model_name>   — retrain specific model
POST /api/train/tune/<model_name>      — hyperparameter search
POST /api/train/upload-labels          — upload corrected labels
GET  /api/train/history                — training run history
```

---

#### Page 7: Augmentation — Additional Filters & Tools

**URL:** `/augmentation`

**Layout:**
- **Custom Feature Builder:**
  - Select two existing features → create ratio, difference, product, or boolean combination
  - Preview distribution of new feature
  - Save to feature matrix

- **Filter Builder:**
  - Build complex filters: "Show apps where attorney_app_count > 100 AND marriage_to_filing_days < 90"
  - Save filters as named presets
  - Apply filter to any results page

- **External Data Integration:**
  - Upload supplementary data (CSV) to join with feature matrix
  - Map columns to application_id
  - Preview joined result before saving

- **Rule Engine:**
  - Define custom fraud rules: IF condition THEN flag
  - Rules run alongside ML models
  - Rule performance tracking (how many apps each rule catches)

- **Threshold Tuning:**
  - Interactive slider for each risk tier threshold
  - Real-time preview of tier distribution changes
  - Cost-benefit analysis: "Moving threshold from 0.3 to 0.25 adds X apps to Tier 2"

**API Endpoints:**
```
POST /api/augment/add-feature          — create derived feature
POST /api/augment/save-filter          — save filter preset
POST /api/augment/upload-data          — upload supplementary data
POST /api/augment/add-rule             — add custom fraud rule
POST /api/augment/adjust-thresholds    — modify tier thresholds
```

---

#### Page 8: Overview — Full Dataset Fraud Analysis

**URL:** `/overview`

**Layout:**
- **Executive Summary Banner:**
  - Total apps: 200,000
  - ML-flagged fraud: X,XXX (X.X%)
  - Fraud rings: XXX clusters
  - Novel detections: XXX
  - Ensemble PR-AUC: 0.XXXX

- **Full Application Table** (paginated, searchable, sortable):
  | App ID | Name | Status | Ensemble Score | Tier | Top Risk Factors | Fraud Ring | SHAP Top Feature |
  - Click any row → expand to full detail panel
  - Color-coded by tier (green/yellow/orange/red)
  - Search by app ID, name, attorney, address
  - Filter by tier, score range, fraud ring membership

- **Fraud Heatmap:**
  - Geographic heatmap by state (colored by fraud rate)
  - Click state → drill down to ZIP-level

- **Timeline View:**
  - Filing volume over time, colored by fraud density
  - Annotate spikes with detected coordinated filing events

- **Attorney/Preparer Leaderboard:**
  | Attorney Bar # | App Count | Denial Rate | Avg Fraud Score | Flagged Apps |
  - Click attorney → see all their apps

- **Fraud Pattern Breakdown:**
  | Pattern | ML-Detected Count | Ground Truth Count | Precision | Recall |
  |---------|-------------------|-------------------|-----------|--------|
  | SSN Sharing | XXX | XXX | XX% | XX% |
  | Address Ring | XXX | XXX | XX% | XX% |
  | Attorney Mill | XXX | XXX | XX% | XX% |
  | ... | | | | |

- **Export Controls:**
  - Export full scored dataset (CSV/Parquet)
  - Export fraud-flagged apps only
  - Export by tier
  - Generate PDF report

**Data Source:** `data/results/final_scores.parquet` + all other result files joined together.

---

### App Styling

Follows existing Blueprint App Template from `dashboard.py`:
- Dark sidebar navigation
- Card-based layouts
- Databricks blue accent color (#1B3139 sidebar, #FF3621 for alerts)
- Chart.js for all charts
- DataTables.js for sortable/searchable tables
- Responsive CSS grid layout
- Loading spinners during API calls

### App Deployment

**Local mode:**
```bash
cd i485-analyzer
HYDRA_MODE=local python app/main.py
# Opens on http://localhost:8060
```

**Databricks mode:**
```yaml
# app.yaml
command:
  - python
  - app/main.py
env:
  - name: HYDRA_MODE
    value: databricks
  - name: I485_SCHEMA
    value: lho_ucm.i485_form
  - name: DATABRICKS_WAREHOUSE_ID
    value: 0c5bd90f54a5bd8b
```

---

## Execution Sequence

### Wave 1: Enhancement Builds (E1–E9)
```
1. Edit 02_features.py — add E1-E6 (5 new functions + 10 placeholder fills)
2. Run 02_features.py — verify ~195 features
3. Edit 03_supervised.py — add focal loss (E7)
4. Run 03_supervised.py — verify PR-AUC improvement
5. Edit 04_unsupervised.py — add OC-SVM (E8)
6. Run 04_unsupervised.py — verify 8 score columns
7. Edit 06_ensemble.py — add ocsvm_score (E9)
8. Run 06_ensemble.py — verify ensemble improvement
```

### Wave 2: Model Expansion (Phase 2)
```
9. Add CatBoost to 03_supervised.py
10. Add VAE + Deep SVDD to 04_unsupervised.py
11. Run updated supervised + unsupervised
12. Update ensemble for new models
13. Run ensemble — measure improvement
```

### Wave 3: Gap Audit & Full Builds (Phase 3)
```
14. Fill remaining 7 placeholders (3B-3G)
15. Fix AE/hybrid calibration (3H)
16. Run full pipeline end-to-end
17. Validate all outputs
```

### Wave 4: Problem Analysis & Hardening (Phases 4-5)
```
18. Fix SMOTENC, LightGBM warnings, Benford sensitivity
19. Add validation checks to every script
20. Add comprehensive output files
21. Run full pipeline — final metrics
```

### Wave 5: Databricks App (Phase 6)
```
22. Build hydra_dashboard.py (all 8 pages)
23. Build API endpoints for run/train/augment
24. Test local mode
25. Deploy to Databricks
```

**Estimated total build time: ~6-8 hours of implementation.**
**Estimated total run time: ~45 minutes per full pipeline execution.**
