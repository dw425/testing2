# I-485 Fraud Detection — Enhancement Build Plan v2

## Context

The v1 pipeline is fully operational: 136 features, 10 models (3 supervised + 7 unsupervised), graph analytics (Node2Vec + HDBSCAN + Louvain), stacking ensemble with SHAP explainability, and 4-tier risk scoring. **Ensemble PR-AUC: 0.9573, F2: 0.9012, Recall: 0.9072.**

This plan adds **~45 new features**, **2 new models**, and **upgrades** to existing models based on gaps identified from two research papers on USCIS fraud detection methodology.

All enhancements run 100% locally on existing synthetic data. No new data generation needed.

---

## Current Pipeline (v1 Baseline)

| Script | What It Does | Features/Models |
|--------|-------------|-----------------|
| `02_features.py` | 136 features across 12 categories (1A–1L) | 136 columns in `feature_matrix.parquet` |
| `03_supervised.py` | XGBoost, LightGBM, Random Forest | 3 models, PR-AUC ~0.79 |
| `04_unsupervised.py` | IF, AE, Hybrid, LOF, COPOD, Mahalanobis, Benford | 7 score columns in `unsupervised_scores.parquet` |
| `05_graph.py` | NetworkX graph, Node2Vec (64d), HDBSCAN, Louvain | 74 columns in `graph_features.parquet` |
| `06_ensemble.py` | Stacking meta-learner + SHAP + 4-tier scoring | PR-AUC 0.9573 |

### Available Synthetic Data Tables (21 Parquets)

| Table | Rows | Key Columns Available for New Features |
|-------|------|---------------------------------------|
| `application` | 200K | `filing_date`, `atty_state_bar_number`, `atty_name`, `status` |
| `applicant_info` | 200K | `date_of_birth`, `ssn`, `passport_number`, `arrival_date`, `country_of_citizenship` |
| `marital_history` | 219K | `marital_status`, `spouse_family_name`, `spouse_given_name`, `marriage_date`, `marriage_end_date`, `marriage_end_reason`, `spouse_country_of_birth` |
| `addresses` | 444K | `address_type`, `street`, `city`, `state`, `zip_code`, `country` |
| `employment_history` | 507K | `employer_name`, `occupation`, `start_date`, `end_date`, `is_current`, `employer_state` |
| `additional_information` | 60K | `additional_text`, `page_number`, `part_number` |
| `contacts_signatures` | 288K | `contact_type`, `family_name`, `given_name`, `signature_date` |
| `public_charge` | 200K | `household_income`, `household_assets`, `household_liabilities`, `education_level` |
| `filing_category` | 200K | `category_group`, `applicant_type`, `principal_a_number` |
| `children` | 413K | `date_of_birth`, `country_of_birth`, `also_applying` |

---

## Enhancement Summary

| # | Enhancement | File | New Features/Scores | Priority | Effort |
|---|------------|------|--------------------:|----------|--------|
| E1 | Marriage fraud indicators | `02_features.py` | 8 | P1 | Low |
| E2 | Temporal velocity features (rolling windows) | `02_features.py` | 9 | P1 | Low |
| E3 | NLP text similarity (boilerplate detection) | `02_features.py` | 5 | P2 | Medium |
| E4 | Employment / shell company detection | `02_features.py` | 7 | P2 | Low |
| E5 | Geographic features (DBSCAN, impossibility) | `02_features.py` | 6 | P2 | Medium |
| E6 | Fill existing placeholder features | `02_features.py` | 10 (upgrades) | P2 | Low |
| E7 | Focal loss for XGBoost | `03_supervised.py` | — (training upgrade) | P3 | Low |
| E8 | One-Class SVM | `04_unsupervised.py` | 1 | P3 | Low |
| E9 | Ensemble picks up new score | `06_ensemble.py` | — (plumbing) | P3 | Low |
| **Total** | | | **~45 new + 10 upgraded** | | |

---

## E1 — Marriage Fraud Indicators (8 new features)

**File:** `02_features.py`
**New function:** `build_1m_marriage_fraud(app_df, info_df, marital_df, feat)`
**Insert after:** line 1406 (after `build_1l_temporal` call)
**Add to `main()`:** `feat = build_1m_marriage_fraud(app_df, info_df, marital_df, feat)`

### Data Sources
- `marital_history.parquet`: `marriage_date`, `marriage_end_date`, `marriage_end_reason`, `spouse_family_name`, `spouse_given_name`, `spouse_country_of_birth`
- `applicant_info.parquet`: `date_of_birth`
- `application.parquet`: `filing_date`

### Features

| Feature | Logic | Source Table(s) | Detects |
|---------|-------|-----------------|---------|
| `spouse_age_gap` | abs(applicant DOB − spouse DOB) in years. Requires joining `marital_history` to `applicant_info` via `application_id`, then computing age diff. Use most recent marriage. | `applicant_info.date_of_birth` + `marital_history` (spouse DOB not directly available — estimate from marriage date and applicant age if spouse DOB absent; otherwise set NaN) | Marriage fraud (large age gaps) |
| `marriage_to_filing_days` | `filing_date − marriage_date` in days. Join `application.filing_date` to `marital_history.marriage_date`. Use most recent marriage. Negative = married after filing. | `application` + `marital_history` | Marriage fraud (very short = suspicious) |
| `marriage_to_filing_suspicious` | `marriage_to_filing_days` < 90 days | derived | Sham marriage |
| `serial_petitioner` | Count of distinct `principal_a_number` values that this applicant's A-number appears as. From `filing_category.principal_a_number`. If same person is principal on many apps → serial petitioner. | `filing_category` + `applicant_info` | Serial petitioner fraud |
| `multiple_marriage_based` | Count apps where same `principal_a_number` appears with `category_group` in ('FAMILY','IMMEDIATE_RELATIVE'). Flag if count > 2. | `filing_category` | Serial marriage petitioner |
| `rapid_remarriage` | `marriage_date − marriage_end_date` of prior marriage < 90 days. Sort marital records by `sort_order`, compute gap. | `marital_history` | Sham marriage |
| `spouse_country_mismatch` | Spouse country of birth is very different from applicant's country of citizenship (cross-continental marriages at higher rate). Binary flag. | `marital_history.spouse_country_of_birth` + `applicant_info.country_of_citizenship` | Marriage fraud pattern |
| `marriage_after_filing` | `marriage_date > filing_date`. Should not normally happen. | `application` + `marital_history` | Data integrity / fraud |

### Implementation Notes
- Spouse DOB is **not** in `marital_history` — we cannot compute exact `spouse_age_gap`. Instead use: if available from a linked applicant_info record (via `spouse_family_name` + `spouse_given_name` fuzzy match to another app's `applicant_info`), compute the gap. Otherwise, leave as 0. This can be enhanced later if spouse DOB is added.
- For `serial_petitioner`: group `filing_category` by `principal_a_number`, count rows. Map back to each application.
- All date arithmetic uses `pd.to_datetime(errors='coerce')` for safety.

---

## E2 — Temporal Velocity Features (9 new features)

**File:** `02_features.py`
**New function:** `build_1n_temporal_velocity(app_df, contacts_df, addr_df, feat)`
**Insert after:** E1 call in `main()`

### Data Sources
- `application.parquet`: `filing_date`, `atty_state_bar_number`
- `contacts_signatures.parquet`: `contact_type`, `family_name`, `given_name`, `signature_date`
- `addresses.parquet`: `address_type`, `street`, `city`, `state`

### Features

| Feature | Logic | Detects |
|---------|-------|---------|
| `atty_7d_rolling_count` | For each app, count how many apps the same attorney filed within ±7 days of this app's `filing_date`. Use `atty_state_bar_number` + `filing_date` window. | Attorney mill batch operations |
| `atty_30d_rolling_count` | Same as above, ±30 days. | Attorney mill sustained volume |
| `atty_90d_rolling_count` | Same as above, ±90 days. | Attorney mill sustained volume |
| `preparer_7d_rolling_count` | For each app, count how many apps the same preparer (from `contacts_signatures` where `contact_type=PREPARER`, keyed by `family_name\|given_name`) filed within ±7 days. | Notario fraud |
| `preparer_30d_rolling_count` | Same, ±30 days. | Notario fraud |
| `address_30d_filing_count` | For each app, count how many other apps share the same current address AND were filed within ±30 days. | Address fraud ring batch |
| `atty_filing_acceleration` | For each attorney, compute the ratio: (apps in last 30 days) / (apps in last 90 days) × 3. Values > 1.5 = accelerating. Map to each app. | Sudden attorney volume spike |
| `filing_hour_suspicious` | If `signature_date` timestamp has hour information, flag filings outside business hours (before 7am, after 8pm, weekends). Use `contacts_signatures` where `contact_type=APPLICANT`. | Batch automated submissions |
| `volume_anomaly_zscore` | Per-week filing volume z-score using ARIMA-like approach: compute rolling 4-week mean and std of weekly filing counts, then z-score the week this app was filed in. If z > 3, flag. | Coordinated fraud campaigns |

### Implementation Notes
- Rolling window counts: group by attorney/preparer, for each app compute count of apps within the window. Use vectorized merge + date filtering rather than per-row loops.
- **Efficient approach for rolling counts:**
  ```python
  # For each attorney, sort by filing_date
  # Use pd.merge_asof or explicit window: for each app,
  # count apps with same atty where abs(filing_date diff) <= window
  # Vectorized: explode atty groups, compute pairwise date diffs
  ```
- For 200K rows, avoid O(n²) — use groupby + rolling merge.
- `filing_hour_suspicious`: parse `signature_date` as datetime, extract hour. If all timestamps are date-only (no time), this feature will be all zeros — that's fine.

---

## E3 — NLP Text Similarity / Boilerplate Detection (5 new features)

**File:** `02_features.py`
**New function:** `build_1o_nlp_similarity(addl_info_text_df, app_df, contacts_df, feat)`
**Insert after:** E2 call in `main()`
**Dependencies:** `scikit-learn` (already installed — uses `TfidfVectorizer`, `cosine_similarity`)

### Data Sources
- `additional_information.parquet` (60K rows): `application_id`, `additional_text`
- `application.parquet`: `atty_state_bar_number`
- `contacts_signatures.parquet`: preparer name

### Features

| Feature | Logic | Detects |
|---------|-------|---------|
| `has_additional_text` | Binary: does this app have any `additional_information` records? | — |
| `additional_text_length` | Total character count of concatenated `additional_text` per app. | — |
| `text_similarity_max_same_atty` | For each app that has text AND shares an attorney with other apps, compute TF-IDF cosine similarity to each other app with same attorney. Take max. | Attorney mill / notario boilerplate |
| `text_similarity_above_085` | Binary: `text_similarity_max_same_atty >= 0.85`. Per research, this threshold flags plagiarism. | Plagiarized narratives |
| `text_duplicate_cluster_size` | Number of apps in this app's "text cluster" — apps from the same attorney/preparer with pairwise cosine similarity > 0.85. | Cookie-cutter applications |

### Implementation Notes
- Only ~60K rows have `additional_text`. The other 140K get zeros for all NLP features.
- **Performance-critical:** Computing all-pairs cosine similarity on 60K docs is O(n²) and expensive. Instead:
  1. Group apps by `atty_state_bar_number` (only compare within same attorney).
  2. For each attorney group, build TF-IDF matrix and compute pairwise cosine similarity.
  3. For attorneys with > 500 apps, sample 500 to keep runtime manageable.
- Use `sklearn.feature_extraction.text.TfidfVectorizer(max_features=5000, stop_words='english')`.
- Use `sklearn.metrics.pairwise.cosine_similarity` on the sparse TF-IDF matrix (efficient for sparse).
- Concatenate all `additional_text` rows per `application_id` before TF-IDF.

### Pseudocode
```python
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Concatenate text per app
text_per_app = addl_text_df.groupby("application_id")["additional_text"].apply(" ".join)

# Merge with attorney
text_atty = text_per_app.to_frame().merge(app_df[["application_id", "atty_state_bar_number"]], ...)

# For each attorney group:
for atty, group in text_atty.groupby("atty_state_bar_number"):
    if len(group) < 2:
        continue
    tfidf = TfidfVectorizer(max_features=5000, stop_words='english')
    X = tfidf.fit_transform(group["additional_text"])
    sim_matrix = cosine_similarity(X)  # (n, n) — n = group size
    np.fill_diagonal(sim_matrix, 0)
    max_sim = sim_matrix.max(axis=1)  # max similarity to any other app in group
    # Map back to features
```

---

## E4 — Employment & Shell Company Detection (7 new features)

**File:** `02_features.py`
**New function:** `build_1p_employment_enhanced(emp_df, pc_df, app_df, feat)`
**Insert after:** E3 call in `main()`

### Data Sources
- `employment_history.parquet` (507K rows): `employer_name`, `occupation`, `start_date`, `end_date`, `is_current`, `employer_state`
- `public_charge.parquet`: `household_income`
- `application.parquet`: `atty_state_bar_number`

### Features

| Feature | Logic | Detects |
|---------|-------|---------|
| `employer_app_count` | Number of I-485 apps sharing the same current `employer_name`. Group `employment_history` where `is_current=True` by `employer_name`, count. | Shell company / petition mill |
| `employer_high_volume` | `employer_app_count > 20`. Flag. | Shell company |
| `employer_extreme_volume` | `employer_app_count > 100`. Flag. | Shell company |
| `employer_atty_concentration` | For the current employer, how many distinct attorneys represent its applicants? Low count + high app count = suspicious. Ratio: `employer_app_count / employer_distinct_attorneys`. | Coordinated employer-attorney fraud |
| `salary_occupation_zscore` | Group apps by `occupation`, compute mean and std of `household_income`. Z-score each app's income within its occupation group. Extreme values = suspicious. | Salary inflation/deflation |
| `employment_gap_max_days` | Max gap (in days) between consecutive employment records per app. Sort by `start_date`, compute gap between `end_date` of previous and `start_date` of next. | Employment fabrication |
| `employment_state_address_mismatch` | Current employer state != current address state. Join `employment_history` (where `is_current=True`) with `addresses` (where `address_type=CURRENT_PHYSICAL`). | Geographic impossibility |

### Implementation Notes
- **Replaces placeholders:** `employment_gap_max_months` (line 1203), `employment_gap_flag` (line 1204), `address_employment_mismatch` (line 942) — these currently output 0.
- `employer_name` normalization: uppercase, strip whitespace, collapse multiple spaces. Don't do fuzzy matching (too expensive for 507K rows).
- `salary_occupation_zscore`: only compute for occupations with >= 10 apps. Otherwise set 0.
- Employment dates: use `pd.to_datetime(errors='coerce')` on `start_date` and `end_date`.

---

## E5 — Geographic Features (6 new features)

**File:** `02_features.py`
**New function:** `build_1q_geographic(addr_df, emp_df, app_df, info_df, feat)`
**Insert after:** E4 call in `main()`
**Dependencies:** `scikit-learn` (DBSCAN already available)

### Data Sources
- `addresses.parquet` (444K rows): `address_type`, `street`, `city`, `state`, `zip_code`, `country`
- `employment_history.parquet`: `employer_state`
- `application.parquet`: `filing_date`
- `applicant_info.parquet`: `country_of_citizenship`

### Features

| Feature | Logic | Detects |
|---------|-------|---------|
| `zip_fraud_rate` | Per-ZIP-code fraud base rate. Group apps by current address ZIP, compute fraud rate from labels. Map back. (Uses label — compute on train split only during supervised training. For feature engineering, use full-data rate as proxy.) | Regional fraud hotspots |
| `po_box_address` | Binary: street address contains "PO BOX", "P.O. BOX", "P O BOX". | P.O. box fraud |
| `address_dbscan_cluster_size` | DBSCAN clustering on (state, city, ZIP) encoded as integers. Use `eps=0` (exact match), `min_samples=5`. Cluster size for each app. This is more principled than simple string matching. | Address mills |
| `foreign_address_domestic_filing` | Current address `country != 'US'` and `country != 'USA'` and `country` is not null/empty, but filing is domestic. | Suspicious filing location |
| `address_unrelated_cluster_real` | At same address (street+city+state), count distinct `family_name` from `applicant_info`. If > 5 unrelated families → flag. Replaces placeholder. | Address fraud ring |
| `multi_state_employment` | Count of distinct `employer_state` values across employment records. > 3 states = suspicious mobility. | Employment fabrication |

### Implementation Notes
- **Replaces placeholders:** `foreign_address_domestic_filing` (line 953), `address_unrelated_cluster` (line 952).
- `zip_fraud_rate`: this uses labels, which means it leaks target info if used naively. **Mitigation:** compute rate with additive smoothing (Bayesian): `(fraud_count + 1) / (total_count + 10)`. This is standard practice for target-encoded features when training will use out-of-fold.
- DBSCAN on addresses: encode `state|city|zip` as categorical, convert to integer codes. DBSCAN with `metric='hamming'` or simply group by the concatenated key (effectively same as cluster by exact address match but with proper cluster IDs).
- Actually, since we already have `address_cluster_size` from string matching, `address_dbscan_cluster_size` should use a slightly fuzzier approach: cluster by `zip_code` only (all apps in same ZIP), then further sub-cluster by street name similarity. For simplicity: group by `zip_code`, flag ZIP codes with > 20 apps sharing the same ZIP as dense clusters.

---

## E6 — Fill Existing Placeholder Features (10 upgrades)

**File:** `02_features.py`
**Modify existing functions** — replace `= 0  # placeholder` with real logic.

| Placeholder (current line) | Real Logic | Function to Modify |
|---------------------------|-----------|-------------------|
| `passport_country_mismatch` (line 406) | `applicant_info.passport_country != applicant_info.country_of_citizenship` | `build_1b_demographic` |
| `passport_expired_before_filing` (line 418) | `applicant_info.passport_expiration < application.filing_date` | `build_1b_demographic` |
| `circular_sponsorship` (line 525) | If app A's `principal_a_number` points to app B, and app B's `principal_a_number` points to app A. Join `filing_category.principal_a_number` to `applicant_info.a_number`. | `build_1c_family` |
| `address_employment_mismatch` (line 942) | Covered by E4 (`employment_state_address_mismatch`) | `build_1g_address` → remove placeholder, E4 adds real feature |
| `address_unrelated_cluster` (line 952) | Covered by E5 (`address_unrelated_cluster_real`) | `build_1g_address` → remove placeholder, E5 adds real feature |
| `foreign_address_domestic_filing` (line 953) | Covered by E5 | `build_1g_address` → remove placeholder, E5 adds real feature |
| `same_country_burst` (line 1059) | Apps filed same day + same `country_of_citizenship` > 10 | `build_1h_filing_patterns` |
| `coordinated_filing` (line 1060) | Same attorney + same day + same current address | `build_1h_filing_patterns` |
| `education_income_mismatch` (line 1236) | `education_level <= 1` (high school or below) AND `income > 200K` | `build_1k_employment` |
| `days_since_arrival` (line 1259) | `filing_date − arrival_date` in days | `build_1l_temporal` |

### Implementation Details

**`passport_country_mismatch` (build_1b_demographic, line 406):**
```python
# Replace: feat["passport_country_mismatch"] = 0
if "passport_country" in info_df.columns and "country_of_citizenship" in info_df.columns:
    pp_country = aids.map(info_lookup["passport_country"]).astype(str).str.upper().str.strip()
    cit_country = aids.map(info_lookup["country_of_citizenship"]).astype(str).str.upper().str.strip()
    feat["passport_country_mismatch"] = (
        (pp_country != cit_country) &
        (~pp_country.isin(["NAN", "NONE", ""])) &
        (~cit_country.isin(["NAN", "NONE", ""]))
    ).astype(int)
else:
    feat["passport_country_mismatch"] = 0
```

**`passport_expired_before_filing` (build_1b_demographic, line 418):**
```python
# Replace: feat["passport_expired_before_filing"] = 0
if "passport_expiration" in info_df.columns:
    pp_exp = _safe_date(aids.map(info_lookup["passport_expiration"]))
    feat["passport_expired_before_filing"] = (
        (pp_exp < filing_dates) & pp_exp.notna() & filing_dates.notna()
    ).astype(int)
else:
    feat["passport_expired_before_filing"] = 0
```

**`circular_sponsorship` (build_1c_family, line 525):**
```python
# Replace: feat["circular_sponsorship"] = 0
# Check if app A sponsors B and B sponsors A
fc = tables["filing_category"]
if "principal_a_number" in fc.columns:
    info = tables["applicant_info"]
    # Map each app's A-number
    app_anum = info.set_index("application_id")["a_number"].dropna()
    app_anum = app_anum[app_anum.astype(str).str.strip() != ""]
    # Map each app's principal (who sponsors them)
    fc_principal = fc.set_index("application_id")["principal_a_number"].dropna()
    fc_principal = fc_principal[fc_principal.astype(str).str.strip() != ""]
    # Build sponsor→sponsored mapping
    # If A sponsors B: A's a_number = B's principal_a_number
    # Circular: A sponsors B AND B sponsors A
    # Vectorized: merge on a_number = principal_a_number bidirectionally
    ... (see implementation)
```

**`same_country_burst` (build_1h_filing_patterns, line 1059):**
```python
# Join application.filing_date + applicant_info.country_of_citizenship
# Group by (filing_date, country), count > 10 → flag
```

**`coordinated_filing` (build_1h_filing_patterns, line 1060):**
```python
# Group by (atty_state_bar_number, filing_date, addr_key), count > 1 → flag
```

**`education_income_mismatch` (build_1k_employment, line 1236):**
```python
feat["education_income_mismatch"] = (
    (feat["education_level_encoded"] <= 1) & (feat["income"] > 200000)
).astype(int)
```

**`days_since_arrival` (build_1l_temporal, line 1259):**
```python
if "arrival_date" in info_df.columns:
    arr = _safe_date(aids.map(info_lookup["arrival_date"]))
    fd = _safe_date(aids.map(app_lookup["filing_date"]))
    feat["days_since_arrival"] = (fd - arr).dt.days.fillna(0).astype(int)
else:
    feat["days_since_arrival"] = 0
```

---

## E7 — Focal Loss for XGBoost (Training Upgrade)

**File:** `03_supervised.py`
**Modify:** XGBoost training section (~line 195-230)

### What Changes
Add a custom focal loss objective function to XGBoost. Focal loss (γ=2.0) down-weights easy-to-classify examples and focuses on hard examples — better than `scale_pos_weight` alone for extreme imbalance.

### Implementation
```python
def focal_loss_objective(y_pred, dtrain, gamma=2.0):
    """Custom focal loss for XGBoost."""
    y_true = dtrain.get_label()
    # Sigmoid to get probability
    p = 1.0 / (1.0 + np.exp(-y_pred))
    # Gradient and hessian for focal loss
    grad = p - y_true  # simplified gradient
    # Focal weight: (1 - p_t)^gamma where p_t = p if y=1, (1-p) if y=0
    p_t = np.where(y_true == 1, p, 1 - p)
    focal_weight = (1 - p_t) ** gamma
    grad = focal_weight * grad
    hess = focal_weight * p * (1 - p)
    hess = np.maximum(hess, 1e-6)  # numerical stability
    return grad, hess
```

### Changes to `03_supervised.py`
1. Add `focal_loss_objective` function (before training section).
2. Add a second XGBoost model: `XGBClassifier(objective=focal_loss_objective, ...)` alongside the existing one.
3. Or: replace existing XGBoost `scale_pos_weight` approach with focal loss. **Recommended:** keep both, compare metrics, save the better one.
4. Note: with custom objective, `eval_metric='aucpr'` still works for early stopping, but the default `base_score` may need adjustment.

### Actually — simpler approach
Since XGBoost 2.0+, focal loss is NOT natively supported as a string objective. The cleanest approach:
- Keep the existing `scale_pos_weight` XGBoost as-is (it works well at PR-AUC 0.7936).
- Add focal loss as an **option** tried during training, keeping whichever performs better.
- This avoids breaking the working pipeline.

---

## E8 — One-Class SVM (New Unsupervised Model)

**File:** `04_unsupervised.py`
**New function:** `run_ocsvm(X_scaled, is_fraud)`
**Insert after:** Benford section (~line 776), before "Assemble output"
**Add to output:** `ocsvm_score` column in `unsupervised_scores.parquet`

### Implementation
```python
from sklearn.svm import OneClassSVM

def run_ocsvm(X_scaled, is_fraud, max_samples=50000):
    """3H. One-Class SVM — trained on clean apps only."""
    t0 = time.time()

    # Subsample for training (OC-SVM is O(n²) in memory)
    clean_mask = is_fraud == 0
    X_clean = X_scaled[clean_mask]
    if len(X_clean) > max_samples:
        rng = np.random.RandomState(42)
        idx = rng.choice(len(X_clean), max_samples, replace=False)
        X_train = X_clean[idx]
    else:
        X_train = X_clean

    ocsvm = OneClassSVM(kernel='rbf', gamma='scale', nu=0.03)
    ocsvm.fit(X_train)

    # Score all apps — decision_function returns signed distance
    # More negative = more anomalous
    raw_scores = ocsvm.decision_function(X_scaled)
    # Normalize to 0-1 range (more anomalous = higher score)
    scores = -raw_scores  # flip sign
    scores = (scores - scores.min()) / (scores.max() - scores.min() + 1e-10)

    elapsed = time.time() - t0
    LOG.info(f"  One-Class SVM trained in {elapsed:.1f}s")

    # Save model
    joblib.dump(ocsvm, MODELS_DIR / "ocsvm.joblib")
    LOG.info(f"  Saved model to {MODELS_DIR / 'ocsvm.joblib'}")

    return scores
```

### Changes to `04_unsupervised.py`
1. Add `run_ocsvm` function.
2. Call after Benford: `ocsvm_scores = run_ocsvm(X_scaled, is_fraud)` (~line 777).
3. Add `"ocsvm_score": ocsvm_scores` to `scores_df` assembly (~line 789).
4. Add to `model_names` and `model_labels` lists for summary output.
5. **Important:** OC-SVM with RBF kernel on 200K × 136 features is memory-heavy. Train on **max 50K clean samples** (subsampled). Score all 200K at inference.

---

## E9 — Ensemble Picks Up New Score (Plumbing)

**File:** `06_ensemble.py`
**Modify:** `build_level0()` function, `unsup_cols` list (~line 249-252)

### Changes
```python
# Line 249-252: Add ocsvm_score to the unsupervised columns list
unsup_cols = [
    "if_score", "ae_score", "hybrid_score",
    "lof_score", "copod_score", "mahalanobis_score", "benford_flag",
    "ocsvm_score",  # NEW from E8
]
```

No other changes needed — the ensemble meta-learner will automatically pick up the new column as an additional Level 0 feature. Feature count goes from 77 → 78.

The new features from E1-E6 are automatically incorporated because the ensemble reads `feature_matrix.parquet` and passes it through the supervised models, which produce probabilities that feed into Level 0. The new features improve the supervised models' probabilities, which improves the ensemble.

---

## Execution Order

```bash
cd /Users/darkstar33/Documents/testing2
source .venv/bin/activate

# Step 1: Update feature engineering (E1-E6)
# Edit 02_features.py — add 5 new functions + fill 10 placeholders

# Step 2: Re-run feature engineering (~5 min)
python i485-analyzer/notebooks/02_features.py
# Expected output: 200K × ~181 features (was 136)

# Step 3: Update supervised training (E7)
# Edit 03_supervised.py — add focal loss option

# Step 4: Re-train supervised models (~5 min)
python i485-analyzer/notebooks/03_supervised.py
# Expected: PR-AUC improvement from ~0.79 → ~0.82-0.85 (more features)

# Step 5: Update unsupervised models (E8)
# Edit 04_unsupervised.py — add One-Class SVM

# Step 6: Re-train unsupervised models (~5 min)
python i485-analyzer/notebooks/04_unsupervised.py
# Expected: 8 score columns (was 7)

# Step 7: Re-run graph analytics (~10 min) — NO CHANGES needed
# Graph features are independent of tabular features
# SKIP unless graph code needs new node types

# Step 8: Update ensemble (E9) + re-run (~3 min)
# Edit 06_ensemble.py — add ocsvm_score to unsup_cols
python i485-analyzer/notebooks/06_ensemble.py
# Expected: PR-AUC improvement from 0.9573 → 0.96-0.97+
```

**Total re-run time: ~23 minutes** (steps 2+4+6+8, skipping graph)

---

## Expected Impact

| Metric | v1 Baseline | v2 Expected | Why |
|--------|------------|-------------|-----|
| Feature count | 136 | ~181 | +45 new features |
| Supervised PR-AUC (XGBoost) | 0.7936 | 0.82-0.85 | Marriage, temporal, NLP, employment features directly target fraud patterns |
| Unsupervised models | 7 | 8 | +OC-SVM |
| Ensemble PR-AUC | 0.9573 | 0.96-0.97 | Better base models + more signal diversity |
| Ensemble F2 | 0.9012 | 0.92+ | More recall from NLP + marriage features |
| Novel detections | 576 | 800+ | OC-SVM catches different anomaly shape than IF |
| SHAP top features | community_fraud_density dominant | More diverse top features | NLP/marriage features add orthogonal signal |

---

## File Change Summary

| File | Lines Added | Lines Modified | Key Changes |
|------|-----------|---------------|-------------|
| `02_features.py` | ~350 | ~30 | 5 new `build_1*` functions, 10 placeholder fills, 5 new calls in `main()` |
| `03_supervised.py` | ~30 | ~5 | `focal_loss_objective` function, optional focal loss XGBoost |
| `04_unsupervised.py` | ~50 | ~10 | `run_ocsvm` function, add to output assembly |
| `06_ensemble.py` | 0 | ~2 | Add `ocsvm_score` to `unsup_cols` list |
| **Total** | ~430 | ~47 | |

---

## Dependencies

No new pip packages needed. All enhancements use libraries already installed:
- `scikit-learn` — TfidfVectorizer, cosine_similarity, DBSCAN, OneClassSVM
- `pandas`, `numpy` — all feature engineering
- `xgboost` — focal loss uses custom objective (Python function)

---

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| NLP similarity (E3) is slow on large attorney groups | Cap at 500 apps per attorney group for pairwise comparison |
| OC-SVM (E8) is O(n²) memory | Train on max 50K subsampled clean apps |
| `zip_fraud_rate` (E5) leaks target | Use Bayesian smoothing: `(fraud + 1) / (total + 10)` |
| Focal loss (E7) might underperform scale_pos_weight | Keep both, compare, save better |
| Rolling window counts (E2) could be slow | Vectorized merge approach, not per-row loops |
| Spouse DOB not in data (E1) | Skip `spouse_age_gap` or estimate via linked records |

---

## Verification Checklist

After running the full pipeline:

- [ ] `feature_matrix.parquet` has 200K rows × ~181 columns (was 136)
- [ ] No NaN in feature matrix (all filled with 0)
- [ ] `marriage_to_filing_days` has non-zero values for married applicants
- [ ] `text_similarity_max_same_atty` has non-zero values for apps with additional_text
- [ ] `employer_app_count` has non-zero values
- [ ] `ocsvm_score` column exists in `unsupervised_scores.parquet`
- [ ] Supervised PR-AUC ≥ 0.80 (improvement over 0.7936)
- [ ] Ensemble PR-AUC ≥ 0.96
- [ ] SHAP values include new features in top-10
- [ ] All 10 placeholder features now have non-zero values
- [ ] `summary_report.txt` shows updated feature count and metrics
