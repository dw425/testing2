# I-485 Synthetic Data Generator

Synthetic data generation pipeline for USCIS Form I-485 (Application to Register Permanent Residence or Adjust Status). Produces 200,000 realistic immigration applications across a 21-table relational data model with intentionally injected anomalies and fraud patterns for predictive analytics testing.

## Overview

The pipeline consists of three generators that run sequentially:

| Generator | Applications | IDs | Purpose |
|-----------|-------------|-----|---------|
| **1. Clean** | 140,000 | 1 – 140,000 | Valid, spec-compliant I-485 profiles |
| **2. Anomaly** | 60,000 | 140,001 – 200,000 | Broken/missing fields, data quality problems |
| **3. Fraud** | ~14,000 modified | Mixed | Conspiracy patterns overlaid on existing records |

**Total output: 200,000 applications, 4,020,348 rows across 20 tables + 1 reference table**

## Data Model

The generator produces a 21-table relational schema (`lho_ucm.i485_form`) that mirrors the actual I-485 form structure:

| Table | Rows | Relationship | Description |
|-------|------|-------------|-------------|
| `ref_filing_categories` | 51 | Reference | Filing category codes and descriptions |
| `application` | 200,000 | Root | Application metadata, status, attorney info |
| `applicant_info` | 200,000 | 1:1 | Identity, immigration details, SSN, passport |
| `filing_category` | 200,000 | 1:1 | Filing basis, applicant type, eligibility |
| `additional_info` | 200,000 | 1:1 | Prior applications, abroad processing |
| `biographic_info` | 200,000 | 1:1 | Height, weight, eye/hair color, demographics |
| `eligibility_responses` | 200,000 | 1:1 | 77 yes/no eligibility questions (q10–q86) |
| `public_charge` | 200,000 | 1:1 | Income, assets, liabilities, education |
| `employment_history` | 507,148 | 1:N | 5-year employment history (~2.5/app) |
| `addresses` | 444,566 | 1:N | Current, mailing, prior, foreign (~2.2/app) |
| `children` | 413,824 | 1:N | Applicant's children (~2.1/app) |
| `parents` | 390,546 | 1:N | Parent 1 and Parent 2 (~2/app) |
| `contacts_signatures` | 288,755 | 1:N | Applicant, preparer, interpreter (~1.4/app) |
| `marital_history` | 219,558 | 1:N | Current and prior marriages (~1.1/app) |
| `organizations` | 97,433 | 1:N | Organization memberships |
| `other_names` | 87,818 | 1:N | Maiden, alias, birth names |
| `additional_information` | 59,988 | 1:N | Free-text supplemental info |
| `affidavit_exemption` | 52,217 | 1:N | Public charge affidavit exemptions |
| `interview_signature` | 49,981 | 1:N | USCIS officer interview records |
| `benefits_received` | 7,035 | 1:N | Public benefits history |
| `institutionalization` | 1,479 | 1:N | Institutionalization records |

## File Structure

```
synthetic_PL/i485_generators/
├── config_i485.py            # Constants, distributions, category weights, I/O helpers
├── profiles.py               # Profile builder — coherent person records with country-specific names
├── 01_generate_clean.py      # Generator 1: 140K valid applications
├── 02_generate_anomalies.py  # Generator 2: 60K applications with data quality problems
├── 03_generate_fraud.py      # Generator 3: fraud conspiracy overlay on ~14K records
├── 04_run_pipeline.py        # Orchestrator: runs 1→2→3 in sequence
├── 05_load_to_databricks.py  # Uploads Parquet to Unity Catalog and registers Delta tables
└── README.md
```

**Output directory:** `synthetic_PL/synthetic_data/i485_form/`
```
synthetic_data/i485_form/
├── csv/                              # CSV files (one per table)
│   ├── application.csv
│   ├── applicant_info.csv
│   ├── ...
│   ├── _anomaly_manifest.csv         # Anomaly answer key
│   └── _fraud_manifest.csv           # Fraud answer key
├── parquet/                          # Parquet files (one per table)
│   ├── application.parquet
│   ├── applicant_info.parquet
│   └── ...
└── fraud_and_anomaly_records.xlsx    # Excel verification workbook
```

## Generators

### Generator 1: Clean Data (`01_generate_clean.py`)

Produces 140,000 valid, internally consistent I-485 applications. Each application starts as a **coherent person profile** (62 attributes) built by `profiles.py`, then all 20 tables are derived from that profile.

**Key features:**
- Country-specific romanized names for 73 countries (26 non-Latin-script countries use curated name pools)
- Realistic demographic distributions matching USCIS filing volumes
- 52 filing categories across 7 groups (Family 65%, Employment 20%, Asylee/Refugee 8%, etc.)
- Cross-table consistency rules (e.g., IR_SPOUSE filers are married, arrival before filing, parent DOB > 15yr before applicant)
- Seed-controlled reproducibility (`numpy.random.default_rng`)

### Generator 2: Anomaly Data (`02_generate_anomalies.py`)

Produces 60,000 applications with intentional data quality problems. Each record gets 1–5 randomly selected anomaly types from a catalog of 30 types across 8 categories:

| Category | Types | Examples |
|----------|-------|---------|
| **Missing Required Fields** | 6 | NULL name, DOB, country, address, category, signature |
| **Invalid Formats** | 5 | Bad SSN, A-Number, receipt number, date, zip code |
| **Date Logic Violations** | 4 | Future DOB, arrival after filing, expired passport |
| **Contradictory Data** | 5 | SSN contradiction, married without spouse, children mismatch |
| **Out-of-Range Values** | 4 | Extreme height/weight/age/children count |
| **Orphaned References** | 3 | Wrong category group, mismatched names, duplicate A-Number |
| **Eligibility Contradictions** | 2 | All security questions true, approved with disqualifying bars |
| **Completeness Issues** | 1 | Sparse record (most child tables empty) |

**Total: 140,844 anomaly entries across 60,000 applications (avg 2.3 per record)**

### Generator 3: Fraud Overlay (`03_generate_fraud.py`)

Modifies ~14,000 existing records in-place to create 10 detectable conspiracy patterns. Biased 70/30 toward the clean pool so fraud hides in "good" data.

| Pattern | Records | Description |
|---------|---------|-------------|
| **Address Fraud Ring** | 2,995 | 10–20 unrelated applicants at same address |
| **Attorney Mill** | 2,550 | 5 attorneys filing 500+ apps in bursts, cookie-cutter data |
| **SSN Sharing Ring** | 1,560 | 5 people sharing one SSN, spread across states |
| **Family Relationship Fraud** | 1,500 | Circular sponsorship, shared spouses, impossible parent ages |
| **Rapid Filing** | 1,050 | Same A-Number filing multiple I-485s within 30 days |
| **Financial Pattern Fraud** | 1,040 | Identical round-number financials across 20+ unrelated applicants |
| **Document Recycling** | 649 | Shared passport/I-94 numbers across different people |
| **Identity Theft** | 525 | Same SSN + DOB with slight name variations |
| **Country Mismatch Ring** | 450 | Incompatible birth/passport/citizenship country combinations |
| **Temporal Impossibility** | 450 | Arrival vs I-94 mismatch, filing before form edition, overlapping jobs |

**Total: 12,769 fraud manifest entries across 14,184 unique modified records**

## Usage

### Run the full pipeline locally

```bash
cd synthetic_PL/i485_generators

# Full 200K pipeline (clean → anomaly → fraud)
python3 04_run_pipeline.py \
  --clean-count 140000 \
  --anomaly-count 60000 \
  --fraud-count 15000 \
  --seed 42

# Or run generators individually
python3 01_generate_clean.py --count 140000 --start-id 1 --seed 42
python3 02_generate_anomalies.py --count 60000 --start-id 140001 --seed 84
python3 03_generate_fraud.py --fraud-count 15000 --seed 126
```

### Run as Databricks notebooks

Each generator is formatted as a Databricks notebook (`# COMMAND ----------` separators, `dbutils.widgets` for parameters). Import them into your Databricks workspace and run in sequence.

### Load to Databricks

```bash
# Uploads Parquet to Unity Catalog volume and registers as Delta tables
python3 05_load_to_databricks.py \
  --catalog lho_ucm \
  --schema i485_form
```

Requires:
- `databricks-sdk` and `databricks-sql-connector` Python packages
- Databricks CLI profile `planxs` configured
- Unity Catalog schema `lho_ucm.i485_form` and volume `staging_data` created

### Scale up or down

All generators accept count parameters. For a smaller test run:

```bash
python3 04_run_pipeline.py --clean-count 1000 --anomaly-count 500 --fraud-count 100 --seed 42
```

## Verification

### Answer keys

- **`_anomaly_manifest.csv`** — Every injected anomaly: `application_id, anomaly_type, affected_table, affected_column`
- **`_fraud_manifest.csv`** — Every fraud modification: `application_id, fraud_pattern, pattern_group_id, details`
- **`fraud_and_anomaly_records.xlsx`** — Excel workbook with summary, per-pattern sheets, and full manifests

### Sample detection queries

```sql
-- Detect SSN sharing rings
SELECT ssn, COUNT(DISTINCT application_id) AS cnt
FROM applicant_info WHERE ssn IS NOT NULL
GROUP BY ssn HAVING cnt > 1;

-- Detect address fraud rings
SELECT street, city, state, COUNT(*) AS cnt
FROM addresses WHERE address_type = 'CURRENT_PHYSICAL'
GROUP BY street, city, state HAVING cnt > 10;

-- Detect attorney mills
SELECT atty_state_bar_number, COUNT(*) AS cnt
FROM application WHERE atty_state_bar_number IS NOT NULL
GROUP BY atty_state_bar_number HAVING cnt > 200;

-- Detect missing required fields
SELECT COUNT(*) FROM applicant_info
WHERE family_name IS NULL OR given_name IS NULL OR date_of_birth IS NULL;

-- Detect impossible dates
SELECT COUNT(*) FROM applicant_info
WHERE date_of_birth > CURRENT_DATE();

-- Detect document recycling
SELECT passport_number, COUNT(DISTINCT application_id) AS cnt
FROM applicant_info WHERE passport_number IS NOT NULL
GROUP BY passport_number HAVING cnt > 1;

-- Detect financial pattern fraud
SELECT household_income, household_assets, household_liabilities,
       COUNT(*) AS cnt
FROM public_charge
GROUP BY household_income, household_assets, household_liabilities
HAVING cnt > 15;
```

## Configuration

Key distributions are defined in `config_i485.py`:

- **52 filing categories** with realistic USCIS volume weights
- **73 countries** of birth (top 20 weighted + 53 long-tail)
- **Demographics**: sex, ethnicity, race, age brackets, marital status, biometrics
- **US state distribution** by immigration volume (CA 22%, NY 12%, TX 10%, etc.)
- **Application status**: APPROVED 35%, PENDING 25%, RECEIVED 15%, DENIED 10%, etc.
- **Date ranges**: filing 2018–2026, priority 2005–2026, arrival 1990–2026

## Dependencies

```
pandas
numpy
pyarrow
faker
databricks-sdk          # for loading to Databricks
databricks-sql-connector # for loading to Databricks
certifi                  # for SSL cert verification
```

## Performance

| Stage | Time | Notes |
|-------|------|-------|
| Clean (140K) | ~3 min | Profile building with Faker is the bottleneck |
| Anomaly (60K) | ~18 min | Generates 60K base profiles + injects anomalies |
| Fraud (15K) | ~17 sec | Indexed lookups (O(1) per record) |
| Load to Databricks | ~5 min | Upload + Delta table registration |
| **Total** | **~27 min** | |
