# Databricks notebook source
# MAGIC %md
# MAGIC # I-485 Fraud Detection Engine
# MAGIC **Analyzes 200K+ I-485 applications across 8 fraud use cases**
# MAGIC
# MAGIC | Use Case | Rule Code | Description | Severity |
# MAGIC |----------|-----------|-------------|----------|
# MAGIC | 1 | DUP_SSN | Duplicate Social Security Numbers | CRITICAL |
# MAGIC | 2 | DUP_ANUM | Duplicate A-Numbers | CRITICAL |
# MAGIC | 3 | DUP_NAME_DOB | Same name + DOB across applications | HIGH |
# MAGIC | 4 | ELIG_CRITICAL | Critical inadmissibility flags (criminal, security, trafficking) | CRITICAL |
# MAGIC | 5 | ELIG_WARNING | Warning-level inadmissibility patterns | HIGH |
# MAGIC | 6 | WRONG_CATEGORY | Filing category marked WRONG_GROUP | MEDIUM |
# MAGIC | 7 | FINANCIAL_ANOMALY | Income/asset outliers, negative net worth | MEDIUM |
# MAGIC | 8 | FILING_BURST | Abnormal same-day filing volume | LOW |
# MAGIC
# MAGIC Outputs populate 7 tables in `lho_ucm.i485_form.fraud_*`

# COMMAND ----------

S = "lho_ucm.i485_form"
from datetime import datetime
TS = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"Schema: {S}")
print(f"Run timestamp: {TS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Clear previous run results

# COMMAND ----------

for t in ["fraud_alerts", "fraud_flags", "fraud_dup_identity", "fraud_eligibility_risk",
          "fraud_address_anomalies", "fraud_filing_patterns", "fraud_financial_anomalies"]:
    spark.sql(f"TRUNCATE TABLE {S}.{t}")
print("All fraud tables truncated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 1: Duplicate SSNs (CRITICAL)
# MAGIC Multiple applications sharing the same SSN — strong indicator of identity fraud or data entry errors.

# COMMAND ----------

dup_ssn_df = spark.sql(f"""
    SELECT ssn, COLLECT_LIST(application_id) AS app_ids, COUNT(*) AS cnt
    FROM {S}.applicant_info
    WHERE ssn IS NOT NULL AND TRIM(ssn) != ''
    GROUP BY ssn
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
""")
dup_ssn_count = dup_ssn_df.count()
print(f"Duplicate SSN clusters found: {dup_ssn_count}")
dup_ssn_df.show(10, truncate=False)

# COMMAND ----------

# Insert duplicate SSN identity matches
spark.sql(f"""
    INSERT INTO {S}.fraud_dup_identity (application_id_1, application_id_2, match_type, matched_value, confidence, analyzed_at)
    SELECT
        app_ids[0] AS application_id_1,
        app_ids[1] AS application_id_2,
        'SSN' AS match_type,
        CONCAT('***-**-', RIGHT(ssn, 4)) AS matched_value,
        'EXACT' AS confidence,
        '{TS}' AS analyzed_at
    FROM (
        SELECT ssn, COLLECT_LIST(application_id) AS app_ids
        FROM {S}.applicant_info
        WHERE ssn IS NOT NULL AND TRIM(ssn) != ''
        GROUP BY ssn
        HAVING COUNT(*) > 1
    )
""")

# Insert fraud flags for every application involved in SSN duplication
spark.sql(f"""
    INSERT INTO {S}.fraud_flags (application_id, rule_code, category, severity, description, evidence, analyzed_at)
    SELECT
        ai.application_id,
        'DUP_SSN',
        'IDENTITY',
        'CRITICAL',
        CONCAT('SSN ***-**-', RIGHT(ai.ssn, 4), ' shared by ', dup.cnt, ' applications'),
        CONCAT('{{"ssn_masked":"***-**-', RIGHT(ai.ssn, 4), '","total_matches":', dup.cnt, '}}'),
        '{TS}'
    FROM {S}.applicant_info ai
    JOIN (
        SELECT ssn, COUNT(*) AS cnt
        FROM {S}.applicant_info
        WHERE ssn IS NOT NULL AND TRIM(ssn) != ''
        GROUP BY ssn HAVING COUNT(*) > 1
    ) dup ON ai.ssn = dup.ssn
""")
print(f"DUP_SSN flags inserted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 2: Duplicate A-Numbers (CRITICAL)
# MAGIC Same Alien Registration Number on multiple applications.

# COMMAND ----------

dup_anum_df = spark.sql(f"""
    SELECT a_number, COLLECT_LIST(application_id) AS app_ids, COUNT(*) AS cnt
    FROM {S}.application
    WHERE a_number IS NOT NULL AND TRIM(a_number) != ''
    GROUP BY a_number
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
""")
dup_anum_count = dup_anum_df.count()
print(f"Duplicate A-Number clusters found: {dup_anum_count}")
dup_anum_df.show(10, truncate=False)

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {S}.fraud_dup_identity (application_id_1, application_id_2, match_type, matched_value, confidence, analyzed_at)
    SELECT
        app_ids[0], app_ids[1], 'A_NUMBER', a_number, 'EXACT', '{TS}'
    FROM (
        SELECT a_number, COLLECT_LIST(application_id) AS app_ids
        FROM {S}.application
        WHERE a_number IS NOT NULL AND TRIM(a_number) != ''
        GROUP BY a_number HAVING COUNT(*) > 1
    )
""")

spark.sql(f"""
    INSERT INTO {S}.fraud_flags (application_id, rule_code, category, severity, description, evidence, analyzed_at)
    SELECT
        a.application_id, 'DUP_ANUM', 'IDENTITY', 'CRITICAL',
        CONCAT('A-Number ', a.a_number, ' shared by ', dup.cnt, ' applications'),
        CONCAT('{{"a_number":"', a.a_number, '","total_matches":', dup.cnt, '}}'),
        '{TS}'
    FROM {S}.application a
    JOIN (
        SELECT a_number, COUNT(*) AS cnt
        FROM {S}.application WHERE a_number IS NOT NULL AND TRIM(a_number) != ''
        GROUP BY a_number HAVING COUNT(*) > 1
    ) dup ON a.a_number = dup.a_number
""")
print(f"DUP_ANUM flags inserted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 3: Duplicate Name + DOB (HIGH)
# MAGIC Same full name and date of birth on different applications — potential identity reuse.

# COMMAND ----------

dup_name_dob_df = spark.sql(f"""
    SELECT family_name, given_name, date_of_birth,
           COLLECT_LIST(application_id) AS app_ids, COUNT(*) AS cnt
    FROM {S}.applicant_info
    WHERE family_name IS NOT NULL AND given_name IS NOT NULL AND date_of_birth IS NOT NULL
    GROUP BY family_name, given_name, date_of_birth
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
""")
dup_name_dob_count = dup_name_dob_df.count()
print(f"Duplicate Name+DOB clusters found: {dup_name_dob_count}")
dup_name_dob_df.show(10, truncate=False)

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {S}.fraud_dup_identity (application_id_1, application_id_2, match_type, matched_value, confidence, analyzed_at)
    SELECT
        app_ids[0], app_ids[1], 'NAME_DOB',
        CONCAT(family_name, ', ', given_name, ' (', date_of_birth, ')'),
        'HIGH', '{TS}'
    FROM (
        SELECT family_name, given_name, date_of_birth, COLLECT_LIST(application_id) AS app_ids
        FROM {S}.applicant_info
        WHERE family_name IS NOT NULL AND given_name IS NOT NULL AND date_of_birth IS NOT NULL
        GROUP BY family_name, given_name, date_of_birth HAVING COUNT(*) > 1
    )
""")

spark.sql(f"""
    INSERT INTO {S}.fraud_flags (application_id, rule_code, category, severity, description, evidence, analyzed_at)
    SELECT
        ai.application_id, 'DUP_NAME_DOB', 'IDENTITY', 'HIGH',
        CONCAT(ai.family_name, ', ', ai.given_name, ' DOB ', ai.date_of_birth, ' — ', dup.cnt, ' matches'),
        CONCAT('{{"name":"', ai.family_name, ', ', ai.given_name, '","dob":"', ai.date_of_birth, '","matches":', dup.cnt, '}}'),
        '{TS}'
    FROM {S}.applicant_info ai
    JOIN (
        SELECT family_name, given_name, date_of_birth, COUNT(*) AS cnt
        FROM {S}.applicant_info
        WHERE family_name IS NOT NULL AND given_name IS NOT NULL AND date_of_birth IS NOT NULL
        GROUP BY family_name, given_name, date_of_birth HAVING COUNT(*) > 1
    ) dup ON ai.family_name = dup.family_name AND ai.given_name = dup.given_name AND ai.date_of_birth = dup.date_of_birth
""")
print("DUP_NAME_DOB flags inserted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 4: Critical Inadmissibility Flags (CRITICAL)
# MAGIC Applications where applicant answered YES to criminal, security, or trafficking questions.
# MAGIC
# MAGIC **Critical questions:** q22 (arrested), q24 (convicted), q26 (controlled substance), q27 (drug trafficking),
# MAGIC q30 (prostitution), q36 (sex trafficking), q37 (labor trafficking), q41 (money laundering),
# MAGIC q42 (espionage/overthrow), q53 (torture/genocide/killing), q70 (false citizenship claim), q74 (deported)

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {S}.fraud_eligibility_risk
    SELECT
        application_id,
        -- Criminal: q22-q41
        (CAST(COALESCE(q22,false) AS INT) + CAST(COALESCE(q23,false) AS INT) +
         CAST(COALESCE(q24,false) AS INT) + CAST(COALESCE(q25,false) AS INT) +
         CAST(COALESCE(q26,false) AS INT) + CAST(COALESCE(q27,false) AS INT) +
         CAST(COALESCE(q28,false) AS INT) + CAST(COALESCE(q29,false) AS INT) +
         CAST(COALESCE(q30,false) AS INT) + CAST(COALESCE(q31,false) AS INT) +
         CAST(COALESCE(q32,false) AS INT) + CAST(COALESCE(q33,false) AS INT) +
         CAST(COALESCE(q34,false) AS INT) + CAST(COALESCE(q35,false) AS INT) +
         CAST(COALESCE(q36,false) AS INT) + CAST(COALESCE(q37,false) AS INT) +
         CAST(COALESCE(q38,false) AS INT) + CAST(COALESCE(q39,false) AS INT) +
         CAST(COALESCE(q40,false) AS INT) + CAST(COALESCE(q41,false) AS INT)) AS criminal_flag_count,
        -- Security: q42-q55
        (CAST(COALESCE(q42,false) AS INT) + CAST(COALESCE(q43,false) AS INT) +
         CAST(COALESCE(q44,false) AS INT) + CAST(COALESCE(q45,false) AS INT) +
         CAST(COALESCE(q46,false) AS INT) + CAST(COALESCE(q47,false) AS INT) +
         CAST(COALESCE(q48,false) AS INT) + CAST(COALESCE(q49,false) AS INT) +
         CAST(COALESCE(q50,false) AS INT) + CAST(COALESCE(q51,false) AS INT) +
         CAST(COALESCE(q52,false) AS INT) + CAST(COALESCE(q53,false) AS INT) +
         CAST(COALESCE(q54,false) AS INT) + CAST(COALESCE(q55,false) AS INT)) AS security_flag_count,
        -- Immigration: q10-q21, q67-q78
        (CAST(COALESCE(q10,false) AS INT) + CAST(COALESCE(q11,false) AS INT) +
         CAST(COALESCE(q12,false) AS INT) + CAST(COALESCE(q13,false) AS INT) +
         CAST(COALESCE(q14,false) AS INT) + CAST(COALESCE(q15,false) AS INT) +
         CAST(COALESCE(q16,false) AS INT) + CAST(COALESCE(q17,false) AS INT) +
         CAST(COALESCE(q18,false) AS INT) + CAST(COALESCE(q19,false) AS INT) +
         CAST(COALESCE(q20,false) AS INT) + CAST(COALESCE(q21,false) AS INT) +
         CAST(COALESCE(q67,false) AS INT) + CAST(COALESCE(q68,false) AS INT) +
         CAST(COALESCE(q69,false) AS INT) + CAST(COALESCE(q70,false) AS INT) +
         CAST(COALESCE(q71,false) AS INT) + CAST(COALESCE(q72,false) AS INT) +
         CAST(COALESCE(q73,false) AS INT) + CAST(COALESCE(q74,false) AS INT) +
         CAST(COALESCE(q75,false) AS INT) + CAST(COALESCE(q76,false) AS INT) +
         CAST(COALESCE(q77,false) AS INT) + CAST(COALESCE(q78,false) AS INT)) AS immigration_flag_count,
        -- Fraud-specific: q68 (fraudulent docs), q69 (lied), q70 (false citizen), q71 (stowaway)
        (CAST(COALESCE(q68,false) AS INT) + CAST(COALESCE(q69,false) AS INT) +
         CAST(COALESCE(q70,false) AS INT) + CAST(COALESCE(q71,false) AS INT)) AS fraud_flag_count,
        -- Total yes
        (CAST(COALESCE(q10,false) AS INT) + CAST(COALESCE(q11,false) AS INT) +
         CAST(COALESCE(q12,false) AS INT) + CAST(COALESCE(q13,false) AS INT) +
         CAST(COALESCE(q14,false) AS INT) + CAST(COALESCE(q15,false) AS INT) +
         CAST(COALESCE(q16,false) AS INT) + CAST(COALESCE(q17,false) AS INT) +
         CAST(COALESCE(q18,false) AS INT) + CAST(COALESCE(q19,false) AS INT) +
         CAST(COALESCE(q20,false) AS INT) + CAST(COALESCE(q21,false) AS INT) +
         CAST(COALESCE(q22,false) AS INT) + CAST(COALESCE(q23,false) AS INT) +
         CAST(COALESCE(q24,false) AS INT) + CAST(COALESCE(q25,false) AS INT) +
         CAST(COALESCE(q26,false) AS INT) + CAST(COALESCE(q27,false) AS INT) +
         CAST(COALESCE(q28,false) AS INT) + CAST(COALESCE(q29,false) AS INT) +
         CAST(COALESCE(q30,false) AS INT) + CAST(COALESCE(q31,false) AS INT) +
         CAST(COALESCE(q32,false) AS INT) + CAST(COALESCE(q33,false) AS INT) +
         CAST(COALESCE(q34,false) AS INT) + CAST(COALESCE(q35,false) AS INT) +
         CAST(COALESCE(q36,false) AS INT) + CAST(COALESCE(q37,false) AS INT) +
         CAST(COALESCE(q38,false) AS INT) + CAST(COALESCE(q39,false) AS INT) +
         CAST(COALESCE(q40,false) AS INT) + CAST(COALESCE(q41,false) AS INT) +
         CAST(COALESCE(q42,false) AS INT) + CAST(COALESCE(q43,false) AS INT) +
         CAST(COALESCE(q44,false) AS INT) + CAST(COALESCE(q45,false) AS INT) +
         CAST(COALESCE(q46,false) AS INT) + CAST(COALESCE(q47,false) AS INT) +
         CAST(COALESCE(q48,false) AS INT) + CAST(COALESCE(q49,false) AS INT) +
         CAST(COALESCE(q50,false) AS INT) + CAST(COALESCE(q51,false) AS INT) +
         CAST(COALESCE(q52,false) AS INT) + CAST(COALESCE(q53,false) AS INT) +
         CAST(COALESCE(q54,false) AS INT) + CAST(COALESCE(q55,false) AS INT) +
         CAST(COALESCE(q67,false) AS INT) + CAST(COALESCE(q68,false) AS INT) +
         CAST(COALESCE(q69,false) AS INT) + CAST(COALESCE(q70,false) AS INT) +
         CAST(COALESCE(q71,false) AS INT) + CAST(COALESCE(q72,false) AS INT) +
         CAST(COALESCE(q73,false) AS INT) + CAST(COALESCE(q74,false) AS INT) +
         CAST(COALESCE(q75,false) AS INT) + CAST(COALESCE(q76,false) AS INT) +
         CAST(COALESCE(q77,false) AS INT) + CAST(COALESCE(q78,false) AS INT) +
         CAST(COALESCE(q79,false) AS INT) + CAST(COALESCE(q80,false) AS INT) +
         CAST(COALESCE(q81,false) AS INT) + CAST(COALESCE(q82,false) AS INT) +
         CAST(COALESCE(q83,false) AS INT) + CAST(COALESCE(q84,false) AS INT) +
         CAST(COALESCE(q85,false) AS INT) + CAST(COALESCE(q86,false) AS INT)) AS total_yes_count,
        -- Risk tier
        CASE
            WHEN (CAST(COALESCE(q26,false) AS INT) + CAST(COALESCE(q27,false) AS INT) +
                  CAST(COALESCE(q36,false) AS INT) + CAST(COALESCE(q37,false) AS INT) +
                  CAST(COALESCE(q41,false) AS INT) + CAST(COALESCE(q42,false) AS INT) +
                  CAST(COALESCE(q53,false) AS INT)) > 0 THEN 'CRITICAL'
            WHEN (CAST(COALESCE(q22,false) AS INT) + CAST(COALESCE(q24,false) AS INT) +
                  CAST(COALESCE(q30,false) AS INT) + CAST(COALESCE(q70,false) AS INT) +
                  CAST(COALESCE(q74,false) AS INT)) > 0 THEN 'HIGH'
            WHEN (CAST(COALESCE(q10,false) AS INT) + CAST(COALESCE(q11,false) AS INT) +
                  CAST(COALESCE(q12,false) AS INT) + CAST(COALESCE(q13,false) AS INT) +
                  CAST(COALESCE(q14,false) AS INT) + CAST(COALESCE(q15,false) AS INT)) > 2 THEN 'MEDIUM'
            WHEN (CAST(COALESCE(q10,false) AS INT) + CAST(COALESCE(q11,false) AS INT) +
                  CAST(COALESCE(q12,false) AS INT) + CAST(COALESCE(q13,false) AS INT) +
                  CAST(COALESCE(q14,false) AS INT) + CAST(COALESCE(q15,false) AS INT)) > 0 THEN 'LOW'
            ELSE 'CLEAN'
        END AS risk_tier,
        -- Flagged questions (only those answered YES)
        CONCAT_WS(',',
            IF(q10, 'q10', NULL), IF(q11, 'q11', NULL), IF(q12, 'q12', NULL),
            IF(q13, 'q13', NULL), IF(q14, 'q14', NULL), IF(q15, 'q15', NULL),
            IF(q22, 'q22', NULL), IF(q24, 'q24', NULL), IF(q26, 'q26', NULL),
            IF(q27, 'q27', NULL), IF(q30, 'q30', NULL), IF(q36, 'q36', NULL),
            IF(q37, 'q37', NULL), IF(q41, 'q41', NULL), IF(q42, 'q42', NULL),
            IF(q53, 'q53', NULL), IF(q68, 'q68', NULL), IF(q69, 'q69', NULL),
            IF(q70, 'q70', NULL), IF(q74, 'q74', NULL), IF(q75, 'q75', NULL),
            IF(q76, 'q76', NULL)
        ) AS flagged_questions,
        '{TS}' AS analyzed_at
    FROM {S}.eligibility_responses
""")

elig_stats = spark.sql(f"""
    SELECT risk_tier, COUNT(*) AS cnt
    FROM {S}.fraud_eligibility_risk
    GROUP BY risk_tier ORDER BY
    CASE risk_tier WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 WHEN 'LOW' THEN 4 ELSE 5 END
""")
print("Eligibility risk distribution:")
elig_stats.show()

# COMMAND ----------

# Insert flags for CRITICAL and HIGH eligibility risks
spark.sql(f"""
    INSERT INTO {S}.fraud_flags (application_id, rule_code, category, severity, description, evidence, analyzed_at)
    SELECT
        application_id,
        CASE WHEN risk_tier = 'CRITICAL' THEN 'ELIG_CRITICAL' ELSE 'ELIG_WARNING' END,
        'ELIGIBILITY',
        risk_tier,
        CONCAT(risk_tier, ' risk: ', total_yes_count, ' inadmissibility flags (',
               criminal_flag_count, ' criminal, ', security_flag_count, ' security, ',
               immigration_flag_count, ' immigration)'),
        CONCAT('{{"criminal":', criminal_flag_count, ',"security":', security_flag_count,
               ',"immigration":', immigration_flag_count, ',"total_yes":', total_yes_count,
               ',"questions":"', COALESCE(flagged_questions,''), '"}}'),
        '{TS}'
    FROM {S}.fraud_eligibility_risk
    WHERE risk_tier IN ('CRITICAL', 'HIGH')
""")
print("ELIG_CRITICAL and ELIG_WARNING flags inserted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 5: Wrong Filing Category (MEDIUM)
# MAGIC Applications with category_group = 'WRONG_GROUP' — data integrity issue or intentional misclassification.

# COMMAND ----------

wrong_group_count = spark.sql(f"""
    SELECT COUNT(*) FROM {S}.filing_category WHERE category_group = 'WRONG_GROUP'
""").collect()[0][0]
print(f"Applications with WRONG_GROUP category: {wrong_group_count}")

spark.sql(f"""
    INSERT INTO {S}.fraud_filing_patterns (pattern_type, application_ids, app_count, detail, analyzed_at)
    SELECT
        'WRONG_GROUP',
        TO_JSON(COLLECT_LIST(application_id)),
        COUNT(*),
        CONCAT(COUNT(*), ' applications filed under WRONG_GROUP category'),
        '{TS}'
    FROM {S}.filing_category
    WHERE category_group = 'WRONG_GROUP'
""")

spark.sql(f"""
    INSERT INTO {S}.fraud_flags (application_id, rule_code, category, severity, description, evidence, analyzed_at)
    SELECT
        fc.application_id,
        'WRONG_CATEGORY',
        'FILING',
        'MEDIUM',
        CONCAT('Category code "', fc.category_code, '" mapped to WRONG_GROUP'),
        CONCAT('{{"category_code":"', COALESCE(fc.category_code,''), '","category_desc":"', COALESCE(fc.category_description,''), '"}}'),
        '{TS}'
    FROM {S}.filing_category fc
    WHERE fc.category_group = 'WRONG_GROUP'
""")
print("WRONG_CATEGORY flags inserted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 6: Financial Anomalies (MEDIUM)
# MAGIC Negative net worth, zero-income households, or extreme outliers in public charge data.

# COMMAND ----------

# Compute stats for thresholds
stats = spark.sql(f"""
    SELECT
        PERCENTILE(household_income, 0.99) AS income_p99,
        PERCENTILE(household_assets, 0.99) AS assets_p99,
        PERCENTILE(household_liabilities, 0.99) AS liab_p99
    FROM {S}.public_charge
    WHERE household_income IS NOT NULL
""").collect()[0]
income_p99 = stats[0] or 999999999
assets_p99 = stats[1] or 999999999
print(f"Income P99: {income_p99}, Assets P99: {assets_p99}")

# Negative net worth
spark.sql(f"""
    INSERT INTO {S}.fraud_financial_anomalies (application_id, anomaly_type, detail, household_income, household_assets, household_liabilities, analyzed_at)
    SELECT application_id, 'NEGATIVE_NETWORTH',
        CONCAT('Assets ', household_assets, ' minus liabilities ', household_liabilities, ' = ', (household_assets - household_liabilities)),
        household_income, household_assets, household_liabilities, '{TS}'
    FROM {S}.public_charge
    WHERE household_assets IS NOT NULL AND household_liabilities IS NOT NULL
      AND (household_assets - household_liabilities) < -50000
""")

# Zero income with high assets
spark.sql(f"""
    INSERT INTO {S}.fraud_financial_anomalies (application_id, anomaly_type, detail, household_income, household_assets, household_liabilities, analyzed_at)
    SELECT application_id, 'ZERO_INCOME_HIGH_ASSETS',
        CONCAT('Zero income but assets of ', household_assets),
        household_income, household_assets, household_liabilities, '{TS}'
    FROM {S}.public_charge
    WHERE (household_income IS NULL OR household_income = 0)
      AND household_assets IS NOT NULL AND household_assets > 500000
""")

# Income outliers (above 99th percentile)
spark.sql(f"""
    INSERT INTO {S}.fraud_financial_anomalies (application_id, anomaly_type, detail, household_income, household_assets, household_liabilities, analyzed_at)
    SELECT application_id, 'INCOME_OUTLIER',
        CONCAT('Income of ', household_income, ' exceeds P99 threshold of {int(income_p99)}'),
        household_income, household_assets, household_liabilities, '{TS}'
    FROM {S}.public_charge
    WHERE household_income > {int(income_p99)}
""")

spark.sql(f"""
    INSERT INTO {S}.fraud_flags (application_id, rule_code, category, severity, description, evidence, analyzed_at)
    SELECT application_id, 'FINANCIAL_ANOMALY', 'FINANCIAL', 'MEDIUM',
        CONCAT(anomaly_type, ': ', detail),
        CONCAT('{{"type":"', anomaly_type, '","income":', COALESCE(household_income,0),
               ',"assets":', COALESCE(household_assets,0), ',"liabilities":', COALESCE(household_liabilities,0), '}}'),
        '{TS}'
    FROM {S}.fraud_financial_anomalies
    WHERE analyzed_at = '{TS}'
""")

fin_count = spark.sql(f"SELECT COUNT(*) FROM {S}.fraud_financial_anomalies WHERE analyzed_at = '{TS}'").collect()[0][0]
print(f"Financial anomalies found: {fin_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 7: Filing Date Bursts (LOW)
# MAGIC Abnormally high number of applications filed on the same date — potential batch fraud.

# COMMAND ----------

# Find dates with filing volume > 3 standard deviations above mean
burst_df = spark.sql(f"""
    WITH daily AS (
        SELECT filing_date, COUNT(*) AS cnt
        FROM {S}.application
        WHERE filing_date IS NOT NULL
        GROUP BY filing_date
    ),
    stats AS (
        SELECT AVG(cnt) AS avg_cnt, STDDEV(cnt) AS std_cnt FROM daily
    )
    SELECT d.filing_date, d.cnt,
           ROUND((d.cnt - s.avg_cnt) / s.std_cnt, 2) AS z_score
    FROM daily d CROSS JOIN stats s
    WHERE d.cnt > (s.avg_cnt + 3 * s.std_cnt)
    ORDER BY d.cnt DESC
""")
burst_count = burst_df.count()
print(f"Filing date bursts (>3σ): {burst_count}")
burst_df.show(20, truncate=False)

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {S}.fraud_filing_patterns (pattern_type, app_count, detail, filing_date, analyzed_at)
    WITH daily AS (
        SELECT filing_date, COUNT(*) AS cnt FROM {S}.application WHERE filing_date IS NOT NULL GROUP BY filing_date
    ),
    stats AS (
        SELECT AVG(cnt) AS avg_cnt, STDDEV(cnt) AS std_cnt FROM daily
    )
    SELECT 'SAME_DAY_BURST', d.cnt,
        CONCAT(d.cnt, ' applications on ', d.filing_date, ' (z-score: ', ROUND((d.cnt - s.avg_cnt)/s.std_cnt, 2), ')'),
        d.filing_date, '{TS}'
    FROM daily d CROSS JOIN stats s
    WHERE d.cnt > (s.avg_cnt + 3 * s.std_cnt)
""")
print("SAME_DAY_BURST patterns inserted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 8: Composite Risk Scoring & Master Alerts
# MAGIC Aggregate all flags into a composite risk score per application and populate `fraud_alerts`.

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {S}.fraud_alerts (application_id, risk_score, risk_level, total_flags,
                                   critical_flags, warning_flags, info_flags, flag_categories, analyzed_at)
    WITH flag_agg AS (
        SELECT
            application_id,
            COUNT(*) AS total_flags,
            SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_flags,
            SUM(CASE WHEN severity IN ('HIGH', 'MEDIUM') THEN 1 ELSE 0 END) AS warning_flags,
            SUM(CASE WHEN severity = 'LOW' THEN 1 ELSE 0 END) AS info_flags,
            CONCAT_WS(',', COLLECT_SET(category)) AS flag_categories,
            -- Score: CRITICAL=30, HIGH=15, MEDIUM=5, LOW=1
            SUM(CASE severity
                WHEN 'CRITICAL' THEN 30
                WHEN 'HIGH' THEN 15
                WHEN 'MEDIUM' THEN 5
                WHEN 'LOW' THEN 1
                ELSE 0
            END) AS raw_score
        FROM {S}.fraud_flags
        WHERE analyzed_at = '{TS}'
        GROUP BY application_id
    )
    SELECT
        application_id,
        LEAST(raw_score, 100) AS risk_score,
        CASE
            WHEN raw_score >= 60 THEN 'CRITICAL'
            WHEN raw_score >= 30 THEN 'HIGH'
            WHEN raw_score >= 10 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,
        total_flags, critical_flags, warning_flags, info_flags,
        flag_categories, '{TS}'
    FROM flag_agg
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

print("=" * 70)
print("FRAUD DETECTION RUN COMPLETE")
print("=" * 70)

for t in ["fraud_alerts", "fraud_flags", "fraud_dup_identity", "fraud_eligibility_risk",
          "fraud_address_anomalies", "fraud_filing_patterns", "fraud_financial_anomalies"]:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {S}.{t}").collect()[0][0]
    print(f"  {t:<35} {cnt:>8} rows")

print()
alert_summary = spark.sql(f"""
    SELECT risk_level, COUNT(*) AS apps, SUM(total_flags) AS total_flags
    FROM {S}.fraud_alerts
    GROUP BY risk_level
    ORDER BY CASE risk_level WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END
""")
print("Risk Level Distribution:")
alert_summary.show()

flag_summary = spark.sql(f"""
    SELECT rule_code, severity, COUNT(*) AS cnt
    FROM {S}.fraud_flags
    GROUP BY rule_code, severity
    ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END, cnt DESC
""")
print("Flags by Rule:")
flag_summary.show(20)
print("=" * 70)
