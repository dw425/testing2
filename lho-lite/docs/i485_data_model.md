# USCIS Form I-485 — Complete Data Model

**Application to Register Permanent Residence or Adjust Status**
OMB No. 1615-0023 | Edition 01/20/25 | 24 pages | 14 Parts

---

## USCIS Internal Use (Header Block)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `uscis_preference_category` | Preference Category | VARCHAR(50) | Free text | Y |
| `uscis_receipt` | Receipt | VARCHAR(30) | Receipt number | Y |
| `uscis_action_block` | Action Block | VARCHAR(100) | Free text | Y |
| `uscis_country_chargeable` | Country Chargeable | VARCHAR(60) | Country name | Y |
| `uscis_priority_date` | Priority Date | DATE | mm/dd/yyyy | Y |
| `uscis_i693_signed_date` | Date Form I-693 Signed By Civil Surgeon | DATE | mm/dd/yyyy | Y |
| `uscis_applicant_interviewed` | Applicant Interviewed | BOOLEAN | Checkbox | Y |
| `uscis_interview_waived` | Interview Waived | BOOLEAN | Checkbox | Y |
| `uscis_date_initial_interview` | Date of Initial Interview | DATE | mm/dd/yyyy | Y |
| `uscis_lawful_permanent_resident_as_of` | Lawful Permanent Resident as of | DATE | mm/dd/yyyy | Y |
| `uscis_section_of_law` | Section of Law | VARCHAR(30) | ENUM: INA 209(a), INA 209(b), INA 245(a), INA 245(i), INA 245(j), INA 245(m), INA 249, Sec. 13 Act of 9/11/57, Cuban Adjustment Act, Other | Y |

## Attorney / Representative Block

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `atty_g28_attached` | Select this box if Form G-28 is attached | BOOLEAN | Checkbox | Y |
| `atty_volag_number` | Volag Number | VARCHAR(20) | Alphanumeric | Y |
| `atty_state_bar_number` | Attorney State Bar Number | VARCHAR(30) | Alphanumeric | Y |
| `atty_uscis_oan` | Attorney or Accredited Representative USCIS Online Account Number | VARCHAR(12) | Alphanumeric | Y |

## Global Identifier

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `a_number` | Alien Registration Number (A-Number) | VARCHAR(9) | 9-digit numeric (A-XXXXXXXXX) | Y |

---

## Part 1. Information About You (Items 1-19)

### 1.1 Identity

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_1_family_name` | Current Legal Family Name (Last Name) | VARCHAR(60) | Text | N |
| `p1_1_given_name` | Current Legal Given Name (First Name) | VARCHAR(60) | Text | N |
| `p1_1_middle_name` | Current Legal Middle Name | VARCHAR(60) | Text | Y |
| `p1_2_other_family_name` | Other Names - Family Name (Last Name) | VARCHAR(60) | Text (repeatable) | Y |
| `p1_2_other_given_name` | Other Names - Given Name (First Name) | VARCHAR(60) | Text (repeatable) | Y |
| `p1_2_other_middle_name` | Other Names - Middle Name | VARCHAR(60) | Text (repeatable) | Y |
| `p1_3_date_of_birth` | Date of Birth | DATE | mm/dd/yyyy | N |
| `p1_3_used_other_dob` | Have you ever used any other date of birth? | BOOLEAN | Yes / No | N |
| `p1_3_other_dob` | Other dates of birth | VARCHAR(100) | mm/dd/yyyy (repeatable) | Y |
| `p1_4_has_a_number` | Do you have an A-Number? | BOOLEAN | Yes / No | N |
| `p1_4_a_number` | A-Number (if any) | VARCHAR(9) | 9-digit numeric | Y |
| `p1_5_other_a_number` | Have you ever used or been assigned any other A-Number? | BOOLEAN | Yes / No | N |
| `p1_5_other_a_numbers` | Other A-Numbers | TEXT | Free text (repeatable) | Y |
| `p1_6_sex` | Sex | VARCHAR(6) | ENUM: Male, Female | N |

### 1.2 Birth & Nationality

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_7_city_of_birth` | City or Town of Birth | VARCHAR(60) | Text | N |
| `p1_7_country_of_birth` | Country of Birth | VARCHAR(60) | Country name | N |
| `p1_8_country_of_citizenship` | Country of Citizenship or Nationality | VARCHAR(60) | Country name | N |
| `p1_9_uscis_oan` | USCIS Online Account Number (if any) | VARCHAR(12) | Alphanumeric | Y |

### 1.3 Recent Immigration History (Item 10)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_10_passport_number` | Passport or Travel Document Number Used at Last Arrival | VARCHAR(30) | Alphanumeric | Y |
| `p1_10_passport_expiration` | Expiration Date of Passport or Travel Document | DATE | mm/dd/yyyy | Y |
| `p1_10_passport_country` | Country that Issued this Passport or Travel Document | VARCHAR(60) | Country name | Y |
| `p1_10_visa_number` | Nonimmigrant Visa Number Used During Most Recent Arrival | VARCHAR(30) | Alphanumeric | Y |
| `p1_10_visa_issued_date` | Date Nonimmigrant Visa Was Issued | DATE | mm/dd/yyyy | Y |
| `p1_10_arrival_city` | Place of Last Arrival - City or Town | VARCHAR(60) | Text | Y |
| `p1_10_arrival_state` | Place of Last Arrival - State | VARCHAR(2) | US state code | Y |
| `p1_10_arrival_date` | Date of Last Arrival | DATE | mm/dd/yyyy | Y |

### 1.4 Arrival Status (Item 11)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_11_arrival_type` | How you last arrived | VARCHAR(20) | ENUM: inspected_admitted, inspected_paroled, without_admission, other | N |
| `p1_11_admitted_as` | Admitted as (class of admission) | VARCHAR(100) | Free text (e.g., exchange visitor, student) | Y |
| `p1_11_paroled_as` | Paroled as | VARCHAR(100) | Free text (e.g., humanitarian parole) | Y |
| `p1_11_other_detail` | Other (explain) | VARCHAR(200) | Free text | Y |

### 1.5 I-94 Record (Item 12)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_12_i94_family_name` | I-94 Family Name (Last Name) | VARCHAR(60) | Text | Y |
| `p1_12_i94_given_name` | I-94 Given Name (First Name) | VARCHAR(60) | Text | Y |
| `p1_12_i94_number` | Form I-94 Arrival/Departure Record Number | VARCHAR(11) | 11-digit numeric | Y |
| `p1_12_i94_expiration` | Expiration Date of Authorized Stay on I-94 | VARCHAR(20) | mm/dd/yyyy or "D/S" | Y |
| `p1_12_i94_status` | Immigration Status on Form I-94 | VARCHAR(60) | Free text (class of admission) | Y |

### 1.6 Current Immigration Status (Items 13-17)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_13_first_time_physical_presence` | Was your last arrival the first time you were physically present in the US? | BOOLEAN | Yes / No | N |
| `p1_14_current_immigration_status` | Current immigration status (if changed since last arrival) | VARCHAR(60) | Free text | Y |
| `p1_15_status_expiration` | Expiration Date of Current Immigration Status | VARCHAR(20) | mm/dd/yyyy or "D/S" | Y |
| `p1_16_alien_crewman_visa` | Have you ever been issued an "alien crewman" visa? | BOOLEAN | Yes / No | N |
| `p1_17_arrived_as_crewman` | Did you last arrive to join a vessel as a seaman or crewman? | BOOLEAN | Yes / No | N |

### 1.7 Current U.S. Physical Address (Item 18)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_18_phys_care_of` | In Care Of Name | VARCHAR(80) | Text | Y |
| `p1_18_phys_street` | Street Number and Name | VARCHAR(100) | Text | N |
| `p1_18_phys_apt_type` | Apt. / Ste. / Flr. | VARCHAR(4) | ENUM: Apt, Ste, Flr | Y |
| `p1_18_phys_apt_number` | Unit Number | VARCHAR(10) | Alphanumeric | Y |
| `p1_18_phys_city` | City or Town | VARCHAR(60) | Text | N |
| `p1_18_phys_state` | State | VARCHAR(2) | US state code | N |
| `p1_18_phys_zip` | ZIP Code | VARCHAR(10) | 5 or 9 digit | N |
| `p1_18_phys_date_from` | Date You First Resided at This Address | DATE | mm/dd/yyyy | N |
| `p1_18_is_mailing_address` | Is this your current mailing address? | BOOLEAN | Yes / No | N |

### 1.8 Current Mailing Address (Item 18 continued)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_18_mail_care_of` | In Care Of Name | VARCHAR(80) | Text | Y |
| `p1_18_mail_street` | Street Number and Name | VARCHAR(100) | Text | Y |
| `p1_18_mail_apt_type` | Apt. / Ste. / Flr. | VARCHAR(4) | ENUM: Apt, Ste, Flr | Y |
| `p1_18_mail_apt_number` | Unit Number | VARCHAR(10) | Alphanumeric | Y |
| `p1_18_mail_city` | City or Town | VARCHAR(60) | Text | Y |
| `p1_18_mail_state` | State | VARCHAR(2) | US state code | Y |
| `p1_18_mail_zip` | ZIP Code | VARCHAR(10) | 5 or 9 digit | Y |

### 1.9 Address History (Item 18 continued)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_18_resided_5_years` | Have you resided at your current address for at least 5 years? | BOOLEAN | Yes / No | N |

**Prior Address (repeatable if < 5 years at current)**

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_18_prior_care_of` | In Care Of Name | VARCHAR(80) | Text | Y |
| `p1_18_prior_street` | Street Number and Name | VARCHAR(100) | Text | Y |
| `p1_18_prior_apt_type` | Apt. / Ste. / Flr. | VARCHAR(4) | ENUM: Apt, Ste, Flr | Y |
| `p1_18_prior_apt_number` | Unit Number | VARCHAR(10) | Alphanumeric | Y |
| `p1_18_prior_city` | City or Town | VARCHAR(60) | Text | Y |
| `p1_18_prior_state` | State | VARCHAR(2) | US state code | Y |
| `p1_18_prior_zip` | ZIP Code | VARCHAR(10) | 5 or 9 digit | Y |
| `p1_18_prior_province` | Province | VARCHAR(60) | Text | Y |
| `p1_18_prior_postal_code` | Postal Code | VARCHAR(20) | Alphanumeric | Y |
| `p1_18_prior_country` | Country | VARCHAR(60) | Country name | Y |
| `p1_18_prior_date_from` | Dates of Residence - From | DATE | mm/dd/yyyy | Y |
| `p1_18_prior_date_to` | Dates of Residence - To | DATE | mm/dd/yyyy | Y |

**Most Recent Address Outside the United States (>1 year)**

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_18_foreign_street` | Street Number and Name | VARCHAR(100) | Text | Y |
| `p1_18_foreign_apt_type` | Apt. / Ste. / Flr. | VARCHAR(4) | ENUM: Apt, Ste, Flr | Y |
| `p1_18_foreign_apt_number` | Unit Number | VARCHAR(10) | Alphanumeric | Y |
| `p1_18_foreign_city` | City or Town | VARCHAR(60) | Text | Y |
| `p1_18_foreign_state` | State | VARCHAR(60) | Text | Y |
| `p1_18_foreign_zip` | ZIP Code | VARCHAR(20) | Alphanumeric | Y |
| `p1_18_foreign_province` | Province | VARCHAR(60) | Text | Y |
| `p1_18_foreign_postal_code` | Postal Code | VARCHAR(20) | Alphanumeric | Y |
| `p1_18_foreign_country` | Country | VARCHAR(60) | Country name | Y |
| `p1_18_foreign_date_from` | Dates of Residence - From | DATE | mm/dd/yyyy | Y |
| `p1_18_foreign_date_to` | Dates of Residence - To | DATE | mm/dd/yyyy | Y |

### 1.10 Social Security (Item 19)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p1_19_has_ssn` | Has SSA ever officially issued a Social Security card to you? | BOOLEAN | Yes / No | N |
| `p1_19_ssn` | Social Security Number (SSN) | VARCHAR(9) | 9-digit numeric (XXX-XX-XXXX) | Y |
| `p1_19_want_ssn_card` | Do you want the SSA to issue you a Social Security card? | BOOLEAN | Yes / No | Y |
| `p1_19_consent_disclosure` | Consent for Disclosure to SSA | BOOLEAN | Yes / No | Y |

---

## Part 2. Application Type or Filing Category (Items 1-5)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p2_1_filing_with_eoir` | Filing for adjustment while in removal/deportation proceedings with EOIR? | BOOLEAN | Yes / No | N |
| `p2_2_petition_receipt_number` | Receipt Number of Underlying Petition | VARCHAR(30) | Alphanumeric | Y |
| `p2_2_priority_date` | Priority Date from Underlying Petition | DATE | mm/dd/yyyy | Y |
| `p2_applicant_type` | Filing as a: | VARCHAR(25) | ENUM: principal_applicant, derivative_applicant | N |
| `p2_principal_family_name` | Principal Applicant's Family Name | VARCHAR(60) | Text | Y |
| `p2_principal_given_name` | Principal Applicant's Given Name | VARCHAR(60) | Text | Y |
| `p2_principal_middle_name` | Principal Applicant's Middle Name | VARCHAR(60) | Text | Y |
| `p2_principal_a_number` | Principal Applicant's A-Number | VARCHAR(9) | 9-digit numeric | Y |
| `p2_principal_dob` | Principal Applicant's Date of Birth | DATE | mm/dd/yyyy | Y |

### 2.1 Filing Category (Item 3) — Select ONE

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p2_3_category` | Immigration category code | VARCHAR(10) | See enum below | N |

**Category Enum Values:**

**3.a. Family-based**
| Code | Description |
|------|------------|
| `FAM_IR_SPOUSE` | Spouse of a U.S. Citizen |
| `FAM_IR_CHILD` | Unmarried child under 21 of a U.S. citizen |
| `FAM_IR_PARENT` | Parent of a U.S. citizen (citizen is at least 21) |
| `FAM_IR_K1K2` | Fiancé(e) or child of fiancé(e) of a U.S. citizen (K-1/K-2) |
| `FAM_IR_WIDOW` | Widow or widower of a U.S. citizen |
| `FAM_IR_NDAA` | Spouse/child/parent of deceased U.S. active-duty service member (NDAA) |
| `FAM_F1` | Unmarried son or daughter of a U.S. citizen, age 21 or older |
| `FAM_F2A` | Spouse of a lawful permanent resident |
| `FAM_F2B_CHILD` | Unmarried child under 21 of a lawful permanent resident |
| `FAM_F2B_ADULT` | Unmarried son or daughter of a LPR, age 21 or older |
| `FAM_F3` | Married son or daughter of a U.S. citizen |
| `FAM_F4` | Brother or sister of a U.S. citizen (citizen is at least 21) |
| `FAM_VAWA_SPOUSE` | VAWA self-petitioning spouse of a U.S. citizen or LPR |
| `FAM_VAWA_CHILD` | VAWA self-petitioning child of a U.S. citizen or LPR |
| `FAM_VAWA_PARENT` | VAWA self-petitioning parent of a U.S. citizen (citizen at least 21) |

**3.b. Employment-based**
| Code | Description |
|------|------------|
| `EMP_INVESTOR` | Alien Investor (Form I-526 or I-526E) |
| `EMP_EB1_EXTRAORDINARY` | Alien of Extraordinary Ability |
| `EMP_EB1_PROFESSOR` | Outstanding Professor or Researcher |
| `EMP_EB1_MANAGER` | Multinational Executive or Manager |
| `EMP_EB2_ADV_DEGREE` | Member of Professions with Advanced Degree or Exceptional Ability (not NIW) |
| `EMP_EB2_NIW` | National Interest Waiver (Advanced Degree / Exceptional Ability) |
| `EMP_EB3_PROFESSIONAL` | Professional (bachelor's degree minimum) |
| `EMP_EB3_SKILLED` | Skilled Worker (2+ years specialized training) |
| `EMP_EB3_OTHER` | Any Other Worker (< 2 years training) |

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p2_3b_relative_filed_i140` | Did a relative file the associated I-140 or own 5%+ of the business? | VARCHAR(10) | ENUM: na_self_petition, no, yes | Y |
| `p2_3b_relative_relationship` | If yes, relationship | VARCHAR(20) | ENUM: Father, Mother, Child, Adult Son, Adult Daughter, Brother, Sister, None of These | Y |
| `p2_3b_relative_status` | Is the relative a: | VARCHAR(30) | ENUM: US Citizen, US National, Lawful Permanent Resident, None of These | Y |

**3.c. Special Immigrant**
| Code | Description |
|------|------------|
| `SI_JUVENILE` | Special Immigrant Juvenile (Form I-360) |
| `SI_AFGHAN_IRAQI` | Certain Afghan or Iraqi National (Form I-360 or DS-157) |
| `SI_BROADCASTER` | Certain International Broadcaster (Form I-360) |
| `SI_INTL_ORG` | Certain G-4 International Org or NATO-6 Employee/Family (Form I-360) |
| `SI_ARMED_FORCES` | Certain U.S. Armed Forces Members (Six and Six program, Form I-360) |
| `SI_PANAMA_CANAL` | Panama Canal Zone Employees (Form I-360) |
| `SI_PHYSICIANS` | Certain Physicians (Form I-360) |
| `SI_GOVT_ABROAD` | Certain Employee/Former Employee of U.S. Government Abroad (DS-1884) |
| `SI_RELIGIOUS_MINISTER` | Religious Worker - Minister of Religion |
| `SI_RELIGIOUS_OTHER` | Religious Worker - Other Religious Worker |

**3.d. Asylee or Refugee**
| Code | Description |
|------|------------|
| `AR_ASYLEE` | Asylum Status (INA section 208, Form I-589 or I-730) |
| `AR_REFUGEE` | Refugee Status (INA section 207, Form I-590 or I-730) |

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p2_3d_asylum_date` | Date you were granted asylum | DATE | mm/dd/yyyy | Y |
| `p2_3d_refugee_date` | Date of initial admission as refugee | DATE | mm/dd/yyyy | Y |

**3.e. Human Trafficking Victim or Crime Victim**
| Code | Description |
|------|------------|
| `HT_T_VISA` | Human Trafficking Victim (T Nonimmigrant, Form I-914 or I-914A) |
| `HT_U_VISA` | Victim of Qualifying Criminal Activity (U Nonimmigrant, Form I-918/I-918A/I-929) |

**3.f. Special Programs Based on Certain Public Laws**
| Code | Description |
|------|------------|
| `SP_CUBAN_ADJ` | The Cuban Adjustment Act |
| `SP_CUBAN_VAWA` | Victim of Battery/Cruelty under Cuban Adjustment Act |
| `SP_HAITIAN_DEPENDENT` | Dependent Status under Haitian Refugee Immigrant Fairness Act |
| `SP_HAITIAN_VAWA` | Victim of Battery/Cruelty under Haitian Refugee Immigrant Fairness Act |
| `SP_LAUTENBERG` | Lautenberg Parolees |
| `SP_SEC13_1957` | Diplomats/High-Ranking Officials unable to return home (Section 13, Act of 9/11/57) |
| `SP_VIETNAM_CAMBODIA_LAOS` | Nationals of Vietnam, Cambodia, Laos (PL 106-429 section 586) |
| `SP_AMERASIAN` | Amerasian Act (October 22, 1982, Form I-360) |

**3.g. Additional Options**
| Code | Description |
|------|------------|
| `AO_DIVERSITY_VISA` | Diversity Visa program |
| `AO_REGISTRY` | Continuous Residence in US Since Before January 1, 1972 ("Registry") |
| `AO_DIPLOMATIC_BIRTH` | Individual Born in the United States Under Diplomatic Status |
| `AO_S_NONIMMIGRANT` | S Nonimmigrants (approved Form I-854B filed by law enforcement) |
| `AO_OTHER` | Other Eligibility |

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p2_3g_dv_rank_number` | Diversity Visa Rank Number | VARCHAR(20) | Alphanumeric | Y |
| `p2_3g_other_eligibility` | Other Eligibility (explain) | VARCHAR(200) | Free text | Y |

### 2.2 Additional Filing Questions (Items 4-5)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p2_4_applying_245i` | Applying for adjustment based on INA section 245(i)? | BOOLEAN | Yes / No | Y |
| `p2_5_cspa` | Age 21+ and applying under Child Status Protection Act (CSPA)? | BOOLEAN | Yes / No | Y |

---

## Part 3. Request for Exemption from Affidavit of Support (Section 213A INA)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p3_exemption_reason` | Exemption reason (select one) | VARCHAR(5) | ENUM: 1a, 1b, 1c, 1d, 1e, 1f | Y |

| Code | Description |
|------|------------|
| `1a` | Earned/can receive credit for 40 qualifying quarters of work (SSA) |
| `1b` | Under 18, unmarried, child of U.S. citizen, not likely public charge (INA 320) |
| `1c` | Applying as widow/widower of U.S. citizen (Form I-360) |
| `1d` | Applying as a VAWA self-petitioner |
| `1e` | None of these exemptions apply and not required to submit affidavit nor request exemption |
| `1f` | None of these exemptions apply and not requesting exemption; required to submit affidavit |

---

## Part 4. Additional Information About You (Items 1-8)

### 4.1 Prior Immigrant Visa Application

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p4_1_applied_abroad` | Have you ever applied for immigrant visa at U.S. Embassy/Consulate abroad? | BOOLEAN | Yes / No | N |
| `p4_2_embassy_city` | Location of Embassy/Consulate - City or Town | VARCHAR(60) | Text | Y |
| `p4_2_embassy_country` | Location of Embassy/Consulate - Country | VARCHAR(60) | Country name | Y |
| `p4_3_decision` | Decision | VARCHAR(30) | Free text (approved, refused, denied, withdrawn) | Y |
| `p4_4_decision_date` | Date of Decision | DATE | mm/dd/yyyy | Y |
| `p4_5_previously_applied_us` | Have you previously applied for permanent residence in the US? | BOOLEAN | Yes / No | N |
| `p4_6_had_lpr_rescinded` | Have you EVER held LPR status later rescinded under INA 246? | BOOLEAN | Yes / No | N |

### 4.2 Employment and Educational History (Items 7-8)

**Current/Most Recent Employment or School (repeatable, last 5 years)**

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p4_7_employer_or_school` | Employer or School (current or most recent) | VARCHAR(100) | Text | N |
| `p4_7_employer_name` | Name of Employer, Company, or School | VARCHAR(100) | Text | N |
| `p4_7_occupation` | Your Occupation (if unemployed or retired, so state) | VARCHAR(100) | Text | N |
| `p4_7_street` | Address - Street Number and Name | VARCHAR(100) | Text | Y |
| `p4_7_apt_type` | Apt. / Ste. / Flr. | VARCHAR(4) | ENUM: Apt, Ste, Flr | Y |
| `p4_7_apt_number` | Unit Number | VARCHAR(10) | Alphanumeric | Y |
| `p4_7_city` | City or Town | VARCHAR(60) | Text | Y |
| `p4_7_state` | State | VARCHAR(60) | Text | Y |
| `p4_7_zip` | ZIP Code | VARCHAR(20) | Alphanumeric | Y |
| `p4_7_province` | Province | VARCHAR(60) | Text | Y |
| `p4_7_postal_code` | Postal Code | VARCHAR(20) | Alphanumeric | Y |
| `p4_7_country` | Country | VARCHAR(60) | Country name | Y |
| `p4_7_date_from` | Date From | DATE | mm/dd/yyyy | Y |
| `p4_7_date_to` | Date To | DATE | mm/dd/yyyy | Y |
| `p4_7_financial_support` | If unemployed or retired, source of financial support | VARCHAR(200) | Free text | Y |

**Most Recent Employer or School Outside the US (Item 8) — same field structure as Item 7**

---

## Part 5. Information About Your Parents (Items 1-8)

### Parent 1

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p5_1_parent1_family_name` | Parent 1's Legal Family Name (Last Name) | VARCHAR(60) | Text | N |
| `p5_1_parent1_given_name` | Parent 1's Legal Given Name (First Name) | VARCHAR(60) | Text | N |
| `p5_1_parent1_middle_name` | Parent 1's Legal Middle Name | VARCHAR(60) | Text | Y |
| `p5_2_parent1_birth_family_name` | Parent 1's Name at Birth - Family Name (if different) | VARCHAR(60) | Text | Y |
| `p5_2_parent1_birth_given_name` | Parent 1's Name at Birth - Given Name (if different) | VARCHAR(60) | Text | Y |
| `p5_2_parent1_birth_middle_name` | Parent 1's Name at Birth - Middle Name (if different) | VARCHAR(60) | Text | Y |
| `p5_3_parent1_dob` | Parent 1's Date of Birth | DATE | mm/dd/yyyy | N |
| `p5_4_parent1_country_of_birth` | Parent 1's Country of Birth | VARCHAR(60) | Country name | N |

### Parent 2

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p5_5_parent2_family_name` | Parent 2's Legal Family Name (Last Name) | VARCHAR(60) | Text | N |
| `p5_5_parent2_given_name` | Parent 2's Legal Given Name (First Name) | VARCHAR(60) | Text | N |
| `p5_5_parent2_middle_name` | Parent 2's Legal Middle Name | VARCHAR(60) | Text | Y |
| `p5_6_parent2_birth_family_name` | Parent 2's Name at Birth - Family Name (if different) | VARCHAR(60) | Text | Y |
| `p5_6_parent2_birth_given_name` | Parent 2's Name at Birth - Given Name (if different) | VARCHAR(60) | Text | Y |
| `p5_6_parent2_birth_middle_name` | Parent 2's Name at Birth - Middle Name (if different) | VARCHAR(60) | Text | Y |
| `p5_7_parent2_dob` | Parent 2's Date of Birth | DATE | mm/dd/yyyy | N |
| `p5_8_parent2_country_of_birth` | Parent 2's Country of Birth | VARCHAR(60) | Country name | N |

---

## Part 6. Information About Your Marital History (Items 1-18)

### 6.1 Current Status

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p6_1_marital_status` | Current marital status | VARCHAR(25) | ENUM: Single Never Married, Married, Divorced, Widowed, Marriage Annulled, Legally Separated | N |
| `p6_2_spouse_military` | Is your spouse a current member of the U.S. armed forces or Coast Guard? | VARCHAR(5) | ENUM: N/A, Yes, No | Y |
| `p6_3_times_married` | How many times have you been married? | INTEGER | 0-99 | N |

### 6.2 Current Spouse (Items 4-10)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p6_4_spouse_family_name` | Current Spouse's Family Name (Last Name) | VARCHAR(60) | Text | Y |
| `p6_4_spouse_given_name` | Current Spouse's Given Name (First Name) | VARCHAR(60) | Text | Y |
| `p6_4_spouse_middle_name` | Current Spouse's Middle Name | VARCHAR(60) | Text | Y |
| `p6_5_spouse_a_number` | Current Spouse's A-Number | VARCHAR(9) | 9-digit numeric | Y |
| `p6_6_spouse_dob` | Current Spouse's Date of Birth | DATE | mm/dd/yyyy | Y |
| `p6_7_spouse_country_of_birth` | Current Spouse's Country of Birth | VARCHAR(60) | Country name | Y |
| `p6_8_spouse_street` | Current Spouse's Address - Street | VARCHAR(100) | Text | Y |
| `p6_8_spouse_apt_type` | Apt. / Ste. / Flr. | VARCHAR(4) | ENUM | Y |
| `p6_8_spouse_apt_number` | Unit Number | VARCHAR(10) | Text | Y |
| `p6_8_spouse_city` | City or Town | VARCHAR(60) | Text | Y |
| `p6_8_spouse_state` | State | VARCHAR(60) | Text | Y |
| `p6_8_spouse_zip` | ZIP Code | VARCHAR(20) | Alphanumeric | Y |
| `p6_8_spouse_province` | Province | VARCHAR(60) | Text | Y |
| `p6_8_spouse_postal_code` | Postal Code | VARCHAR(20) | Alphanumeric | Y |
| `p6_8_spouse_country` | Country | VARCHAR(60) | Country name | Y |
| `p6_9_marriage_city` | Place of Marriage - City or Town | VARCHAR(60) | Text | Y |
| `p6_9_marriage_state` | Place of Marriage - State or Province | VARCHAR(60) | Text | Y |
| `p6_9_marriage_country` | Place of Marriage - Country | VARCHAR(60) | Country name | Y |
| `p6_9_marriage_date` | Date of Marriage to Current Spouse | DATE | mm/dd/yyyy | Y |
| `p6_10_spouse_applying` | Is your current spouse applying with you? | BOOLEAN | Yes / No | Y |

### 6.3 Prior Spouse (Items 11-18, repeatable)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p6_11_prior_spouse_family_name` | Prior Spouse's Family Name (family name before marriage) | VARCHAR(60) | Text | Y |
| `p6_11_prior_spouse_given_name` | Prior Spouse's Given Name | VARCHAR(60) | Text | Y |
| `p6_11_prior_spouse_middle_name` | Prior Spouse's Middle Name | VARCHAR(60) | Text | Y |
| `p6_12_prior_spouse_dob` | Prior Spouse's Date of Birth | DATE | mm/dd/yyyy | Y |
| `p6_13_prior_spouse_country_of_birth` | Prior Spouse's Country of Birth | VARCHAR(60) | Country name | Y |
| `p6_14_prior_spouse_citizenship` | Prior Spouse's Country of Citizenship or Nationality | VARCHAR(60) | Country name | Y |
| `p6_15_prior_marriage_date` | Date of Marriage to Prior Spouse | DATE | mm/dd/yyyy | Y |
| `p6_16_prior_marriage_city` | Place of Marriage - City or Town | VARCHAR(60) | Text | Y |
| `p6_16_prior_marriage_state` | Place of Marriage - State or Province | VARCHAR(60) | Text | Y |
| `p6_16_prior_marriage_country` | Place of Marriage - Country | VARCHAR(60) | Country name | Y |
| `p6_17_prior_marriage_end_city` | Place Where Marriage Ended - City or Town | VARCHAR(60) | Text | Y |
| `p6_17_prior_marriage_end_state` | Place Where Marriage Ended - State or Province | VARCHAR(60) | Text | Y |
| `p6_17_prior_marriage_end_country` | Place Where Marriage Ended - Country | VARCHAR(60) | Country name | Y |
| `p6_17_prior_marriage_end_date` | Date Marriage Legally Ended | DATE | mm/dd/yyyy | Y |
| `p6_18_how_marriage_ended` | How Marriage Ended | VARCHAR(20) | ENUM: Annulled, Divorced, Spouse Deceased, Other | Y |
| `p6_18_how_ended_other` | Other (Explain) | VARCHAR(100) | Free text | Y |

---

## Part 7. Information About Your Children (Items 1-3)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p7_1_total_children` | Total number of ALL living children anywhere in the world | INTEGER | 0-99 | N |

**Per Child (repeatable, 2 on form, additional via Part 14)**

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p7_child_family_name` | Child's Family Name (Last Name) | VARCHAR(60) | Text | Y |
| `p7_child_given_name` | Child's Given Name (First Name) | VARCHAR(60) | Text | Y |
| `p7_child_middle_name` | Child's Middle Name | VARCHAR(60) | Text | Y |
| `p7_child_a_number` | Child's A-Number (if any) | VARCHAR(9) | 9-digit numeric | Y |
| `p7_child_dob` | Child's Date of Birth | DATE | mm/dd/yyyy | Y |
| `p7_child_country_of_birth` | Child's Country of Birth | VARCHAR(60) | Country name | Y |
| `p7_child_relationship` | Relationship to you | VARCHAR(40) | Free text (biological, stepchild, legally adopted) | Y |
| `p7_child_also_applying` | Is this child also applying on a separate Form I-485? | BOOLEAN | Yes / No | Y |

---

## Part 8. Biographic Information (Items 1-6)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p8_1_ethnicity` | Ethnicity | VARCHAR(25) | ENUM: Hispanic or Latino, Not Hispanic or Latino | N |
| `p8_2_race` | Race (select all applicable) | VARCHAR(100) | SET: American Indian or Alaska Native, Asian, Black or African American, Native Hawaiian or Other Pacific Islander, White | N |
| `p8_3_height_feet` | Height - Feet | INTEGER | 1-9 | N |
| `p8_3_height_inches` | Height - Inches | INTEGER | 0-11 | N |
| `p8_4_weight_pounds` | Weight - Pounds | INTEGER | 1-999 | N |
| `p8_5_eye_color` | Eye Color | VARCHAR(15) | ENUM: Black, Blue, Brown, Gray, Green, Hazel, Maroon, Pink, Unknown/Other | N |
| `p8_6_hair_color` | Hair Color | VARCHAR(15) | ENUM: Bald (No hair), Black, Blond, Brown, Gray, Red, Sandy, White, Unknown/Other | N |

---

## Part 9. General Eligibility and Inadmissibility Grounds (Items 1-86)

### 9.1 Organization Membership (Items 1-9, 2 organizations on form, additional via Part 14)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p9_1_ever_member_org` | Have you EVER been a member of any organization? | BOOLEAN | Yes / No | N |
| `p9_org_name` | Name of Organization | VARCHAR(100) | Text (repeatable) | Y |
| `p9_org_city` | City or Town | VARCHAR(60) | Text | Y |
| `p9_org_state` | State or Province | VARCHAR(60) | Text | Y |
| `p9_org_country` | Country | VARCHAR(60) | Country name | Y |
| `p9_org_nature` | Nature of Organization (purposes and activities) | VARCHAR(500) | Free text | Y |
| `p9_org_involvement` | Nature of involvement (role/positions held) | VARCHAR(500) | Free text | Y |
| `p9_org_date_from` | Dates of Membership - From | DATE | mm/dd/yyyy | Y |
| `p9_org_date_to` | Dates of Membership - To | DATE | mm/dd/yyyy | Y |

### 9.2 Immigration History (Items 10-21) — All BOOLEAN Yes/No

| Field ID | Item | Question |
|----------|------|---------|
| `p9_10_denied_admission` | 10 | Have you EVER been denied admission to the US? |
| `p9_11_denied_visa` | 11 | Have you EVER been denied a visa to the US? |
| `p9_12_worked_unauthorized` | 12 | Have you EVER worked in the US without authorization? |
| `p9_13_violated_status` | 13 | Have you EVER violated the terms of your nonimmigrant status? |
| `p9_14_removal_proceedings` | 14 | Are you presently or have you EVER been in removal/deportation proceedings? |
| `p9_15_final_order` | 15 | Have you EVER been issued a final order of exclusion/deportation/removal? |
| `p9_16_prior_order_reinstated` | 16 | Have you EVER had a prior final order reinstated? |
| `p9_17_voluntary_departure_failed` | 17 | Have you EVER been granted voluntary departure but failed to depart? |
| `p9_18_applied_relief` | 18 | Have you EVER applied for relief from removal/deportation? |
| `p9_19_j_visa_2year` | 19 | Have you EVER been a J nonimmigrant subject to 2-year foreign residence requirement? |
| `p9_20_complied_2year` | 20 | If Yes to 19, have you complied with the requirement? |
| `p9_21_waiver_granted` | 21 | If Yes to 19 and No to 20, have you been granted a waiver? |

### 9.3 Criminal Acts and Violations (Items 22-41) — All BOOLEAN Yes/No

| Field ID | Item | Question |
|----------|------|---------|
| `p9_22_ever_arrested` | 22 | Have you EVER been arrested, cited, charged, or detained? |
| `p9_23_ever_committed_crime` | 23 | Have you EVER committed a crime of any kind? |
| `p9_24_pled_guilty` | 24 | Have you EVER pled guilty to or been convicted of a crime or offense? |
| `p9_25_ordered_punished` | 25 | Have you EVER been ordered punished or had conditions imposed on your liberty? |
| `p9_26_controlled_substance` | 26 | Have you EVER violated any controlled substance law? |
| `p9_27_drug_trafficking` | 27 | Have you EVER trafficked in or aided trafficking of controlled substances? |
| `p9_28_family_drug_trafficking` | 28 | Are you the spouse/child of an alien drug trafficker and received financial benefit? |
| `p9_29_knew_drug_benefit` | 29 | If Yes to 28, did you know the benefit resulted from trafficking? |
| `p9_30_prostitution` | 30 | Have you EVER engaged in prostitution? |
| `p9_31_procured_prostitution` | 31 | Have you EVER procured or imported prostitutes? |
| `p9_32_prostitution_proceeds` | 32 | Have you EVER received proceeds from prostitution? |
| `p9_33_commercialized_vice` | 33 | Do you intend to engage in illegal gambling or other commercialized vice? |
| `p9_34_diplomatic_immunity` | 34 | Have you EVER exercised diplomatic immunity to avoid prosecution? |
| `p9_35a_foreign_govt_official` | 35.a | Have you EVER served as a foreign government official? |
| `p9_35b_religious_freedom_violation` | 35.b | If Yes to 35.a, have you EVER been responsible for violations of religious freedoms? |
| `p9_36_sex_trafficking` | 36 | Have you EVER induced trafficking for commercial sex acts? |
| `p9_37_labor_trafficking` | 37 | Have you EVER trafficked a person into involuntary servitude/slavery? |
| `p9_38_aided_trafficking` | 38 | Have you EVER aided/abetted others in trafficking for sex/servitude? |
| `p9_39_family_trafficking_benefit` | 39 | Are you the spouse/child of a trafficking alien and received financial benefit? |
| `p9_40_knew_trafficking_benefit` | 40 | If Yes to 39, did you know the benefit resulted from trafficking? |
| `p9_41_money_laundering` | 41 | Have you EVER engaged in money laundering? |

### 9.4 Security and Related (Items 42-55) — All BOOLEAN Yes/No

| Field ID | Item | Question |
|----------|------|---------|
| `p9_42a_espionage` | 42.a | Do you intend to engage in espionage or sabotage? |
| `p9_42b_export_violation` | 42.b | Do you intend to violate export control laws? |
| `p9_42c_overthrow_govt` | 42.c | Do you intend to oppose/overthrow the U.S. Government by force? |
| `p9_42d_other_unlawful` | 42.d | Do you intend to engage in any other unlawful activity? |
| `p9_43a_weapons_training` | 43.a | Have you EVER received weapons/paramilitary training? |
| `p9_43b_kidnapping_hijacking` | 43.b | Have you EVER committed kidnapping, assassination, or hijacking? |
| `p9_43c_used_weapon` | 43.c | Have you EVER used a weapon/explosive to endanger safety? |
| `p9_43d_threatened_above` | 43.d | Have you EVER threatened/planned to do things in 43.b-43.c? |
| `p9_43e_incited_violence` | 43.e | Have you EVER incited death or serious harm related to 43.b-43.c? |
| `p9_43f_member_of_violent_group` | 43.f | Have you EVER been a member of a group that did activities in 43.b-43.e? |
| `p9_43g_recruited_for_violence` | 43.g | Have you EVER recruited for a group that did activities in 43.b-43.e? |
| `p9_43h_provided_support` | 43.h | Have you EVER provided money/support for activities in 43.b-43.e? |
| `p9_43i_provided_support_individual` | 43.i | Have you EVER provided support to an individual who did activities in 43.b-43.e? |
| `p9_44_intend_violence` | 44 | Do you intend to engage in activities in 43.b-43.e? |
| `p9_45_endanger_us` | 45 | Do you intend to endanger welfare/safety/security of the US? |
| `p9_46_spouse_child_of_violent` | 46 | Are you the spouse/child of an individual who engaged in 43.b-43.i? |
| `p9_47_sold_weapons` | 47 | Have you EVER sold/transported weapons? |
| `p9_48_prison_labor_camp` | 48 | Have you EVER worked/volunteered in a prison/detention facility? |
| `p9_49_member_weapon_group` | 49 | Have you EVER been a member of a group that used weapons against any person? |
| `p9_50_military_police` | 50 | Have you EVER served in any military or police unit? |
| `p9_51_armed_group` | 51 | Have you EVER served in any armed group (paramilitary, guerrilla, etc.)? |
| `p9_52_communist_totalitarian` | 52 | Have you EVER been affiliated with the Communist Party or totalitarian party? |
| `p9_53a_torture` | 53.a | Have you EVER ordered/participated in torture? |
| `p9_53b_genocide` | 53.b | Have you EVER ordered/participated in genocide? |
| `p9_53c_killing` | 53.c | Have you EVER killed or tried to kill any person? |
| `p9_53d_injuring` | 53.d | Have you EVER intentionally and severely injured any person? |
| `p9_54_child_soldiers` | 54 | Have you EVER recruited child soldiers (under 15)? |
| `p9_55_used_child_soldiers` | 55 | Have you EVER used child soldiers (under 15) in hostilities? |

### 9.5 Public Charge (Items 56-66)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p9_56_public_charge_exempt_category` | Public charge exemption category (select one) | VARCHAR(50) | See 26 ENUM values from form page 18-19 | Y |
| `p9_57_household_size` | What is the size of your household? | INTEGER | 1-99 | Y |
| `p9_58_annual_income` | Annual household income | VARCHAR(25) | ENUM: $0-27000, $27001-52000, $52001-85000, $85001-141000, Over $141000 | Y |
| `p9_59_household_assets` | Total value of household assets | VARCHAR(25) | ENUM: $0-18400, $18401-136000, $136001-321400, $321401-707100, Over $707100 | Y |
| `p9_60_household_liabilities` | Total value of household liabilities | VARCHAR(25) | ENUM: $0, $1-10100, $10101-57700, $57701-186800, Over $186800 | Y |
| `p9_61_highest_education` | Highest degree or grade of school completed | VARCHAR(50) | ENUM: Less than high school, High school/GED, Some college no degree, Associate's, Bachelor's, Master's, Professional (JD/MD/DMD), Doctorate | Y |
| `p9_61_highest_grade` | If less than HS, highest grade completed | VARCHAR(20) | Free text | Y |
| `p9_62_certifications` | List of certifications, licenses, skills | TEXT | Free text (repeatable rows) | Y |
| `p9_63_received_public_benefits` | Have you ever received SSI, TANF, or other cash benefits? | BOOLEAN | Yes / No | Y |
| `p9_64_long_term_institutionalized` | Have you ever received long-term institutionalization at government expense? | BOOLEAN | Yes / No | Y |

**Benefits Received Table (Item 65, repeatable rows)**

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p9_65_benefit_received` | Benefit Received | VARCHAR(100) | Text | Y |
| `p9_65_benefit_start_date` | Start Date | DATE | mm/dd/yyyy | Y |
| `p9_65_benefit_end_date` | End Date | DATE | mm/dd/yyyy | Y |
| `p9_65_benefit_dollar_amount` | Dollar Amount | DECIMAL(12,2) | Currency | Y |
| `p9_65_benefit_exempt` | In a Category Exempt from Public Charge? | BOOLEAN | Yes / No | Y |

**Institutionalization Table (Item 66, repeatable rows)**

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p9_66_institution_name` | Institution Name/City/State | VARCHAR(150) | Text | Y |
| `p9_66_inst_date_from` | Date From | DATE | mm/dd/yyyy | Y |
| `p9_66_inst_date_to` | Date To | DATE | mm/dd/yyyy | Y |
| `p9_66_inst_reason` | Reason | VARCHAR(200) | Text | Y |
| `p9_66_inst_exempt` | In a Category Exempt from Public Charge? | BOOLEAN | Yes / No | Y |

### 9.6 Illegal Entries and Other Immigration Violations (Items 67-75) — All BOOLEAN Yes/No

| Field ID | Item | Question |
|----------|------|---------|
| `p9_67_failed_attend_removal` | 67 | Have you EVER failed to attend a removal proceeding (on/after April 1, 1997)? |
| `p9_68_fraudulent_documents` | 68 | Have you EVER submitted altered/fraudulent documents for immigration benefit? |
| `p9_69_lied_on_application` | 69 | Have you EVER lied or misrepresented on an immigration application? |
| `p9_70_falsely_claimed_citizen` | 70 | Have you EVER falsely claimed to be a U.S. citizen? |
| `p9_71_stowaway` | 71 | Have you EVER been a stowaway on a vessel or aircraft? |
| `p9_72_alien_smuggling` | 72 | Have you EVER encouraged/aided an alien to enter the US illegally? |
| `p9_73_civil_penalty_274c` | 73 | Are you under a final order of civil penalty for INA 274C (fraudulent documents)? |
| `p9_74_deported_removed` | 74 | Have you EVER been excluded, deported, or removed from the US? |
| `p9_75_entered_without_inspection` | 75 | Have you EVER entered the US without being inspected and admitted/paroled? |

### 9.7 Removal, Unlawful Presence, or Illegal Reentry (Items 76-78) — All BOOLEAN Yes/No

| Field ID | Item | Question |
|----------|------|---------|
| `p9_76_unlawfully_present` | 76 | Since April 1, 1997, have you been unlawfully present in the US? |
| `p9_77_trafficking_victim` | 77 | If Yes to 76, was a severe form of trafficking the central reason? |
| `p9_78a_reentry_after_unlawful` | 78.a | Having been unlawfully present >1 year, have you EVER reentered without inspection? |
| `p9_78b_reentry_after_removal` | 78.b | Having been deported/removed, have you reentered without inspection? |

### 9.8 Miscellaneous Conduct (Items 79-86) — All BOOLEAN Yes/No

| Field ID | Item | Question |
|----------|------|---------|
| `p9_79_polygamy` | 79 | Do you plan to practice polygamy? |
| `p9_80_accompanying_inadmissible` | 80 | Are you accompanying an inadmissible alien who is helpless? |
| `p9_81_child_custody_violation` | 81 | Have you EVER assisted in withholding custody of a U.S. citizen child? |
| `p9_82_voted_illegally` | 82 | Have you EVER voted in violation of any law? |
| `p9_83_renounced_citizenship` | 83 | Have you EVER renounced U.S. citizenship to avoid being taxed? |
| `p9_84a_military_exemption` | 84.a | Have you EVER applied for exemption from US military service as an alien? |
| `p9_84b_military_discharged` | 84.b | Have you EVER been relieved or discharged from military as an alien? |
| `p9_84c_desertion` | 84.c | Have you EVER been convicted of desertion from the U.S. armed forces? |
| `p9_85_left_during_war` | 85 | Have you EVER left/remained outside the US to avoid military service during war? |
| `p9_86_status_before_leaving` | 86 | If Yes to 85, what was your nationality/immigration status before you left? | VARCHAR(100) |

---

## Part 10. Applicant's Contact Information, Certification, and Signature

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p10_1_daytime_phone` | Applicant's Daytime Telephone Number | VARCHAR(20) | Phone number | Y |
| `p10_2_mobile_phone` | Applicant's Mobile Telephone Number | VARCHAR(20) | Phone number | Y |
| `p10_3_email` | Applicant's Email Address | VARCHAR(100) | Email address | Y |
| `p10_4_signature` | Applicant's Signature | SIGNATURE | Ink signature | N |
| `p10_4_signature_date` | Date of Signature | DATE | mm/dd/yyyy | N |

---

## Part 11. Interpreter's Contact Information, Certification, and Signature

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p11_1_family_name` | Interpreter's Family Name (Last Name) | VARCHAR(60) | Text | Y |
| `p11_1_given_name` | Interpreter's Given Name (First Name) | VARCHAR(60) | Text | Y |
| `p11_2_business_name` | Interpreter's Business or Organization Name | VARCHAR(100) | Text | Y |
| `p11_3_daytime_phone` | Interpreter's Daytime Telephone Number | VARCHAR(20) | Phone number | Y |
| `p11_4_mobile_phone` | Interpreter's Mobile Telephone Number | VARCHAR(20) | Phone number | Y |
| `p11_5_email` | Interpreter's Email Address | VARCHAR(100) | Email address | Y |
| `p11_language` | Language interpreted (fluent in English and ___) | VARCHAR(60) | Language name | Y |
| `p11_6_signature` | Interpreter's Signature | SIGNATURE | Ink signature | Y |
| `p11_6_signature_date` | Date of Signature | DATE | mm/dd/yyyy | Y |

---

## Part 12. Preparer's Contact Information, Certification, and Signature

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p12_1_family_name` | Preparer's Family Name (Last Name) | VARCHAR(60) | Text | Y |
| `p12_1_given_name` | Preparer's Given Name (First Name) | VARCHAR(60) | Text | Y |
| `p12_2_business_name` | Preparer's Business or Organization Name | VARCHAR(100) | Text | Y |
| `p12_3_daytime_phone` | Preparer's Daytime Telephone Number | VARCHAR(20) | Phone number | Y |
| `p12_4_mobile_phone` | Preparer's Mobile Telephone Number | VARCHAR(20) | Phone number | Y |
| `p12_5_email` | Preparer's Email Address | VARCHAR(100) | Email address | Y |
| `p12_6_signature` | Preparer's Signature | SIGNATURE | Ink signature | Y |
| `p12_6_signature_date` | Date of Signature | DATE | mm/dd/yyyy | Y |

---

## Part 13. Signature at Interview (USCIS Officer completes at interview)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p13_num_changes` | Number of changes made | INTEGER | Numeric | Y |
| `p13_changes_through` | Changes numbered ___ through ___ | VARCHAR(20) | Range | Y |
| `p13_additional_pages` | Additional numbered pages ___ through ___ | VARCHAR(20) | Range | Y |
| `p13_officer_name` | USCIS Officer's Printed Name or Stamp | VARCHAR(100) | Text | Y |
| `p13_officer_signature_date` | Date of Signature (Officer) | DATE | mm/dd/yyyy | Y |
| `p13_applicant_signature` | Applicant's Signature (sign in ink) | SIGNATURE | Ink signature | Y |
| `p13_officer_signature` | USCIS Officer's Signature (sign in ink) | SIGNATURE | Ink signature | Y |

---

## Part 14. Additional Information (Overflow)

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p14_family_name` | Family Name (Last Name) | VARCHAR(60) | Text | Y |
| `p14_given_name` | Given Name (First Name) | VARCHAR(60) | Text | Y |
| `p14_middle_name` | Middle Name | VARCHAR(60) | Text | Y |

**Per Additional Item (repeatable, 4 blocks on form, additional pages allowed)**

| Field ID | Field Name | Type | Values / Format | Nullable |
|----------|-----------|------|----------------|----------|
| `p14_page_number` | Page Number | INTEGER | Form page reference | Y |
| `p14_part_number` | Part Number | INTEGER | 1-14 | Y |
| `p14_item_number` | Item Number | VARCHAR(10) | Item reference | Y |
| `p14_additional_text` | Additional Information | TEXT | Free text (multiline) | Y |

---

## Summary Statistics

| Metric | Count |
|--------|-------|
| **Total Parts** | 14 |
| **Total Form Pages** | 24 |
| **Total Unique Fields** | ~340 |
| **Text/VARCHAR fields** | ~170 |
| **BOOLEAN (Yes/No) fields** | ~95 |
| **DATE fields** | ~40 |
| **ENUM/Select fields** | ~25 |
| **INTEGER fields** | ~8 |
| **DECIMAL fields** | ~1 |
| **SIGNATURE fields** | ~6 |
| **Repeatable Sections** | 8 (other names, prior addresses, employment history, children, organizations, prior marriages, benefits, institutionalization) |
| **Filing Categories** | 48 distinct codes across 7 sub-groups |
| **Inadmissibility Questions** | 66 (Items 10-86) |
