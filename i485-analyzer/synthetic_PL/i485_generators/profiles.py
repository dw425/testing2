"""I-485 profile builder — generates coherent person records.

Every table generator draws from the profile DataFrame returned by
``build_profiles()``, ensuring cross-table consistency (names, dates,
addresses, family relationships, etc.).
"""

import os
import sys

import numpy as np
import pandas as pd
from faker import Faker

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SEED
from utils import pick, gen_dates

from config_i485 import (
    CATEGORY_CODES, CATEGORY_WEIGHTS, CATEGORY_GROUPS,
    COUNTRY_CODES, COUNTRY_WEIGHTS, NON_LATIN_COUNTRIES, FAKER_LOCALES,
    SEX_OPTIONS, SEX_WEIGHTS,
    ETHNICITY_OPTIONS, ETHNICITY_WEIGHTS,
    RACE_OPTIONS, RACE_WEIGHTS,
    AGE_BRACKETS, AGE_BRACKET_WEIGHTS,
    MARITAL_OPTIONS, MARITAL_WEIGHTS,
    EYE_COLOR_OPTIONS, EYE_COLOR_WEIGHTS,
    HAIR_COLOR_OPTIONS, HAIR_COLOR_WEIGHTS,
    STATUS_OPTIONS, STATUS_WEIGHTS,
    STATE_CODES, STATE_WEIGHTS,
    SECTION_OF_LAW_OPTIONS, SECTION_OF_LAW_WEIGHTS,
    SECTION_OF_LAW_BY_GROUP,
    RECEIPT_PREFIXES, RECEIPT_PREFIX_WEIGHTS,
    ARRIVAL_TYPES, ARRIVAL_TYPE_WEIGHTS,
    IMMIGRATION_STATUS_OPTIONS,
    CHILDREN_COUNTS, CHILDREN_WEIGHTS,
    FILING_DATE_START, FILING_DATE_END,
    ARRIVAL_DATE_START, ARRIVAL_DATE_END,
)

# ── Romanised name pools for non-Latin-script countries ─────────────────────
_SURNAMES = {
    "China":       ["Wang", "Li", "Zhang", "Liu", "Chen", "Yang", "Huang",
                    "Zhou", "Wu", "Zhao", "Lin", "Zhu", "Ma", "Guo", "He",
                    "Luo", "Gao", "Xu", "Sun", "Hu"],
    "Taiwan":      ["Chen", "Lin", "Huang", "Zhang", "Li", "Wang", "Wu",
                    "Liu", "Cai", "Yang", "Xu", "Zheng", "Xie", "Guo"],
    "South Korea": ["Kim", "Lee", "Park", "Choi", "Jung", "Kang", "Cho",
                    "Yoon", "Jang", "Lim", "Song", "Shin", "Oh", "Seo"],
    "Japan":       ["Sato", "Suzuki", "Takahashi", "Tanaka", "Watanabe",
                    "Ito", "Yamamoto", "Nakamura", "Kobayashi", "Kato",
                    "Yoshida", "Yamada", "Sasaki", "Matsumoto"],
    "Vietnam":     ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Vo",
                    "Dang", "Bui", "Do", "Ho", "Ngo", "Duong", "Ly"],
    "India":       ["Patel", "Sharma", "Singh", "Kumar", "Shah", "Gupta",
                    "Reddy", "Rao", "Joshi", "Verma", "Mishra", "Das",
                    "Nair", "Pillai", "Desai", "Mehta", "Agarwal", "Iyer"],
    "Bangladesh":  ["Ahmed", "Hossain", "Rahman", "Islam", "Khan",
                    "Chowdhury", "Alam", "Miah", "Uddin", "Begum"],
    "Pakistan":    ["Khan", "Ali", "Ahmed", "Malik", "Hussain", "Sheikh",
                    "Iqbal", "Butt", "Qureshi", "Siddiqui"],
    "Nepal":       ["Sharma", "Thapa", "Adhikari", "Shrestha", "Gurung",
                    "Tamang", "Rai", "Lama", "Bhandari", "Karki"],
    "Sri Lanka":   ["Fernando", "Perera", "De Silva", "Jayawardena",
                    "Bandara", "Gunaratne", "Wijesinghe", "Dissanayake"],
    "Iran":        ["Mohammadi", "Hosseini", "Rezaei", "Moradi", "Ahmadi",
                    "Hashemi", "Mousavi", "Jafari", "Karimi"],
    "Egypt":       ["Mohamed", "Ahmed", "Hassan", "Ali", "Ibrahim",
                    "Mahmoud", "Mustafa", "Osman", "Khalil"],
    "Syria":       ["Al-Ali", "Al-Ahmad", "Al-Hassan", "Al-Hussein",
                    "Al-Mohammad", "Al-Omar", "Haddad", "Khoury"],
    "Iraq":        ["Al-Saadi", "Al-Rashid", "Al-Maliki", "Al-Jabouri",
                    "Al-Tamimi", "Al-Dulaimi", "Hussein", "Abbas"],
    "Afghanistan": ["Ahmadzai", "Noorzai", "Wardak", "Popalzai",
                    "Mohammadi", "Ghani", "Karimi", "Rahimi"],
    "Russia":      ["Ivanov", "Smirnov", "Kuznetsov", "Popov", "Vasilev",
                    "Petrov", "Sokolov", "Mikhailov", "Fedorov"],
    "Ukraine":     ["Shevchenko", "Bondarenko", "Kovalenko", "Tkachenko",
                    "Boyko", "Kravchenko", "Lysenko", "Marchenko"],
    "Israel":      ["Cohen", "Levy", "Mizrahi", "Peretz", "Friedman",
                    "Goldberg", "Shapiro", "Abramov", "Katz"],
    "Thailand":    ["Saetang", "Srisai", "Boonsri", "Suksa", "Wongsawat",
                    "Chaiyasit", "Phumphothingam", "Wongpanit"],
    "Myanmar":     ["Aung", "Win", "Htun", "Kyaw", "Zaw", "Hla", "Naing"],
    "Cambodia":    ["Sok", "Chhun", "Chea", "Kim", "Meas", "Heng", "Phan"],
    "Laos":        ["Phongsa", "Chanthavong", "Sisavath", "Boupha",
                    "Souvannakhily", "Keomany"],
    "Jordan":      ["Al-Masri", "Al-Rashed", "Saleh", "Nasser", "Obeid"],
    "Lebanon":     ["Khoury", "Haddad", "Nassar", "Sabbagh", "Rizk"],
    "Somalia":     ["Mohamed", "Hassan", "Ali", "Abdi", "Omar", "Farah"],
    "Sudan":       ["Ahmed", "Mohamed", "Ibrahim", "Osman", "Abdalla"],
    "Uzbekistan":  ["Karimov", "Rakhimov", "Tashmatov", "Mirzaev",
                    "Abdullaev", "Yusupov"],
}

_FIRST_MALE = {
    "China":       ["Wei", "Jian", "Ming", "Hao", "Jun", "Tao", "Lei",
                    "Feng", "Yong", "Kai", "Bin", "Peng", "Qiang", "Yi"],
    "Taiwan":      ["Wei", "Chih", "Ming", "Chia", "Cheng", "Yu", "Hao",
                    "Hsiang", "Tzu", "Yi"],
    "South Korea": ["Minjun", "Seojun", "Doyun", "Jiho", "Junseo",
                    "Yejun", "Hyun", "Sung", "Taehyun", "Donghyun"],
    "Japan":       ["Hiroshi", "Takeshi", "Kenji", "Yuki", "Daiki",
                    "Sota", "Ren", "Haruto", "Yuto", "Riku"],
    "Vietnam":     ["Minh", "Duc", "Hieu", "Tuan", "Hoang", "Long",
                    "Thanh", "Quang", "Hai", "Nam"],
    "India":       ["Amit", "Raj", "Vikram", "Sanjay", "Arun", "Pradeep",
                    "Deepak", "Suresh", "Rahul", "Ravi", "Arjun", "Kiran"],
    "Bangladesh":  ["Rahim", "Karim", "Tariq", "Jamal", "Rashid",
                    "Farhan", "Imran", "Sohel"],
    "Pakistan":    ["Omar", "Hassan", "Bilal", "Faisal", "Usman", "Zain",
                    "Hamza", "Asad", "Saad"],
    "Nepal":       ["Bikash", "Sunil", "Rajesh", "Sanjay", "Deepak",
                    "Ramesh", "Prakash"],
    "Sri Lanka":   ["Ashan", "Chathura", "Dinesh", "Nuwan", "Kasun"],
    "Iran":        ["Ali", "Mohammad", "Reza", "Amir", "Hossein",
                    "Mehdi", "Saeed", "Ahmad"],
    "Egypt":       ["Omar", "Youssef", "Khaled", "Tarek", "Amr",
                    "Mahmoud", "Mostafa"],
    "Syria":       ["Ahmad", "Khalil", "Yasir", "Samir", "Nabil"],
    "Iraq":        ["Ali", "Hussein", "Omar", "Ahmed", "Hassan", "Mustafa"],
    "Afghanistan": ["Ahmad", "Abdul", "Mohammad", "Hamid", "Nasir"],
    "Russia":      ["Dmitry", "Alexei", "Sergei", "Andrei", "Nikolai",
                    "Vladimir", "Pavel", "Mikhail"],
    "Ukraine":     ["Oleksandr", "Andrii", "Dmytro", "Yuriy", "Taras",
                    "Bohdan", "Mykola"],
    "Israel":      ["David", "Moshe", "Yosef", "Avraham", "Itzhak",
                    "Yaakov", "Ari", "Noam"],
    "Thailand":    ["Somchai", "Sompong", "Nattapong", "Prawit",
                    "Tanawat", "Kittipong"],
    "Myanmar":     ["Aung", "Zaw", "Kyaw", "Myo", "Thet", "Hlaing"],
    "Cambodia":    ["Sokha", "Dara", "Rithy", "Vann", "Chanthy"],
    "Laos":        ["Souphanouvong", "Kham", "Boun", "Seng", "Thong"],
    "Jordan":      ["Faris", "Tariq", "Sami", "Nidal", "Ziad"],
    "Lebanon":     ["Georges", "Pierre", "Elie", "Antoine", "Michel"],
    "Somalia":     ["Abdi", "Mahdi", "Yusuf", "Saeed", "Nur"],
    "Sudan":       ["Bashir", "Idris", "Hamza", "Yousif", "Gamal"],
    "Uzbekistan":  ["Timur", "Rustam", "Sherzod", "Jamshid", "Alisher"],
}

_FIRST_FEMALE = {
    "China":       ["Mei", "Xia", "Ying", "Jing", "Lan", "Fang", "Yan",
                    "Xue", "Ping", "Li", "Na", "Hui", "Yun", "Wen"],
    "Taiwan":      ["Yiting", "Shufen", "Meiling", "Yawen", "Huiwen",
                    "Peishan", "Yuru"],
    "South Korea": ["Jiyeon", "Seoyeon", "Hayoon", "Eunji", "Minji",
                    "Subin", "Yeji", "Dahyun"],
    "Japan":       ["Yuko", "Haruka", "Sakura", "Aoi", "Hana", "Mei",
                    "Rin", "Mio", "Yui", "Akari"],
    "Vietnam":     ["Linh", "Huong", "Lan", "Mai", "Ngoc", "Thao",
                    "Trang", "Hanh", "Phuong", "Van"],
    "India":       ["Priya", "Sunita", "Anita", "Kavita", "Lakshmi",
                    "Deepa", "Neha", "Pooja", "Aarti", "Sita", "Meera"],
    "Bangladesh":  ["Fatima", "Ayesha", "Nasrin", "Sultana", "Rehana"],
    "Pakistan":    ["Ayesha", "Fatima", "Sana", "Hira", "Zara", "Amna"],
    "Nepal":       ["Sita", "Rita", "Gita", "Sunita", "Anita", "Laxmi"],
    "Sri Lanka":   ["Nilmini", "Chamari", "Kumari", "Sachini", "Dilhani"],
    "Iran":        ["Zahra", "Fatemeh", "Maryam", "Sara", "Negar",
                    "Niloofar", "Shirin"],
    "Egypt":       ["Fatma", "Nour", "Sara", "Mariam", "Dina", "Heba"],
    "Syria":       ["Nour", "Rana", "Reem", "Lina", "Hala"],
    "Iraq":        ["Zainab", "Noor", "Fatima", "Sara", "Maryam"],
    "Afghanistan": ["Fatima", "Zahra", "Mariam", "Nazifa", "Freshta"],
    "Russia":      ["Anna", "Maria", "Elena", "Olga", "Natalia",
                    "Irina", "Ekaterina", "Tatiana"],
    "Ukraine":     ["Oksana", "Natalia", "Iryna", "Olena", "Svitlana",
                    "Tetiana", "Yulia"],
    "Israel":      ["Sarah", "Rachel", "Miriam", "Leah", "Tamar",
                    "Noa", "Shira"],
    "Thailand":    ["Siriwan", "Ploy", "Nan", "Fah", "Kanya", "Ning"],
    "Myanmar":     ["Aye", "Khin", "Hla", "Thin", "May", "Su"],
    "Cambodia":    ["Chanthou", "Bopha", "Srey", "Maly", "Theary"],
    "Laos":        ["Pheng", "Keo", "Dao", "Bouapha", "Manola"],
    "Jordan":      ["Rania", "Lina", "Hana", "Noor", "Dina"],
    "Lebanon":     ["Nadia", "Maya", "Rita", "Layla", "Rima"],
    "Somalia":     ["Amina", "Halima", "Khadija", "Fartun", "Hodan"],
    "Sudan":       ["Amira", "Hanan", "Muna", "Rania", "Salma"],
    "Uzbekistan":  ["Dilnoza", "Gulnora", "Nigora", "Shahlo", "Nodira"],
}

# ── Faker instance cache ───────────────────────────────────────────────────
_faker_cache: dict[str, Faker] = {}


def _get_faker(locale: str) -> Faker:
    if locale not in _faker_cache:
        try:
            _faker_cache[locale] = Faker(locale)
        except Exception:
            _faker_cache[locale] = Faker("en_US")
    return _faker_cache[locale]


# ── Name generators ─────────────────────────────────────────────────────────

def _gen_surname(country: str, rng: np.random.Generator) -> str:
    pool = _SURNAMES.get(country)
    if pool:
        return str(rng.choice(pool))
    locale = FAKER_LOCALES.get(country, "en_US")
    return _get_faker(locale).last_name()


def _gen_first_name(country: str, sex: str,
                    rng: np.random.Generator) -> str:
    pool = (_FIRST_MALE if sex == "Male" else _FIRST_FEMALE).get(country)
    if pool:
        return str(rng.choice(pool))
    locale = FAKER_LOCALES.get(country, "en_US")
    f = _get_faker(locale)
    return f.first_name_male() if sex == "Male" else f.first_name_female()


def _gen_middle_name(country: str, rng: np.random.Generator) -> str | None:
    if rng.random() < 0.4:
        return None
    pool = _FIRST_MALE.get(country) or _FIRST_FEMALE.get(country)
    if pool:
        return str(rng.choice(pool))
    locale = FAKER_LOCALES.get(country, "en_US")
    return _get_faker(locale).first_name()


# ── Format generators ──────────────────────────────────────────────────────

def _gen_a_number(rng: np.random.Generator) -> str:
    return "A" + str(rng.integers(100_000_000, 999_999_999))


def _gen_receipt_number(rng: np.random.Generator) -> str:
    prefix = str(pick(rng, RECEIPT_PREFIXES, 1, RECEIPT_PREFIX_WEIGHTS)[0])
    digits = rng.integers(1_000_000_000, 9_999_999_999)
    return f"{prefix}{digits}"


def _gen_ssn(rng: np.random.Generator) -> str:
    """Valid-format SSN (no 000/666 area, no 00 group, no 0000 serial)."""
    area = rng.integers(1, 900)
    while area == 666:
        area = rng.integers(1, 900)
    group = rng.integers(1, 100)
    serial = rng.integers(1, 10000)
    return f"{area:03d}-{group:02d}-{serial:04d}"


def _gen_passport(country: str, rng: np.random.Generator) -> str:
    length = rng.integers(7, 10)
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(str(rng.choice(list(chars))) for _ in range(length))


def _gen_i94(rng: np.random.Generator) -> str:
    return str(rng.integers(10_000_000_000, 99_999_999_999))


# ── Main profile builder ──────────────────────────────────────────────────

def build_profiles(
    n: int,
    start_id: int = 1,
    seed: int = SEED,
    categories: np.ndarray | None = None,
) -> pd.DataFrame:
    """Build *n* coherent I-485 applicant profiles.

    Parameters
    ----------
    n : int
        Number of profiles to generate.
    start_id : int
        First application_id (inclusive).
    seed : int
        RNG seed for reproducibility.
    categories : array-like, optional
        Pre-selected category codes (length *n*).  If ``None``, drawn
        from the weighted distribution.

    Returns
    -------
    pd.DataFrame
        One row per profile, every field the table generators need.
    """
    rng = np.random.default_rng(seed)
    fake_us = _get_faker("en_US")

    ids = np.arange(start_id, start_id + n)

    # ── Filing category ─────────────────────────────────────────────────
    if categories is None:
        cats = pick(rng, CATEGORY_CODES, n, CATEGORY_WEIGHTS)
    else:
        cats = np.asarray(categories)
    groups = np.array([CATEGORY_GROUPS[c] for c in cats])

    # ── Demographics ────────────────────────────────────────────────────
    sex       = pick(rng, SEX_OPTIONS, n, SEX_WEIGHTS)
    countries = pick(rng, COUNTRY_CODES, n, COUNTRY_WEIGHTS)
    ethnicity = pick(rng, ETHNICITY_OPTIONS, n, ETHNICITY_WEIGHTS)
    race      = pick(rng, RACE_OPTIONS, n, RACE_WEIGHTS)
    eye_color = pick(rng, EYE_COLOR_OPTIONS, n, EYE_COLOR_WEIGHTS)
    hair_color = pick(rng, HAIR_COLOR_OPTIONS, n, HAIR_COLOR_WEIGHTS)

    # ── Status ──────────────────────────────────────────────────────────
    status = pick(rng, STATUS_OPTIONS, n, STATUS_WEIGHTS)

    # ── Dates ───────────────────────────────────────────────────────────
    filing_dates = gen_dates(rng, n, FILING_DATE_START, FILING_DATE_END)
    filing_dates_dt = pd.to_datetime(filing_dates)

    # Age → DOB
    bracket_idx = pick(rng, list(range(len(AGE_BRACKETS))), n,
                       AGE_BRACKET_WEIGHTS).astype(int)
    ages = np.array([
        rng.integers(AGE_BRACKETS[i][0], AGE_BRACKETS[i][1] + 1)
        for i in bracket_idx
    ])
    dob = filing_dates_dt - pd.to_timedelta(ages * 365.25, unit="D")

    # Arrival date: between 1990 and filing_date
    arrival_offsets = rng.integers(0, (ages * 365).clip(min=365), size=n)
    arrival_dates = pd.Series(
        filing_dates_dt - pd.to_timedelta(arrival_offsets, unit="D"))
    lower_bound = pd.Timestamp(ARRIVAL_DATE_START)
    arrival_dates = arrival_dates.clip(lower=lower_bound)
    # Ensure arrival is before filing
    filing_series = pd.Series(filing_dates_dt)
    too_late = arrival_dates >= filing_series
    arrival_dates[too_late] = filing_series[too_late] - pd.Timedelta(days=1)

    # Passport expiration: filing_date + 1-10 years
    passport_offsets = rng.integers(365, 3650, size=n)
    passport_exp = filing_dates_dt + pd.to_timedelta(passport_offsets, unit="D")

    # ── Marital status (with consistency) ───────────────────────────────
    marital = pick(rng, MARITAL_OPTIONS, n, MARITAL_WEIGHTS)
    # IR_SPOUSE filers must be married
    marital[cats == "FAM_IR_SPOUSE"] = "Married"
    # WIDOW filers must be widowed
    marital[cats == "FAM_IR_WIDOW"] = "Widowed"

    # ── Children count ──────────────────────────────────────────────────
    num_children = pick(rng, CHILDREN_COUNTS, n, CHILDREN_WEIGHTS).astype(int)

    # ── US address ──────────────────────────────────────────────────────
    us_states = pick(rng, STATE_CODES, n, STATE_WEIGHTS)

    # ── Height / weight (correlated by sex) ─────────────────────────────
    height_inches_raw = np.where(
        sex == "Male",
        rng.normal(69, 3, n),   # men avg 5'9"
        rng.normal(64, 2.5, n), # women avg 5'4"
    ).clip(55, 84).astype(int)
    height_feet   = height_inches_raw // 12
    height_inches = height_inches_raw % 12

    weight_base = np.where(sex == "Male", 180, 150).astype(float)
    weight = (weight_base + rng.normal(0, 25, n)).clip(90, 350).astype(int)

    # ── Section of law ──────────────────────────────────────────────────
    section_of_law = np.array([SECTION_OF_LAW_BY_GROUP[g] for g in groups])
    # 4 % override to INA 245(i)
    mask_245i = rng.random(n) < 0.04
    section_of_law[mask_245i] = "INA 245(i)"

    # ── Immigration details ─────────────────────────────────────────────
    has_a_number = rng.random(n) < 0.85
    has_ssn      = rng.random(n) < 0.70
    has_attorney = rng.random(n) < 0.40
    has_i94      = rng.random(n) < 0.85

    arrival_type = pick(rng, ARRIVAL_TYPES, n, ARRIVAL_TYPE_WEIGHTS)
    imm_status   = pick(rng, IMMIGRATION_STATUS_OPTIONS, n)

    # Citizenship usually matches birth country (85 %)
    citizenship = countries.copy()
    mismatch = rng.random(n) < 0.15
    citizenship[mismatch] = pick(rng, COUNTRY_CODES, int(mismatch.sum()),
                                 COUNTRY_WEIGHTS)

    # ── Applicant type ──────────────────────────────────────────────────
    applicant_type = np.where(rng.random(n) < 0.85, "PRINCIPAL", "DERIVATIVE")

    # ── Per-record fields (names, identifiers, addresses) ───────────────
    # These require iteration but are fast enough for 200K
    family_names   = []
    given_names    = []
    middle_names   = []
    a_numbers      = []
    receipt_numbers = []
    ssns           = []
    passport_nums  = []
    i94_numbers    = []
    us_streets     = []
    us_cities      = []
    us_zips        = []
    arrival_cities = []
    arrival_states_list = []
    p1_family = []; p1_given = []; p1_country = []
    p2_family = []; p2_given = []; p2_country = []
    sp_family = []; sp_given = []; sp_country = []
    employer_names  = []
    occupations     = []
    annual_incomes  = []
    atty_names      = []
    atty_bar_numbers = []
    city_of_birth_list = []

    _occ_eb = ["Software Engineer", "Data Scientist", "Physician",
               "Research Scientist", "Professor", "Financial Analyst",
               "Mechanical Engineer", "Civil Engineer", "Architect",
               "Management Consultant", "Electrical Engineer",
               "Biomedical Researcher", "Pharmacist", "Actuary"]
    _occ_general = ["Cashier", "Cook", "Housekeeper", "Landscaper",
                    "Driver", "Retail Associate", "Warehouse Worker",
                    "Mechanic", "Plumber", "Electrician", "Carpenter",
                    "Painter", "Teacher Aide", "Security Guard",
                    "Caregiver", "Office Clerk", "Restaurant Manager"]

    _income_eb  = ["$50,000-$74,999", "$75,000-$99,999",
                   "$100,000-$149,999", "$150,000+"]
    _income_gen = ["$0-$14,999", "$15,000-$29,999", "$30,000-$49,999",
                   "$50,000-$74,999"]

    for i in range(n):
        c   = str(countries[i])
        s   = str(sex[i])
        g   = str(groups[i])

        # Names
        family_names.append(_gen_surname(c, rng))
        given_names.append(_gen_first_name(c, s, rng))
        middle_names.append(_gen_middle_name(c, rng))

        # Identifiers
        a_numbers.append(_gen_a_number(rng) if has_a_number[i] else None)
        receipt_numbers.append(_gen_receipt_number(rng))
        ssns.append(_gen_ssn(rng) if has_ssn[i] else None)
        passport_nums.append(_gen_passport(c, rng))
        i94_numbers.append(_gen_i94(rng) if has_i94[i] else None)

        # US address
        us_streets.append(fake_us.street_address())
        us_cities.append(fake_us.city())
        us_zips.append(fake_us.zipcode())

        # Arrival port
        arrival_cities.append(fake_us.city())
        arrival_states_list.append(str(rng.choice(["CA", "NY", "TX", "FL",
                                                    "IL", "NJ", "HI"])))

        # City of birth
        locale = FAKER_LOCALES.get(c, "en_US")
        if c not in NON_LATIN_COUNTRIES:
            city_of_birth_list.append(_get_faker(locale).city())
        else:
            city_of_birth_list.append(fake_us.city())

        # Parents
        p1_family.append(_gen_surname(c, rng))
        p1_given.append(_gen_first_name(c, "Male", rng))
        p1_country.append(c)
        p2_family.append(_gen_surname(c, rng))
        p2_given.append(_gen_first_name(c, "Female", rng))
        # Mother's country matches applicant 90 %
        p2_country.append(c if rng.random() < 0.90
                          else str(rng.choice(COUNTRY_CODES)))

        # Spouse (only if married / divorced / widowed / separated)
        if str(marital[i]) in ("Married", "Divorced", "Widowed",
                               "Legally Separated"):
            # 70 % same country
            sp_c = c if rng.random() < 0.70 else str(rng.choice(COUNTRY_CODES))
            sp_sex = "Female" if s == "Male" else "Male"
            sp_family.append(_gen_surname(sp_c, rng))
            sp_given.append(_gen_first_name(sp_c, sp_sex, rng))
            sp_country.append(sp_c)
        else:
            sp_family.append(None)
            sp_given.append(None)
            sp_country.append(None)

        # Employment
        if g == "EMPLOYMENT":
            occupations.append(str(rng.choice(_occ_eb)))
            annual_incomes.append(str(rng.choice(_income_eb)))
        else:
            occupations.append(str(rng.choice(_occ_general)))
            annual_incomes.append(str(rng.choice(_income_gen)))
        employer_names.append(fake_us.company())

        # Attorney
        if has_attorney[i]:
            atty_names.append(fake_us.name())
            atty_bar_numbers.append(
                str(rng.choice(STATE_CODES))
                + str(rng.integers(100000, 999999))
            )
        else:
            atty_names.append(None)
            atty_bar_numbers.append(None)

    # ── Parent DOBs (20-45 yrs before applicant) ───────────────────────
    dob_series = pd.Series(dob)
    parent_age_offset = rng.integers(20, 46, size=n)
    p1_dob = dob_series - pd.to_timedelta(
        parent_age_offset * 365.25, unit="D")
    p2_dob = dob_series - pd.to_timedelta(
        rng.integers(20, 46, size=n) * 365.25, unit="D"
    )

    # Spouse DOB: applicant age ± 5 years
    sp_age_delta = rng.integers(-5, 6, size=n)
    sp_dob_raw = pd.Series(dob) + pd.to_timedelta(
        sp_age_delta * 365.25, unit="D")
    sp_dob = pd.Series([
        sp_dob_raw.iloc[i] if sp_family[i] is not None else pd.NaT
        for i in range(n)
    ])

    # ── Assemble DataFrame ──────────────────────────────────────────────
    profiles = pd.DataFrame({
        "application_id":        ids,
        "a_number":              a_numbers,
        "receipt_number":        receipt_numbers,
        "filing_date":           filing_dates_dt,
        "category_code":         cats,
        "category_group":        groups,
        "status":                status,
        "section_of_law":        section_of_law,
        "form_edition":          "01/20/25",
        "applicant_type":        applicant_type,

        "family_name":           family_names,
        "given_name":            given_names,
        "middle_name":           middle_names,
        "sex":                   sex,
        "date_of_birth":         dob,
        "city_of_birth":         city_of_birth_list,
        "country_of_birth":      countries,
        "country_of_citizenship": citizenship,
        "ethnicity":             ethnicity,
        "race":                  race,
        "height_feet":           height_feet,
        "height_inches":         height_inches,
        "weight_pounds":         weight,
        "eye_color":             eye_color,
        "hair_color":            hair_color,
        "marital_status":        marital,
        "num_children":          num_children,

        "has_a_number":          has_a_number,
        "ssn":                   ssns,
        "has_ssn":               has_ssn,
        "passport_number":       passport_nums,
        "passport_country":      countries,  # same as birth for clean
        "passport_expiration":   passport_exp,
        "i94_number":            i94_numbers,
        "has_i94":               has_i94,
        "arrival_date":          arrival_dates,
        "arrival_city":          arrival_cities,
        "arrival_state":         arrival_states_list,
        "arrival_type":          arrival_type,
        "current_immigration_status": imm_status,

        "us_street":             us_streets,
        "us_city":               us_cities,
        "us_state":              us_states,
        "us_zip":                us_zips,

        "parent1_family_name":   p1_family,
        "parent1_given_name":    p1_given,
        "parent1_country":       p1_country,
        "parent1_dob":           p1_dob,
        "parent2_family_name":   p2_family,
        "parent2_given_name":    p2_given,
        "parent2_country":       p2_country,
        "parent2_dob":           p2_dob,

        "spouse_family_name":    sp_family,
        "spouse_given_name":     sp_given,
        "spouse_country":        sp_country,
        "spouse_dob":            sp_dob,

        "employer_name":         employer_names,
        "occupation":            occupations,
        "annual_income":         annual_incomes,

        "has_attorney":          has_attorney,
        "atty_name":             atty_names,
        "atty_bar_number":       atty_bar_numbers,
    })

    return profiles
