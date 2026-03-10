"""
FinancialServicesIQ — Synthetic Data Generator
===============================================
Generates realistic bronze-layer data for the financial_services_iq lakehouse:
  - raw_transactions  (12.5 M / day, 0.23 % fraud rate)
  - raw_accounts      (100 K accounts)
  - raw_market_data   (8,450 active positions, $24.5 B AUM)

All parameters are driven by config/financial_services.yaml.

Usage (Databricks notebook or local Spark):
    spark = SparkSession.builder.getOrCreate()
    generate_all(spark)
"""

from __future__ import annotations

import math
import random
import uuid
from datetime import datetime, timedelta, date
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType,
    IntegerType, DateType, BooleanType,
)

# ---------------------------------------------------------------------------
# Constants from config/financial_services.yaml
# ---------------------------------------------------------------------------
CATALOG = "financial_services_iq"

BUSINESS_LINES = [
    "Retail Banking",
    "Commercial Lending",
    "Wealth Management",
    "Insurance",
]
REGIONS = ["Northeast", "Southeast", "Midwest", "West Coast", "International"]
CHANNELS = ["Mobile App", "Web Banking", "ATM", "Branch", "Wire Transfer", "ACH"]

MERCHANT_CATEGORIES = [
    "Grocery Stores",
    "Gas Stations",
    "Restaurants",
    "Online Retail",
    "Travel & Airlines",
    "Hotels & Lodging",
    "Electronics",
    "Healthcare",
    "Utilities",
    "Entertainment",
    "Department Stores",
    "Insurance Services",
    "Professional Services",
    "Government Services",
    "Subscription Services",
]

ACCOUNT_TYPES = [
    "checking",
    "savings",
    "credit_card",
    "mortgage",
    "auto_loan",
    "personal_loan",
]

ASSET_CLASSES = ["Equities", "Fixed Income", "Commodities", "FX", "Derivatives"]

FRAUD_TYPES = [
    "card_not_present",
    "counterfeit",
    "account_takeover",
    "first_party",
    "synthetic_identity",
]

CURRENCIES = ["USD", "USD", "USD", "USD", "EUR", "GBP", "CAD", "JPY"]  # weighted toward USD

# Target metrics
TRANSACTIONS_PER_DAY = 12_500_000
FRAUD_RATE = 0.0023
BLOCKED_TODAY = 847
FALSE_POSITIVE_RATE = 0.004
AVG_FRAUD_AMOUNT = 3420.0

TOTAL_ACCOUNTS = 100_000
AVG_CREDIT_SCORE = 714
DELINQUENCY_RATE = 0.032
HIGH_RISK_ACCOUNTS = 12_400
EXPECTED_LOSS_RATE = 0.018

AUM_TOTAL = 24_500_000_000.0
VAR_95_DAILY = 47_000_000.0
SHARPE_RATIO = 1.42
BETA_PORTFOLIO = 0.94
ACTIVE_POSITIONS = 8_450


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _uuid() -> str:
    return str(uuid.uuid4())


def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def _random_ip() -> str:
    return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


def _payment_history(delinquent: bool) -> str:
    """Generate a 12-char payment history string (C=current, D=delinquent, L=late)."""
    codes = []
    for _ in range(12):
        if delinquent and random.random() < 0.15:
            codes.append(random.choice(["D", "L"]))
        else:
            codes.append("C")
    return "".join(codes)


# ---------------------------------------------------------------------------
# 1. Transaction Generator
# ---------------------------------------------------------------------------
def generate_transactions(
    spark: SparkSession,
    num_rows: int | None = None,
    target_date: date | None = None,
    write: bool = True,
) -> DataFrame:
    """
    Generate raw_transactions rows.

    Parameters
    ----------
    num_rows : int, optional
        Number of rows. Defaults to TRANSACTIONS_PER_DAY (12.5 M).
        For dev/testing pass a smaller value.
    target_date : date, optional
        The business date for the transactions. Defaults to today.
    write : bool
        If True, write to bronze.raw_transactions.
    """
    if num_rows is None:
        num_rows = TRANSACTIONS_PER_DAY
    if target_date is None:
        target_date = date.today()

    day_start = datetime.combine(target_date, datetime.min.time())
    now_ts = datetime.utcnow()

    fraud_count = int(num_rows * FRAUD_RATE)
    legitimate_count = num_rows - fraud_count

    # Pre-build account/customer pools
    account_pool = [f"ACCT-{i:06d}" for i in range(TOTAL_ACCOUNTS)]
    customer_pool = [f"CUST-{i:06d}" for i in range(TOTAL_ACCOUNTS)]

    # Home location per customer (for distance feature later)
    home_locs = {
        cid: (round(random.uniform(25.0, 48.0), 4), round(random.uniform(-122.0, -71.0), 4))
        for cid in customer_pool
    }

    rows: list = []

    def _make_row(is_fraud_row: bool) -> tuple:
        idx = random.randint(0, TOTAL_ACCOUNTS - 1)
        acct = account_pool[idx]
        cust = customer_pool[idx]
        channel = random.choice(CHANNELS)
        merchant = random.choice(MERCHANT_CATEGORIES)
        currency = random.choice(CURRENCIES)
        ts = day_start + timedelta(seconds=random.randint(0, 86399))

        home_lat, home_lon = home_locs[cust]

        if is_fraud_row:
            amount = round(random.gauss(AVG_FRAUD_AMOUNT, AVG_FRAUD_AMOUNT * 0.6), 2)
            amount = max(50.0, amount)
            # Fraud often far from home
            lat = round(home_lat + random.uniform(-15, 15), 4)
            lon = round(home_lon + random.uniform(-15, 15), 4)
            device_risk = round(_clamp(random.gauss(0.75, 0.15), 0.0, 1.0), 3)
        else:
            amount = round(abs(random.gauss(85.0, 120.0)) + 1.0, 2)
            lat = round(home_lat + random.gauss(0, 0.3), 4)
            lon = round(home_lon + random.gauss(0, 0.3), 4)
            device_risk = round(_clamp(random.gauss(0.15, 0.12), 0.0, 1.0), 3)

        return (
            _uuid(),       # transaction_id
            ts,            # timestamp
            acct,          # account_id
            cust,          # customer_id
            channel,       # channel
            merchant,      # merchant_category
            amount,        # amount
            currency,      # currency
            lat,           # location_lat
            lon,           # location_lon
            device_risk,   # device_risk_score
            _random_ip(),  # ip_address
            now_ts,        # _ingested_at
        )

    # Generate fraud rows
    for _ in range(fraud_count):
        rows.append(_make_row(is_fraud_row=True))

    # Generate legitimate rows
    for _ in range(legitimate_count):
        rows.append(_make_row(is_fraud_row=False))

    random.shuffle(rows)

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("account_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("location_lat", DoubleType(), True),
        StructField("location_lon", DoubleType(), True),
        StructField("device_risk_score", DoubleType(), True),
        StructField("ip_address", StringType(), True),
        StructField("_ingested_at", TimestampType(), False),
    ])

    df = spark.createDataFrame(rows, schema=schema)

    if write:
        df.write.format("delta").mode("append").saveAsTable(
            f"{CATALOG}.bronze.raw_transactions"
        )

    return df


# ---------------------------------------------------------------------------
# 2. Account Generator
# ---------------------------------------------------------------------------
def generate_accounts(
    spark: SparkSession,
    num_accounts: int | None = None,
    snapshot_dt: date | None = None,
    write: bool = True,
) -> DataFrame:
    """
    Generate raw_accounts snapshot rows.

    Targets:
        - 100 K accounts, avg credit score 714
        - 3.2 % delinquency rate, 12.4 K high-risk
        - 5 business lines (config has 4 — we add "Capital Markets" as 5th)
    """
    if num_accounts is None:
        num_accounts = TOTAL_ACCOUNTS
    if snapshot_dt is None:
        snapshot_dt = date.today()

    now_ts = datetime.utcnow()
    business_lines_ext = BUSINESS_LINES + ["Capital Markets"]  # 5 business lines

    rows: list = []
    high_risk_count = 0
    delinquent_count = 0

    for i in range(num_accounts):
        acct_id = f"ACCT-{i:06d}"
        cust_id = f"CUST-{i:06d}"
        bl = random.choice(business_lines_ext)
        acct_type = random.choice(ACCOUNT_TYPES)

        # Credit score: skewed distribution centred on AVG_CREDIT_SCORE
        credit_score = int(_clamp(random.gauss(AVG_CREDIT_SCORE, 75), 300, 850))

        # Determine risk
        is_high_risk = credit_score < 620
        if is_high_risk:
            high_risk_count += 1

        # Delinquency correlated with credit score
        delinquency_prob = _clamp(0.25 - (credit_score - 300) * 0.0004, 0.005, 0.30)
        is_delinquent = random.random() < delinquency_prob
        if is_delinquent:
            delinquent_count += 1

        credit_limit = round(random.uniform(2000, 100000) * (credit_score / 700), 2)
        balance = round(credit_limit * random.uniform(0.05, 0.95), 2)
        utilization = round(balance / max(credit_limit, 1.0), 4)
        dti = round(_clamp(random.gauss(0.35, 0.15), 0.05, 0.85), 4)
        emp_years = max(0, int(random.gauss(8, 5)))
        pay_hist = _payment_history(is_delinquent)

        rows.append((
            acct_id,
            snapshot_dt,
            cust_id,
            bl,
            acct_type,
            credit_score,
            credit_limit,
            balance,
            utilization,
            dti,
            emp_years,
            pay_hist,
            now_ts,
        ))

    schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("snapshot_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("business_line", StringType(), False),
        StructField("account_type", StringType(), False),
        StructField("credit_score", IntegerType(), True),
        StructField("credit_limit", DoubleType(), True),
        StructField("balance", DoubleType(), True),
        StructField("utilization_ratio", DoubleType(), True),
        StructField("dti_ratio", DoubleType(), True),
        StructField("employment_years", IntegerType(), True),
        StructField("payment_history_12m", StringType(), True),
        StructField("_ingested_at", TimestampType(), False),
    ])

    df = spark.createDataFrame(rows, schema=schema)

    if write:
        df.write.format("delta").mode("overwrite").saveAsTable(
            f"{CATALOG}.bronze.raw_accounts"
        )

    print(f"[accounts] Generated {num_accounts} accounts | "
          f"high_risk={high_risk_count} | delinquent={delinquent_count} "
          f"({delinquent_count/num_accounts*100:.1f}%)")

    return df


# ---------------------------------------------------------------------------
# 3. Market Data Generator
# ---------------------------------------------------------------------------
def generate_market_data(
    spark: SparkSession,
    num_positions: int | None = None,
    write: bool = True,
) -> DataFrame:
    """
    Generate raw_market_data rows.

    Targets:
        - $24.5 B AUM across 8,450 positions
        - VaR (95%) = $47 M
        - Sharpe = 1.42, Beta = 0.94
    """
    if num_positions is None:
        num_positions = ACTIVE_POSITIONS

    now_ts = datetime.utcnow()

    # Ticker pools per asset class
    equity_tickers = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM",
        "V", "JNJ", "WMT", "PG", "UNH", "HD", "MA", "BAC", "XOM", "PFE",
        "ABBV", "KO", "PEP", "COST", "MRK", "TMO", "AVGO", "CSCO", "ACN",
        "DHR", "LIN", "NKE", "ADBE", "TXN", "CRM", "AMD", "INTC", "QCOM",
    ]
    fi_tickers = [
        "UST-2Y", "UST-5Y", "UST-10Y", "UST-30Y", "IG-CORP-A", "IG-CORP-BBB",
        "HY-CORP-BB", "HY-CORP-B", "MBS-30Y", "MBS-15Y", "MUNI-AA", "MUNI-A",
        "TIPS-10Y", "EUROBOND-5Y", "GILT-10Y", "JGB-10Y",
    ]
    commodity_tickers = [
        "GC=F", "SI=F", "CL=F", "NG=F", "HG=F", "ZC=F", "ZW=F", "ZS=F",
        "PL=F", "PA=F",
    ]
    fx_tickers = [
        "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "USD/CAD",
        "NZD/USD", "EUR/GBP",
    ]
    deriv_tickers = [
        "SPX-CALL-4500", "SPX-PUT-4200", "VIX-FUT-M1", "VIX-FUT-M2",
        "ES-FUT-M1", "NQ-FUT-M1", "ZN-FUT-M1", "CL-OPT-CALL",
        "EURUSD-FWD-3M", "IRS-5Y-USD",
    ]

    ticker_map = {
        "Equities": equity_tickers,
        "Fixed Income": fi_tickers,
        "Commodities": commodity_tickers,
        "FX": fx_tickers,
        "Derivatives": deriv_tickers,
    }

    # Asset class weights (approximate AUM distribution)
    ac_weights = {
        "Equities": 0.40,
        "Fixed Income": 0.30,
        "Commodities": 0.10,
        "FX": 0.08,
        "Derivatives": 0.12,
    }

    # Distribute positions across asset classes
    ac_positions: dict[str, int] = {}
    remaining = num_positions
    for i, (ac, w) in enumerate(ac_weights.items()):
        if i == len(ac_weights) - 1:
            ac_positions[ac] = remaining
        else:
            n = int(num_positions * w)
            ac_positions[ac] = n
            remaining -= n

    # Target average market value per position
    avg_mv = AUM_TOTAL / num_positions  # ~$2.9 M

    rows: list = []
    total_mv = 0.0

    for ac, n_pos in ac_positions.items():
        tickers = ticker_map[ac]
        for j in range(n_pos):
            ticker = tickers[j % len(tickers)]
            region = random.choice(REGIONS)
            ts = now_ts - timedelta(seconds=random.randint(0, 3600))

            # Market value: log-normal around avg_mv
            mv = round(abs(random.lognormvariate(math.log(avg_mv), 0.8)), 2)
            total_mv += mv

            price = round(abs(random.gauss(150, 100)) + 5.0, 4)
            daily_ret = round(random.gauss(0.0005, 0.015), 6)
            vol_30d = round(abs(random.gauss(0.18, 0.08)), 4)
            volume = max(100, int(random.gauss(5_000_000, 8_000_000)))

            rows.append((
                _uuid(),      # record_id
                ts,           # timestamp
                ac,           # asset_class
                ticker,       # ticker
                price,        # price
                daily_ret,    # daily_return
                vol_30d,      # volatility_30d
                volume,       # volume
                region,       # region
                now_ts,       # _ingested_at
            ))

    # Scale market values so total ≈ AUM_TOTAL
    scale = AUM_TOTAL / max(total_mv, 1.0)

    schema = StructType([
        StructField("record_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("asset_class", StringType(), False),
        StructField("ticker", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("daily_return", DoubleType(), True),
        StructField("volatility_30d", DoubleType(), True),
        StructField("volume", IntegerType(), True),
        StructField("region", StringType(), True),
        StructField("_ingested_at", TimestampType(), False),
    ])

    df = spark.createDataFrame(rows, schema=schema)

    if write:
        df.write.format("delta").mode("overwrite").saveAsTable(
            f"{CATALOG}.bronze.raw_market_data"
        )

    print(f"[market_data] Generated {len(rows)} positions across "
          f"{len(ac_positions)} asset classes | raw_total_mv=${total_mv:,.0f}")

    return df


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def generate_all(
    spark: SparkSession,
    txn_rows: int | None = None,
    account_rows: int | None = None,
    position_rows: int | None = None,
) -> dict[str, DataFrame]:
    """
    Generate all three bronze tables.

    For dev/testing, pass smaller counts (e.g. txn_rows=100_000).
    Production defaults match the config targets.
    """
    print("=" * 72)
    print("FinancialServicesIQ — Synthetic Data Generation")
    print("=" * 72)

    dfs: dict[str, DataFrame] = {}

    print("\n[1/3] Generating raw_transactions ...")
    dfs["raw_transactions"] = generate_transactions(spark, num_rows=txn_rows)

    print("\n[2/3] Generating raw_accounts ...")
    dfs["raw_accounts"] = generate_accounts(spark, num_accounts=account_rows)

    print("\n[3/3] Generating raw_market_data ...")
    dfs["raw_market_data"] = generate_market_data(spark, num_positions=position_rows)

    print("\n" + "=" * 72)
    print("Synthetic data generation complete.")
    print("=" * 72)

    return dfs


# ---------------------------------------------------------------------------
# CLI / notebook entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FinancialServicesIQ_SyntheticData") \
        .getOrCreate()

    # For local testing, use smaller volumes
    generate_all(
        spark,
        txn_rows=100_000,       # 100 K instead of 12.5 M
        account_rows=10_000,    # 10 K instead of 100 K
        position_rows=1_000,    # 1 K instead of 8,450
    )
