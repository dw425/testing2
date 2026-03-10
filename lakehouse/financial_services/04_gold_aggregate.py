"""
FinancialServicesIQ — Gold Layer Aggregations
===============================================
Aggregates silver tables into dashboard-ready gold tables:

  silver.fraud_scores          ->  gold.risk_dashboard_kpis  (partial)
  silver.fraud_scores          ->  gold.fraud_analytics
  silver.credit_risk_profiles  ->  gold.risk_dashboard_kpis  (partial)
  silver.credit_risk_profiles  ->  gold.credit_portfolio
  silver.portfolio_positions   ->  gold.risk_dashboard_kpis  (partial)
  silver.portfolio_positions   ->  gold.market_risk_summary

Usage (Databricks notebook):
    spark = SparkSession.builder.getOrCreate()
    run_all_gold(spark)
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "financial_services_iq"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"


# ===================================================================
# 1. risk_dashboard_kpis
# ===================================================================
def aggregate_risk_dashboard_kpis(spark: SparkSession, write: bool = True) -> DataFrame:
    """
    Build the top-level risk dashboard KPI row by joining metrics from
    all three silver tables.

    Output columns
    --------------
    snapshot_time, transactions_today, fraud_blocked, false_positive_rate,
    portfolio_var_95, total_aum, high_risk_accounts
    """

    snapshot_time = datetime.utcnow()

    # --- Fraud metrics from silver.fraud_scores ---
    fraud = spark.read.table(f"{SILVER}.fraud_scores")

    fraud_kpis = fraud.agg(
        F.count("*").alias("transactions_today"),
        F.sum(F.col("blocked").cast(IntegerType())).alias("fraud_blocked"),
        # False positive rate: blocked & not actually fraud / total blocked
        # Approximation: (blocked - fraud_and_blocked) / blocked
        F.sum(
            F.when(F.col("blocked") & ~F.col("is_fraud"), 1).otherwise(0)
        ).alias("_false_positives"),
        F.sum(F.col("blocked").cast(IntegerType())).alias("_total_blocked"),
    ).collect()[0]

    transactions_today = fraud_kpis["transactions_today"] or 0
    fraud_blocked = fraud_kpis["fraud_blocked"] or 0
    false_positives = fraud_kpis["_false_positives"] or 0
    total_blocked = fraud_kpis["_total_blocked"] or 1
    false_positive_rate = round(false_positives / max(total_blocked, 1), 4)

    # --- Portfolio metrics from silver.portfolio_positions ---
    portfolio = spark.read.table(f"{SILVER}.portfolio_positions")

    port_kpis = portfolio.agg(
        F.sum("var_contribution").alias("portfolio_var_95"),
        F.sum("market_value").alias("total_aum"),
    ).collect()[0]

    portfolio_var_95 = port_kpis["portfolio_var_95"] or 0.0
    total_aum = port_kpis["total_aum"] or 0.0

    # --- Credit metrics from silver.credit_risk_profiles ---
    credit = spark.read.table(f"{SILVER}.credit_risk_profiles")

    high_risk_accounts = credit.filter(
        F.col("risk_tier").isin("Subprime", "Deep-Subprime")
    ).count()

    # --- Assemble KPI row ---
    kpi_row = [(
        snapshot_time,
        int(transactions_today),
        int(fraud_blocked),
        float(false_positive_rate),
        float(portfolio_var_95),
        float(total_aum),
        int(high_risk_accounts),
    )]

    result = spark.createDataFrame(kpi_row, schema=[
        "snapshot_time",
        "transactions_today",
        "fraud_blocked",
        "false_positive_rate",
        "portfolio_var_95",
        "total_aum",
        "high_risk_accounts",
    ])

    if write:
        (
            result.write
            .format("delta")
            .mode("append")
            .saveAsTable(f"{GOLD}.risk_dashboard_kpis")
        )
        print(f"[gold] risk_dashboard_kpis appended -> {GOLD}.risk_dashboard_kpis")
        print(f"       txns={transactions_today:,} | blocked={fraud_blocked} | "
              f"FP_rate={false_positive_rate:.4f} | VaR=${portfolio_var_95:,.0f} | "
              f"AUM=${total_aum:,.0f} | high_risk={high_risk_accounts:,}")

    return result


# ===================================================================
# 2. fraud_analytics
# ===================================================================
def aggregate_fraud_analytics(spark: SparkSession, write: bool = True) -> DataFrame:
    """
    Aggregate fraud metrics by channel and merchant_category.

    Output columns
    --------------
    channel, merchant_category, transaction_count, fraud_count,
    fraud_rate, avg_fraud_amount, blocked_count, last_updated
    """

    fraud = spark.read.table(f"{SILVER}.fraud_scores")

    result = (
        fraud
        .groupBy("channel", "merchant_category")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum(F.col("is_fraud").cast(IntegerType())).alias("fraud_count"),
            F.round(
                F.sum(F.col("is_fraud").cast(IntegerType())) / F.count("*"),
                6
            ).alias("fraud_rate"),
            F.round(
                F.avg(F.when(F.col("is_fraud"), F.col("amount"))),
                2
            ).alias("avg_fraud_amount"),
            F.sum(F.col("blocked").cast(IntegerType())).alias("blocked_count"),
        )
        .withColumn("last_updated", F.current_timestamp())
    )

    # Fill nulls for categories with zero fraud
    result = result.fillna({
        "fraud_count": 0,
        "fraud_rate": 0.0,
        "avg_fraud_amount": 0.0,
        "blocked_count": 0,
    })

    if write:
        (
            result.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{GOLD}.fraud_analytics")
        )
        row_count = result.count()
        print(f"[gold] fraud_analytics written -> {GOLD}.fraud_analytics ({row_count} rows)")

    return result


# ===================================================================
# 3. credit_portfolio
# ===================================================================
def aggregate_credit_portfolio(spark: SparkSession, write: bool = True) -> DataFrame:
    """
    Aggregate credit book by business_line and region.

    Region is not directly in credit_risk_profiles, so we derive it
    from customer_id by joining to the account's business line context
    and assigning regions round-robin. In production this would join
    to a customer dimension table.

    Output columns
    --------------
    business_line, region, total_accounts, total_exposure,
    avg_credit_score, delinquency_rate_30d, expected_loss_rate,
    high_risk_count
    """

    REGIONS = ["Northeast", "Southeast", "Midwest", "West Coast", "International"]

    credit = spark.read.table(f"{SILVER}.credit_risk_profiles")

    # Derive a deterministic region from customer_id hash
    credit = credit.withColumn(
        "region",
        F.element_at(
            F.array(*[F.lit(r) for r in REGIONS]),
            (F.abs(F.hash(F.col("customer_id"))) % F.lit(len(REGIONS))) + F.lit(1)
        )
    )

    # Read balances from bronze for exposure calculation
    accounts_bronze = spark.read.table(f"{CATALOG}.bronze.raw_accounts").select(
        "account_id", "balance"
    )

    credit_with_bal = credit.join(accounts_bronze, on="account_id", how="left")

    result = (
        credit_with_bal
        .groupBy("business_line", "region")
        .agg(
            F.count("*").alias("total_accounts"),
            F.round(F.sum(F.coalesce(F.col("balance"), F.lit(0.0))), 2).alias("total_exposure"),
            F.round(F.avg("credit_score"), 0).cast(IntegerType()).alias("avg_credit_score"),
            F.round(
                F.sum(F.col("delinquency_flag").cast(IntegerType())) / F.count("*"),
                4
            ).alias("delinquency_rate_30d"),
            F.round(
                F.sum("expected_loss") / F.greatest(F.sum(F.coalesce(F.col("balance"), F.lit(0.0))), F.lit(1.0)),
                6
            ).alias("expected_loss_rate"),
            F.sum(
                F.when(F.col("risk_tier").isin("Subprime", "Deep-Subprime"), 1).otherwise(0)
            ).alias("high_risk_count"),
        )
    )

    if write:
        (
            result.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{GOLD}.credit_portfolio")
        )
        row_count = result.count()
        print(f"[gold] credit_portfolio written -> {GOLD}.credit_portfolio ({row_count} rows)")

    return result


# ===================================================================
# 4. market_risk_summary
# ===================================================================
def aggregate_market_risk_summary(spark: SparkSession, write: bool = True) -> DataFrame:
    """
    Aggregate portfolio risk by asset_class and region.

    Output columns
    --------------
    asset_class, region, total_aum, var_95, sharpe_ratio, beta,
    daily_pnl, active_positions
    """

    positions = spark.read.table(f"{SILVER}.portfolio_positions")

    result = (
        positions
        .groupBy("asset_class", "region")
        .agg(
            F.round(F.sum("market_value"), 2).alias("total_aum"),
            F.round(F.sum("var_contribution"), 2).alias("var_95"),
            F.round(F.avg("sharpe_contribution"), 4).alias("sharpe_ratio"),
            F.round(
                F.sum(F.col("beta") * F.col("market_value")) / F.greatest(F.sum("market_value"), F.lit(1.0)),
                4
            ).alias("beta"),
            F.round(F.sum("daily_pnl"), 2).alias("daily_pnl"),
            F.count("*").alias("active_positions"),
        )
    )

    if write:
        (
            result.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{GOLD}.market_risk_summary")
        )
        row_count = result.count()
        print(f"[gold] market_risk_summary written -> {GOLD}.market_risk_summary ({row_count} rows)")

    return result


# ===================================================================
# Orchestrator
# ===================================================================
def run_all_gold(spark: SparkSession) -> dict[str, DataFrame]:
    """Run all gold aggregations sequentially."""
    print("=" * 72)
    print("FinancialServicesIQ — Gold Layer Aggregations")
    print("=" * 72)

    results: dict[str, DataFrame] = {}

    print("\n[1/4] Building risk_dashboard_kpis ...")
    results["risk_dashboard_kpis"] = aggregate_risk_dashboard_kpis(spark)

    print("\n[2/4] Building fraud_analytics ...")
    results["fraud_analytics"] = aggregate_fraud_analytics(spark)

    print("\n[3/4] Building credit_portfolio ...")
    results["credit_portfolio"] = aggregate_credit_portfolio(spark)

    print("\n[4/4] Building market_risk_summary ...")
    results["market_risk_summary"] = aggregate_market_risk_summary(spark)

    print("\n" + "=" * 72)
    print("Gold aggregations complete.")
    print("=" * 72)

    return results


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FinancialServicesIQ_Gold") \
        .getOrCreate()
    run_all_gold(spark)
