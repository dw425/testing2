-- =============================================================================
-- FinancialServicesIQ — Unity Catalog DDL
-- Catalog: financial_services_iq
-- Schemas: bronze (raw ingestion), silver (enriched/scored), gold (aggregated)
-- =============================================================================

-- ----------------------------- Catalog & Schemas ----------------------------
CREATE CATALOG IF NOT EXISTS financial_services_iq
  COMMENT 'FinancialServicesIQ: Risk & Portfolio Intelligence lakehouse';

USE CATALOG financial_services_iq;

CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw ingestion layer – append-only landing tables';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Enriched / scored layer – cleaned, feature-engineered tables';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Aggregated layer – dashboard-ready KPIs and analytics';


-- ============================= BRONZE TABLES ================================

-- Raw financial transactions arriving from payment rails
CREATE TABLE IF NOT EXISTS bronze.raw_transactions (
  transaction_id    STRING        NOT NULL  COMMENT 'Unique transaction identifier (UUID)',
  timestamp         TIMESTAMP     NOT NULL  COMMENT 'Transaction timestamp in UTC',
  account_id        STRING        NOT NULL  COMMENT 'Source account identifier',
  customer_id       STRING        NOT NULL  COMMENT 'Customer identifier',
  channel           STRING        NOT NULL  COMMENT 'Transaction channel (Mobile App, Web Banking, ATM, Branch, Wire Transfer, ACH)',
  merchant_category STRING        NOT NULL  COMMENT 'Merchant Category Code description',
  amount            DOUBLE        NOT NULL  COMMENT 'Transaction amount in local currency',
  currency          STRING        NOT NULL  COMMENT 'ISO 4217 currency code',
  location_lat      DOUBLE                  COMMENT 'Transaction latitude',
  location_lon      DOUBLE                  COMMENT 'Transaction longitude',
  device_risk_score DOUBLE                  COMMENT 'Device fingerprint risk score (0.0-1.0)',
  ip_address        STRING                  COMMENT 'Originating IP address',
  _ingested_at      TIMESTAMP     NOT NULL  COMMENT 'Ingestion timestamp (pipeline metadata)'
)
USING DELTA
COMMENT 'Raw financial transactions – 12.5 M rows/day target'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality'                          = 'bronze'
);

-- Nightly account snapshots from core banking
CREATE TABLE IF NOT EXISTS bronze.raw_accounts (
  account_id          STRING    NOT NULL  COMMENT 'Account identifier',
  snapshot_date       DATE      NOT NULL  COMMENT 'Snapshot date',
  customer_id         STRING    NOT NULL  COMMENT 'Customer identifier',
  business_line       STRING    NOT NULL  COMMENT 'Business line (Retail Banking, Commercial Lending, Wealth Management, Insurance)',
  account_type        STRING    NOT NULL  COMMENT 'Account type (checking, savings, credit_card, mortgage, auto_loan, personal_loan)',
  credit_score        INT                 COMMENT 'FICO credit score',
  credit_limit        DOUBLE              COMMENT 'Credit limit in USD',
  balance             DOUBLE              COMMENT 'Current balance in USD',
  utilization_ratio   DOUBLE              COMMENT 'Credit utilization ratio (0.0-1.0)',
  dti_ratio           DOUBLE              COMMENT 'Debt-to-income ratio',
  employment_years    INT                 COMMENT 'Years of current employment',
  payment_history_12m STRING              COMMENT 'Payment history codes for past 12 months (e.g. CCCCCCCCCCCC)',
  _ingested_at        TIMESTAMP NOT NULL  COMMENT 'Ingestion timestamp (pipeline metadata)'
)
USING DELTA
COMMENT 'Nightly account snapshot – 100 K accounts'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality'                          = 'bronze'
);

-- Intraday market / portfolio data feed
CREATE TABLE IF NOT EXISTS bronze.raw_market_data (
  record_id       STRING    NOT NULL  COMMENT 'Unique record identifier (UUID)',
  timestamp       TIMESTAMP NOT NULL  COMMENT 'Market data timestamp in UTC',
  asset_class     STRING    NOT NULL  COMMENT 'Asset class (Equities, Fixed Income, Commodities, FX, Derivatives)',
  ticker          STRING    NOT NULL  COMMENT 'Instrument ticker symbol',
  price           DOUBLE    NOT NULL  COMMENT 'Last traded price',
  daily_return    DOUBLE              COMMENT 'Daily return as decimal',
  volatility_30d  DOUBLE              COMMENT '30-day rolling volatility',
  volume          INT                 COMMENT 'Daily traded volume',
  region          STRING              COMMENT 'Geographic region (Northeast, Southeast, Midwest, West Coast, International)',
  _ingested_at    TIMESTAMP NOT NULL  COMMENT 'Ingestion timestamp (pipeline metadata)'
)
USING DELTA
COMMENT 'Intraday market and portfolio data – 8,450 active positions'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality'                          = 'bronze'
);


-- ============================= SILVER TABLES ================================

-- Transactions enriched with fraud features and model scores
CREATE TABLE IF NOT EXISTS silver.fraud_scores (
  transaction_id      STRING    NOT NULL  COMMENT 'Transaction identifier (FK to bronze.raw_transactions)',
  timestamp           TIMESTAMP NOT NULL  COMMENT 'Transaction timestamp in UTC',
  account_id          STRING    NOT NULL  COMMENT 'Source account identifier',
  customer_id         STRING    NOT NULL  COMMENT 'Customer identifier',
  channel             STRING    NOT NULL  COMMENT 'Transaction channel',
  merchant_category   STRING    NOT NULL  COMMENT 'Merchant category',
  amount              DOUBLE    NOT NULL  COMMENT 'Transaction amount',
  velocity_1h         INT                 COMMENT 'Number of transactions by this customer in the past 1 hour',
  distance_from_home  DOUBLE              COMMENT 'Distance from customer home address (km)',
  time_since_last_txn INT                 COMMENT 'Seconds since previous transaction by this customer',
  fraud_score         DOUBLE    NOT NULL  COMMENT 'ML fraud probability score (0.0-1.0)',
  is_fraud            BOOLEAN   NOT NULL  COMMENT 'True if classified as fraud',
  fraud_type          STRING              COMMENT 'Fraud type classification (card_not_present, counterfeit, account_takeover, first_party, synthetic_identity, null)',
  blocked             BOOLEAN   NOT NULL  COMMENT 'True if transaction was blocked in real time'
)
USING DELTA
COMMENT 'Fraud-scored transactions with engineered features'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality'                          = 'silver'
);

-- Credit risk profile per account
CREATE TABLE IF NOT EXISTS silver.credit_risk_profiles (
  account_id          STRING  NOT NULL  COMMENT 'Account identifier (FK to bronze.raw_accounts)',
  customer_id         STRING  NOT NULL  COMMENT 'Customer identifier',
  business_line       STRING  NOT NULL  COMMENT 'Business line',
  credit_score        INT               COMMENT 'FICO credit score',
  dti_ratio           DOUBLE            COMMENT 'Debt-to-income ratio',
  utilization_ratio   DOUBLE            COMMENT 'Credit utilization ratio',
  payment_history_12m STRING            COMMENT 'Payment history codes',
  delinquency_flag    BOOLEAN NOT NULL  COMMENT 'True if any delinquency in past 12 months',
  default_probability DOUBLE  NOT NULL  COMMENT 'Predicted probability of default (0.0-1.0)',
  risk_tier           STRING  NOT NULL  COMMENT 'Risk tier (Prime, Near-Prime, Subprime, Deep-Subprime)',
  expected_loss       DOUBLE  NOT NULL  COMMENT 'Expected loss in USD'
)
USING DELTA
COMMENT 'Credit risk profiles – one row per account'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality'                          = 'silver'
);

-- Portfolio positions with risk analytics
CREATE TABLE IF NOT EXISTS silver.portfolio_positions (
  position_id         STRING    NOT NULL  COMMENT 'Unique position identifier',
  timestamp           TIMESTAMP NOT NULL  COMMENT 'Valuation timestamp',
  asset_class         STRING    NOT NULL  COMMENT 'Asset class',
  ticker              STRING    NOT NULL  COMMENT 'Instrument ticker',
  region              STRING              COMMENT 'Geographic region',
  market_value        DOUBLE    NOT NULL  COMMENT 'Current market value in USD',
  daily_pnl           DOUBLE              COMMENT 'Daily profit & loss in USD',
  var_contribution    DOUBLE              COMMENT 'Contribution to portfolio VaR (USD)',
  beta                DOUBLE              COMMENT 'Position beta relative to benchmark',
  sharpe_contribution DOUBLE              COMMENT 'Contribution to portfolio Sharpe ratio'
)
USING DELTA
COMMENT 'Portfolio positions with risk decomposition – 8,450 active'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality'                          = 'silver'
);


-- ============================== GOLD TABLES =================================

-- Real-time risk dashboard KPIs (refreshed every 5 minutes)
CREATE TABLE IF NOT EXISTS gold.risk_dashboard_kpis (
  snapshot_time       TIMESTAMP NOT NULL  COMMENT 'KPI snapshot timestamp',
  transactions_today  INT       NOT NULL  COMMENT 'Total transactions processed today',
  fraud_blocked       INT       NOT NULL  COMMENT 'Number of fraudulent transactions blocked today',
  false_positive_rate DOUBLE    NOT NULL  COMMENT 'Fraud model false positive rate',
  portfolio_var_95    DOUBLE    NOT NULL  COMMENT 'Portfolio 95% VaR in USD',
  total_aum           DOUBLE    NOT NULL  COMMENT 'Total assets under management in USD',
  high_risk_accounts  INT       NOT NULL  COMMENT 'Count of accounts in Subprime or Deep-Subprime tiers'
)
USING DELTA
COMMENT 'Risk overview dashboard KPIs – refreshed every 5 min'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality'                          = 'gold'
);

-- Fraud analytics by channel and merchant category
CREATE TABLE IF NOT EXISTS gold.fraud_analytics (
  channel             STRING  NOT NULL  COMMENT 'Transaction channel',
  merchant_category   STRING  NOT NULL  COMMENT 'Merchant category',
  transaction_count   INT     NOT NULL  COMMENT 'Total transactions',
  fraud_count         INT     NOT NULL  COMMENT 'Confirmed fraud transactions',
  fraud_rate          DOUBLE  NOT NULL  COMMENT 'Fraud rate (fraud_count / transaction_count)',
  avg_fraud_amount    DOUBLE  NOT NULL  COMMENT 'Average fraud transaction amount in USD',
  blocked_count       INT     NOT NULL  COMMENT 'Number of blocked transactions',
  last_updated        TIMESTAMP NOT NULL COMMENT 'Last refresh timestamp'
)
USING DELTA
COMMENT 'Fraud analytics aggregated by channel and merchant category'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality'                          = 'gold'
);

-- Credit portfolio summary by business line and region
CREATE TABLE IF NOT EXISTS gold.credit_portfolio (
  business_line         STRING  NOT NULL  COMMENT 'Business line',
  region                STRING  NOT NULL  COMMENT 'Geographic region',
  total_accounts        INT     NOT NULL  COMMENT 'Number of accounts',
  total_exposure        DOUBLE  NOT NULL  COMMENT 'Total credit exposure in USD',
  avg_credit_score      INT     NOT NULL  COMMENT 'Average FICO credit score',
  delinquency_rate_30d  DOUBLE  NOT NULL  COMMENT '30-day delinquency rate',
  expected_loss_rate    DOUBLE  NOT NULL  COMMENT 'Expected loss rate',
  high_risk_count       INT     NOT NULL  COMMENT 'Accounts in Subprime or Deep-Subprime tiers'
)
USING DELTA
COMMENT 'Credit book analysis by business line and region'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality'                          = 'gold'
);

-- Market and portfolio risk summary by asset class and region
CREATE TABLE IF NOT EXISTS gold.market_risk_summary (
  asset_class       STRING  NOT NULL  COMMENT 'Asset class',
  region            STRING  NOT NULL  COMMENT 'Geographic region',
  total_aum         DOUBLE  NOT NULL  COMMENT 'Total AUM in USD',
  var_95            DOUBLE  NOT NULL  COMMENT '95% Value at Risk in USD',
  sharpe_ratio      DOUBLE  NOT NULL  COMMENT 'Sharpe ratio',
  beta              DOUBLE  NOT NULL  COMMENT 'Portfolio beta',
  daily_pnl         DOUBLE  NOT NULL  COMMENT 'Aggregate daily P&L in USD',
  active_positions  INT     NOT NULL  COMMENT 'Number of active positions'
)
USING DELTA
COMMENT 'Portfolio risk summary by asset class and region'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality'                          = 'gold'
);
