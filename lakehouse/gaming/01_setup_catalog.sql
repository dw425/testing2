-- =============================================================================
-- GamingIQ — Unity Catalog DDL
-- Creates catalog, schemas (bronze / silver / gold), and all 10 Delta tables.
-- Idempotent: safe to re-run.
-- =============================================================================

-- -------------------------------------------------------------------------
-- Catalog
-- -------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS gaming_iq;
USE CATALOG gaming_iq;

-- -------------------------------------------------------------------------
-- Schemas
-- -------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw ingestion layer — append-only player events, transactions, and match data.';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Curated layer — enriched player profiles, economy metrics, and match quality.';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Aggregation layer — KPIs, retention cohorts, and dashboard-ready tables.';

-- =========================================================================
-- BRONZE TABLES
-- =========================================================================

-- 1. raw_player_events
CREATE TABLE IF NOT EXISTS bronze.raw_player_events (
  event_id              STRING        COMMENT 'Unique player event identifier',
  timestamp             TIMESTAMP     COMMENT 'Event timestamp',
  player_id             STRING        COMMENT 'Unique player identifier',
  game_title            STRING        COMMENT 'Game title (Stellar Conquest, Shadow Realms, Velocity Rush)',
  region                STRING        COMMENT 'Player region (NA-East, NA-West, EU-West, EU-North, APAC-SEA, APAC-JP)',
  event_type            STRING        COMMENT 'Event type (login, logout, level_up, achievement, death, purchase, social)',
  session_id            STRING        COMMENT 'Player session identifier',
  device_type           STRING        COMMENT 'Device type (PC, Console, Mobile)',
  session_duration_sec  INT           COMMENT 'Session duration in seconds',
  level                 INT           COMMENT 'Player level at time of event',
  _ingested_at          TIMESTAMP     DEFAULT current_timestamp()
                                      COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw player session and action events from game telemetry pipelines.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 2. raw_transactions
CREATE TABLE IF NOT EXISTS bronze.raw_transactions (
  transaction_id        STRING        COMMENT 'Unique transaction identifier',
  timestamp             TIMESTAMP     COMMENT 'Transaction timestamp',
  player_id             STRING        COMMENT 'Unique player identifier',
  game_title            STRING        COMMENT 'Game title',
  transaction_type      STRING        COMMENT 'Transaction type (purchase, trade, gift, refund)',
  item_name             STRING        COMMENT 'Name of the item transacted',
  item_rarity           STRING        COMMENT 'Item rarity tier (Common, Uncommon, Rare, Epic, Legendary)',
  amount_usd            DOUBLE        COMMENT 'Transaction amount in USD',
  currency_amount       INT           COMMENT 'In-game currency amount',
  _ingested_at          TIMESTAMP     DEFAULT current_timestamp()
                                      COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw in-game purchase and trade transactions.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 3. raw_match_events
CREATE TABLE IF NOT EXISTS bronze.raw_match_events (
  match_id              STRING        COMMENT 'Unique match identifier',
  timestamp             TIMESTAMP     COMMENT 'Match start timestamp',
  game_title            STRING        COMMENT 'Game title',
  region                STRING        COMMENT 'Server region',
  game_mode             STRING        COMMENT 'Game mode (Ranked, Casual, Tournament, Custom)',
  player_count          INT           COMMENT 'Number of players in the match',
  avg_skill_rating      DOUBLE        COMMENT 'Average skill rating of participants',
  skill_spread          DOUBLE        COMMENT 'Skill rating spread (std dev) among participants',
  queue_time_sec        DOUBLE        COMMENT 'Time spent in matchmaking queue in seconds',
  match_duration_sec    INT           COMMENT 'Match duration in seconds',
  reported_unfair       BOOLEAN       COMMENT 'Whether any player reported the match as unfair',
  _ingested_at          TIMESTAMP     DEFAULT current_timestamp()
                                      COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Raw matchmaking and game result events.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =========================================================================
-- SILVER TABLES
-- =========================================================================

-- 4. player_profiles
CREATE TABLE IF NOT EXISTS silver.player_profiles (
  player_id             STRING        COMMENT 'Unique player identifier',
  game_title            STRING        COMMENT 'Game title',
  region                STRING        COMMENT 'Player region',
  segment               STRING        COMMENT 'Player segment (Whale, Dolphin, Minnow, Free-to-Play)',
  days_since_last_login INT           COMMENT 'Days since the player last logged in',
  session_frequency_7d  DOUBLE        COMMENT 'Average sessions per day over the last 7 days',
  purchase_count_30d    INT           COMMENT 'Number of purchases in the last 30 days',
  friend_count          INT           COMMENT 'Number of friends on the player social graph',
  skill_rating          DOUBLE        COMMENT 'Current skill rating (Elo/MMR)',
  churn_risk_score      DOUBLE        COMMENT 'ML-predicted churn probability 0-1',
  churn_risk_level      STRING        COMMENT 'Churn risk bucket (Low, Medium, High, Critical)',
  ltv                   DOUBLE        COMMENT 'Predicted lifetime value in USD'
)
USING DELTA
COMMENT 'Enriched player profiles with engagement metrics and churn scoring.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 5. economy_metrics
CREATE TABLE IF NOT EXISTS silver.economy_metrics (
  timestamp             TIMESTAMP     COMMENT 'Aggregation window timestamp',
  game_title            STRING        COMMENT 'Game title',
  item_category         STRING        COMMENT 'Item category (Weapon, Armor, Consumable, Cosmetic, Currency)',
  transaction_volume    INT           COMMENT 'Number of transactions in the window',
  avg_price             DOUBLE        COMMENT 'Average transaction price in USD',
  price_change_pct      DOUBLE        COMMENT 'Price change percentage vs prior window',
  inflation_index       DOUBLE        COMMENT 'Cumulative inflation index (1.0 = baseline)',
  suspicious_flag       BOOLEAN       COMMENT 'True if anomalous transaction patterns detected'
)
USING DELTA
COMMENT 'In-game economy health metrics aggregated by item category.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 6. match_quality
CREATE TABLE IF NOT EXISTS silver.match_quality (
  match_id              STRING        COMMENT 'Unique match identifier',
  timestamp             TIMESTAMP     COMMENT 'Match start timestamp',
  game_title            STRING        COMMENT 'Game title',
  region                STRING        COMMENT 'Server region',
  game_mode             STRING        COMMENT 'Game mode',
  skill_spread          DOUBLE        COMMENT 'Skill rating spread among participants',
  queue_time_sec        DOUBLE        COMMENT 'Matchmaking queue time in seconds',
  fairness_score        DOUBLE        COMMENT 'Computed fairness score 0-1 (higher = fairer)',
  completion_rate       DOUBLE        COMMENT 'Fraction of players who completed the match',
  reported_unfair       BOOLEAN       COMMENT 'Whether any player reported the match as unfair'
)
USING DELTA
COMMENT 'Matchmaking quality metrics with computed fairness scores.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =========================================================================
-- GOLD TABLES
-- =========================================================================

-- 7. live_ops_kpis
CREATE TABLE IF NOT EXISTS gold.live_ops_kpis (
  snapshot_time         TIMESTAMP     COMMENT 'KPI snapshot timestamp',
  dau                   INT           COMMENT 'Daily active users',
  concurrent_peak       INT           COMMENT 'Peak concurrent players',
  events_per_second     INT           COMMENT 'Events ingested per second',
  avg_session_min       DOUBLE        COMMENT 'Average session duration in minutes',
  daily_revenue         DOUBLE        COMMENT 'Total daily revenue in USD',
  arpdau                DOUBLE        COMMENT 'Average revenue per daily active user'
)
USING DELTA
COMMENT 'Real-time live operations KPI snapshots for the ops dashboard.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 8. retention_cohorts
CREATE TABLE IF NOT EXISTS gold.retention_cohorts (
  cohort_date           DATE          COMMENT 'Cohort registration date',
  game_title            STRING        COMMENT 'Game title',
  d1_retention          DOUBLE        COMMENT 'Day-1 retention rate',
  d7_retention          DOUBLE        COMMENT 'Day-7 retention rate',
  d30_retention         DOUBLE        COMMENT 'Day-30 retention rate',
  cohort_size           INT           COMMENT 'Number of players in the cohort'
)
USING DELTA
COMMENT 'Player retention metrics by registration cohort.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 9. economy_health
CREATE TABLE IF NOT EXISTS gold.economy_health (
  game_title            STRING        COMMENT 'Game title',
  item_category         STRING        COMMENT 'Item category',
  inflation_index       DOUBLE        COMMENT 'Current inflation index',
  transaction_volume    INT           COMMENT 'Total transaction volume',
  suspicious_transactions INT         COMMENT 'Count of flagged suspicious transactions',
  health_status         STRING        COMMENT 'Economy health status (Healthy, Warning, Critical)',
  last_updated          TIMESTAMP     COMMENT 'Row last-updated timestamp'
)
USING DELTA
COMMENT 'Economy health dashboard showing inflation, volume, and fraud signals.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- 10. matchmaking_quality
CREATE TABLE IF NOT EXISTS gold.matchmaking_quality (
  game_title            STRING        COMMENT 'Game title',
  region                STRING        COMMENT 'Server region',
  game_mode             STRING        COMMENT 'Game mode',
  avg_queue_sec         DOUBLE        COMMENT 'Average matchmaking queue time in seconds',
  avg_fairness_score    DOUBLE        COMMENT 'Average match fairness score 0-1',
  unfair_report_rate    DOUBLE        COMMENT 'Fraction of matches reported as unfair',
  matches_24h           INT           COMMENT 'Total matches in the last 24 hours'
)
USING DELTA
COMMENT 'Matchmaking quality KPIs aggregated by game, region, and mode.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =========================================================================
-- Done.  All 10 tables created across bronze / silver / gold schemas.
-- =========================================================================
