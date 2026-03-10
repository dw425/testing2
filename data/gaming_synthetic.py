"""
GamingIQ — Synthetic Data Generator
=====================================
Config-driven synthetic data generator for the GamingIQ vertical.

Reads parameters from ``config/gaming.yaml`` and writes realistic
synthetic data into bronze-layer Delta tables in Unity Catalog.

Target metrics (from config):
  - 2.4 M DAU, 847 K concurrent peak, 125 K events/sec
  - $284 K daily revenue, ARPDAU $0.118
  - 1.85 M items traded/day, inflation index 1.03
  - 4.2 M matches/day, 12.4 s avg queue, 0.8% unfair reports
  - Retention: D1=68%, D7=41%, D30=22%, 34.2 K high churn risk

Supports two execution modes:
  - **full**        : Truncate-and-load.  Generates a complete day's records.
  - **incremental** : Append-only.  Generates a micro-batch (~1/24 of a day).

Usage::

    python -m data.gaming_synthetic --mode full
    python -m data.gaming_synthetic --mode incremental
"""

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("GamingIQ.SyntheticGenerator")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_INCREMENTAL_FRACTION = 1 / 24  # one hour of a full day

# Segment distribution weights (Whale, Dolphin, Minnow, Free-to-Play)
_SEGMENT_WEIGHTS = [0.02, 0.08, 0.25, 0.65]

# Segment-specific spending multipliers (USD per transaction)
_SEGMENT_SPEND = {
    "Whale": (20.0, 200.0),
    "Dolphin": (5.0, 40.0),
    "Minnow": (0.99, 10.0),
    "Free-to-Play": (0.0, 0.0),
}

# Item categories and rarity tiers
_ITEM_CATEGORIES = ["Weapon", "Armor", "Consumable", "Cosmetic", "Currency"]
_ITEM_RARITIES = ["Common", "Uncommon", "Rare", "Epic", "Legendary"]
_RARITY_WEIGHTS = [0.40, 0.25, 0.20, 0.10, 0.05]

# Event types
_EVENT_TYPES = ["login", "logout", "level_up", "achievement", "death", "purchase", "social"]

# Device types
_DEVICE_TYPES = ["PC", "Console", "Mobile"]
_DEVICE_WEIGHTS = [0.35, 0.40, 0.25]

# Transaction types
_TRANSACTION_TYPES = ["purchase", "trade", "gift", "refund"]
_TRANSACTION_WEIGHTS = [0.50, 0.30, 0.15, 0.05]

# Game modes
_GAME_MODES = ["Ranked", "Casual", "Tournament", "Custom"]
_GAME_MODE_WEIGHTS = [0.40, 0.35, 0.15, 0.10]

# Example item names per category
_ITEM_NAMES = {
    "Weapon": ["Plasma Rifle", "Shadow Blade", "Turbo Cannon", "Void Staff", "Frost Bow"],
    "Armor": ["Titan Shield", "Phantom Cloak", "Speed Suit", "Dragon Mail", "Energy Barrier"],
    "Consumable": ["Health Potion", "Mana Crystal", "Speed Boost", "XP Scroll", "Shield Flask"],
    "Cosmetic": ["Neon Skin", "Victory Emote", "Star Trail", "Galaxy Mount", "Holo Badge"],
    "Currency": ["Gold Pack 100", "Gold Pack 500", "Gold Pack 1000", "Gem Bundle", "Season Pass"],
}

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
RAW_PLAYER_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("player_id", StringType(), False),
    StructField("game_title", StringType(), False),
    StructField("region", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("session_duration_sec", IntegerType(), True),
    StructField("level", IntegerType(), True),
    StructField("_ingested_at", TimestampType(), True),
])

RAW_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("player_id", StringType(), False),
    StructField("game_title", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("item_name", StringType(), False),
    StructField("item_rarity", StringType(), False),
    StructField("amount_usd", DoubleType(), True),
    StructField("currency_amount", IntegerType(), True),
    StructField("_ingested_at", TimestampType(), True),
])

RAW_MATCH_EVENTS_SCHEMA = StructType([
    StructField("match_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("game_title", StringType(), False),
    StructField("region", StringType(), False),
    StructField("game_mode", StringType(), False),
    StructField("player_count", IntegerType(), False),
    StructField("avg_skill_rating", DoubleType(), True),
    StructField("skill_spread", DoubleType(), True),
    StructField("queue_time_sec", DoubleType(), True),
    StructField("match_duration_sec", IntegerType(), True),
    StructField("reported_unfair", BooleanType(), True),
    StructField("_ingested_at", TimestampType(), True),
])


# ===================================================================
# Configuration helpers
# ===================================================================

def _resolve_config_path(config_rel: str = "config/gaming.yaml") -> Path:
    """Resolve config path relative to the repository root."""
    cwd = Path.cwd()
    candidate = cwd / config_rel
    if candidate.is_file():
        return candidate
    repo_root = Path(__file__).resolve().parent.parent
    candidate = repo_root / config_rel
    if candidate.is_file():
        return candidate
    raise FileNotFoundError(
        f"Cannot locate config file '{config_rel}' from {cwd} or {repo_root}"
    )


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load and return the YAML configuration dictionary."""
    path = Path(config_path) if config_path else _resolve_config_path()
    with open(path, "r") as fh:
        cfg = yaml.safe_load(fh)
    logger.info("Loaded config from %s", path)
    return cfg


# ===================================================================
# Spark session
# ===================================================================

def get_spark() -> SparkSession:
    """Return or create the SparkSession for the generator."""
    return (
        SparkSession.builder
        .appName("GamingIQ-SyntheticGenerator")
        .getOrCreate()
    )


# ===================================================================
# Deterministic helpers
# ===================================================================

def _uuid() -> str:
    return str(uuid.uuid4())


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _random_timestamps(
    rng: np.random.Generator,
    start: datetime,
    end: datetime,
    n: int,
) -> np.ndarray:
    """Return *n* uniformly distributed timestamp floats in [start, end), sorted."""
    start_ts = start.timestamp()
    end_ts = end.timestamp()
    return np.sort(rng.uniform(start_ts, end_ts, size=n))


def _pick(rng: np.random.Generator, choices: List[str]) -> str:
    return choices[rng.integers(0, len(choices))]


def _pick_weighted(
    rng: np.random.Generator,
    choices: List[str],
    weights: List[float],
) -> str:
    return rng.choice(choices, p=weights)


# ===================================================================
# Player pool
# ===================================================================

def _build_player_pool(
    cfg: Dict[str, Any],
    rng: np.random.Generator,
) -> List[Dict[str, Any]]:
    """Pre-generate a pool of player profiles that events reference.

    Each player has a stable player_id, game_title, region, segment,
    skill_rating, and level. This ensures consistent cross-table references.
    """
    dau = cfg["data"]["telemetry"]["dau"]
    game_titles = cfg["data"]["game_titles"]
    regions = cfg["data"]["regions"]
    segments = cfg["data"]["player_segments"]

    players: List[Dict[str, Any]] = []
    for i in range(dau):
        segment = _pick_weighted(rng, segments, _SEGMENT_WEIGHTS)
        players.append({
            "player_id": f"P-{i:07d}",
            "game_title": _pick(rng, game_titles),
            "region": _pick(rng, regions),
            "segment": segment,
            "skill_rating": float(rng.normal(1500, 300)),
            "level": int(rng.integers(1, 100)),
            "device_type": _pick_weighted(rng, _DEVICE_TYPES, _DEVICE_WEIGHTS),
        })

    logger.info("Built player pool: %d players", len(players))
    return players


# ===================================================================
# Core generators
# ===================================================================

def generate_player_events(
    cfg: Dict[str, Any],
    player_pool: List[Dict[str, Any]],
    mode: str = "full",
    seed: int = 42,
) -> DataFrame:
    """Generate synthetic player event rows for ``bronze.raw_player_events``.

    Targets ~125K events/sec => ~10.8B per day. For demo purposes, we generate
    a scaled sample that preserves statistical distributions while keeping the
    dataset manageable. The gold layer KPIs are seeded to match the true
    target metrics from config.

    Args:
        cfg:         Loaded YAML config dict.
        player_pool: Pre-generated player profiles.
        mode:        ``"full"`` or ``"incremental"``.
        seed:        Numpy random seed for reproducibility.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_PLAYER_EVENTS_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    telem_cfg = cfg["data"]["telemetry"]
    avg_session_min = telem_cfg["avg_session_minutes"]  # 42

    # Scale: generate ~5 events per DAU player for demo (full day).
    # Real production would stream at 125K/sec; this produces a representative sample.
    events_per_player = 5
    n_events = len(player_pool) * events_per_player
    if mode == "incremental":
        n_events = int(n_events * _INCREMENTAL_FRACTION)

    now = _utcnow()
    window_hours = 24 if mode == "full" else 1
    start = now - timedelta(hours=window_hours)
    ts_floats = _random_timestamps(rng, start, now, n_events)

    logger.info(
        "Generating %d player event rows (mode=%s, window=%dh)",
        n_events, mode, window_hours,
    )

    rows: List[Dict[str, Any]] = []
    pool_size = len(player_pool)

    for i in range(n_events):
        player = player_pool[rng.integers(0, pool_size)]
        event_type = _pick(rng, _EVENT_TYPES)

        # Session duration: only meaningful for logout events.
        session_dur = None
        if event_type == "logout":
            # Gamma distribution centered around avg_session_min * 60 sec.
            session_dur = int(rng.gamma(4.0, avg_session_min * 60 / 4.0))
            session_dur = max(30, min(session_dur, 14400))  # 30s - 4h clamp

        rows.append({
            "event_id": _uuid(),
            "timestamp": datetime.fromtimestamp(ts_floats[i], tz=timezone.utc),
            "player_id": player["player_id"],
            "game_title": player["game_title"],
            "region": player["region"],
            "event_type": event_type,
            "session_id": f"S-{_uuid()[:12]}",
            "device_type": player["device_type"],
            "session_duration_sec": session_dur,
            "level": player["level"],
            "_ingested_at": now,
        })

    logger.info("Generated %d player event rows", len(rows))
    return spark.createDataFrame(rows, schema=RAW_PLAYER_EVENTS_SCHEMA)


def generate_transactions(
    cfg: Dict[str, Any],
    player_pool: List[Dict[str, Any]],
    mode: str = "full",
    seed: int = 43,
) -> DataFrame:
    """Generate synthetic transaction rows for ``bronze.raw_transactions``.

    Targets:
      - $284K daily revenue
      - 1.85M items traded per day
      - 89 suspicious transactions (injected via anomalous price spikes)

    Args:
        cfg:         Loaded YAML config dict.
        player_pool: Pre-generated player profiles.
        mode:        ``"full"`` or ``"incremental"``.
        seed:        Numpy random seed.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_TRANSACTIONS_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    econ_cfg = cfg["data"]["economy"]
    items_traded = econ_cfg["items_traded_24h"]       # 1,850,000
    suspicious_count = econ_cfg["suspicious_transactions"]  # 89

    n_transactions = items_traded
    if mode == "incremental":
        n_transactions = int(n_transactions * _INCREMENTAL_FRACTION)

    now = _utcnow()
    window_hours = 24 if mode == "full" else 1
    start = now - timedelta(hours=window_hours)
    ts_floats = _random_timestamps(rng, start, now, n_transactions)

    # Pre-compute suspicious transaction indices.
    suspicious_target = suspicious_count if mode == "full" else max(1, int(suspicious_count * _INCREMENTAL_FRACTION))
    suspicious_indices = set(rng.choice(n_transactions, size=suspicious_target, replace=False))

    logger.info(
        "Generating %d transaction rows (mode=%s, %d suspicious)",
        n_transactions, mode, len(suspicious_indices),
    )

    # Filter to spending players (not Free-to-Play for purchases).
    spending_players = [p for p in player_pool if p["segment"] != "Free-to-Play"]
    all_players_count = len(player_pool)
    spending_count = len(spending_players)

    rows: List[Dict[str, Any]] = []

    for i in range(n_transactions):
        is_suspicious = i in suspicious_indices

        # Pick transaction type.
        tx_type = _pick_weighted(rng, _TRANSACTION_TYPES, _TRANSACTION_WEIGHTS)

        # Pick player: purchases come from spending players, trades from anyone.
        if tx_type == "purchase" and spending_count > 0:
            player = spending_players[rng.integers(0, spending_count)]
        else:
            player = player_pool[rng.integers(0, all_players_count)]

        # Pick item.
        category = _pick(rng, _ITEM_CATEGORIES)
        item_name = _pick(rng, _ITEM_NAMES[category])
        item_rarity = _pick_weighted(rng, _ITEM_RARITIES, _RARITY_WEIGHTS)

        # Compute USD amount based on segment.
        segment = player["segment"]
        spend_range = _SEGMENT_SPEND.get(segment, (0.0, 0.0))

        if tx_type in ("purchase", "trade") and spend_range[1] > 0:
            amount_usd = float(rng.uniform(spend_range[0], spend_range[1]))
            # Rarity multiplier.
            rarity_mult = {"Common": 1.0, "Uncommon": 1.5, "Rare": 2.5, "Epic": 5.0, "Legendary": 10.0}
            amount_usd *= rarity_mult.get(item_rarity, 1.0)
        elif tx_type == "refund":
            amount_usd = -float(rng.uniform(1.0, 50.0))
        else:
            amount_usd = 0.0

        # Suspicious: inject anomalous price spike.
        if is_suspicious:
            amount_usd = float(rng.uniform(500.0, 5000.0))
            item_rarity = "Legendary"

        currency_amount = int(abs(amount_usd) * rng.uniform(80, 120))

        rows.append({
            "transaction_id": _uuid(),
            "timestamp": datetime.fromtimestamp(ts_floats[i], tz=timezone.utc),
            "player_id": player["player_id"],
            "game_title": player["game_title"],
            "transaction_type": tx_type,
            "item_name": item_name,
            "item_rarity": item_rarity,
            "amount_usd": round(amount_usd, 2),
            "currency_amount": currency_amount,
            "_ingested_at": now,
        })

    logger.info("Generated %d transaction rows", len(rows))
    return spark.createDataFrame(rows, schema=RAW_TRANSACTIONS_SCHEMA)


def generate_match_events(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 44,
) -> DataFrame:
    """Generate synthetic match event rows for ``bronze.raw_match_events``.

    Targets:
      - 4.2M matches per day
      - 12.4s average queue time
      - 0.8% unfair report rate
      - Skill spread ~0.15 (normalised)

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_MATCH_EVENTS_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    mm_cfg = cfg["data"]["matchmaking"]
    matches_24h = mm_cfg["matches_24h"]                # 4,200,000
    avg_queue_sec = mm_cfg["avg_queue_time_sec"]        # 12.4
    skill_spread_target = mm_cfg["skill_rating_spread"] # 0.15
    unfair_rate = mm_cfg["reported_unfair"]             # 0.008

    game_titles = cfg["data"]["game_titles"]
    regions = cfg["data"]["regions"]

    n_matches = matches_24h
    if mode == "incremental":
        n_matches = int(n_matches * _INCREMENTAL_FRACTION)

    now = _utcnow()
    window_hours = 24 if mode == "full" else 1
    start = now - timedelta(hours=window_hours)
    ts_floats = _random_timestamps(rng, start, now, n_matches)

    logger.info(
        "Generating %d match event rows (mode=%s, window=%dh)",
        n_matches, mode, window_hours,
    )

    # Pre-draw vectorised arrays for performance.
    game_indices = rng.integers(0, len(game_titles), size=n_matches)
    region_indices = rng.integers(0, len(regions), size=n_matches)
    game_mode_draws = rng.choice(len(_GAME_MODES), size=n_matches, p=_GAME_MODE_WEIGHTS)
    player_counts = rng.integers(2, 101, size=n_matches)
    avg_skills = rng.normal(1500, 300, size=n_matches)
    skill_spreads = np.abs(rng.normal(skill_spread_target * 1500, 50, size=n_matches))
    # Queue time: exponential distribution centered on avg_queue_sec.
    queue_times = rng.exponential(avg_queue_sec, size=n_matches)
    queue_times = np.clip(queue_times, 1.0, 300.0)
    match_durations = rng.integers(120, 3600, size=n_matches)
    unfair_flags = rng.random(size=n_matches) < unfair_rate

    rows: List[Dict[str, Any]] = []

    for i in range(n_matches):
        rows.append({
            "match_id": _uuid(),
            "timestamp": datetime.fromtimestamp(ts_floats[i], tz=timezone.utc),
            "game_title": game_titles[game_indices[i]],
            "region": regions[region_indices[i]],
            "game_mode": _GAME_MODES[game_mode_draws[i]],
            "player_count": int(player_counts[i]),
            "avg_skill_rating": round(float(avg_skills[i]), 1),
            "skill_spread": round(float(skill_spreads[i]), 2),
            "queue_time_sec": round(float(queue_times[i]), 2),
            "match_duration_sec": int(match_durations[i]),
            "reported_unfair": bool(unfair_flags[i]),
            "_ingested_at": now,
        })

    logger.info("Generated %d match event rows", len(rows))
    return spark.createDataFrame(rows, schema=RAW_MATCH_EVENTS_SCHEMA)


# ===================================================================
# Writer
# ===================================================================

def _write_delta(
    df: DataFrame,
    catalog: str,
    schema: str,
    table: str,
    mode: str,
) -> None:
    """Write a DataFrame to a Unity Catalog Delta table.

    Args:
        df:      DataFrame to persist.
        catalog: UC catalog name (e.g. ``gaming_iq``).
        schema:  UC schema / database name (e.g. ``bronze``).
        table:   Table name.
        mode:    ``"full"`` maps to overwrite; ``"incremental"`` maps to append.
    """
    fqn = f"{catalog}.{schema}.{table}"
    write_mode = "overwrite" if mode == "full" else "append"

    logger.info("Writing to %s (mode=%s) ...", fqn, write_mode)

    (
        df.write
        .format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true" if write_mode == "overwrite" else "false")
        .saveAsTable(fqn)
    )

    count = df.count()
    logger.info("Wrote %d rows to %s", count, fqn)


def _ensure_schemas_exist(spark: SparkSession, catalog: str) -> None:
    """Create the catalog and medallion schemas if they do not exist."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"USE CATALOG {catalog}")
    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    logger.info("Ensured catalog '%s' and schemas bronze/silver/gold exist", catalog)


# ===================================================================
# Orchestrator
# ===================================================================

def run_generation(
    cfg: Dict[str, Any],
    mode: str = "full",
) -> None:
    """Run the full synthetic-data generation pipeline.

    Generates player events, transactions, and match events, then writes
    them to the corresponding bronze Delta tables.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
    """
    spark = get_spark()
    catalog: str = cfg["app"]["catalog"]
    rng = np.random.default_rng(42)

    logger.info(
        "=== GamingIQ Synthetic Data Generation ===  catalog=%s  mode=%s",
        catalog, mode,
    )

    _ensure_schemas_exist(spark, catalog)

    # Build a shared player pool for cross-table consistency.
    player_pool = _build_player_pool(cfg, rng)

    # --- Bronze: player events ---
    events_df = generate_player_events(cfg, player_pool, mode=mode)
    _write_delta(events_df, catalog, "bronze", "raw_player_events", mode)

    # --- Bronze: transactions ---
    transactions_df = generate_transactions(cfg, player_pool, mode=mode)
    _write_delta(transactions_df, catalog, "bronze", "raw_transactions", mode)

    # --- Bronze: match events ---
    match_df = generate_match_events(cfg, mode=mode)
    _write_delta(match_df, catalog, "bronze", "raw_match_events", mode)

    logger.info("=== GamingIQ generation complete ===")


# ===================================================================
# CLI entry point
# ===================================================================

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="GamingIQ synthetic data generator",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["full", "incremental"],
        default="full",
        help="Generation mode: 'full' (truncate & load) or 'incremental' (append).",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to the YAML config file. Defaults to config/gaming.yaml.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logger.info(
        "Starting GamingIQ generator — mode=%s  config=%s",
        args.mode, args.config or "(auto-detect)",
    )
    config = load_config(args.config)
    run_generation(config, mode=args.mode)
