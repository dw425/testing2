"""
SQLite-backed config + data-cache persistence for LHO Lite.

Tables
------
config        key/value pairs (sensitive values Fernet-encrypted at rest)
data_cache    timestamped JSON snapshots of collected data
"""

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from cryptography.fernet import Fernet

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_DB_DIR = Path(os.environ.get("LHO_DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
_DB_PATH = _DB_DIR / "lho_lite.db"
_KEY_PATH = _DB_DIR / ".fernet.key"

_SENSITIVE_KEYS = {"pat_token", "sp_client_secret"}

_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Fernet helpers
# ---------------------------------------------------------------------------

def _ensure_dirs():
    _DB_DIR.mkdir(parents=True, exist_ok=True)


def _get_fernet() -> Fernet:
    _ensure_dirs()
    if _KEY_PATH.exists():
        key = _KEY_PATH.read_bytes().strip()
    else:
        key = Fernet.generate_key()
        _KEY_PATH.write_bytes(key)
        os.chmod(str(_KEY_PATH), 0o600)
    return Fernet(key)


def _encrypt(value: str) -> str:
    return _get_fernet().encrypt(value.encode()).decode()


def _decrypt(token: str) -> str:
    return _get_fernet().decrypt(token.encode()).decode()


# ---------------------------------------------------------------------------
# DB bootstrap
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    _ensure_dirs()
    c = sqlite3.connect(str(_DB_PATH), check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute(
        "CREATE TABLE IF NOT EXISTS config "
        "(key TEXT PRIMARY KEY, value TEXT NOT NULL, encrypted INTEGER DEFAULT 0)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS data_cache "
        "(id INTEGER PRIMARY KEY AUTOINCREMENT, "
        " snapshot TEXT NOT NULL, "
        " collected_at TEXT NOT NULL, "
        " duration_sec REAL DEFAULT 0)"
    )
    c.commit()
    return c


# ---------------------------------------------------------------------------
# Config CRUD
# ---------------------------------------------------------------------------

def has_config() -> bool:
    """Return True if at least workspace_url is configured."""
    with _lock:
        db = _conn()
        row = db.execute("SELECT value FROM config WHERE key='workspace_url'").fetchone()
        db.close()
        return bool(row and row[0])


def get_config() -> dict:
    """Return all config as a plain dict (sensitive values decrypted)."""
    with _lock:
        db = _conn()
        rows = db.execute("SELECT key, value, encrypted FROM config").fetchall()
        db.close()
    cfg = {}
    for key, value, encrypted in rows:
        if encrypted:
            try:
                cfg[key] = _decrypt(value)
            except Exception:
                cfg[key] = ""
        else:
            cfg[key] = value
    return cfg


def save_config(data: dict):
    """Upsert config keys. Encrypts sensitive values."""
    with _lock:
        db = _conn()
        for key, value in data.items():
            if value is None:
                value = ""
            value = str(value)
            if key in _SENSITIVE_KEYS and value:
                db.execute(
                    "INSERT INTO config (key, value, encrypted) VALUES (?, ?, 1) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value, encrypted=1",
                    (key, _encrypt(value)),
                )
            else:
                db.execute(
                    "INSERT INTO config (key, value, encrypted) VALUES (?, ?, 0) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value, encrypted=0",
                    (key, value),
                )
        db.commit()
        db.close()


def delete_config():
    """Wipe all config (for reset)."""
    with _lock:
        db = _conn()
        db.execute("DELETE FROM config")
        db.commit()
        db.close()


# ---------------------------------------------------------------------------
# Data cache
# ---------------------------------------------------------------------------

def save_data_snapshot(data: dict, duration_sec: float = 0):
    """Persist a JSON snapshot of collected data."""
    with _lock:
        db = _conn()
        db.execute(
            "INSERT INTO data_cache (snapshot, collected_at, duration_sec) VALUES (?, ?, ?)",
            (json.dumps(data, default=str), datetime.now(timezone.utc).isoformat(), duration_sec),
        )
        # Keep only last 10 snapshots
        db.execute(
            "DELETE FROM data_cache WHERE id NOT IN "
            "(SELECT id FROM data_cache ORDER BY id DESC LIMIT 10)"
        )
        db.commit()
        db.close()


def get_latest_snapshot() -> dict | None:
    """Return the most recent snapshot or None."""
    with _lock:
        db = _conn()
        row = db.execute(
            "SELECT snapshot, collected_at, duration_sec FROM data_cache ORDER BY id DESC LIMIT 1"
        ).fetchone()
        db.close()
    if not row:
        return None
    return {
        "data": json.loads(row[0]),
        "collected_at": row[1],
        "duration_sec": row[2],
    }
