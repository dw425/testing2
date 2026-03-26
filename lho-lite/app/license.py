"""
License validation for LHO Lite.

Validates license keys against a GitHub-hosted license registry.
Re-checks every 30 days. Between checks, the app validates locally
against the stored expiration date. If the license is expired or
revoked, the app blocks all access until a valid license is provided.

License registry format (JSON file hosted in a private GitHub repo):
{
    "licenses": {
        "<license-key>": {
            "valid": true,
            "expires": "2026-12-31",
            "customer": "Acme Corp",
            "message": "Licensed to Acme Corp"
        },
        ...
    }
}
"""

import hashlib
import json
import logging
import threading
from datetime import datetime, timezone, timedelta

import requests

from app.config_store import get_config, save_config

log = logging.getLogger("lho.license")

# ---------------------------------------------------------------------------
# GitHub-hosted License Registry
# ---------------------------------------------------------------------------
# Private GitHub repo containing a licenses.json file.
# The app fetches this file using a GitHub token for authentication.
# Set these to your actual repo details.

GITHUB_OWNER = "dw425"
GITHUB_REPO = "testing2"
GITHUB_BRANCH = "main"
GITHUB_FILE_PATH = "licenses/licenses.json"

# Construct the raw content URL
LICENSE_REGISTRY_URL = (
    f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}"
    f"/{GITHUB_BRANCH}/{GITHUB_FILE_PATH}"
)

# GitHub token for accessing private repo — read from env or config
# Set LHO_GITHUB_TOKEN env var, or store as "github_token" in config
GITHUB_TOKEN_ENV = "LHO_GITHUB_TOKEN"

# How often to re-validate against the remote registry (in days)
RECHECK_INTERVAL_DAYS = 30

# Grace period after failed check before blocking (hours)
# Allows the app to keep running briefly if GitHub is temporarily unreachable
GRACE_PERIOD_HOURS = 48

# ---------------------------------------------------------------------------
# License state (in-memory cache)
# ---------------------------------------------------------------------------
_license_state = {
    "valid": False,
    "checked_at": None,
    "expires": None,
    "message": "",
    "grace_until": None,
}
_state_lock = threading.Lock()


def _get_github_token() -> str:
    """Get GitHub token from env var or stored config."""
    import os
    token = os.environ.get(GITHUB_TOKEN_ENV, "")
    if not token:
        cfg = get_config()
        token = cfg.get("github_token", "")
    return token


def _fingerprint() -> str:
    """Generate a workspace fingerprint for license binding."""
    cfg = get_config()
    ws = cfg.get("workspace_url", "unknown")
    return hashlib.sha256(ws.encode()).hexdigest()[:16]


def _is_expired(expires_str: str) -> bool:
    """Check if an expiration date string is in the past."""
    if not expires_str:
        return True
    try:
        expires_date = datetime.fromisoformat(expires_str).replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) > expires_date
    except Exception:
        try:
            expires_date = datetime.strptime(expires_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            # Set to end of day
            expires_date = expires_date.replace(hour=23, minute=59, second=59)
            return datetime.now(timezone.utc) > expires_date
        except Exception:
            log.warning("Cannot parse expiration date: %s", expires_str)
            return True


def validate_license(license_key: str = None, workspace_url: str = None) -> dict:
    """
    Validate a license key against the GitHub-hosted license registry.

    Fetches licenses.json from the private GitHub repo and checks if the
    provided key exists and is valid.

    Returns: {"valid": bool, "expires": str, "message": str, "new_key": str|None}
    """
    if not license_key:
        cfg = get_config()
        license_key = cfg.get("license_key", "")
    if not workspace_url:
        cfg = get_config()
        workspace_url = cfg.get("workspace_url", "")

    if not license_key:
        return {"valid": False, "expires": None, "message": "No license key configured.", "new_key": None}

    github_token = _get_github_token()

    try:
        headers = {"Accept": "application/vnd.github.v3.raw"}
        if github_token:
            headers["Authorization"] = f"token {github_token}"

        resp = requests.get(
            LICENSE_REGISTRY_URL,
            headers=headers,
            timeout=15,
        )

        if resp.status_code == 200:
            registry = resp.json()
            licenses = registry.get("licenses", {})

            # Look up the license key
            entry = licenses.get(license_key)

            if entry is None:
                return {
                    "valid": False,
                    "expires": None,
                    "message": "License key not found in registry.",
                    "new_key": None,
                }

            is_valid = entry.get("valid", False)
            expires = entry.get("expires", "")
            customer = entry.get("customer", "")
            message = entry.get("message", "")
            new_key = entry.get("new_key")

            # Check if explicitly marked invalid (revoked)
            if not is_valid:
                return {
                    "valid": False,
                    "expires": expires,
                    "message": message or "License has been revoked.",
                    "new_key": new_key,
                }

            # Check expiration
            if expires and _is_expired(expires):
                return {
                    "valid": False,
                    "expires": expires,
                    "message": f"License expired on {expires}.",
                    "new_key": new_key,
                }

            # Valid and not expired
            display_msg = message or f"Licensed to {customer}" if customer else "License valid."
            return {
                "valid": True,
                "expires": expires,
                "message": display_msg,
                "new_key": new_key,
            }

        elif resp.status_code == 404:
            log.warning("License registry not found at %s", LICENSE_REGISTRY_URL)
            return {
                "valid": None,
                "expires": None,
                "message": "License registry not found. Contact support.",
                "new_key": None,
            }
        elif resp.status_code == 401 or resp.status_code == 403:
            log.warning("GitHub auth failed for license registry (HTTP %d)", resp.status_code)
            return {
                "valid": None,
                "expires": None,
                "message": "License registry auth failed. Contact support.",
                "new_key": None,
            }
        else:
            log.warning("License registry returned %d: %s", resp.status_code, resp.text[:200])
            return {
                "valid": None,
                "expires": None,
                "message": f"License registry error (HTTP {resp.status_code})",
                "new_key": None,
            }

    except requests.exceptions.ConnectionError:
        log.warning("Cannot reach license registry at %s", LICENSE_REGISTRY_URL)
        return {
            "valid": None,  # None = unreachable, use grace period / local expiry
            "expires": None,
            "message": "Cannot reach license registry. Will retry.",
            "new_key": None,
        }
    except Exception as e:
        log.error("License validation error: %s", e)
        return {
            "valid": None,
            "expires": None,
            "message": f"Validation error: {e}",
            "new_key": None,
        }


def check_and_update_license(force: bool = False) -> dict:
    """
    Check license validity. Re-validates against the remote registry if:
    - Never checked before
    - Last check was more than RECHECK_INTERVAL_DAYS ago
    - force=True

    Between remote checks, validates locally against the stored expiration
    date. If the expiration date is still in the future, the license is
    considered valid without contacting the server.

    Updates the stored license key if the registry issues a new one.
    Returns the current license state dict.
    """
    global _license_state

    cfg = get_config()
    license_key = cfg.get("license_key", "")
    last_check_str = cfg.get("license_checked_at", "")
    last_valid_str = cfg.get("license_valid", "")
    stored_expires = cfg.get("license_expires", "")

    if not license_key:
        with _state_lock:
            _license_state = {
                "valid": False,
                "checked_at": None,
                "expires": None,
                "message": "No license key configured.",
                "grace_until": None,
            }
        return _license_state

    # Determine if we need a remote re-check
    needs_check = force
    now = datetime.now(timezone.utc)

    if not needs_check and last_check_str:
        try:
            last_check = datetime.fromisoformat(last_check_str)
            if (now - last_check) > timedelta(days=RECHECK_INTERVAL_DAYS):
                needs_check = True
                log.info("License re-check due (last checked %s)", last_check_str)
        except Exception:
            needs_check = True
    elif not last_check_str:
        needs_check = True

    if not needs_check:
        # Between remote checks — validate locally using stored expiration date
        if last_valid_str == "true" and stored_expires:
            if _is_expired(stored_expires):
                # Expiration date has passed — license expired locally
                log.warning("License expired locally (expires: %s)", stored_expires)
                save_config({
                    "license_valid": "false",
                    "license_message": f"License expired on {stored_expires}.",
                })
                with _state_lock:
                    _license_state = {
                        "valid": False,
                        "checked_at": last_check_str,
                        "expires": stored_expires,
                        "message": f"License expired on {stored_expires}.",
                        "grace_until": None,
                    }
                return _license_state
            else:
                # Expiration still in the future — license is valid
                with _state_lock:
                    _license_state = {
                        "valid": True,
                        "checked_at": last_check_str,
                        "expires": stored_expires,
                        "message": cfg.get("license_message", ""),
                        "grace_until": None,
                    }
                return _license_state

        # Use cached state (no expiration to check)
        with _state_lock:
            _license_state = {
                "valid": last_valid_str == "true",
                "checked_at": last_check_str,
                "expires": stored_expires,
                "message": cfg.get("license_message", ""),
                "grace_until": None,
            }
        return _license_state

    # Perform remote validation against GitHub registry
    log.info("Validating license key against remote registry...")
    result = validate_license(license_key)

    if result["valid"] is True:
        # License is valid
        log.info("License valid. Expires: %s", result.get("expires", "N/A"))

        # If registry issued a new key, update it
        if result.get("new_key") and result["new_key"] != license_key:
            log.info("License key rotated by registry.")
            save_config({"license_key": result["new_key"]})

        save_config({
            "license_checked_at": now.isoformat(),
            "license_valid": "true",
            "license_expires": result.get("expires") or "",
            "license_message": result.get("message") or "",
        })

        with _state_lock:
            _license_state = {
                "valid": True,
                "checked_at": now.isoformat(),
                "expires": result.get("expires"),
                "message": result.get("message", ""),
                "grace_until": None,
            }

    elif result["valid"] is False:
        # License explicitly invalid/revoked/expired
        log.warning("License INVALID: %s", result.get("message", ""))

        save_config({
            "license_checked_at": now.isoformat(),
            "license_valid": "false",
            "license_expires": result.get("expires") or "",
            "license_message": result.get("message") or "License is invalid or has been revoked.",
        })

        with _state_lock:
            _license_state = {
                "valid": False,
                "checked_at": now.isoformat(),
                "expires": result.get("expires"),
                "message": result.get("message") or "License is invalid or has been revoked.",
                "grace_until": None,
            }

    else:
        # Registry unreachable — check local expiration first, then grace period
        if last_valid_str == "true" and stored_expires and not _is_expired(stored_expires):
            # Still within expiration date — stay valid
            log.info("Registry unreachable but license not expired (expires %s). Staying valid.", stored_expires)
            with _state_lock:
                _license_state = {
                    "valid": True,
                    "checked_at": last_check_str,
                    "expires": stored_expires,
                    "message": f"Registry unreachable. License valid until {stored_expires}.",
                    "grace_until": None,
                }
        elif last_valid_str == "true":
            # Was valid but no expiration or expired — use grace period
            grace_until = now + timedelta(hours=GRACE_PERIOD_HOURS)
            log.warning("Registry unreachable. Grace period until %s", grace_until.isoformat())
            with _state_lock:
                _license_state = {
                    "valid": True,
                    "checked_at": last_check_str,
                    "expires": stored_expires,
                    "message": f"License registry unreachable. Grace period active until {grace_until.strftime('%Y-%m-%d %H:%M UTC')}.",
                    "grace_until": grace_until.isoformat(),
                }
        else:
            # Was already invalid, stay invalid
            with _state_lock:
                _license_state = {
                    "valid": False,
                    "checked_at": now.isoformat(),
                    "expires": None,
                    "message": "Cannot verify license. Please check your license key.",
                    "grace_until": None,
                }

    return _license_state


def is_licensed() -> bool:
    """Quick check: is the app currently licensed?

    Checks in-memory state first. Also validates the stored expiration
    date — if the license has expired since the last check, it flips
    the state to invalid.
    """
    with _state_lock:
        if not _license_state.get("valid", False):
            return False

        # Double-check expiration date hasn't passed
        expires = _license_state.get("expires")
        if expires and _is_expired(expires):
            _license_state["valid"] = False
            _license_state["message"] = f"License expired on {expires}."
            return False

        return True


def get_license_state() -> dict:
    """Return current license state."""
    with _state_lock:
        return dict(_license_state)


def license_blocked_page() -> str:
    """Return HTML page shown when the license is invalid."""
    state = get_license_state()
    message = state.get("message") or "Your license is invalid or has expired."

    return f'''<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>LHO Lite — License Required</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<style>
body{{background:#0D1117;color:#E6EDF3;font-family:'DM Sans',sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
.box{{text-align:center;max-width:480px;padding:40px}}
.icon{{font-size:64px;color:#F87171;margin-bottom:24px}}
h2{{margin:0 0 12px;font-size:24px}}
p{{color:#8B949E;font-size:14px;line-height:1.6}}
.msg{{background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.3);border-radius:8px;padding:16px;margin:20px 0;color:#F87171;font-size:13px}}
.btn{{display:inline-block;background:#4B7BF5;color:white;padding:10px 24px;border-radius:8px;text-decoration:none;font-weight:600;font-size:14px;margin-top:16px}}
.footer{{margin-top:32px;font-size:11px;color:#484F58}}
</style></head><body>
<div class="box">
  <div class="icon"><i class="fas fa-lock"></i></div>
  <h2>License Required</h2>
  <p>LHO Lite requires a valid license from Blueprint Technologies to operate.</p>
  <div class="msg">{message}</div>
  <a href="/admin" class="btn">Enter License Key</a>
  <div class="footer">
    Contact Blueprint Technologies for licensing<br>
    <a href="mailto:support@blueprint.tech" style="color:#4B7BF5">support@blueprint.tech</a>
  </div>
</div>
</body></html>'''
