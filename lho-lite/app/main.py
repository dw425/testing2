#!/usr/bin/env python3
"""
LHO Lite — Lakehouse Optimizer Lite
====================================
Flask app with Blueprint dark theme dashboard, admin setup, scheduled refresh,
and Excel export.  Universal across AWS / Azure / GCP Databricks workspaces.

Usage:
  # Local mode (interactive)
  python3 app/main.py --host https://<workspace> --token dapi...

  # Databricks App mode (auto-detects env)
  python3 app/main.py
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

# Ensure project root is on sys.path so `from app.xxx` imports work
# (required for Databricks Apps where cwd is the source_code dir)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from flask import Flask, request, redirect, send_file, jsonify

from app.config_store import (
    has_config, get_config, save_config, delete_config, save_data_snapshot, get_latest_snapshot,
)
from app.collector import DatabricksCollector
from app.analyzer import (
    analyze_security, compute_security_score, generate_mermaid_architecture,
    assess_compliance, build_workspace_profile,
)
from app.dashboard import render_dashboard
from app.admin import render_admin_page
from app.excel_export import generate_security_excel, generate_usage_excel
from app import scheduler as sched
from app.license import (
    check_and_update_license, is_licensed, get_license_state, license_blocked_page,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lho")

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
flask_app = Flask(__name__)
flask_app.secret_key = os.urandom(24)

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_PORT"))


# ---------------------------------------------------------------------------
# License gate — blocks all routes except /admin and /health if unlicensed
# ---------------------------------------------------------------------------
@flask_app.before_request
def _check_license():
    # Allow admin page (to enter/update license key), health check, and static files
    allowed = ("/admin", "/health", "/static/")
    if any(request.path.startswith(p) for p in allowed):
        return None
    if not is_licensed():
        return license_blocked_page(), 403


@flask_app.errorhandler(Exception)
def _handle_error(e):
    log.error("Unhandled error on %s: %s", request.path, e)
    return jsonify({"error": "Internal server error", "detail": str(e)}), 500


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_collector() -> DatabricksCollector:
    """Build a collector from stored config.
    In Databricks App mode, defaults to SDK auto-auth if no config is set.
    """
    cfg = get_config()
    auth_method = cfg.get("auth_method", "auto" if IS_DATABRICKS_APP else "pat")
    return DatabricksCollector(
        workspace_url=cfg.get("workspace_url", ""),
        auth_method=auth_method,
        pat_token=cfg.get("pat_token", ""),
        sp_client_id=cfg.get("sp_client_id", ""),
        sp_client_secret=cfg.get("sp_client_secret", ""),
        sp_tenant_id=cfg.get("sp_tenant_id", ""),
    )


def _do_refresh():
    """Full data collection + analysis + save snapshot."""
    log.info("Starting data refresh...")
    t0 = time.time()
    try:
        collector = _build_collector()
        data = collector.collect_all()
        duration = round(time.time() - t0, 1)
        save_data_snapshot(data, duration)
        log.info("Data refresh completed in %.1f seconds.", duration)
    except Exception as e:
        log.error("Data refresh failed after %.1f seconds: %s", time.time() - t0, e)


def _collecting_page() -> str:
    """Show a loading page while initial data collection runs."""
    return '''<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>LHO Lite — Collecting Data</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600;700&display=swap" rel="stylesheet">
<style>body{background:#0D1117;color:#E6EDF3;font-family:'DM Sans',sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
.box{text-align:center;max-width:400px}.spinner{width:48px;height:48px;border:4px solid #272D3F;border-top:4px solid #4B7BF5;border-radius:50%;animation:spin 1s linear infinite;margin:0 auto 24px}
@keyframes spin{to{transform:rotate(360deg)}}h2{margin-bottom:8px}p{color:#8B949E;font-size:14px}
</style></head><body><div class="box"><div class="spinner"></div><h2>Collecting workspace data...</h2>
<p>This may take 1-3 minutes depending on workspace size.<br>The page will refresh automatically.</p></div>
<script>setTimeout(()=>location.reload(),5000);</script></body></html>'''


def _get_dashboard_html() -> str:
    """Render dashboard from latest snapshot."""
    snap = get_latest_snapshot()
    if not snap:
        return "<h2 style='color:#E6EDF3;font-family:sans-serif;padding:40px'>No data collected yet. <a href='/admin?setup=1' style='color:#4B7BF5'>Configure your workspace</a> and run a refresh.</h2>"

    data = snap["data"]
    sec_data = data.get("security", {})
    usage_data = data.get("usage", {})

    findings = analyze_security(sec_data)
    score = compute_security_score(findings)
    diagrams = generate_mermaid_architecture(sec_data)
    compliance = assess_compliance(sec_data, findings)
    workspace_profile = build_workspace_profile(sec_data)

    collected_at = snap.get("collected_at", "")
    try:
        dt = datetime.fromisoformat(collected_at)
        last_refresh = dt.strftime("Updated %Y-%m-%d %H:%M UTC")
    except Exception:
        last_refresh = f"Updated {collected_at}"

    return render_dashboard(
        snapshot=data,
        findings=findings,
        security_score=score,
        mermaid_diagrams=diagrams,
        compliance=compliance,
        workspace_profile=workspace_profile,
        last_refresh=last_refresh,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@flask_app.route("/")
def index():
    cfg = get_config()
    setup_done = cfg.get("setup_complete") == "true"
    if not setup_done:
        return redirect("/admin?setup=1")
    # If data is still being collected for the first time, show a waiting page
    if not get_latest_snapshot():
        if sched.get_status().get("is_refreshing"):
            return _collecting_page()
        # Trigger refresh if we have config or running as Databricks App
        if has_config() or IS_DATABRICKS_APP:
            sched.trigger_manual_refresh()
            return _collecting_page()
        return redirect("/admin?setup=1")
    return _get_dashboard_html()


@flask_app.route("/admin")
def admin():
    is_setup = request.args.get("setup") == "1"
    cfg = get_config() if has_config() else {}
    # Default to auto auth in Databricks App mode
    if IS_DATABRICKS_APP and not cfg.get("auth_method"):
        cfg["auth_method"] = "auto"
    msg = request.args.get("msg", "")
    lic_state = get_license_state()
    return render_admin_page(config=cfg, is_setup=is_setup, message=msg, license_state=lic_state)


@flask_app.route("/admin/validate-license", methods=["POST"])
def admin_validate_license():
    """AJAX license key validation."""
    from app.license import validate_license
    body = request.get_json(force=True)
    key = body.get("license_key", "").strip()
    result = validate_license(license_key=key)
    return jsonify(result)


@flask_app.route("/admin/save", methods=["POST"])
def admin_save():
    data = {
        "license_key": request.form.get("license_key", "").strip(),
        "workspace_url": request.form.get("workspace_url", "").strip(),
        "auth_method": request.form.get("auth_method", "pat"),
        "pat_token": request.form.get("pat_token", "").strip(),
        "sp_client_id": request.form.get("sp_client_id", "").strip(),
        "sp_client_secret": request.form.get("sp_client_secret", "").strip(),
        "sp_tenant_id": request.form.get("sp_tenant_id", "").strip(),
        "refresh_schedule": request.form.get("refresh_schedule", "manual"),
        "refresh_hour": request.form.get("refresh_hour", "6"),
        "data_destination": request.form.get("data_destination", "local"),
        "dest_catalog": request.form.get("dest_catalog", "").strip(),
        "dest_schema": request.form.get("dest_schema", "").strip(),
        "dest_table_prefix": request.form.get("dest_table_prefix", "lho_").strip(),
        "lakebase_instance": request.form.get("lakebase_instance", "").strip(),
        "lakebase_schema": request.form.get("lakebase_schema", "public").strip(),
    }

    # Normalize URL
    url = data["workspace_url"]
    if url and not url.startswith("http"):
        data["workspace_url"] = "https://" + url

    # Mark setup as complete
    data["setup_complete"] = "true"
    save_config(data)

    # Validate and activate license
    if data["license_key"]:
        check_and_update_license(force=True)

    # Configure schedule
    sched.configure_schedule(data["refresh_schedule"], int(data.get("refresh_hour", 6)))

    # Trigger initial refresh if first setup
    if not get_latest_snapshot():
        sched.trigger_manual_refresh()
        return redirect("/?msg=collecting")

    return redirect("/admin?msg=Settings+saved")


@flask_app.route("/admin/test", methods=["POST"])
def admin_test():
    """AJAX connection test."""
    workspace_url = request.form.get("workspace_url", "").strip()
    if not workspace_url.startswith("http"):
        workspace_url = "https://" + workspace_url

    auth_method = request.form.get("auth_method", "pat")

    try:
        collector = DatabricksCollector(
            workspace_url=workspace_url,
            auth_method=auth_method,
            pat_token=request.form.get("pat_token", "").strip(),
            sp_client_id=request.form.get("sp_client_id", "").strip(),
            sp_client_secret=request.form.get("sp_client_secret", "").strip(),
            sp_tenant_id=request.form.get("sp_tenant_id", "").strip(),
        )
        result = collector.test_connection()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@flask_app.route("/admin/reset")
def admin_reset():
    delete_config()
    return redirect("/admin?setup=1")


# ---- API endpoints ----

@flask_app.route("/api/data")
def api_data():
    snap = get_latest_snapshot()
    if not snap:
        return jsonify({"error": "No data available"}), 404
    return jsonify(snap)


@flask_app.route("/api/refresh", methods=["POST"])
def api_refresh():
    started = sched.trigger_manual_refresh()
    if started:
        return jsonify({"ok": True, "message": "Refresh started"})
    return jsonify({"ok": False, "message": "Refresh already in progress"})


@flask_app.route("/api/status")
def api_status():
    snap = get_latest_snapshot()
    status = sched.get_status()
    status["last_collected"] = snap["collected_at"] if snap else None
    status["last_duration"] = snap["duration_sec"] if snap else None
    return jsonify(status)


# ---- Excel exports ----

@flask_app.route("/export/security")
def export_security():
    snap = get_latest_snapshot()
    if not snap:
        return "No data available", 404
    sec_data = snap["data"].get("security", {})
    findings = analyze_security(sec_data)
    buf = generate_security_excel(sec_data, findings)
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=f"LHO_Security_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
    )


@flask_app.route("/export/usage")
def export_usage():
    snap = get_latest_snapshot()
    if not snap:
        return "No data available", 404
    sec_data = snap["data"].get("security", {})
    usage_data = snap["data"].get("usage", {})
    buf = generate_usage_excel(sec_data, usage_data)
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=f"LHO_Usage_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
    )


# ---- Health ----

@flask_app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "1.0.0"})


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="LHO Lite — Lakehouse Optimizer")
    parser.add_argument("--host", help="Databricks workspace URL")
    parser.add_argument("--token", help="Personal Access Token")
    parser.add_argument("--port", type=int, default=None, help="Port (default: 8050)")
    parser.add_argument("--no-browser", action="store_true", help="Don't open browser")
    parser.add_argument("--dev-license", action="store_true",
                        help="Skip license check (local development only)")
    args = parser.parse_args()

    # Initialize scheduler
    sched.init_scheduler(_do_refresh)

    # Promote pre-seeded license key from installer (if present)
    cfg_pre = get_config()
    pending_key = cfg_pre.get("license_key_pending", "")
    if pending_key and not cfg_pre.get("license_key"):
        save_config({"license_key": pending_key, "license_key_pending": ""})
        log.info("License key promoted from installer pre-seed.")

    # Validate license on startup
    if args.dev_license:
        log.info("DEV MODE: License check bypassed.")
        from app import license as lic_mod
        lic_mod._license_state["valid"] = True
        lic_mod._license_state["message"] = "Development mode — license check bypassed"
    else:
        try:
            log.info("Checking license...")
            lic = check_and_update_license()
            if lic.get("valid"):
                log.info("License valid.")
            else:
                log.warning("License not valid: %s", lic.get("message", "No license key"))
                log.warning("App will show license gate. Configure via /admin.")
        except Exception as e:
            log.warning("License check failed on startup: %s (app will show license gate)", e)

        # Schedule periodic license re-check (every 30 days, polled every 24 hours)
        def _license_recheck():
            try:
                check_and_update_license()
            except Exception as e:
                log.warning("Periodic license check failed: %s", e)
        sched.add_license_check(_license_recheck)

    # CLI args → save as config for convenience
    if args.host and args.token:
        save_config({
            "workspace_url": args.host if args.host.startswith("http") else f"https://{args.host}",
            "auth_method": "pat",
            "pat_token": args.token,
            "refresh_schedule": "manual",
            "setup_complete": "true",
        })
        log.info("Config saved from CLI arguments.")

    # If setup is complete and we have config but no data, trigger initial refresh
    cfg_startup = get_config()
    if cfg_startup.get("setup_complete") == "true":
        can_collect = has_config() or IS_DATABRICKS_APP
        if can_collect and not get_latest_snapshot():
            log.info("No cached data found. Triggering initial data collection...")
            sched.trigger_manual_refresh()
    else:
        log.info("Setup not complete. Waiting for admin configuration...")

    # Restore schedule from config
    if has_config():
        cfg = get_config()
        sched.configure_schedule(
            cfg.get("refresh_schedule", "manual"),
            int(cfg.get("refresh_hour", 6)),
        )

    # Determine port
    port = args.port or int(os.environ.get("DATABRICKS_APP_PORT", 8050))

    # Open browser in local mode
    if not IS_DATABRICKS_APP and not args.no_browser:
        import webbrowser
        import threading
        threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{port}")).start()

    log.info("Starting LHO Lite on port %d (Databricks App: %s)", port, IS_DATABRICKS_APP)
    flask_app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    main()
