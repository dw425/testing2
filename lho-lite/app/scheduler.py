"""
Background refresh scheduler for LHO Lite.

Uses APScheduler BackgroundScheduler with configurable interval/cron triggers.
Thread-safe with a lock to prevent concurrent refreshes.
"""

import logging
import threading
import time

from apscheduler.schedulers.background import BackgroundScheduler

log = logging.getLogger("lho.scheduler")

_scheduler: BackgroundScheduler | None = None
_lock = threading.Lock()
_is_refreshing = False
_last_error: str = ""
_refresh_func = None  # Set by main.py


def init_scheduler(refresh_func):
    """Initialize the scheduler with a refresh callback."""
    global _scheduler, _refresh_func
    _refresh_func = refresh_func
    _scheduler = BackgroundScheduler(daemon=True)
    _scheduler.start()
    log.info("Scheduler initialized.")


def configure_schedule(schedule: str, hour: int = 6):
    """Configure the refresh schedule.

    Parameters
    ----------
    schedule : str
        ``"manual"`` | ``"hourly"`` | ``"daily"`` | ``"weekly"``
    hour : int
        UTC hour for daily/weekly (0-23)
    """
    if not _scheduler:
        return

    # Remove existing job if any
    if _scheduler.get_job("refresh"):
        _scheduler.remove_job("refresh")

    if schedule == "manual":
        log.info("Schedule set to manual — no automatic refresh.")
        return

    if schedule == "hourly":
        _scheduler.add_job(
            _run_refresh, "interval", hours=1, id="refresh",
            replace_existing=True, max_instances=1,
        )
        log.info("Schedule set to hourly.")

    elif schedule == "daily":
        _scheduler.add_job(
            _run_refresh, "cron", hour=hour, id="refresh",
            replace_existing=True, max_instances=1,
        )
        log.info("Schedule set to daily at %02d:00 UTC.", hour)

    elif schedule == "weekly":
        _scheduler.add_job(
            _run_refresh, "cron", day_of_week="mon", hour=hour, id="refresh",
            replace_existing=True, max_instances=1,
        )
        log.info("Schedule set to weekly (Monday) at %02d:00 UTC.", hour)


def _run_refresh():
    """Execute the refresh callback under the lock."""
    global _is_refreshing, _last_error
    if not _refresh_func:
        return
    if not _lock.acquire(blocking=False):
        log.warning("Refresh already in progress, skipping.")
        return
    try:
        _is_refreshing = True
        _last_error = ""
        log.info("Scheduled refresh starting...")
        _refresh_func()
        log.info("Scheduled refresh completed.")
    except Exception as e:
        _last_error = str(e)
        log.error("Scheduled refresh failed: %s", e)
    finally:
        _is_refreshing = False
        _lock.release()


def trigger_manual_refresh():
    """Trigger a one-off refresh in a background thread.

    Returns True if a refresh was started, False if one is already running.
    """
    global _is_refreshing
    if _is_refreshing:
        return False
    t = threading.Thread(target=_run_refresh, daemon=True)
    t.start()
    return True


def get_status() -> dict:
    """Return scheduler status for the /api/status endpoint."""
    next_run = None
    if _scheduler:
        job = _scheduler.get_job("refresh")
        if job and job.next_run_time:
            next_run = job.next_run_time.isoformat()

    return {
        "is_refreshing": _is_refreshing,
        "last_error": _last_error,
        "next_scheduled": next_run,
    }


def shutdown():
    """Gracefully stop the scheduler."""
    if _scheduler:
        _scheduler.shutdown(wait=False)
