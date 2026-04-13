"""
scrapers/scheduler.py — جدولة الكشط التلقائي v2.0
═══════════════════════════════════════════════════
يشغّل async_scraper.py كعملية منفصلة وفق الجدول المضبوط،
مع منع التشغيلات المتداخلة والتحقق الفعلي من PID وتنظيف الحالات العالقة.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# ── مسارات ─────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
if os.environ.get("DATA_DIR"):
    _DATA_DIR = Path(os.environ["DATA_DIR"]).resolve()
else:
    _DATA_DIR = _ROOT / "data"

_STATE_FILE = _DATA_DIR / "scheduler_state.json"
_PROGRESS_FILE = _DATA_DIR / "scraper_progress.json"
_PID_FILE = _DATA_DIR / "scraper.pid"
_SCRAPER_SCRIPT = _ROOT / "scrapers" / "async_scraper.py"

# ── الافتراضيات ────────────────────────────────────────────────────────────────
DEFAULT_INTERVAL_HOURS = int(os.environ.get("SCRAPE_INTERVAL_HOURS", "12"))
_scheduler_thread: threading.Thread | None = None
_running = threading.Event()


def _default_state() -> dict:
    return {
        "enabled": False,
        "next_run": None,
        "interval_hours": DEFAULT_INTERVAL_HOURS,
        "last_run": None,
        "runs_count": 0,
        "max_products": 0,
        "concurrency": 8,
    }


def _load_state() -> dict:
    try:
        return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return _default_state()


def _save_state(state: dict) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    _STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_progress() -> dict:
    try:
        return json.loads(_PROGRESS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"running": False}


def _write_progress(data: dict) -> None:
    try:
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        _PROGRESS_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.debug("تعذر حفظ progress أثناء التنظيف: %s", exc)


def _fmt_duration(seconds: int) -> str:
    if seconds <= 0:
        return "الآن"
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}س {m}د"
    if m:
        return f"{m}د {s}ث"
    return f"{s}ث"


def _read_pid() -> int:
    try:
        if not _PID_FILE.exists():
            return 0
        raw = (_PID_FILE.read_text(encoding="utf-8") or "").strip()
        return int(raw) if raw.isdigit() else 0
    except Exception:
        return 0


def _is_process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _cleanup_stale_state() -> None:
    progress = _load_progress()
    progress["running"] = False
    progress["phase"] = "stopped"
    progress["finished_at"] = progress.get("finished_at") or datetime.now().isoformat()
    _write_progress(progress)
    try:
        if _PID_FILE.exists():
            _PID_FILE.unlink()
    except Exception as exc:
        logger.debug("تعذر حذف PID file: %s", exc)


def is_scraper_running() -> bool:
    progress = _load_progress()
    json_says_running = bool(progress.get("running", False))
    pid = _read_pid()
    alive = _is_process_alive(pid)

    if json_says_running and not alive:
        _cleanup_stale_state()
        return False

    return json_says_running and alive


def get_scheduler_status() -> dict:
    state = _load_state()
    now = datetime.utcnow()

    if state.get("next_run"):
        try:
            nxt = datetime.fromisoformat(state["next_run"])
            seconds = max(0, int((nxt - now).total_seconds()))
            state["remaining_seconds"] = seconds
            state["next_run_label"] = _fmt_duration(seconds)
        except Exception:
            state["remaining_seconds"] = 0
            state["next_run_label"] = "—"
    else:
        state["remaining_seconds"] = 0
        state["next_run_label"] = "—"

    state["scraper_running"] = is_scraper_running()
    return state


def enable_scheduler(interval_hours: int = DEFAULT_INTERVAL_HOURS) -> None:
    state = _load_state()
    state["enabled"] = True
    state["interval_hours"] = interval_hours
    state["next_run"] = (datetime.utcnow() + timedelta(hours=interval_hours)).isoformat()
    _save_state(state)
    logger.info("المجدول مُفعّل — كل %d ساعة", interval_hours)


def disable_scheduler() -> None:
    state = _load_state()
    state["enabled"] = False
    state["next_run"] = None
    _save_state(state)
    logger.info("المجدول مُعطّل")


def trigger_now(max_products: int = 0, concurrency: int = 8) -> bool:
    if not _SCRAPER_SCRIPT.exists():
        logger.error("الكاشط غير موجود: %s", _SCRAPER_SCRIPT)
        return False

    if is_scraper_running():
        logger.warning("تم رفض التشغيل لأن الكاشط يعمل بالفعل")
        return False

    try:
        subprocess.Popen(
            [
                sys.executable,
                str(_SCRAPER_SCRIPT),
                "--max-products",
                str(max_products),
                "--concurrency",
                str(concurrency),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )

        state = _load_state()
        state["last_run"] = datetime.utcnow().isoformat()
        state["runs_count"] = state.get("runs_count", 0) + 1
        state["max_products"] = max_products
        state["concurrency"] = concurrency
        interval = state.get("interval_hours", DEFAULT_INTERVAL_HOURS)
        state["next_run"] = (datetime.utcnow() + timedelta(hours=interval)).isoformat()
        _save_state(state)

        logger.info("انطلق الكاشط — التشغيل رقم %d", state["runs_count"])
        return True
    except Exception as exc:
        logger.error("فشل تشغيل الكاشط: %s", exc)
        return False


def _scheduler_loop() -> None:
    logger.info("خيط المجدول بدأ")
    while _running.is_set():
        try:
            state = _load_state()
            if state.get("enabled") and state.get("next_run"):
                next_run = datetime.fromisoformat(state["next_run"])
                if datetime.utcnow() >= next_run:
                    if is_scraper_running():
                        logger.info("تم تخطي تشغيل تلقائي لأن الكاشط ما زال يعمل")
                        interval = state.get("interval_hours", DEFAULT_INTERVAL_HOURS)
                        state["next_run"] = (datetime.utcnow() + timedelta(hours=interval)).isoformat()
                        _save_state(state)
                    else:
                        logger.info("حان وقت الكشط التلقائي — جاري التشغيل")
                        trigger_now(
                            max_products=state.get("max_products", 0),
                            concurrency=state.get("concurrency", 8),
                        )
        except Exception as exc:
            logger.debug("scheduler loop error: %s", exc)

        _running.wait(timeout=60)


def start_scheduler_thread() -> None:
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _running.set()
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        name="scraper-scheduler",
        daemon=True,
    )
    _scheduler_thread.start()
    logger.info("خيط المجدول بدأ (daemon)")


def stop_scheduler_thread() -> None:
    _running.clear()
