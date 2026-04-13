from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
STATE_FILE = DATA_DIR / "daemon_state.json"
PID_FILE = DATA_DIR / "daemon_pid.json"
STOP_FILE = DATA_DIR / "daemon_stop.flag"
OUTPUT_CSV = DATA_DIR / "competitors_latest.csv"

STRICT_DELAY_SECONDS = 5

DEFAULT_COMPETITORS: List[Dict[str, str]] = [
    {"name": "سعيد صلاح", "store_url": "https://saeedsalah.com/", "sitemap_url": "https://saeedsalah.com/sitemap.xml"},
    {"name": "فانيلا", "store_url": "https://vanilla.sa/", "sitemap_url": "https://vanilla.sa/sitemap.xml"},
    {"name": "سارا ميكب", "store_url": "https://sara-makeup.com/", "sitemap_url": "https://sara-makeup.com/sitemap.xml"},
    {"name": "خبير العطور", "store_url": "https://alkhabeershop.com/", "sitemap_url": "https://alkhabeershop.com/sitemap.xml"},
    {"name": "قولدن سنت", "store_url": "https://www.goldenscent.com/", "sitemap_url": "https://www.goldenscent.com/sitemap.xml"},
    {"name": "لي سانتو", "store_url": "https://leesanto.com/", "sitemap_url": "https://leesanto.com/sitemap.xml"},
    {"name": "آزال", "store_url": "https://azalperfume.com/", "sitemap_url": "https://azalperfume.com/sitemap.xml"},
    {"name": "كاندي نيش", "store_url": "https://candyniche.com/", "sitemap_url": "https://candyniche.com/sitemap.xml"},
    {"name": "الفاخرة للنيش", "store_url": "https://luxuryperfumesnish.com/", "sitemap_url": "https://luxuryperfumesnish.com/sitemap.xml"},
    {"name": "حنان العطور", "store_url": "https://hanan-store55.com/", "sitemap_url": "https://hanan-store55.com/sitemap.xml"},
    {"name": "اريج امواج", "store_url": "https://areejamwaj.com/", "sitemap_url": "https://areejamwaj.com/sitemap.xml"},
    {"name": "نايس ون", "store_url": "https://niceonesa.com/", "sitemap_url": "https://niceonesa.com/sitemap.xml"},
    {"name": "سيفورا", "store_url": "https://www.sephora.me/sa-ar", "sitemap_url": "https://www.sephora.me/sa-ar/sitemap.xml"},
    {"name": "وجوه", "store_url": "https://www.faces.sa/ar", "sitemap_url": "https://www.faces.sa/ar/sitemap.xml"},
    {"name": "نيش", "store_url": "https://niche.sa/", "sitemap_url": "https://niche.sa/sitemap.xml"},
    {"name": "عالم جيفنشي", "store_url": "https://worldgivenchy.com/", "sitemap_url": "https://worldgivenchy.com/sitemap.xml"},
    {"name": "ساره ستور", "store_url": "https://sarahmakeup37.com/", "sitemap_url": "https://sarahmakeup37.com/sitemap.xml"},
    {"name": "اروماتيك كلاود", "store_url": "https://aromaticcloud.com/", "sitemap_url": "https://aromaticcloud.com/sitemap.xml"},
]


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    _ensure_data_dir()
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _read_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, dict):
                return loaded
    except Exception:
        pass
    return default


def read_state() -> Dict[str, Any]:
    return _read_json(
        STATE_FILE,
        {
            "running": False,
            "status": "idle",
            "message": "idle",
            "updated_at": _now_iso(),
            "started_at": "",
            "finished_at": "",
            "pid": None,
            "stores_total": len(DEFAULT_COMPETITORS),
            "stores_done": 0,
            "current_store": "",
            "current_store_index": 0,
            "products_total": 0,
            "products_done": 0,
            "success_count": 0,
            "error_count": 0,
            "last_error": "",
            "stores": [],
        },
    )


def _write_state(**updates: Any) -> None:
    state = read_state()
    state.update(updates)
    state["updated_at"] = _now_iso()
    _write_json(STATE_FILE, state)


def _build_initial_state(pid: int) -> Dict[str, Any]:
    stores = []
    for i, item in enumerate(DEFAULT_COMPETITORS, start=1):
        stores.append(
            {
                "index": i,
                "name": item["name"],
                "store_url": item["store_url"],
                "status": "pending",
                "products_total": 0,
                "products_done": 0,
                "success_count": 0,
                "error_count": 0,
                "last_error": "",
            }
        )
    return {
        "running": True,
        "status": "running",
        "message": "daemon started",
        "updated_at": _now_iso(),
        "started_at": _now_iso(),
        "finished_at": "",
        "pid": pid,
        "stores_total": len(DEFAULT_COMPETITORS),
        "stores_done": 0,
        "current_store": "",
        "current_store_index": 0,
        "products_total": 0,
        "products_done": 0,
        "success_count": 0,
        "error_count": 0,
        "last_error": "",
        "stores": stores,
    }


def _is_process_alive(pid: int) -> bool:
    try:
        if pid <= 0:
            return False
        if os.name == "nt":
            import ctypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid)
            if handle == 0:
                return False
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def get_daemon_pid() -> int:
    info = _read_json(PID_FILE, {})
    try:
        return int(info.get("pid", 0))
    except Exception:
        return 0


def is_daemon_running() -> bool:
    pid = get_daemon_pid()
    return _is_process_alive(pid)


def _resolve_product_urls_for_store(store_url: str, sitemap_url: str) -> List[str]:
    import aiohttp
    from engines.sitemap_resolve import resolve_product_entries

    async def _resolve() -> List[str]:
        timeout = aiohttp.ClientTimeout(total=45)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            entries = await resolve_product_entries(sitemap_url or store_url, session, max_products=0)
            if not entries:
                entries = await resolve_product_entries(store_url, session, max_products=0)
            return [e.url for e in entries if getattr(e, "url", "")]

    return asyncio.run(_resolve())


def _mark_store_state(state: Dict[str, Any], store_idx: int, **updates: Any) -> Dict[str, Any]:
    stores = list(state.get("stores", []))
    if 0 <= store_idx < len(stores):
        store = dict(stores[store_idx])
        store.update(updates)
        stores[store_idx] = store
        state["stores"] = stores
    return state


def _refresh_output_csv_from_db() -> None:
    """
    يحدّث ملف competitors_latest.csv تلقائياً من جدول التراكم في SQLite
    حتى يكون جاهزاً مباشرة لصفحة التحليل (Dashboard Auto Bridge).
    """
    try:
        import pandas as pd
        from utils.db_manager import get_all_competitor_products

        rows = get_all_competitor_products("")
        if not rows:
            return

        df = pd.DataFrame(rows)
        if df.empty:
            return

        rename_map = {
            "competitor": "store",
            "product_name": "name",
            "price": "price",
            "product_url": "product_url",
            "image_url": "image_url",
            "brand": "brand",
            "updated_at": "updated_at",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        _ensure_data_dir()
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    except Exception:
        # لا نوقف الكاشط إذا فشل التصدير — الكشط نفسه أهم.
        pass


def run_worker_loop() -> None:
    from utils.gemini_stealth_scraper import SmartAIScraper, save_results_to_db

    _ensure_data_dir()
    if STOP_FILE.exists():
        STOP_FILE.unlink(missing_ok=True)

    state = _build_initial_state(os.getpid())
    _write_json(STATE_FILE, state)
    _write_json(PID_FILE, {"pid": os.getpid(), "started_at": _now_iso()})

    # وضع صلب جداً: retries أعلى + timeout أعلى لتقليل الفشل لأدنى حد.
    scraper = SmartAIScraper(concurrency=1, delay_between=0.0, timeout=45, max_retries=6)

    try:
        for i, comp in enumerate(DEFAULT_COMPETITORS):
            if STOP_FILE.exists():
                state = read_state()
                state["running"] = False
                state["status"] = "stopped"
                state["message"] = "stop requested"
                state["finished_at"] = _now_iso()
                _write_json(STATE_FILE, state)
                return

            state = read_state()
            state["current_store"] = comp["name"]
            state["current_store_index"] = i + 1
            state["message"] = f"resolving sitemap for {comp['name']}"
            state = _mark_store_state(state, i, status="resolving")
            _write_json(STATE_FILE, state)

            try:
                urls = _resolve_product_urls_for_store(comp["store_url"], comp["sitemap_url"])
            except Exception as exc:
                state = read_state()
                state["error_count"] = int(state.get("error_count", 0)) + 1
                state["last_error"] = f"{comp['name']}: {str(exc)[:250]}"
                state["message"] = f"failed to resolve sitemap for {comp['name']}"
                state = _mark_store_state(
                    state, i, status="error", last_error=str(exc)[:250], products_total=0, products_done=0
                )
                _write_json(STATE_FILE, state)
                continue

            state = read_state()
            urls_total = len(urls)
            state["products_total"] = int(state.get("products_total", 0)) + urls_total
            state["message"] = f"جاري الكشط: {comp['name']} — {urls_total} رابط منتج"
            state = _mark_store_state(state, i, status="running", products_total=urls_total, products_done=0)
            _write_json(STATE_FILE, state)

            store_success = 0
            store_errors = 0
            for idx, url in enumerate(urls, start=1):
                if STOP_FILE.exists():
                    state = read_state()
                    state["running"] = False
                    state["status"] = "stopped"
                    state["message"] = "stop requested"
                    state["finished_at"] = _now_iso()
                    _write_json(STATE_FILE, state)
                    return

                try:
                    # Outer retries على مستوى الرابط بالكامل (بالإضافة إلى retries داخل SmartAIScraper).
                    final_res = None
                    for outer_try in range(1, 4):
                        res = scraper.scrape_url_sync(url)
                        final_res = res
                        if res.success:
                            break
                        if outer_try < 3:
                            time.sleep(7 * outer_try)

                    if final_res is not None:
                        save_results_to_db([final_res])
                        if final_res.success:
                            store_success += 1
                            # تصدير فوري لـ CSV حتى تظهر الأعداد الحية في لوحة التحكم
                            _refresh_output_csv_from_db()
                        else:
                            store_errors += 1
                            state = read_state()
                            state["last_error"] = f"{comp['name']}: {str(final_res.error)[:250]}"
                            _write_json(STATE_FILE, state)
                except Exception as exc:
                    store_errors += 1
                    state = read_state()
                    state["last_error"] = f"{comp['name']}: {str(exc)[:250]}"
                    _write_json(STATE_FILE, state)
                finally:
                    state = read_state()
                    state["products_done"] = int(state.get("products_done", 0)) + 1
                    state["success_count"] = int(state.get("success_count", 0))
                    state["error_count"] = int(state.get("error_count", 0))
                    state = _mark_store_state(
                        state,
                        i,
                        status="running",
                        products_done=idx,
                        success_count=store_success,
                        error_count=store_errors,
                    )
                    state["message"] = f"{idx}/{len(urls)} · {comp['name']}"
                    _write_json(STATE_FILE, state)
                    # نسخ احتياطي للملف كل 15 منتجًا (نجاح أو فشل) لتقليل التخلف عن قاعدة البيانات
                    if idx % 15 == 0:
                        _refresh_output_csv_from_db()
                    time.sleep(STRICT_DELAY_SECONDS)

            state = read_state()
            state["success_count"] = int(state.get("success_count", 0)) + store_success
            state["error_count"] = int(state.get("error_count", 0)) + store_errors
            state["stores_done"] = int(state.get("stores_done", 0)) + 1
            state["message"] = f"completed {comp['name']}"
            state = _mark_store_state(
                state,
                i,
                status="done",
                products_done=len(urls),
                success_count=store_success,
                error_count=store_errors,
            )
            _write_json(STATE_FILE, state)
            _refresh_output_csv_from_db()

        state = read_state()
        state["running"] = False
        state["status"] = "completed"
        state["message"] = "full auto-scraping daemon completed"
        state["finished_at"] = _now_iso()
        _write_json(STATE_FILE, state)
        _refresh_output_csv_from_db()
    except Exception as exc:
        state = read_state()
        state["running"] = False
        state["status"] = "error"
        state["message"] = f"daemon failed: {str(exc)[:250]}"
        state["last_error"] = str(exc)[:500]
        state["finished_at"] = _now_iso()
        _write_json(STATE_FILE, state)
    finally:
        STOP_FILE.unlink(missing_ok=True)


def start_daemon() -> Dict[str, Any]:
    _ensure_data_dir()
    if is_daemon_running():
        return {"ok": False, "message": "Daemon is already running."}

    STOP_FILE.unlink(missing_ok=True)

    cmd = [sys.executable, "-m", "utils.auto_daemon", "--run-worker"]
    kwargs: Dict[str, Any] = {
        "cwd": str(Path(__file__).resolve().parents[1]),
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "close_fds": True,
    }
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS

    proc = subprocess.Popen(cmd, **kwargs)
    _write_json(PID_FILE, {"pid": proc.pid, "started_at": _now_iso()})
    _write_state(
        running=True,
        status="starting",
        message="daemon process spawned",
        started_at=_now_iso(),
        pid=proc.pid,
    )
    return {"ok": True, "message": "Daemon started.", "pid": proc.pid}


def stop_daemon() -> Dict[str, Any]:
    _ensure_data_dir()
    STOP_FILE.write_text("stop", encoding="utf-8")
    _write_state(running=False, status="stopping", message="stop requested")
    return {"ok": True, "message": "Stop requested.", "pid": get_daemon_pid(), "signal_sent": False}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-worker", action="store_true")
    args = parser.parse_args()
    if args.run_worker:
        run_worker_loop()


if __name__ == "__main__":
    main()
