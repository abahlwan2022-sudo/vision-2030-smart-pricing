"""
pages/scraper_advanced.py — لوحة كشط مهووس v5.0 (Unified Scraping Hub)
══════════════════════════════════════════════════════════════════════════
⚡ تبويب 1 — الكشط السريع (التقليدي):
   ▸ عرض فوري للمنتجات أثناء الكشط (Real-time streaming إلى SQLite)
   ▸ بطاقات منافسين احترافية مع شريط تقدم حي
   ▸ إدارة كاملة: إضافة / حذف / إعادة ضبط / تخطي

🤖 تبويب 2 — الكشط الذكي AI (تخطي الحظر):
   ▸ ZenRows (اختياري — ZENROWS_API_KEY) أولاً عند الضبط
   ▸ Crawl4AI + Playwright stealth
   ▸ curl_cffi Chrome120 TLS fingerprint
   ▸ cloudscraper Cloudflare bypass
   ▸ Gemini AI → {"title", "price", "stock_status"}
   ▸ حفظ تلقائي في DB عبر upsert_competitor_products
"""
from __future__ import annotations

import io
import json
import os
import threading
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from utils.background_agent import is_daemon_running, read_state, start_daemon, stop_daemon

# ── مسارات ─────────────────────────────────────────────────────────────────
_DATA_DIR         = os.environ.get("DATA_DIR", "data")
_COMPETITORS_FILE = os.path.join(_DATA_DIR, "competitors_list.json")
_PROGRESS_FILE    = os.path.join(_DATA_DIR, "scraper_progress.json")
_STATE_FILE       = os.path.join(_DATA_DIR, "scraper_state.json")
_OUTPUT_CSV       = os.path.join(_DATA_DIR, "competitors_latest.csv")

os.makedirs(_DATA_DIR, exist_ok=True)

_STATE_LOCK  = threading.Lock()
_RESULT_LOCK = threading.Lock()
_DAEMON_STATE_FILE = os.path.join(_DATA_DIR, "daemon_state.json")

# ════════════════════════════════════════════════════════════════════════════
#  CSS موحَّد للتبويبَين
# ════════════════════════════════════════════════════════════════════════════
_CSS = """
<style>
.sc-card{background:linear-gradient(135deg,#0d1b2a,#0a1520);border:1.5px solid #1e3a5f;
         border-radius:12px;padding:16px 18px 12px;margin-bottom:10px;
         transition:border-color .3s,box-shadow .3s;}
.sc-card:hover{box-shadow:0 4px 18px rgba(79,195,247,.12);}
.sc-card.done   {border-color:#00C853;}
.sc-card.error  {border-color:#FF1744;}
.sc-card.running{border-color:#4fc3f7;animation:pulse 2s infinite;}
.sc-card.pending{border-color:#37474f;}
.sc-card.skipped{border-color:#FFA000;}
@keyframes pulse{0%,100%{box-shadow:none}50%{box-shadow:0 0 14px rgba(79,195,247,.35)}}
.sc-badge{display:inline-flex;align-items:center;gap:4px;padding:3px 12px;
          border-radius:20px;font-size:.72rem;font-weight:700;}
.done-b  {background:rgba(0,200,83,.15);color:#00C853;border:1px solid #00C853;}
.error-b {background:rgba(255,23,68,.15);color:#FF1744;border:1px solid #FF1744;}
.run-b   {background:rgba(79,195,247,.18);color:#4fc3f7;border:1px solid #4fc3f7;}
.pend-b  {background:rgba(96,125,139,.15);color:#90a4ae;border:1px solid #37474f;}
.skip-b  {background:rgba(255,160,0,.15);color:#FFA000;border:1px solid #FFA000;}
.sc-bar-bg{background:#0a1520;border-radius:6px;height:8px;overflow:hidden;margin-top:6px;}
.sc-bar-fill{height:100%;background:linear-gradient(90deg,#4fc3f7,#0091ea);
             border-radius:6px;transition:width .4s ease;}
.sc-meta{font-size:.75rem;color:#78909c;display:flex;gap:12px;flex-wrap:wrap;margin-top:5px;}
.sc-kpi{background:#0d1b2a;border:1px solid #1e3a5f;border-radius:10px;
        padding:12px 16px;text-align:center;flex:1;min-width:100px;}
.sc-kpi .num{font-size:1.8rem;font-weight:900;color:#4fc3f7;}
.sc-kpi .lbl{font-size:.75rem;color:#607d8b;margin-top:2px;}
.live-count{font-size:2.5rem;font-weight:900;color:#00C853;text-align:center;line-height:1;}
.sai-header{background:linear-gradient(135deg,#0d1b2a 0%,#0a1520 100%);
            border:1.5px solid #1e3a5f;border-radius:14px;padding:18px 22px 14px;margin-bottom:16px;}
.sai-result-card{background:linear-gradient(135deg,#0a1628,#0e1a30);border:1px solid #1e3a5f;
                 border-radius:10px;padding:11px 15px;margin:4px 0;font-size:.85rem;}
.sai-result-card.success{border-left:4px solid #00C853;}
.sai-result-card.failed {border-left:4px solid #FF1744;}
.sai-badge{display:inline-block;padding:2px 10px;border-radius:12px;font-size:.7rem;font-weight:700;}
.badge-zenrows     {background:#26a69a22;color:#26a69a;border:1px solid #26a69a;}
.badge-crawl4ai    {background:#4fc3f722;color:#4fc3f7;border:1px solid #4fc3f7;}
.badge-curl_cffi   {background:#ff980022;color:#ff9800;border:1px solid #ff9800;}
.badge-cloudscraper{background:#ab47bc22;color:#ce93d8;border:1px solid #ab47bc;}
.badge-requests    {background:#78909c22;color:#90a4ae;border:1px solid #78909c;}
.badge-none        {background:#FF174422;color:#ef9a9a;border:1px solid #FF1744;}
.stock-in {color:#00C853;font-weight:700;}
.stock-out{color:#FF1744;font-weight:700;}
.stock-unk{color:#9e9e9e;}
</style>
"""


# ════════════════════════════════════════════════════════════════════════════
#  دوال مساعدة — الكشط التقليدي
# ════════════════════════════════════════════════════════════════════════════
def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "").strip() or url
    except Exception:
        return url


def _effective_concurrency() -> int:
    try:
        return int(st.session_state.get("sc_concurrency_adv", 6))
    except Exception:
        return 6


def _load_stores() -> list:
    try:
        with open(_COMPETITORS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _save_stores(lst: list) -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)
    with _STATE_LOCK:
        with open(_COMPETITORS_FILE, "w", encoding="utf-8") as f:
            json.dump(lst, f, ensure_ascii=False, indent=2)


def _load_progress() -> dict:
    try:
        with open(_PROGRESS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"running": False}


def _load_state() -> dict:
    try:
        with _STATE_LOCK:
            with open(_STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return {}


def _save_state(s: dict) -> None:
    with _STATE_LOCK:
        with open(_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(s, f, ensure_ascii=False, indent=2)


def _live_path(domain: str) -> str:
    return os.path.join(_DATA_DIR, f"_sc_live_{domain}.json")


def _read_live(domain: str) -> dict:
    try:
        with open(_live_path(domain), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _result_path(domain: str) -> str:
    return os.path.join(_DATA_DIR, f"_sc_result_{domain}.json")


def _read_result(domain: str):
    try:
        with _RESULT_LOCK:
            with open(_result_path(domain), encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return None


def _write_result(domain: str, data: dict) -> None:
    with _RESULT_LOCK:
        try:
            with open(_result_path(domain), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception:
            pass


def _reset_store(domain: str) -> None:
    state = _load_state()
    if domain in state:
        state[domain].update({
            "status": "pending", "last_url_index": 0,
            "last_page": 0, "urls_done": 0, "error": "", "finished_at": "",
        })
        _save_state(state)
    for p in [_live_path(domain), _result_path(domain)]:
        try:
            os.remove(p)
        except Exception:
            pass


def _get_db_count(domain: str) -> int:
    try:
        from utils.db_manager import get_competitor_products_df
        return len(get_competitor_products_df(domain))
    except Exception:
        return 0


def _get_all_db_products(domain: str = "", limit: int = 50) -> pd.DataFrame:
    try:
        from utils.db_manager import get_competitor_products_df
        df = get_competitor_products_df(domain)
        if df.empty:
            return pd.DataFrame()
        return df.tail(limit).iloc[::-1]
    except Exception:
        return pd.DataFrame()


def _total_db_products() -> dict:
    try:
        from utils.db_manager import get_competitor_store_stats
        return get_competitor_store_stats()
    except Exception:
        return {"total_products": 0, "by_competitor": {}}


def _run_store_bg(store_url: str, concurrency: int = 6,
                  max_products: int = 0, force: bool = False) -> None:
    domain = _domain(store_url)
    try:
        import sys
        sys.path.insert(0, ".")
        from engines.async_scraper import run_single_store
        result = run_single_store(store_url, concurrency=concurrency,
                                   max_products=max_products, force=force)
        _write_result(domain, result)
    except Exception as e:
        _write_result(domain, {"success": False, "rows": 0,
                                "message": str(e)[:300], "domain": domain})


def _launch_store(store_url: str, concurrency: int = 6,
                  max_products: int = 0, force: bool = False) -> None:
    domain = _domain(store_url)
    state  = _load_state()
    state[domain] = state.get(domain, {})
    state[domain].update({"status": "running", "store_url": store_url,
                           "domain": domain, "started_at": datetime.now().isoformat()})
    _save_state(state)
    t = threading.Thread(target=_run_store_bg,
                          args=(store_url, concurrency, max_products, force),
                          daemon=True, name=f"scraper-{domain}")
    t.start()
    if "sc_threads" not in st.session_state:
        st.session_state["sc_threads"] = {}
    st.session_state["sc_threads"][domain] = t


def _is_thread_alive(domain: str) -> bool:
    threads = st.session_state.get("sc_threads", {})
    t = threads.get(domain)
    return bool(t and t.is_alive())


def _feed_to_analysis(domain: str, label: str) -> None:
    try:
        df = _get_all_db_products(domain, limit=10000)
        if df.empty:
            try:
                csv_df = pd.read_csv(_OUTPUT_CSV, encoding="utf-8-sig", low_memory=False)
                df = csv_df[csv_df["store"].astype(str) == domain].copy()
            except Exception:
                pass
        if df.empty:
            st.warning(f"لا توجد منتجات مكشوطة من {label}")
            return
        rename_map = {"product_name": "المنتج", "name": "المنتج",
                      "price": "السعر", "image_url": "صورة_المنافس",
                      "product_url": "رابط_المنافس", "brand": "الماركة"}
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        if "المنتج" not in df.columns and "المنافس" not in df.columns:
            st.error("الأعمدة غير متطابقة")
            return
        df["المنافس"]       = domain
        df["منتج_المنافس"] = df.get("المنتج", df.get("name", ""))
        df["سعر_المنافس"]  = df.get("السعر", 0)
        existing = st.session_state.get("comp_dfs") or {}
        existing[label] = df
        st.session_state["comp_dfs"]          = existing
        st.session_state["_scraper_fed_comp"] = domain
        st.success(f"✅ {len(df):,} منتج من {label} جاهز للتحليل — اذهب للوحة التحكم")
    except Exception as e:
        st.error(f"خطأ في الإرسال: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  دوال مساعدة — كشط AI الذكي
# ════════════════════════════════════════════════════════════════════════════
def _check_api_key() -> bool:
    try:
        from config import GEMINI_API_KEYS, ANY_AI_PROVIDER_CONFIGURED
        return bool(GEMINI_API_KEYS) or ANY_AI_PROVIDER_CONFIGURED
    except ImportError:
        return bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEYS"))


def _parse_urls_from_text(text: str) -> List[str]:
    raw   = text.replace(",", "\n").replace(";", "\n")
    lines = [l.strip() for l in raw.splitlines()]
    urls: List[str] = []
    for line in lines:
        line = line.strip().strip('"\'\"').strip("'")
        if not line:
            continue
        if not line.startswith("http"):
            line = "https://" + line
        if "." in line:
            urls.append(line)
    return list(dict.fromkeys(urls))


def _parse_urls_from_csv(uploaded_file) -> List[str]:
    try:
        df = pd.read_csv(io.BytesIO(uploaded_file.read()), encoding="utf-8-sig")
        uploaded_file.seek(0)
        url_col = None
        for col in df.columns:
            if any(k in str(col).lower() for k in ["url", "رابط", "link", "href"]):
                url_col = col
                break
        if url_col is None:
            url_col = df.columns[0]
        return _parse_urls_from_text("\n".join(df[url_col].dropna().astype(str).tolist()))
    except Exception as e:
        st.error(f"خطأ في قراءة CSV: {e}")
        return []


def _method_badge(method: str) -> str:
    cls = {"zenrows": "badge-zenrows", "crawl4ai": "badge-crawl4ai", "curl_cffi": "badge-curl_cffi",
           "cloudscraper": "badge-cloudscraper", "requests": "badge-requests"}.get(method, "badge-none")
    return f'<span class="sai-badge {cls}">{method or "none"}</span>'


def _stock_html(status: str) -> str:
    if status == "in_stock":
        return '<span class="stock-in">✅ متوفر</span>'
    if status == "out_of_stock":
        return '<span class="stock-out">❌ غير متوفر</span>'
    return '<span class="stock-unk">❓ غير محدد</span>'


def _run_ai_scraping_job(urls: List[str], concurrency: int,
                          delay: float, session_key: str) -> None:
    """يُشغَّل في daemon thread — يحدّث session_state بعد كل نتيجة."""
    try:
        from utils.gemini_stealth_scraper import SmartAIScraper, ScrapeResult
    except ImportError as e:
        st.session_state[f"{session_key}_status"]  = f"error: {e}"
        st.session_state[f"{session_key}_running"] = False
        return

    scraper = SmartAIScraper(concurrency=concurrency, delay_between=delay)
    results: List[Any] = []

    def on_progress(done: int, total: int, result: Any) -> None:
        results.append(result)
        st.session_state[f"{session_key}_done"]    = done
        st.session_state[f"{session_key}_total"]   = total
        st.session_state[f"{session_key}_results"] = [r.to_dict() for r in results]

    try:
        final = scraper.scrape_batch_sync(
            urls=urls, concurrency=concurrency,
            delay=delay, progress_cb=on_progress,
        )
        st.session_state[f"{session_key}_results"] = [r.to_dict() for r in final]
        st.session_state[f"{session_key}_status"]  = "done"
    except Exception as e:
        st.session_state[f"{session_key}_status"]  = f"error: {e}"
    finally:
        st.session_state[f"{session_key}_running"] = False


def _render_full_auto_daemon_ui() -> None:
    st.markdown("## 🤖 One-Click Automated AI Scraping Daemon")
    st.caption("Runs in a detached background process and keeps scraping even if this page reruns.")

    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=2500, key="full_auto_daemon_refresh")
    except Exception:
        pass

    state = read_state()
    running = bool(state.get("running")) or is_daemon_running()

    c1, c2 = st.columns(2)
    with c1:
        if st.button("▶️ Start Full Auto-Scraping Daemon", type="primary", use_container_width=True):
            result = start_daemon()
            if result.get("ok"):
                st.success(f"Daemon started (PID: {result.get('pid')})")
            else:
                st.warning(result.get("message", "Unable to start daemon."))
            st.rerun()
    with c2:
        if st.button("⏹️ Stop", use_container_width=True):
            result = stop_daemon()
            st.warning(result.get("message", "Stop requested."))
            st.rerun()

    st.markdown("---")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Status", "🟢 Running" if running else "⚫ Idle")
    k2.metric("Stores", f"{int(state.get('stores_done', 0))}/{int(state.get('stores_total', 18))}")
    k3.metric("Products", f"{int(state.get('products_done', 0))}/{int(state.get('products_total', 0))}")
    k4.metric("Success / Errors", f"{int(state.get('success_count', 0))} / {int(state.get('error_count', 0))}")

    try:
        from config import ZENROWS_API_KEY

        if (ZENROWS_API_KEY or "").strip():
            st.success(
                "🛡️ **ZenRows مفعّل** — الكشط الذكي (الدايمون) والمكشط التقليدي يجرّبان وكيل ZenRows أولاً "
                "للصفحات المحمية (مثل سلة + Cloudflare). المفتاح من `ZENROWS_API_KEY`."
            )
        else:
            st.caption(
                "💡 متاجر سلة/Cloudflare: عيّن `ZENROWS_API_KEY` في البيئة أو Streamlit secrets لتفعيل ZenRows تلقائياً."
            )
    except Exception:
        pass

    # سطر واحد بدون تكرار اسم المتجر (كان يظهر في current_store وفي message معًا)
    st.info(state.get("message") or "—")
    st.caption(f"آخر تحديث: {state.get('updated_at') or '—'}")

    if state.get("last_error"):
        st.error(f"Last error: {state.get('last_error')}")

    stores = state.get("stores", [])
    if stores:
        rows = []
        for item in stores:
            rows.append(
                {
                    "المتجر": item.get("name", ""),
                    "الحالة": item.get("status", "pending"),
                    "المنتجات": f"{int(item.get('products_done', 0))}/{int(item.get('products_total', 0))}",
                    "ناجحة": int(item.get("success_count", 0)),
                    "فاشلة": int(item.get("error_count", 0)),
                    "آخر خطأ": item.get("last_error", ""),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=420)

    if os.path.exists(_DAEMON_STATE_FILE):
        with st.expander("Raw daemon state (JSON)"):
            try:
                with open(_DAEMON_STATE_FILE, encoding="utf-8") as f:
                    st.code(f.read(), language="json")
            except Exception as ex:
                st.error(str(ex))


# ════════════════════════════════════════════════════════════════════════════
#  الواجهة الرئيسية الموحَّدة
# ════════════════════════════════════════════════════════════════════════════
def show(embedded: bool = False) -> None:
    _render_full_auto_daemon_ui()
    return

    st.markdown(_CSS, unsafe_allow_html=True)

    # ── Auto-Refresh ──────────────────────────────────────────────────────
    state        = _load_state()
    _any_running = any(v.get("status") == "running" and _is_thread_alive(k)
                       for k, v in state.items())
    _ai_running  = bool(st.session_state.get("sai_running"))

    if _any_running or _ai_running:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=2500, key="sc_unified_refresh")
        except ImportError:
            pass

    st.markdown("## 🕷️ كاشط المنافسين — لوحة التحكم")

    # ── KPIs العلوية ──────────────────────────────────────────────────────
    stats             = _total_db_products()
    total_prods       = stats.get("total_products", 0)
    by_comp           = stats.get("by_competitor", {})
    total_comps       = len(by_comp)
    progress          = _load_progress()
    is_global_running = bool(progress.get("running")) or _any_running

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(f'<div class="sc-kpi"><div class="num">{total_prods:,}</div>'
                    f'<div class="lbl">إجمالي المنتجات المكشوطة</div></div>', unsafe_allow_html=True)
    with k2:
        st.markdown(f'<div class="sc-kpi"><div class="num">{total_comps}</div>'
                    f'<div class="lbl">منافسين في قاعدة البيانات</div></div>', unsafe_allow_html=True)
    with k3:
        _done_c = sum(1 for v in state.values() if v.get("status") == "done")
        st.markdown(f'<div class="sc-kpi"><div class="num" style="color:#00C853">{_done_c}</div>'
                    f'<div class="lbl">متاجر مكتملة الكشط</div></div>', unsafe_allow_html=True)
    with k4:
        _run_icon = "🟢 يعمل" if (is_global_running or _ai_running) else "⚫ متوقف"
        st.markdown(f'<div class="sc-kpi"><div class="num" style="font-size:1.3rem">{_run_icon}</div>'
                    f'<div class="lbl">حالة الكشط</div></div>', unsafe_allow_html=True)

    st.write("")

    # ════════════════════════════════════════════════════════════════════════
    #  التبويبان الرئيسيان
    # ════════════════════════════════════════════════════════════════════════
    tab_standard, tab_ai_stealth = st.tabs([
        "⚡ الكشط السريع (التقليدي)",
        "🤖 الكشط الذكي AI (تخطي الحظر)",
    ])

    # ════════════════════════════════════════════════════════════════════════
    #  تبويب 1 — الكشط التقليدي
    # ════════════════════════════════════════════════════════════════════════
    with tab_standard:
        sub_main, sub_add, sub_live, sub_settings = st.tabs([
            "🏪 إدارة المنافسين", "➕ إضافة منافس", "📡 بث مباشر", "⚙️ الإعدادات",
        ])

        with sub_main:
            stores = _load_stores()
            if not stores:
                st.info("لم تُضف أي متجر منافس بعد. اذهب لتبويب «إضافة منافس».")
            else:
                ba, bb, _ = st.columns([2, 2, 4])
                with ba:
                    if st.button("▶️ كشط كل المنافسين", type="primary", use_container_width=True):
                        for s in stores:
                            d = _domain(s)
                            if not _is_thread_alive(d):
                                _launch_store(s, concurrency=_effective_concurrency())
                        st.success("✅ بدأ الكشط لكل المتاجر")
                        st.rerun()
                with bb:
                    if st.button("⏹️ إيقاف الكل", use_container_width=True):
                        try:
                            prog = _load_progress()
                            prog["running"] = False
                            with open(_PROGRESS_FILE, "w", encoding="utf-8") as f:
                                json.dump(prog, f)
                        except Exception:
                            pass
                        st.warning("تم طلب الإيقاف — الخيوط الجارية ستكتمل دورتها الحالية")

                st.markdown("---")
                state = _load_state()

                for store_url in stores:
                    domain = _domain(store_url)
                    cp     = state.get(domain, {})
                    status = cp.get("status", "pending")

                    if status == "running" and not _is_thread_alive(domain):
                        result = _read_result(domain)
                        if result:
                            status = "done" if result.get("success") else "error"
                            cp["status"] = status
                            state[domain] = cp
                            _save_state(state)

                    live     = _read_live(domain)
                    db_count = _get_db_count(domain)

                    icon_map  = {"done": "✅", "error": "❌", "running": "⏳",
                                 "pending": "⏸️", "skipped": "⏭️"}
                    badge_map = {"done": "done-b", "error": "error-b", "running": "run-b",
                                 "pending": "pend-b", "skipped": "skip-b"}
                    sc_cls    = {"done": "done", "error": "error", "running": "running",
                                 "pending": "pending", "skipped": "skipped"}.get(status, "pending")

                    pct = 0
                    if status == "running" and live:
                        pct = max(0, min(100, int(live.get("pct", 0))))
                    elif status == "done":
                        pct = 100

                    rows_saved = db_count or cp.get("rows_saved", 0)
                    urls_done  = live.get("urls_done", cp.get("urls_done", 0)) if status == "running" else cp.get("urls_done", 0)
                    urls_total = live.get("urls_total", cp.get("urls_total", 0))
                    upd_at     = live.get("updated_at", cp.get("last_checkpoint_at", ""))

                    st.markdown(
                        f'<div class="sc-card {sc_cls}">'
                        f'<div style="display:flex;justify-content:space-between;align-items:center">'
                        f'<div><span style="font-weight:700;font-size:1rem">{icon_map.get(status,"❓")} {domain}</span>'
                        f'&nbsp;<span class="sc-badge {badge_map.get(status,"pend-b")}">{status}</span></div>'
                        f'<div style="font-size:.75rem;color:#4fc3f7">{"🔴 يعمل الآن" if status=="running" else ""}</div></div>'
                        f'<div class="sc-meta"><span>🛍️ {rows_saved:,} منتج محفوظ</span>'
                        + (f'<span>📶 {urls_done:,}/{urls_total:,} رابط</span>' if urls_total else "")
                        + (f'<span>🕐 {upd_at}</span>' if upd_at else "")
                        + f'</div>'
                        + (f'<div class="sc-bar-bg"><div class="sc-bar-fill" style="width:{pct}%"></div></div>' if pct > 0 else "")
                        + f'</div>',
                        unsafe_allow_html=True,
                    )

                    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 2])
                    _dis = status == "running" and _is_thread_alive(domain)

                    with c1:
                        if st.button("▶️ بدء" if status != "running" else "🔄 جاري...",
                                     key=f"run_{domain}", disabled=_dis, use_container_width=True):
                            _launch_store(store_url, concurrency=_effective_concurrency())
                            st.rerun()
                    with c2:
                        if st.button("🔁 إعادة", key=f"re_{domain}", disabled=_dis, use_container_width=True):
                            _reset_store(domain)
                            _launch_store(store_url, force=True, concurrency=_effective_concurrency())
                            st.rerun()
                    with c3:
                        if st.button("⏭️ تخطي", key=f"skip_{domain}", disabled=_dis, use_container_width=True):
                            _reset_store(domain)
                            ns = _load_state()
                            ns[domain] = {"status": "done", "domain": domain,
                                           "store_url": store_url, "rows_saved": 0, "error": "skipped"}
                            _save_state(ns)
                            st.rerun()
                    with c4:
                        if st.button("📊 للتحليل", key=f"feed_{domain}", use_container_width=True,
                                     help="أرسل منتجات هذا المنافس مباشرةً لنظام المقارنة"):
                            _feed_to_analysis(domain, domain)
                    with c5:
                        if st.button("🗑️ حذف", key=f"del_{domain}", use_container_width=True):
                            _save_stores([s for s in stores if _domain(s) != domain])
                            _reset_store(domain)
                            try:
                                from utils.db_manager import clear_competitor_store
                                clear_competitor_store(domain)
                            except Exception:
                                pass
                            st.success(f"حُذف {domain}")
                            st.rerun()

                    if status == "running" and rows_saved > 0:
                        with st.expander(f"📦 آخر المنتجات من {domain}", expanded=False):
                            _ldf = _get_all_db_products(domain, limit=10)
                            if not _ldf.empty:
                                sc = [c for c in ["product_name","price","brand","updated_at"] if c in _ldf.columns]
                                st.dataframe(_ldf[sc].head(10), use_container_width=True, height=230)

                    if status == "error":
                        err = cp.get("error", "")
                        res = _read_result(domain)
                        msg = (res or {}).get("message", err)
                        if msg and msg != "skipped":
                            if str(msg).strip().startswith("✅ 0 منتج"):
                                msg = "لم يتم استخراج منتجات جديدة (غالباً حظر/timeout/قيود الموقع)."
                            st.error(f"❌ {domain}: {str(msg)[:200]}")

        with sub_add:
            st.markdown("### ➕ إضافة متجر منافس جديد")
            with st.form("add_store_form", clear_on_submit=True):
                new_url    = st.text_input("🔗 رابط المتجر",
                    placeholder="https://example.com أو https://store.salla.sa/...",
                    help="يدعم: Shopify، سلة، Zid، WooCommerce")
                col_a, col_b = st.columns(2)
                with col_a:
                    start_now = st.checkbox("▶️ ابدأ الكشط فور الإضافة", value=True)
                with col_b:
                    max_p = st.number_input("حد المنتجات (0=كل)", min_value=0, step=100, value=0)
                submitted = st.form_submit_button("✅ إضافة", type="primary", use_container_width=True)
                if submitted and new_url:
                    new_url = new_url.strip().rstrip("/")
                    if not new_url.startswith("http"):
                        new_url = "https://" + new_url
                    domain = _domain(new_url)
                    stores = _load_stores()
                    if new_url in stores or any(_domain(s) == domain for s in stores):
                        st.warning(f"⚠️ {domain} موجود بالفعل في القائمة")
                    else:
                        stores.append(new_url)
                        _save_stores(stores)
                        if start_now:
                            _launch_store(new_url, concurrency=_effective_concurrency(),
                                          max_products=int(max_p))
                            st.success(f"✅ أُضيف {domain} وبدأ الكشط تلقائياً!")
                        else:
                            st.success(f"✅ أُضيف {domain} — اضغط ▶️ لبدء الكشط")
                        st.rerun()

            st.markdown("---")
            st.markdown("**🏪 قائمة المتاجر المضافة حالياً:**")
            stores = _load_stores()
            for s in stores:
                st.markdown(f"- `{s}`")
            if not stores:
                st.caption("لا توجد متاجر مضافة بعد.")
            st.markdown("---")
            st.markdown("#### 📋 استيراد قائمة متاجر (كل متجر في سطر)")
            bulk_text = st.text_area("أدخل روابط المتاجر", height=150,
                placeholder="https://store1.com\nhttps://store2.salla.sa")
            if st.button("📥 استيراد القائمة", use_container_width=True):
                urls = [u.strip() for u in bulk_text.strip().splitlines() if u.strip()]
                stores = _load_stores()
                added = 0
                for u in urls:
                    if not u.startswith("http"):
                        u = "https://" + u
                    if u not in stores:
                        stores.append(u)
                        added += 1
                _save_stores(stores)
                st.success(f"✅ أُضيف {added} متجر جديد من أصل {len(urls)}")
                st.rerun()

        with sub_live:
            st.markdown("### 📡 المنتجات المكشوطة حديثاً (Real-Time)")
            stores  = _load_stores()
            domains = [_domain(s) for s in stores] if stores else []
            filter_comp   = st.selectbox("عرض منتجات:", ["كل المنافسين"] + domains, key="live_filter_comp")
            filter_domain = "" if filter_comp == "كل المنافسين" else filter_comp
            db_count = _get_db_count(filter_domain) if filter_domain else total_prods
            st.markdown(
                f'<div style="text-align:center;padding:20px 0">'
                f'<div class="live-count">{db_count:,}</div>'
                f'<div style="color:#607d8b;font-size:.9rem;margin-top:6px">'
                f'منتج {"من " + filter_domain if filter_domain else "إجمالي"} في قاعدة البيانات</div></div>',
                unsafe_allow_html=True,
            )
            live_df = _get_all_db_products(filter_domain, limit=100)
            if not live_df.empty:
                col_map = {"competitor":"المنافس","product_name":"اسم المنتج",
                           "price":"السعر","brand":"الماركة","updated_at":"آخر تحديث"}
                live_display = live_df.rename(columns=col_map)
                show_cols    = [v for v in col_map.values() if v in live_display.columns]
                st.dataframe(live_display[show_cols], use_container_width=True, height=400)
                csv_b = live_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                st.download_button(f"📥 تصدير ({db_count:,} منتج) CSV", data=csv_b,
                    file_name=f"scraped_{filter_domain or 'all'}_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv")
            else:
                st.info("⏳ لا توجد بيانات بعد — ابدأ الكشط من تبويب «إدارة المنافسين»")

            if by_comp:
                st.markdown("---")
                st.markdown("#### 📊 توزيع المنتجات حسب المنافس")
                comp_df = pd.DataFrame(
                    sorted(by_comp.items(), key=lambda x: x[1], reverse=True),
                    columns=["المنافس", "عدد المنتجات"])
                st.dataframe(comp_df, use_container_width=True, height=250)
                st.markdown("---")
                if st.button("🚀 إرسال كل البيانات المكشوطة للتحليل", type="primary", use_container_width=True):
                    all_df = _get_all_db_products("", limit=100000)
                    if not all_df.empty and "competitor" in all_df.columns:
                        comp_dfs: Dict[str, pd.DataFrame] = {}
                        for comp, gdf in all_df.groupby("competitor"):
                            gdf2 = gdf.rename(columns={"product_name":"المنتج","price":"السعر",
                                "image_url":"صورة_المنافس","product_url":"رابط_المنافس"}).copy()
                            gdf2["المنافس"]       = comp
                            gdf2["منتج_المنافس"] = gdf2.get("المنتج", "")
                            gdf2["سعر_المنافس"]  = gdf2.get("السعر", 0)
                            comp_dfs[comp]        = gdf2
                        if comp_dfs:
                            st.session_state["comp_dfs"] = comp_dfs
                            st.success(f"✅ {len(all_df):,} منتج من {len(comp_dfs)} منافس جاهزة للتحليل")
                        else:
                            st.warning("لا يوجد عمود منافس في البيانات")

        with sub_settings:
            st.markdown("### ⚙️ إعدادات الكشط")
            st.slider("التزامن — عدد الطلبات المتزامنة",
                min_value=2, max_value=20, value=int(st.session_state.get("sc_concurrency_adv", 6)),
                step=1, key="sc_concurrency_adv",
                help="قيمة أقل = أبطأ لكن أأمن | قيمة أعلى = أسرع لكن خطر حجب")
            st.markdown("---")
            st.markdown("#### 🗓️ الجدولة التلقائية")
            try:
                from scrapers.scheduler import (get_scheduler_status, enable_scheduler,
                    disable_scheduler, trigger_now, start_scheduler_thread, DEFAULT_INTERVAL_HOURS)
                sched_status = get_scheduler_status()
                is_enabled   = bool(sched_status.get("enabled"))
                s1, s2 = st.columns(2)
                with s1:
                    interval = st.number_input("الجدول (ساعات)", min_value=1, max_value=168,
                        value=int(sched_status.get("interval_hours", DEFAULT_INTERVAL_HOURS)))
                with s2:
                    st.metric("آخر تشغيل", sched_status.get("last_run","—")[:16] if sched_status.get("last_run") else "—")
                    st.metric("التشغيل القادم", sched_status.get("next_run_label","—"))
                c_en, c_dis, c_now = st.columns(3)
                with c_en:
                    if st.button("✅ تفعيل الجدولة", disabled=is_enabled, use_container_width=True):
                        enable_scheduler(interval_hours=interval)
                        start_scheduler_thread()
                        st.success(f"✅ الجدولة كل {interval} ساعة")
                        st.rerun()
                with c_dis:
                    if st.button("⏹️ تعطيل الجدولة", disabled=not is_enabled, use_container_width=True):
                        disable_scheduler()
                        st.info("المجدول معطّل")
                        st.rerun()
                with c_now:
                    if st.button("⚡ كشط الآن (فوري)", use_container_width=True):
                        for s in _load_stores():
                            d = _domain(s)
                            if not _is_thread_alive(d):
                                _launch_store(s, concurrency=_effective_concurrency())
                        st.success("✅ بدأ الكشط الفوري")
                        st.rerun()
            except Exception as e:
                st.error(f"خطأ في الجدولة: {e}")

            st.markdown("---")
            st.markdown("#### 🧹 إدارة البيانات")
            col_x, col_y = st.columns(2)
            with col_x:
                if st.button("🔄 إعادة ضبط نقاط الاستئناف", use_container_width=True):
                    try:
                        if os.path.exists(_STATE_FILE):
                            os.remove(_STATE_FILE)
                        st.success("✅ تمت إعادة الضبط")
                    except Exception as ex:
                        st.error(str(ex))
            with col_y:
                if st.button("🗑️ مسح قاعدة بيانات الكشط", use_container_width=True):
                    try:
                        from utils.db_manager import clear_competitor_store
                        n = clear_competitor_store()
                        st.success(f"✅ حُذف {n} سجل")
                    except Exception as ex:
                        st.error(str(ex))

            st.markdown("---")
            st.markdown("#### 📁 ملفات البيانات")
            for fp, label in [
                (_COMPETITORS_FILE, "قائمة المنافسين"),
                (_PROGRESS_FILE,    "ملف التقدم"),
                (_STATE_FILE,       "ملف نقاط الاستئناف"),
                (_OUTPUT_CSV,       "ملف CSV المُدمج"),
            ]:
                size = ""
                if os.path.exists(fp):
                    try:
                        sz = os.path.getsize(fp)
                        size = f" ({sz//1024} KB)" if sz > 1024 else f" ({sz} B)"
                    except Exception:
                        pass
                st.caption(f"{'✅' if os.path.exists(fp) else '❌'} **{label}**: `{fp}`{size}")

    # ════════════════════════════════════════════════════════════════════════
    #  تبويب 2 — الكشط الذكي AI
    # ════════════════════════════════════════════════════════════════════════
    with tab_ai_stealth:
        for _k, _v in [("sai_running", False), ("sai_done", 0), ("sai_total", 0),
                        ("sai_results", []), ("sai_status", "idle"), ("sai_saved_to_db", False)]:
            st.session_state.setdefault(_k, _v)

        st.markdown("""
        <div class="sai-header">
            <h3 style="margin:0;color:#4fc3f7">🤖 الكشط الذكي — تخطي الحماية بـ AI</h3>
            <p style="margin:6px 0 0;color:#78909c;font-size:.85rem">
                أدخل روابط منتجات → 4 طبقات لتجاوز الحظر → Gemini يستخرج البيانات تلقائياً
            </p>
        </div>
        <div style="background:#0d1b2a;border:1px solid #1e3a5f;border-radius:8px;
                    padding:10px 14px;font-size:.8rem;color:#78909c;margin-bottom:14px">
            <b style="color:#4fc3f7">طبقات الكشط:</b>
            <span class="sai-badge badge-crawl4ai">Crawl4AI</span>&nbsp;JS كامل &nbsp;|&nbsp;
            <span class="sai-badge badge-curl_cffi">curl_cffi</span>&nbsp;TLS Chrome120 &nbsp;|&nbsp;
            <span class="sai-badge badge-cloudscraper">cloudscraper</span>&nbsp;Cloudflare &nbsp;|&nbsp;
            <span class="sai-badge badge-requests">requests</span>&nbsp;Fallback
        </div>
        """, unsafe_allow_html=True)

        if not _check_api_key():
            st.error("❌ **مفتاح Gemini API غير موجود** — أضف GEMINI_API_KEY كمتغير بيئة")
        else:
            st.markdown("#### 🔗 إدخال روابط المنتجات")
            ai_in1, ai_in2 = st.tabs(["📝 إدخال نصي", "📄 رفع CSV"])

            with ai_in1:
                url_text = st.text_area(
                    "روابط المنتجات (رابط في كل سطر أو بفاصلة)",
                    placeholder="https://store.salla.sa/products/item-xyz\nhttps://shop.example.com/p/123",
                    height=130, key="sai_url_text",
                    disabled=st.session_state.sai_running,
                )
                urls_from_text = _parse_urls_from_text(url_text or "")
                if urls_from_text:
                    st.caption(f"✅ تم التعرف على **{len(urls_from_text)}** رابط صحيح")

            with ai_in2:
                csv_file = st.file_uploader("ارفع ملف CSV يحتوي على عمود روابط",
                    type=["csv"], key="sai_csv_upload",
                    disabled=st.session_state.sai_running)
                urls_from_csv: List[str] = []
                if csv_file:
                    urls_from_csv = _parse_urls_from_csv(csv_file)
                    if urls_from_csv:
                        st.success(f"✅ تم استخراج **{len(urls_from_csv)}** رابط")

            all_ai_urls = list(dict.fromkeys(urls_from_text + urls_from_csv))

            with st.expander("⚙️ إعدادات الكشط الذكي", expanded=False):
                ai_c1, ai_c2, ai_c3 = st.columns(3)
                with ai_c1:
                    ai_concurrency = st.number_input("طلبات متزامنة",
                        min_value=1, max_value=8, value=3, step=1, key="sai_concurrency",
                        disabled=st.session_state.sai_running)
                with ai_c2:
                    ai_delay = st.number_input("تأخير (ثانية)",
                        min_value=0.0, max_value=10.0, value=1.0, step=0.5, key="sai_delay",
                        disabled=st.session_state.sai_running)
                with ai_c3:
                    ai_auto_save = st.checkbox("حفظ تلقائي في DB", value=True,
                        key="sai_auto_save", disabled=st.session_state.sai_running)

            st.markdown("---")
            btn_col, info_col = st.columns([2, 3])
            with btn_col:
                if not st.session_state.sai_running:
                    if st.button("🚀 بدء الكشط الذكي", type="primary",
                                 disabled=not bool(all_ai_urls),
                                 use_container_width=True, key="sai_start_btn"):
                        st.session_state.sai_running     = True
                        st.session_state.sai_done        = 0
                        st.session_state.sai_total       = len(all_ai_urls)
                        st.session_state.sai_results     = []
                        st.session_state.sai_status      = "running"
                        st.session_state.sai_saved_to_db = False
                        t = threading.Thread(
                            target=_run_ai_scraping_job,
                            args=(all_ai_urls, int(ai_concurrency), float(ai_delay), "sai"),
                            daemon=True, name="smart-ai-scraper")
                        t.start()
                        st.rerun()
                else:
                    if st.button("⏹️ إيقاف", use_container_width=True,
                                 type="secondary", key="sai_stop_btn"):
                        st.session_state.sai_running = False
                        st.session_state.sai_status  = "stopped"
                        st.warning("تم طلب الإيقاف — سيتوقف بعد الطلبات الجارية")
            with info_col:
                if all_ai_urls:
                    _pdoms = list({_domain(u) for u in all_ai_urls[:5]})
                    st.markdown(f"**{len(all_ai_urls)} رابط** من {'، '.join(_pdoms[:3])}"
                                + ("..." if len(_pdoms) > 3 else ""))
                elif not st.session_state.sai_running:
                    st.caption("👈 أدخل روابط المنتجات أعلاه لتبدأ")

            if st.session_state.sai_running or st.session_state.sai_status == "running":
                _ai_done  = int(st.session_state.sai_done)
                _ai_total = max(int(st.session_state.sai_total), 1)
                st.progress(min(_ai_done / _ai_total, 0.99),
                            text=f"⏳ معالجة {_ai_done} / {_ai_total} رابط...")
                _partial = [r for r in st.session_state.sai_results if r.get("success")]
                _failedl = [r for r in st.session_state.sai_results if not r.get("success")]
                mk1, mk2, mk3, mk4 = st.columns(4)
                mk1.metric("تم معالجتها", _ai_done)
                mk2.metric("✅ ناجحة",   len(_partial))
                mk3.metric("❌ فاشلة",   len(_failedl))
                mk4.metric("⏳ متبقية",  _ai_total - _ai_done)

            if st.session_state.sai_status == "done":
                _res = st.session_state.sai_results
                _sn  = sum(1 for r in _res if r.get("success"))
                st.success(f"✅ اكتمل! — {_sn} ناجح | {len(_res)-_sn} فاشل من أصل {len(_res)}")
                if st.session_state.get("sai_auto_save") and not st.session_state.sai_saved_to_db:
                    try:
                        from utils.gemini_stealth_scraper import save_results_to_db, ScrapeResult
                        sr_list = [ScrapeResult(**{k: d.get(k,"") for k in
                            ["url","title","price","stock_status","success","error",
                             "scrape_method","domain","raw_price_float","competitor","image_url"]})
                            for d in _res]
                        db_stats = save_results_to_db(sr_list)
                        st.session_state.sai_saved_to_db = True
                        st.info(f"💾 حُفظ: {db_stats['inserted']} جديد | {db_stats['updated']} محدَّث | {db_stats['failed']} فشل")
                    except Exception as _dbe:
                        st.warning(f"فشل الحفظ التلقائي: {_dbe}")
            elif st.session_state.sai_status.startswith("error:"):
                st.error(f"❌ {st.session_state.sai_status[6:]}")

            ai_results = st.session_state.sai_results
            if ai_results:
                st.markdown("---")
                st.markdown("#### 📊 النتائج")
                res_t1, res_t2, res_t3 = st.tabs(["📋 جدول", "🃏 بطاقات", "📤 تصدير وحفظ"])

                with res_t1:
                    df_rows = [{"الحالة": "✅" if r.get("success") else "❌",
                                "اسم المنتج": (r.get("title") or "")[:80],
                                "السعر (ر.س)": r.get("raw_price_float", 0.0),
                                "التوفر": {"in_stock":"✅ متوفر","out_of_stock":"❌ غير متوفر"}.get(r.get("stock_status","unknown"),"❓ غير محدد"),
                                "طريقة الكشط": r.get("scrape_method",""),
                                "الدومين": r.get("domain",""),
                                "الرابط": r.get("url","")} for r in ai_results]
                    df_res = pd.DataFrame(df_rows)
                    st.dataframe(df_res, use_container_width=True,
                                 height=min(550, 100 + len(df_res)*36),
                                 column_config={"الرابط": st.column_config.LinkColumn("الرابط"),
                                                "السعر (ر.س)": st.column_config.NumberColumn(format="%.2f")})

                with res_t2:
                    _cf = st.radio("عرض:", ["الكل","الناجحة فقط","الفاشلة فقط"],
                                   horizontal=True, key="sai_card_filter")
                    _fc = ai_results
                    if _cf == "الناجحة فقط":
                        _fc = [r for r in ai_results if r.get("success")]
                    elif _cf == "الفاشلة فقط":
                        _fc = [r for r in ai_results if not r.get("success")]
                    _PG = 20
                    _tp = max(1, (len(_fc)+_PG-1)//_PG)
                    _pn = st.number_input("صفحة", 1, _tp, 1, key="sai_card_pg") if _tp > 1 else 1
                    for r in _fc[(_pn-1)*_PG: _pn*_PG]:
                        _s = r.get("success", False)
                        _cl = "success" if _s else "failed"
                        _ti = r.get("title") or "—"
                        _pf = r.get("raw_price_float", 0.0)
                        _me = r.get("scrape_method","none")
                        _ul = r.get("url","")
                        _er = r.get("error","")
                        _do = r.get("domain","")
                        _sk = r.get("stock_status","unknown")
                        if _s:
                            st.markdown(
                                f'<div class="sai-result-card {_cl}">'
                                f'<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:10px">'
                                f'<div style="flex:1;min-width:0">'
                                f'<div style="font-weight:700;font-size:.9rem;color:#e0e0e0;margin-bottom:3px">{_ti[:100]}</div>'
                                f'<div style="display:flex;gap:14px;flex-wrap:wrap;font-size:.82rem">'
                                f'<span><b style="color:#ff9800">{_pf:,.2f} ر.س</b></span>{_stock_html(_sk)}'
                                f'<span style="color:#9e9e9e">{_do}</span></div></div>'
                                f'<div style="flex-shrink:0">{_method_badge(_me)}</div></div>'
                                f'<div style="margin-top:5px"><a href="{_ul}" target="_blank" rel="noopener noreferrer" '
                                f'style="color:#4fc3f7;font-size:.72rem">🔗 {_ul[:70]}{"..." if len(_ul)>70 else ""}</a></div></div>',
                                unsafe_allow_html=True)
                        else:
                            st.markdown(
                                f'<div class="sai-result-card {_cl}">'
                                f'<div style="display:flex;justify-content:space-between;align-items:center">'
                                f'<div><div style="font-size:.82rem;color:#ef9a9a;margin-bottom:2px">❌ {_er[:80]}</div>'
                                f'<div style="font-size:.72rem;color:#555">{_ul[:80]}</div></div>'
                                f'{_method_badge(_me)}</div></div>', unsafe_allow_html=True)
                    if not _fc:
                        st.info("لا توجد نتائج تطابق الفلتر المحدد")

                with res_t3:
                    st.markdown("#### 💾 حفظ في قاعدة البيانات")
                    if st.session_state.sai_saved_to_db:
                        st.success("✅ تم الحفظ تلقائياً في قاعدة البيانات")
                    else:
                        if st.button("💾 حفظ النتائج الناجحة في DB الآن",
                                     type="primary", use_container_width=True, key="sai_save_db_btn",
                                     disabled=st.session_state.sai_running):
                            try:
                                from utils.gemini_stealth_scraper import save_results_to_db, ScrapeResult
                                sr_list = [ScrapeResult(**{k: d.get(k,"") for k in
                                    ["url","title","price","stock_status","success","error",
                                     "scrape_method","domain","raw_price_float","competitor","image_url"]})
                                    for d in ai_results if d.get("success")]
                                db_stats = save_results_to_db(sr_list)
                                st.session_state.sai_saved_to_db = True
                                st.success(f"✅ {db_stats['inserted']} جديد | {db_stats['updated']} محدَّث | {db_stats['failed']} فشل")
                            except Exception as _e2:
                                st.error(f"فشل الحفظ: {_e2}")

                    st.markdown("---")
                    st.markdown("#### 📥 تصدير النتائج")
                    _exp_df = pd.DataFrame([{"url": r.get("url",""), "title": r.get("title",""),
                        "price": r.get("raw_price_float",0.0), "stock_status": r.get("stock_status","unknown"),
                        "success": r.get("success",False), "scrape_method": r.get("scrape_method",""),
                        "domain": r.get("domain",""), "error": r.get("error",""),
                        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M")} for r in ai_results])
                    _ec1, _ec2 = st.columns(2)
                    with _ec1:
                        st.download_button("📄 تنزيل CSV",
                            data=_exp_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                            file_name=f"ai_scraper_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                            mime="text/csv", use_container_width=True, key="sai_dl_csv")
                    with _ec2:
                        try:
                            _xb = io.BytesIO()
                            _exp_df.to_excel(_xb, index=False, engine="openpyxl")
                            _xb.seek(0)
                            st.download_button("📊 تنزيل Excel", data=_xb.read(),
                                file_name=f"ai_scraper_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True, key="sai_dl_xlsx")
                        except Exception:
                            st.caption("تثبيت openpyxl لتصدير Excel")

                    st.markdown("---")
                    if st.button("🔄 مسح النتائج وبدء جلسة جديدة",
                                 use_container_width=True, key="sai_reset_btn",
                                 disabled=st.session_state.sai_running):
                        for _ki, _vi in [("sai_running",False),("sai_done",0),("sai_total",0),
                                          ("sai_results",[]),("sai_status","idle"),("sai_saved_to_db",False)]:
                            st.session_state[_ki] = _vi
                        st.rerun()

            elif not st.session_state.sai_running and st.session_state.sai_status == "idle":
                st.info("💡 أدخل روابط المنتجات أعلاه واضغط «بدء الكشط الذكي»")
