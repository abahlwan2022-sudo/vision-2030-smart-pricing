"""
pages/scraper_advanced.py — لوحة كشط مهووس v4.0 (Real-Time Intelligence)
══════════════════════════════════════════════════════════════════════════
▸ عرض فوري للمنتجات أثناء الكشط (Real-time streaming إلى SQLite)
▸ بطاقات منافسين احترافية مع شريط تقدم حي
▸ إدارة كاملة: إضافة / حذف / إعادة ضبط / تخطي
▸ جدول حي يتحدث كل 3 ثوانٍ أثناء الكشط
▸ زر "إرسال للتحليل" — يُغذّي بيانات المنافس مباشرةً لبطاقات المنتجات
▸ تحديث دوري تلقائي بدون توقف
▸ معالجة شاملة للأخطاء مع عرض واضح للمستخدم
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import streamlit as st

# ── مسارات ─────────────────────────────────────────────────────────────────
_DATA_DIR        = os.environ.get("DATA_DIR", "data")
_COMPETITORS_FILE = os.path.join(_DATA_DIR, "competitors_list.json")
_PROGRESS_FILE   = os.path.join(_DATA_DIR, "scraper_progress.json")
_STATE_FILE      = os.path.join(_DATA_DIR, "scraper_state.json")
_OUTPUT_CSV      = os.path.join(_DATA_DIR, "competitors_latest.csv")

os.makedirs(_DATA_DIR, exist_ok=True)

_STATE_LOCK    = threading.Lock()
_RESULT_LOCK   = threading.Lock()

# ── CSS ─────────────────────────────────────────────────────────────────────
_CSS = """
<style>
.sc-card{
    background:linear-gradient(135deg,#0d1b2a,#0a1520);
    border:1.5px solid #1e3a5f;border-radius:12px;
    padding:16px 18px 12px;margin-bottom:10px;
    transition:border-color .3s,box-shadow .3s;
}
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
.product-row{display:flex;align-items:center;gap:10px;padding:6px 10px;
             background:#0d1b2a;border-radius:8px;margin:3px 0;border:1px solid #1e3a5f;}
.product-row img{width:36px;height:36px;object-fit:cover;border-radius:5px;flex-shrink:0;}
.product-row .pname{flex:1;font-size:.82rem;color:#e0e0e0;overflow:hidden;
                    text-overflow:ellipsis;white-space:nowrap;}
.product-row .pprice{font-size:.82rem;font-weight:700;color:#ff9800;white-space:nowrap;}
.product-row .pstore{font-size:.7rem;color:#4fc3f7;white-space:nowrap;}
.live-count{font-size:2.5rem;font-weight:900;color:#00C853;text-align:center;line-height:1;}
</style>
"""


# ═══════════════════════════════════════════════════════════════════════════
#  دوال مساعدة
# ═══════════════════════════════════════════════════════════════════════════
def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "").strip() or url
    except Exception:
        return url


def _effective_concurrency() -> int:
    """قيمة التزامن بدون تعديل مفتاح widget فعّال."""
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


def _read_result(domain: str) -> dict | None:
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
    # احذف ملفات التقدم
    for p in [_live_path(domain), _result_path(domain)]:
        try:
            os.remove(p)
        except Exception:
            pass


def _get_db_count(domain: str) -> int:
    """عدد منتجات المنافس في SQLite (real-time)."""
    try:
        from utils.db_manager import get_competitor_products_df
        df = get_competitor_products_df(domain)
        return len(df)
    except Exception:
        return 0


def _get_all_db_products(domain: str = "", limit: int = 50) -> pd.DataFrame:
    """آخر المنتجات المكشوطة من SQLite."""
    try:
        from utils.db_manager import get_competitor_products_df
        df = get_competitor_products_df(domain)
        if df.empty:
            return pd.DataFrame()
        return df.tail(limit).iloc[::-1]
    except Exception:
        return pd.DataFrame()


def _total_db_products() -> dict:
    """إحصاءات SQLite الكلية."""
    try:
        from utils.db_manager import get_competitor_store_stats
        return get_competitor_store_stats()
    except Exception:
        return {"total_products": 0, "by_competitor": {}}


# ═══════════════════════════════════════════════════════════════════════════
#  تشغيل الكشط في خيط daemon
# ═══════════════════════════════════════════════════════════════════════════
def _run_store_bg(store_url: str, concurrency: int = 6, max_products: int = 0, force: bool = False) -> None:
    """يُشغَّل في daemon thread — يستدعي run_single_store من engines."""
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


def _launch_store(store_url: str, concurrency: int = 6, max_products: int = 0, force: bool = False) -> None:
    """يُطلق خيط daemon للكشط ويُسجّل حالة running."""
    domain = _domain(store_url)
    # سجّل حالة running في scraper_state
    state = _load_state()
    state[domain] = state.get(domain, {})
    state[domain].update({"status": "running", "store_url": store_url,
                           "domain": domain, "started_at": datetime.now().isoformat()})
    _save_state(state)
    t = threading.Thread(target=_run_store_bg, args=(store_url, concurrency, max_products, force),
                          daemon=True, name=f"scraper-{domain}")
    t.start()
    # سجّل الـ thread لاحقاً
    if "sc_threads" not in st.session_state:
        st.session_state["sc_threads"] = {}
    st.session_state["sc_threads"][domain] = t


def _is_thread_alive(domain: str) -> bool:
    threads = st.session_state.get("sc_threads", {})
    t = threads.get(domain)
    return bool(t and t.is_alive())


# ═══════════════════════════════════════════════════════════════════════════
#  تغذية بيانات المنافس للتحليل
# ═══════════════════════════════════════════════════════════════════════════
def _feed_to_analysis(domain: str, label: str) -> None:
    """يُرسل منتجات المنافس من SQLite إلى session_state للتحليل."""
    try:
        df = _get_all_db_products(domain, limit=10000)
        if df.empty:
            # حاول من CSV
            try:
                csv_df = pd.read_csv(_OUTPUT_CSV, encoding="utf-8-sig", low_memory=False)
                df = csv_df[csv_df["store"].astype(str) == domain].copy()
            except Exception:
                pass

        if df.empty:
            st.warning(f"⚠️ لا توجد منتجات مكشوطة من {label}")
            return

        # توحيد الأعمدة
        rename_map = {
            "product_name": "المنتج", "name": "المنتج",
            "price": "السعر", "image_url": "صورة_المنافس",
            "product_url": "رابط_المنافس", "brand": "الماركة",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        if "المنتج" not in df.columns and "المنافس" not in df.columns:
            st.error("❌ الأعمدة غير متطابقة")
            return

        df["المنافس"] = domain
        df["منتج_المنافس"] = df.get("المنتج", df.get("name", ""))
        df["سعر_المنافس"] = df.get("السعر", 0)

        # دمج مع comp_dfs في session_state
        existing = st.session_state.get("comp_dfs") or {}
        existing[label] = df
        st.session_state["comp_dfs"] = existing
        st.session_state["_scraper_fed_comp"] = domain

        st.success(f"✅ {len(df):,} منتج من **{label}** جاهز للتحليل — اذهب للوحة التحكم")
    except Exception as e:
        st.error(f"❌ خطأ في الإرسال: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#  الواجهة الرئيسية
# ═══════════════════════════════════════════════════════════════════════════
def show() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)

    # ── Auto-Refresh (فقط أثناء وجود كشط نشط) ────────────────────────────
    state = _load_state()
    _any_running = any(
        v.get("status") == "running" and _is_thread_alive(k)
        for k, v in state.items()
    )
    if _any_running:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=3000, key="sc_live_refresh")
        except ImportError:
            pass

    st.markdown("## 🕷️ كاشط المنافسين — لوحة التحكم")

    # ── KPIs العلوية ──────────────────────────────────────────────────────
    stats = _total_db_products()
    total_prods = stats.get("total_products", 0)
    by_comp     = stats.get("by_competitor", {})
    total_comps = len(by_comp)
    progress    = _load_progress()
    is_global_running = bool(progress.get("running")) or _any_running

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(f'<div class="sc-kpi"><div class="num">{total_prods:,}</div>'
                    f'<div class="lbl">إجمالي المنتجات المكشوطة</div></div>', unsafe_allow_html=True)
    with k2:
        st.markdown(f'<div class="sc-kpi"><div class="num">{total_comps}</div>'
                    f'<div class="lbl">منافسين في قاعدة البيانات</div></div>', unsafe_allow_html=True)
    with k3:
        _done = sum(1 for v in state.values() if v.get("status") == "done")
        st.markdown(f'<div class="sc-kpi"><div class="num" style="color:#00C853">{_done}</div>'
                    f'<div class="lbl">متاجر مكتملة الكشط</div></div>', unsafe_allow_html=True)
    with k4:
        _run_icon = "🟢 يعمل" if is_global_running else "⚫ متوقف"
        st.markdown(f'<div class="sc-kpi"><div class="num" style="font-size:1.3rem">{_run_icon}</div>'
                    f'<div class="lbl">حالة الكشط</div></div>', unsafe_allow_html=True)

    st.write("")

    # ── تبويبات ───────────────────────────────────────────────────────────
    tab_main, tab_add, tab_live, tab_settings = st.tabs([
        "🏪 إدارة المنافسين", "➕ إضافة منافس", "📡 بث مباشر", "⚙️ الإعدادات"
    ])

    # ══════════════════════════════════════════════════════════════════════
    #  تبويب 1: إدارة المنافسين
    # ══════════════════════════════════════════════════════════════════════
    with tab_main:
        stores = _load_stores()
        if not stores:
            st.info("لم تُضف أي متجر منافس بعد. اذهب لتبويب «إضافة منافس».")
            return

        # أزرار التشغيل الكلي
        ba, bb, bc = st.columns([2, 2, 4])
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
                # نضبط ملف progress لوقف الـ scheduler
                try:
                    prog = _load_progress()
                    prog["running"] = False
                    with open(_PROGRESS_FILE, "w", encoding="utf-8") as f:
                        json.dump(prog, f)
                except Exception:
                    pass
                st.warning("⚠️ تم طلب الإيقاف — الخيوط الجارية ستكتمل دورتها الحالية")

        st.markdown("---")

        # ── بطاقة كل منافس ────────────────────────────────────────────
        state = _load_state()
        for store_url in stores:
            domain = _domain(store_url)
            cp     = state.get(domain, {})
            status = cp.get("status", "pending")

            # تحقق من الخيط الفعلي
            if status == "running" and not _is_thread_alive(domain):
                # الخيط انتهى → حدّث الحالة
                result = _read_result(domain)
                if result:
                    status = "done" if result.get("success") else "error"
                    cp["status"] = status
                    state[domain] = cp
                    _save_state(state)

            live  = _read_live(domain)
            db_count = _get_db_count(domain)

            # رمز + لون
            icon_map = {"done": "✅", "error": "❌", "running": "⏳", "pending": "⏸️", "skipped": "⏭️"}
            badge_map= {"done": "done-b", "error": "error-b", "running": "run-b",
                        "pending": "pend-b", "skipped": "skip-b"}
            sc_cls   = {"done": "done", "error": "error", "running": "running",
                        "pending": "pending", "skipped": "skipped"}.get(status, "pending")

            icon  = icon_map.get(status, "❓")
            badge = badge_map.get(status, "pend-b")

            # حساب التقدم
            pct = 0
            if status == "running" and live:
                d_pct = live.get("pct", 0)
                pct   = max(0, min(100, int(d_pct)))
            elif status == "done":
                pct = 100

            rows_saved = db_count or cp.get("rows_saved", 0)
            urls_done  = live.get("urls_done", cp.get("urls_done", 0)) if status=="running" else cp.get("urls_done", 0)
            urls_total = live.get("urls_total", cp.get("urls_total", 0))
            upd_at     = live.get("updated_at", cp.get("last_checkpoint_at", ""))

            st.markdown(
                f'<div class="sc-card {sc_cls}">'
                f'<div style="display:flex;justify-content:space-between;align-items:center">'
                f'<div>'
                f'<span style="font-weight:700;font-size:1rem">{icon} {domain}</span>'
                f'&nbsp;<span class="sc-badge {badge}">{status}</span>'
                f'</div>'
                f'<div style="font-size:.75rem;color:#4fc3f7">'
                f'{"🔴 يعمل الآن" if status=="running" else ""}</div>'
                f'</div>'
                f'<div class="sc-meta">'
                f'<span>🛍️ {rows_saved:,} منتج محفوظ</span>'
                + (f'<span>📶 {urls_done:,}/{urls_total:,} رابط</span>' if urls_total else '') +
                (f'<span>🕐 {upd_at}</span>' if upd_at else '') +
                f'</div>'
                + (f'<div class="sc-bar-bg"><div class="sc-bar-fill" style="width:{pct}%"></div></div>' if pct > 0 else '') +
                f'</div>',
                unsafe_allow_html=True
            )

            # أزرار الإجراء
            c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 2])
            _disabled_run = status == "running" and _is_thread_alive(domain)

            with c1:
                if st.button(
                    "▶️ بدء" if status != "running" else "🔄 جاري...",
                    key=f"run_{domain}",
                    disabled=_disabled_run,
                    use_container_width=True,
                ):
                    _launch_store(store_url,
                                  concurrency=_effective_concurrency())
                    st.rerun()
            with c2:
                if st.button("🔁 إعادة", key=f"re_{domain}", disabled=_disabled_run, use_container_width=True):
                    _reset_store(domain)
                    _launch_store(store_url, force=True,
                                  concurrency=_effective_concurrency())
                    st.rerun()
            with c3:
                if st.button("⏭️ تخطي", key=f"skip_{domain}", disabled=_disabled_run, use_container_width=True):
                    _reset_store(domain)
                    new_state = _load_state()
                    new_state[domain] = {"status": "done", "domain": domain,
                                          "store_url": store_url, "rows_saved": 0,
                                          "error": "skipped"}
                    _save_state(new_state)
                    st.rerun()
            with c4:
                if st.button("📊 للتحليل", key=f"feed_{domain}", use_container_width=True,
                              help="أرسل منتجات هذا المنافس مباشرةً لنظام المقارنة"):
                    _feed_to_analysis(domain, domain)
            with c5:
                if st.button("🗑️ حذف", key=f"del_{domain}", use_container_width=True):
                    updated = [s for s in stores if _domain(s) != domain]
                    _save_stores(updated)
                    _reset_store(domain)
                    # احذف من SQLite
                    try:
                        from utils.db_manager import clear_competitor_store
                        clear_competitor_store(domain)
                    except Exception:
                        pass
                    st.success(f"حُذف {domain}")
                    st.rerun()

            # عرض آخر المنتجات إذا كان يعمل
            if status == "running" and rows_saved > 0:
                with st.expander(f"📦 آخر المنتجات المكشوطة من {domain}", expanded=False):
                    _live_df = _get_all_db_products(domain, limit=10)
                    if not _live_df.empty:
                        show_cols = [c for c in ["product_name", "price", "brand", "updated_at"]
                                     if c in _live_df.columns]
                        st.dataframe(_live_df[show_cols].head(10), use_container_width=True, height=230)

            # عرض الخطأ إذا فشل
            if status == "error":
                err = cp.get("error", "")
                result = _read_result(domain)
                msg = (result or {}).get("message", err)
                if msg and msg != "skipped":
                    if str(msg).strip().startswith("✅ 0 منتج"):
                        msg = "لم يتم استخراج منتجات جديدة في هذه الدورة (غالباً حظر/timeout/قيود الموقع)."
                    st.error(f"❌ {domain}: {str(msg)[:200]}")

    # ══════════════════════════════════════════════════════════════════════
    #  تبويب 2: إضافة منافس
    # ══════════════════════════════════════════════════════════════════════
    with tab_add:
        st.markdown("### ➕ إضافة متجر منافس جديد")

        with st.form("add_store_form", clear_on_submit=True):
            new_url = st.text_input(
                "🔗 رابط المتجر",
                placeholder="https://example.com أو https://store.salla.sa/...",
                help="يدعم: Shopify، سلة، Zid، WooCommerce، أي متجر عربي"
            )
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
                        _launch_store(new_url,
                                      concurrency=_effective_concurrency(),
                                      max_products=int(max_p))
                        st.success(f"✅ أُضيف {domain} وبدأ الكشط تلقائياً!")
                    else:
                        st.success(f"✅ أُضيف {domain} — اضغط ▶️ لبدء الكشط")
                    st.rerun()

        st.markdown("---")
        st.markdown("**🏪 قائمة المتاجر المضافة حالياً:**")
        stores = _load_stores()
        if stores:
            for s in stores:
                st.markdown(f"- `{s}`")
        else:
            st.caption("لا توجد متاجر مضافة بعد.")

        st.markdown("---")
        st.markdown("#### 📋 استيراد قائمة متاجر (كل متجر في سطر)")
        bulk_text = st.text_area(
            "أدخل روابط المتاجر (سطر لكل متجر)",
            placeholder="https://store1.com\nhttps://store2.salla.sa\nhttps://store3.com",
            height=150
        )
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

    # ══════════════════════════════════════════════════════════════════════
    #  تبويب 3: بث مباشر
    # ══════════════════════════════════════════════════════════════════════
    with tab_live:
        st.markdown("### 📡 المنتجات المكشوطة حديثاً (Real-Time)")

        stores = _load_stores()
        domains = [_domain(s) for s in stores] if stores else []

        # فلتر المنافس
        filter_comp = st.selectbox(
            "عرض منتجات:", ["كل المنافسين"] + domains,
            key="live_filter_comp"
        )
        filter_domain = "" if filter_comp == "كل المنافسين" else filter_comp

        # عداد حي
        db_count = _get_db_count(filter_domain) if filter_domain else total_prods
        st.markdown(
            f'<div style="text-align:center;padding:20px 0">'
            f'<div class="live-count">{db_count:,}</div>'
            f'<div style="color:#607d8b;font-size:.9rem;margin-top:6px">'
            f'منتج {"من " + filter_domain if filter_domain else "إجمالي"} في قاعدة البيانات</div>'
            f'</div>',
            unsafe_allow_html=True
        )

        # جدول المنتجات
        live_df = _get_all_db_products(filter_domain, limit=100)
        if not live_df.empty:
            # تجميل الأعمدة
            show_cols = []
            col_map = {
                "competitor": "المنافس", "product_name": "اسم المنتج",
                "price": "السعر", "brand": "الماركة",
                "updated_at": "آخر تحديث",
            }
            live_display = live_df.rename(columns=col_map)
            show_cols = [v for v in col_map.values() if v in live_display.columns]

            st.dataframe(
                live_display[show_cols],
                use_container_width=True,
                height=400,
            )

            # تصدير
            csv_b = live_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button(
                f"📥 تصدير ({db_count:,} منتج) CSV",
                data=csv_b,
                file_name=f"scraped_{filter_domain or 'all'}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
        else:
            st.info("⏳ لا توجد بيانات بعد — ابدأ الكشط من تبويب «إدارة المنافسين»")

        # إحصاءات تفصيلية
        if by_comp:
            st.markdown("---")
            st.markdown("#### 📊 توزيع المنتجات حسب المنافس")
            comp_df = pd.DataFrame(
                [(k, v) for k, v in sorted(by_comp.items(), key=lambda x: x[1], reverse=True)],
                columns=["المنافس", "عدد المنتجات"]
            )
            st.dataframe(comp_df, use_container_width=True, height=250)

            # زر "إرسال الكل للتحليل"
            st.markdown("---")
            if st.button("🚀 إرسال كل البيانات المكشوطة للتحليل", type="primary", use_container_width=True):
                all_df = _get_all_db_products("", limit=100000)
                if not all_df.empty:
                    # قسّم حسب المنافس
                    comp_dfs = {}
                    if "competitor" in all_df.columns:
                        for comp, gdf in all_df.groupby("competitor"):
                            gdf2 = gdf.rename(columns={
                                "product_name": "المنتج", "price": "السعر",
                                "image_url": "صورة_المنافس", "product_url": "رابط_المنافس",
                            }).copy()
                            gdf2["المنافس"] = comp
                            gdf2["منتج_المنافس"] = gdf2.get("المنتج", gdf2.get("product_name", ""))
                            gdf2["سعر_المنافس"] = gdf2.get("السعر", 0)
                            comp_dfs[comp] = gdf2
                    if comp_dfs:
                        st.session_state["comp_dfs"] = comp_dfs
                        st.success(f"✅ {len(all_df):,} منتج من {len(comp_dfs)} منافس جاهزة للتحليل — اذهب للوحة التحكم")
                    else:
                        st.warning("⚠️ لا يوجد عمود منافس في البيانات")

    # ══════════════════════════════════════════════════════════════════════
    #  تبويب 4: الإعدادات
    # ══════════════════════════════════════════════════════════════════════
    with tab_settings:
        st.markdown("### ⚙️ إعدادات الكشط")

        st.slider(
            "التزامن (Concurrency) — عدد الطلبات المتزامنة",
            min_value=2, max_value=20,
            value=int(st.session_state.get("sc_concurrency_adv", 6)),
            step=1,
            key="sc_concurrency_adv",
            help="قيمة أقل = أبطأ لكن أأمن من الحجب | قيمة أعلى = أسرع لكن خطر حجب أكبر"
        )

        st.markdown("---")
        st.markdown("#### 🗓️ الجدولة التلقائية")
        try:
            from scrapers.scheduler import (get_scheduler_status, enable_scheduler,
                                             disable_scheduler, trigger_now,
                                             start_scheduler_thread, DEFAULT_INTERVAL_HOURS)
            sched_status = get_scheduler_status()
            is_enabled   = bool(sched_status.get("enabled"))

            s1, s2 = st.columns(2)
            with s1:
                interval = st.number_input(
                    "الجدول (ساعات بين كل كشط)",
                    min_value=1, max_value=168,
                    value=int(sched_status.get("interval_hours", DEFAULT_INTERVAL_HOURS))
                )
            with s2:
                st.metric("آخر تشغيل", sched_status.get("last_run", "—")[:16] if sched_status.get("last_run") else "—")
                st.metric("التشغيل القادم", sched_status.get("next_run_label", "—"))

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
                    # أطلق كشط كل المتاجر
                    for s in _load_stores():
                        d = _domain(s)
                        if not _is_thread_alive(d):
                            _launch_store(s, concurrency=_effective_concurrency())
                    st.success("✅ بدأ الكشط الفوري")
                    st.rerun()

        except Exception as e:
            st.error(f"❌ خطأ في الجدولة: {e}")

        st.markdown("---")
        st.markdown("#### 🧹 إدارة البيانات")

        col_x, col_y = st.columns(2)
        with col_x:
            if st.button("🔄 إعادة ضبط كل نقاط الاستئناف", use_container_width=True):
                try:
                    if os.path.exists(_STATE_FILE):
                        os.remove(_STATE_FILE)
                    st.success("✅ تمت إعادة ضبط نقاط الاستئناف")
                except Exception as ex:
                    st.error(f"❌ {ex}")

        with col_y:
            if st.button("🗑️ مسح قاعدة بيانات الكشط", use_container_width=True):
                try:
                    from utils.db_manager import clear_competitor_store
                    n = clear_competitor_store()
                    st.success(f"✅ حُذف {n} سجل من قاعدة البيانات")
                except Exception as ex:
                    st.error(f"❌ {ex}")

        # معلومات الملفات
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
            exists_icon = "✅" if os.path.exists(fp) else "❌"
            st.caption(f"{exists_icon} **{label}**: `{fp}`{size}")
