"""
app.py - نظام التسعير الذكي مهووس v26.0
# SYSTEM STATUS: LOCKED & AUTONOMOUS — Fire and Forget Mode
✅ معالجة خلفية مع حفظ تلقائي
✅ جداول مقارنة بصرية في كل الأقسام
✅ أزرار AI + قرارات لكل منتج
✅ بحث أسعار السوق والمنافسين
✅ بحث mahwous.com للمنتجات المفقودة
✅ تحديث تلقائي للأسعار عند إعادة رفع المنافس
✅ تصدير Make لكل منتج وللمجموعات
✅ Gemini Chat مباشر
✅ فلاتر ذكية في كل قسم
✅ تاريخ جميل لكل العمليات
✅ محرك أتمتة ذكي مع قواعد تسعير قابلة للتخصيص (v26.0)
✅ لوحة تحكم الأتمتة متصلة بالتنقل (v26.0)
✅ محرك كشط غير متزامن (Async Scraper + Detached Process)
✅ فحص ذاتي عند الإقلاع (Health Check)
"""
import html
import json
import re
from urllib.parse import urlparse
import streamlit as st
import pandas as pd
import threading
import time
import uuid
from functools import partial
from datetime import datetime

try:
    from streamlit.runtime.scriptrunner import add_script_run_ctx
except ImportError:
    try:
        from streamlit.scriptrunner import add_script_run_ctx
    except ImportError:
        def add_script_run_ctx(t): return t

from config import *

# ── تجاوز SECTIONS لإضافة مصنع المنتجات وإعادة الترتيب (v26.0 UI Update) ──
SECTIONS = [
    "✨ مصنع المنتجات",
    "📊 لوحة التحكم",
    "🔴 سعر أعلى",
    "🟢 سعر أقل",
    "✅ موافق عليها",
    "🔍 منتجات مفقودة",
    "⚠️ تحت المراجعة",
    "⚪ مستبعد (لا يوجد تطابق)",
    "✅ تمت المعالجة",
    "⚡ أتمتة Make",
    "🔄 الأتمتة الذكية",
    "🕷️ كشط المنافسين",
    "⚙️ الإعدادات",
]
from styles import (get_styles, vs_card, comp_strip, miss_card,
                    get_sidebar_toggle_js, lazy_img_tag, linked_product_title)
from engines.mahwous_core import validate_export_product_dataframe
from engines.engine import (read_file, run_full_analysis, find_missing_products,
                             smart_missing_barrier,
                             extract_brand, extract_size, extract_type, is_sample,
                             resolve_catalog_columns, detect_input_columns,
                             apply_user_column_map,
                             _first_image_url_from_row)
from engines.ai_engine import (call_ai, verify_match, analyze_product,
                                bulk_verify, suggest_price,
                                search_market_price, search_mahwous,
                                check_duplicate,
                                fetch_fragrantica_info, fetch_product_images,
                                generate_mahwous_description, _parse_seo_json_block,
                                reclassify_review_items, ai_deep_analysis,
                                generate_seo_description, generate_action_summary)
from engines.analysis_job_runner import run_analysis_background_job as _run_analysis_background
from engines.reconciliation_engine import (
    failed_rows_to_csv_bytes,
    failed_rows_to_xlsx_bytes,
    merge_reconciliation_into_audit,
    reconcile_competitor_upload,
)
from engines.file_reader import load_competitor_csv_for_matching
from engines.automation import (AutomationEngine, ScheduledSearchManager,
                                 auto_push_decisions, auto_process_review_items,
                                 log_automation_decision, get_automation_log,
                                 get_automation_stats)
from utils.helpers import (apply_filters, get_filter_options, export_to_excel,
                            export_multiple_sheets, parse_pasted_text,
                            safe_float, format_price, format_diff,
                            fetch_og_image_url, favicon_url_for_site,
                            fetch_page_title_from_url)
from utils.make_helper import (send_price_updates, send_new_products,
                                send_missing_products, send_single_product,
                                verify_webhook_connection, export_to_make_format,
                                send_batch_smart)
from utils.salla_shamel_export import (
    export_to_salla_shamel,
    export_to_salla_shamel_csv,
    verify_truly_missing,
    merge_competitor_uploads,
    SALLA_SHAMEL_COLUMNS,
)
from utils.product_analyzer import analyze_product_inline, render_analysis_result
from utils.filter_ui import (render_sidebar_filters, apply_global_filters,
                              get_active_filter_summary)
from utils.data_helpers import (safe_results_for_json, restore_results_from_json,
                                merge_missing_products_dataframes,
                                merge_price_analysis_dataframes,
                                ts_badge, decision_badge,
                                row_media_urls_from_analysis,
                                our_product_url_from_row,
                                competitor_product_url_from_row)
from utils.db_manager import (init_db, log_event, log_decision,
                               log_analysis, get_events, get_decisions,
                               get_analysis_history, upsert_price_history,
                               get_price_history, get_price_changes,
                               save_job_progress, get_job_progress, get_last_job,
                               save_hidden_product, get_hidden_product_keys,
                               init_db_v26, upsert_our_catalog, upsert_comp_catalog,
                               save_processed, get_processed, undo_processed,
                               get_processed_keys, migrate_db_v26,
                               upsert_competitor_products, get_competitor_products_df,
                               get_competitor_store_stats, init_competitor_store)

# ── استيراد صفحات الدمج (مع try/except لضمان عدم توقف التطبيق) ────────────
try:
    import pages.magic_factory as _magic_factory_mod
except Exception as _mf_import_err:
    _magic_factory_mod = None

_scraper_advanced_mod = None
_scraper_advanced_import_error = None


def _get_scraper_advanced_module():
    """
    تحميل كسول لأدوات الكشط المتقدمة.
    مهم جداً لنسخة Streamlit Cloud لأن تنفيذ أوامر واجهة عند الاستيراد المبكر
    قد يؤدي إلى رندر جزئي أو شاشة فارغة قبل اكتمال الصفحة الرئيسية.
    """
    global _scraper_advanced_mod, _scraper_advanced_import_error
    if _scraper_advanced_mod is not None:
        return _scraper_advanced_mod
    if _scraper_advanced_import_error is not None:
        return None
    try:
        import importlib
        _scraper_advanced_mod = importlib.import_module("pages.scraper_advanced")
        return _scraper_advanced_mod
    except Exception as _sa_import_err:
        _scraper_advanced_import_error = _sa_import_err
        return None




@st.cache_data(ttl=86400, show_spinner=False)
def _cached_thumb_from_product_url(page_url: str) -> str:
    """صورة معاينة من صفحة المنتج عندما لا يوجد عمود صورة في الجدول المحفوظ."""
    u = (page_url or "").strip()
    if not u.startswith("http"):
        return ""
    og = fetch_og_image_url(u)
    if og:
        return og
    return favicon_url_for_site(u)


@st.cache_data(ttl=86400, show_spinner=False)
def _cached_title_from_product_url(page_url: str) -> str:
    """عنوان المنتج من og:title / <title> عندما يكون الاسم مخزّناً كرابط."""
    return fetch_page_title_from_url(page_url) or ""


def _norm_dup_text(s: str) -> str:
    """تطبيع اسم المنتج لمقارنة تكرار محلية أدق."""
    t = str(s or "").strip().lower()
    t = re.sub(r"(eau de parfum|eau de toilette|parfum|edp|edt|for men|for women)", " ", t, flags=re.I)
    t = re.sub(r"(للرجال|للنساء|رجالي|نسائي|او دي بارفان|او دو بارفان|او دي تواليت)", " ", t)
    t = re.sub(r"[^0-9a-z\u0600-\u06FF\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _dup_similarity(a: str, b: str) -> float:
    aa = set(_norm_dup_text(a).split())
    bb = set(_norm_dup_text(b).split())
    if not aa or not bb:
        return 0.0
    return len(aa & bb) / max(len(aa), len(bb))


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict | None = None, run_id: str = "pre-fix") -> None:
    # region agent log
    try:
        payload = {
            "sessionId": "aea738",
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        with open("debug-aea738.log", "a", encoding="utf-8") as _fh:
            _fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass
    # endregion


# ── إعداد الصفحة ──────────────────────────
st.set_page_config(page_title=APP_TITLE, page_icon=APP_ICON,
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(get_styles(), unsafe_allow_html=True)
# إخفاء روابط التنقل التلقائية (app, magic factory, scraper advanced) من أعلى الشريط الجانبي
st.markdown("<style>[data-testid='stSidebarNav'] {display: none;}</style>", unsafe_allow_html=True)
st.markdown(get_sidebar_toggle_js(), unsafe_allow_html=True)
_debug_log("H1", "app.py:set_page_config", "App bootstrap reached", {"app_title": APP_TITLE})

# ── فحص ذاتي عند الإقلاع (يعمل مرة واحدة فقط لكل جلسة) ────────────────
if "health_check_done" not in st.session_state:
    try:
        from utils.health_check import run_system_diagnostics
        _hc = run_system_diagnostics()
        st.session_state["health_check_done"] = True
        st.session_state["health_status"] = {
            "ok": _hc.ok,
            "warnings": _hc.warnings,
            "errors":   _hc.errors,
            "details":  _hc.details,
        }
    except Exception as _hce:
        st.session_state["health_check_done"] = True
        st.session_state["health_status"] = {
            "ok": True, "warnings": [], "errors": [], "details": {}
        }

# ── تشغيل خيط المجدول التلقائي (مرة واحدة عند أول تشغيل للبيئة) ─────────
if "scheduler_started" not in st.session_state:
    try:
        from scrapers.scheduler import start_scheduler_thread
        start_scheduler_thread()
        st.session_state["scheduler_started"] = True
    except Exception:
        st.session_state["scheduler_started"] = False

# أخطاء حرجة فقط تُعرض عالمياً (مثل DB تالفة) — التحذيرات تُعرض في الشريط الجانبي
_hs = st.session_state.get("health_status", {})
for _hc_err in _hs.get("errors", []):
    st.error(f"⚠️ فحص النظام: {_hc_err}")
try:
    init_db()
    init_db_v26()
    migrate_db_v26()  # v26.0 — ترحيل آمن (idempotent)
except Exception as e:
    st.error(f"Database Initialization Error: {e}")

# ── Session State ─────────────────────────
_defaults = {
    "results": None, "missing_df": None, "analysis_df": None,
    "job_id": None, "job_running": False,
    "decisions_pending": {},   # {product_name: action}
    "our_df": None, "comp_dfs": None,  # حفظ الملفات للمنتجات المفقودة
    "hidden_products": set(),  # منتجات أُرسلت لـ Make أو أُزيلت
    "nav_flash": None,    # رسالة انتقال سريعة من أزرار لوحة التحكم
    "last_audit_stats": None,  # عدادات تدقيق من run_full_analysis
    "reconciliation_report": None,
    "reconciliation_failed_csv": None,
    "_action_toast": None, # رسالة نجاح/فشل Callback تُعرض كـ toast
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v
st.session_state.setdefault("processed_price_skus", set())  # FIX: Smart Workflow & AI Tracking
st.session_state.setdefault("processed_missing_urls", set())  # FIX: Smart Workflow & AI Tracking
# FIX: Relaxed Constraints — التراكم دائم افتراضياً لحماية النتائج السابقة من الفقد.
st.session_state["dash_accumulate_results"] = True

# تحميل المنتجات المخفية من قاعدة البيانات عند كل تشغيل
_db_hidden = get_hidden_product_keys()
st.session_state.hidden_products = st.session_state.hidden_products | _db_hidden

# تنقل من أزرار لوحة التحكم — يُطبَّق هنا قبل `st.radio(..., key="main_nav")` في الشريط الجانبي
# (Streamlit يمنع تعيين st.session_state.main_nav بعد إنشاء الودجت في نفس التشغيل)
_nav_apply = st.session_state.pop("_nav_pending", None)
if _nav_apply and _nav_apply in SECTIONS:
    st.session_state.main_nav = _nav_apply

# ════════════════════════════════════════════════
#  دوال المعالجة — يجب تعريفها قبل استخدامها
# ════════════════════════════════════════════════
def _split_results(df):
    """تقسيم نتائج التحليل على الأقسام بأمان تام."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        empty = pd.DataFrame()
        return {
            "price_raise": empty.copy(),
            "price_lower": empty.copy(),
            "approved": empty.copy(),
            "review": empty.copy(),
            "excluded": empty.copy(),
            "all": empty.copy(),
        }

    work = df.copy()
    if "القرار" not in work.columns:
        work["القرار"] = ""
    work["القرار"] = work["القرار"].fillna("").astype(str).str.strip()

    def _contains(txt):
        try:
            return work["القرار"].str.contains(txt, na=False, regex=False)
        except Exception:
            return pd.Series([False] * len(work), index=work.index)

    return {
        "price_raise": work[_contains("أعلى")].reset_index(drop=True),
        "price_lower": work[_contains("أقل")].reset_index(drop=True),
        "approved": work[_contains("موافق")].reset_index(drop=True),
        "review": work[_contains("مراجعة")].reset_index(drop=True),
        "excluded": work[_contains("مستبعد")].reset_index(drop=True),
        "all": work.reset_index(drop=True),
    }


def _analysis_mask_for_review_row(adf: pd.DataFrame, row: pd.Series) -> pd.Series:
    """مفتاح مطابقة صف المراجعة مع جدول التحليل الكامل."""
    try:
        oid = str(row.get("معرف_المنتج", "") or "").strip()
        cid = str(row.get("معرف_المنافس", "") or "").strip()
        if oid and oid != "nan" and cid and cid != "nan":
            m = (adf["معرف_المنتج"].astype(str).str.strip() == oid) & (
                adf["معرف_المنافس"].astype(str).str.strip() == cid
            )
            if m.any():
                return m
        n1 = str(row.get("المنتج", "") or "").strip()
        n2 = str(row.get("منتج_المنافس", "") or "").strip()
        return (adf["المنتج"].astype(str).str.strip() == n1) & (
            adf["منتج_المنافس"].astype(str).str.strip() == n2
        )
    except Exception:
        return pd.Series([False] * len(adf))


def _reclassify_section_to_qarar(section: str):
    """يحوّل قيمة section بعد التطبيع في ai_engine إلى نص عمود القرار."""
    if not section:
        return None
    s = str(section)
    if "مراجعة" in s or s.strip() == "⚠️ تحت المراجعة":
        return None
    if "🔵" in s or ("مفقود" in s and "منتجات" not in s):
        return "🔍 منتجات مفقودة"
    if "🔴" in s or "أعلى" in s:
        return "🔴 سعر أعلى"
    if "🟢" in s or "أقل" in s:
        return "🟢 سعر أقل"
    if "✅" in s or "موافق" in s:
        return "✅ موافق"
    return None


def _apply_reclassify_to_analysis(adf: pd.DataFrame, review_df: pd.DataFrame,
                                  rc_results: list, min_conf: float = 75.0):
    """
    يحدّث عمود القرار في analysis_df حسب نتائج reclassify_review_items.
    يعيد (الجدول المحدث، إحصاءات).
    """
    stats = {
        "applied": 0, "skip_conf": 0, "skip_review": 0, "skip_idx": 0,
        "skip_no_row": 0, "skip_no_qarar": 0,
    }
    if adf is None or adf.empty or not rc_results:
        return adf, stats
    out = adf.copy()
    batch = review_df.head(30).reset_index(drop=True)
    nbatch = len(batch)
    for rc in rc_results:
        try:
            conf = float(rc.get("confidence") or 0)
        except Exception:
            conf = 0.0
        if conf < min_conf:
            stats["skip_conf"] += 1
            continue
        sec = rc.get("section", "")
        qarar = _reclassify_section_to_qarar(sec)
        if qarar is None:
            stats["skip_review"] += 1
            continue
        try:
            idx = int(rc.get("idx", 0) or 0)
        except Exception:
            idx = 0
        if idx < 1 or idx > nbatch:
            stats["skip_idx"] += 1
            continue
        row = batch.iloc[idx - 1]
        mask = _analysis_mask_for_review_row(out, row)
        if not mask.any():
            stats["skip_no_row"] += 1
            continue
        out.loc[mask, "القرار"] = qarar
        stats["applied"] += 1
    return out, stats


def _persist_analysis_after_reclassify(adf: pd.DataFrame):
    """يحدّث job_progress إن وُجد job_id وحالة done."""
    jid = st.session_state.get("job_id")
    if not jid:
        return
    try:
        job = get_job_progress(jid)
        if not job or str(job.get("status", "")) != "done":
            return
        miss = job.get("missing") if isinstance(job.get("missing"), list) else []
        save_job_progress(
            jid,
            int(job.get("total") or len(adf)),
            int(job.get("processed") or len(adf)),
            safe_results_for_json(adf.to_dict("records")),
            "done",
            str(job.get("our_file") or ""),
            str(job.get("comp_files") or ""),
            missing=miss,
        )
    except Exception:
        pass


# ── تحميل تلقائي للنتائج المحفوظة عند فتح التطبيق ──
if st.session_state.results is None and not st.session_state.job_running:
    _auto_job = get_last_job()
    if _auto_job and _auto_job["status"] == "done" and _auto_job.get("results"):
        _auto_records = restore_results_from_json(_auto_job["results"])
        _auto_df = pd.DataFrame(_auto_records)
        if not _auto_df.empty:
            _auto_miss = pd.DataFrame(_auto_job.get("missing", [])) if _auto_job.get("missing") else pd.DataFrame()
            _auto_r = _split_results(_auto_df)
            _auto_r["missing"] = _auto_miss
            st.session_state.results     = _auto_r
            st.session_state.analysis_df = _auto_df
            st.session_state.job_id      = _auto_job.get("job_id")


# ── دوال مساعدة ───────────────────────────
def db_log(page, action, details=""):
    try: log_event(page, action, details)
    except: pass


def _effective_column_map(df: pd.DataFrame, key_prefix: str):
    """
    يقرأ اختيارات القوائم المنسدلة (إن وُجدت) وإلا يعود لنتيجة التعرف التلقائي.
    """
    if df is None or df.empty:
        return {"name": None, "price": None, "id_col": None, "img": None, "url": None}
    rc = resolve_catalog_columns(df)
    skip = "— (تخطي)"
    cols = {str(c) for c in df.columns}

    def _one(suffix: str, fallback_raw):
        k = f"{key_prefix}_{suffix}"
        v = st.session_state.get(k)
        fb = str(fallback_raw or "").strip()
        if v is None or v == skip:
            return fb if fb and fb in cols else None
        sv = str(v).strip()
        if sv == skip or sv not in cols:
            return fb if fb and fb in cols else None
        return sv

    return {
        "name": _one("name", rc.get("name")),
        "price": _one("price", rc.get("price")),
        "id_col": _one("id", rc.get("id")),
        "img": _one("img", rc.get("img")),
        "url": _one("url", rc.get("url")),
    }


def _resolve_catalog_columns_relaxed(df: pd.DataFrame) -> dict:
    """
    FIX: Relaxed Constraints — fallback مرن لاختيار أعمدة الاسم/السعر/المعرف
    لضمان حفظ الكتالوج حتى لو فشل التعرف الصارم.
    """
    from engines.engine import resolve_catalog_columns
    base = resolve_catalog_columns(df) if df is not None else {}
    if df is None or df.empty:
        return {"name": None, "price": None, "id": None, "img": None, "url": None}
    cols = list(df.columns)
    out = {
        "name": base.get("name"),
        "price": base.get("price"),
        "id": base.get("id"),
        "img": base.get("img"),
        "url": base.get("url"),
    }
    if not out["name"]:
        text_candidates = []
        for c in cols:
            s = df[c]
            if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
                nn = s.fillna("").astype(str).str.strip()
                score = int((nn != "").sum())
                if score > 0:
                    text_candidates.append((score, c))
        if text_candidates:
            text_candidates.sort(reverse=True)
            out["name"] = text_candidates[0][1]
    if not out["price"]:
        num_candidates = []
        for c in cols:
            s = pd.to_numeric(df[c], errors="coerce")
            score = int(s.notna().sum())
            if score > 0:
                num_candidates.append((score, c))
        if num_candidates:
            num_candidates.sort(reverse=True)
            out["price"] = num_candidates[0][1]
    if not out["id"]:
        for c in cols:
            lc = str(c).strip().lower()
            if any(k in lc for k in ("id", "sku", "معرف", "رقم", "barcode", "باركود")):
                out["id"] = c
                break
        if not out["id"] and out["name"] and out["name"] in cols:
            out["id"] = out["name"]
    return out


def _dashboard_competitor_label(upload_name: str) -> str:
    """اسم عرض للمنافس من اسم الملف (بدون .csv)."""
    n = (upload_name or "").strip()
    if not n:
        return "منافس"
    return n.rsplit(".", 1)[0] if n.lower().endswith(".csv") else n


def _render_column_mapping_expander(df: pd.DataFrame, key_prefix: str):
    """
    تحديد الأعمدة بقوائم منسدلة + معاينة صفوف قابلة للضبط + 5 قيم من عمود واحد.
    """
    if df is None or df.empty:
        st.warning("ملف فارغ أو غير مقروء")
        return
    rc = resolve_catalog_columns(df)
    cols_list = [str(c) for c in df.columns]
    skip = "— (تخطي)"
    options = [skip] + cols_list
    n_total = len(df)

    def _ix(fallback_raw):
        fb = str(fallback_raw or "").strip()
        if fb and fb in options:
            return options.index(fb)
        return 0

    st.caption(f"📊 **{len(cols_list)}** عمود — اضبط الأدوار أو اترك التعرف التلقائي")
    if len(cols_list) <= 4:
        st.caption("أسماء الأعمدة: " + "، ".join(f"«{c}»" for c in cols_list))
    g1, g2 = st.columns(2)
    with g1:
        st.selectbox("🏷️ اسم المنتج", options, index=_ix(rc.get("name")), key=f"{key_prefix}_name")
        st.selectbox("💰 السعر", options, index=_ix(rc.get("price")), key=f"{key_prefix}_price")
        st.selectbox("🔢 المعرف / SKU", options, index=_ix(rc.get("id")), key=f"{key_prefix}_id")
    with g2:
        st.selectbox("🖼️ صورة المنتج", options, index=_ix(rc.get("img")), key=f"{key_prefix}_img")
        st.selectbox("🔗 رابط المنتج", options, index=_ix(rc.get("url")), key=f"{key_prefix}_url")

    st.markdown("**عرض صفوف الملف**")
    pr1, pr2 = st.columns([1, 2])
    with pr1:
        n_preview = st.number_input(
            "عدد الصفوف",
            min_value=1,
            max_value=min(n_total, 500),
            value=min(5, n_total),
            step=1,
            key=f"{key_prefix}_preview_rows",
            help="معاينة من بداية الملف (كل الأعمدة).",
        )
    with pr2:
        st.caption(f"إجمالي الصفوف في الملف: **{n_total}**")
    _n = int(n_preview)
    st.dataframe(
        df.head(_n),
        use_container_width=True,
        height=min(520, 100 + _n * 28 + len(cols_list) * 2),
    )

    st.markdown("**معاينة — 5 قيم من عمود واحد**")
    peek_opts = ["— اختر عموداً —"] + cols_list
    pc = st.selectbox("العمود", peek_opts, key=f"{key_prefix}_peek")
    if pc and not str(pc).startswith("—"):
        try:
            st.dataframe(df[[pc]].head(5), use_container_width=True)
        except Exception:
            st.caption("تعذر عرض هذا العمود.")

    with st.expander("🔧 JSON — تفاصيل التعرف الخام", expanded=False):
        st.json(detect_input_columns(df))


def _validate_uploaded_catalog(df, label: str):
    """حارس أعمدة: اسم + سعر مطلوبان قبل التحليل (بعد read_file + التعرف العميق)."""
    if df is None or df.empty:
        st.error(f"⚠️ ملف فارغ أو غير مقروء: {label}")
        st.stop()
    m = resolve_catalog_columns(df)
    if not m.get("name") or not m.get("price"):
        st.error(
            f"⚠️ فشل التعرف الذكي على الأعمدة المطلوبة (**اسم المنتج** + **سعر**) في: **{label}**"
        )
        st.warning("معاينة خام — أول 10 صفوف:")
        st.dataframe(df.head(10), use_container_width=True)
        st.stop()


def _render_audit_bar(audit_stats: dict):
    """شريط تدقيق Zero Data Loss — يطابق المدخلات مع المخرجات المحاسَبة."""
    if not audit_stats:
        return
    ti = int(audit_stats.get("total_input") or 0)
    pr = int(audit_stats.get("processed") or 0)
    nc = int(audit_stats.get("no_competitor_found") or 0)
    se = int(audit_stats.get("skipped_empty") or 0)
    sk = int(audit_stats.get("skipped_samples") or 0)
    tot = pr + nc + se + sk
    st.markdown(
        f"""
    <div style="display:flex;flex-wrap:wrap;justify-content:space-between;gap:10px;
        background:#2c3e50;color:#fff;padding:15px;border-radius:8px;margin-bottom:16px;">
        <div style="text-align:center;flex:1;min-width:88px;"><strong>📦 إجمالي المدخلات</strong><br>
            <span style="font-size:1.5rem;">{ti}</span></div>
        <div style="text-align:center;flex:1;min-width:88px;"><strong>✅ وُجد منافس</strong><br>
            <span style="font-size:1.5rem;color:#4caf50;">{pr}</span></div>
        <div style="text-align:center;flex:1;min-width:88px;"><strong>⚪ لا منافس</strong><br>
            <span style="font-size:1.5rem;color:#ff9800;">{nc}</span></div>
        <div style="text-align:center;flex:1;min-width:88px;"><strong>👻 صفوف فارغة</strong><br>
            <span style="font-size:1.5rem;color:#9e9e9e;">{se}</span></div>
        <div style="text-align:center;flex:1;min-width:88px;"><strong>🚫 عينة / &lt;10مل</strong><br>
            <span style="font-size:1.5rem;color:#e53935;">{sk}</span></div>
    </div>
    """,
        unsafe_allow_html=True,
    )
    if ti > 0 and tot != ti:
        st.error(
            f"🚨 تحذير تدقيق: المدخلات ({ti}) لا تساوي مجموع الحالات ({tot}) — "
            f"معالج={pr} + بدون منافس={nc} + فارغ={se} + عينة/صغير={sk}."
        )


def _render_reconciliation_dashboard(audit_stats: dict):
    """لوحة محاسبة صفوف ملفات المنافسين (مدخلات = متطابق + جديد + تالف)."""
    if not audit_stats:
        return
    rec = audit_stats.get("reconciliation")
    if not rec:
        return
    x = int(rec.get("total_read") or 0)
    y = int(rec.get("matched") or 0)
    z = int(rec.get("new_ready") or 0)
    w = int(rec.get("corrupted") or 0)

    # ── التحقق البرمجي من معادلة المحاسبة (إلزامي): إجمالي = متطابق + جديد + تالف ──
    _balance_sum = y + z + w
    _balance_ok = (_balance_sum == x) if x > 0 else True

    st.markdown("##### 📊 محاسبة رفع المنافسين (Reconciliation)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🟢 إجمالي تمت قراءته", x)
    c2.metric("🔵 متطابق وتمت معالجته", y)
    c3.metric("🟡 منتجات جديدة (جاهزة للتصدير لسلة)", z)
    c4.metric("🔴 سجلات تالفة", w)

    if not _balance_ok:
        _gap = x - _balance_sum
        st.error(
            f"🚨 **انتهاك معادلة المحاسبة** — "
            f"المدخل ({x}) ≠ مجموع المخرجات ({_balance_sum}) | "
            f"متطابق={y} + جديد={z} + تالف={w} | فجوة={_gap:+d}\n\n"
            "يعني هذا وجود صفوف لم تُصنَّف — راجع محرك المحاسبة."
        )
    elif x > 0:
        st.success(
            f"✅ معادلة المحاسبة محققة: {x} = {y} + {z} + {w}"
        )

    if not rec.get("balance_ok", True) and rec.get("warning_message"):
        st.warning(rec["warning_message"])
    _diag = rec.get("diagnostics") or {}
    _dup = int(_diag.get("duplicate_skipped") or 0)
    _excluded_total = int(w + _dup)
    _fb = st.session_state.get("reconciliation_failed_csv")
    if _excluded_total > 0:
        ex1, ex2 = st.columns([2, 1])
        ex1.metric("🚨 المنتجات المكررة/المستبعدة", f"{_excluded_total:,} منتج")
        with ex2:
            if _fb:
                st.download_button(
                    label="⬇️ تنزيل المستبعدات الآن",
                    data=_fb,
                    file_name="failed_rows.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_failed_rows_prominent",
                )
    if not _fb:
        from pathlib import Path

        _fp = audit_stats.get("reconciliation_failed_csv_path")
        if _fp:
            p = Path(str(_fp))
            if p.is_file():
                try:
                    _fb = p.read_bytes()
                    st.session_state.reconciliation_failed_csv = _fb
                except OSError:
                    _fb = None
    if _fb:
        st.download_button(
            label="⬇️ تنزيل failed_rows.xlsx (الصفوف التالفة)",
            data=_fb,
            file_name="failed_rows.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_failed_rows_log",
        )
    st.caption(
        "الأرقام تعكس **صفوف ملفات المنافس** في آخر تشغيل؛ إن فعّلت الدمج التراكمي قد يزيد عدد "
        "صفوف «منتجات مفقودة» في الجداول دون أن يغيّر إجمالي المدخلات أعلاه."
    )


def _find_analysis_row_for_processed(product_name: str):
    """
    يعيد صف التحليل المطابق لسجل «تمت المعالجة»: اسم منتجنا أو اسم المنتج عند المنافس.
    يبحث في analysis_df ثم في أقسام results (والجدول الكامل all).
    """
    pn = str(product_name or "").strip()
    if not pn:
        return None

    def _match_df(df):
        if df is None or getattr(df, "empty", True):
            return None
        for col in ("المنتج", "منتج_المنافس"):
            if col not in df.columns:
                continue
            try:
                m = df[df[col].astype(str).str.strip() == pn]
                if not m.empty:
                    return m.iloc[0]
            except Exception:
                continue
        return None

    adf = st.session_state.get("analysis_df")
    r = _match_df(adf)
    if r is not None:
        return r

    res = st.session_state.get("results") or {}
    for key in ("all", "price_raise", "price_lower", "approved", "review", "excluded", "missing"):
        r = _match_df(res.get(key))
        if r is not None:
            return r
    return None


def _lookup_images_from_analysis_session(product_name: str):
    """صورة منتجنا + صورة المنافس من جلسة التحليل أو أقسام النتائج."""
    row = _find_analysis_row_for_processed(product_name)
    if row is None:
        return "", ""
    try:
        return row_media_urls_from_analysis(row)
    except Exception:
        return "", ""


def _lookup_product_urls_from_analysis_session(product_name: str):
    """رابط منتجنا + رابط صفحة المنتج عند المنافس."""
    row = _find_analysis_row_for_processed(product_name)
    if row is None:
        return "", ""
    try:
        return our_product_url_from_row(row), competitor_product_url_from_row(row)
    except Exception:
        return "", ""


def _processed_dual_image_html(our_img: str, comp_img: str, title_our: str, title_comp: str) -> str:
    """خليتان للصور: منتجنا | المنافس — تحميل eager حتى تظهر فوراً في Streamlit."""
    w, h = 56, 56

    def _slot(label: str, url: str, alt: str) -> str:
        if url and str(url).strip():
            img = lazy_img_tag(url, w, h, alt, loading="eager")
        else:
            img = (
                f'<div style="width:{w}px;height:{h}px;border-radius:8px;background:#121c2e;'
                f'border:1px dashed #2a3f5f;display:flex;align-items:center;justify-content:center;'
                f'color:#4a5c78;font-size:.75rem">—</div>'
            )
        return (
            f'<div style="display:flex;flex-direction:column;align-items:center;gap:5px;min-width:64px">'
            f'<span style="font-size:.68rem;color:#7eb8ff;font-weight:800;letter-spacing:.02em">{label}</span>'
            f"{img}</div>"
        )

    return (
        '<div style="display:flex;gap:16px;flex-shrink:0;align-items:flex-end;padding:2px 0">'
        f'{_slot("منتجنا", our_img, title_our[:40])}'
        f'{_slot("المنافس", comp_img, title_comp[:40])}'
        "</div>"
    )


def _is_http_url_text(s) -> bool:
    t = str(s or "").strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _humanize_competitor_upload(comp: str) -> str:
    """تطبيع اسم المنافس ليظهر كاسم متجر مقروء بدلاً من رابط أو اسم ملف خام."""
    c = str(comp or "").strip()
    if not c:
        return "—"

    parsed = urlparse(c if re.match(r"^https?://", c, flags=re.I) else f"https://{c}")
    host = (parsed.netloc or parsed.path or "").strip().lower()
    if host and ("." in host or "/" in c):
        host = host.split("/")[0].strip()
        host = re.sub(r"^www\.", "", host, flags=re.I)
        if host:
            c = host

    low = c.lower()
    for ext in (".csv", ".xlsx", ".xls", ".tsv", ".ods"):
        if low.endswith(ext):
            c = c[: -len(ext)].strip() or c
            break

    c = re.sub(r"[_\-]+", " ", c).strip()
    c = re.sub(r"\s+", " ", c).strip()
    return c or "—"


def _normalize_all_competitors(raw_comps) -> list:
    """إزالة التكرار من قائمة جميع المنافسين مع تطبيع أسماء المتاجر دون فقدان أي منافس فعلي."""
    if not isinstance(raw_comps, list):
        return []

    cleaned = []
    seen = set()
    for comp in raw_comps:
        if not isinstance(comp, dict):
            continue
        comp_copy = dict(comp)
        comp_name = _humanize_competitor_upload(comp_copy.get("competitor", ""))
        prod_name = str(comp_copy.get("name", "") or "").strip()
        prod_id = str(comp_copy.get("product_id", "") or "").strip()
        prod_url = str(comp_copy.get("product_url") or comp_copy.get("url") or "").strip().lower()
        key = (comp_name.lower(), prod_id or prod_url or prod_name.lower())
        if key in seen:
            continue
        seen.add(key)
        comp_copy["competitor"] = comp_name
        cleaned.append(comp_copy)
    return cleaned


def _display_competitor_name(row) -> str:
    """اسم المنافس الأساسي في البطاقة مع fallback من قائمة جميع المنافسين عند الحاجة."""
    direct = _humanize_competitor_upload(row.get("المنافس", ""))
    if direct and direct != "—":
        return direct
    normalized = _normalize_all_competitors(row.get("جميع_المنافسين", row.get("جميع المنافسين", [])))
    if normalized:
        return _humanize_competitor_upload(normalized[0].get("competitor", ""))
    return "—"


def _display_name_for_missing_row(row) -> str:
    """
    اسم عرض للمفقودات: يفضّل نصاً حقيقياً من أي عمود معروف قبل اعتبار الاسم رابطاً فقط.
    """
    def _clean(v):
        x = str(v or "").strip()
        if not x or x.lower() in ("nan", "none", "<na>"):
            return ""
        return x

    for key in (
        "المنتج",
        "اسم المنتج",
        "اسم_المنتج",
        "Product",
        "Name",
        "name",
        "title",
        "الاسم",
        "منتج_المنافس",
    ):
        if key not in row.index:
            continue
        v = _clean(row.get(key))
        if v and not _is_http_url_text(v):
            return v

    br = _clean(row.get("الماركة"))
    sz = _clean(row.get("الحجم"))
    pt = _clean(row.get("النوع"))
    chunks = [c for c in (br, sz, pt) if c]
    if chunks:
        return " · ".join(chunks)

    return ""


def _processed_row_url_chips_html(our_url: str, comp_url: str) -> str:
    """روابط مختصرة بجانب سطر الملاحظات في «تمت المعالجة»."""
    parts = []
    ou = (our_url or "").strip()
    cu = (comp_url or "").strip()
    if ou.startswith("http"):
        parts.append(
            f'<a href="{html.escape(ou, quote=True)}" target="_blank" rel="noopener noreferrer" '
            f'style="color:#4fc3f7;font-size:.73rem;font-weight:600;text-decoration:underline">🔗 رابط منتجنا</a>'
        )
    if cu.startswith("http"):
        parts.append(
            f'<a href="{html.escape(cu, quote=True)}" target="_blank" rel="noopener noreferrer" '
            f'style="color:#ff9800;font-size:.73rem;font-weight:600;text-decoration:underline">🔗 عند المنافس</a>'
        )
    if not parts:
        return ""
    return '<span style="margin-right:8px">&nbsp;|&nbsp;</span>' + '<span style="margin:0 4px;color:#555">·</span>'.join(parts)


def _track_processed_price_sku(product_id) -> None:
    # FIX: Smart Workflow & AI Tracking
    _pid = str(product_id or "").strip()
    if _pid and _pid not in ("nan", "None", "NaN"):
        st.session_state["processed_price_skus"].add(_pid)


def _track_processed_missing_url(comp_url: str) -> None:
    # FIX: Smart Workflow & AI Tracking
    _url = str(comp_url or "").strip()
    if _url:
        st.session_state["processed_missing_urls"].add(_url)


def _show_transparency_counter(total_count: int, visible_count: int, label: str = "منتجاً") -> None:
    # FIX: Transparency & Reversibility
    hidden_count = max(0, int(total_count or 0) - int(visible_count or 0))
    st.info(
        f"يوجد {int(total_count or 0)} {label} في هذه الفئة. "
        f"(تم إخفاء {hidden_count} {label} لأنها في قائمة 'تمت المعالجة' أو مخفية يدوياً)."
    )


# ════════════════════════════════════════════════
#  Callbacks — أحداث الأزرار التفاعلية (Event-Driven)
#  تُعرَّف هنا (خارج حلقة الرسم) حتى تتوافق مع on_click.
#  ضمان: تُنفَّذ مرة واحدة بالضبط عند كل نقرة، والحالة تُحدَّث
#  تلقائياً قبل إعادة رسم الصفحة — بدون st.rerun() صريح.
# ════════════════════════════════════════════════
def _cb_send_make(
    prefix: str, idx,
    our_name: str, comp_name: str,
    our_price: float, comp_price: float, diff: float,
    decision: str, comp_src: str, pid: str, comp_url: str,
) -> None:
    """
    Callback: إرسال تحديث سعر واحد إلى Make.com عبر on_click.
    يقرأ السعر المستهدف من st.session_state لضمان القراءة اللحظية.
    """
    _price_key = f"target_price_{prefix}_{idx}"
    _tp = float(st.session_state.get(_price_key, 0) or 0)
    if _tp <= 0:
        st.session_state[f"_act_{prefix}_{idx}"] = (
            "error", "❌ السعر يجب أن يكون أكبر من صفر"
        )
        return

    # FIX: Transparency & Reversibility
    _mk_res = send_single_product({
        "product_id": pid,
        "name": our_name,
        "price": float(_tp),
        "comp_name": comp_name,
        "comp_price": comp_price,
        "diff": diff,
        "decision": decision,
        "competitor": comp_src,
        "comp_url": comp_url or "",
    })
    _mk_status = int(_mk_res.get("status_code") or 0)
    _ok = bool(_mk_res.get("success")) and _mk_status in (200, 201, 202, 204)

    _hk = f"{prefix}_{our_name}_{idx}"
    if _ok:
        _track_processed_price_sku(pid)  # FIX: Smart Workflow & AI Tracking
        _track_processed_missing_url(comp_url)  # FIX: Smart Workflow & AI Tracking
        st.session_state.hidden_products.add(_hk)
        try:
            save_hidden_product(_hk, our_name, "sent_to_make")
            save_processed(
                _hk, our_name, comp_src, "send_price",
                old_price=our_price, new_price=_tp, product_id=pid,
                notes=f"Make ← {prefix} | {comp_src} | {comp_price:.0f}→{_tp:.0f}ر.س",
            )
        except Exception:
            pass
        # toast يُعرض على مستوى الصفحة بعد إعادة الرسم
        st.session_state["_action_toast"] = (
            "success", f"✅ تم إرسال «{our_name}» ← {_tp:,.0f} ر.س"
        )
        st.rerun()  # FIX: Smart Workflow & AI Tracking
    else:
        st.session_state[f"_act_{prefix}_{idx}"] = (
            "error", f"❌ فشل الإرسال إلى Make ({_mk_status or 'NO_HTTP'}) — لم يتم تعليم المنتج كمعالج."
        )
        st.error(f"❌ فشل الإرسال إلى Make: {_mk_res.get('message', 'خطأ غير معروف')}")  # FIX: Transparency & Reversibility


def _cb_exclude(
    prefix: str, idx,
    our_name: str, our_price: float,
    comp_price: float, diff: float,
    comp_src: str, pid: str,
) -> None:
    """Callback: استبعاد المنتج وحفظه في DB عبر on_click."""
    st.session_state[f"excluded_{prefix}_{idx}"] = True
    st.session_state.hidden_products.add(f"{prefix}_{our_name}_{idx}")
    st.session_state.decisions_pending[our_name] = {
        "action": "removed", "reason": "استبعاد",
        "our_price": our_price, "comp_price": comp_price,
        "diff": diff, "competitor": comp_src,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    try:
        _hk = f"{prefix}_{our_name}_{idx}"
        log_decision(our_name, prefix, "removed", "استبعاد",
                     our_price, comp_price, diff, comp_src)
        save_hidden_product(_hk, our_name, "removed")
        save_processed(
            _hk, our_name, comp_src, "removed",
            old_price=our_price, new_price=our_price, product_id=pid,
            notes=f"استبعاد من {prefix}",
        )
    except Exception:
        pass


# ════════════════════════════════════════════════
#  مكوّن جدول المقارنة البصري (مشترك)
# ════════════════════════════════════════════════
def render_pro_table(df, prefix, section_type="update", show_search=True,
                     compact_cards=False, inline_filters=True):
    """
    جدول احترافي بصري مع:
    - فلاتر ذكية (مكشوفة في شبكة أو داخل Expander)
    - أزرار AI + قرار لكل منتج (Event-Driven via on_click)
    - تصدير Make
    - Pagination
    """
    if df is None or df.empty:
        st.info("لا توجد منتجات")
        return

    # ── تطبيق الفلاتر العالمية (Global Quick-Filters من الشريط الجانبي) ──
    df = apply_global_filters(df)
    if df.empty:
        _gf_sum = get_active_filter_summary()
        st.info(f"لا توجد منتجات تطابق الفلاتر الحالية ({_gf_sum})" if _gf_sum
                else "لا توجد منتجات")
        return

    # ── فلاتر ─────────────────────────────────
    opts = get_filter_options(df)
    if inline_filters:
        st.markdown(
            '<div class="filter-inline-wrap">'
            '<div class="filter-inline-title">🔍 فلاتر — بحث، ماركة، منافس، نوع</div></div>',
            unsafe_allow_html=True,
        )
        c1, c2, c3, c4 = st.columns([1.15, 1, 1, 1])
        search = c1.text_input("🔎 بحث", key=f"{prefix}_s")
        brand_f = c2.selectbox("🏷️ الماركة", opts["brands"], key=f"{prefix}_b")
        comp_f = c3.selectbox("🏪 المنافس", opts["competitors"], key=f"{prefix}_c")
        type_f = c4.selectbox("🧴 النوع", opts["types"], key=f"{prefix}_t")
        c5, c6, c7 = st.columns([1.2, 1, 1])
        match_min = c5.slider("أقل تطابق %", 0, 100, 0, key=f"{prefix}_m")
        price_min = c6.number_input("سعر من", 0.0, key=f"{prefix}_p1")
        price_max = c7.number_input("سعر إلى", 0.0, key=f"{prefix}_p2")
    else:
        with st.expander("🔍 فلاتر متقدمة", expanded=False):
            c1, c2, c3, c4 = st.columns(4)
            search = c1.text_input("🔎 بحث", key=f"{prefix}_s")
            brand_f = c2.selectbox("🏷️ الماركة", opts["brands"], key=f"{prefix}_b")
            comp_f = c3.selectbox("🏪 المنافس", opts["competitors"], key=f"{prefix}_c")
            type_f = c4.selectbox("🧴 النوع", opts["types"], key=f"{prefix}_t")
            c5, c6, c7 = st.columns(3)
            match_min = c5.slider("أقل تطابق%", 0, 100, 0, key=f"{prefix}_m")
            price_min = c6.number_input("سعر من", 0.0, key=f"{prefix}_p1")
            price_max = c7.number_input("سعر لـ", 0.0, key=f"{prefix}_p2")

    filters = {
        "search": search, "brand": brand_f, "competitor": comp_f,
        "type": type_f,
        "match_min": match_min if match_min > 0 else None,
        "price_min": price_min if price_min > 0 else 0.0,
        "price_max": price_max if price_max > 0 else None,
    }
    filtered = apply_filters(df, filters)

    # ── شريط الأدوات ───────────────────────────
    ac1, ac2, ac3, ac4, ac5 = st.columns(5)
    with ac1:
        _exdf = filtered.copy()
        if "جميع المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع_المنافسين"])
        excel_data = export_to_excel(_exdf, prefix)
        st.download_button("📥 Excel", data=excel_data,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{prefix}_xl")
    with ac2:
        _csdf = filtered.copy()
        if "جميع المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع_المنافسين"])
        _csv_bytes = _csdf.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("📄 CSV", data=_csv_bytes,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv", key=f"{prefix}_csv")
    with ac3:
        _bulk_labels = {"raise": "🤖 تحليل ذكي — خفض (أول 20)",
                        "lower": "🤖 تحليل ذكي — رفع (أول 20)",
                        "review": "🤖 تحقق جماعي (أول 20)",
                        "approved": "🤖 مراجعة (أول 20)"}
        if st.button(_bulk_labels.get(prefix, "🤖 AI جماعي (أول 20)"), key=f"{prefix}_bulk"):
            with st.spinner("🤖 AI يحلل البيانات..."):
                _section_map = {"raise": "price_raise", "lower": "price_lower",
                                "review": "review", "approved": "approved"}
                items = [{
                    "our": str(r.get("المنتج", "")),
                    "comp": str(r.get("منتج_المنافس", "")),
                    "our_price": safe_float(r.get("السعر", 0)),
                    "comp_price": safe_float(r.get("سعر_المنافس", 0))
                } for _, r in filtered.head(20).iterrows()]
                res = bulk_verify(items, _section_map.get(prefix, "general"))
                st.markdown(f'<div class="ai-box">{res["response"]}</div>',
                            unsafe_allow_html=True)
    with ac4:
        if section_type == "excluded":
            st.caption("إرسال Make غير متاح لهذا القسم")
        elif st.button("📤 إرسال كل لـ Make", key=f"{prefix}_make_all"):
            products = export_to_make_format(filtered, section_type)
            if section_type in ("missing", "new"):
                res = send_new_products(products)
            else:
                res = send_price_updates(products)
            _mk_status = int(res.get("status_code") or 0)  # FIX: Transparency & Reversibility
            _mk_ok = bool(res.get("success")) and _mk_status in (200, 201, 202, 204)  # FIX: Transparency & Reversibility
            if _mk_ok:
                if section_type in ("missing", "new"):  # FIX: Smart Workflow & AI Tracking
                    if "رابط_المنافس" in filtered.columns:
                        for _u in filtered["رابط_المنافس"].dropna().astype(str):
                            _track_processed_missing_url(_u)
                else:
                    if "معرف_المنتج" in filtered.columns:
                        for _pid in filtered["معرف_المنتج"].dropna().astype(str):
                            _track_processed_price_sku(_pid)
                st.success(res["message"])
                # v26: سجّل كل منتج في processed_products
                for _i, (_idx, _r) in enumerate(filtered.iterrows()):
                    _pname = str(_r.get("المنتج", _r.get("منتج_المنافس", "")))
                    _pkey  = f"{prefix}_{_pname}_{_i}"
                    _pid_r = str(_r.get("معرف_المنتج", _r.get("معرف_المنافس", "")))
                    _comp  = str(_r.get("المنافس",""))
                    _op    = safe_float(_r.get("السعر", _r.get("سعر_المنافس", 0)))
                    _np    = safe_float(_r.get("سعر_المنافس", _r.get("السعر", 0)))
                    st.session_state.hidden_products.add(_pkey)
                    save_hidden_product(_pkey, _pname, "sent_to_make_bulk")
                    save_processed(_pkey, _pname, _comp, "send_price",
                                   old_price=_op, new_price=_np,
                                   product_id=_pid_r,
                                   notes=f"إرسال جماعي ← {prefix}")
                st.rerun()
            else:
                st.error(f"❌ فشل الإرسال إلى Make: {res.get('message', 'خطأ غير معروف')}")  # FIX: Transparency & Reversibility
    with ac5:
        # جمع القرارات المعلقة وإرسالها
        pending = {k: v for k, v in st.session_state.decisions_pending.items()
                   if v["action"] in ["approved", "deferred", "removed"]}
        if pending and st.button(f"📦 ترحيل {len(pending)} قرار → Make", key=f"{prefix}_send_decisions"):
            to_send = [{"name": k, "action": v["action"], "reason": v.get("reason", "")}
                       for k, v in pending.items()]
            res = send_price_updates(to_send)
            st.success(f"✅ تم إرسال {len(to_send)} قرار لـ Make")
            # v26: سجّل القرارات المعلقة في processed_products
            for k, v in pending.items():
                _pkey = f"decision_{k}"
                _act  = v.get("action","approved")
                save_processed(_pkey, k, v.get("competitor",""), _act,
                               old_price=safe_float(v.get("our_price",0)),
                               new_price=safe_float(v.get("comp_price",0)),
                               notes=f"قرار معلق → Make | {v.get('reason','')}")
            st.session_state.decisions_pending = {}
            st.rerun()

    # FIX: Transparency & Reversibility
    _hidden_in_view = 0
    for _idx, _row in filtered.iterrows():
        _our_name_h = str(_row.get("المنتج", "—"))
        _hide_key_h = f"{prefix}_{_our_name_h}_{_idx}"
        if _hide_key_h in st.session_state.hidden_products:
            _hidden_in_view += 1
            continue
        if prefix in ("raise", "lower") and st.session_state.get(f"excluded_{prefix}_{_idx}"):
            _hidden_in_view += 1
    _show_transparency_counter(len(df), max(0, len(filtered) - _hidden_in_view))
    st.caption(f"عرض {len(filtered)} من {len(df)} منتج — {datetime.now().strftime('%H:%M:%S')}")

    # ── Pagination ─────────────────────────────
    PAGE_SIZE = 20 if (compact_cards and prefix == "raise") else 25
    total_pages = max(1, (len(filtered) + PAGE_SIZE - 1) // PAGE_SIZE)
    if total_pages > 1:
        page_num = st.number_input("الصفحة", 1, total_pages, 1, key=f"{prefix}_pg")
    else:
        page_num = 1
    start = (page_num - 1) * PAGE_SIZE
    page_df = filtered.iloc[start:start + PAGE_SIZE]

       # ── الجدول البصري ─────────────────────
    for idx, row in page_df.iterrows():
        our_name   = str(row.get("المنتج", "—"))
        # تخطي المنتجات التي أُرسلت لـ Make أو أُزيلت
        _hide_key = f"{prefix}_{our_name}_{idx}"
        if _hide_key in st.session_state.hidden_products:
            continue
        if prefix in ("raise", "lower") and st.session_state.get(f"excluded_{prefix}_{idx}"):
            continue
        comp_name  = str(row.get("منتج_المنافس", "—"))
        our_price  = safe_float(row.get("السعر", 0))
        comp_price = safe_float(row.get("سعر_المنافس", 0))
        diff       = safe_float(row.get("الفرق", our_price - comp_price))
        match_pct  = safe_float(row.get("نسبة_التطابق", 0))
        all_comps  = _normalize_all_competitors(row.get("جميع_المنافسين", row.get("جميع المنافسين", [])))
        comp_src   = _display_competitor_name(row)
        brand      = str(row.get("الماركة", ""))
        size       = row.get("الحجم", "")
        ptype      = str(row.get("النوع", ""))
        risk       = str(row.get("الخطورة", ""))
        decision   = str(row.get("القرار", ""))
        ts_now     = datetime.now().strftime("%Y-%m-%d %H:%M")
        _is_excluded = "مستبعد" in decision
        _vs_border = "#9e9e9e" if _is_excluded else None
        _vs_row_bg = "rgba(245,245,245,0.07)" if _is_excluded else None

        # سحب رقم المنتج من جميع الأعمدة المحتملة
        _pid_raw = (
            row.get("معرف_المنتج", "") or
            row.get("product_id", "") or
            row.get("رقم المنتج", "") or
            row.get("رقم_المنتج", "") or
            row.get("معرف المنتج", "") or ""
        )
        _pid_str = ""
        if _pid_raw and str(_pid_raw) not in ("", "nan", "None", "0"):
            try: _pid_str = str(int(float(str(_pid_raw))))
            except: _pid_str = str(_pid_raw)

        _our_img_v, _comp_img_v = row_media_urls_from_analysis(row)
        _comp_url_v = competitor_product_url_from_row(row)
        _our_url_v = our_product_url_from_row(row)

        # بطاقة VS مع رقم المنتج + صور (lazy) عند توفرها — وضع مضغوط لقسم «سعر أعلى»
        _vs_compact = bool(compact_cards and prefix == "raise")
        _vs_html = vs_card(our_name, our_price, comp_name,
                           comp_price, diff, comp_src, _pid_str,
                           our_img=_our_img_v, comp_img=_comp_img_v,
                           comp_url=_comp_url_v, our_url=_our_url_v,
                           accent_border=_vs_border, row_bg=_vs_row_bg,
                           compact=_vs_compact)
        st.markdown(_vs_html, unsafe_allow_html=True)

        # شريط المعلومات
        match_color = ("#00C853" if match_pct >= 90
                       else "#FFD600" if match_pct >= 70 else "#FF1744")
        risk_html = ""
        if risk:
            rc = {"حرج": "#FF1744", "عالي": "#FF1744", "متوسط": "#FFD600", "منخفض": "#00C853", "عادي": "#00C853"}.get(risk.replace("🔴 ","").replace("🟡 ","").replace("🟢 ",""), "#888")
            risk_html = f'<span style="color:{rc};font-size:.75rem;font-weight:700">⚡{risk}</span>'

        # تاريخ آخر تغيير سعر
        ph = get_price_history(our_name, comp_src, limit=2)
        price_change_html = ""
        if len(ph) >= 2:
            old_p = ph[1]["price"]
            chg = ph[0]["price"] - old_p
            chg_c = "#FF1744" if chg > 0 else "#00C853"
            price_change_html = f'<span style="color:{chg_c};font-size:.7rem">{"▲" if chg>0 else "▼"}{abs(chg):.0f} منذ {ph[1]["date"]}</span>'

        # قرار معلق؟
        pend = st.session_state.decisions_pending.get(our_name, {})
        pend_html = decision_badge(pend.get("action", "")) if pend else ""

        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;
                    padding:3px 12px;font-size:.8rem;flex-wrap:wrap;gap:4px;">
          <span>🏷️ <b>{brand}</b> {size} {ptype}</span>
          <span>تطابق: <b style="color:{match_color}">{match_pct:.0f}%</b></span>
          {risk_html}
          {price_change_html}
          {pend_html}
          {ts_badge(ts_now)}
        </div>""", unsafe_allow_html=True)

        # شريط المنافسين المصغر — يعرض كل المنافسين بأسعارهم بعد التطبيع وإزالة التكرار
        if all_comps:
            st.markdown(comp_strip(all_comps), unsafe_allow_html=True)

        # ── شريط الإجراءات التفاعلي (Event-Driven via on_click) ─────────
        if prefix in ("raise", "lower"):
            st.write("")
            _suggested = float(comp_price) - 1.0 if comp_price > 0 else float(our_price)
            if _suggested <= 0:
                _suggested = float(our_price)

            # pid يُحسب هنا لأنه مطلوب كـ arg للـ Callbacks
            _pid_cb_raw = (
                row.get("معرف_المنتج", "") or row.get("product_id", "")
                or row.get("رقم المنتج", "") or row.get("رقم_المنتج", "")
                or row.get("معرف المنتج", "") or ""
            )
            try:
                _fv_cb = float(_pid_cb_raw)
                _pid_cb = str(int(_fv_cb)) if _fv_cb == int(_fv_cb) else str(_pid_cb_raw)
            except (ValueError, TypeError):
                _pid_cb = str(_pid_cb_raw).strip()
            if _pid_cb in ("nan", "None", "NaN", ""):
                _pid_cb = ""

            _comp_url_make = (_comp_url_v or str(row.get("رابط_المنافس", "") or "")).strip()

            act_col1, act_col2, act_col3, _act_sp = st.columns([2.5, 2.5, 2, 4])
            with act_col1:
                st.number_input(
                    "🎯 السعر المستهدف (ر.س)",
                    value=float(_suggested),
                    min_value=0.0,
                    step=1.0,
                    key=f"target_price_{prefix}_{idx}",
                )
            with act_col2:
                st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                st.button(
                    "🚀 تحديث السعر (Make)",
                    key=f"send_make_{prefix}_{idx}",
                    type="primary",
                    use_container_width=True,
                    on_click=_cb_send_make,
                    args=(
                        prefix, idx, our_name, comp_name,
                        our_price, comp_price, diff,
                        decision, comp_src, _pid_cb, _comp_url_make,
                    ),
                )
            with act_col3:
                st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                st.button(
                    "🗑️ استبعاد",
                    key=f"reject_bar_{prefix}_{idx}",
                    use_container_width=True,
                    on_click=_cb_exclude,
                    args=(
                        prefix, idx, our_name, our_price,
                        comp_price, diff, comp_src, _pid_cb,
                    ),
                )
            # عرض نتيجة الإجراء (خطأ فقط؛ النجاح يُعرض كـ toast أعلى الصفحة)
            _act_res = st.session_state.pop(f"_act_{prefix}_{idx}", None)
            if _act_res:
                _atype, _amsg = _act_res
                st.error(_amsg) if _atype == "error" else st.success(_amsg)

            _hr_act = (
                '<hr style="border:none;border-top:1px solid #2a2a3d;margin:10px 0 14px">'
                if _vs_compact
                else "<hr style='margin:16px 0;border-top:2px dashed rgba(238,238,238,.25);'>"
            )
            st.markdown(_hr_act, unsafe_allow_html=True)

        # ── تعريف الأعمدة (b1..ba) لجميع الأقسام لضمان عدم حدوث UnboundLocalError ──
        if prefix in ("raise", "lower"):
            # b1:AI, b2:Market, b3:OK, b4:Defer, b8:Verify, b9:History, ba:Analyze
            b1, b2, b3, b4, b8, b9, ba = st.columns([1, 1, 1, 1, 1, 1, 1])
        elif prefix == "approved":
            # b1:AI, b2:Market, b3:OK, b4:Defer, b5:Remove, b6:Price, b7:Make
            b1, b2, b3, b4, b5, b6, b7 = st.columns(7)
        else:
            # b1..ba (10 columns)
            b1, b2, b3, b4, b5, b6, b7, b8, b9, ba = st.columns([1, 1, 1, 1, 1, 1, 1, 1, 1, 1])

        with b1:  # AI تحقق ذكي — يُصحح القسم
            _ai_label = {"raise": "🤖 هل نخفض؟", "lower": "🤖 هل نرفع؟",
                         "review": "🤖 هل يطابق؟", "approved": "🤖 تحقق"}.get(prefix, "🤖 تحقق")
            if st.button(_ai_label, key=f"v_{prefix}_{idx}"):
                with st.spinner("🤖 AI يحلل ويتحقق..."):
                    r = verify_match(our_name, comp_name, our_price, comp_price)
                    if r.get("success"):
                        icon = "✅" if r.get("match") else "❌"
                        conf = r.get("confidence", 0)
                        reason = r.get("reason","")[:200]
                        correct_sec = r.get("correct_section","")
                        suggested_price = r.get("suggested_price", 0)

                        # تحديد القسم الحالي من prefix
                        current_sec_map = {
                            "raise": "🔴 سعر أعلى",
                            "lower": "🟢 سعر أقل",
                            "approved": "✅ موافق",
                            "review": "⚠️ تحت المراجعة",
                            "excluded": "⚪ مستبعد (لا يوجد تطابق)",
                        }
                        current_sec = current_sec_map.get(prefix, "")

                        # هل AI يوافق على القسم الحالي؟
                        section_ok = True
                        if correct_sec and current_sec:
                            # مقارنة مبسطة
                            if ("اعلى" in correct_sec or "أعلى" in correct_sec) and prefix != "raise":
                                section_ok = False
                            elif ("اقل" in correct_sec or "أقل" in correct_sec) and prefix != "lower":
                                section_ok = False
                            elif "موافق" in correct_sec and prefix != "approved":
                                section_ok = False
                            elif ("مفقود" in correct_sec or "🔵" in correct_sec) and r.get("match") == False:
                                section_ok = False

                        if r.get("match"):
                            # مطابقة صحيحة — عرض نتيجة السعر
                            diff_info = ""
                            if prefix == "raise":
                                diff_info = f"\n\n💡 **توصية:** {'خفض السعر' if diff > 20 else 'إبقاء السعر'}"
                            elif prefix == "lower":
                                diff_info = f"\n\n💡 **توصية:** {'رفع السعر' if abs(diff) > 20 else 'إبقاء السعر'}"
                            if suggested_price > 0:
                                diff_info += f"\n💰 **السعر المقترح: {suggested_price:,.0f} ر.س**"

                            st.success(f"{icon} **تطابق {conf}%** — المطابقة صحيحة\n\n{reason}{diff_info}")

                            if not section_ok:
                                st.warning(f"⚠️ AI يرى أن هذا المنتج يجب أن يكون في قسم: **{correct_sec}**")
                        else:
                            # مطابقة خاطئة — تنبيه
                            st.error(f"{icon} **المطابقة خاطئة** ({conf}%)\n\n{reason}")
                            st.warning("🔵 هذا المنتج يجب أن يكون في **المنتجات المفقودة**")
                    else:
                        st.error("فشل AI")

        with b2:  # بحث سعر السوق ذكي
            _mkt_label = {"raise": "🌐 سعر عادل؟", "lower": "🌐 فرصة رفع؟"}.get(prefix, "🌐 سوق")
            if st.button(_mkt_label, key=f"mkt_{prefix}_{idx}"):
                with st.spinner("🌐 يبحث في السوق السعودي..."):
                    r = search_market_price(our_name, our_price)
                    if r.get("success"):
                        mp  = r.get("market_price", 0)
                        rng = r.get("price_range", {})
                        rec = r.get("recommendation", "")[:250]
                        web_ctx = r.get("web_context","")
                        comps = r.get("competitors", [])
                        conf = r.get("confidence", 0)

                        _verdict = ""
                        if prefix == "raise" and mp > 0:
                            _verdict = "✅ سعرنا ضمن السوق" if our_price <= mp * 1.1 else "⚠️ سعرنا أعلى من السوق — يُنصح بالخفض"
                        elif prefix == "lower" and mp > 0:
                            _gap = mp - our_price
                            _verdict = f"💰 فرصة رفع ~{_gap:.0f} ر.س" if _gap > 10 else "✅ سعرنا قريب من السوق"

                        _comps_txt = ""
                        if comps:
                            _comps_txt = "\n\n**منافسون:**\n" + "\n".join(
                                f"• {c.get('name','')}: {c.get('price',0):,.0f} ر.س" for c in comps[:3]
                            )

                        _price_range = f"{rng.get('min',0):.0f}–{rng.get('max',0):.0f}" if rng else "—"
                        st.info(
                            f"💹 **سعر السوق: {mp:,.0f} ر.س** ({_price_range} ر.س)\n\n"
                            f"{rec}{_comps_txt}\n\n{'**' + _verdict + '**' if _verdict else ''}"
                        )
                        if web_ctx:
                            with st.expander("🔍 مصادر البحث"):
                                st.caption(web_ctx)
                    else:
                        st.warning("تعذر البحث في السوق")

        with b3:  # موافق
            if st.button("✅ موافق", key=f"ok_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "approved", "reason": "موافقة يدوية",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "approved",
                             "موافقة يدوية", our_price, comp_price, diff, comp_src)
                _hk3 = f"{prefix}_{our_name}_{idx}"
                st.session_state.hidden_products.add(_hk3)
                save_hidden_product(_hk3, our_name, "approved")
                # ── توجيه آلي → تمت المعالجة ──
                _auto_route_to_processed(
                    our_name, str(row.get("معرف_المنتج","")),
                    comp_src, "approved",
                    old_price=our_price, new_price=our_price,
                    notes=f"موافق من {prefix}",
                )
                st.rerun()

        with b4:  # تأجيل
            if st.button("⏸️ تأجيل", key=f"df_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "deferred", "reason": "تأجيل",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "deferred",
                             "تأجيل", our_price, comp_price, diff, comp_src)
                st.warning("⏸️")

        if prefix not in ("raise", "lower"):
            with b5:  # إزالة
                if st.button("🗑️ إزالة", key=f"rm_{prefix}_{idx}"):
                    st.session_state.decisions_pending[our_name] = {
                        "action": "removed", "reason": "إزالة",
                        "our_price": our_price, "comp_price": comp_price,
                        "diff": diff, "competitor": comp_src,
                        "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                    }
                    log_decision(our_name, prefix, "removed",
                                 "إزالة", our_price, comp_price, diff, comp_src)
                    _hk = f"{prefix}_{our_name}_{idx}"
                    st.session_state.hidden_products.add(_hk)
                    save_hidden_product(_hk, our_name, "removed")
                    save_processed(_hk, our_name, comp_src, "removed",
                                   old_price=our_price, new_price=our_price,
                                   product_id=str(row.get("معرف_المنتج","")),
                                   notes=f"إزالة من {prefix}")
                    st.rerun()

            with b6:  # سعر يدوي
                _auto_price_row = round(comp_price - 1, 2) if comp_price > 0 else our_price
                _custom_price = st.number_input(
                    "سعر", value=_auto_price_row, min_value=0.0,
                    step=1.0, key=f"cp_{prefix}_{idx}",
                    label_visibility="collapsed"
                )

            with b7:  # تصدير Make
                if st.button("📤 Make", key=f"mk_{prefix}_{idx}"):
                    _pid_raw = (
                        row.get("معرف_المنتج", "") or
                        row.get("product_id", "") or
                        row.get("رقم المنتج", "") or
                        row.get("رقم_المنتج", "") or
                        row.get("معرف المنتج", "") or ""
                    )
                    try:
                        _fv = float(_pid_raw)
                        _pid = str(int(_fv)) if _fv == int(_fv) else str(_pid_raw)
                    except (ValueError, TypeError):
                        _pid = str(_pid_raw).strip()
                    if _pid in ("nan", "None", "NaN", ""):
                        _pid = ""
                    _final_price = _custom_price if _custom_price > 0 else _auto_price_row
                    res = send_single_product({
                        "product_id": _pid,
                        "name": our_name, "price": _final_price,
                        "comp_name": comp_name, "comp_price": comp_price,
                        "diff": diff, "decision": decision, "competitor": comp_src
                    })
                    if res["success"]:
                        _hk = f"{prefix}_{our_name}_{idx}"
                        _track_processed_price_sku(_pid)  # FIX: Smart Workflow & AI Tracking
                        st.session_state.hidden_products.add(_hk)
                        save_hidden_product(_hk, our_name, "sent_to_make")
                        # ── توجيه آلي → تمت المعالجة ──
                        _auto_route_to_processed(
                            our_name, _pid,
                            comp_src, "send_price",
                            old_price=our_price, new_price=_final_price,
                            notes=f"إرسال لـ Make من {prefix}",
                        )
                        st.success(f"✅ تم الإرسال: {_pid}")
                        st.rerun()

        if prefix != "approved":
            with b8:  # تحقق AI — يُصحح القسم (مكرر مع b1 في «موافق» فأُزيل من قسم الموافقات)
                if st.button("🔍 تحقق", key=f"vrf_{prefix}_{idx}"):
                    with st.spinner("🤖 يتحقق..."):
                        _vr2 = verify_match(our_name, comp_name, our_price, comp_price)
                        if _vr2.get("success"):
                            _mc2 = "✅ متطابق" if _vr2.get("match") else "❌ غير متطابق"
                            _conf2 = _vr2.get("confidence",0)
                            _sec2 = _vr2.get("correct_section","")
                            _reason2 = _vr2.get("reason","")[:150]
                            st.markdown(f"{_mc2} {_conf2}%\n\n{_reason2}")
                            if _sec2 and not _vr2.get("match"):
                                st.warning(f"يجب نقله → **{_sec2}**")

            with b9:  # تاريخ السعر
                if st.button("📈 تاريخ", key=f"ph_{prefix}_{idx}"):
                    history = get_price_history(our_name, comp_src)
                    if history:
                        rows_h = [f"📅 {h['date']}: {h['price']:,.0f} ر.س" for h in history[:5]]
                        st.info("\n".join(rows_h))
                    else:
                        st.info("لا يوجد تاريخ بعد")

            with ba:  # 📊 تحليل المنتج الموضعي
                if st.button("📊 تحليل", key=f"analyze_{prefix}_{idx}",
                             help="تحليل شامل: سعر + مطابقة + قسم صحيح"):
                    _section_map_an = {
                        "raise": "price_raise", "lower": "price_lower",
                        "approved": "approved", "review": "review",
                        "excluded": "excluded",
                    }
                    with st.spinner("🔍 يتم التحليل الآن..."):
                        an_res = analyze_product_inline(row, _section_map_an.get(prefix, prefix))
                        render_analysis_result(an_res)

        _hr_m = "3px 0" if (compact_cards and prefix == "raise") else "6px 0"
        st.markdown(
            f'<hr style="border:none;border-top:1px solid #1a1a2e;margin:{_hr_m}">',
            unsafe_allow_html=True,
        )


# ════════════════════════════════════════════════
# ════════════════════════════════════════════════
#  نظام التوجيه الآلي (Auto-Routing)
# ════════════════════════════════════════════════
def _auto_route_to_processed(our_name, our_id, comp_src, status, old_price=0, new_price=0, notes=""):
    """نقل المنتج تلقائياً إلى 'تمت المعالجة' وحفظ حالته في DB."""
    try:
        # 1. حفظ في جدول المعالجة
        save_processed(our_name, our_id, comp_src, status, old_price, new_price, notes)
        # 2. إخفاء من الواجهة (حفظ في جدول المنتجات المخفية)
        save_hidden_product(our_name, our_id, comp_src)
        return True
    except Exception as e:
        st.error(f"خطأ في التوجيه الآلي: {e}")
        return False


# ════════════════════════════════════════════════
#  الشريط الجانبي
# ════════════════════════════════════════════════
# تهيئة آمنة للتنقّل قبل رسم الشريط الجانبي.
# هذا يمنع NameError في أسفل الملف إذا تعذر تعيين قيمة `page`
# لأي سبب أثناء بناء عناصر الشريط الجانبي في بعض البيئات.
page = st.session_state.get("main_nav", SECTIONS[0] if SECTIONS else "📊 لوحة التحكم")
with st.sidebar:
    st.markdown(f"## {APP_ICON} {APP_TITLE}")
    st.caption(f"الإصدار {APP_VERSION}")

    # حالة AI — أي مزود (Gemini و/أو OpenRouter و/أو Cohere) يكفي للمسار الهجين
    ai_ok = ANY_AI_PROVIDER_CONFIGURED
    if ai_ok:
        ai_color = "#00C853"
        _ai_bits = []
        if GEMINI_API_KEYS:
            _ai_bits.append(f"Gemini×{len(GEMINI_API_KEYS)}")
        if (OPENROUTER_API_KEY or "").strip():
            _ai_bits.append("OpenRouter")
        if (COHERE_API_KEY or "").strip():
            _ai_bits.append("Cohere")
        ai_label = f"🤖 {' · '.join(_ai_bits)} ✅"
    else:
        ai_color = "#FF1744"
        ai_label = "🔴 AI غير متصل — أضف مفتاحاً (Gemini أو OpenRouter أو Cohere)"

    st.markdown(
        f'<div style="background:{ai_color}22;border:1px solid {ai_color};'
        f'border-radius:6px;padding:6px;text-align:center;color:{ai_color};'
        f'font-weight:700;font-size:.85rem">{ai_label}</div>',
        unsafe_allow_html=True
    )

    # زر تشخيص سريع — Railway يستخدم متغيرات البيئة وليس secrets.toml
    if not ai_ok:
        if st.button("🔍 تشخيص المشكلة", key="diag_btn"):
            import os

            def _mask(v: str) -> str:
                v = str(v or "").strip()
                if len(v) <= 12:
                    return "***" if v else ""
                return v[:8] + "…" + v[-4:]

            st.info(
                "على **Railway / Docker**: أضف **أحد** المسارات: `GEMINI_API_KEY` / `GEMINI_API_KEYS` "
                "أو **`OPENROUTER_API_KEY`** أو **`COHERE_API_KEY`** في Variables للخدمة "
                "(لا يعتمد التطبيق على ملف secrets.toml هناك). المحرك يجرّب Gemini ثم OpenRouter ثم Cohere."
            )
            st.write("**متغيرات البيئة — Gemini:**")
            _any = False
            for key_name in (
                "GEMINI_API_KEYS",
                "GEMINI_API_KEY",
                "GEMINI_KEY_1",
                "GEMINI_KEY_2",
                "GEMINI_KEY_3",
            ):
                raw = os.environ.get(key_name, "")
                if raw:
                    _any = True
                    st.success(f"✅ `{key_name}` = `{_mask(raw)}` (طول {len(raw)})")
                else:
                    st.caption(f"— `{key_name}` غير مضبوط")
            st.write("**متغيرات البيئة — بدائل (كافية بدون Gemini):**")
            for key_name in ("OPENROUTER_API_KEY", "OPENROUTER_KEY", "COHERE_API_KEY"):
                raw = os.environ.get(key_name, "")
                if raw:
                    _any = True
                    st.success(f"✅ `{key_name}` = `{_mask(raw)}` (طول {len(raw)})")
                else:
                    st.caption(f"— `{key_name}` غير مضبوط")
            st.write(
                f"**ما يقرأه التطبيق:** Gemini={len(GEMINI_API_KEYS)} | "
                f"OpenRouter={'نعم' if (OPENROUTER_API_KEY or '').strip() else 'لا'} | "
                f"Cohere={'نعم' if (COHERE_API_KEY or '').strip() else 'لا'}"
            )
            if not _any:
                st.warning(
                    "لم يُعثر على أي مفتاح. إما مفتاح **Google AI Studio** (`GEMINI_API_KEY`) "
                    "أو مفتاح **OpenRouter** (`OPENROUTER_API_KEY`) — الأخير يكفي لتشغيل مسار الـ fallback."
                )
            st.write("**Streamlit secrets (اختياري — Streamlit Cloud فقط):**")
            try:
                _sk = list(st.secrets.keys())
                for k in _sk:
                    val = str(st.secrets[k])
                    st.caption(f"  `{k}` = `{_mask(val)}`")
                if not _sk:
                    st.caption("لا مفاتيح — طبيعي على Railway عند الاعتماد على Variables فقط.")
            except Exception as e:
                st.caption(f"لا ملف secrets (طبيعي على Railway): {e}")

    # حالة المعالجة — تحديث حي مع auto-rerun
    if st.session_state.job_id:
        job = get_job_progress(st.session_state.job_id)
        if job:
            if job["status"] == "running":
                tot = max(int(job.get("total") or 0), 1)
                proc = min(int(job.get("processed") or 0), tot)
                pct = proc / tot
                pct_lbl = f"{100.0 * pct:.1f}%"
                st.progress(
                    min(pct, 0.99),
                    f"⚙️ {proc}/{tot} منتج — {pct_lbl}",
                )
                st.caption("تحليل خلفي — يُحدَّث كل بضع ثوانٍ. لا تغلق الصفحة حتى يكتمل.")
                # تحديث تلقائي كل 4 ثوانٍ بدون إعادة تشغيل الكود كاملاً
                try:
                    from streamlit_autorefresh import st_autorefresh
                    st_autorefresh(interval=4000, key="progress_refresh")
                except ImportError:
                    # fallback: rerun عادي إذا لم تكن المكتبة موجودة
                    # ✅ إصلاح: بدون time.sleep() في Main Thread — يجمّد الـ UI ويخالف القاعدة #2
                    # المستخدم يرى الشريط ويضغط تحديث يدوي، أو يثبّت streamlit-autorefresh
                    st.info(
                        "💡 للتحديث التلقائي: `pip install streamlit-autorefresh`  \n"
                        "أو اضغط الزر أدناه لتحديث الحالة يدوياً.",
                        icon="ℹ️",
                    )
                    if st.button("🔄 تحديث الحالة", key="manual_progress_refresh"):
                        st.rerun()
                # اكتمل — حمّل النتائج تلقائياً مع استعادة القوائم
                if job.get("results"):
                    _restored = restore_results_from_json(job["results"])
                    df_all = pd.DataFrame(_restored)
                    missing_df = pd.DataFrame(job.get("missing", [])) if job.get("missing") else pd.DataFrame()
                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results     = _r
                    st.session_state.analysis_df = df_all
                st.session_state.last_audit_stats = job.get("audit") or {}
                st.session_state.job_running = False
                st.balloons()
                st.rerun()
            elif job["status"].startswith("error"):
                st.error(f"❌ فشل: {job['status'][7:80]}")
    page = st.radio("الأقسام", SECTIONS, label_visibility="collapsed", key="main_nav")
    st.markdown("---")
    if st.session_state.results:
        r = st.session_state.results
        _all_df_summary = r.get("all", pd.DataFrame())
        _analysis_total = len(_all_df_summary) if isinstance(_all_df_summary, pd.DataFrame) else 0
        _selected_page = st.session_state.get("main_nav", "")
        _is_scraper_page = _selected_page == "🕷️ كشط المنافسين"

        if _is_scraper_page:
            st.info(
                "📊 توجد نتائج تحليل محفوظة، لكن تم إخفاء ملخصها هنا حتى لا يختلط مع أرقام الكشط الحالية. "
                "يمكنك مراجعة الملخص الكامل من صفحة «📊 لوحة التحكم»."
            )
        else:
            st.markdown("**📊 ملخص آخر تحليل:**")
            if _analysis_total:
                st.caption(f"يعرض توزيع **{_analysis_total:,}** من منتجاتنا المحللة، وليس عدد صفوف ملف المنافس.")
            _audit = st.session_state.get("last_audit_stats") or {}
            for key, icon, label in [
                ("price_raise","🔴","أعلى"), ("price_lower","🟢","أقل"),
                ("approved","✅","موافق"), ("missing","🔍","مفقود"),
                ("review","⚠️","مراجعة"), ("excluded","⚪","مستبعد"),
            ]:
                cnt = len(r.get(key, pd.DataFrame()))
                audit_key = {
                    "price_raise": "price_raise",
                    "price_lower": "price_lower",
                    "approved": "approved",
                    "missing": "missing",
                    "review": "review",
                    "excluded": "excluded",
                }.get(key)
                if audit_key and isinstance(_audit, dict):
                    try:
                        cnt = int(_audit.get(audit_key, cnt) or cnt)
                    except Exception:
                        pass
                st.caption(f"{icon} {label}: **{cnt}**")

            _miss_df = r.get("missing", pd.DataFrame())
            if not _miss_df.empty and "مستوى_الثقة" in _miss_df.columns:
                _gc = len(_miss_df[_miss_df["مستوى_الثقة"] == "green"])
                _yc = len(_miss_df[_miss_df["مستوى_الثقة"] == "yellow"])
                _rc = len(_miss_df[_miss_df["مستوى_الثقة"] == "red"])
                st.markdown(
                    f'<div style="background:#1a1a2e;border-radius:6px;padding:6px;margin-top:4px;font-size:.75rem">'
                    f'🟢 مؤكد: <b>{_gc}</b> &nbsp; '
                    f'🟡 محتمل: <b>{_yc}</b> &nbsp; '
                    f'🔴 مشكوك: <b>{_rc}</b></div>',
                    unsafe_allow_html=True)
    pending_cnt = len(st.session_state.decisions_pending)
    if pending_cnt:
        st.markdown(f'<div style="background:#FF174422;border:1px solid #FF1744;'
                    f'border-radius:6px;padding:6px;text-align:center;color:#FF1744;'
                    f'font-size:.8rem">📦 {pending_cnt} قرار معلق</div>',
                    unsafe_allow_html=True)

    # ── فلاتر سريعة عالمية في نهاية الشريط الجانبي ──
    if st.session_state.results:
        _all_df = st.session_state.results.get("all", pd.DataFrame())
        if not _all_df.empty:
            render_sidebar_filters(_all_df)

    # ── تحذيرات الفحص الذاتي — في الشريط الجانبي فقط ───────────────────
    _hs_sb = st.session_state.get("health_status", {})
    _sb_warns = _hs_sb.get("warnings", [])
    if _sb_warns:
        st.sidebar.markdown("---")
        for _w in _sb_warns:
            st.sidebar.caption(f"🔔 {_w}")


# إشعار خفيف بعد الانتقال من أزرار لوحة التحكم
if st.session_state.get("nav_flash"):
    _nf = st.session_state.pop("nav_flash", None)
    if _nf:
        if hasattr(st, "toast"):
            st.toast(_nf, icon="⏳")
        else:
            st.info(_nf)

# Toast نتائج Callbacks (إرسال Make / فشل)
_at = st.session_state.pop("_action_toast", None)
if _at:
    _at_type, _at_msg = _at
    if hasattr(st, "toast"):
        st.toast(_at_msg, icon="✅" if _at_type == "success" else "❌")
    elif _at_type == "success":
        st.success(_at_msg)
    else:
        st.error(_at_msg)


# ── الاعتماد النهائي والمتين لقيمة الصفحة الحالية ──
_fallback_page = SECTIONS[0] if SECTIONS else "📊 لوحة التحكم"
page = st.session_state.get("main_nav", _fallback_page)

# ════════════════════════════════════════════════
#  0. مصنع المنتجات (Magic Factory) — مدمج من pages/magic_factory.py
# ════════════════════════════════════════════════
if page == "✨ مصنع المنتجات":
    try:
        if _magic_factory_mod is not None and hasattr(_magic_factory_mod, "show"):
            _magic_factory_mod.show()
        elif _magic_factory_mod is not None:
            st.error("⚠️ دالة show() غير موجودة في ملف pages/magic_factory.py")
        else:
            st.error("❌ تعذّر تحميل وحدة مصنع المنتجات — تحقق من وجود الملف pages/magic_factory.py")
    except Exception as _mf_render_err:
        st.error(f"❌ خطأ في تشغيل مصنع المنتجات: {_mf_render_err}")


# ════════════════════════════════════════════════
#  1. لوحة التحكم
# ════════════════════════════════════════════════
if page == "📊 لوحة التحكم":
    st.header("📊 لوحة التحكم")
    db_log("dashboard", "view")
    if st.session_state.get("last_audit_stats"):
        try:
            _render_audit_bar(st.session_state.last_audit_stats)
            _render_reconciliation_dashboard(st.session_state.last_audit_stats)
        except Exception as _dash_render_err:
            st.error(f"⚠️ خطأ في عرض لوحة المحاسبة: {_dash_render_err}")

    # تغييرات الأسعار
    changes = get_price_changes(7)
    if changes:
        st.markdown("#### 🔔 تغييرات أسعار آخر 7 أيام")
        c_df = pd.DataFrame(changes)
        st.dataframe(c_df[["product_name","competitor","old_price","new_price",
                            "price_diff","new_date"]].rename(columns={
            "product_name": "المنتج", "competitor": "المنافس",
            "old_price": "السعر السابق", "new_price": "السعر الجديد",
            "price_diff": "التغيير", "new_date": "التاريخ"
        }).head(200), use_container_width=True, height=200)
        st.markdown("---")

    if st.session_state.results:
        r = st.session_state.results
        _analysis_total_dash = len(r.get("all", pd.DataFrame())) if isinstance(r.get("all", pd.DataFrame()), pd.DataFrame) else 0
        if _analysis_total_dash:
            st.caption(f"ملخص هذه الصفحة يخص آخر تحليل محفوظ لعدد **{_analysis_total_dash:,}** من منتجاتنا.")
        _dash_nav = [
            ("🔴 سعر أعلى", "🔴", "سعر أعلى", "price_raise"),
            ("🟢 سعر أقل", "🟢", "سعر أقل", "price_lower"),
            ("✅ موافق عليها", "✅", "موافق", "approved"),
            ("🔍 منتجات مفقودة", "🔍", "مفقود", "missing"),
            ("⚠️ تحت المراجعة", "⚠️", "مراجعة", "review"),
            ("⚪ مستبعد (لا يوجد تطابق)", "⚪", "مستبعد", "excluded"),
        ]
        cols = st.columns(6)
        for col, (sec_title, icon, short_lbl, rkey) in zip(cols, _dash_nav):
            val = len(r.get(rkey, pd.DataFrame()))
            with col:
                if st.button(
                    f"{icon} {val}\n{short_lbl}",
                    key=f"dash_go_{rkey}",
                    use_container_width=True,
                    help=f"انتقل إلى {sec_title}",
                ):
                    st.session_state._nav_pending = sec_title
                    st.session_state.nav_flash = f"➡️ {sec_title}"
                    st.rerun()

        # ملخص الثقة للمفقودات في لوحة التحكم
        _miss_dash = r.get("missing", pd.DataFrame())
        if not _miss_dash.empty and "مستوى_الثقة" in _miss_dash.columns:
            _g = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "green"])
            _y = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "yellow"])
            _rd = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "red"])
            st.markdown(
                f'<div style="display:flex;gap:12px;justify-content:center;padding:8px;'
                f'background:#1a1a2e;border-radius:8px;margin:8px 0">'
                f'<span style="color:#00C853">🟢 مؤكد: <b>{_g}</b></span>'
                f'<span style="color:#FFD600">🟡 محتمل: <b>{_y}</b></span>'
                f'<span style="color:#FF1744">🔴 مشكوك: <b>{_rd}</b></span>'
                f'</div>', unsafe_allow_html=True)

        st.markdown("---")
        cc1, cc2 = st.columns(2)
        with cc1:
            sheets = {}
            for key, name in [("price_raise","سعر_أعلى"),("price_lower","سعر_أقل"),
                               ("approved","موافق"),("missing","مفقود"),("review","مراجعة"),
                               ("excluded","مستبعد")]:
                if key in r and not r[key].empty:
                    df_ex = r[key].copy()
                    if "جميع المنافسين" in df_ex.columns:
                        df_ex = df_ex.drop(columns=["جميع المنافسين"])
                    sheets[name] = df_ex
            if sheets:
                excel_all = export_multiple_sheets(sheets)
                st.download_button("📥 تصدير كل الأقسام Excel",
                    data=excel_all, file_name="mahwous_all.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with cc2:
            if st.button("📤 إرسال كل شيء لـ Make (دفعات ذكية)",
                         key="dash_send_all_make"):
                _prog_all = st.progress(0, text="جاري الإرسال...")
                _status_all = st.empty()
                _sent_total = 0
                _fail_total = 0
                _sections = [
                    ("price_raise", "raise", "update", "🔴 سعر أعلى"),
                    ("price_lower", "lower", "update", "🟢 سعر أقل"),
                    ("approved",    "approved", "update", "✅ موافق"),
                    ("missing",     "missing", "new", "🔍 مفقودة"),
                ]
                for _si, (_key, _sec, _btype, _label) in enumerate(_sections):
                    if _key in r and not r[_key].empty:
                        _p = export_to_make_format(r[_key], _sec)
                        _res = send_batch_smart(_p, batch_type=_btype, batch_size=20, max_retries=3)
                        _full_success = (_res.get("sent", 0) == len(_p)) and (_res.get("failed", 0) == 0)  # FIX: Transparency & Reversibility
                        if _full_success:
                            if _key == "missing":
                                if "رابط_المنافس" in r[_key].columns:
                                    for _u in r[_key]["رابط_المنافس"].dropna().astype(str):
                                        _track_processed_missing_url(_u)
                            else:
                                if "معرف_المنتج" in r[_key].columns:
                                    for _pid in r[_key]["معرف_المنتج"].dropna().astype(str):
                                        _track_processed_price_sku(_pid)
                        elif _res.get("failed", 0) > 0:
                            st.error(f"❌ {_label}: فشل جزئي/كامل، لم يتم وسم المنتجات كـ(تمت المعالجة).")  # FIX: Transparency & Reversibility
                        _sent_total += _res.get("sent", 0)
                        _fail_total += _res.get("failed", 0)
                        _status_all.caption(f"{_label}: ✅ {_res.get('sent',0)} | ❌ {_res.get('failed',0)}")
                    _prog_all.progress((_si + 1) / len(_sections), text=f"جاري: {_label}")
                _prog_all.progress(1.0, text="اكتمل")
                st.success(f"✅ تم إرسال {_sent_total} منتج لـ Make!" + (f" (فشل {_fail_total})" if _fail_total else ""))
    else:
        # استئناف آخر job؟
        last = get_last_job()
        if last and last["status"] == "done" and last.get("results"):
            st.info(f"💾 يوجد تحليل محفوظ من {last.get('updated_at','')}")
            if st.button("🔄 استعادة النتائج المحفوظة"):
                _restored_last = restore_results_from_json(last["results"])
                df_all = pd.DataFrame(_restored_last)
                if not df_all.empty:
                    missing_df = pd.DataFrame(last.get("missing", [])) if last.get("missing") else pd.DataFrame()
                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results     = _r
                    st.session_state.analysis_df = df_all
                    st.rerun()
        else:
            st.info("👈 ارفع الملفات في القسم أدناه ثم اضغط «بدء التحليل»")

    st.markdown("---")
    st.subheader("📂 رفع الملفات وبدء التحليل")

    our_file = st.file_uploader(
        "📦 ملف منتجاتنا (CSV/Excel)",
        type=["csv", "xlsx", "xls"],
        key="dash_our_file",
    )

    # ── جسر الكشط التلقائي (Auto-Scraper Bridge) ─────────────────────────
    import os as _os_dash
    _AUTO_CSV = _os_dash.path.join(
        _os_dash.environ.get("DATA_DIR", "data"), "competitors_latest.csv"
    )
    _auto_available = _os_dash.path.exists(_AUTO_CSV)
    _auto_rows = 0   # ← يُهيَّأ دائماً لمنع NameError إذا تغيّرت حالة الملف بين reruns

    if _auto_available:
        _auto_rows = 0
        try:
            with open(_AUTO_CSV, encoding="utf-8-sig") as _af:
                _auto_rows = sum(1 for _ in _af) - 1
        except Exception:
            pass
        st.markdown(
            f'<div style="background:#0a2a0a;border:1px solid #00C853;border-radius:8px;'
            f'padding:10px 14px;margin:6px 0;font-size:.88rem">'
            f'🤖 <b>بيانات الكشط التلقائي جاهزة</b> — '
            f'{_auto_rows:,} منتج من المنافسين<br>'
            f'<span style="color:#9e9e9e;font-size:.78rem">'
            f'استخدمها مباشرةً بدلاً من رفع ملف يدوي</span></div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="background:#1a1a1a;border:1px dashed #555;border-radius:8px;'
            'padding:8px 14px;margin:6px 0;font-size:.82rem;color:#888">'
            '🤖 البيانات التلقائية غير متوفرة بعد — '
            '<a href="#" style="color:#4fc3f7">اذهب لصفحة الكشط</a> لتشغيل المحرك</div>',
            unsafe_allow_html=True,
        )

    _use_auto = st.checkbox(
        "🤖 استخدام بيانات الكشط التلقائي من المنافسين",
        value=bool(st.session_state.pop("_use_auto_scraper", False)) and _auto_available,
        disabled=not _auto_available,
        key="dash_use_auto_scraper",
        help="يستخدم الملف المُنتج تلقائياً من محرك الكشط بدلاً من رفع ملف يدوياً",
    )

    if not _use_auto:
        comp_files = st.file_uploader(
            "🏪 ملفات المنافسين (متعدد — CSV/Excel)",
            type=["csv", "xlsx", "xls"],
            accept_multiple_files=True,
            key="dash_comp_files",
            help="ملفات CSV بتصدير سلة/كشط (أعمدة مثل text-sm-2 وstyles_productCard__name__…) تُعرَف تلقائياً هنا.",
        )
    else:
        comp_files = None  # غير مستخدم عند التحميل التلقائي
        st.success(
            f"✅ سيُستخدم الملف الآلي: `{_AUTO_CSV}` ({_auto_rows:,} منتج)"
        )

    if our_file is not None:
        try:
            our_file.seek(0)
        except Exception:
            pass
        _odf, _oe = read_file(our_file)
        try:
            our_file.seek(0)
        except Exception:
            pass
        if not _oe and _odf is not None:
            with st.expander("📋 تعرف تلقائي على أعمدة ملف المتجر", expanded=False):
                _render_column_mapping_expander(_odf, "dash_map_our")
    if comp_files:
        for _ci, cf in enumerate(comp_files):
            _salla_err = None
            try:
                cf.seek(0)
            except Exception:
                pass
            _cfn = getattr(cf, "name", "") or ""
            if _cfn.lower().endswith(".csv"):
                _cdf_salla, _salla_err, _enc_used = load_competitor_csv_for_matching(
                    cf, competitor_label=_dashboard_competitor_label(_cfn)
                )
                if _cdf_salla is not None and not _salla_err:
                    st.caption(
                        f"✅ **{_cfn}** — تعريف تلقائي لتصدير سلة ({_enc_used}) · **{len(_cdf_salla):,}** صف"
                    )
                    with st.expander(f"📋 معاينة منظّفة — {_cfn}", expanded=False):
                        st.dataframe(_cdf_salla.head(8), use_container_width=True, height=260)
                    continue
                try:
                    cf.seek(0)
                except Exception:
                    pass
            _cdf, _ce = read_file(cf)
            try:
                cf.seek(0)
            except Exception:
                pass
            if not _ce and _cdf is not None:
                if _cfn.lower().endswith(".csv") and _salla_err:
                    st.caption(
                        f"⚠️ **{_cfn}**: تعيين سلة تلقائي: {_salla_err} — "
                        "يُستخدم التعرف العام؛ اضبط الأعمدة في المُوسّع إن لزم."
                    )
                with st.expander(f"📋 تعرف تلقائي — {cf.name}", expanded=False):
                    _render_column_mapping_expander(_cdf, f"dash_map_comp_{_ci}")

    col_opt1, col_opt2 = st.columns(2)
    with col_opt1:
        bg_mode = st.checkbox(
            "⚡ معالجة خلفية (يمكنك التنقل أثناء التحليل)",
            value=True,
            key="dash_bg_mode",
        )
    with col_opt2:
        max_rows = st.number_input(
            "حد الصفوف للمعالجة (0=كل)", 0, step=500, key="dash_max_rows"
        )

    _copt3a, _copt3b = st.columns(2)
    with _copt3a:
        # FIX: Relaxed Constraints — منع فقدان النتائج السابقة بإجبار الدمج التراكمي دائماً.
        st.caption("📎 الدمج التراكمي للنتائج: **مفعّل دائماً (Zero Data Loss)**")
    with _copt3b:
        st.checkbox(
            "📚 تحديث كتالوج قاعدة البيانات من الملفات المرفوعة",
            value=True,
            key="dash_update_db_catalog",
            help="عطّلها لتشغيل المقارنة فقط دون تعديل جداول كتالوجنا والمنافسين في SQLite.",
        )

    if st.button("🚀 بدء التحليل", type="primary", key="dash_btn_start_analysis"):
        st.session_state["processed_price_skus"].clear()  # FIX: Smart Workflow & AI Tracking
        st.session_state["processed_missing_urls"].clear()  # FIX: Smart Workflow & AI Tracking
        # ── حارس المدخلات (يدعم الوضعين: يدوي وتلقائي) ──────────────────
        _auto_mode = bool(st.session_state.get("dash_use_auto_scraper")) and _auto_available
        if not our_file:
            st.warning("⚠️ ارفع ملف منتجاتنا أولاً")
        elif not _auto_mode and not comp_files:
            st.warning("⚠️ ارفع ملف منافس واحد على الأقل، أو فعّل الكشط التلقائي")
        else:
            _prep_ok = False
            our_df = None
            comp_dfs = {}
            job_id = None
            comp_names = ""
            _dash_upd_db_cat = bool(st.session_state.get("dash_update_db_catalog", True))
            _spin_read = (
                "⏳ جاري قراءة الملفات وتحديث كتالوج قاعدة البيانات..."
                if _dash_upd_db_cat
                else "⏳ جاري قراءة الملفات (بدون تحديث كتالوج قاعدة البيانات)..."
            )
            with st.spinner(_spin_read):
                try:
                    our_file.seek(0)
                except Exception:
                    pass
                our_df, err = read_file(our_file)
                if err:
                    st.error(f"❌ {err}")
                else:
                    our_df = apply_user_column_map(our_df, **_effective_column_map(our_df, "dash_map_our"))
                    if max_rows > 0:
                        our_df = our_df.head(int(max_rows))

                    comp_dfs = {}
                    if _auto_mode:
                        # ── وضع الكشط التلقائي: تحميل CSV من القرص مع فصل كل متجر كمنافس مستقل ────────
                        try:
                            _auto_df = pd.read_csv(_AUTO_CSV, encoding="utf-8-sig")
                            _auto_store_col = next(
                                (
                                    _c for _c in _auto_df.columns
                                    if str(_c).strip().lower() in ("store", "domain", "المتجر", "المنافس")
                                ),
                                None,
                            )

                            if _auto_store_col:
                                _auto_df[_auto_store_col] = _auto_df[_auto_store_col].fillna("").astype(str).str.strip()
                                _grouped_auto = _auto_df[_auto_df[_auto_store_col] != ""].groupby(_auto_store_col, sort=False)
                                for _store_name, _store_df in _grouped_auto:
                                    _store_key = str(_store_name).strip()
                                    _store_key = _store_key.replace("https://", "").replace("http://", "").strip("/")
                                    _store_key = _store_key.split("/")[0] or "competitors_latest.csv"
                                    if _store_key in comp_dfs:
                                        comp_dfs[_store_key] = pd.concat(
                                            [comp_dfs[_store_key], _store_df.copy()],
                                            ignore_index=True,
                                        )
                                    else:
                                        comp_dfs[_store_key] = _store_df.reset_index(drop=True).copy()

                                _unassigned_auto = _auto_df[_auto_df[_auto_store_col] == ""]
                                if not _unassigned_auto.empty:
                                    comp_dfs["competitors_latest.csv"] = _unassigned_auto.reset_index(drop=True).copy()

                                if comp_dfs:
                                    st.caption(
                                        f"✅ تم تحميل البيانات الآلية: {len(_auto_df):,} صف من {len(comp_dfs):,} متجر منافس"
                                    )
                                else:
                                    comp_dfs["competitors_latest.csv"] = _auto_df
                                    st.caption(f"✅ تم تحميل البيانات الآلية: {len(_auto_df):,} منتج")
                            else:
                                comp_dfs["competitors_latest.csv"] = _auto_df
                                st.caption(f"✅ تم تحميل البيانات الآلية: {len(_auto_df):,} منتج")
                        except Exception as _ae:
                            st.error(f"❌ فشل تحميل الملف الآلي: {_ae}")
                    else:
                        # ── وضع الرفع اليدوي (CSV سلة/كشط → تعريف تلقائي ثم read_file احتياطاً) ──
                        for _ci, cf in enumerate(comp_files):
                            try:
                                cf.seek(0)
                            except Exception:
                                pass
                            _fn = getattr(cf, "name", "") or ""
                            if _fn.lower().endswith(".csv"):
                                cdf_norm, salla_err, _enc = load_competitor_csv_for_matching(
                                    cf, competitor_label=_dashboard_competitor_label(_fn)
                                )
                                if cdf_norm is not None and not salla_err:
                                    comp_dfs[_fn] = cdf_norm
                                    continue
                                try:
                                    cf.seek(0)
                                except Exception:
                                    pass
                            cdf, cerr = read_file(cf)
                            if cerr:
                                st.warning(f"⚠️ {_fn or cf.name}: {cerr}")
                            else:
                                cdf = apply_user_column_map(
                                    cdf, **_effective_column_map(cdf, f"dash_map_comp_{_ci}")
                                )
                                comp_dfs[_fn or cf.name] = cdf

                    if not comp_dfs:
                        st.error("❌ لم يُحمّل أي ملف منافس صالح")
                    else:
                        _catc = _resolve_catalog_columns_relaxed(our_df)
                        if _dash_upd_db_cat:
                            r_our = upsert_our_catalog(
                                our_df,
                                name_col=_catc["name"] or "اسم المنتج",
                                id_col=_catc["id"] or "رقم المنتج",
                                price_col=_catc["price"] or "سعر المنتج",
                            )
                            r_comp = upsert_comp_catalog(comp_dfs)

                            # ── تراكم بيانات المنافسين عبر الجلسات ──────
                            try:
                                init_competitor_store()
                                _accum_total_new = 0
                                _accum_total_upd = 0
                                for _cname, _cdf in comp_dfs.items():
                                    if _cdf is None or _cdf.empty:
                                        continue
                                    _nc = next((c for c in ["المنتج","اسم المنتج","منتج_المنافس","name"] if c in _cdf.columns), None)
                                    _pc = next((c for c in ["سعر المنتج","السعر","سعر_المنافس","price"] if c in _cdf.columns), None)
                                    if _nc:
                                        _products_list = _cdf.rename(columns={_nc:"المنتج", _pc:"السعر"} if _pc else {_nc:"المنتج"}).to_dict("records")
                                        _r = upsert_competitor_products(_cname, _products_list)
                                        _accum_total_new += _r["inserted"]
                                        _accum_total_upd += _r["updated"]
                                _store_stats = get_competitor_store_stats()
                                st.caption(
                                    f"✅ كتالوجنا: {r_our['inserted']} جديد / {r_our['updated']} تحديث | "
                                    f"المنافسين: {r_comp['new_products']} جديد / {r_comp.get('updated',0)} تحديث | "
                                    f"🗄️ مخزن التراكم: {_accum_total_new} أُضيف / {_accum_total_upd} حُدِّث "
                                    f"(إجمالي: {_store_stats['total_products']:,} منتج)"
                                )
                            except Exception as _acc_err:
                                st.caption(
                                    f"✅ كتالوجنا: {r_our['inserted']} جديد / {r_our['updated']} تحديث | "
                                    f"المنافسين: {r_comp['new_products']} جديد / {r_comp.get('updated', 0)} تحديث"
                                )
                        else:
                            st.caption(
                                "⏭️ تم تخطي تحديث كتالوج قاعدة البيانات — يُحفظ التحليل في الجلسة فقط."
                            )
                        st.session_state.our_df = our_df
                        st.session_state.comp_dfs = comp_dfs
                        job_id = str(uuid.uuid4())[:8]
                        st.session_state.job_id = job_id
                        comp_names = ",".join(comp_dfs.keys())
                        _prep_ok = True

            if _prep_ok and our_df is not None and comp_dfs:
                _validate_uploaded_catalog(our_df, "ملف منتجاتنا")
                for _cfn, _cdf in comp_dfs.items():
                    _validate_uploaded_catalog(_cdf, f"ملف منافس: {_cfn}")
                if bg_mode:
                    _dash_acc = bool(st.session_state.get("dash_accumulate_results", True))
                    _prev_ar = None
                    _prev_mr = None
                    if _dash_acc:
                        _adf_prev = st.session_state.get("analysis_df")
                        if _adf_prev is not None and not getattr(_adf_prev, "empty", True):
                            _prev_ar = safe_results_for_json(_adf_prev.to_dict("records"))
                        _res_prev = st.session_state.get("results") or {}
                        _miss_prev = _res_prev.get("missing")
                        if isinstance(_miss_prev, pd.DataFrame) and not _miss_prev.empty:
                            _prev_mr = _miss_prev.to_dict("records")
                    _bg_target = partial(
                        _run_analysis_background,
                        job_id,
                        our_df,
                        comp_dfs,
                        our_file.name,
                        comp_names,
                        merge_previous=_dash_acc,
                        prev_analysis_records=_prev_ar,
                        prev_missing_records=_prev_mr,
                    )
                    t = threading.Thread(target=_bg_target, daemon=True)
                    add_script_run_ctx(t)
                    t.start()
                    st.session_state.job_running = True
                    st.success(f"✅ بدأ التحليل في الخلفية (Job: {job_id})")
                    st.rerun()
                else:
                    prog = st.progress(0, "جاري التحليل...")

                    def upd(p, _r=None):
                        prog.progress(min(float(p), 0.99), f"{float(p)*100:.0f}%")

                    df_all, audit_stats = run_full_analysis(our_df, comp_dfs, progress_callback=upd)
                    if st.session_state.get("dash_accumulate_results", True):
                        _prev_adf = st.session_state.get("analysis_df")
                        if _prev_adf is not None and not getattr(_prev_adf, "empty", True):
                            df_all = merge_price_analysis_dataframes(_prev_adf, df_all)
                            st.caption("📎 وُدمت نتائج التحليل مع الجلسة السابقة.")
                    st.session_state.last_audit_stats = audit_stats
                    _render_audit_bar(audit_stats)
                    try:
                        _rec = reconcile_competitor_upload(our_df, comp_dfs)
                        missing_df = smart_missing_barrier(_rec.new_products_df, our_df)
                        _rec.apply_smart_barrier_adjustment(missing_df)
                        audit_stats = merge_reconciliation_into_audit(audit_stats, _rec)
                        st.session_state.last_audit_stats = audit_stats
                        st.session_state.reconciliation_report = _rec.to_dict()
                        st.session_state.reconciliation_failed_csv = (
                            failed_rows_to_xlsx_bytes(_rec.failed_df)
                            if _rec.failed_df is not None and not _rec.failed_df.empty
                            else None
                        )
                        if _rec.failed_df is not None and not _rec.failed_df.empty:
                            import os

                            _dd = os.environ.get("DATA_DIR", "data")
                            os.makedirs(_dd, exist_ok=True)
                            _fj = os.path.join(
                                _dd,
                                f"failed_rows_{st.session_state.get('job_id') or 'local'}.xlsx",
                            )
                            try:
                                _rec.failed_df.to_excel(_fj, index=False, engine="openpyxl")
                                audit_stats["reconciliation_failed_csv_path"] = _fj
                                st.session_state.last_audit_stats = audit_stats
                            except OSError:
                                pass
                    except Exception as _rec_err:
                        st.warning(f"⚠️ محرك المحاسبة: {_rec_err} — يُستخدم مسار المفقودات السابق.")
                        raw_missing_df = find_missing_products(our_df, comp_dfs)
                        missing_df = smart_missing_barrier(raw_missing_df, our_df)
                        st.session_state.reconciliation_report = None
                        st.session_state.reconciliation_failed_csv = None
                    _render_reconciliation_dashboard(st.session_state.get("last_audit_stats") or {})
                    if st.session_state.get("dash_accumulate_results", True):
                        _prev_miss_df = None
                        if st.session_state.get("results") and isinstance(
                            st.session_state["results"], dict
                        ):
                            _prev_miss_df = st.session_state["results"].get("missing")
                        if (
                            isinstance(_prev_miss_df, pd.DataFrame)
                            and not _prev_miss_df.empty
                        ):
                            missing_df = merge_missing_products_dataframes(
                                _prev_miss_df, missing_df
                            )

                    for _, row in df_all.iterrows():
                        if row.get("نسبة_التطابق", 0) > 0:
                            upsert_price_history(
                                str(row.get("المنتج", "")),
                                str(row.get("المنافس", "")),
                                safe_float(row.get("سعر_المنافس", 0)),
                                safe_float(row.get("السعر", 0)),
                                safe_float(row.get("الفرق", 0)),
                                safe_float(row.get("نسبة_التطابق", 0)),
                                str(row.get("القرار", "")),
                            )

                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results = _r
                    st.session_state.analysis_df = df_all
                    log_analysis(
                        our_file.name,
                        comp_names,
                        len(our_df),
                        int((df_all.get("نسبة_التطابق", pd.Series(dtype=float)) > 0).sum()),
                        len(missing_df),
                    )
                    prog.progress(1.0, "✅ اكتمل!")
                    st.balloons()
                    st.rerun()


# ════════════════════════════════════════════════
#  2. سعر أعلى
# ════════════════════════════════════════════════
elif page == "🔴 سعر أعلى":
    st.markdown(
        '<div style="display:flex;flex-wrap:wrap;align-items:center;gap:10px;margin:0 0 4px 0">'
        '<span class="b-high" style="display:inline-block;padding:6px 12px;border-radius:10px;'
        'font-weight:800;font-size:.95rem">🔴 فرصة خفض</span>'
        '<span style="color:#9e9e9e;font-size:.82rem;font-weight:600">مقارنة مع أقل سعر منافس</span>'
        "</div>",
        unsafe_allow_html=True,
    )
    st.header("منتجات سعرنا أعلى")
    db_log("price_raise", "view")
    if st.session_state.results and "price_raise" in st.session_state.results:
        df = st.session_state.results["price_raise"]
        _price_raise_total = len(df) if isinstance(df, pd.DataFrame) else 0  # FIX: Transparency & Reversibility
        if isinstance(df, pd.DataFrame) and not df.empty and "معرف_المنتج" in df.columns:
            _proc_price = {str(x) for x in st.session_state.get("processed_price_skus", set())}
            df = df[~df["معرف_المنتج"].astype(str).isin(_proc_price)]  # FIX: Smart Workflow & AI Tracking
        _show_transparency_counter(_price_raise_total, len(df) if isinstance(df, pd.DataFrame) else 0)  # FIX: Transparency & Reversibility
        if not df.empty:
            st.markdown(
                f'<p style="margin:4px 0 8px;font-size:1.05rem;font-weight:700;color:#FF5252">'
                f"{len(df)} منتج — سعرنا أعلى من المنافس (بيانات التحليل الحالي)</p>",
                unsafe_allow_html=True,
            )
            # AI تدريب لهذا القسم
            with st.expander("🤖 نصيحة AI لهذا القسم", expanded=False):
                if st.button("📡 احصل على تحليل شامل للقسم", key="ai_section_raise"):
                    with st.spinner("🤖 AI يحلل البيانات الفعلية..."):
                        _top = df.nlargest(min(15, len(df)), "الفرق") if "الفرق" in df.columns else df.head(15)
                        _lines = "\n".join(
                            f"- {r.get('المنتج','')}: سعرنا {safe_float(r.get('السعر',0)):.0f} | المنافس ({r.get('المنافس','')}) {safe_float(r.get('سعر_المنافس',0)):.0f} | فرق +{safe_float(r.get('الفرق',0)):.0f}"
                            for _, r in _top.iterrows())
                        _avg_diff = safe_float(df["الفرق"].mean()) if "الفرق" in df.columns else 0
                        _prompt = (f"عندي {len(df)} منتج سعرنا أعلى من المنافسين.\n"
                                   f"متوسط الفرق: {_avg_diff:.0f} ر.س\n"
                                   f"أعلى 15 فرق:\n{_lines}\n\n"
                                   f"أعطني:\n1. أي المنتجات يجب خفض سعرها فوراً (فرق>30)؟\n"
                                   f"2. أي المنتجات يمكن إبقاؤها (فرق<10)؟\n"
                                   f"3. استراتيجية تسعير مخصصة لكل ماركة")
                        r = call_ai(_prompt, "price_raise")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
            render_pro_table(df, "raise", "raise", compact_cards=True)
        else:
            st.success("✅ ممتاز! لا توجد منتجات بسعر أعلى")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  4. سعر أقل
# ════════════════════════════════════════════════
elif page == "🟢 سعر أقل":
    st.header("🟢 منتجات سعرنا أقل — فرصة رفع")
    db_log("price_lower", "view")
    if st.session_state.results and "price_lower" in st.session_state.results:
        df = st.session_state.results["price_lower"]
        _price_lower_total = len(df) if isinstance(df, pd.DataFrame) else 0  # FIX: Transparency & Reversibility
        if isinstance(df, pd.DataFrame) and not df.empty and "معرف_المنتج" in df.columns:
            _proc_price = {str(x) for x in st.session_state.get("processed_price_skus", set())}
            df = df[~df["معرف_المنتج"].astype(str).isin(_proc_price)]  # FIX: Smart Workflow & AI Tracking
        _show_transparency_counter(_price_lower_total, len(df) if isinstance(df, pd.DataFrame) else 0)  # FIX: Transparency & Reversibility
        if not df.empty:
            st.info(f"💰 {len(df)} منتج يمكن رفع سعره لزيادة الهامش")
            with st.expander("🤖 نصيحة AI لهذا القسم", expanded=False):
                if st.button("📡 استراتيجية رفع الأسعار", key="ai_section_lower"):
                    with st.spinner("🤖 AI يحلل فرص الربح..."):
                        _top = df.nsmallest(min(15, len(df)), "الفرق") if "الفرق" in df.columns else df.head(15)
                        _lines = "\n".join(
                            f"- {r.get('المنتج','')}: سعرنا {safe_float(r.get('السعر',0)):.0f} | المنافس ({r.get('المنافس','')}) {safe_float(r.get('سعر_المنافس',0)):.0f} | فرق {safe_float(r.get('الفرق',0)):.0f}"
                            for _, r in _top.iterrows())
                        _total_lost = safe_float(df["الفرق"].sum()) if "الفرق" in df.columns else 0
                        _prompt = (f"عندي {len(df)} منتج سعرنا أقل من المنافسين.\n"
                                   f"إجمالي الأرباح الضائعة: {abs(_total_lost):.0f} ر.س\n"
                                   f"أكبر 15 فرصة ربح:\n{_lines}\n\n"
                                   f"أعطني:\n1. أي المنتجات يمكن رفع سعرها فوراً (فرق>50)؟\n"
                                   f"2. أي المنتجات نرفعها تدريجياً (فرق 10-50)؟\n"
                                   f"3. كم الربح المتوقع إذا رفعنا الأسعار؟")
                        r = call_ai(_prompt, "price_lower")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
            render_pro_table(df, "lower", "lower")
        else:
            st.info("لا توجد منتجات")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  5. موافق عليها
# ════════════════════════════════════════════════
elif page == "✅ موافق عليها":
    st.header("✅ منتجات موافق عليها")
    db_log("approved", "view")
    if st.session_state.results and "approved" in st.session_state.results:
        df = st.session_state.results["approved"]
        if not df.empty:
            st.success(f"✅ {len(df)} منتج بأسعار تنافسية مناسبة")
            render_pro_table(df, "approved", "approved")
        else:
            st.info("لا توجد منتجات موافق عليها")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  6. منتجات مفقودة — v26 مع كشف التستر/الأساسي
# ════════════════════════════════════════════════
elif page == "🔍 منتجات مفقودة":
    st.header("🔍 منتجات المنافسين غير الموجودة عندنا")
    _debug_log("H2", "app.py:missing_page_entry", "Entered missing page", {
        "has_results": bool(st.session_state.results),
        "has_missing_key": bool(st.session_state.results and "missing" in st.session_state.results),
    })
    # ── المستشار الذكي للمفقودات ─────────────────────────────────────────
    with st.expander("🧠 المستشار الذكي للمفقودات (AI Expert)", expanded=False):
        st.markdown("اسأل المستشار عن استراتيجية إضافة هذه المنتجات أو تحليل السوق لها:")
        miss_query = st.text_input(
            "سؤالك للمستشار (مثال: ما هي أكثر ماركة مطلوبة من هذه القائمة؟)",
            key="miss_expert_q",
        )
        if st.button("💬 اسأل المستشار", key="ask_miss_expert"):
            if not miss_query.strip():
                st.warning("اكتب سؤالاً أولاً.")
            else:
                with st.spinner("المستشار يحلل القائمة..."):
                    _sample_data = []
                    if st.session_state.results and "missing" in st.session_state.results:
                        _src_df = st.session_state.results["missing"]
                        if _src_df is not None and not _src_df.empty:
                            _sample_data = _src_df.head(50).to_dict("records")
                    _prompt = (
                        f"بناء على هذه المنتجات المفقودة: {str(_sample_data)[:3000]}\n"
                        f"أجب على: {miss_query}"
                    )
                    _response = call_ai(_prompt, "missing")
                    st.markdown(f'<div class="ai-box">{_response["response"]}</div>', unsafe_allow_html=True)

    st.caption(
        "العدد هنا = **عناوين فريدة** بعد إزالة التكرار والمطابقة مع كتالوجنا — وليس بالضرورة كل صفوف ملف المنافس."
    )
    db_log("missing", "view")

    if st.session_state.results and "missing" in st.session_state.results:
        df_missing = st.session_state.results["missing"]
        df_missing_to_show = df_missing.copy() if isinstance(df_missing, pd.DataFrame) else pd.DataFrame()
        _missing_total = len(df_missing) if isinstance(df_missing, pd.DataFrame) else 0  # FIX: Missing Products Display Recovery
        # FIX: Safe Filtering for Missing Products to prevent KeyError
        if isinstance(df_missing, pd.DataFrame):
            if not df_missing.empty:
                link_col_actual = None
                possible_link_cols = ["رابط_المنافس", "الرابط", "رابط المنتج", "url", "رابط", "Link"]
                for col in possible_link_cols:
                    if col in df_missing.columns:
                        link_col_actual = col
                        break
                if link_col_actual:
                    df_missing_to_show = df_missing[
                        ~df_missing[link_col_actual].astype(str).isin(
                            {str(x) for x in st.session_state.get("processed_missing_urls", set())}
                        )
                    ]
                else:
                    name_col_actual = "المنتج"
                    for ncol in ["المنتج", "اسم المنتج", "منتج_المنافس", "Name"]:
                        if ncol in df_missing.columns:
                            name_col_actual = ncol
                            break
                    if name_col_actual in df_missing.columns:
                        df_missing_to_show = df_missing[
                            ~df_missing[name_col_actual].astype(str).isin(
                                {str(x) for x in st.session_state.get("processed_missing_urls", set())}
                            )
                        ]
                    else:
                        df_missing_to_show = df_missing.copy()
            else:
                df_missing_to_show = df_missing.copy()
        df = df_missing_to_show
        _show_transparency_counter(_missing_total, len(df_missing_to_show) if isinstance(df_missing_to_show, pd.DataFrame) else 0)  # FIX: Missing Products Display Recovery
        if df is not None and not df.empty:
            # ── إحصاءات سريعة ──────────────────────────────────────────────
            total_miss   = len(df)
            has_tester   = df["نوع_متاح"].str.contains("تستر", na=False).sum()    if "نوع_متاح" in df.columns else 0
            has_base     = df["نوع_متاح"].str.contains("العطر الأساسي", na=False).sum() if "نوع_متاح" in df.columns else 0
            pure_missing = total_miss - has_tester - has_base

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("🔍 مفقود فعلاً",    pure_missing)
            c2.metric("🏷️ يوجد تستر",      has_tester)
            c3.metric("✅ يوجد الأساسي",   has_base)
            c4.metric("📦 إجمالي المنافسين", total_miss)

            # ── تحليل AI الأولويات ────────────────────────────────────────
            with st.expander("🤖 تحليل AI — أولويات الإضافة", expanded=False):
                if st.button("📡 تحليل الأولويات", key="ai_missing_section"):
                    with st.spinner("🤖 AI يحلل أولويات الإضافة..."):
                        _pure = df[df["نوع_متاح"].str.strip() == ""] if "نوع_متاح" in df.columns else df
                        _brands = _pure["الماركة"].value_counts().head(10).to_dict() if "الماركة" in _pure.columns else {}
                        _summary = " | ".join(f"{b}:{c}" for b,c in _brands.items()) if _brands else "غير محدد"
                        _lines   = "\n".join(
                            f"- {r.get('منتج_المنافس','')}: {safe_float(r.get('سعر_المنافس',0)):.0f}ر.س ({r.get('الماركة','')}) — {r.get('المنافس','')}"
                            for _, r in _pure.head(20).iterrows())
                        _prompt = (
                            f"لديّ {len(_pure)} منتج مفقود فعلاً (بدون التستر/الأساسي المتاح).\n"
                            f"توزيع الماركات: {_summary}\nعينة:\n{_lines}\n\n"
                            "أعطني:\n1. ترتيب أولويات الإضافة (عالية/متوسطة/منخفضة) مع السبب\n"
                            "2. أي الماركات الأكثر ربحية؟\n"
                            "3. سعر مقترح (أقل من المنافس بـ5-10 ر.س)\n"
                            "4. منتجات لا تستحق الإضافة — ولماذا؟"
                        )
                        r_ai = call_ai(_prompt, "missing")
                        resp = r_ai["response"] if r_ai["success"] else "❌ فشل AI"
                        # تنظيف JSON من المخرجات
                        import re as _re
                        resp = _re.sub(r'```json.*?```', '', resp, flags=_re.DOTALL)
                        resp = _re.sub(r'```.*?```', '', resp, flags=_re.DOTALL)
                        st.markdown(f'<div class="ai-box">{resp}</div>', unsafe_allow_html=True)

            # ── فلاتر ─────────────────────────────────────────────────────
            opts = get_filter_options(df)
            with st.expander("🔍 فلاتر", expanded=False):
                c1,c2,c3,c4,c5 = st.columns(5)
                search   = c1.text_input("🔎 بحث", key="miss_s")
                brand_f  = c2.selectbox("الماركة", opts["brands"], key="miss_b")
                comp_f   = c3.selectbox("المنافس", opts["competitors"], key="miss_c")
                variant_f= c4.selectbox("النوع",
                    ["الكل","مفقود فعلاً","يوجد تستر","يوجد الأساسي"], key="miss_v")
                conf_f   = c5.selectbox("الثقة",
                    ["الكل","🟢 مؤكد","🟡 محتمل","🔴 مشكوك"], key="miss_conf_f")

            filtered = df.copy()
            if search:
                filtered = filtered[filtered.apply(lambda r: search.lower() in str(r.values).lower(), axis=1)]
            if brand_f != "الكل" and "الماركة" in filtered.columns:
                filtered = filtered[filtered["الماركة"].str.contains(brand_f, case=False, na=False, regex=False)]
            if comp_f != "الكل" and "المنافس" in filtered.columns:
                filtered = filtered[filtered["المنافس"].str.contains(comp_f, case=False, na=False, regex=False)]
            if variant_f == "مفقود فعلاً" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.strip() == ""]
            elif variant_f == "يوجد تستر" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("تستر", na=False)]
            elif variant_f == "يوجد الأساسي" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("الأساسي", na=False)]
            # فلتر الثقة
            if conf_f != "الكل" and "مستوى_الثقة" in filtered.columns:
                _conf_map = {"🟢 مؤكد": "green", "🟡 محتمل": "yellow", "🔴 مشكوك": "red"}
                _cv = _conf_map.get(conf_f, "")
                if _cv:
                    filtered = filtered[filtered["مستوى_الثقة"] == _cv]

            # ── ترتيب حسب الثقة (الأكثر ثقة أولاً) ─────────────────────
            if "مستوى_الثقة" in filtered.columns:
                _conf_order = {"green": 0, "yellow": 1, "red": 2}
                filtered = filtered.assign(
                    _conf_sort=filtered["مستوى_الثقة"].map(_conf_order).fillna(3)
                ).sort_values("_conf_sort").drop(columns=["_conf_sort"])

            _export_ok, _export_issues = validate_export_product_dataframe(filtered)
            if not _export_ok:
                with st.expander(
                    "⚠️ تنبيه جودة التصدير: صفوف لا تطابق معايير سلة (اسم/سعر) — راجع قبل الاستيراد",
                    expanded=False,
                ):
                    for _ei in _export_issues[:40]:
                        st.caption(_ei)

            # ── خط الإنتاج الذكي (المعالجة والتحقق الإلزامي) ────────────────
            st.markdown("### ⚙️ تجهيز وتصدير المنتجات المحددة")

            if "selected_missing_indices" not in st.session_state:
                st.session_state.selected_missing_indices = []
            if "ready_missing_df" not in st.session_state:
                st.session_state.ready_missing_df = None
            if "missing_dup_uncertain" not in st.session_state:
                st.session_state.missing_dup_uncertain = []

            c1, c2 = st.columns([2, 1])
            with c1:
                st.info(f"المنتجات المحددة للمعالجة: {len(st.session_state.selected_missing_indices)}")
                _debug_log("H4", "app.py:missing_pipeline_ui", "Missing pipeline rendered", {
                    "selected_count": len(st.session_state.selected_missing_indices),
                    "policy": st.session_state.get("miss_dup_policy", ""),
                })
                with st.expander("🛡️ سياسة منع التكرار قبل البدء", expanded=False):
                    uncertain_policy = st.radio(
                        "عند وجود حالة مشكوك فيها:",
                        ["❌ استبعاد تلقائي", "⏸️ إيقاف وطلب قرار", "▶️ متابعة مع التحذير"],
                        index=1,
                        key="miss_dup_policy",
                        horizontal=True,
                    )
                    st.checkbox(
                        "استخدم تحقق AI للحالات المشكوك فيها فقط (أدق لكنه أبطأ)",
                        value=True,
                        key="miss_dup_ai_verify",
                    )
                if st.button("🤖 1. بدء الفحص والمعالجة الذكية (إلزامي)", type="primary", use_container_width=True):
                    if not st.session_state.selected_missing_indices:
                        st.warning("الرجاء تحديد منتج واحد على الأقل من القائمة بالأسفل.")
                    else:
                        st.session_state.ready_missing_df = None
                        st.session_state.missing_dup_uncertain = []
                        with st.status("جاري المعالجة الذكية...", expanded=True) as status:
                            processed_rows = []
                            selected_df = filtered.loc[
                                filtered.index.isin(st.session_state.selected_missing_indices)
                            ]

                            st.write("🔍 جاري التحقق من التكرار...")
                            our_prods = (
                                st.session_state.analysis_df["المنتج"].tolist()
                                if st.session_state.analysis_df is not None
                                else []
                            )
                            our_rows = []
                            if st.session_state.analysis_df is not None and not st.session_state.analysis_df.empty:
                                for _, _r in st.session_state.analysis_df.iterrows():
                                    _our_name = str(_r.get("المنتج", "")).strip()
                                    if not _our_name:
                                        continue
                                    our_rows.append({
                                        "name": _our_name,
                                        "price": safe_float(_r.get("السعر", 0)),
                                    })

                            confirmed_skipped = 0
                            uncertain_skipped = 0
                            uncertain_pending = []

                            for idx, row in selected_df.iterrows():
                                p_name = str(row.get("منتج_المنافس", "")).strip()
                                p_price = safe_float(row.get("سعر_المنافس", 0))
                                if not p_name:
                                    continue

                                st.write(f"🔄 معالجة: {p_name[:30]}...")

                                dup_check = check_duplicate(p_name, our_prods)
                                if (
                                    dup_check.get("success")
                                    and "نعم" in str(dup_check.get("response", "")).lower()
                                ):
                                    st.write(f"⚠️ تم تخطي {p_name[:20]} (محتمل التكرار)")
                                    confirmed_skipped += 1
                                    continue

                                # فحص كتالوج محلي صارم: الاسم + الحجم + التشابه
                                local_best = None
                                p_size = extract_size(p_name)
                                p_norm = _norm_dup_text(p_name)
                                for _orow in our_rows:
                                    _name = _orow["name"]
                                    _sim = _dup_similarity(p_norm, _name)
                                    _o_size = extract_size(_name)
                                    _size_ok = True
                                    if p_size and _o_size:
                                        _size_ok = abs(float(p_size) - float(_o_size)) <= 1.0
                                    if (local_best is None) or (_sim > local_best["sim"]):
                                        local_best = {
                                            "name": _name,
                                            "sim": _sim,
                                            "size_ok": _size_ok,
                                            "price": _orow.get("price", 0),
                                        }

                                if local_best:
                                    # مؤكد: تشابه عالي + حجم متوافق (أو غير معروف)
                                    if local_best["sim"] >= 0.88 and local_best["size_ok"]:
                                        st.write(f"⛔ مستبعد (موجود لدينا): {p_name[:30]} ≈ {local_best['name'][:30]}")
                                        confirmed_skipped += 1
                                        continue
                                    # مشكوك: تشابه متوسط/مرتفع
                                    if local_best["sim"] >= 0.68:
                                        _is_still_uncertain = True
                                        _reason = f"تشابه {local_best['sim']:.0%}"
                                        if not local_best["size_ok"]:
                                            _reason += " مع اختلاف حجم"

                                        # AI fallback فقط للمشكوك
                                        if st.session_state.get("miss_dup_ai_verify", True):
                                            v = verify_match(
                                                p_name,
                                                local_best["name"],
                                                p_price,
                                                safe_float(local_best.get("price", 0)),
                                            )
                                            if v.get("success"):
                                                _m = bool(v.get("match", False))
                                                _c = safe_float(v.get("confidence", 0))
                                                if _m and _c >= 80:
                                                    st.write(
                                                        f"⛔ AI أكد التكرار: {p_name[:28]} ≈ {local_best['name'][:28]} ({_c:.0f}%)"
                                                    )
                                                    confirmed_skipped += 1
                                                    continue
                                                if (not _m) and _c >= 70:
                                                    _is_still_uncertain = False

                                        if _is_still_uncertain:
                                            _item = {
                                                "المنتج_المنافس": p_name,
                                                "مرشح_لدينا": local_best["name"],
                                                "سبب": _reason,
                                            }
                                            uncertain_pending.append(_item)
                                            if uncertain_policy == "❌ استبعاد تلقائي":
                                                st.write(f"⚠️ استبعاد مشكوك: {p_name[:30]} ({_reason})")
                                                uncertain_skipped += 1
                                                continue
                                            if uncertain_policy == "⏸️ إيقاف وطلب قرار":
                                                continue
                                            st.write(f"⚠️ متابعة رغم الشك: {p_name[:30]} ({_reason})")

                                frag_info = fetch_fragrantica_info(p_name)
                                raw_data = f"الاسم: {p_name}, السعر: {p_price}"
                                if frag_info.get("success"):
                                    raw_data += f", المكونات: {', '.join(frag_info.get('top_notes', []))}"

                                # ── وصف تسويقي طويل (Markdown) لـ وصف_AI فقط — ليس عمود «الوصف» في CSV سلة ──
                                # ملف سلة الجاهز يبني عمود «الوصف» من القالب الثابت داخل export_to_salla_shamel.
                                html_body = generate_mahwous_description(
                                    product_name=p_name,
                                    price=p_price,
                                    fragrantica_data=frag_info if frag_info.get("success") else None,
                                )
                                # الماركة والتصنيف نستخرجهما من generate_seo_description (JSON structured)
                                seo_data = generate_seo_description(raw_data)

                                new_row = row.copy()
                                new_row["وصف_AI"] = html_body or seo_data.get("markdown_desc", "")
                                new_row["الماركة_الرسمية"] = seo_data.get(
                                    "exact_brand",
                                    str(row.get("الماركة", "")),
                                )
                                new_row["التصنيف_الرسمي"] = seo_data.get(
                                    "exact_category",
                                    "العطور",
                                )
                                # حفظ المكونات منفصلة لتحسين الوصف الاحتياطي
                                if frag_info.get("success"):
                                    new_row["top_notes"]   = ", ".join(frag_info.get("top_notes", []))
                                    new_row["heart_notes"] = ", ".join(frag_info.get("middle_notes", []))
                                    new_row["base_notes"]  = ", ".join(frag_info.get("base_notes", []))
                                processed_rows.append(new_row)

                            if uncertain_pending and uncertain_policy == "⏸️ إيقاف وطلب قرار":
                                st.session_state.missing_dup_uncertain = uncertain_pending
                                status.update(label="⏸️ تم إيقاف المعالجة لوجود حالات مشكوك فيها", state="error", expanded=True)
                                st.warning("تم الإيقاف: راجع جدول الحالات المشكوك فيها بالأسفل ثم غيّر السياسة أو عدّل الاختيار.")
                            else:
                                status.update(label="✅ اكتملت المعالجة!", state="complete", expanded=False)

                            if confirmed_skipped or uncertain_skipped:
                                st.caption(
                                    f"منع التكرار: مؤكد {confirmed_skipped} | مشكوك مستبعد {uncertain_skipped}"
                                )
                            if processed_rows:
                                st.session_state.ready_missing_df = pd.DataFrame(processed_rows)
                                st.success(
                                    f"تمت معالجة {len(processed_rows)} منتج بنجاح، "
                                    "ومطابقة الماركات وتوليد الأوصاف."
                                )
                            else:
                                st.error("لم يتم معالجة أي منتج (قد تكون جميعها مكررة).")

            with c2:
                if st.session_state.get("ready_missing_df") is not None and not st.session_state.ready_missing_df.empty:
                    _ready_df = st.session_state.ready_missing_df
                    _our_df_ref = st.session_state.get("our_df")
                    salla_export_mode = st.radio(
                        "⚙️ وضع تصدير ملف سلة (للمفقودات):",  # FIX: Salla Export Mode Toggle
                        options=[
                            "Strict Safe Mode (ينصح به لتجنب الأخطاء)",
                            "Category Path Mode (استخدام المسار الكامل للتصنيف)",
                        ],
                        index=0 if st.session_state.get("salla_export_mode", "safe") == "safe" else 1,
                        help="الوضع الآمن يرسل اسم التصنيف النهائي فقط. وضع المسار يرسل المسار الكامل (مثل: العطور > عطور رجالية) ويجب أن يكون متطابقاً 100% في متجرك.",
                        key="missing_salla_export_mode_ui",
                    )
                    st.session_state["salla_export_mode"] = (  # FIX: Salla Export Mode Toggle
                        "safe" if "Strict" in salla_export_mode else "path"
                    )
                    _export_mode = st.session_state.get("salla_export_mode", "safe")

                    # ── التحقق من المنتجات المفقودة فعلاً ─────────────────
                    try:
                        _truly_missing, _found_in_cat = verify_truly_missing(
                            _ready_df, _our_df_ref, fuzzy_threshold=82.0
                        )
                        if not _found_in_cat.empty:
                            st.warning(
                                f"⚠️ **{len(_found_in_cat)}** منتج وُجد في الكتالوج بأسماء مختلفة "
                                f"وسيُستبعد من التصدير لتفادي التكرار.",
                                icon="🔍"
                            )
                            with st.expander(f"👁️ منتجات موجودة في الكتالوج ({len(_found_in_cat)})", expanded=False):
                                st.dataframe(_found_in_cat[["منتج_المنافس","المنافس","سعر_المنافس"]].head(20)
                                             if "منتج_المنافس" in _found_in_cat.columns
                                             else _found_in_cat.head(20),
                                             use_container_width=True)
                        export_df = _truly_missing if not _truly_missing.empty else _ready_df
                    except Exception:
                        export_df = _ready_df

                    st.caption(f"📦 **{len(export_df)}** منتج جاهز للتصدير لسلة")

                    # ── زر CSV (مطابق لقالب سلة الرسمي) ─────────────────
                    try:
                        _csv_bytes, _csv_count, _ = export_to_salla_shamel_csv(
                            export_df, _our_df_ref, verify_missing=False, export_mode=_export_mode  # FIX: Salla Export Mode Toggle
                        )
                        st.download_button(
                            "📥 2. تحميل ملف سلة CSV (مطابق للقالب الرسمي)",
                            data=_csv_bytes,
                            file_name="mahwous_missing_ready.csv",
                            mime="text/csv; charset=utf-8",
                            type="primary",
                            use_container_width=True,
                            help=f"يحتوي على {_csv_count} منتج — قالب سلة الرسمي مع صف بيانات المنتج",
                        )
                    except Exception as _csv_exp:
                        st.error(f"❌ فشل توليد CSV سلة: {_csv_exp}")

                    # ── زر XLSX (احتياطي) ────────────────────────────────
                    try:
                        _xlsx_bytes = export_to_salla_shamel(
                            export_df, _our_df_ref, verify_missing=False, export_mode=_export_mode  # FIX: Salla Export Mode Toggle
                        )
                        st.download_button(
                            "📥 تحميل ملف سلة XLSX (Excel)",
                            data=_xlsx_bytes,
                            file_name="mahwous_missing_ready.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                        )
                    except Exception as _xlsx_exp:
                        st.error(f"❌ فشل توليد XLSX: {_xlsx_exp}")
                else:
                    st.button(
                        "📥 2. تحميل ملف سلة (جاهز للاستيراد)",
                        disabled=True,
                        use_container_width=True,
                        help="قم بالمعالجة أولاً",
                    )

            if st.session_state.get("missing_dup_uncertain"):
                with st.expander("⚠️ حالات مشكوك فيها (تحتاج قرار)", expanded=True):
                    st.dataframe(pd.DataFrame(st.session_state.missing_dup_uncertain), use_container_width=True)
                    st.caption("اختر سياسة مختلفة من «سياسة منع التكرار قبل البدء» ثم أعد تنفيذ المعالجة.")

            # ── خيارات الإرسال الذكي ─────────────────────────────
            _conf_opts = {"🟢 مؤكدة فقط": "green", "🟡 محتملة": "yellow", "🔵 الكل": ""}
            _conf_sel = st.selectbox("مستوى الثقة", list(_conf_opts.keys()), key="miss_conf_sel")
            _conf_val = _conf_opts[_conf_sel]
            if st.button("📤 إرسال بدفعات ذكية لـ Make", key="miss_make_all"):
                # فلتر المفقودة الفعلية فقط (بدون التستر/الأساسي المتاح)
                _to_send = filtered[filtered["نوع_متاح"].str.strip() == ""] if "نوع_متاح" in filtered.columns else filtered

                is_valid, issues = validate_export_product_dataframe(_to_send)
                if not is_valid:
                    st.error("❌ تم إيقاف الإرسال! البيانات لا تطابق معايير سلة الصارمة:")
                    for issue in issues:
                        st.warning(issue)
                else:
                    products = export_to_make_format(_to_send, "missing")
                    # إضافة مستوى الثقة لكل منتج
                    for _ip, _pr_row in enumerate(products):
                        if _ip < len(_to_send):
                            _pr_row["مستوى_الثقة"] = str(_to_send.iloc[_ip].get("مستوى_الثقة", "green"))
                    _prog_bar = st.progress(0, text="جاري الإرسال...")
                    _status_txt = st.empty()

                    def _miss_progress(sent, failed, total, cur_name):
                        pct = (sent + failed) / max(total, 1)
                        _prog_bar.progress(min(pct, 1.0), text=f"إرسال: {sent}/{total} | {cur_name}")
                        _status_txt.caption(f"✅ {sent} | ❌ {failed} | الإجمالي {total}")

                    res = send_batch_smart(
                        products,
                        batch_type="new",
                        batch_size=20,
                        max_retries=3,
                        progress_cb=_miss_progress,
                        confidence_filter=_conf_val,
                    )
                    _prog_bar.progress(1.0, text="اكتمل")
                    _full_success = (res.get("sent", 0) == len(products)) and (res.get("failed", 0) == 0)  # FIX: Transparency & Reversibility
                    if _full_success:
                        st.success(res["message"])
                        # FIX: Missing Products Display Recovery
                        _miss_link_col = None
                        for _c in ["رابط_المنافس", "الرابط", "رابط المنتج", "url", "رابط", "Link"]:
                            if _c in _to_send.columns:
                                _miss_link_col = _c
                                break
                        if _miss_link_col:
                            for _u in _to_send[_miss_link_col].dropna().astype(str):
                                _track_processed_missing_url(_u)
                        # v26: احفظ في قائمة المعالجة
                        for _, _pr in _to_send.iterrows():
                            _pk = f"miss_{str(_pr.get('منتج_المنافس',''))[:30]}_{str(_pr.get('المنافس',''))}"
                            save_processed(
                                _pk,
                                str(_pr.get('منتج_المنافس','')),
                                str(_pr.get('المنافس','')),
                                "send_missing",
                                new_price=safe_float(_pr.get('سعر_المنافس',0)),
                            )
                        st.rerun()  # FIX: Smart Workflow & AI Tracking
                    else:
                        st.error(f"❌ فشل الإرسال الكامل إلى Make: {res.get('message', 'خطأ غير معروف')}")  # FIX: Transparency & Reversibility
                        st.error("لم يتم تعليم أي منتج كمُعالج لأن الإرسال لم ينجح بالكامل.")  # FIX: Transparency & Reversibility
                    if res.get("errors"):
                        with st.expander(f"❌ منتجات فشلت ({len(res['errors'])})"):
                            for _en in res["errors"]:
                                st.caption(f"• {_en}")

            st.caption(f"{len(filtered)} منتج — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

            # ── عرض المنتجات ──────────────────────────────────────────────
            PAGE_SIZE = 20
            total_p = len(filtered)
            tp = max(1, (total_p + PAGE_SIZE - 1) // PAGE_SIZE)
            pn = st.number_input("الصفحة", 1, tp, 1, key="miss_pg") if tp > 1 else 1
            page_df = filtered.iloc[(pn-1)*PAGE_SIZE : pn*PAGE_SIZE]

            for idx, row in page_df.iterrows():
                name  = str(row.get("منتج_المنافس", ""))
                _miss_key = f"missing_{name}_{idx}"
                if _miss_key in st.session_state.hidden_products:
                    continue

                select_col, card_col = st.columns([0.5, 9.5])
                with select_col:
                    _selected_ids = st.session_state.get("selected_missing_indices", [])
                    is_selected = st.checkbox(
                        "",
                        key=f"sel_{idx}",
                        value=idx in _selected_ids,
                    )
                    if is_selected and idx not in st.session_state.selected_missing_indices:
                        st.session_state.selected_missing_indices.append(idx)
                    elif not is_selected and idx in st.session_state.selected_missing_indices:
                        st.session_state.selected_missing_indices.remove(idx)

                price           = safe_float(row.get("سعر_المنافس", 0))
                brand           = str(row.get("الماركة", ""))
                comp            = str(row.get("المنافس", ""))
                size            = str(row.get("الحجم", ""))
                ptype           = str(row.get("النوع", ""))
                _comp_show = _humanize_competitor_upload(comp)
                _title_display = _display_name_for_missing_row(row)
                if not _title_display:
                    _u_title = competitor_product_url_from_row(row)
                    if not str(_u_title or "").strip().lower().startswith("http") and _is_http_url_text(name):
                        _u_title = name.strip()
                    if str(_u_title or "").strip().lower().startswith("http"):
                        _ft = _cached_title_from_product_url(str(_u_title).strip())
                        if _ft:
                            _title_display = _ft
                if _title_display:
                    nm_ai = _title_display
                elif not _is_http_url_text(name):
                    nm_ai = name
                else:
                    _fb = f"{brand} {size} {ptype}".strip()
                    if not _fb:
                        _fb = _comp_show if _comp_show != "—" else "منتج"
                    nm_ai = _fb
                note            = str(row.get("ملاحظة", ""))
                # استخراج معرف المنتج (SKU/الكود)
                _miss_pid_raw = (
                    row.get("معرف_المنافس", "") or
                    row.get("product_id", "") or
                    row.get("رقم المنتج", "") or
                    row.get("رقم_المنتج", "") or
                    row.get("SKU", "") or
                    row.get("sku", "") or
                    row.get("الكود", "") or
                    row.get("كود", "") or
                    row.get("الباركود", "") or ""
                )
                _miss_pid = ""
                if _miss_pid_raw and str(_miss_pid_raw) not in ("", "nan", "None", "0", "NaN"):
                    try: _miss_pid = str(int(float(str(_miss_pid_raw))))
                    except: _miss_pid = str(_miss_pid_raw).strip()
                variant_label   = str(row.get("نوع_متاح", ""))
                variant_product = str(row.get("منتج_متاح", ""))
                variant_score   = safe_float(row.get("نسبة_التشابه", 0))
                is_tester_flag  = bool(row.get("هو_تستر", False))
                conf_level      = str(row.get("مستوى_الثقة", "green"))
                conf_score      = safe_float(row.get("درجة_التشابه", 0))
                suggested_price = round(price - 1, 2) if price > 0 else 0

                _is_similar = "⚠️" in note
                _has_variant= bool(variant_label and variant_label.strip())
                _is_tester_type = "تستر" in variant_label if _has_variant else False
                if idx == page_df.index[0]:
                    _debug_log("H3", "app.py:missing_cards_loop", "Rendering first missing card", {
                        "idx": str(idx),
                        "name": name[:80],
                        "has_variant": _has_variant,
                        "variant_product": variant_product[:80],
                    })

                # ── لون البطاقة حسب الحالة ────────────────────────────
                if _has_variant and _is_tester_type:
                    _border = "#ff980055"; _badge_bg = "#ff9800"
                elif _has_variant:
                    _border = "#4caf5055"; _badge_bg = "#4caf50"
                elif _is_similar:
                    _border = "#ff572255"; _badge_bg = "#ff5722"
                else:
                    _border = "#007bff44"; _badge_bg = "#007bff"

                # ── بادج النوع المتاح ──────────────────────────────────
                _variant_html = ""
                if _has_variant:
                    _variant_html = f"""
                    <div style="margin-top:6px;padding:5px 10px;border-radius:6px;
                                background:{_badge_bg}22;border:1px solid {_badge_bg}88;
                                font-size:.78rem;color:{_badge_bg};font-weight:700">
                        {variant_label}
                        <span style="font-weight:400;color:#aaa;margin-right:6px">
                            ({variant_score:.0f}%) → {variant_product[:50]}
                        </span>
                    </div>"""

                # ── بادج تستر ─────────────────────────────────────────
                _tester_badge = ""
                if is_tester_flag:
                    _tester_badge = '<span style="font-size:.68rem;padding:2px 7px;border-radius:10px;background:#9c27b022;color:#ce93d8;margin-right:6px">🏷️ تستر</span>'

                _miss_img = str(row.get("صورة_المنافس", "") or "").strip()
                if not _miss_img:
                    _miss_img = _first_image_url_from_row(row) or ""
                _miss_comp_url = competitor_product_url_from_row(row)
                if not _miss_comp_url and _is_http_url_text(name):
                    _miss_comp_url = name.strip()
                if not _miss_img and _miss_comp_url.startswith("http"):
                    _miss_img = _cached_thumb_from_product_url(_miss_comp_url)

                _our_potential_img = ""
                if variant_product and st.session_state.analysis_df is not None:
                    _match_row = st.session_state.analysis_df[
                        st.session_state.analysis_df["المنتج"] == variant_product
                    ]
                    if not _match_row.empty:
                        _our_potential_img, _ = row_media_urls_from_analysis(_match_row.iloc[0])

                with card_col:
                    if _our_potential_img and _has_variant:
                        images_html = _processed_dual_image_html(
                            _our_potential_img,
                            _miss_img,
                            "منتجنا (محتمل)",
                            name[:40],
                        )
                        st.markdown(images_html, unsafe_allow_html=True)
                    st.markdown(miss_card(
                        name=name, price=price, brand=brand, size=size,
                        ptype=ptype, comp=_comp_show, suggested_price=suggested_price,
                        note=note if _is_similar else "",
                        variant_html=_variant_html, tester_badge=_tester_badge,
                        border_color=_border,
                        confidence_level=conf_level, confidence_score=conf_score,
                        product_id=_miss_pid,
                        image_url=_miss_img,
                        comp_url=_miss_comp_url,
                        title_override=_title_display,
                    ), unsafe_allow_html=True)

                # ── إجراءات مختصرة على البطاقة ───────────────────────────
                a4 = st.columns(1)[0]
                with a4:
                    if st.button("🗑️ تجاهل", key=f"ign_{idx}", use_container_width=True):
                        log_decision(nm_ai,"missing","ignored","تجاهل",0,price,-price,comp)
                        _ign = f"missing_{name}_{idx}"
                        st.session_state.hidden_products.add(_ign)
                        save_hidden_product(_ign, nm_ai, "ignored")
                        save_processed(_ign, nm_ai, comp, "ignored",
                                       new_price=price,
                                       notes="تجاهل من قسم المفقودة")
                        st.rerun()

                st.markdown('<hr style="border:none;border-top:1px solid #0d1a2e;margin:8px 0">', unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات مفقودة!")
    else:
        st.info("ارفع الملفات أولاً")
# ════════════════════════════════════════════════
#  مستبعد — لا تطابق كافٍ في الفهارس (Zero Data Drop)
# ════════════════════════════════════════════════
elif page == "⚪ مستبعد (لا يوجد تطابق)":
    st.header("⚪ منتجات مستبعدة — لا يوجد تطابق مناسب مع منافس")
    st.caption(
        "منتجاتنا التي لم يُعثر لها على مرشح منافس بدرجة كافية، أو بلا أي مرشح في الفهارس. "
        "ليس نفس قسم «مفقود» (منتج عند المنافس ولا يوجد عندنا)."
    )
    db_log("excluded", "view")
    if st.session_state.results and "excluded" in st.session_state.results:
        df = st.session_state.results["excluded"]
        if df is not None and not df.empty:
            st.info(f"⚪ {len(df)} منتج مستبعد — يمكن مراجعة الأسباب في عمود القرار والمصدر")
            render_pro_table(df, "excluded", "excluded")
        else:
            st.success("✅ لا توجد منتجات مستبعدة — كل المنتجات لها مسار مطابقة أو مراجعة")
    else:
        st.info("ارفع الملفات وأجرِ التحليل أولاً")
# ════════════════════════════════════════════════
#  7. تحت المراجعة — v26 مقارنة جنباً إلى جنب
# ════════════════════════════════════════════════
elif page == "⚠️ تحت المراجعة":
    st.header("⚠️ منتجات تحت المراجعة — مطابقة غير مؤكدة")
    db_log("review", "view")

    if st.session_state.results and "review" in st.session_state.results:
        df = st.session_state.results["review"]
        if df is not None and not df.empty:
            st.warning(f"⚠️ {len(df)} منتج بمطابقة غير مؤكدة — يحتاج مراجعة بشرية أو AI")

            # ── تصنيف تلقائي بـ AI ────────────────────────────────────────
            col_r1, col_r2 = st.columns([2, 1])
            with col_r1:
                if st.button("🤖 إعادة تصنيف بالذكاء الاصطناعي", type="primary", key="reclassify_review"):
                    with st.spinner("🤖 AI يعيد تصنيف المنتجات..."):
                        _items_rc = []
                        for _, rr in df.head(30).iterrows():
                            _items_rc.append({
                                "our":       str(rr.get("المنتج","")),
                                "comp":      str(rr.get("منتج_المنافس","")),
                                "our_price": safe_float(rr.get("السعر",0)),
                                "comp_price":safe_float(rr.get("سعر_المنافس",0)),
                            })
                        _rc_results = reclassify_review_items(_items_rc)
                        _adf = st.session_state.get("analysis_df")
                        if _rc_results and _adf is not None and not _adf.empty:
                            _new_adf, _st = _apply_reclassify_to_analysis(_adf, df, _rc_results)
                            st.session_state.analysis_df = _new_adf
                            _r2 = _split_results(_new_adf)
                            _prev_miss = st.session_state.results.get("missing") if st.session_state.results else None
                            if _prev_miss is not None and not (isinstance(_prev_miss, pd.DataFrame) and _prev_miss.empty):
                                _r2["missing"] = _prev_miss
                            else:
                                _r2["missing"] = pd.DataFrame()
                            st.session_state.results = _r2
                            _persist_analysis_after_reclassify(_new_adf)
                            _moved = int(_st.get("applied", 0))
                            st.success(
                                f"✅ تم تطبيق {_moved} تعديلاً على جدول التحليل "
                                f"(تجاهل: ثقة {_st.get('skip_conf',0)}، مراجعة {_st.get('skip_review',0)}، "
                                f"idx {_st.get('skip_idx',0)}، بدون صف {_st.get('skip_no_row',0)})"
                            )
                            st.rerun()
                        elif _rc_results:
                            st.warning("لا يوجد جدول تحليل (analysis_df) — لم يُحفظ التصنيف")
                        else:
                            st.warning("لم يتمكن AI من إعادة التصنيف")
            with col_r2:
                excel_rv = export_to_excel(df, "مراجعة")
                st.download_button("📥 Excel", data=excel_rv, file_name="review.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="rv_dl")

            # ── فلتر بحث ──────────────────────────────────────────────────
            search_rv = st.text_input("🔎 بحث في المنتجات", key="rv_search")
            df_rv = df.copy()
            if search_rv:
                df_rv = df_rv[df_rv.apply(lambda r: search_rv.lower() in str(r.values).lower(), axis=1)]

            st.caption(f"{len(df_rv)} منتج للمراجعة")

            # ── عرض المقارنة جنباً إلى جنب ────────────────────────────────
            PAGE_SIZE = 15
            tp = max(1, (len(df_rv) + PAGE_SIZE - 1) // PAGE_SIZE)
            pn = st.number_input("الصفحة", 1, tp, 1, key="rv_pg") if tp > 1 else 1
            page_rv = df_rv.iloc[(pn-1)*PAGE_SIZE : pn*PAGE_SIZE]

            for idx, row in page_rv.iterrows():
                our_name   = str(row.get("المنتج",""))
                comp_name  = str(row.get("منتج_المنافس","—"))
                our_price  = safe_float(row.get("السعر",0))
                comp_price = safe_float(row.get("سعر_المنافس",0))
                score      = safe_float(row.get("نسبة_التطابق",0))
                brand      = str(row.get("الماركة",""))
                size       = str(row.get("الحجم",""))
                comp_name_s= str(row.get("المنافس",""))
                diff       = our_price - comp_price

                _rv_key = f"review_{our_name}_{idx}"
                if _rv_key in st.session_state.hidden_products:
                    continue

                # لون الثقة
                _score_color = "#4caf50" if score >= 85 else "#ff9800" if score >= 70 else "#f44336"
                _diff_color  = "#f44336" if diff > 10 else "#4caf50" if diff < -10 else "#888"
                _diff_label  = f"+{diff:.0f}" if diff > 0 else f"{diff:.0f}"

                _rv_our_img, _rv_comp_img = row_media_urls_from_analysis(row)
                _rv_our_thumb = lazy_img_tag(_rv_our_img, 56, 56, our_name[:40]) if _rv_our_img else ""
                _rv_comp_thumb = lazy_img_tag(_rv_comp_img, 56, 56, comp_name[:40]) if _rv_comp_img else ""
                _rv_our_url = our_product_url_from_row(row)
                _rv_comp_url = competitor_product_url_from_row(row)
                _rv_our_title = linked_product_title(
                    our_name[:60], _rv_our_url, color="#fff", font_size=".88rem",
                )
                _rv_comp_title = linked_product_title(
                    comp_name[:60], _rv_comp_url, color="#fff", font_size=".88rem",
                )

                # ── بطاقة المقارنة (مع صور عند التوفر) ─────────────────
                st.markdown(f"""
                <div style="border:1px solid #ff980055;border-radius:10px;padding:12px;
                            margin:6px 0;background:linear-gradient(135deg,#0a1628,#0e1a30);">
                  <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                    <span style="font-size:.75rem;color:#888">🏷️ {brand} | 📏 {size}</span>
                    <span style="font-size:.75rem;padding:2px 8px;border-radius:10px;
                                 background:{_score_color}22;color:{_score_color};font-weight:700">
                      نسبة المطابقة: {score:.0f}%
                    </span>
                  </div>
                  <div style="display:grid;grid-template-columns:1fr 60px 1fr;gap:8px;align-items:stretch">
                    <div style="background:#0d2040;border-radius:8px;padding:10px;border:1px solid #4fc3f733;
                                display:flex;align-items:flex-start;gap:10px;flex-direction:row-reverse">
                      <div style="flex:1;min-width:0">
                        <div style="font-size:.65rem;color:#4fc3f7;margin-bottom:4px">📦 منتجنا</div>
                        <div style="line-height:1.35">{_rv_our_title}</div>
                        <div style="font-size:1.1rem;font-weight:900;color:#4caf50;margin-top:6px">{our_price:,.0f} ر.س</div>
                      </div>
                      <div style="flex-shrink:0">{_rv_our_thumb}</div>
                    </div>
                    <div style="text-align:center;display:flex;flex-direction:column;justify-content:center">
                      <div style="font-size:1.2rem;color:{_diff_color};font-weight:900">{_diff_label}</div>
                      <div style="font-size:.6rem;color:#555">ر.س</div>
                    </div>
                    <div style="background:#1a0d20;border-radius:8px;padding:10px;border:1px solid #ff572233;
                                display:flex;align-items:flex-start;gap:10px">
                      <div style="flex-shrink:0">{_rv_comp_thumb}</div>
                      <div style="flex:1;min-width:0">
                        <div style="font-size:.65rem;color:#ff5722;margin-bottom:4px">🏪 {comp_name_s}</div>
                        <div style="line-height:1.35">{_rv_comp_title}</div>
                        <div style="font-size:1.1rem;font-weight:900;color:#ff9800;margin-top:6px">{comp_price:,.0f} ر.س</div>
                      </div>
                    </div>
                  </div>
                </div>""", unsafe_allow_html=True)

                # ── أزرار المراجعة ─────────────────────────────────────
                ba,bb,bc,bd,be = st.columns(5)

                with ba:
                    if st.button("🤖 تحقق AI", key=f"rv_verify_{idx}"):
                        with st.spinner("..."):
                            r_v = verify_match(our_name, comp_name, our_price, comp_price)
                            if r_v.get("success"):
                                conf = r_v.get("confidence",0)
                                match = r_v.get("match", False)
                                reason = str(r_v.get("reason",""))[:200]
                                # تنظيف JSON
                                import re as _re
                                reason = _re.sub(r'```.*?```','', reason, flags=_re.DOTALL)
                                reason = _re.sub(r'\{[^}]{0,200}\}','', reason).strip()
                                _lbl = "✅ نفس المنتج" if match else "❌ مختلف"
                                st.info(f"**{_lbl}** ({conf}%)\n{reason[:150]}")
                            else:
                                st.warning("فشل التحقق")

                with bb:
                    if st.button("✅ موافق", key=f"rv_approve_{idx}"):
                        log_decision(our_name,"review","approved","موافق",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "approved_from_review")
                        save_processed(_rv_key, our_name, comp_name_s, "approved",
                                       old_price=our_price, new_price=our_price,
                                       notes="موافق من تحت المراجعة")
                        st.rerun()

                with bc:
                    if st.button("🔴 سعر أعلى", key=f"rv_raise_{idx}"):
                        log_decision(our_name,"review","price_raise","سعر أعلى",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "moved_price_raise")
                        save_processed(_rv_key, our_name, comp_name_s, "send_price",
                                       old_price=our_price, new_price=comp_price - 1 if comp_price > 0 else our_price,
                                       notes="نُقل من المراجعة → سعر أعلى")
                        st.rerun()

                with bd:
                    if st.button("🔵 مفقود", key=f"rv_missing_{idx}"):
                        log_decision(our_name,"review","missing","مفقود",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "moved_missing")
                        save_processed(_rv_key, our_name, comp_name_s, "send_missing",
                                       new_price=comp_price,
                                       notes="نُقل من المراجعة → مفقود")
                        st.rerun()

                with be:
                    if st.button("🗑️ تجاهل", key=f"rv_ign_{idx}"):
                        log_decision(our_name,"review","ignored","تجاهل",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "ignored_review")
                        save_processed(_rv_key, our_name, comp_name_s, "ignored",
                                       old_price=our_price,
                                       notes="تجاهل من تحت المراجعة")
                        st.rerun()

                st.markdown('<hr style="border:none;border-top:1px solid #0d1a2e;margin:6px 0">',
                            unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات تحت المراجعة!")
    else:
        st.info("ارفع الملفات أولاً")

# ════════════════════════════════════════════════
#  تمت المعالجة — v26
# ════════════════════════════════════════════════
elif page in ("✔️ تمت المعالجة", "✅ تمت المعالجة"):
    st.header("✔️ المنتجات المعالجة")
    st.caption("جميع المنتجات التي تم ترحيلها أو تحديث سعرها أو إضافتها")
    db_log("processed", "view")

    _analysis_df = st.session_state.get("analysis_df", pd.DataFrame())
    _missing_df = (st.session_state.get("results") or {}).get("missing", pd.DataFrame())
    _proc_ids = {str(x) for x in st.session_state.get("processed_price_skus", set())}
    _proc_urls = {str(x) for x in st.session_state.get("processed_missing_urls", set())}

    _processed_price_df = pd.DataFrame()
    if isinstance(_analysis_df, pd.DataFrame) and not _analysis_df.empty and "معرف_المنتج" in _analysis_df.columns:
        _processed_price_df = _analysis_df[_analysis_df["معرف_المنتج"].astype(str).isin(_proc_ids)].copy()

    _processed_missing_df = pd.DataFrame()
    if isinstance(_missing_df, pd.DataFrame) and not _missing_df.empty and "رابط_المنافس" in _missing_df.columns:
        _processed_missing_df = _missing_df[_missing_df["رابط_المنافس"].astype(str).isin(_proc_urls)].copy()

    proc_t1, proc_t2, proc_t3 = st.tabs(["💰 أسعار تم تعديلها", "📦 مفقودات تمت إضافتها", "🤖 ملخص ذكي"])  # FIX: Smart Workflow & AI Tracking
    with proc_t1:
        if _processed_price_df.empty:
            st.info("لا توجد عناصر سعرية معالجة في هذه الجلسة.")
        else:
            st.dataframe(_processed_price_df, use_container_width=True, height=260)
            # FIX: Transparency & Reversibility
            _price_revert_ids = sorted({
                str(x) for x in _processed_price_df.get("معرف_المنتج", pd.Series(dtype=str)).dropna().astype(str).tolist()
                if str(x).strip() not in ("", "nan", "None", "NaN")
            })
            _sel_price_revert = st.multiselect(
                "اختر معرفات المنتجات لإلغاء المعالجة",
                _price_revert_ids,
                key="processed_price_revert_ids",
            )
            if st.button("↩️ إلغاء المعالجة للأسعار المحددة", key="processed_price_revert_btn", disabled=not _sel_price_revert):
                for _pid in _sel_price_revert:
                    st.session_state["processed_price_skus"].discard(str(_pid))
                st.success(f"تمت إعادة {len(_sel_price_revert)} منتج إلى الأقسام الأصلية.")
                st.rerun()
    with proc_t2:
        if _processed_missing_df.empty:
            st.info("لا توجد منتجات مفقودة معالجة في هذه الجلسة.")
        else:
            st.dataframe(_processed_missing_df, use_container_width=True, height=260)
            # FIX: Transparency & Reversibility
            _miss_revert_urls = sorted({
                str(x) for x in _processed_missing_df.get("رابط_المنافس", pd.Series(dtype=str)).dropna().astype(str).tolist()
                if str(x).strip()
            })
            _sel_miss_revert = st.multiselect(
                "اختر روابط المفقودات لإلغاء المعالجة",
                _miss_revert_urls,
                key="processed_missing_revert_urls",
            )
            if st.button("↩️ إلغاء معالجة المفقودات المحددة", key="processed_missing_revert_btn", disabled=not _sel_miss_revert):
                for _u in _sel_miss_revert:
                    st.session_state["processed_missing_urls"].discard(str(_u))
                st.success(f"تمت إعادة {len(_sel_miss_revert)} مفقود إلى قائمته الأصلية.")
                st.rerun()
    with proc_t3:
        if st.button("🤖 توليد تقرير ذكي للإجراءات (AI Summary)", key="processed_ai_summary_btn"):  # FIX: Smart Workflow & AI Tracking
            _price_lines = []
            if not _processed_price_df.empty:
                for _, _r in _processed_price_df.head(120).iterrows():
                    _price_lines.append(
                        f"- المنتج: {str(_r.get('المنتج',''))} | قديم: {safe_float(_r.get('السعر',0)):.2f} | جديد: {safe_float(_r.get('سعر_المنافس',0)):.2f}"
                    )
            _missing_lines = []
            if not _processed_missing_df.empty:
                for _, _r in _processed_missing_df.head(120).iterrows():
                    _missing_lines.append(
                        f"- منتج مفقود مضاف: {str(_r.get('منتج_المنافس',''))} | سعر مرجعي: {safe_float(_r.get('سعر_المنافس',0)):.2f}"
                    )
            _actions_text = (
                "## Price Actions\n"
                + ("\n".join(_price_lines) if _price_lines else "- لا توجد تعديلات أسعار مسجلة في هذه الجلسة")
                + "\n\n## Missing Products Added\n"
                + ("\n".join(_missing_lines) if _missing_lines else "- لا توجد منتجات مفقودة مضافة في هذه الجلسة")
            )
            _ai_sum = generate_action_summary(_actions_text)
            if _ai_sum.get("success"):
                st.success(_ai_sum.get("response", ""))
            else:
                st.info(_ai_sum.get("response", "تعذر توليد الملخص حالياً."))

    processed = get_processed(limit=500)
    if not processed:
        st.info("📭 لا توجد منتجات معالجة بعد")
    else:
        df_proc = pd.DataFrame(processed)

        # إحصاء
        actions = df_proc["action"].value_counts()
        cols_p = st.columns(len(actions) + 1)
        for i, (act, cnt) in enumerate(actions.items()):
            icon = {"send_price":"💰","send_missing":"📦","approved":"✅","removed":"🗑️"}.get(act,"📌")
            cols_p[i].metric(f"{icon} {act}", cnt)
        cols_p[-1].metric("📦 الإجمالي", len(df_proc))

        # فلتر
        act_filter = st.selectbox("نوع الإجراء", ["الكل"] + list(actions.index))
        show_df = df_proc if act_filter == "الكل" else df_proc[df_proc["action"] == act_filter]

        st.markdown("---")

        for _, row in show_df.iterrows():
            p_key  = str(row.get("product_key",""))
            p_name = str(row.get("product_name",""))
            p_act  = str(row.get("action",""))
            p_ts   = str(row.get("timestamp",""))
            p_price_old = safe_float(row.get("old_price",0))
            p_price_new = safe_float(row.get("new_price",0))
            p_notes = str(row.get("notes",""))
            p_comp  = str(row.get("competitor",""))

            icon_map = {"send_price":"💰","send_missing":"📦","approved":"✅","removed":"🗑️"}
            icon = icon_map.get(p_act, "📌")

            col_a, col_b = st.columns([5, 1])
            with col_a:
                price_info = ""
                if p_price_old > 0 and p_price_new > 0:
                    price_info = f" | {p_price_old:.0f} → {p_price_new:.0f} ر.س"
                elif p_price_new > 0:
                    price_info = f" | {p_price_new:.0f} ر.س"
                _notes_html = ("<br><span style='color:#aaa;font-size:.73rem'>" + p_notes[:80] + "</span>") if p_notes else ""
                _arow = _find_analysis_row_for_processed(p_name)
                _p_our_u, _p_comp_u = _lookup_product_urls_from_analysis_session(p_name)
                _url_chips_html = _processed_row_url_chips_html(_p_our_u, _p_comp_u)
                _po, _pc = (
                    row_media_urls_from_analysis(_arow)
                    if _arow is not None
                    else ("", "")
                )
                # إن وُجد رابط صفحة بلا صورة في الجدول — جرّب og:image / أيقونة الموقع
                if (not _po) and (_p_our_u or "").strip().lower().startswith("http"):
                    _po = _cached_thumb_from_product_url(_p_our_u) or ""
                if (not _pc) and (_p_comp_u or "").strip().lower().startswith("http"):
                    _pc = _cached_thumb_from_product_url(_p_comp_u) or ""
                _comp_disp = (
                    str(_arow.get("منتج_المنافس", "") or "").strip()
                    if _arow is not None
                    else ""
                )
                if not _comp_disp:
                    _comp_disp = p_comp or "منافس"
                _thumb_cell = _processed_dual_image_html(_po, _pc, p_name[:100], _comp_disp[:100])
                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:10px;padding:6px 10px;border-radius:6px;background:#0a1628;'
                    f'border:1px solid #1a2a44;font-size:.85rem">'
                    f'{_thumb_cell}'
                    f'<div style="flex:1;min-width:0">'
                    f'<span style="color:#888;font-size:.75rem">{p_ts[:16]}</span> &nbsp;'
                    f'{icon} <b style="color:#4fc3f7">{p_name[:60]}</b>'
                    f'<span style="color:#888"> — {p_act}{price_info}</span>'
                    f'{_notes_html}{_url_chips_html}</div></div>',
                    unsafe_allow_html=True
                )
            with col_b:
                if st.button("↩️ تراجع", key=f"undo_{p_key}"):
                    undo_processed(p_key)
                    # أعد للقائمة النشطة
                    if p_key in st.session_state.hidden_products:
                        st.session_state.hidden_products.discard(p_key)
                    st.success(f"✅ تم التراجع: {p_name[:40]}")
                    st.rerun()

        # تصدير
        st.markdown("---")
        csv_proc = df_proc.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("📥 تصدير CSV", data=csv_proc,
                           file_name="processed_products.csv", mime="text/csv")



# ════════════════════════════════════════════════
#  9. أتمتة Make
# ════════════════════════════════════════════════
elif page == "⚡ أتمتة Make":
    st.header("⚡ أتمتة Make.com")
    db_log("make", "view")

    tab1, tab2, tab3 = st.tabs(["🔗 حالة الاتصال", "📤 إرسال", "📦 القرارات المعلقة"])

    with tab1:
        if st.button("🔍 فحص الاتصال"):
            with st.spinner("..."):
                results = verify_webhook_connection()
                for name, r in results.items():
                    if name != "all_connected":
                        color = "🟢" if r["success"] else "🔴"
                        st.markdown(f"{color} **{name}:** {r['message']}")
                if results.get("all_connected"):
                    st.success("✅ جميع الاتصالات تعمل")

    with tab2:
        if st.session_state.results:
            wh = st.selectbox("نوع الإرسال", ["سعر أعلى (تخفيض)","سعر أقل (رفع)","موافق عليها","مفقودة"])
            key_map = {
                "سعر أعلى (تخفيض)": "price_raise",
                "سعر أقل (رفع)":    "price_lower",
                "موافق عليها":      "approved",
                "مفقودة":           "missing",
            }
            section_type_map = {
                "price_raise": "raise",
                "price_lower": "lower",
                "approved":    "approved",
                "missing":     "missing",
            }
            sec_key  = key_map[wh]
            sec_type = section_type_map[sec_key]
            df_s     = st.session_state.results.get(sec_key, pd.DataFrame())

            if not df_s.empty:
                # معاينة ما سيُرسل
                _prev_cols = ["المنتج","السعر","سعر_المنافس","الماركة"]
                _prev_cols = [c for c in _prev_cols if c in df_s.columns]
                if _prev_cols:
                    st.dataframe(df_s[_prev_cols].head(10), use_container_width=True)

                products = export_to_make_format(df_s, sec_type)
                _sendable = [p for p in products if p.get("name") and p.get("price",0) > 0]
                st.info(f"سيتم إرسال {len(_sendable)} منتج → Make (Payload: product_id + name + price)")

                if st.button("📤 إرسال الآن", type="primary"):
                    if sec_type == "missing":
                        res = send_missing_products(_sendable)
                    else:
                        res = send_price_updates(_sendable)
                    # FIX: Missing Products Display Recovery
                    if sec_type == "missing" and res.get("success"):
                        _missing_link_col = None
                        for _c in ["رابط_المنافس", "الرابط", "رابط المنتج", "url", "رابط", "Link"]:
                            if _c in df_s.columns:
                                _missing_link_col = _c
                                break
                        if _missing_link_col:
                            for _u in df_s[_missing_link_col].dropna().astype(str):
                                _track_processed_missing_url(_u)
                    st.success(res["message"]) if res["success"] else st.error(res["message"])
            else:
                st.info("لا توجد بيانات في هذا القسم")

    with tab3:
        pending = st.session_state.decisions_pending
        if pending:
            st.info(f"📦 {len(pending)} قرار معلق")
            df_p = pd.DataFrame([
                {"المنتج": k, "القرار": v["action"],
                 "وقت القرار": v.get("ts",""), "المنافس": v.get("competitor","")}
                for k, v in pending.items()
            ])
            st.dataframe(df_p.head(200), use_container_width=True)

            c1, c2 = st.columns(2)
            with c1:
                if st.button("📤 إرسال كل القرارات لـ Make"):
                    to_send = [{"name": k, **v} for k, v in pending.items()]
                    res = send_price_updates(to_send)
                    st.success(res["message"])
                    st.session_state.decisions_pending = {}
                    st.rerun()
            with c2:
                if st.button("🗑️ مسح القرارات"):
                    st.session_state.decisions_pending = {}
                    st.rerun()
        else:
            st.info("لا توجد قرارات معلقة")


# ════════════════════════════════════════════════
#  11. كشط المنافسين (Async Scraper Dashboard)
# ════════════════════════════════════════════════
# ════════════════════════════════════════════════
#  11. كشط المنافسين (Unified Dashboard v26.1 — FIXED)
#
#  التغييرات الجذرية عن النسخة السابقة:
#  ✅ Single Source of Truth عبر _get_true_scraper_status()
#  ✅ تحقق PID فعلي من OS (os.kill(pid,0)) وليس JSON فقط
#  ✅ تنظيف الحالة العالقة تلقائياً (stale state cleanup)
#  ✅ كشف التجميد إذا لم يتحدث last_updated منذ 5+ دقائق
#  ✅ منع التشغيل المتداخل على مستوى PID
#  ✅ واجهة موحدة (expanders بدل tabs) — بدون ازدواجية
#  ✅ زر إيقاف مع SIGTERM + تنظيف
#  ✅ Phase labels واضحة: discovering/scraping/retrying/stale/completed/failed/stopped
#  ✅ لا time.sleep()، لا infinite reruns
# ════════════════════════════════════════════════
elif page == "🕷️ كشط المنافسين":
    # vNext: delegate this page بالكامل إلى واجهة scraper_advanced
    # لمنع تكرار الواجهات (legacy dashboard + new daemon panel) في نفس الشاشة.
    try:
        import pages.scraper_advanced as _scraper_advanced_page
        _scraper_advanced_page.show(embedded=False)
        st.stop()
    except Exception as _delegation_err:
        st.error(f"❌ تعذّر تحميل شاشة الكشط الجديدة: {_delegation_err}")

    import subprocess
    import sys as _sys_sc
    import os as _os_scraper
    import json as _json_sc
    import tempfile

    try:
        import fcntl as _fcntl_sc
    except ImportError:
        _fcntl_sc = None

    st.header("🕷️ كشط بيانات المنافسين")
    db_log("scraper", "view")

    # ─── ثوابت المسارات ──────────────────────────────────────────────────────
    _SCRAPER_SCRIPT   = _os_scraper.path.join("scrapers", "async_scraper.py")
    _DATA_SC          = _os_scraper.environ.get("DATA_DIR", "data")
    _PROGRESS_FILE    = _os_scraper.path.join(_DATA_SC, "scraper_progress.json")
    _OUTPUT_CSV       = _os_scraper.path.join(_DATA_SC, "competitors_latest.csv")
    _COMPETITORS_FILE = _os_scraper.path.join(_DATA_SC, "competitors_list.json")
    _PID_FILE         = _os_scraper.path.join(_DATA_SC, "scraper.pid")
    _LOG_FILE         = _os_scraper.path.join(_DATA_SC, "scraper_stderr.log")

    # ════════════════════════════════════════════════════════════════════════
    #  دوال البنية التحتية (Infrastructure Layer)
    # ════════════════════════════════════════════════════════════════════════

    def _is_process_alive(pid: int) -> bool:
        """
        يتحقق على مستوى kernel من أن العملية بهذا PID موجودة فعلاً.
        os.kill(pid, 0) لا يرسل إشارة — فقط يتحقق من وجود العملية.
        """
        if not pid or pid <= 0:
            return False
        try:
            _os_scraper.kill(pid, 0)
            return True
        except ProcessLookupError:
            # العملية غير موجودة
            return False
        except PermissionError:
            # موجودة لكن لا صلاحية لإشارتها (= حية بالفعل)
            return True
        except Exception:
            return False

    def _read_pid_file() -> int:
        """يقرأ PID من الملف — يعيد 0 عند أي خطأ."""
        try:
            if not _os_scraper.path.exists(_PID_FILE):
                return 0
            with open(_PID_FILE, "r", encoding="utf-8") as pf:
                raw = (pf.read() or "").strip()
                return int(raw) if raw.isdigit() else 0
        except (ValueError, OSError):
            return 0

    def _load_progress_raw() -> dict:
        """
        يقرأ ملف التقدم كما هو — دون أي تحقق من العملية.
        يستخدم shared lock على Linux لتجنب Partial Read.
        """
        _EMPTY = {"running": False}
        if not _os_scraper.path.exists(_PROGRESS_FILE):
            return _EMPTY
        try:
            with open(_PROGRESS_FILE, "r", encoding="utf-8") as fh:
                if _fcntl_sc is not None:
                    try:
                        _fcntl_sc.flock(fh, _fcntl_sc.LOCK_SH | _fcntl_sc.LOCK_NB)
                    except OSError:
                        pass
                raw = fh.read()
                if not raw or not raw.strip():
                    return _EMPTY
                data = _json_sc.loads(raw)
                return data if isinstance(data, dict) else _EMPTY
        except (_json_sc.JSONDecodeError, OSError):
            return _EMPTY
        except Exception:
            return _EMPTY

    def _write_progress_safe(data: dict) -> None:
        """كتابة آمنة لملف التقدم عبر ملف مؤقت ثم استبدال ذري."""
        _os_scraper.makedirs(_DATA_SC, exist_ok=True)
        content = _json_sc.dumps(data, ensure_ascii=False, indent=2)
        dir_path = _os_scraper.path.dirname(_os_scraper.path.abspath(_PROGRESS_FILE))
        try:
            fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp", prefix="prog_")
            try:
                with _os_scraper.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(content)
                    fh.flush()
                    _os_scraper.fsync(fh.fileno())
                _os_scraper.replace(tmp_path, _PROGRESS_FILE)
            except Exception:
                try:
                    _os_scraper.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception:
            # fallback مباشر بدون ملف مؤقت
            with open(_PROGRESS_FILE, "w", encoding="utf-8") as fh:
                fh.write(content)

    def _cleanup_stale_state() -> None:
        """
        ينظف الحالة العالقة:
        - يكتب running=False + phase=stopped في JSON
        - يحذف PID file
        يُستدعى تلقائياً عند اكتشاف: JSON يقول running=True لكن العملية ميتة.
        """
        try:
            prog = _load_progress_raw()
            prog["running"] = False
            prog["phase"] = "stopped"
            if not prog.get("finished_at"):
                prog["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _write_progress_safe(prog)
        except Exception:
            pass
        try:
            if _os_scraper.path.exists(_PID_FILE):
                _os_scraper.remove(_PID_FILE)
        except OSError:
            pass

    def _get_true_scraper_status() -> dict:
        """
        ═══════════════════════════════════════════════════
        SINGLE SOURCE OF TRUTH لحالة الكاشط
        ═══════════════════════════════════════════════════
        يقرأ JSON + يتحقق من PID فعلياً على مستوى OS.
        إذا اكتشف حالة عالقة → ينظفها فوراً.

        يُعيد dict يحتوي على:
          is_alive  : bool — الكاشط حي فعلاً (PID موجود + JSON يقول running)
          phase     : str  — discovering/scraping/retrying/stale/completed/failed/stopped
          data      : dict — بيانات التقدم الكاملة من JSON
          pid       : int  — PID العملية الحالية (0 إذا لا يوجد)
          was_stale : bool — هل كانت حالة عالقة تم تنظيفها
        """
        prog = _load_progress_raw()
        json_says_running = bool(prog.get("running", False))
        pid = _read_pid_file()
        process_alive = _is_process_alive(pid) if pid > 0 else False
        was_stale = False

        # ── تحقق الحالة العالقة ──────────────────────────────────────────
        if json_says_running and not process_alive:
            # JSON يقول running لكن العملية ميتة → حالة عالقة
            _cleanup_stale_state()
            prog["running"] = False
            prog["phase"] = "stopped"
            was_stale = True
            is_alive = False
        else:
            is_alive = json_says_running and process_alive

        # ── استنتاج الـ phase ─────────────────────────────────────────────
        phase = str(prog.get("phase", "")).strip()
        if not phase:
            if is_alive:
                phase = "scraping"
            elif prog.get("finished_at"):
                phase = "completed"
            else:
                phase = "stopped"

        # ── كشف التجميد: last_updated ثابت منذ +5 دقائق رغم is_alive ─────
        if is_alive and not was_stale:
            last_upd_str = str(prog.get("last_updated", "")).strip()
            if last_upd_str:
                try:
                    last_upd = datetime.strptime(last_upd_str[:19], "%Y-%m-%d %H:%M:%S")
                    age_sec = (datetime.now() - last_upd).total_seconds()
                    if age_sec > 300:   # 5 دقائق بدون تحديث = مشبوه
                        phase = "stale"
                except Exception:
                    pass

        return {
            "is_alive":  is_alive,
            "phase":     phase,
            "data":      prog,
            "pid":       pid,
            "was_stale": was_stale,
        }

    def _load_stores() -> list:
        try:
            return _json_sc.loads(open(_COMPETITORS_FILE, encoding="utf-8").read())
        except Exception:
            return []

    def _save_stores(lst: list) -> None:
        """حفظ آمن لقائمة المتاجر عبر ملف مؤقت ثم استبدال ذري."""
        _os_scraper.makedirs(_DATA_SC, exist_ok=True)
        content = _json_sc.dumps(lst, ensure_ascii=False, indent=2)
        dir_path = _os_scraper.path.dirname(_os_scraper.path.abspath(_COMPETITORS_FILE))
        try:
            fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp", prefix="stores_")
            try:
                with _os_scraper.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(content)
                    fh.flush()
                    _os_scraper.fsync(fh.fileno())
                _os_scraper.replace(tmp_path, _COMPETITORS_FILE)
            except Exception:
                try:
                    _os_scraper.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception:
            with open(_COMPETITORS_FILE, "w", encoding="utf-8") as fh:
                fh.write(content)

    def _load_scraper_state_map() -> dict:
        _state_file = _os_scraper.path.join(_DATA_SC, "scraper_state.json")
        try:
            if not _os_scraper.path.exists(_state_file):
                return {}
            with open(_state_file, "r", encoding="utf-8") as sf:
                data = _json_sc.loads(sf.read() or "{}")
                return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    @st.cache_data(ttl=5, show_spinner=False)
    def _load_csv_rows_by_store(_csv_path: str) -> dict:
        try:
            if not _os_scraper.path.exists(_csv_path):
                return {}
            _df_store = pd.read_csv(_csv_path, usecols=["store"], encoding="utf-8-sig", low_memory=False)
            if "store" not in _df_store.columns:
                return {}
            _counts = _df_store["store"].astype(str).value_counts(dropna=False)
            return {str(k): int(v) for k, v in _counts.to_dict().items()}
        except Exception:
            return {}

    def _read_live_store_progress(domain: str) -> dict:
        _live_file = _os_scraper.path.join(_DATA_SC, f"_sc_live_{domain}.json")
        try:
            if not _os_scraper.path.exists(_live_file):
                return {}
            with open(_live_file, "r", encoding="utf-8") as lf:
                data = _json_sc.loads(lf.read() or "{}")
                return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    # ════════════════════════════════════════════════════════════════════════
    #  الحصول على الحالة الحقيقية — مرة واحدة فقط لكل render cycle
    # ════════════════════════════════════════════════════════════════════════
    _status   = _get_true_scraper_status()
    _is_alive = _status["is_alive"]
    _phase    = _status["phase"]
    _prog     = _status["data"]
    _pid      = _status["pid"]

    # تنبيه بالحالة العالقة إذا اكتُشفت ونُظِّفت
    if _status["was_stale"]:
        st.warning(
            "⚠️ **تم اكتشاف حالة عالقة** — الكاشط انتهى بشكل غير طبيعي لكن الحالة لم تُحدَّث. "
            "تم التنظيف التلقائي. يمكنك الآن إعادة التشغيل."
        )

    # ════════════════════════════════════════════════════════════════════════
    #  Callbacks (جميعها تعمل على on_click لتجنب rerun issues)
    # ════════════════════════════════════════════════════════════════════════

    def _cb_add_store():
        """إضافة متجر مع تطبيع الرابط والتحقق من صحته."""
        url = (st.session_state.get("sc_new_url") or "").strip()
        if not url:
            return
        if not url.startswith("http"):
            url = "https://" + url
        # تحقق بسيط: الرابط يجب أن يحتوي نقطة
        _host = url.replace("https://", "").replace("http://", "").split("/")[0]
        if "." not in _host or len(_host) < 4:
            st.session_state["_sc_msg"] = ("error", "❌ رابط غير صحيح — مثال: https://store.com")
            return
        lst = _load_stores()
        if url not in lst:
            lst.append(url)
            _save_stores(lst)
            st.session_state["_sc_msg"] = ("success", f"✅ تمت الإضافة: {url}")
        else:
            st.session_state["_sc_msg"] = ("warning", "⚠️ الرابط موجود مسبقاً")
        st.session_state["sc_new_url"] = ""

    def _cb_remove_store(idx_to_remove: int):
        lst = _load_stores()
        if 0 <= idx_to_remove < len(lst):
            removed = lst.pop(idx_to_remove)
            _save_stores(lst)
            st.session_state["_sc_msg"] = ("success", f"🗑️ تم حذف: {removed}")

    def _cb_stop_scraper():
        """
        إيقاف الكاشط الجاري بأمان:
        1. SIGTERM للعملية
        2. تنظيف الحالة
        """
        _cur_pid = _read_pid_file()
        if _cur_pid and _is_process_alive(_cur_pid):
            try:
                _os_scraper.kill(_cur_pid, 15)  # SIGTERM
                st.session_state["_sc_msg"] = (
                    "warning",
                    f"⏹️ تم إرسال إشارة إيقاف للكاشط (PID: {_cur_pid})"
                )
            except ProcessLookupError:
                st.session_state["_sc_msg"] = ("info", "العملية انتهت بالفعل")
            except Exception as e:
                st.session_state["_sc_msg"] = ("error", f"❌ فشل الإيقاف: {e}")
        _cleanup_stale_state()

    def _start_scraper_bg():
        """
        تشغيل الكاشط في الخلفية بحارس PID كامل:
        1. تحقق من وجود متاجر
        2. تحقق من PID القديم (هل هو حي؟)
        3. نظف الحالة العالقة إذا وجدت
        4. أطلق العملية الجديدة + سجل PID فوراً
        5. ابدأ ملف التقدم بحالة نظيفة
        """
        stores = _load_stores()
        if not stores:
            st.session_state["_sc_err"] = "لا توجد متاجر — أضف رابطاً أولاً"
            return

        if not _os_scraper.path.exists(_SCRAPER_SCRIPT):
            st.session_state["_sc_err"] = f"ملف الكاشط غير موجود: {_SCRAPER_SCRIPT}"
            return

        # ── حارس PID: منع التشغيل المتداخل ──────────────────────────────
        old_pid = _read_pid_file()
        if old_pid and _is_process_alive(old_pid):
            st.session_state["_sc_err"] = (
                f"⚠️ الكاشط يعمل بالفعل (PID: {old_pid}). "
                "اضغط «إيقاف» إذا أردت إعادة التشغيل."
            )
            return

        # تنظيف الحالة العالقة إذا وجدت
        if old_pid and not _is_process_alive(old_pid):
            _cleanup_stale_state()

        _os_scraper.makedirs(_DATA_SC, exist_ok=True)

        try:
            max_prod = (
                0 if st.session_state.get("sc_all_products", True)
                else int(st.session_state.get("sc_max_prod", 0) or 0)
            )
            concurrency = int(st.session_state.get("sc_concurrency", 8))

            log_fh = open(_LOG_FILE, "w", encoding="utf-8")
            proc = subprocess.Popen(
                [
                    _sys_sc.executable, _SCRAPER_SCRIPT,
                    "--max-products", str(max_prod),
                    "--concurrency",  str(concurrency),
                ],
                stdout=subprocess.DEVNULL,
                stderr=log_fh,
                start_new_session=True,  # عملية مستقلة تماماً عن Streamlit
            )

            # حفظ PID فوراً قبل أي شيء آخر
            with open(_PID_FILE, "w", encoding="utf-8") as pf:
                pf.write(str(proc.pid))

            # تهيئة ملف التقدم بحالة نظيفة
            _write_progress_safe({
                "running":           True,
                "phase":             "discovering",
                "pid":               proc.pid,
                "started_at":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_updated":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "stores_total":      len(stores),
                "stores_done":       0,
                "urls_total":        0,
                "urls_processed":    0,
                "rows_in_csv":       0,
                "fetch_exceptions":  0,
                "success_rate_pct":  0.0,
                "current_store":     "",
                "store_urls_done":   0,
                "store_urls_total":  0,
                "stores_results":    {},
                "last_error":        "",
                "finished_at":       "",
            })

            st.session_state["_sc_started"] = True
            st.session_state["_sc_pid"]     = proc.pid

        except FileNotFoundError:
            st.session_state["_sc_err"] = f"Python غير موجود: {_sys_sc.executable}"
        except PermissionError:
            st.session_state["_sc_err"] = "رُفض الإذن لتشغيل العملية — تحقق من صلاحيات النظام"
        except Exception as _exc:
            st.session_state["_sc_err"] = f"فشل التشغيل: {str(_exc)[:150]}"

    # ════════════════════════════════════════════════════════════════════════
    #  عرض الرسائل الآنية من Callbacks
    # ════════════════════════════════════════════════════════════════════════
    if _sc_msg := st.session_state.pop("_sc_msg", None):
        getattr(st, _sc_msg[0], st.info)(_sc_msg[1])
    if st.session_state.pop("_sc_started", False):
        st.success(
            f"✅ بدأ الكاشط في الخلفية (PID: {st.session_state.get('_sc_pid','?')}) "
            "— التقدم يتحدث تلقائياً كل 3 ثوانٍ"
        )
    if _sc_err := st.session_state.pop("_sc_err", None):
        st.error(f"❌ {_sc_err}")

    # ════════════════════════════════════════════════════════════════════════
    #  القسم 1 — إدارة متاجر المنافسين
    # ════════════════════════════════════════════════════════════════════════
    with st.expander("🌐 إدارة متاجر المنافسين", expanded=not _is_alive):
        _col_url, _col_add = st.columns([5, 1])
        with _col_url:
            st.text_input(
                "رابط متجر المنافس",
                placeholder="https://example.com  ← أدخل الرابط ثم اضغط إضافة",
                key="sc_new_url",
                label_visibility="collapsed",
                help="يقبل أي متجر: سلة، زد، Shopify — النظام يكتشف المنصة تلقائياً",
                disabled=_is_alive,
            )
        with _col_add:
            st.button(
                "➕ إضافة",
                on_click=_cb_add_store,
                key="btn_add_store",
                use_container_width=True,
                disabled=_is_alive,
            )

        _stores_list = _load_stores()
        if _stores_list:
            st.caption(f"**{len(_stores_list)} متجر مستهدف:**")
            for _si, _surl in enumerate(_stores_list):
                _domain = (
                    _surl.replace("https://", "").replace("http://", "")
                    .rstrip("/").split("/")[0]
                )
                _r1, _r2 = st.columns([7, 1])
                with _r1:
                    st.markdown(
                        f'<div style="padding:5px 10px;background:#1a1a2e;border-radius:6px;'
                        f'font-size:.85rem;margin-bottom:2px">'
                        f'{_si + 1}. <b>{_domain}</b>'
                        f'<span style="color:#444;font-size:.75rem"> — {_surl}</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                with _r2:
                    st.button(
                        "🗑️",
                        key=f"del_store_{_si}",
                        on_click=_cb_remove_store,
                        args=(_si,),
                        use_container_width=True,
                        help=f"حذف {_surl}",
                        disabled=_is_alive,  # لا حذف أثناء الكشط
                    )
        else:
            st.info(
                "💡 **أضف رابط متجر** — النظام يتولى تلقائياً:\n"
                "اكتشاف المنصة ← جمع الروابط ← الكشط ← حفظ النتائج"
            )

    # ════════════════════════════════════════════════════════════════════════
    #  القسم 2 — الإعدادات والجدولة
    # ════════════════════════════════════════════════════════════════════════
    with st.expander("⚙️ إعدادات الكشط والجدولة", expanded=False):
        _sc_c1, _sc_c2, _sc_c3 = st.columns(3)
        with _sc_c1:
            st.checkbox(
                "🔄 جميع المنتجات (بلا سقف)",
                value=True,
                key="sc_all_products",
                help="يكشط كل منتج موجود في Sitemap — موصى به",
                disabled=_is_alive,
            )
        with _sc_c2:
            st.number_input(
                "أقصى منتجات / متجر",
                0, 50000,
                0 if st.session_state.get("sc_all_products", True) else 1000,
                step=500,
                key="sc_max_prod",
                disabled=_is_alive or bool(st.session_state.get("sc_all_products", True)),
                help="0 = جميع المنتجات",
            )
        with _sc_c3:
            st.number_input(
                "طلبات متزامنة",
                2, 30, 8,
                step=1,
                key="sc_concurrency",
                help="زيادة الرقم = أسرع لكن احتمال حظر أكبر. موصى به: 6–10",
                disabled=_is_alive,
            )

        # ── الجدولة التلقائية ────────────────────────────────────────────
        st.markdown("---")
        st.markdown("**⏰ الجدولة التلقائية**")

        try:
            from scrapers.scheduler import (
                get_scheduler_status, enable_scheduler, disable_scheduler,
                trigger_now as _trigger_now,
            )
            _sch        = get_scheduler_status()
            _sch_enabled  = bool(_sch.get("enabled", False))
            _sch_interval = int(_sch.get("interval_hours", 12))
            _sch_runs     = int(_sch.get("runs_count", 0))
            _sch_last     = str(_sch.get("last_run", "") or "لم يعمل بعد")[:19]
            _sch_next     = _sch.get("next_run_label", "—")
            _sch_ok       = True
        except Exception:
            _sch_ok      = False
            _sch_enabled = False

        if _sch_ok:
            _sh1, _sh2 = st.columns([4, 2])
            with _sh1:
                if _sch_enabled:
                    st.success(
                        f"🤖 مُفعَّلة — كل {_sch_interval}h "
                        f"| التالي: **{_sch_next}** "
                        f"| التشغيلات: {_sch_runs}"
                    )
                else:
                    st.caption("⏸️ الجدولة التلقائية معطَّلة — فعّلها لكشط المنافسين آلياً")
            with _sh2:
                st.number_input(
                    "تكرار (ساعات)", 1, 168, _sch_interval,
                    step=1, key="sc_interval_h",
                )

            def _cb_toggle_scheduler():
                _h = int(st.session_state.get("sc_interval_h", 12))
                if not _sch_enabled:
                    enable_scheduler(interval_hours=_h)
                    st.session_state["_sc_msg"] = (
                        "success", f"✅ الجدولة مُفعَّلة — كشط كل {_h} ساعة"
                    )
                else:
                    disable_scheduler()
                    st.session_state["_sc_msg"] = ("warning", "⏸️ الجدولة معطَّلة")

            def _cb_run_now_sched():
                """
                تشغيل فوري من الجدولة — مع حارس PID لمنع التداخل.
                """
                _old_pid = _read_pid_file()
                if _old_pid and _is_process_alive(_old_pid):
                    st.session_state["_sc_msg"] = (
                        "error",
                        f"⚠️ الكاشط يعمل بالفعل (PID: {_old_pid}) — انتظر أو أوقفه أولاً"
                    )
                    return
                # تنظيف العالق إن وجد
                if _old_pid and not _is_process_alive(_old_pid):
                    _cleanup_stale_state()
                _mp = (
                    0 if st.session_state.get("sc_all_products", True)
                    else int(st.session_state.get("sc_max_prod", 0) or 0)
                )
                _cc = int(st.session_state.get("sc_concurrency", 8))
                try:
                    ok = _trigger_now(max_products=_mp, concurrency=_cc)
                    if ok:
                        st.session_state["_sc_msg"] = ("success", "🚀 تم إطلاق الكشط الآن!")
                    else:
                        st.session_state["_sc_msg"] = ("error", "❌ فشل تشغيل الكاشط من الجدولة")
                except Exception as _te:
                    st.session_state["_sc_msg"] = ("error", f"❌ خطأ في trigger_now: {_te}")

            _sb1, _sb2 = st.columns(2)
            with _sb1:
                st.button(
                    "⏸️ تعطيل الجدولة" if _sch_enabled else "🤖 تفعيل الجدولة",
                    on_click=_cb_toggle_scheduler,
                    key="btn_toggle_sched",
                    use_container_width=True,
                    type="secondary" if _sch_enabled else "primary",
                )
            with _sb2:
                st.button(
                    "🚀 تشغيل الآن (جدولة)",
                    on_click=_cb_run_now_sched,
                    key="btn_run_now_sched",
                    use_container_width=True,
                    disabled=_is_alive,
                )
        else:
            st.caption("⚠️ وحدة الجدولة غير متاحة (scrapers/scheduler.py)")

    # ════════════════════════════════════════════════════════════════════════
    #  القسم 3 — أزرار التشغيل الرئيسية + تقدير الحجم
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    _stores_count = len(_load_stores())

    _btn_c1, _btn_c2 = st.columns([3, 2])
    with _btn_c1:
        if _is_alive:
            st.button(
                f"⏹️ إيقاف الكاشط (PID: {_pid})",
                on_click=_cb_stop_scraper,
                key="btn_stop_scraper",
                use_container_width=True,
                type="secondary",
            )
        else:
            _start_label = (
                "🚀 بدء الكشط" if _stores_count > 0
                else "🚀 بدء الكشط — أضف متجراً أولاً"
            )
            st.button(
                _start_label,
                type="primary",
                on_click=_start_scraper_bg,
                key="btn_start_scraper",
                use_container_width=True,
                disabled=(_stores_count == 0),
            )

    with _btn_c2:
        if _stores_count > 0:
            _all_flag = bool(st.session_state.get("sc_all_products", True))
            _limit    = int(st.session_state.get("sc_max_prod", 0) or 0)
            _est_txt  = (
                f"جميع المنتجات من **{_stores_count}** متجر"
                if (_all_flag or _limit == 0)
                else f"حتى **{_stores_count * _limit:,}** منتج"
            )
            st.info(f"📊 {_est_txt}")

    # ════════════════════════════════════════════════════════════════════════
    #  القسم 4 — لوحة المراقبة الحية
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.subheader("📊 لوحة المراقبة")

    # ── تحديث تلقائي فقط عند التشغيل الفعلي (لا infinite reruns) ─────────
    if _is_alive:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=3000, key="sc_autorefresh", limit=None)
        except ImportError:
            st.caption("💡 ثبّت `streamlit-autorefresh` للتحديث التلقائي")

    # ── تعريف labels لكل حالة ─────────────────────────────────────────────
    _PHASE_META = {
        "discovering": ("🔍 اكتشاف الروابط...",      "#1a2a3a", "#4fc3f7"),
        "scraping":    ("🔄 جاري الكشط...",           "#0a2a0a", "#00C853"),
        "retrying":    ("⏳ إعادة محاولة (backoff)...", "#2a1a00", "#FFA000"),
        "stale":       ("⚠️ يبدو معلقاً — يجري التحقق (5+ دق بدون تحديث)", "#2a1800", "#FF6F00"),
        "completed":   ("✅ اكتمل بنجاح",              "#0a2a0a", "#00C853"),
        "failed":      ("❌ فشل",                      "#2a0a0a", "#EF5350"),
        "stopped":     ("⏹️ موقوف",                   "#1a1a1a", "#9e9e9e"),
    }
    _plabel, _pbg, _pcolor = _PHASE_META.get(_phase, ("◻️ غير معروف", "#111", "#666"))

    # ── استخراج أرقام التقدم ─────────────────────────────────────────────
    _rows        = int(_prog.get("rows_in_csv", 0))
    _errors      = int(_prog.get("fetch_exceptions", 0))
    _success_raw = float(_prog.get("success_rate_pct", 0))
    _success     = min(_success_raw, 100.0)           # لا تتجاوز 100%
    _current     = str(_prog.get("current_store", ""))
    _last_err    = str(_prog.get("last_error", ""))
    _stores_done = int(_prog.get("stores_done", 0))
    _stores_tot  = max(int(_prog.get("stores_total", 1)), 1)
    _s_urls_done = int(_prog.get("store_urls_done", 0))
    _s_urls_tot  = max(int(_prog.get("store_urls_total", 1)), 1)
    _stores_res  = dict(_prog.get("stores_results") or {})
    _finished    = str(_prog.get("finished_at", ""))
    _started     = str(_prog.get("started_at", ""))

    if not _os_scraper.path.exists(_PROGRESS_FILE) and not _is_alive:
        # لم يبدأ أي كشط بعد
        if _stores_count > 0:
            st.info(
                f"💡 **{_stores_count} متجر جاهز** — اضغط «بدء الكشط» للانطلاق.\n\n"
                "النظام يتولى تلقائياً: اكتشاف الروابط ← الكشط ← حفظ النتائج ← إدارة الحظر"
            )
        else:
            st.info("💡 أضف متجر منافس أولاً من القسم الأول.")
    else:
        # ── شارة الحالة ──────────────────────────────────────────────────
        _status_extra = ""
        if _is_alive and _current:
            _status_extra = (
                f' — المتجر: <b style="color:{_pcolor}">{_current}</b>'
                f'<span style="color:#555;font-size:.78rem;margin-right:8px"> PID:{_pid}</span>'
            )
        elif _finished and not _is_alive:
            _status_extra = f'<span style="color:#555;font-size:.78rem"> — انتهى: {_finished[:16]}</span>'

        st.markdown(
            f'<div style="background:{_pbg};border:1px solid {_pcolor};'
            f'border-radius:8px;padding:10px 16px;margin-bottom:10px">'
            f'<b style="color:{_pcolor}">{_plabel}</b>'
            f'{_status_extra}'
            f'</div>',
            unsafe_allow_html=True,
        )

        # ── أشرطة التقدم ─────────────────────────────────────────────────
        _store_pct = min(_stores_done / _stores_tot, 1.0)
        st.progress(
            _store_pct,
            text=f"🏪 المتاجر: {_stores_done} / {_stores_tot}  ({_store_pct * 100:.0f}%)",
        )

        if _is_alive and _current and _s_urls_tot > 1:
            _cur_pct = min(_s_urls_done / _s_urls_tot, 1.0)
            st.progress(
                _cur_pct,
                text=f"🔗 {_current}: {_s_urls_done:,} / {_s_urls_tot:,} رابط  ({_cur_pct * 100:.0f}%)",
            )

        # ── بطاقات الأرقام ────────────────────────────────────────────────
        _mc1, _mc2, _mc3, _mc4 = st.columns(4)
        _mc1.metric("🏪 متاجر",     f"{_stores_done}/{_stores_tot}")
        _mc2.metric("📦 منتجات",    f"{_rows:,}")
        _mc3.metric("📈 نجاح",      f"{_success:.1f}%")
        _mc4.metric("⚠️ أخطاء",    str(_errors))

        # ── قائمة المتاجر التفصيلية ──────────────────────────────────────
        _all_stores_list = _load_stores()
        _state_map = _load_scraper_state_map()
        _csv_counts = _load_csv_rows_by_store(_OUTPUT_CSV)
        if _all_stores_list:
            st.markdown("**📋 تفاصيل المتاجر:**")
            _html_items = []
            for _si, _surl in enumerate(_all_stores_list):
                _d = (
                    _surl.replace("https://", "").replace("http://", "")
                    .rstrip("/").split("/")[0]
                )
                _cp = _state_map.get(_d, {}) if isinstance(_state_map, dict) else {}
                _live = _read_live_store_progress(_d)
                _cnt_candidates = [
                    _stores_res.get(_d),
                    _cp.get("rows_saved"),
                    _live.get("rows_saved"),
                    _csv_counts.get(_d),
                ]
                _cnt = None
                for _candidate in _cnt_candidates:
                    try:
                        if _candidate is not None:
                            _candidate_int = int(_candidate)
                            _cnt = max(_cnt or 0, _candidate_int)
                    except Exception:
                        continue

                if _d == _current and _is_alive:
                    _live_urls_done = int(_live.get("urls_done", _s_urls_done) or 0)
                    _live_urls_total = max(int(_live.get("urls_total", _s_urls_tot) or 1), 1)
                    _live_rows = max(int(_live.get("rows_saved", 0) or 0), int(_cnt or 0))
                    _cbar = int(min(_live_urls_done / _live_urls_total, 1.0) * 100) if _live_urls_total > 1 else 0
                    _item = (
                        f'<div style="background:#0a1a2a;border:1px solid #4fc3f7;'
                        f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                        f'🔄 <b style="color:#4fc3f7">{_si+1}. {_d}</b>'
                        f'<span style="color:#9e9e9e"> — {_live_urls_done:,}/{_live_urls_total:,} رابط</span>'
                        f'<span style="color:#4fc3f7"> — {_live_rows:,} منتج محفوظ</span>'
                        f'<div style="margin-top:4px;height:4px;background:#1a2a3a;border-radius:2px">'
                        f'<div style="width:{_cbar}%;height:100%;background:#4fc3f7;border-radius:2px"></div>'
                        f'</div></div>'
                    )
                elif _cnt is not None:
                    _item = (
                        f'<div style="background:#0a1a0a;border:1px solid #1e3a1e;'
                        f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                        f'✅ <span style="color:#9e9e9e">{_si+1}. {_d}</span>'
                        f'<span style="color:#00C853"> — {_cnt:,} منتج</span>'
                        f'</div>'
                    )
                elif _cp.get("status") == "done" or _si < _stores_done:
                    _item = (
                        f'<div style="background:#0a1a0a;border:1px solid #1e3a1e;'
                        f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                        f'✅ <span style="color:#777">{_si+1}. {_d}</span>'
                        f'<span style="color:#90a4ae"> — اكتمل، جارِ مزامنة العدد</span>'
                        f'</div>'
                    )
                elif _cp.get("status") == "error":
                    _item = (
                        f'<div style="background:#2a0a0a;border:1px solid #7f1d1d;'
                        f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                        f'❌ <span style="color:#ef9a9a">{_si+1}. {_d}</span>'
                        f'</div>'
                    )
                elif _is_alive:
                    _item = (
                        f'<div style="background:#111;border:1px dashed #333;'
                        f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                        f'⏳ <span style="color:#555">{_si+1}. {_d}</span>'
                        f'</div>'
                    )
                else:
                    _item = (
                        f'<div style="background:#111;border:1px solid #222;'
                        f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                        f'⬜ <span style="color:#777">{_si+1}. {_d}</span>'
                        f'</div>'
                    )
                _html_items.append(_item)

            st.markdown(
                '<div style="display:flex;flex-direction:column;gap:4px;margin-top:6px">'
                + "".join(_html_items)
                + "</div>",
                unsafe_allow_html=True,
            )

        # ── سجل الأخطاء ──────────────────────────────────────────────────
        if _last_err:
            with st.expander("⚠️ آخر خطأ مسجل", expanded=False):
                st.error(_last_err)

        # ── سجل stderr ───────────────────────────────────────────────────
        if _os_scraper.path.exists(_LOG_FILE):
            _log_size_bytes = _os_scraper.path.getsize(_LOG_FILE)
            if _log_size_bytes > 0:
                with st.expander(
                    f"📄 سجل التشغيل ({_log_size_bytes // 1024 + 1} KB)",
                    expanded=False,
                ):
                    try:
                        with open(_LOG_FILE, "r", encoding="utf-8", errors="replace") as _lf:
                            _log_content = _lf.read()
                        # آخر 3000 حرف فقط
                        _log_tail = _log_content[-3000:] if len(_log_content) > 3000 else _log_content
                        st.code(_log_tail, language=None)
                    except Exception:
                        st.caption("تعذّر قراءة السجل")

        # زر تحديث يدوي فقط إذا لم يكن التحديث تلقائياً
        if not _is_alive:
            st.button("🔄 تحديث يدوي", key="sc_manual_refresh")

    # ════════════════════════════════════════════════════════════════════════
    #  القسم 5 — الناتج وبدء المطابقة
    # ════════════════════════════════════════════════════════════════════════
    if _os_scraper.path.exists(_OUTPUT_CSV):
        st.markdown("---")
        st.subheader("📥 الناتج وبدء المطابقة")

        _csv_size_kb = round(_os_scraper.path.getsize(_OUTPUT_CSV) / 1024, 1)
        _csv_rows    = 0
        try:
            with open(_OUTPUT_CSV, encoding="utf-8-sig") as _f:
                _csv_rows = sum(1 for _ in _f) - 1
        except Exception:
            pass

        _dl_col, _go_col = st.columns(2)
        with _dl_col:
            with open(_OUTPUT_CSV, "rb") as _fout:
                st.download_button(
                    f"📥 تحميل الملف ({_csv_size_kb} KB · {_csv_rows:,} منتج)",
                    data=_fout.read(),
                    file_name="competitors_latest.csv",
                    mime="text/csv",
                    key="sc_download_csv",
                    use_container_width=True,
                )
        with _go_col:
            if st.button(
                "🚀 انتقل للمطابقة واستخدم البيانات",
                key="sc_go_match",
                type="primary",
                use_container_width=True,
            ):
                st.session_state._nav_pending    = "📊 لوحة التحكم"
                st.session_state["_use_auto_scraper"] = True
                st.session_state.results         = None
                st.session_state.analysis_df     = None
                st.session_state.last_audit_stats = None
                st.session_state.nav_flash       = "🤖 تم تفعيل البيانات الآلية وتمت تهيئة الشاشة لتحليل جديد"
                st.rerun()

    # ════════════════════════════════════════════════════════════════════════
    #  القسم 6 — أدوات الكشط المتقدمة (مدمجة في expander بدل tab منفصل)
    # ════════════════════════════════════════════════════════════════════════
    _scraper_advanced_runtime_mod = _get_scraper_advanced_module()
    if _scraper_advanced_runtime_mod is not None:
        st.markdown("---")
        with st.expander("🚀 أدوات الكشط المتقدمة", expanded=False):
            if hasattr(_scraper_advanced_runtime_mod, "show"):
                try:
                    _show_fn = getattr(_scraper_advanced_runtime_mod, "show", None)
                    if callable(_show_fn):
                        _supports_embedded = False
                        try:
                            import inspect as _inspect
                            _supports_embedded = "embedded" in _inspect.signature(_show_fn).parameters
                        except Exception:
                            _supports_embedded = False

                        if _supports_embedded:
                            _show_fn(embedded=True)
                        else:
                            _show_fn()
                    else:
                        st.error("⚠️ show ليس دالة قابلة للتنفيذ في pages/scraper_advanced.py")
                except Exception as _sa_render_err:
                    st.error(f"❌ خطأ في الأدوات المتقدمة: {_sa_render_err}")
            else:
                st.error("⚠️ دالة show() غير موجودة في pages/scraper_advanced.py")
    elif _scraper_advanced_import_error is not None:
        st.warning(f"⚠️ تعذر تحميل الأدوات المتقدمة حالياً: {_scraper_advanced_import_error}")

elif page == "⚙️ الإعدادات":
    st.header("⚙️ الإعدادات")
    db_log("settings", "view")

    tab1, tab2, tab3, tab4 = st.tabs(
        ["🔑 المفاتيح", "⚙️ المطابقة", "📋 قرارات المنتجات", "📜 السجل الكامل"]
    )

    with tab1:
        # ── الحالة الحالية ────────────────────────────────────────────────
        st.success(
            "✅ **مسار AI جاهز** (Gemini و/أو OpenRouter و/أو Cohere)"
            if ANY_AI_PROVIDER_CONFIGURED
            else "❌ **لا يوجد أي مزود** — أضف مفتاحاً على الأقل"
        )
        gemini_s = f"✅ {len(GEMINI_API_KEYS)} مفتاح" if GEMINI_API_KEYS else "❌ لا توجد مفاتيح"
        or_s     = "✅ مفعل" if OPENROUTER_API_KEY else "❌ غير موجود"
        co_s     = "✅ مفعل" if COHERE_API_KEY else "❌ غير موجود"
        st.info(f"Gemini API: {gemini_s}")
        st.info(f"OpenRouter: {or_s}")
        st.info(f"Cohere:     {co_s}")
        st.info(f"Webhook أسعار:   {'✅' if WEBHOOK_UPDATE_PRICES else '❌'}")
        st.info(f"Webhook منتجات:  {'✅' if WEBHOOK_NEW_PRODUCTS else '❌'}")

        st.markdown("---")

        # ── تشخيص شامل ───────────────────────────────────────────────────
        st.subheader("🔬 تشخيص AI")
        st.caption("يختبر الاتصال الفعلي بكل مزود ويُظهر الخطأ الحقيقي")

        if st.button("🔬 تشخيص شامل لجميع المزودين", type="primary"):
            with st.spinner("يختبر الاتصال بـ Gemini, OpenRouter, Cohere..."):
                from engines.ai_engine import diagnose_ai_providers
                diag = diagnose_ai_providers()

            # ── نتائج Gemini ──────────────────────────────────────────────
            st.markdown("**Gemini API:**")
            any_gemini_ok = False
            for g in diag.get("gemini", []):
                status = g["status"]
                if "✅" in status:
                    st.success(f"مفتاح {g['key']}: {status}")
                    any_gemini_ok = True
                elif "⚠️" in status:
                    st.warning(f"مفتاح {g['key']}: {status}")
                else:
                    st.error(f"مفتاح {g['key']}: {status}")
                _gd = (g.get("detail") or "").strip()
                if _gd and ("❌" in status or "⚠️" in status):
                    st.caption(f"تفاصيل API: {_gd[:500]}")

            # ── نتائج OpenRouter ──────────────────────────────────────────
            or_res = diag.get("openrouter","")
            st.markdown("**OpenRouter:**")
            if "✅" in or_res: st.success(or_res)
            elif "⚠️" in or_res: st.warning(or_res)
            else: st.error(or_res)

            # ── نتائج Cohere ──────────────────────────────────────────────
            co_res = diag.get("cohere","")
            st.markdown("**Cohere:**")
            if "✅" in co_res: st.success(co_res)
            elif "⚠️" in co_res: st.warning(co_res)
            else: st.error(co_res)

            # ── تحليل وتوصية ─────────────────────────────────────────────
            or_ok = "✅" in or_res
            co_ok = "✅" in co_res

            _recs = diag.get("recommendations") or []
            if _recs:
                st.markdown("**💡 توصيات تلقائية (حسب نتيجة التشخيص)**")
                for _r in _recs:
                    st.info(_r)

            st.markdown("---")
            if any_gemini_ok or or_ok or co_ok:
                working = []
                if any_gemini_ok: working.append("Gemini")
                if or_ok: working.append("OpenRouter")
                if co_ok: working.append("Cohere")
                st.success(f"✅ AI يعمل عبر: {' + '.join(working)}")
            else:
                st.error("❌ جميع المزودين فاشلون")
                # تحليل السبب
                _all_errs = [g["status"] for g in diag.get("gemini",[]) if "❌" in g.get("status","")]
                if any("اتصال" in e or "ConnectionError" in e or "Pool" in e for e in _all_errs + [or_res, co_res]):
                    st.warning("""
**🔴 السبب المحتمل: Streamlit Cloud يحجب الطلبات الخارجية**

الحل: في صفحة تطبيقك على Streamlit Cloud:
1. اذهب إلى ⚙️ Settings → General
2. ابحث عن **"Network"** أو **"Egress"**
3. تأكد أن Outbound connections مسموح بها

أو جرب نشر التطبيق على **Railway** بدلاً من Streamlit Cloud.
                    """)
                elif any("403" in e or "IP" in e for e in _all_errs):
                    st.warning("🔴 مفاتيح Gemini محظورة من IP هذا الخادم — جرب OpenRouter")
                elif any("401" in e for e in _all_errs + [or_res, co_res]):
                    st.warning("🔴 مفتاح غير صحيح — تحقق من المفاتيح في Secrets")

        st.markdown("---")

        # ── سجل الأخطاء الأخيرة ──────────────────────────────────────────
        st.subheader("📋 آخر أخطاء AI")
        from engines.ai_engine import get_last_errors
        errs = get_last_errors()
        if errs:
            for e in errs:
                st.code(e, language=None)
        else:
            st.caption("لا أخطاء مسجلة بعد — جرب أي زر AI ثم ارجع هنا")

        st.markdown("---")

        # ── اختبار سريع ──────────────────────────────────────────────────
        if st.button("🧪 اختبار سريع"):
            with st.spinner("يتصل بـ AI..."):
                r = call_ai("أجب بكلمة واحدة فقط: يعمل", "general")
            if r["success"]:
                st.success(f"✅ AI يعمل عبر {r['source']}: {r['response'][:80]}")
            else:
                st.error("❌ فشل — اضغط 'تشخيص شامل' لمعرفة السبب الدقيق")
                from engines.ai_engine import get_last_errors
                for e in get_last_errors()[:5]:
                    st.code(e, language=None)

    with tab2:
        st.info(f"حد التطابق الأدنى: {MIN_MATCH_SCORE}%")
        st.info(f"حد التطابق العالي: {HIGH_MATCH_SCORE}%")
        st.info(f"هامش فرق السعر: {PRICE_DIFF_THRESHOLD} ر.س")

    with tab3:
        decisions = get_decisions(limit=30)
        if decisions:
            df_dec = pd.DataFrame(decisions)
            st.dataframe(df_dec[["timestamp","product_name","old_status",
                                  "new_status","reason","competitor"]].rename(columns={
                "timestamp":"التاريخ","product_name":"المنتج",
                "old_status":"من","new_status":"إلى",
                "reason":"السبب","competitor":"المنافس"
            }).head(200), use_container_width=True)
        else:
            st.info("لا توجد قرارات مسجلة")

    with tab4:
        db_log("settings", "full_log")
        st.caption("سجل التحليلات، تتبع الأسعار، وأحداث التنقل — مدمج مع الإعدادات")
        log_t1, log_t2, log_t3 = st.tabs(["📊 التحليلات", "💰 تغييرات الأسعار", "📝 الأحداث"])

        with log_t1:
            history = get_analysis_history(20)
            if history:
                df_h = pd.DataFrame(history)
                st.dataframe(df_h[["timestamp","our_file","comp_file",
                                    "total_products","matched","missing"]].rename(columns={
                    "timestamp":"التاريخ","our_file":"ملف منتجاتنا",
                    "comp_file":"ملف المنافس","total_products":"الإجمالي",
                    "matched":"متطابق","missing":"مفقود"
                }).head(200), use_container_width=True)
            else:
                st.info("لا يوجد تاريخ")

        with log_t2:
            days = st.slider("آخر X يوم", 1, 30, 7, key="settings_price_changes_days")
            changes = get_price_changes(days)
            if changes:
                df_c = pd.DataFrame(changes)
                st.dataframe(df_c.rename(columns={
                    "product_name":"المنتج","competitor":"المنافس",
                    "old_price":"السعر السابق","new_price":"السعر الجديد",
                    "price_diff":"التغيير","new_date":"تاريخ التغيير"
                }).head(200), use_container_width=True)
            else:
                st.info(f"لا توجد تغييرات في آخر {days} يوم")

        with log_t3:
            events = get_events(limit=50)
            if events:
                df_e = pd.DataFrame(events)
                st.dataframe(df_e[["timestamp","page","event_type","details"]].rename(columns={
                    "timestamp":"التاريخ","page":"الصفحة",
                    "event_type":"الحدث","details":"التفاصيل"
                }).head(200), use_container_width=True)
            else:
                st.info("لا توجد أحداث")


# ════════════════════════════════════════════════
#  12. الأتمتة الذكية (v26.0 — متصل بالتنقل)
# ════════════════════════════════════════════════
elif page == "🔄 الأتمتة الذكية":
    st.header("🔄 الأتمتة الذكية — محرك القرارات التلقائية")
    db_log("automation", "view")

    # ── إنشاء محرك الأتمتة ──
    if "auto_engine" not in st.session_state:
        st.session_state.auto_engine = AutomationEngine()
    if "search_manager" not in st.session_state:
        st.session_state.search_manager = ScheduledSearchManager()

    engine = st.session_state.auto_engine
    search_mgr = st.session_state.search_manager

    tab_a1, tab_a2, tab_a3, tab_a4 = st.tabs([
        "🤖 تشغيل الأتمتة", "⚙️ قواعد التسعير", "🔍 البحث الدوري", "📊 سجل القرارات"
    ])

    # ── تاب 1: تشغيل الأتمتة ──
    with tab_a1:
        st.subheader("تطبيق القواعد التلقائية على نتائج التحليل")

        if st.session_state.results and st.session_state.analysis_df is not None:
            adf = st.session_state.analysis_df
            matched_df = adf[adf["نسبة_التطابق"].apply(lambda x: safe_float(x)) >= 85].copy()
            st.info(f"📦 {len(matched_df)} منتج مؤكد المطابقة جاهز للتقييم التلقائي")

            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("🚀 تشغيل الأتمتة الآن", type="primary", key="run_auto"):
                    with st.spinner("⚙️ محرك الأتمتة يقيّم المنتجات..."):
                        engine.clear_log()
                        decisions = engine.evaluate_batch(matched_df)
                        st.session_state._auto_decisions = decisions

                        # تسجيل كل قرار في قاعدة البيانات
                        for d in decisions:
                            log_automation_decision(d)

                    if decisions:
                        summary = engine.get_summary()
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("إجمالي القرارات", summary["total"])
                        c2.metric("⬇️ خفض سعر", summary["lower"])
                        c3.metric("⬆️ رفع سعر", summary["raise"])
                        c4.metric("✅ إبقاء", summary["keep"])

                        if summary["net_impact"] > 0:
                            st.success(f"💰 الأثر المالي المتوقع: +{summary['net_impact']:.0f} ر.س (صافي ربح إضافي)")
                        elif summary["net_impact"] < 0:
                            st.warning(f"📉 الأثر المالي: {summary['net_impact']:.0f} ر.س (خفض لتحقيق التنافسية)")

                        # عرض القرارات في جدول
                        dec_df = pd.DataFrame(decisions)
                        display_cols = ["product_name", "action", "old_price", "new_price",
                                        "comp_price", "competitor", "match_score", "reason"]
                        available = [c for c in display_cols if c in dec_df.columns]
                        st.dataframe(dec_df[available].rename(columns={
                            "product_name": "المنتج", "action": "الإجراء",
                            "old_price": "السعر الحالي", "new_price": "السعر الجديد",
                            "comp_price": "سعر المنافس", "competitor": "المنافس",
                            "match_score": "نسبة التطابق", "reason": "السبب"
                        }), use_container_width=True)
                    else:
                        st.info("لم يتم اتخاذ أي قرارات — جميع الأسعار ضمن الهامش المقبول")

            with col_b:
                auto_decisions = st.session_state.get("_auto_decisions", [])
                push_eligible = [d for d in auto_decisions
                                 if d.get("action") in ("lower_price", "raise_price")
                                 and d.get("product_id")]
                if push_eligible:
                    st.warning(f"📤 {len(push_eligible)} قرار جاهز للإرسال إلى Make.com/سلة")
                    if st.button("📤 إرسال القرارات إلى Make.com", key="push_auto"):
                        with st.spinner("يُرسل إلى Make.com..."):
                            result = auto_push_decisions(auto_decisions)
                        if result.get("success"):
                            st.success(result["message"])
                        else:
                            st.error(result["message"])
                else:
                    st.caption("لا توجد قرارات جاهزة للإرسال — شغّل الأتمتة أولاً")

        else:
            st.warning("⚠️ لا توجد نتائج تحليل — ارفع الملفات من أسفل «لوحة التحكم» ثم ابدأ التحليل")

        # ── معالجة قسم المراجعة تلقائياً ──
        st.divider()
        st.subheader("🔄 معالجة قسم المراجعة تلقائياً")
        st.caption("يستخدم AI للتحقق المزدوج من المطابقات غير المؤكدة")

        if st.session_state.results and "review" in st.session_state.results:
            rev_df = st.session_state.results.get("review", pd.DataFrame())
            if not rev_df.empty:
                st.info(f"📋 {len(rev_df)} منتج تحت المراجعة")
                if st.button("🤖 تحقق AI تلقائي لقسم المراجعة", key="auto_review"):
                    with st.spinner("🤖 AI يتحقق من المطابقات..."):
                        confirmed = auto_process_review_items(rev_df.head(15))
                    if not confirmed.empty:
                        st.success(f"✅ تم تأكيد {len(confirmed)} منتج من أصل {min(15, len(rev_df))}")
                        st.dataframe(confirmed[["المنتج", "منتج_المنافس", "القرار"]].head(20),
                                     use_container_width=True)
                    else:
                        st.info("لم يتم تأكيد أي مطابقة — المنتجات تحتاج مراجعة يدوية")
            else:
                st.success("لا توجد منتجات تحت المراجعة")

    # ── تاب 2: قواعد التسعير ──
    with tab_a2:
        st.subheader("⚙️ قواعد التسعير النشطة")
        st.caption("القواعد تُطبّق بالترتيب — أول قاعدة تنطبق تُنفَّذ")

        for i, rule in enumerate(engine.rules):
            with st.expander(f"{'✅' if rule.enabled else '⬜'} {rule.name}", expanded=False):
                st.write(f"**الإجراء:** {rule.action}")
                st.write(f"**حد التطابق الأدنى:** {rule.min_match_score}%")
                for k, v in rule.params.items():
                    if k not in ("name", "enabled", "action", "min_match_score", "condition"):
                        st.write(f"**{k}:** {v}")

        st.divider()
        st.subheader("📝 تخصيص القواعد")
        st.caption("يمكنك تعديل القواعد من ملف config.py → AUTOMATION_RULES_DEFAULT")
        st.code("""
# مثال: إضافة قاعدة جديدة في config.py
AUTOMATION_RULES_DEFAULT.append({
    "name": "خفض عدواني",
    "enabled": True,
    "action": "undercut",
    "min_diff": 5,
    "undercut_amount": 2,
    "min_match_score": 95,
    "max_loss_pct": 10,
})
        """, language="python")

    # ── تاب 3: البحث الدوري ──
    with tab_a3:
        st.subheader("🔍 البحث الدوري عن أسعار المنافسين")

        c1, c2 = st.columns(2)
        c1.metric("⏱️ البحث القادم", search_mgr.time_until_next())
        c2.metric("📊 آخر نتائج", f"{len(search_mgr.last_results)} منتج")

        if st.session_state.analysis_df is not None:
            scan_count = st.slider("عدد المنتجات للمسح", 5, 50, 15, key="scan_n")
            if st.button("🔍 مسح السوق الآن", type="primary", key="scan_now"):
                with st.spinner(f"يبحث عن أسعار {scan_count} منتج في السوق..."):
                    scan_results = search_mgr.run_scan(st.session_state.analysis_df, scan_count)
                if scan_results:
                    st.success(f"✅ تم مسح {len(scan_results)} منتج بنجاح")
                    for sr in scan_results[:10]:
                        md = sr.get("market_data", {})
                        rec = md.get("recommendation", md.get("market_price", "—"))
                        st.markdown(f"**{sr['product']}** — سعرنا: {sr['our_price']:.0f} | السوق: {rec}")
                else:
                    st.warning("لم يتم العثور على نتائج — تحقق من اتصال AI")
        else:
            st.warning("ارفع ملفات التحليل أولاً")

    # ── تاب 4: سجل القرارات ──
    with tab_a4:
        st.subheader("📊 سجل قرارات الأتمتة")
        days_filter = st.selectbox("الفترة", [7, 14, 30], index=0, key="auto_log_days")

        stats = get_automation_stats(days_filter)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("إجمالي", stats["total"])
        c2.metric("خفض", stats["lower"])
        c3.metric("رفع", stats["raise"])
        c4.metric("أُرسل لـ Make", stats["pushed"])

        log_data = get_automation_log(limit=100)
        if log_data:
            log_df = pd.DataFrame(log_data)
            display = ["timestamp", "product_name", "action", "old_price",
                        "new_price", "competitor", "match_score", "pushed_to_make"]
            available = [c for c in display if c in log_df.columns]
            st.dataframe(log_df[available].rename(columns={
                "timestamp": "التاريخ", "product_name": "المنتج",
                "action": "الإجراء", "old_price": "السعر القديم",
                "new_price": "السعر الجديد", "competitor": "المنافس",
                "match_score": "التطابق%", "pushed_to_make": "أُرسل؟"
            }), use_container_width=True)
        else:
            st.info("لا توجد قرارات مسجلة بعد — شغّل الأتمتة من التاب الأول")
