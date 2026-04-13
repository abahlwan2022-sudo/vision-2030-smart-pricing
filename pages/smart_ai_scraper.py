"""
pages/smart_ai_scraper.py — كاشط AI الذكي v1.0
══════════════════════════════════════════════════════════
واجهة Streamlit متكاملة للكشط بضغطة واحدة:

  ① أدخل روابط المنتجات (نص مباشر أو CSV)
  ② اضغط «بدء الكشط الذكي»
  ③ المحرك يعمل طبقة تلو الأخرى لتجاوز الحماية
  ④ Gemini يحلل ويستخرج: title | price | stock_status
  ⑤ النتائج تُعرض فوراً في جدول تفاعلي
  ⑥ تُحفظ تلقائياً في قاعدة البيانات

التكامل مع app.py:
    import pages.smart_ai_scraper as _smart_ai_mod
    # في elif handler:
    _smart_ai_mod.show()
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

import pandas as pd
import streamlit as st

# ── CSS مخصص للصفحة ─────────────────────────────────────────────────────────
_CSS = """
<style>
.sai-header {
    background: linear-gradient(135deg, #0d1b2a 0%, #0a1520 100%);
    border: 1.5px solid #1e3a5f;
    border-radius: 14px;
    padding: 20px 24px 16px;
    margin-bottom: 18px;
}
.sai-kpi {
    background: #0d1b2a;
    border: 1px solid #1e3a5f;
    border-radius: 10px;
    padding: 14px 18px;
    text-align: center;
}
.sai-kpi .num  { font-size: 2rem; font-weight: 900; color: #4fc3f7; }
.sai-kpi .lbl  { font-size: .75rem; color: #607d8b; margin-top: 3px; }
.sai-result-card {
    background: linear-gradient(135deg, #0a1628, #0e1a30);
    border: 1px solid #1e3a5f;
    border-radius: 10px;
    padding: 12px 16px;
    margin: 5px 0;
    font-size: .85rem;
}
.sai-result-card.success { border-left: 4px solid #00C853; }
.sai-result-card.failed  { border-left: 4px solid #FF1744; }
.sai-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: .7rem;
    font-weight: 700;
}
.badge-crawl4ai    { background: #4fc3f722; color: #4fc3f7; border: 1px solid #4fc3f7; }
.badge-curl_cffi   { background: #ff980022; color: #ff9800; border: 1px solid #ff9800; }
.badge-cloudscraper{ background: #ab47bc22; color: #ce93d8; border: 1px solid #ab47bc; }
.badge-requests    { background: #78909c22; color: #90a4ae; border: 1px solid #78909c; }
.badge-none        { background: #FF174422; color: #ef9a9a; border: 1px solid #FF1744; }
.stock-in    { color: #00C853; font-weight: 700; }
.stock-out   { color: #FF1744; font-weight: 700; }
.stock-unk   { color: #9e9e9e; }
</style>
"""

# ════════════════════════════════════════════════════════════════════════════
#  دوال مساعدة
# ════════════════════════════════════════════════════════════════════════════

def _domain(url: str) -> str:
    try:
        return urlparse(url.strip()).netloc.replace("www.", "").strip() or url
    except Exception:
        return url


def _parse_urls_from_text(text: str) -> List[str]:
    """يستخرج روابط صالحة من نص حر (سطر لكل رابط أو مفصولة بفاصلة)."""
    raw   = text.replace(",", "\n").replace(";", "\n")
    lines = [l.strip() for l in raw.splitlines()]
    urls  = []
    for line in lines:
        line = line.strip().strip('"').strip("'")
        if not line:
            continue
        if not line.startswith("http"):
            line = "https://" + line
        if "." in line:
            urls.append(line)
    return list(dict.fromkeys(urls))   # إزالة المكررات مع حفظ الترتيب


def _parse_urls_from_csv(uploaded_file) -> List[str]:
    """يستخرج عمود الروابط من ملف CSV مرفوع."""
    try:
        import io
        df = pd.read_csv(io.BytesIO(uploaded_file.read()), encoding="utf-8-sig")
        uploaded_file.seek(0)
        # ابحث عن عمود رابط
        url_col = None
        for col in df.columns:
            low = str(col).lower()
            if any(k in low for k in ["url", "رابط", "link", "href"]):
                url_col = col
                break
        if url_col is None:
            # استخدم أول عمود
            url_col = df.columns[0]
        urls = df[url_col].dropna().astype(str).tolist()
        return _parse_urls_from_text("\n".join(urls))
    except Exception as e:
        st.error(f"❌ خطأ في قراءة CSV: {e}")
        return []


def _method_badge(method: str) -> str:
    badge_cls = {
        "crawl4ai":    "badge-crawl4ai",
        "curl_cffi":   "badge-curl_cffi",
        "cloudscraper":"badge-cloudscraper",
        "requests":    "badge-requests",
    }.get(method, "badge-none")
    label = method or "none"
    return f'<span class="sai-badge {badge_cls}">{label}</span>'


def _stock_html(status: str) -> str:
    if status == "in_stock":
        return '<span class="stock-in">✅ متوفر</span>'
    elif status == "out_of_stock":
        return '<span class="stock-out">❌ غير متوفر</span>'
    return '<span class="stock-unk">❓ غير محدد</span>'


def _check_api_key() -> bool:
    """تحقق من وجود مفتاح Gemini أو مزود AI آخر."""
    try:
        from config import GEMINI_API_KEYS, ANY_AI_PROVIDER_CONFIGURED
        return bool(GEMINI_API_KEYS) or ANY_AI_PROVIDER_CONFIGURED
    except ImportError:
        import os
        return bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEYS"))


# ════════════════════════════════════════════════════════════════════════════
#  منطق الكشط في خيط مستقل
# ════════════════════════════════════════════════════════════════════════════

def _run_scraping_job(
    urls: List[str],
    concurrency: int,
    delay: float,
    session_key: str,
) -> None:
    """
    يُشغَّل في daemon thread — يحدّث st.session_state مباشرة.
    Streamlit يقرأ الحالة عند كل rerun.
    """
    from utils.gemini_stealth_scraper import SmartAIScraper, ScrapeResult

    scraper = SmartAIScraper(concurrency=concurrency, delay_between=delay)
    results: List[ScrapeResult] = []

    def on_progress(done: int, total: int, result: ScrapeResult) -> None:
        st.session_state[f"{session_key}_done"]    = done
        st.session_state[f"{session_key}_total"]   = total
        st.session_state[f"{session_key}_results"] = [r.to_dict() for r in results + [result]]

    try:
        results = scraper.scrape_batch_sync(
            urls        = urls,
            concurrency = concurrency,
            delay       = delay,
            progress_cb = on_progress,
        )
        st.session_state[f"{session_key}_results"] = [r.to_dict() for r in results]
        st.session_state[f"{session_key}_status"]  = "done"
    except Exception as e:
        st.session_state[f"{session_key}_status"]  = f"error: {e}"
    finally:
        st.session_state[f"{session_key}_running"] = False


# ════════════════════════════════════════════════════════════════════════════
#  الصفحة الرئيسية
# ════════════════════════════════════════════════════════════════════════════

def show(embedded: bool = False) -> None:
    """
    نقطة الدخول الرئيسية — تُستدعى من app.py.
    embedded=True: تُظهر بدون عنوان رئيسي (للدمج في Expander).
    """
    st.markdown(_CSS, unsafe_allow_html=True)

    # ── auto-refresh أثناء التشغيل ──────────────────────────────────────
    if st.session_state.get("sai_running"):
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=2500, key="sai_autorefresh")
        except ImportError:
            pass

    # ── العنوان ─────────────────────────────────────────────────────────
    if not embedded:
        st.markdown("""
        <div class="sai-header">
            <h2 style="margin:0;color:#4fc3f7">🤖 كاشط AI الذكي</h2>
            <p style="margin:6px 0 0;color:#78909c;font-size:.88rem">
                يتجاوز حمايات البوتات تلقائياً ويستخرج بيانات المنتجات بدقة عالية عبر Gemini AI
            </p>
        </div>
        """, unsafe_allow_html=True)

    # ── تحقق من مفتاح API ───────────────────────────────────────────────
    if not _check_api_key():
        st.error(
            "❌ **مفتاح Gemini API غير موجود**\n\n"
            "أضف `GEMINI_API_KEY` كمتغير بيئة أو في Streamlit Secrets لتفعيل هذه الميزة."
        )
        return

    # ── Session State ───────────────────────────────────────────────────
    for k, v in [
        ("sai_running", False),
        ("sai_done", 0),
        ("sai_total", 0),
        ("sai_results", []),
        ("sai_status", "idle"),
        ("sai_saved_to_db", False),
    ]:
        st.session_state.setdefault(k, v)

    # ════════════════════════════════════════════════════════════════════
    #  القسم 1 — إدخال الروابط
    # ════════════════════════════════════════════════════════════════════
    st.markdown("### 🔗 إدخال روابط المنتجات")

    input_tab1, input_tab2 = st.tabs(["📝 إدخال نصي", "📄 رفع CSV"])

    with input_tab1:
        url_text = st.text_area(
            "أدخل روابط المنتجات (رابط في كل سطر أو مفصولة بفاصلة)",
            placeholder=(
                "https://store.salla.sa/products/perfume-xyz\n"
                "https://shop.example.com/item/123\n"
                "https://another-store.com/p/456"
            ),
            height=160,
            key="sai_url_text",
            disabled=st.session_state.sai_running,
            help="يقبل روابط مباشرة لصفحات المنتجات من أي متجر",
        )
        urls_from_text = _parse_urls_from_text(url_text or "")
        if urls_from_text:
            st.caption(f"✅ تم التعرف على **{len(urls_from_text)}** رابط صحيح")

    with input_tab2:
        csv_file = st.file_uploader(
            "ارفع ملف CSV يحتوي على عمود روابط المنتجات",
            type=["csv"],
            key="sai_csv_upload",
            disabled=st.session_state.sai_running,
        )
        urls_from_csv: List[str] = []
        if csv_file:
            urls_from_csv = _parse_urls_from_csv(csv_file)
            if urls_from_csv:
                st.success(f"✅ تم استخراج **{len(urls_from_csv)}** رابط من الملف")
            else:
                st.warning("⚠️ لم يُعثر على روابط صحيحة في الملف")

    # دمج الروابط من المصدرين
    all_urls = list(dict.fromkeys(urls_from_text + urls_from_csv))

    # ════════════════════════════════════════════════════════════════════
    #  القسم 2 — الإعدادات
    # ════════════════════════════════════════════════════════════════════
    with st.expander("⚙️ إعدادات الكشط", expanded=False):
        col_s1, col_s2, col_s3 = st.columns(3)

        with col_s1:
            concurrency = st.number_input(
                "طلبات متزامنة",
                min_value=1, max_value=8, value=3, step=1,
                key="sai_concurrency",
                help="قيمة أقل = أأمن، قيمة أعلى = أسرع لكن خطر حجب أعلى",
                disabled=st.session_state.sai_running,
            )
        with col_s2:
            delay = st.number_input(
                "تأخير بين الطلبات (ثانية)",
                min_value=0.0, max_value=10.0, value=1.0, step=0.5,
                key="sai_delay",
                help="تأخير بين كل طلب لتفادي الحظر",
                disabled=st.session_state.sai_running,
            )
        with col_s3:
            auto_save = st.checkbox(
                "حفظ تلقائي في DB بعد الكشط",
                value=True,
                key="sai_auto_save",
                help="يحفظ النتائج الناجحة في جدول competitor_products",
                disabled=st.session_state.sai_running,
            )

        # معلومات عن الطبقات
        st.markdown("""
        <div style="background:#0d1b2a;border:1px solid #1e3a5f;border-radius:8px;
                    padding:10px 14px;font-size:.8rem;color:#78909c;margin-top:8px">
            <b style="color:#4fc3f7">طبقات الكشط (بالترتيب):</b><br>
            <span class="sai-badge badge-crawl4ai" style="margin:2px">Crawl4AI</span>
            Playwright stealth — يتعامل مع JavaScript الكامل<br>
            <span class="sai-badge badge-curl_cffi" style="margin:2px">curl_cffi</span>
            محاكاة TLS/JA3 لـ Chrome 120 — يتجاوز Cloudflare<br>
            <span class="sai-badge badge-cloudscraper" style="margin:2px">cloudscraper</span>
            يحل JavaScript challenges<br>
            <span class="sai-badge badge-requests" style="margin:2px">requests</span>
            Fallback أخير مع User-Agent محاكٍ
        </div>
        """, unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════════════════
    #  القسم 3 — زر البدء + التقدم
    # ════════════════════════════════════════════════════════════════════
    st.markdown("---")

    btn_col, info_col = st.columns([2, 3])

    with btn_col:
        if not st.session_state.sai_running:
            btn_label    = "🚀 بدء الكشط الذكي"
            btn_disabled = not bool(all_urls)
            btn_help     = (
                "اضغط لبدء الكشط" if all_urls
                else "أدخل رابطاً واحداً على الأقل"
            )

            if st.button(
                btn_label,
                type="primary",
                disabled=btn_disabled,
                use_container_width=True,
                help=btn_help,
                key="sai_start_btn",
            ):
                # تنظيف الحالة السابقة
                st.session_state.sai_running    = True
                st.session_state.sai_done       = 0
                st.session_state.sai_total      = len(all_urls)
                st.session_state.sai_results    = []
                st.session_state.sai_status     = "running"
                st.session_state.sai_saved_to_db = False

                # تشغيل الكشط في daemon thread
                t = threading.Thread(
                    target=_run_scraping_job,
                    args=(
                        all_urls,
                        int(concurrency),
                        float(delay),
                        "sai",
                    ),
                    daemon=True,
                    name="smart-ai-scraper",
                )
                t.start()
                st.rerun()

        else:
            # زر إيقاف (يضع علامة فقط — الخيط ينهي دورته)
            if st.button(
                "⏹️ إيقاف",
                use_container_width=True,
                type="secondary",
                key="sai_stop_btn",
            ):
                st.session_state.sai_running = False
                st.session_state.sai_status  = "stopped"
                st.warning("⏹️ تم طلب الإيقاف — سيتوقف بعد اكتمال الطلبات الجارية")

    with info_col:
        if all_urls:
            domains_preview = list({_domain(u) for u in all_urls[:5]})
            st.markdown(
                f"**{len(all_urls)} رابط** من "
                f"{'، '.join(domains_preview[:3])}"
                + ("..." if len(domains_preview) > 3 else ""),
            )
        elif not st.session_state.sai_running:
            st.caption("👈 أدخل روابط المنتجات من فوق لتبدأ")

    # ── شريط التقدم ─────────────────────────────────────────────────────
    if st.session_state.sai_running or st.session_state.sai_status == "running":
        done  = int(st.session_state.sai_done)
        total = max(int(st.session_state.sai_total), 1)
        pct   = done / total

        st.progress(min(pct, 0.99), text=f"⏳ معالجة {done} / {total} رابط...")

        # KPIs حية
        partial = [r for r in st.session_state.sai_results if r.get("success")]
        failed  = [r for r in st.session_state.sai_results if not r.get("success")]

        k1, k2, k3, k4 = st.columns(4)
        k1.markdown(
            f'<div class="sai-kpi"><div class="num">{done}</div>'
            f'<div class="lbl">تم معالجتها</div></div>',
            unsafe_allow_html=True,
        )
        k2.markdown(
            f'<div class="sai-kpi"><div class="num" style="color:#00C853">{len(partial)}</div>'
            f'<div class="lbl">ناجحة</div></div>',
            unsafe_allow_html=True,
        )
        k3.markdown(
            f'<div class="sai-kpi"><div class="num" style="color:#FF1744">{len(failed)}</div>'
            f'<div class="lbl">فاشلة</div></div>',
            unsafe_allow_html=True,
        )
        k4.markdown(
            f'<div class="sai-kpi"><div class="num">{total - done}</div>'
            f'<div class="lbl">متبقية</div></div>',
            unsafe_allow_html=True,
        )

    # اكتمل الكشط
    if st.session_state.sai_status == "done":
        results_list = st.session_state.sai_results
        success_n = sum(1 for r in results_list if r.get("success"))
        fail_n    = len(results_list) - success_n
        st.success(
            f"✅ **اكتمل الكشط!** — {success_n} ناجح | {fail_n} فاشل "
            f"من أصل {len(results_list)} رابط"
        )

        # حفظ تلقائي في DB
        if st.session_state.get("sai_auto_save") and not st.session_state.sai_saved_to_db:
            try:
                from utils.gemini_stealth_scraper import save_results_to_db, ScrapeResult
                from dataclasses import fields

                sr_list = []
                for d in results_list:
                    sr = ScrapeResult(**{
                        k: d.get(k, "") for k in
                        ["url", "title", "price", "stock_status", "success",
                         "error", "scrape_method", "domain", "raw_price_float",
                         "competitor", "image_url"]
                    })
                    sr_list.append(sr)

                db_stats = save_results_to_db(sr_list)
                st.session_state.sai_saved_to_db = True
                st.info(
                    f"💾 حُفظ في قاعدة البيانات: "
                    f"{db_stats['inserted']} جديد | "
                    f"{db_stats['updated']} محدَّث | "
                    f"{db_stats['failed']} فشل"
                )
            except Exception as e:
                st.warning(f"⚠️ فشل الحفظ التلقائي: {e}")

    elif st.session_state.sai_status.startswith("error:"):
        st.error(f"❌ خطأ: {st.session_state.sai_status[6:]}")

    # ════════════════════════════════════════════════════════════════════
    #  القسم 4 — عرض النتائج
    # ════════════════════════════════════════════════════════════════════
    results_list = st.session_state.sai_results
    if not results_list:
        if not st.session_state.sai_running and st.session_state.sai_status == "idle":
            st.info("💡 أدخل روابط المنتجات أعلاه واضغط «بدء الكشط الذكي»")
        return

    st.markdown("---")
    st.markdown("### 📊 النتائج")

    # ── Tabs للعرض ───────────────────────────────────────────────────────
    res_tab1, res_tab2, res_tab3 = st.tabs(["📋 جدول النتائج", "🃏 بطاقات تفصيلية", "📤 تصدير وحفظ"])

    # ── الجدول ────────────────────────────────────────────────────────────
    with res_tab1:
        df_rows = []
        for r in results_list:
            stock_ar = {
                "in_stock":     "✅ متوفر",
                "out_of_stock": "❌ غير متوفر",
                "unknown":      "❓ غير محدد",
            }.get(r.get("stock_status", "unknown"), "❓")

            df_rows.append({
                "الحالة":       "✅" if r.get("success") else "❌",
                "اسم المنتج":  (r.get("title") or "")[:80],
                "السعر (ر.س)": r.get("raw_price_float", 0.0),
                "التوفر":       stock_ar,
                "طريقة الكشط": r.get("scrape_method", ""),
                "الدومين":     r.get("domain", ""),
                "الرابط":      r.get("url", ""),
            })

        if df_rows:
            df = pd.DataFrame(df_rows)
            st.dataframe(
                df,
                use_container_width=True,
                height=min(600, 100 + len(df) * 36),
                column_config={
                    "الرابط":      st.column_config.LinkColumn("الرابط"),
                    "السعر (ر.س)": st.column_config.NumberColumn(format="%.2f"),
                },
            )
            st.caption(f"إجمالي {len(df)} نتيجة — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # ── البطاقات ───────────────────────────────────────────────────────────
    with res_tab2:
        # فلتر
        show_filter = st.radio(
            "عرض:",
            ["الكل", "الناجحة فقط", "الفاشلة فقط"],
            horizontal=True,
            key="sai_card_filter",
        )

        filtered = results_list
        if show_filter == "الناجحة فقط":
            filtered = [r for r in results_list if r.get("success")]
        elif show_filter == "الفاشلة فقط":
            filtered = [r for r in results_list if not r.get("success")]

        # Pagination
        PAGE_SIZE = 20
        total_p   = len(filtered)
        n_pages   = max(1, (total_p + PAGE_SIZE - 1) // PAGE_SIZE)
        page_n    = st.number_input("صفحة", 1, n_pages, 1, key="sai_card_pg") if n_pages > 1 else 1
        page_data = filtered[(page_n - 1) * PAGE_SIZE: page_n * PAGE_SIZE]

        for r in page_data:
            success       = r.get("success", False)
            title         = r.get("title") or "—"
            price         = r.get("price", "0")
            price_f       = r.get("raw_price_float", 0.0)
            stock         = r.get("stock_status", "unknown")
            method        = r.get("scrape_method", "none")
            domain        = r.get("domain", "")
            url           = r.get("url", "")
            error         = r.get("error", "")
            card_cls      = "success" if success else "failed"
            badge_html    = _method_badge(method)
            stock_html_v  = _stock_html(stock)
            price_display = f"<b style='color:#ff9800;font-size:1.1rem'>{price_f:,.2f} ر.س</b>"

            if success:
                body = f"""
                <div class="sai-result-card {card_cls}">
                    <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px">
                        <div style="flex:1;min-width:0">
                            <div style="font-weight:700;font-size:.92rem;
                                        color:#e0e0e0;margin-bottom:4px">{title[:100]}</div>
                            <div style="display:flex;gap:16px;flex-wrap:wrap;font-size:.82rem">
                                <span>{price_display}</span>
                                <span>{stock_html_v}</span>
                                <span style="color:#9e9e9e">{domain}</span>
                            </div>
                        </div>
                        <div style="text-align:left;flex-shrink:0">
                            {badge_html}
                        </div>
                    </div>
                    <div style="margin-top:6px">
                        <a href="{url}" target="_blank" rel="noopener noreferrer"
                           style="color:#4fc3f7;font-size:.72rem;text-decoration:none">
                            🔗 {url[:70]}{'...' if len(url)>70 else ''}
                        </a>
                    </div>
                </div>
                """
            else:
                body = f"""
                <div class="sai-result-card {card_cls}">
                    <div style="display:flex;justify-content:space-between;align-items:center">
                        <div>
                            <div style="font-size:.82rem;color:#ef9a9a;margin-bottom:2px">
                                ❌ فشل: {error[:80]}
                            </div>
                            <div style="font-size:.72rem;color:#555">{url[:80]}</div>
                        </div>
                        {badge_html}
                    </div>
                </div>
                """
            st.markdown(body, unsafe_allow_html=True)

        if not filtered:
            st.info("لا توجد نتائج تطابق الفلتر المحدد")

    # ── التصدير والحفظ ────────────────────────────────────────────────────
    with res_tab3:
        st.markdown("#### 💾 حفظ في قاعدة البيانات")

        if st.session_state.sai_saved_to_db:
            st.success("✅ تم الحفظ تلقائياً في قاعدة البيانات")
        else:
            if st.button(
                "💾 حفظ النتائج الناجحة في DB الآن",
                type="primary",
                use_container_width=True,
                key="sai_save_db_btn",
                disabled=st.session_state.sai_running,
            ):
                try:
                    from utils.gemini_stealth_scraper import save_results_to_db, ScrapeResult

                    sr_list = []
                    for d in results_list:
                        if not d.get("success"):
                            continue
                        sr = ScrapeResult(
                            url=d.get("url", ""),
                            title=d.get("title", ""),
                            price=d.get("price", "0"),
                            stock_status=d.get("stock_status", "unknown"),
                            success=d.get("success", False),
                            error=d.get("error", ""),
                            scrape_method=d.get("scrape_method", ""),
                            domain=d.get("domain", ""),
                            raw_price_float=d.get("raw_price_float", 0.0),
                            competitor=d.get("competitor", ""),
                            image_url=d.get("image_url", ""),
                        )
                        sr_list.append(sr)

                    db_stats = save_results_to_db(sr_list)
                    st.session_state.sai_saved_to_db = True
                    st.success(
                        f"✅ تم الحفظ: {db_stats['inserted']} جديد | "
                        f"{db_stats['updated']} محدَّث | {db_stats['failed']} فشل"
                    )
                except Exception as e:
                    st.error(f"❌ فشل الحفظ: {e}")

        st.markdown("---")
        st.markdown("#### 📥 تصدير النتائج")

        # بناء DataFrame للتصدير
        export_rows = []
        for r in results_list:
            export_rows.append({
                "url":          r.get("url", ""),
                "title":        r.get("title", ""),
                "price":        r.get("raw_price_float", 0.0),
                "stock_status": r.get("stock_status", "unknown"),
                "success":      r.get("success", False),
                "scrape_method":r.get("scrape_method", ""),
                "domain":       r.get("domain", ""),
                "error":        r.get("error", ""),
                "scraped_at":   datetime.now().strftime("%Y-%m-%d %H:%M"),
            })

        exp_df = pd.DataFrame(export_rows)

        col_e1, col_e2 = st.columns(2)
        with col_e1:
            csv_bytes = exp_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button(
                "📄 تنزيل CSV",
                data=csv_bytes,
                file_name=f"smart_scraper_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                use_container_width=True,
                key="sai_dl_csv",
            )

        with col_e2:
            # Excel
            try:
                import io
                buf = io.BytesIO()
                exp_df.to_excel(buf, index=False, engine="openpyxl")
                buf.seek(0)
                st.download_button(
                    "📊 تنزيل Excel",
                    data=buf.read(),
                    file_name=f"smart_scraper_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    key="sai_dl_xlsx",
                )
            except Exception:
                st.caption("تثبيت openpyxl لتصدير Excel")

        # إحصاءات
        st.markdown("---")
        st.markdown("#### 📈 إحصاءات الكشط")

        success_list = [r for r in results_list if r.get("success")]
        if success_list:
            method_counts: Dict[str, int] = {}
            for r in results_list:
                m = r.get("scrape_method", "none")
                method_counts[m] = method_counts.get(m, 0) + 1

            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("✅ نجاح", len(success_list))
            mc2.metric("❌ فشل", len(results_list) - len(success_list))
            avg_price = sum(r.get("raw_price_float", 0) for r in success_list) / len(success_list)
            mc3.metric("💰 متوسط السعر", f"{avg_price:,.2f} ر.س")

            st.markdown("**توزيع طرق الكشط:**")
            for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
                pct = count / len(results_list) * 100
                st.markdown(
                    f"{_method_badge(method)} — {count} ({pct:.0f}%)",
                    unsafe_allow_html=True,
                )

        # زر إعادة الضبط
        st.markdown("---")
        if st.button(
            "🔄 مسح النتائج وبدء جلسة جديدة",
            use_container_width=True,
            key="sai_reset_btn",
            disabled=st.session_state.sai_running,
        ):
            st.session_state.sai_running     = False
            st.session_state.sai_done        = 0
            st.session_state.sai_total       = 0
            st.session_state.sai_results     = []
            st.session_state.sai_status      = "idle"
            st.session_state.sai_saved_to_db = False
            st.rerun()
