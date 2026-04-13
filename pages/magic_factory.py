"""
pages/magic_factory.py — مصنع المنتجات (✨ Magic Factory)
══════════════════════════════════════════════════════════
كشط رابط منتج منافس → تحسين بالذكاء الاصطناعي → معاينة وتعديل → تصدير Excel (.xlsx) سلة شامل.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from engines.ai_engine import enhance_competitor_product_for_salla
from styles import get_styles
from utils.competitor_product_scraper import (
    extract_meta_bundle,
    extract_product_from_html,
    fetch_product_page_html,
    looks_like_bot_challenge,
)
from utils.data_sanitizer import sanitize_description_terms
from utils.salla_shamel_export import export_to_salla_shamel

# ══════════════════════════════════════════════════════════════════════════════
#  مفاتيح الجلسة
# ══════════════════════════════════════════════════════════════════════════════
_SS_READY = "mf_ready_bundle"
_SS_WARN = "mf_last_warning"


def _init_session() -> None:
    if _SS_READY not in st.session_state:
        st.session_state[_SS_READY] = None
    if _SS_WARN not in st.session_state:
        st.session_state[_SS_WARN] = ""


def _build_scraped_summary(raw: Dict[str, Any]) -> str:
    """يحوّل ناتج الكشط إلى نص واحد للـ AI."""
    imgs = raw.get("images") or []
    img_txt = ", ".join(str(u) for u in imgs[:15])
    lines = [
        f"العنوان: {raw.get('title', '')}",
        f"الماركة: {raw.get('brand', '')}",
        f"السعر الرقمي: {raw.get('price', '')}",
        f"SKU: {raw.get('sku', '')}",
        f"الباركود/GTIN: {raw.get('barcode', '')}",
        f"النطاق: {raw.get('domain', '')}",
        "",
        "الوصف الخام (نص):",
        str(raw.get("description", ""))[:6000],
        "",
        f"روابط الصور ({len(imgs)}):",
        img_txt,
    ]
    return "\n".join(lines)


def _merge_raw_and_ai(
    raw: Dict[str, Any],
    ai: Dict[str, Any],
    url: str,
) -> Dict[str, Any]:
    """يدمج الكشط والتحسين في حزمة جاهزة للمعاينة والتصدير."""
    title = (ai.get("cleaned_title") or "").strip() or str(raw.get("title") or "").strip()
    price = raw.get("price")
    try:
        price_f = float(price) if price is not None else 0.0
    except (TypeError, ValueError):
        price_f = 0.0

    brand = (ai.get("brand") or "").strip() or str(raw.get("brand") or "").strip()
    sku = str(raw.get("sku") or "").strip()
    barcode = str(raw.get("barcode") or "").strip()
    images: List[str] = list(raw.get("images") or [])

    desc_html = (ai.get("description_html") or "").strip()
    if not desc_html:
        plain = str(raw.get("description") or "").strip()
        if plain:
            desc_html = "<p>" + plain.replace("\n\n", "</p><p>").replace("\n", "<br/>") + "</p>"

    desc_html = sanitize_description_terms(desc_html)

    return {
        "source_url": url,
        "product_name": title,
        "price": price_f,
        "brand": brand,
        "category": (ai.get("category") or "").strip(),
        "description_html": desc_html,
        "seo_title": (ai.get("seo_title") or "").strip(),
        "seo_description": (ai.get("seo_description") or "").strip(),
        "top_notes": (ai.get("top_notes") or "").strip(),
        "heart_notes": (ai.get("heart_notes") or "").strip(),
        "base_notes": (ai.get("base_notes") or "").strip(),
        "gender_hint": (ai.get("gender_hint") or "").strip(),
        "is_perfume": bool(ai.get("is_perfume")),
        "sku": sku,
        "barcode": barcode,
        "images": images,
    }


def _bundle_to_export_row(b: Dict[str, Any]) -> Dict[str, Any]:
    """صف واحد متوافق مع export_to_salla_shamel مع توحيد مسميات الأعمدة."""
    imgs = b.get("images") or []
    img_str = ",".join(str(u).strip() for u in imgs if str(u).strip())

    gender = (b.get("gender_hint") or "").strip()
    if not gender:
        gender = _infer_gender_from_text(
            f"{b.get('product_name','')} {b.get('description_html','')}"
        )

    # توحيد مسميات الأعمدة لتتوافق مع دالة التصدير
    return {
        "المنتج": b.get("product_name", ""),
        "الماركة": b.get("brand", ""),
        "سعر المنتج": float(b.get("price") or 0),
        "صورة_المنافس": img_str,
        "image_url": img_str,
        "وصف_AI": b.get("description_html", ""),
        "تصنيف المنتج": b.get("category", ""),
        "التصنيف_الرسمي": b.get("category", ""),
        "رمز المنتج sku": b.get("sku", ""),
        "الباركود": b.get("barcode", ""),
        "العنوان الترويجي": b.get("seo_title", ""),
        "وصف SEO": b.get("seo_description", ""),
        "الجنس": gender,
        "الافتتاحية": b.get("top_notes", ""),
        "القلب": b.get("heart_notes", ""),
        "القاعدة": b.get("base_notes", ""),
        "is_perfume": b.get("is_perfume", True)
    }


def _infer_gender_from_text(text: str) -> str:
    t = (text or "").lower()
    if any(x in t for x in ("نسائي", "نساء", "للنساء", "women", "female", "pour femme")):
        return "للنساء"
    if any(x in t for x in ("رجالي", "رجال", "للرجال", "men", "homme", "pour homme")):
        return "للرجال"
    if "unisex" in t or "للجنسين" in t:
        return "للجنسين"
    return ""


def _progress_bar(slot, value: float, text: str) -> None:
    with slot.container():
        st.progress(min(1.0, max(0.0, value)))
        st.caption(text)


# ══════════════════════════════════════════════════════════════════════════════
#  واجهة مدمجة من app.py
# ══════════════════════════════════════════════════════════════════════════════

def show() -> None:
    _init_session()
    st.title("✨ مصنع المنتجات")
    st.caption(
        "أدخل رابط منتج من أي متجر منافس — نكشط البيانات، نحسّنها بالذكاء الاصطناعي، "
        "ثم نصدّر ملف Excel (.xlsx) جاهز لاستيراد سلة الشامل."
    )

    progress_ph = st.empty()
    warn_ph = st.empty()

    url = st.text_input(
        "رابط المنتج",
        placeholder="https://example.com/product/...",
        key="mf_product_url",
    )

    col_a, col_b = st.columns([1, 1])
    with col_a:
        run = st.button("🚀 استخراج وتحسين", type="primary", use_container_width=True)
    with col_b:
        clear = st.button("🗑️ مسح النتيجة", use_container_width=True)

    if clear:
        st.session_state[_SS_READY] = None
        st.session_state[_SS_WARN] = ""
        st.rerun()

    if run:
        st.session_state[_SS_WARN] = ""
        target = (url or "").strip()
        if not target.startswith("http"):
            st.error("يرجى إدخال رابط يبدأ بـ http أو https.")
        else:
            try:
                with st.spinner("⏳ جاري جلب الصفحة (curl_cffi / cloudscraper / requests)…"):
                    _progress_bar(progress_ph, 0.08, "جاري الكشط…")
                    html_text, fetch_err = fetch_product_page_html(target)

                meta_only = ""
                if fetch_err == "cloudflare_or_challenge" and html_text:
                    st.session_state[_SS_WARN] = (
                        "⚠️ الصفحة تبدو محمية (مثل Cloudflare). جرّبنا استخراج ما تيسّر من وسوم meta و JSON-LD."
                    )
                elif fetch_err and not html_text:
                    st.error(fetch_err)
                    html_text = None

                if not html_text:
                    _progress_bar(progress_ph, 0.0, "")
                else:
                    with st.spinner("⏳ تحليل HTML واستخراج الحقول…"):
                        _progress_bar(progress_ph, 0.35, "تجهيز البيانات الخام…")
                        raw = extract_product_from_html(html_text, target)

                    if fetch_err == "cloudflare_or_challenge" or looks_like_bot_challenge(html_text):
                        mb = extract_meta_bundle(html_text, target)
                        meta_only = json.dumps(mb, ensure_ascii=False, indent=0)[:4000]

                    summary = _build_scraped_summary(raw)
                    with st.spinner("🤖 جاري التحسين بالذكاء الاصطناعي (عنوان، وصف HTML، SEO، تصنيف)…"):
                        _progress_bar(progress_ph, 0.55, "جاري تحليل AI…")
                        ai = enhance_competitor_product_for_salla(
                            scraped_summary=summary,
                            url=target,
                            meta_fallback=meta_only,
                        )
                        _progress_bar(progress_ph, 0.85, "دمج النتائج…")

                    bundle = _merge_raw_and_ai(raw, ai, target)
                    st.session_state[_SS_READY] = bundle
                    _progress_bar(progress_ph, 1.0, "اكتمل.")
                    st.success("تم تجهيز المنتج — راجع المعاينة ثم حمّل ملف سلة.")

            except Exception as exc:
                _progress_bar(progress_ph, 0.0, "")
                st.error(f"حدث خطأ غير متوقع: {exc}")
                import traceback

                with st.expander("تفاصيل تقنية"):
                    st.code(traceback.format_exc())

    if st.session_state.get(_SS_WARN):
        warn_ph.warning(st.session_state[_SS_WARN])

    bundle: Optional[Dict[str, Any]] = st.session_state.get(_SS_READY)
    if not bundle:
        st.info(
            "بعد التشغيل ستظهر هنا معاينة الصور والحقول القابلة للتعديل، ثم زر تحميل ملف Excel سلة الشامل."
        )
        return

    st.divider()
    st.subheader("👁️ معاينة وتعديل")

    imgs: List[str] = list(bundle.get("images") or [])
    if imgs:
        st.markdown("**صور المنتج المكتشفة**")
        n = min(len(imgs), 8)
        cols = st.columns(min(n, 4))
        for i in range(n):
            with cols[i % 4]:
                try:
                    st.image(imgs[i], use_container_width=True)
                except Exception:
                    st.caption(imgs[i][:80] + "…")

    with st.form("mf_edit_form"):
        c1, c2 = st.columns(2)
        with c1:
            name = st.text_input("اسم المنتج", value=bundle.get("product_name", ""))
            price = st.number_input(
                "السعر (ر.س)",
                min_value=0.0,
                value=float(bundle.get("price") or 0),
                step=0.5,
            )
            brand = st.text_input("الماركة", value=bundle.get("brand", ""))
            category = st.text_input("التصنيف (مطابق لتصنيفات سلة لديك)", value=bundle.get("category", ""))
        with c2:
            sku = st.text_input("SKU / رمز المنتج", value=bundle.get("sku", ""))
            barcode = st.text_input("الباركود (إن وُجد)", value=bundle.get("barcode", ""))
            seo_title = st.text_input("عنوان SEO / ترويجي", value=bundle.get("seo_title", ""))
            seo_desc = st.text_area("وصف SEO", value=bundle.get("seo_description", ""), height=80)

        desc = st.text_area(
            "الوصف (HTML)",
            value=bundle.get("description_html", ""),
            height=320,
            help="يُفضّل إبقاء وسوم HTML البسيطة (p, h2, ul, li, strong) كما تدعمها سلة.",
        )

        notes_c1, notes_c2, notes_c3 = st.columns(3)
        with notes_c1:
            top_n = st.text_input("القمة (عطور)", value=bundle.get("top_notes", ""))
        with notes_c2:
            heart_n = st.text_input("القلب (عطور)", value=bundle.get("heart_notes", ""))
        with notes_c3:
            base_n = st.text_input("القاعدة (عطور)", value=bundle.get("base_notes", ""))

        img_lines = st.text_area(
            "روابط الصور (سطر لكل رابط أو مفصولة بفواصل)",
            value="\n".join(imgs) if imgs else "",
            height=100,
        )

        submitted = st.form_submit_button("💾 تطبيق التعديلات على المعاينة")

    if submitted:
        # تحليل الصور من النص
        raw_img = img_lines.replace("\n", ",")
        parts = [p.strip() for p in re.split(r"[,;\s]+", raw_img) if p.strip() and p.strip().startswith("http")]
        if not parts:
            parts = [ln.strip() for ln in img_lines.splitlines() if ln.strip().startswith("http")]
        bundle.update(
            {
                "product_name": name.strip(),
                "price": float(price),
                "brand": brand.strip(),
                "category": category.strip(),
                "sku": sku.strip(),
                "barcode": barcode.strip(),
                "seo_title": seo_title.strip(),
                "seo_description": seo_desc.strip(),
                "description_html": desc,
                "top_notes": top_n.strip(),
                "heart_notes": heart_n.strip(),
                "base_notes": base_n.strip(),
                "images": _uniq_keep_order(parts),
            }
        )
        st.session_state[_SS_READY] = bundle
        st.toast("تم حفظ التعديلات.", icon="✅")
        st.rerun()

    with st.expander("وصف SEO (للنسخ اليدوي إلى لوحة سلة إن لزم)"):
        st.write(bundle.get("seo_description", ""))

    st.divider()
    st.subheader("📥 تصدير سلة الشامل")

    row = _bundle_to_export_row(bundle)
    df = pd.DataFrame([row])
    xlsx_bytes = export_to_salla_shamel(df, generate_descriptions=False)

    st.download_button(
        label="📥 تحميل ملف سلة (Excel)",
        data=xlsx_bytes,
        file_name="salla_shamel_magic_factory.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True,
    )

    st.caption(
        "تأكد من أن **الماركة** و**التصنيف** يطابقان أسماء الملفات الرسمية لسلة لديك لتفادي رفض الاستيراد."
    )


def _uniq_keep_order(urls: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for u in urls:
        u = u.strip()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  تشغيل مستقل (اختياري)
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    st.set_page_config(
        page_title="مصنع المنتجات | مهووس",
        page_icon="✨",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    st.markdown(get_styles(), unsafe_allow_html=True)
    show()
