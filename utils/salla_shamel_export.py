"""
utils/salla_shamel_export.py — v3.0 (مطابق لقالب سلة الرسمي)
══════════════════════════════════════════════════════════════
▸ القالب مُستخرج من ملف منتج_جديد.csv الرسمي لمنصة سلة
▸ أول صف: بيانات المنتج (meta-header) — إلزامي في سلة
▸ 40 عموداً بالترتيب الحرفي المطابق لـ سلة
▸ التصدير: CSV (مطابق للقالب) + XLSX — كلاهما عبر io.BytesIO
▸ وصف HTML من 7 أقسام مطابق لقالب سلة الرسمي
▸ تحقق مزدوج من المنتجات المفقودة قبل التصدير
"""
from __future__ import annotations

import csv
import difflib
import html as _html_lib
import io
import re
import unicodedata
from typing import Any, Optional

import pandas as pd

# ── 40 عمود بالترتيب الحرفي المطابق لـ سلة ──────────────────────────────
SALLA_SHAMEL_COLUMNS: list[str] = [
    "النوع ",
    "أسم المنتج",
    "تصنيف المنتج",
    "صورة المنتج",
    "وصف صورة المنتج",
    "نوع المنتج",
    "سعر المنتج",
    "الوصف",
    "هل يتطلب شحن؟",
    "رمز المنتج sku",
    "سعر التكلفة",
    "السعر المخفض",
    "تاريخ بداية التخفيض",
    "تاريخ نهاية التخفيض",
    "اقصي كمية لكل عميل",
    "إخفاء خيار تحديد الكمية",
    "اضافة صورة عند الطلب",
    "الوزن",
    "وحدة الوزن",
    "الماركة",
    "العنوان الترويجي",
    "تثبيت المنتج",
    "الباركود",
    "السعرات الحرارية",
    "MPN",
    "GTIN",
    "خاضع للضريبة ؟",
    "سبب عدم الخضوع للضريبة",
    "[1] الاسم",
    "[1] النوع",
    "[1] القيمة",
    "[1] الصورة / اللون",
    "[2] الاسم",
    "[2] النوع",
    "[2] القيمة",
    "[2] الصورة / اللون",
    "[3] الاسم",
    "[3] النوع",
    "[3] القيمة",
    "[3] الصورة / اللون",
]

# صف الـ meta-header المطلوب في قالب سلة CSV
_SALLA_META_HEADER = "بيانات المنتج" + "," * (len(SALLA_SHAMEL_COLUMNS) - 1)

# ── الفئات المعيارية في سلة ──────────────────────────────────────────────
_GENDER_CATEGORY = {
    "للرجال":   "العطور > عطور رجالية",
    "رجالي":    "العطور > عطور رجالية",
    "للنساء":   "العطور > عطور نسائية",
    "نسائي":    "العطور > عطور نسائية",
    "للجنسين":  "العطور > عطور للجنسين",
    "unisex":   "العطور > عطور للجنسين",
}
_DEFAULT_CATEGORY = "العطور > عطور للجنسين"


def _norm_text(s: str) -> str:
    """تطبيع النص للمقارنة — حروف صغيرة + إزالة تشكيل + مسافات"""
    t = unicodedata.normalize("NFKC", str(s or ""))
    t = re.sub(r"[\u064B-\u065F\u0670]", "", t)  # إزالة الحركات
    t = re.sub(r"[أإآا]", "ا", t)
    t = re.sub(r"[ةه]", "ه", t)
    t = re.sub(r"[يى]", "ي", t)
    return re.sub(r"\s+", " ", t).strip().lower()


def _safe_str(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "<na>") else s


def _slug_brand(brand: str) -> str:
    """تحويل اسم الماركة لـ slug مناسب لـ URL سلة"""
    b = str(brand or "").strip().lower()
    b = re.sub(r"[^a-z0-9\u0600-\u06FF\s-]", "", b)
    b = re.sub(r"\s+", "-", b).strip("-")
    return b or "brand"


# ══════════════════════════════════════════════════════════════════════════
#  قالب HTML الشامل — مطابق 100% لقالب سلة الرسمي (7 أقسام)
# ══════════════════════════════════════════════════════════════════════════
def generate_salla_html_description(
    product_name: str,
    brand_name: str = "غير متوفر",
    gender: str = "للجنسين",
    size_ml: str = "100",
    concentration: str = "أو دو بارفيوم",
    fragrance_family: str = "غير متوفر",
    top_notes: str = "غير متوفر",
    heart_notes: str = "غير متوفر",
    base_notes: str = "غير متوفر",
    season: str = "جميع الفصول",
    occasions: str = "المناسبات الرسمية، السهرات، واللقاءات العملية",
    longevity: str = "8",
    sillage: str = "8",
    steadiness: str = "9",
) -> str:
    """
    قالب HTML من 7 أقسام — مطابق لملف منتج_جديد.csv من سلة.

    الأقسام:
      1. h2 + p رئيسي
      2. h3 تفاصيل المنتج (ul)
      3. h3 رحلة العطر — الهرم العطري (ul)
      4. h3 لماذا تختار (ul)
      5. h3 متى وأين ترتديه (p)
      6. h3 لمسة خبير (p)
      7. h3 الأسئلة الشائعة (ul) + h3 اكتشف أكثر (p) + p ختامي
    """
    pn  = _html_lib.escape(_safe_str(product_name) or "منتج", quote=False)
    br  = _html_lib.escape(_safe_str(brand_name)   or "غير متوفر", quote=False)
    gn  = _html_lib.escape(_safe_str(gender)        or "للجنسين", quote=False)
    sz  = _html_lib.escape(_safe_str(size_ml)       or "100", quote=False)
    cc  = _html_lib.escape(_safe_str(concentration) or "أو دو بارفيوم", quote=False)
    ff  = _html_lib.escape(_safe_str(fragrance_family) or "غير متوفر", quote=False)
    tn  = _html_lib.escape(_safe_str(top_notes)     or "غير متوفر", quote=False)
    hn  = _html_lib.escape(_safe_str(heart_notes)   or "غير متوفر", quote=False)
    bn  = _html_lib.escape(_safe_str(base_notes)    or "غير متوفر", quote=False)
    sea = _html_lib.escape(_safe_str(season)        or "جميع الفصول", quote=False)
    occ = _html_lib.escape(_safe_str(occasions)     or "المناسبات الرسمية والسهرات", quote=False)
    lng = _safe_str(longevity) or "8"
    sig = _safe_str(sillage) or "8"
    std = _safe_str(steadiness) or "9"

    brand_slug = _slug_brand(brand_name)
    brand_url  = f"https://mahwous.com/brands/{brand_slug}"
    bu  = _html_lib.escape(brand_url, quote=True)

    size_label = f"{sz} مل" if sz.isdigit() else sz

    return (
        # ── 1. عنوان رئيسي + مقدمة ──────────────────────────────────────
        f"<h2>{pn} {pn} {cc} {size_label} {gn}</h2>\n"
        f"<p>اكتشف سحر <strong>{pn}</strong> من "
        f'<strong><a href="{bu}" target="_blank" rel="noopener">{br}</a></strong>'
        f" — عطر {ff} فاخر يجمع بين الأصالة والتميز. "
        f"صمّم خصيصاً ل{gn} ليرسم بصمتك العطري بثقة وأناقة. "
        f"متوفّر بحجم {size_label} بتركيز <strong>{cc}</strong> لضمان ثبات استثنائي.</p>\n"

        # ── 2. تفاصيل المنتج ─────────────────────────────────────────────
        "<h3>تفاصيل المنتج</h3>\n<ul>\n"
        f'<li><strong>الماركة:</strong> <a href="{bu}" target="_blank" rel="noopener">{br}</a></li>\n'
        f"<li><strong>الاسم:</strong> {pn}</li>\n"
        f"<li><strong>الجنس:</strong> {gn}</li>\n"
        f"<li><strong>العائلة العطرية:</strong> {ff}</li>\n"
        f"<li><strong>الحجم:</strong> {size_label}</li>\n"
        f"<li><strong>التركيز:</strong> {cc}</li>\n"
        "<li><strong>نوع المنتج:</strong> عطر أصلي</li>\n"
        "</ul>\n"

        # ── 3. رحلة العطر — الهرم العطري ──────────────────────────────
        "<h3>رحلة العطر — الهرم العطري</h3>\n"
        f"<p>يأخذك <strong>{pn}</strong> في رحلة عطرية متكاملة تبدأ بطزالة وتنتهي بدفء وعمق.</p>\n"
        "<ul>\n"
        f"<li><strong>المقدمة (Top Notes):</strong> {tn}</li>\n"
        f"<li><strong>القلب (Heart Notes):</strong> {hn}</li>\n"
        f"<li><strong>القاعدة (Base Notes):</strong> {bn}</li>\n"
        "</ul>\n"

        # ── 4. لماذا تختار هذا العطر؟ ────────────────────────────────
        "<h3>لماذا تختار هذا العطر؟</h3>\n<ul>\n"
        f"<li><strong>الثبات والفوحان:</strong> تركيز {cc} يضمن فوحاناً يدوم طويلاً يلفت الأنظار.</li>\n"
        f"<li><strong>التميز والأصالة:</strong> من دار {br} العريقة بتراث عطري أصيل.</li>\n"
        "<li><strong>القيمة الاستثنائية:</strong> عطر فاخر بسعر مناسب من متجر مهووس الموثوق.</li>\n"
        "<li><strong>الجاذبية المضمونة:</strong> عطر يجعلك محور الاهتمام في كل مكان تحضره.</li>\n"
        "</ul>\n"

        # ── 5. متى وأين ترتديه؟ ──────────────────────────────────────
        "<h3>متى وأين ترتديه؟</h3>\n"
        f"<p>مثالي لـ {sea}. يلائم {occ}. "
        "ينصح برشه على نقاط النبض والرسغاوات لأفضل ثبات.</p>\n"

        # ── 6. لمسة خبير من مهووس ────────────────────────────────────
        "<h3>لمسة خبير من مهووس</h3>\n"
        f"<p>الفوحان: {sig}/10 | الثبات: {std}/10 | "
        "نصيحة: ابدأ بكمية صغيرة وابنِ تدريجياً حتى تجد كميتك المثالية.</p>\n"

        # ── 7. الأسئلة الشائعة ───────────────────────────────────────
        "<h3>الأسئلة الشائعة</h3>\n<ul>\n"
        f"<li><strong>كم يدوم العطر؟</strong> بين {lng}-12 ساعة حسب البشرة ودرجة الحرارة.</li>\n"
        "<li><strong>هل يناسب الاستخدام اليومي؟</strong> نعم، بكمية معتدلة للبيئات المختلفة.</li>\n"
        f"<li><strong>ما العائلة العطرية؟</strong> {ff}.</li>\n"
        "<li><strong>هل يناسب الطقس الحار في السعودية؟</strong> جميع الفصول هي الموسم المثالي له.</li>\n"
        f"<li><strong>ما مناسبات ارتداء هذا العطر؟</strong> {occ}.</li>\n"
        "</ul>\n"

        # ── اكتشف أكثر من مهووس ──────────────────────────────────────
        "<h3>اكتشف أكثر من مهووس</h3>\n"
        f'<p>اكتشف <a href="{bu}" target="_blank" rel="noopener">عطور {br}</a> | '
        '<a href="https://mahwous.com/categories/mens-perfumes" target="_blank" rel="noopener">عطور رجالية</a> | '
        '<a href="https://mahwous.com/categories/womens-perfumes" target="_blank" rel="noopener">عطور نسائية</a></p>\n'
        "<p><strong>عالمك العطري يبدأ من مهووس.</strong> أصلي 100% | شحن سريع داخل السعودية.</p>"
    )


def sanitize_salla_description_html(raw: str) -> str:
    """فلتر ثلاثي الطبقات — يزيل أي نص حواري AI قبل الـ HTML"""
    if not raw:
        return ""
    s = str(raw).strip()
    s = re.sub(r"(?is)^```(?:html|xml)?\s*", "", s).strip()
    s = re.sub(r"(?is)\s*```\s*$", "", s).strip()
    first = re.search(r"(?is)<\s*(?:h2|h3|div|p)\b", s)
    if first and first.start() > 0:
        prefix = s[: first.start()]
        if re.search(r"[a-zA-Z\u0600-\u06FF]", prefix):
            s = s[first.start():]
    m = re.search(r"(?is)<\s*(?:h2|h3|div|p)\b", s)
    return s[m.start():].strip() if m else ""


# ══════════════════════════════════════════════════════════════════════════
#  استخراج بيانات الصف
# ══════════════════════════════════════════════════════════════════════════
def _extract_product_name(row: dict) -> str:
    for k in ("أسم المنتج", "اسم المنتج", "منتج_المنافس", "المنتج", "cleaned_title", "name", "title", "الاسم"):
        v = _safe_str(row.get(k, ""))
        if v and not v.lower().startswith(("http://", "https://")):
            return v
    return ""


def _extract_brand(row: dict) -> str:
    for k in ("الماركة_الرسمية", "الماركة", "الماركة_الرسمي", "brand", "Brand"):
        v = _safe_str(row.get(k, ""))
        if v and v.lower() not in ("nan", "none", "unknown", "ماركة عالمية"):
            return v
    return "غير متوفر"


def _extract_gender(row: dict) -> str:
    for k in ("الجنس", "gender_hint", "Gender", "gender"):
        v = _safe_str(row.get(k, ""))
        if v:
            return v
    return "للجنسين"


def _extract_size(row: dict) -> str:
    for k in ("الحجم", "size", "Size"):
        v = _safe_str(row.get(k, ""))
        m = re.search(r"(\d+)\s*ml", v, re.I)
        if m:
            return m.group(1)
    return "100"


def _extract_price(row: dict) -> str:
    for k in ("سعر_المنافس", "سعر المنافس", "السعر", "سعر المنتج", "Price", "price"):
        v = _safe_str(row.get(k, ""))
        try:
            p = float(v.replace(",", ""))
            if p > 0:
                return str(round(p, 2))
        except (ValueError, TypeError):
            pass
    return ""


def _extract_image(row: dict) -> str:
    for k in ("صورة_المنافس", "صورة المنتج", "image_url", "صورة", "الصورة"):
        v = _safe_str(row.get(k, ""))
        if v and v.lower().startswith("http"):
            return v
    return ""


def _extract_category(row: dict, gender: str) -> str:
    for k in ("التصنيف_الرسمي", "تصنيف المنتج", "التصنيف", "category"):
        v = _safe_str(row.get(k, ""))
        if v:
            # إذا كانت فئة سلة جاهزة
            if ">" in v:
                return v
    # اشتق من الجنس
    return _GENDER_CATEGORY.get(gender, _DEFAULT_CATEGORY)


def _extract_notes(row: dict) -> tuple[str, str, str]:
    top   = _safe_str(row.get("top_notes", "")) or "غير متوفر"
    heart = _safe_str(row.get("heart_notes", "")) or "غير متوفر"
    base  = _safe_str(row.get("base_notes", "")) or "غير متوفر"
    return top, heart, base


# ══════════════════════════════════════════════════════════════════════════
#  التحقق من المنتجات المفقودة — هل هي موجودة فعلاً في الكتالوج؟
# ══════════════════════════════════════════════════════════════════════════
def verify_truly_missing(
    missing_df: pd.DataFrame,
    our_catalog_df: Optional[pd.DataFrame] = None,
    fuzzy_threshold: float = 85.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    يفحص كل منتج «مفقود» مقابل الكتالوج بطريقتين:
      1. تطابق نصي مباشر (بعد التطبيع)
      2. تطابق fuzzy ≥ fuzzy_threshold%

    يُعيد (truly_missing_df, found_in_catalog_df)
    """
    if missing_df is None or missing_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    if our_catalog_df is None or our_catalog_df.empty:
        return missing_df.copy(), pd.DataFrame()

    # بناء فهرس الكتالوج
    name_col = None
    for c in ("اسم المنتج", "المنتج", "أسم المنتج", "name", "Name", "title"):
        if c in our_catalog_df.columns:
            name_col = c
            break
    if not name_col:
        return missing_df.copy(), pd.DataFrame()

    catalog_names_raw = our_catalog_df[name_col].dropna().astype(str).tolist()
    catalog_norms = [_norm_text(n) for n in catalog_names_raw]

    truly_missing = []
    found_in_cat  = []

    try:
        from rapidfuzz import process as rf_proc, fuzz
        use_fuzzy = True
    except ImportError:
        use_fuzzy = False

    for _, row in missing_df.iterrows():
        pname = _extract_product_name(row.to_dict())
        if not pname:
            truly_missing.append(row)
            continue
        pnorm = _norm_text(pname)

        # 1. تطابق نصي مباشر
        if pnorm in catalog_norms:
            found_in_cat.append(row)
            continue

        # 2. fuzzy
        found = False
        if use_fuzzy and catalog_norms:
            best = rf_proc.extractOne(pnorm, catalog_norms, scorer=fuzz.token_set_ratio)
            if best and best[1] >= fuzzy_threshold:
                found = True

        if found:
            found_in_cat.append(row)
        else:
            truly_missing.append(row)

    return (
        pd.DataFrame(truly_missing).reset_index(drop=True) if truly_missing else pd.DataFrame(),
        pd.DataFrame(found_in_cat).reset_index(drop=True)  if found_in_cat  else pd.DataFrame(),
    )


# ══════════════════════════════════════════════════════════════════════════
#  بناء DataFrame سلة
# ══════════════════════════════════════════════════════════════════════════
def build_salla_shamel_dataframe(
    missing_df: pd.DataFrame,
    our_catalog_df: Optional[pd.DataFrame] = None,
    verify_missing: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    يبني DataFrame بـ 40 عمود من جدول المنتجات المفقودة.

    ▸ إذا verify_missing=True يتحقق مسبقاً من أن المنتج غير موجود في الكتالوج
    ▸ يُعيد (salla_df, found_in_catalog_df)
    """
    if missing_df is None or missing_df.empty:
        return pd.DataFrame(columns=SALLA_SHAMEL_COLUMNS), pd.DataFrame()

    truly_missing, found_in_cat = (
        verify_truly_missing(missing_df, our_catalog_df)
        if verify_missing and our_catalog_df is not None and not our_catalog_df.empty
        else (missing_df.copy(), pd.DataFrame())
    )

    if truly_missing.empty:
        return pd.DataFrame(columns=SALLA_SHAMEL_COLUMNS), found_in_cat

    rows: list[dict] = []
    for _, row in truly_missing.iterrows():
        r = row.to_dict()
        pname   = _extract_product_name(r)
        brand   = _extract_brand(r)
        gender  = _extract_gender(r)
        size    = _extract_size(r)
        price   = _extract_price(r)
        image   = _extract_image(r)
        cat     = _extract_category(r, gender)
        top_n, heart_n, base_n = _extract_notes(r)
        img_alt = f"زجاجة عطر {pname} {size} مل الأصلية" if pname else ""

        description = generate_salla_html_description(
            product_name=pname,
            brand_name=brand,
            gender=gender,
            size_ml=size,
            fragrance_family=_safe_str(r.get("العائلة_العطرية", "غير متوفر")),
            top_notes=top_n,
            heart_notes=heart_n,
            base_notes=base_n,
        )

        out: dict[str, Any] = {c: "" for c in SALLA_SHAMEL_COLUMNS}
        out["النوع "]                    = "منتج"
        out["أسم المنتج"]                = pname
        out["تصنيف المنتج"]              = cat
        out["صورة المنتج"]               = image
        out["وصف صورة المنتج"]          = img_alt
        out["نوع المنتج"]                = "منتج جاهز"
        out["سعر المنتج"]                = price
        out["الوصف"]                     = description
        out["هل يتطلب شحن؟"]            = "نعم"
        out["رمز المنتج sku"]            = ""
        out["سعر التكلفة"]               = ""
        out["السعر المخفض"]              = ""
        out["تاريخ بداية التخفيض"]       = ""
        out["تاريخ نهاية التخفيض"]       = ""
        out["اقصي كمية لكل عميل"]        = 0
        out["إخفاء خيار تحديد الكمية"]  = 0
        out["اضافة صورة عند الطلب"]     = 0
        out["الوزن"]                     = 0.2
        out["وحدة الوزن"]               = "kg"
        out["الماركة"]                   = brand if brand != "غير متوفر" else ""
        out["العنوان الترويجي"]          = ""
        out["تثبيت المنتج"]              = ""
        out["الباركود"]                  = ""
        out["السعرات الحرارية"]          = ""
        out["MPN"]                       = ""
        out["GTIN"]                      = ""
        out["خاضع للضريبة ؟"]           = "نعم"
        out["سبب عدم الخضوع للضريبة"]   = ""
        rows.append(out)

    df = pd.DataFrame(rows, columns=SALLA_SHAMEL_COLUMNS)
    assert len(df.columns) == 40, f"عدد الأعمدة {len(df.columns)} ≠ 40"
    return df, found_in_cat


# ══════════════════════════════════════════════════════════════════════════
#  التصدير — CSV مطابق لقالب سلة (مع صف بيانات المنتج)
# ══════════════════════════════════════════════════════════════════════════
def export_to_salla_shamel_csv(
    missing_df: pd.DataFrame,
    our_catalog_df: Optional[pd.DataFrame] = None,
    verify_missing: bool = True,
) -> tuple[bytes, int, pd.DataFrame]:
    """
    يصدّر إلى CSV مطابق لقالب سلة الرسمي (منتج_جديد.csv).

    ▸ السطر الأول: بيانات المنتج,...  (meta-header إلزامي في سلة)
    ▸ السطر الثاني: أسماء الأعمدة الـ 40
    ▸ السطر الثالث+: بيانات المنتجات
    ▸ الترميز: UTF-8 with BOM (مطلوب لعرض العربية في Excel)

    يُعيد (csv_bytes, عدد_المنتجات_المُصدَّرة, found_in_catalog_df)
    """
    salla_df, found_df = build_salla_shamel_dataframe(
        missing_df, our_catalog_df, verify_missing=verify_missing
    )

    buf = io.StringIO()
    # السطر الأول: meta-header
    buf.write(_SALLA_META_HEADER + "\n")
    # السطر الثاني+: بيانات
    salla_df.to_csv(buf, index=False, encoding="utf-8")
    csv_text = buf.getvalue()
    return csv_text.encode("utf-8-sig"), len(salla_df), found_df


# ══════════════════════════════════════════════════════════════════════════
#  التصدير — XLSX عبر io.BytesIO
# ══════════════════════════════════════════════════════════════════════════
def export_to_salla_shamel(
    missing_df: pd.DataFrame,
    our_catalog_df: Optional[pd.DataFrame] = None,
    generate_descriptions: bool = True,
    verify_missing: bool = True,
) -> bytes:
    """
    يصدّر إلى xlsx عبر io.BytesIO — بدون disk I/O.
    يُعيد bytes جاهزة لـ st.download_button.
    """
    _ = generate_descriptions
    salla_df, _ = build_salla_shamel_dataframe(
        missing_df, our_catalog_df, verify_missing=verify_missing
    )
    if not salla_df.empty:
        salla_df = salla_df.reindex(columns=SALLA_SHAMEL_COLUMNS)

    buf = io.BytesIO()
    try:
        salla_df.to_excel(buf, index=False, engine="openpyxl")
    except ImportError as e:
        raise ImportError("تثبيت openpyxl مطلوب: pip install openpyxl") from e
    buf.seek(0)
    return buf.read()


# ══════════════════════════════════════════════════════════════════════════
#  تراكم بيانات المنافسين — حفظ + دمج عبر الجلسات
# ══════════════════════════════════════════════════════════════════════════
def merge_competitor_uploads(
    existing_df: Optional[pd.DataFrame],
    new_df: pd.DataFrame,
    competitor_name: str = "",
) -> pd.DataFrame:
    """
    يدمج ملف منافس جديد مع البيانات المحفوظة بدون فقدان سجلات قديمة.

    ▸ التحقق من التكرار عبر: اسم المنتج المُطبَّع + المنافس
    ▸ عند تكرار نفس المنتج من نفس المنافس → يُحدَّث السعر فقط
    ▸ منتجات جديدة → تُضاف في نهاية القائمة
    """
    if new_df is None or new_df.empty:
        return existing_df if existing_df is not None else pd.DataFrame()

    # تحديد عمود الاسم في الملف الجديد
    name_col = None
    for c in ("المنتج", "اسم المنتج", "منتج_المنافس", "name", "Product"):
        if c in new_df.columns:
            name_col = c
            break

    price_col = None
    for c in ("سعر_المنافس", "سعر المنافس", "سعر المنتج", "السعر", "Price", "price"):
        if c in new_df.columns:
            price_col = c
            break

    if existing_df is None or existing_df.empty:
        result = new_df.copy()
        if competitor_name and "المنافس" not in result.columns:
            result["المنافس"] = competitor_name
        return result.reset_index(drop=True)

    result = existing_df.copy()
    if competitor_name and "المنافس" not in result.columns:
        result["المنافس"] = competitor_name

    existing_name_col = None
    for c in ("المنتج", "اسم المنتج", "منتج_المنافس", "name", "Product"):
        if c in result.columns:
            existing_name_col = c
            break

    if not name_col or not existing_name_col:
        # لا يمكن المطابقة — أضف كلها
        combined = pd.concat([result, new_df], ignore_index=True)
        return combined.reset_index(drop=True)

    # بناء فهرس النسخة الموجودة
    existing_index: dict[str, int] = {}
    for i, row in result.iterrows():
        key = _norm_text(str(row.get(existing_name_col, "") or ""))
        comp = _norm_text(str(row.get("المنافس", "") or ""))
        if key:
            existing_index[f"{comp}::{key}"] = int(i)  # type: ignore[arg-type]

    new_rows = []
    for _, row in new_df.iterrows():
        pname = _safe_str(row.get(name_col, ""))
        comp  = _safe_str(row.get("المنافس", competitor_name))
        key   = f"{_norm_text(comp)}::{_norm_text(pname)}"
        if key in existing_index:
            # تحديث السعر فقط
            if price_col:
                result.at[existing_index[key], price_col] = row.get(price_col, "")
        else:
            new_row = row.to_dict()
            if competitor_name and "المنافس" not in new_row:
                new_row["المنافس"] = competitor_name
            new_rows.append(new_row)

    if new_rows:
        result = pd.concat([result, pd.DataFrame(new_rows)], ignore_index=True)

    return result.reset_index(drop=True)


# ── دوال توافق رجعي ──────────────────────────────────────────────────────
def resolve_brand_for_shamel(brand_raw: str) -> str:
    return _safe_str(brand_raw)


def resolve_category_for_shamel(
    category_raw: str,
    gender_hint: str = "",
    product_name_fallback: str = "",
) -> str:
    if category_raw and ">" in category_raw:
        return category_raw
    return _GENDER_CATEGORY.get(gender_hint, _DEFAULT_CATEGORY)


def build_salla_shamel_description_html(
    product_name: str,
    brand_raw: str,
    *,
    resolved_brand: Optional[str] = None,
) -> str:
    brand = resolved_brand or brand_raw or "غير متوفر"
    return generate_salla_html_description(product_name=product_name, brand_name=brand)
