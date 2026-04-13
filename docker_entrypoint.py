#!/usr/bin/env python3
"""
تشغيل Streamlit على Railway.
Railway قد يضع STREAMLIT_SERVER_PORT على النص الحرفي '$PORT' — نزيله ثم نمرّر المنفذ رقماً.

قبل تشغيل Streamlit يُعيد هذا الملف بناء مجلد /data من متغيرات البيئة.
هذا يُتيح إرسال ملفات البيانات لـ Railway دون رفعها لـ GitHub.

═══════════════════════════════════════════════════════════════
 طريقة 1: Base64 (للملفات < 64 KB)
═══════════════════════════════════════════════════════════════
  CATEGORIES_CSV_B64   → محتوى categories.csv   مُشفَّر Base64
  OUR_CATALOG_CSV_B64  → محتوى our_catalog.csv  مُشفَّر Base64
  COMPETITORS_JSON_B64 → محتوى competitors_list.json مُشفَّر Base64

  توليد القيمة محلياً:
    PowerShell: [Convert]::ToBase64String([IO.File]::ReadAllBytes("data\\categories.csv"))
    Linux/Mac:  base64 -w0 data/categories.csv

═══════════════════════════════════════════════════════════════
 طريقة 2: URL عام مؤقت (للملفات الكبيرة مثل brands.csv ≈ 400KB)
═══════════════════════════════════════════════════════════════
  BRANDS_CSV_URL → رابط مباشر لملف brands.csv (Google Drive / Dropbox / S3)
  SALLA_BRANDS_URL     → رابط "ماركات مهووس.csv"
  SALLA_CATEGORIES_URL → رابط "تصنيفات مهووس.csv"

  كيف تحصل على الرابط؟
    - Google Drive: شارك الملف (Anyone with link) ← انسخ الـ file_id
      ثم الرابط: https://drive.google.com/uc?export=download&id=<file_id>
    - Dropbox:     شارك الملف، غيّر ?dl=0 إلى ?dl=1 في نهاية الرابط

═══════════════════════════════════════════════════════════════
 طريقة 3: Railway Volume (الأفضل للملفات الثابتة الكبيرة)
═══════════════════════════════════════════════════════════════
  أنشئ Volume في Railway ← Mount path = /data
  ارفع الملفات مرة واحدة عبر: railway run -- bash
  ثم: cp /local/brands.csv /data/brands.csv
"""
import os
import base64


# ── Base64: مناسب لـ < 64KB ────────────────────────────────────────────────
_B64_FILES = {
    "CATEGORIES_CSV_B64":    "categories.csv",
    "OUR_CATALOG_CSV_B64":   "our_catalog.csv",
    "COMPETITORS_JSON_B64":  "competitors_list.json",
    "SALLA_BRANDS_B64":      "ماركات مهووس.csv",
    "SALLA_CATEGORIES_B64":  "تصنيفات مهووس.csv",
}

# ── URL: مناسب للملفات الكبيرة (brands.csv ≈ 400KB) ─────────────────────────
_URL_FILES = {
    "BRANDS_CSV_URL":        "brands.csv",
    "SALLA_BRANDS_URL":      "ماركات مهووس.csv",
    "SALLA_CATEGORIES_URL":  "تصنيفات مهووس.csv",
    "OUR_CATALOG_CSV_URL":   "our_catalog.csv",
    "COMPETITORS_JSON_URL":  "competitors_list.json",
}


def _restore_data_files() -> None:
    """
    يستعيد الملفات من متغيرات البيئة (Base64 أو URL) إلى DATA_DIR.
    آمن: يتخطى الملفات الموجودة مسبقاً (لا يُعيد الكتابة).
    """
    import urllib.request

    data_dir = (os.environ.get("DATA_DIR") or "/data").strip()
    os.makedirs(data_dir, exist_ok=True)

    # ── Base64 ────────────────────────────────────────────────────────────
    for env_key, filename in _B64_FILES.items():
        b64_val = (os.environ.get(env_key) or "").strip()
        if not b64_val:
            continue
        dest = os.path.join(data_dir, filename)
        if os.path.exists(dest):
            print(f"[entrypoint] ℹ️ موجود (تخطي): {filename}")
            continue
        try:
            with open(dest, "wb") as fh:
                fh.write(base64.b64decode(b64_val))
            sz = os.path.getsize(dest)
            print(f"[entrypoint] ✅ Base64 → {filename} ({sz:,} bytes)")
        except Exception as e:
            print(f"[entrypoint] ❌ فشل Base64 {env_key}: {e}")

    # ── URL ───────────────────────────────────────────────────────────────
    for env_key, filename in _URL_FILES.items():
        url = (os.environ.get(env_key) or "").strip()
        if not url:
            continue
        dest = os.path.join(data_dir, filename)
        if os.path.exists(dest):
            print(f"[entrypoint] ℹ️ موجود (تخطي): {filename}")
            continue
        try:
            print(f"[entrypoint] ⬇️ تحميل {filename} من URL...")
            urllib.request.urlretrieve(url, dest)
            sz = os.path.getsize(dest)
            print(f"[entrypoint] ✅ URL → {filename} ({sz:,} bytes)")
        except Exception as e:
            print(f"[entrypoint] ❌ فشل تحميل {env_key}: {e}")


def _port() -> int:
    raw = (os.environ.get("PORT") or "").strip() or "8501"
    try:
        p = int(raw)
        if 1 <= p <= 65535:
            return p
    except ValueError:
        pass
    return 8501


def _strip_broken_streamlit_server_env() -> None:
    for key in list(os.environ):
        if key.startswith("STREAMLIT_SERVER_"):
            os.environ.pop(key, None)


def main() -> None:
    # ── خطوة 1: استعادة ملفات /data ─────────────────────────────────────
    _restore_data_files()

    # ── خطوة 2: تشغيل Streamlit ─────────────────────────────────────────
    p = _port()
    _strip_broken_streamlit_server_env()
    os.execvp(
        "streamlit",
        [
            "streamlit", "run", "app.py",
            "--server.port", str(p),
            "--server.address", "0.0.0.0",
            "--server.headless", "true",
        ],
    )


if __name__ == "__main__":
    main()
