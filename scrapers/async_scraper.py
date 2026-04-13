"""
scrapers/async_scraper.py
────────────────────────
ملف تشغيل رفيع متوافق مع Railway.

المشكلة الأصلية كانت أن Railway يشغّل هذا الملف مباشرة عبر المسار:
/app/scrapers/async_scraper.py
وعندها يصبح sys.path موجهاً إلى مجلد scrapers فقط، فلا يجد الحزمة الشقيقة
engines. كذلك كان الملف القديم يكتفي بعملية import ولا يستدعي main().

هذه النسخة:
1) تضيف جذر المشروع إلى sys.path.
2) تعيد تصدير كل ما في engines.async_scraper للحفاظ على التوافق.
3) تستدعي main() عند التشغيل المباشر.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from engines.async_scraper import *  # noqa: F401,F403
from engines.async_scraper import main as engine_main


if __name__ == "__main__":
    engine_main()
