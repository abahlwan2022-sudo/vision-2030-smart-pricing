"""
make/sitemap_resolve.py — Shim (توافق رجعي)
════════════════════════════════════════════
المصدر الحقيقي: engines/sitemap_resolve.py
هذا الملف مجرد جسر للتوافق مع أي import قديم يستخدم make.sitemap_resolve
"""
from engines.sitemap_resolve import *  # noqa: F401, F403
from engines.sitemap_resolve import (  # noqa: F401
    SitemapEntry,
    SitemapDiag,
    resolve_product_urls,
    resolve_product_entries,
    resolve_store_to_sitemap_url,
)
