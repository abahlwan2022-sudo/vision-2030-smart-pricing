"""
scrapers/sitemap_resolve.py — Shim (توافق رجعي)
══════════════════════════════════════════════════
المصدر الحقيقي: engines/sitemap_resolve.py
هذا الملف مجرد جسر للتوافق مع أي import قديم يستخدم scrapers.sitemap_resolve
"""
from engines.sitemap_resolve import *  # noqa: F401, F403
from engines.sitemap_resolve import (  # noqa: F401 — explicit للـ linters
    SitemapEntry,
    SitemapDiag,
    resolve_product_urls,
    resolve_product_entries,
    resolve_store_to_sitemap_url,
    _fallback_shopify_api,
    _fallback_html_product_page,
)
