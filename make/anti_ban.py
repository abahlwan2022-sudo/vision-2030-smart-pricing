"""
make/anti_ban.py — Shim (توافق رجعي)
══════════════════════════════════════
المصدر الحقيقي: scrapers/anti_ban.py
هذا الملف مجرد جسر للتوافق مع أي import قديم يستخدم make.anti_ban
"""
# noinspection PyUnresolvedReferences
from scrapers.anti_ban import *  # noqa: F401, F403
