"""
engines/async_scraper.py — محرك الكشط الرئيسي v2.0 (MASTER)
═══════════════════════════════════════════════════════════════
✅ نقاط استئناف ذكية (Checkpointing) لكل منافس على حدة
✅ استئناف تلقائي من آخر نقطة توقف عند الانقطاع
✅ كشط مفرد لأي منافس بشكل مستقل (run_single_store)
✅ دعم كامل للـ CLI (--store / --resume / --reset-state)
✅ شيمات scrapers/ و utils/ و make/ تستورد منه تلقائياً
✅ (مُحدّث) حماية الذاكرة، تسريع الـ Regex، و Connection Pooling
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import aiohttp
import pandas as pd

# ─── قفل مزامنة مشترك لحماية ملفات الحالة من Race Conditions ────────────────
_STATE_WRITE_LOCK    = threading.Lock()
_PROGRESS_WRITE_LOCK = threading.Lock()
_LIVE_WRITE_LOCK     = threading.Lock()

# ─── إعداد السجل ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("AsyncScraper")

# ─── ثوابت مُترجمة مسبقاً (Precompiled Regex) لحماية الذاكرة ────────────────
import re as _re
import concurrent.futures as _futures

_RE_OG_TITLE    = _re.compile(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', _re.I)
_RE_OG_IMAGE    = _re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', _re.I)
_RE_OG_URL      = _re.compile(r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']+)["\']',   _re.I)
_RE_OG_PRICE    = _re.compile(r'<meta[^>]+property=["\']product:price:amount["\'][^>]+content=["\']([^"\']+)["\']', _re.I)
_RE_PRICE_SPAN  = _re.compile(r'class="[^"]*price[^"]*"[^>]*>\s*(?:<[^>]+>)?([\d,. ]+)', _re.I)
_RE_H1_PRODUCT  = _re.compile(r'<h1[^>]*>\s*([^<]{3,120}?)\s*</h1>', _re.S | _re.I)

# ─── Anti-ban imports مُسبقة على مستوى الـ Module ─────────────────────────
try:
    from scrapers.anti_ban import get_browser_headers as _get_browser_headers
    from scrapers.anti_ban import try_all_sync_fallbacks as _try_all_sync_fallbacks
    _ANTI_BAN_AVAILABLE = True
except ImportError:
    _ANTI_BAN_AVAILABLE = False
    logger.warning("⚠️ scrapers.anti_ban غير متاح — سيتم استخدام headers افتراضية")
    _get_browser_headers      = lambda url: {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    _try_all_sync_fallbacks   = lambda url, timeout=25: None

# ─── ThreadPoolExecutor مخصص للـ fallback المزامن ─────────────────────────
_FALLBACK_EXECUTOR = _futures.ThreadPoolExecutor(
    max_workers=16,
    thread_name_prefix="scraper_fallback",
)

# ─── مسارات البيانات ──────────────────────────────────────────────────────────
_DATA_DIR = os.environ.get("DATA_DIR", "data")
os.makedirs(_DATA_DIR, exist_ok=True)

COMPETITORS_FILE = os.path.join(_DATA_DIR, "competitors_list.json")
OUTPUT_CSV       = os.path.join(_DATA_DIR, "competitors_latest.csv")
PROGRESS_FILE    = os.path.join(_DATA_DIR, "scraper_progress.json")
LASTMOD_FILE     = os.path.join(_DATA_DIR, "scraper_lastmod.json")
STATE_FILE       = os.path.join(_DATA_DIR, "scraper_state.json")   # نقاط الاستئناف
PID_FILE         = os.path.join(_DATA_DIR, "scraper.pid")

CSV_COLS = [
    "store", "name", "price", "original_price",
    "sku", "url", "image", "brand", "category",
    "availability", "scraped_at",
]


# ══════════════════════════════════════════════════════════════════════════════
#  هياكل البيانات
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Progress:
    """تقدم الكشط الكلي — يُكتب دورياً إلى PROGRESS_FILE"""
    running: bool = False
    started_at: str = ""
    finished_at: str = ""
    last_updated: str = ""
    phase: str = "discovering"
    pid: int = 0
    stores_total: int = 0
    stores_done: int = 0
    urls_total: int = 0
    urls_processed: int = 0
    rows_in_csv: int = 0
    fetch_exceptions: int = 0
    success_rate_pct: float = 0.0
    current_store: str = ""
    store_urls_done: int = 0
    store_urls_total: int = 0
    last_error: str = ""
    stores_results: Dict[str, int] = field(default_factory=dict)
    stores_http_errors: Dict[str, dict] = field(default_factory=dict) # ✅ تمت الإضافة للحماية من الانهيار

    def save(self, path: str = PROGRESS_FILE) -> None:
        try:
            self.last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.pid = os.getpid()
            with _PROGRESS_WRITE_LOCK:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(asdict(self), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"تعذّر حفظ التقدم: {e}")

    @classmethod
    def load(cls, path: str = PROGRESS_FILE) -> "Progress":
        try:
            with open(path, encoding="utf-8") as f:
                return cls(**json.load(f))
        except Exception:
            return cls()


def _write_pid_file() -> None:
    try:
        with open(PID_FILE, "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
    except Exception as e:
        logger.warning(f"تعذّر حفظ PID: {e}")


def _cleanup_pid_file() -> None:
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception as e:
        logger.warning(f"تعذّر حذف PID file: {e}")


def _mark_progress_failed(message: str) -> None:
    try:
        progress = Progress.load()
        progress.running = False
        progress.phase = "failed"
        progress.finished_at = datetime.now().isoformat()
        progress.last_error = (message or "")[:300]
        progress.save()
    except Exception as e:
        logger.warning(f"تعذّر تحديث حالة الفشل: {e}")


@dataclass
class StoreCheckpoint:
    """نقطة استئناف خاصة بمتجر واحد"""
    store_url: str
    domain: str
    status: str = "pending"       # pending | running | done | error
    last_page: int = 0            # رقم الصفحة الأخيرة (لـ /products.json)
    last_url_index: int = 0       # فهرس آخر URL في قائمة sitemap
    urls_done: int = 0
    urls_total: int = 0
    rows_saved: int = 0
    started_at: str = ""
    finished_at: str = ""
    error: str = ""
    last_checkpoint_at: str = ""


class ScraperState:
    """
    نظام نقاط الاستئناف الكامل.
    يقرأ/يكتب scraper_state.json ويحتفظ بحالة كل متجر.
    """

    def __init__(self, path: str = STATE_FILE):
        self._path = path
        self._data: Dict[str, StoreCheckpoint] = {}
        self._load()

    # ── قراءة / كتابة ────────────────────────────────────────────────────────

    def _load(self) -> None:
        try:
            with open(self._path, encoding="utf-8") as f:
                raw = json.load(f)
            for domain, d in raw.items():
                try:
                    self._data[domain] = StoreCheckpoint(**d)
                except Exception:
                    pass
        except Exception:
            self._data = {}

    def save(self) -> None:
        try:
            out = {k: asdict(v) for k, v in self._data.items()}
            with _STATE_WRITE_LOCK:
                with open(self._path, "w", encoding="utf-8") as f:
                    json.dump(out, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"تعذّر حفظ الحالة: {e}")

    # ── واجهة المستخدم ───────────────────────────────────────────────────────

    def get(self, domain: str, store_url: str) -> StoreCheckpoint:
        if domain not in self._data:
            self._data[domain] = StoreCheckpoint(store_url=store_url, domain=domain)
        return self._data[domain]

    def update(self, domain: str, **kwargs) -> None:
        if domain in self._data:
            cp = self._data[domain]
            for k, v in kwargs.items():
                if hasattr(cp, k):
                    setattr(cp, k, v)
            cp.last_checkpoint_at = datetime.now().isoformat()
            self.save()

    def mark_done(self, domain: str, rows: int) -> None:
        self.update(
            domain,
            status="done",
            rows_saved=rows,
            finished_at=datetime.now().isoformat(),
        )

    def mark_error(self, domain: str, error: str) -> None:
        self.update(domain, status="error", error=error[:200])

    def is_done(self, domain: str) -> bool:
        return self._data.get(domain, StoreCheckpoint("", "")).status == "done"

    def reset(self, domain: str | None = None) -> None:
        """إعادة تعيين متجر واحد أو الكل"""
        if domain:
            if domain in self._data:
                cp = self._data[domain]
                cp.status = "pending"
                cp.last_page = 0
                cp.last_url_index = 0
                cp.urls_done = 0
                cp.error = ""
                self.save()
        else:
            self._data = {}
            self.save()

    def get_summary(self) -> dict:
        total = len(self._data)
        done  = sum(1 for c in self._data.values() if c.status == "done")
        err   = sum(1 for c in self._data.values() if c.status == "error")
        return {"total": total, "done": done, "errors": err, "pending": total - done - err}

    def all_checkpoints(self) -> Dict[str, StoreCheckpoint]:
        return self._data


# ══════════════════════════════════════════════════════════════════════════════
#  استخراج المنتجات من JSON / HTML
# ══════════════════════════════════════════════════════════════════════════════

def _domain(url: str) -> str:
    return urlparse(url).netloc.replace("www.", "")


def _write_live_progress(domain: str, data: dict) -> None:
    """يكتب ملف تقدم حي خاص بالمتجر — يُقرأ من واجهة Streamlit."""
    try:
        with _LIVE_WRITE_LOCK:
            with open(os.path.join(_DATA_DIR, f"_sc_live_{domain}.json"), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


def extract_product(data: dict, store_url: str) -> dict | None:
    """
    يُحوّل بيانات منتج خام إلى صف موحّد بـ CSV_COLS.
    يدعم: Shopify / Salla / Zid / WooCommerce / HTML meta
    """
    name = (
        data.get("name") or data.get("title") or
        data.get("product_name") or data.get("الاسم") or ""
    ).strip()
    if not name:
        return None

    def _price(raw):
        try:
            return float(str(raw).replace(",", "").replace("ر.س", "").strip())
        except Exception:
            return 0.0

    price = _price(
        data.get("price") or data.get("Price") or
        data.get("regular_price") or data.get("السعر") or 0
    )
    orig  = _price(
        data.get("compare_at_price") or data.get("original_price") or
        data.get("السعر_الأصلي") or price
    )
    sku   = str(data.get("sku") or data.get("id") or data.get("SKU") or "")
    url   = (data.get("url") or data.get("link") or data.get("handle") or "").strip()
    if url and not url.startswith("http"):
        base = store_url.rstrip("/")
        url  = f"{base}/{url.lstrip('/')}"
    image = (
        data.get("image") or data.get("featured_image") or
        data.get("thumbnail") or ""
    )
    if isinstance(image, dict):
        image = image.get("src", "")
    brand = str(data.get("vendor") or data.get("brand") or data.get("الماركة") or "")
    cat   = str(data.get("product_type") or data.get("category") or "")
    avail = str(data.get("available") or data.get("in_stock") or "true")

    return {
        "store":          _domain(store_url),
        "name":           name,
        "price":          price,
        "original_price": orig,
        "sku":            sku,
        "url":            url,
        "image":          image if isinstance(image, str) else "",
        "brand":          brand,
        "category":       cat,
        "availability":   avail,
        "scraped_at":     datetime.now().isoformat()[:19],
    }


def _url_looks_like_product_page(url: str) -> bool:
    """
    يمنح og:title / h1 معنى «منتج» فقط على مسارات تبدو صفحة منتج،
    حتى لا يُسجَّل اسم المتجر من Organization كمنتج على كل الصفحات.
    """
    try:
        from engines.sitemap_resolve import _is_product_url, _is_salla_product

        return bool(_is_salla_product(url) or _is_product_url(url))
    except Exception:
        p = (urlparse(url).path or "").lower()
        return bool(
            _re.search(r"/p\d{5,}(?:/|\?|$)", p)
            or "/products/" in p
            or "/product/" in p
        )


def _product_fields_from_all_json_ld(html: str) -> dict:
    """يجمع حقول Product من كل بلوكات JSON-LD (@graph، قوائم، …)."""
    try:
        from utils.competitor_product_scraper import _walk_json_ld
    except Exception:
        return {}
    acc: dict = {}
    for m in _re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>([\s\S]*?)</script>',
        html,
        _re.I,
    ):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        _walk_json_ld(data, acc)
    return acc


# ══════════════════════════════════════════════════════════════════════════════
#  جلب منتج واحد من URL
# ══════════════════════════════════════════════════════════════════════════════

def _sync_fetch_fallback(url: str, timeout: int = 25) -> str | None:
    """
    سلسلة الـ fallback المزامنة لتخطي الحماية (تُستدعى من ThreadPoolExecutor):
    1. curl_cffi  → TLS Impersonation (أقوى ضد Cloudflare)
    2. cloudscraper → JS Challenge bypass
    3. requests   → طلب عادي بـ headers محاكية
    """
    if not _ANTI_BAN_AVAILABLE:
        return None

    try:
        result = _try_all_sync_fallbacks(url, timeout=timeout)
        return result
    except TypeError:
        # إذا كانت try_all_sync_fallbacks لا تقبل timeout بعد
        try:
            return _try_all_sync_fallbacks(url)
        except Exception as exc:
            logger.debug("_sync_fetch_fallback (no-timeout variant) فشل: %s — %s", url, exc)
            return None
    except Exception as exc:
        logger.debug("_sync_fetch_fallback فشل: %s — %s", url, exc)
        return None


async def fetch_product(
    session: aiohttp.ClientSession,
    url: str,
    store_url: str,
    semaphore: asyncio.Semaphore,
    http_status_counters: Dict[str, int] | None = None,
) -> dict | None:
    """
    يجلب صفحة/API منتج ويُعيد dict موحّد أو None.
    """
    async with semaphore:
        # ── Shopify-style .json ──────────────────────────────────────────
        json_url = url if url.endswith(".json") else url.rstrip("/") + ".json"
        try:
            async with session.get(
                json_url, timeout=aiohttp.ClientTimeout(total=12), ssl=False
            ) as resp:
                if resp.status == 200 and "json" in resp.headers.get("Content-Type", ""):
                    data = await resp.json(content_type=None)
                    prod = data.get("product", data)
                    row  = extract_product(prod, store_url)
                    if row:
                        return row
                elif resp.status in (403, 429) and http_status_counters is not None:
                    http_status_counters[str(resp.status)] = (
                        http_status_counters.get(str(resp.status), 0) + 1
                    )
        except Exception:
            pass

        # ── HTML fallback ────────────────────────────────────────────────
        hdrs = _get_browser_headers(store_url)

        html: str | None = None
        loop = asyncio.get_running_loop()

        # محاولة aiohttp عادية
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=18),
                headers=hdrs, ssl=False, allow_redirects=True,
            ) as resp:
                if resp.status == 200:
                    html = await resp.text(errors="replace")
                elif resp.status in (403, 429, 503):
                    if resp.status in (403, 429) and http_status_counters is not None:
                        http_status_counters[str(resp.status)] = (
                            http_status_counters.get(str(resp.status), 0) + 1
                        )
                    logger.debug("HTTP %d — جرب curl_cffi: %s", resp.status, url)
                    try:
                        html = await asyncio.wait_for(
                            loop.run_in_executor(_FALLBACK_EXECUTOR, _sync_fetch_fallback, url),
                            timeout=35.0,
                        )
                    except asyncio.TimeoutError:
                        logger.debug("Fallback timeout (35s): %s", url)
                        html = None
        except Exception:
            try:
                html = await asyncio.wait_for(
                    loop.run_in_executor(_FALLBACK_EXECUTOR, _sync_fetch_fallback, url),
                    timeout=35.0,
                )
            except asyncio.TimeoutError:
                logger.debug("Fallback timeout (35s) after exception: %s", url)
                html = None
            except Exception:
                html = None

        if not html:
            return None

        # ── JSON-LD + meta (كل البلوكات — سلة وShopify وغيرها) ───────────
        try:
            from utils.competitor_product_scraper import extract_meta_bundle

            bundle = extract_meta_bundle(html, url)
            schema_name = (bundle.get("name") or "").strip()
            og_title = (bundle.get("title") or "").strip()
            label = schema_name or og_title
            if label and (schema_name or _url_looks_like_product_page(url)):
                imgs = bundle.get("images") or []
                img0 = str(imgs[0]).strip() if imgs else ""
                p_raw = bundle.get("price")
                try:
                    p_val = float(p_raw) if p_raw not in (None, "") else 0.0
                except Exception:
                    p_val = 0.0
                row = extract_product(
                    {
                        "name": label,
                        "price": p_val,
                        "sku": bundle.get("sku") or "",
                        "image": img0,
                        "url": url,
                        "brand": bundle.get("brand") or "",
                    },
                    store_url,
                )
                if row:
                    return row
        except Exception as exc:
            logger.debug("extract_meta_bundle: %s — %s", url, exc)

        ld_acc = _product_fields_from_all_json_ld(html)
        if ld_acc.get("name"):
            imgs = ld_acc.get("images") or []
            im0 = str(imgs[0]).strip() if imgs else ""
            try:
                p_ld = float(ld_acc.get("price") or 0)
            except Exception:
                p_ld = 0.0
            row = extract_product(
                {
                    "name": str(ld_acc.get("name", "")).strip(),
                    "price": p_ld,
                    "sku": ld_acc.get("sku") or "",
                    "image": im0,
                    "url": url,
                    "brand": ld_acc.get("brand") or "",
                },
                store_url,
            )
            if row:
                return row

        # ── og:meta + h1 (احتياط عند فشل المسارات أعلاه) ─────────────────
        def _meta(pattern: _re.Pattern) -> str:
            m = pattern.search(html)
            return m.group(1).strip() if m else ""

        pname = _meta(_RE_OG_TITLE)
        pimg = _meta(_RE_OG_IMAGE)
        purl = _meta(_RE_OG_URL) or url
        pprice_raw = _meta(_RE_OG_PRICE)
        try:
            pprice = float(pprice_raw.replace(",", "").strip()) if pprice_raw else 0.0
        except Exception:
            pprice = 0.0

        if pprice == 0.0:
            price_match = _RE_PRICE_SPAN.search(html)
            if price_match:
                try:
                    pprice = float(price_match.group(1).replace(",", "").replace(" ", ""))
                except Exception:
                    pprice = 0.0

        if not pname:
            h1_match = _RE_H1_PRODUCT.search(html)
            if h1_match:
                pname = h1_match.group(1).strip()

        if pname and _url_looks_like_product_page(url):
            return extract_product(
                {"name": pname, "image": pimg, "url": purl, "price": pprice},
                store_url,
            )

        return None


# ══════════════════════════════════════════════════════════════════════════════
#  كاشط متجر واحد مع نقاط استئناف
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_one_store(
    store_url: str,
    progress: Progress,
    state: ScraperState,
    concurrency: int = 8,
    max_products: int = 0,
    resume: bool = True,
    single_mode: bool = False,
) -> List[dict]:
    domain = _domain(store_url)
    cp     = state.get(domain, store_url)

    if resume and cp.status == "done" and not single_mode:
        logger.info(f"⏭️ {domain} — مكتمل ({cp.rows_saved} منتج)")
        return []

    cp.status     = "running"
    cp.started_at = cp.started_at or datetime.now().isoformat()
    state.save()

    try:
        from engines.sitemap_resolve import resolve_store_product_urls
    except ImportError:
        try:
            from scrapers.sitemap_resolve import resolve_store_product_urls
        except ImportError:
            logger.error("تعذّر تحميل sitemap_resolve")
            state.mark_error(domain, "import_error")
            return []

    connector = aiohttp.TCPConnector(ssl=False, limit=concurrency + 5)
    session: aiohttp.ClientSession | None = None

    try:
        session = aiohttp.ClientSession(
            connector=connector,
            connector_owner=True,
            timeout=aiohttp.ClientTimeout(total=30),
        )

        progress.current_store    = domain
        progress.store_urls_done  = 0
        progress.store_urls_total = 0
        progress.save()

        logger.info(f"🗺️ {domain} — يحلل Sitemap…")
        try:
            entries = await asyncio.wait_for(
                resolve_store_product_urls(session, store_url), timeout=300
            )
        except asyncio.TimeoutError:
            state.mark_error(domain, "sitemap_timeout")
            return []
        except Exception as e:
            state.mark_error(domain, str(e)[:150])
            return []

        if not entries:
            logger.warning(f"⚠️ {domain} — لا روابط في Sitemap")
            state.mark_error(domain, "empty_sitemap")
            return []

        all_urls = [e.url for e in entries]
        total    = len(all_urls)
        if max_products and max_products < total:
            all_urls = all_urls[:max_products]
            total    = len(all_urls)

        resume_idx = cp.last_url_index if (resume and cp.last_url_index > 0) else 0
        if resume_idx > 0:
            logger.info(f"🔄 {domain} — استئناف من الرابط {resume_idx}/{total}")
        pending_urls = all_urls[resume_idx:]

        state.update(domain, urls_total=total, urls_done=resume_idx)
        progress.urls_total       += total
        progress.store_urls_total  = total

        semaphore         = asyncio.Semaphore(concurrency)
        rows: List[dict]  = []
        done_count        = resume_idx
        checkpoint_every  = max(50, min(200, total // 10 + 1))
        store_http_status = {"403": 0, "429": 0}

        _TASK_TIMEOUT = 60.0

        async def _fetch_one(url: str) -> None:
            nonlocal done_count
            try:
                row = await asyncio.wait_for(
                    fetch_product(
                        session,
                        url,
                        store_url,
                        semaphore,
                        http_status_counters=store_http_status,
                    ),
                    timeout=_TASK_TIMEOUT,
                )
                if row:
                    rows.append(row)
            except asyncio.TimeoutError:
                logger.debug("URL timeout (%ss): %s", _TASK_TIMEOUT, url)
                progress.fetch_exceptions += 1
            except Exception as e:
                progress.fetch_exceptions += 1
                progress.last_error = str(e)[:100]
            finally:
                done_count += 1
                progress.urls_processed  += 1
                progress.store_urls_done  = done_count

                if done_count % 10 == 0 or done_count >= total:
                    safe = progress.urls_processed
                    progress.success_rate_pct = (
                        (safe - progress.fetch_exceptions) / safe * 100 if safe else 0
                    )
                    progress.save()
                    _write_live_progress(domain, {
                        "urls_done":  done_count,
                        "urls_total": total,
                        "rows_saved": len(rows),
                        "pct":        min(100, int(done_count / max(total, 1) * 100)),
                        "updated_at": datetime.now().isoformat()[:19],
                    })

                if done_count % checkpoint_every == 0:
                    state.update(
                        domain,
                        last_url_index=done_count,
                        urls_done=done_count,
                    )
                    logger.info(
                        f"💾 {domain} — نقطة @ {done_count}/{total} | {len(rows)} منتج"
                    )

        BATCH = 50
        _batch_new_rows: List[dict] = []  # مخزن مؤقت للكتابة الفورية إلى SQLite

        for start in range(0, len(pending_urls), BATCH):
            batch = pending_urls[start: start + BATCH]
            _pre_count = len(rows)
            await asyncio.gather(*[_fetch_one(u) for u in batch], return_exceptions=True)

            # ── كتابة فورية لكل منتج جديد إلى SQLite (Real-time) ──────────
            _new_in_batch = rows[_pre_count:]
            if _new_in_batch:
                try:
                    from utils.db_manager import upsert_competitor_products
                    _db_rows = [{
                        "المنتج":      r.get("name", ""),
                        "السعر":       r.get("price", 0),
                        "image_url":   r.get("image", ""),
                        "product_url": r.get("url", ""),
                        "brand":       r.get("brand", ""),
                        "size":        "",
                        "gender":      "للجنسين",
                    } for r in _new_in_batch if r.get("name")]
                    if _db_rows:
                        upsert_competitor_products(domain, _db_rows)
                except Exception as _db_exc:
                    logger.debug("SQLite real-time write error: %s", _db_exc)

            total_blocks = int(store_http_status.get("403", 0)) + int(store_http_status.get("429", 0))
            processed_so_far = start + len(batch)
            block_rate = total_blocks / max(processed_so_far, 1)

            if block_rate > 0.3:
                adaptive_delay = 4.0
            elif block_rate > 0.1:
                adaptive_delay = 2.0
            else:
                adaptive_delay = 0.5

            if start + BATCH < len(pending_urls):
                await asyncio.sleep(adaptive_delay)

    finally:
        if session is not None and not session.closed:
            await session.close()
        await asyncio.sleep(0.25)

    state.mark_done(domain, len(rows))
    progress.stores_http_errors[domain] = {
        "403": int(store_http_status.get("403", 0)),
        "429": int(store_http_status.get("429", 0)),
    }
    progress.save()
    logger.info(f"✅ {domain} — {len(rows)} منتج")
    return rows


# ══════════════════════════════════════════════════════════════════════════════
#  كشط متجر مفرد (تُستدعى من زر الواجهة)
# ══════════════════════════════════════════════════════════════════════════════

def run_single_store(
    store_url: str,
    concurrency: int = 8,
    max_products: int = 0,
    force: bool = False,
) -> dict:
    domain = _domain(store_url)
    state  = ScraperState()
    if force:
        state.reset(domain)

    progress = Progress(
        running=True,
        started_at=datetime.now().isoformat(),
        stores_total=1,
        current_store=domain,
        phase="discovering",
    )
    progress.save()

    try:
        progress.phase = "scraping"
        progress.save()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        rows = loop.run_until_complete(
            scrape_one_store(
                store_url, progress, state,
                concurrency=concurrency,
                max_products=max_products,
                resume=not force,
                single_mode=True,
            )
        )
    except Exception as e:
        progress.running = False
        progress.phase = "failed"
        progress.finished_at = datetime.now().isoformat()
        progress.last_error = str(e)[:300]
        progress.save()
        state.mark_error(domain, str(e))
        return {"success": False, "rows": 0, "message": str(e), "domain": domain}
    finally:
        try:
            loop.close()
        except Exception:
            pass

    n = _merge_rows_to_csv(rows, domain)
    progress.running      = False
    progress.phase        = "completed"
    progress.finished_at  = datetime.now().isoformat()
    progress.stores_done  = 1
    progress.stores_results[domain] = len(rows)
    progress.rows_in_csv  = n
    progress.save()

    return {
        "success": True,
        "rows":    len(rows),
        "message": f"✅ {len(rows)} منتج من {domain}",
        "domain":  domain,
    }


def _merge_rows_to_csv(new_rows: List[dict], domain: str) -> int:
    if not new_rows:
        return _count_csv_rows()

    new_df = pd.DataFrame(new_rows)
    for col in CSV_COLS:
        if col not in new_df.columns:
            new_df[col] = ""

    try:
        old_df = pd.read_csv(OUTPUT_CSV, encoding="utf-8-sig", low_memory=False)
        old_df = old_df[old_df["store"].astype(str) != domain]
        combined = pd.concat([old_df, new_df[CSV_COLS]], ignore_index=True)
    except Exception:
        combined = new_df[CSV_COLS]

    combined.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    return len(combined)


def _count_csv_rows() -> int:
    try:
        return sum(1 for _ in open(OUTPUT_CSV, encoding="utf-8-sig")) - 1
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════════════════════
#  حلقة الكشط الرئيسية (كل المتاجر)
# ══════════════════════════════════════════════════════════════════════════════

async def run_scraper(
    concurrency: int = 8,
    max_products: int = 0,
    resume: bool = True,
) -> None:
    try:
        with open(COMPETITORS_FILE, encoding="utf-8") as f:
            stores: List[str] = json.load(f)
    except Exception:
        stores = []

    if not stores:
        logger.error("لا توجد متاجر في competitors_list.json")
        return

    state    = ScraperState()
    progress = Progress(
        running=True,
        started_at=datetime.now().isoformat(),
        stores_total=len(stores),
        phase="discovering",
    )
    progress.save()

    for i, store_url in enumerate(stores, 1):
        domain = _domain(store_url)
        logger.info(f"\n{'═'*60}\n🏪 [{i}/{len(stores)}] {domain}\n{'═'*60}")
        progress.stores_done   = i - 1
        progress.current_store = domain
        progress.phase         = "scraping"
        progress.save()

        rows = await scrape_one_store(
            store_url, progress, state,
            concurrency=concurrency,
            max_products=max_products,
            resume=resume,
        )

        progress.stores_done = i
        progress.stores_results[domain] = len(rows)
        progress.rows_in_csv = _merge_rows_to_csv(rows, domain)
        progress.save()

    progress.running     = False
    progress.phase       = "completed"
    progress.finished_at = datetime.now().isoformat()
    progress.save()

    summary = state.get_summary()
    logger.info(
        f"\n✅ اكتمل | متاجر: {summary['done']}/{summary['total']} "
        f"| أخطاء: {summary['errors']} "
        f"| منتجات: {progress.rows_in_csv:,}"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="محرك كشط مهووس v2.0")
    parser.add_argument("--store", default="",
                        help="رابط متجر واحد (فارغ = كل المتاجر)")
    parser.add_argument("--max-products", type=int, default=0,
                        help="أقصى عدد منتجات لكل متجر (0 = بلا حد)")
    parser.add_argument("--concurrency", type=int, default=8,
                        help="عدد الطلبات المتزامنة")
    parser.add_argument("--no-resume", action="store_true",
                        help="إعادة الكشط من الصفر (تجاهل نقاط الاستئناف)")
    parser.add_argument("--reset-state", action="store_true",
                        help="مسح كل نقاط الاستئناف قبل البدء")
    args = parser.parse_args()

    resume = not args.no_resume
    _write_pid_file()

    try:
        if args.reset_state:
            ScraperState().reset()
            logger.info("🗑️ تم مسح نقاط الاستئناف")

        if args.store:
            result = run_single_store(
                args.store,
                concurrency=args.concurrency,
                max_products=args.max_products,
                force=not resume,
            )
            logger.info(result["message"])
            if not result.get("success", False):
                _mark_progress_failed(result.get("message", "فشل تشغيل الكاشط"))
        else:
            asyncio.run(
                run_scraper(
                    concurrency=args.concurrency,
                    max_products=args.max_products,
                    resume=resume,
                )
            )
    except Exception as e:
        _mark_progress_failed(str(e))
        raise
    finally:
        _cleanup_pid_file()


if __name__ == "__main__":
    main()
