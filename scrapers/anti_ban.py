"""
scrapers/anti_ban.py — ترسانة ضد الحظر v2.1 (2026)  *** MASTER ***
═══════════════════════════════════════════════════════════════════════
✅ تم الإصلاح: منع تسرب اتصالات Aiohttp (Memory Leaks)
✅ تم الإصلاح: Connection Pooling لمحركات الـ Fallback لتخفيف الـ CPU
✅ تم الإصلاح: تمرير Timeouts للسيطرة الكاملة على الـ Threads
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
import threading
import warnings
from collections import defaultdict
from typing import Optional
from urllib.parse import urlparse

import aiohttp

# تجاهل تحذيرات SSL المزعجة في الـ Terminal لتنظيف الـ Logs
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════
#  1. User-Agents — قاعدة بيانات حقيقية من متصفحات 2026
# ══════════════════════════════════════════════════════════════════════════
_REAL_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:134.0) Gecko/20100101 Firefox/134.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.99 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
]

_ACCEPT_LANGUAGES = [
    "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
    "ar,en-US;q=0.9,en;q=0.8",
    "en-US,en;q=0.9,ar;q=0.8",
    "ar-SA,ar;q=0.8,en;q=0.5",
]

_ACCEPT_HEADERS = [
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
]


def get_browser_headers(referer: str = "") -> dict:
    """يولّد headers تحاكي متصفحاً حقيقياً بالكامل — تتغير كل مرة."""
    ua = random.choice(_REAL_UA_POOL)
    headers = {
        "User-Agent":      ua,
        "Accept":          random.choice(_ACCEPT_HEADERS),
        "Accept-Language":  random.choice(_ACCEPT_LANGUAGES),
        "Accept-Encoding":  "gzip, deflate",
        "Connection":       "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":   "document",
        "Sec-Fetch-Mode":   "navigate",
        "Sec-Fetch-Site":   "none" if not referer else "cross-site",
        "Sec-Fetch-User":   "?1",
        "Cache-Control":    "max-age=0",
        "DNT":              "1",
    }
    if referer:
        headers["Referer"] = referer
        headers["Sec-Fetch-Site"] = "cross-site"
        
    try:
        if "Chrome" in ua and "Edg" not in ua:
            major = ua.split("Chrome/")[1].split(".")[0] if "Chrome/" in ua else "134"
            headers.update({
                "sec-ch-ua":          f'"Chromium";v="{major}", "Google Chrome";v="{major}", "Not-A.Brand";v="99"',
                "sec-ch-ua-mobile":   "?0" if "Mobile" not in ua else "?1",
                "sec-ch-ua-platform": '"Windows"' if "Windows" in ua else ('"macOS"' if "Mac" in ua else '"Android"'),
            })
        elif "Edg" in ua:
            major = ua.split("Edg/")[1].split(".")[0] if "Edg/" in ua else "134"
            headers.update({
                "sec-ch-ua":          f'"Chromium";v="{major}", "Microsoft Edge";v="{major}", "Not-A.Brand";v="99"',
                "sec-ch-ua-mobile":   "?0",
                "sec-ch-ua-platform": '"Windows"',
            })
    except IndexError:
        pass # تجاوز آمن في حال تغيير صيغة User-Agent

    return headers


def get_xml_headers() -> dict:
    """رؤوس خاصة بطلبات Sitemap XML."""
    ua = random.choice([
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    ])
    return {
        "User-Agent": ua,
        "Accept": "application/xml,text/xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
    }


# ══════════════════════════════════════════════════════════════════════════
#  2. Adaptive Rate Limiter
# ══════════════════════════════════════════════════════════════════════════
class AdaptiveRateLimiter:
    def __init__(self):
        self._state: dict[str, dict] = defaultdict(lambda: {
            "delay":          random.uniform(0.5, 1.5),
            "consecutive_ok": 0,
            "backing_off":    False,
            "backoff_until":  0.0,
        })

    async def wait(self, domain: str) -> None:
        s = self._state[domain]
        now = time.monotonic()
        if s["backing_off"] and now < s["backoff_until"]:
            wait_t = s["backoff_until"] - now
            logger.debug("domain=%s backing-off %.1fs", domain, wait_t)
            await asyncio.sleep(wait_t)
        else:
            jitter = random.uniform(-0.2, 0.3)
            await asyncio.sleep(max(0.1, s["delay"] + jitter))

    def record_success(self, domain: str) -> None:
        s = self._state[domain]
        s["consecutive_ok"] += 1
        s["backing_off"] = False
        if s["consecutive_ok"] >= 5 and s["delay"] > 0.25:
            s["delay"] = max(0.25, s["delay"] * 0.85)

    def record_error(self, domain: str, status: int) -> None:
        s = self._state[domain]
        s["consecutive_ok"] = 0
        if status == 429:
            backoff = min(s["delay"] * 3, 30.0) + random.uniform(2, 8)
            s["delay"] = min(s["delay"] * 2, 15.0)
            s["backing_off"] = True
            s["backoff_until"] = time.monotonic() + backoff
            logger.warning("429 من %s — توقف %.0f ثانية", domain, backoff)
        elif status == 403:
            backoff = random.uniform(15, 45)
            s["backing_off"] = True
            s["backoff_until"] = time.monotonic() + backoff
            logger.warning("403 من %s — توقف %.0f ثانية", domain, backoff)
        elif status in (500, 502, 503, 504):
            s["delay"] = min(s["delay"] * 1.5, 10.0)


_rate_limiter = AdaptiveRateLimiter()

def get_rate_limiter() -> AdaptiveRateLimiter:
    return _rate_limiter


# ══════════════════════════════════════════════════════════════════════════
#  3. Retry مع Exponential Backoff (Fixed Memory Leaks)
# ══════════════════════════════════════════════════════════════════════════
async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    *,
    max_retries: int = 3,
    base_delay:  float = 2.0,
    referer:     str = "",
) -> Optional[aiohttp.ClientResponse]:
    """يجلب URL مع إعادة محاولة + حماية الذاكرة من تسرب الاتصالات (resp.close)."""
    domain = urlparse(url).netloc
    rl = get_rate_limiter()

    for attempt in range(max_retries):
        headers = get_browser_headers(referer=referer or f"https://{domain}/")
        try:
            await rl.wait(domain)
            resp = await session.get(url, headers=headers, ssl=False, allow_redirects=True)

            if resp.status == 200:
                rl.record_success(domain)
                return resp  # الاتصال يبقى مفتوحاً لأن المستدعي سيقرأه

            rl.record_error(domain, resp.status)

            if resp.status in (404, 410):
                resp.close()  # ✅ إغلاق الاتصال المرفوض لمنع تسرب الذاكرة
                return None
                
            if resp.status in (429, 403, 500, 502, 503):
                resp.close()  # ✅ إغلاق الاتصال قبل الانتظار والمحاولة مجدداً
                delay = base_delay * (2 ** attempt) + random.uniform(0, 3)
                logger.debug("attempt %d/%d → %d, سينتظر %.1fs", attempt + 1, max_retries, resp.status, delay)
                await asyncio.sleep(delay)
                continue

            resp.close() # ✅ إغلاق الاتصال للحالات الأخرى
            return None

        except (aiohttp.ClientConnectorError, asyncio.TimeoutError) as exc:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 2)
            logger.debug("attempt %d: %s — انتظار %.1fs", attempt + 1, type(exc).__name__, delay)
            await asyncio.sleep(delay)
        except Exception as exc:
            logger.debug("fetch_with_retry unexpected: %s", exc)
            return None

    return None


# ══════════════════════════════════════════════════════════════════════════
#  Global Fallback Sessions (Thread-Safe) لمنع احتراق الـ CPU
# ══════════════════════════════════════════════════════════════════════════
_SESSION_LOCK = threading.Lock()
_CFFI_SESSION = None
_CLOUD_SCRAPER = None
_REQ_SESSION = None

def _get_cffi_session():
    global _CFFI_SESSION
    if _CFFI_SESSION is None:
        with _SESSION_LOCK:
            if _CFFI_SESSION is None:
                try:
                    from curl_cffi import requests as cffi_requests
                    _CFFI_SESSION = cffi_requests.Session(impersonate="chrome110")
                except ImportError:
                    pass
    return _CFFI_SESSION

def _get_cloudscraper():
    global _CLOUD_SCRAPER
    if _CLOUD_SCRAPER is None:
        with _SESSION_LOCK:
            if _CLOUD_SCRAPER is None:
                try:
                    import cloudscraper
                    _CLOUD_SCRAPER = cloudscraper.create_scraper(
                        browser={"browser": "chrome", "platform": "windows", "mobile": False}
                    )
                except ImportError:
                    pass
    return _CLOUD_SCRAPER

def _get_req_session():
    global _REQ_SESSION
    if _REQ_SESSION is None:
        with _SESSION_LOCK:
            if _REQ_SESSION is None:
                import requests
                _REQ_SESSION = requests.Session()
    return _REQ_SESSION


# ══════════════════════════════════════════════════════════════════════════
#  4. curl_cffi — TLS Fingerprint حقيقي 
# ══════════════════════════════════════════════════════════════════════════
def try_curl_cffi(url: str, timeout: int = 25) -> Optional[str]:
    session = _get_cffi_session()
    if session is None:
        return None
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception as exc:
        logger.debug("curl_cffi %s: %s", url, type(exc).__name__)
    return None


# ══════════════════════════════════════════════════════════════════════════
#  5. cloudscraper — Fallback ثانوي 
# ══════════════════════════════════════════════════════════════════════════
def try_cloudscraper(url: str, timeout: int = 25) -> Optional[str]:
    scraper = _get_cloudscraper()
    if scraper is None:
        return None
    try:
        resp = scraper.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp.text
    except Exception as exc:
        logger.debug("cloudscraper %s: %s", url, type(exc).__name__)
    return None


# ══════════════════════════════════════════════════════════════════════════
#  6. سلسلة الـ Fallback الكاملة (مزامن — يُستدعى من executor)
# ══════════════════════════════════════════════════════════════════════════
def try_all_sync_fallbacks(url: str, timeout: int = 25) -> Optional[str]:
    """
    يحاول curl_cffi أولاً، ثم cloudscraper، ثم requests.
    ✅ يستقبل timeout لمنع الـ Threads من التعلق إلى الأبد.
    """
    # المحاولة 1: curl_cffi مع انتحال شخصية Chrome (الأقوى حالياً)
    html = try_curl_cffi(url, timeout=timeout)
    if html and not looks_like_bot_challenge(html):
        return html

    # المحاولة 2: cloudscraper (حل كلاسيكي لـ Cloudflare JS Challenge)
    html_cs = try_cloudscraper(url, timeout=timeout)
    if html_cs and not looks_like_bot_challenge(html_cs):
        return html_cs

    # المحاولة 3: requests مع رؤوس متصفح حقيقية (Fallback نهائي)
    try:
        headers = get_browser_headers(referer=f"https://{urlparse(url).netloc}/")
        session = _get_req_session()
        resp = session.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=False)
        
        if resp.status_code == 200:
            if not looks_like_bot_challenge(resp.text):
                return resp.text
            return resp.text
    except Exception as exc:
        logger.debug("requests fallback %s: %s", url, type(exc).__name__)

    return html or html_cs or None

def looks_like_bot_challenge(html: str) -> bool:
    """التحقق من وجود علامات تحدي البوت (Cloudflare/DDoS)."""
    if not html or len(html) < 500:
        return True
    snippets = [
        "just a moment", "checking your browser", "cf-browser-verification",
        "enable javascript", "ddos protection by", "attention required! | cloudflare"
    ]
    head = html[:15000].lower()
    return any(s in head for s in snippets)


class _StealthManagerCompat:
    """
    واجهة توافقية لملفات المشروع التي ما زالت تعتمد stealth_manager.
    """

    def get_secure_headers(self, referer: str = "") -> dict:
        return get_browser_headers(referer=referer)

    async def apply_smart_delay(self, min_delay: float = 0.5, max_delay: float = 1.5) -> None:
        await asyncio.sleep(random.uniform(max(0.0, min_delay), max(min_delay, max_delay)))

    def is_shadow_banned(self, html: str, status_code: int) -> tuple[bool, str]:
        if status_code in (403, 429):
            return True, f"http_{status_code}"
        if status_code >= 500:
            return True, f"http_{status_code}"
        text = (html or "").strip().lower()
        is_xml_payload = text.startswith("<?xml") or "<urlset" in text or "<sitemapindex" in text
        is_html_payload = "<html" in text or "<!doctype html" in text
        if is_html_payload and not is_xml_payload:
            markers = (
                "just a moment",
                "checking your browser",
                "cf-browser-verification",
                "attention required! | cloudflare",
                "ddos protection by",
            )
            if any(m in text[:20000] for m in markers):
                return True, "bot_challenge"
        return False, ""

    async def dynamic_backoff(self, attempt_number: int = 1) -> None:
        attempt = max(1, int(attempt_number))
        delay = min(30.0, (2 ** attempt) + random.uniform(0.5, 2.0))
        await asyncio.sleep(delay)


# كائن متوافق يُستورد من المحركات القديمة والجديدة
stealth_manager = _StealthManagerCompat()
