"""
utils/gemini_stealth_scraper.py — محرك الكشط الذكي v2.0
══════════════════════════════════════════════════════════
طبقات دفاعية لتجاوز حماية البوتات (بدون متصفح محلي / Playwright):
  ⓪ ZenRows Web Unlocker ← عند ضبط ZENROWS_API_KEY: js_render + premium_proxy
  ① curl_cffi + chrome impersonation ← TLS/JA3 fingerprint كامل
  ② cloudscraper ← حماية Cloudflare layer
  ③ requests fallback ← آخر ملاذ

بعد الجلب → Gemini AI يستخرج JSON صارم:
  {"title": "...", "price": "...", "stock_status": "..."}

الاستخدام:
    from utils.gemini_stealth_scraper import SmartAIScraper
    scraper = SmartAIScraper()
    result  = scraper.scrape_url_sync("https://store.com/product")
    results = scraper.scrape_batch_sync(urls, concurrency=3, progress_cb=cb)
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import threading
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

# ── إعداد السجل ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SmartAIScraper")

# ── مفاتيح Gemini لمسار SDK الاحتياطي (يُحدَّث من config عند كل استدعاء) ───
_KEY_LOCK  = threading.Lock()
_KEY_INDEX = 0


def _refresh_sdk_keys() -> List[str]:
    """نفس منطق config — حتى يعمل الكشط بعد تحميل الأسرار لاحقاً."""
    try:
        from config import GEMINI_API_KEYS

        return [k.strip() for k in (GEMINI_API_KEYS or []) if k and len(str(k).strip()) > 20]
    except ImportError:
        import os

        keys: List[str] = []
        raw = (os.environ.get("GEMINI_API_KEY", "") or os.environ.get("GEMINI_API_KEYS", "")).strip()
        if raw.startswith("["):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    keys = [str(k).strip() for k in parsed if k]
            except Exception:
                keys = [k.strip() for k in raw.strip("[]").replace('"', "").split(",") if k.strip()]
        elif raw:
            keys = [raw]
        for i in range(1, 10):
            k = os.environ.get(f"GEMINI_KEY_{i}", "").strip()
            if len(k) > 20:
                keys.append(k)
        return list(dict.fromkeys(keys))


def _gemini_model_name() -> str:
    try:
        from config import GEMINI_MODEL

        return (GEMINI_MODEL or "gemini-2.0-flash").strip()
    except ImportError:
        return "gemini-2.0-flash"


def _get_next_gemini_key() -> str:
    """يدور على مفاتيح Gemini بشكل thread-safe (للمسار SDK فقط)."""
    global _KEY_INDEX
    keys = _refresh_sdk_keys()
    if not keys:
        return ""
    with _KEY_LOCK:
        key = keys[_KEY_INDEX % len(keys)]
        _KEY_INDEX += 1
    return key


# ════════════════════════════════════════════════════════════════════════════
#  هيكل النتيجة
# ════════════════════════════════════════════════════════════════════════════
@dataclass
class ScrapeResult:
    """نتيجة كشط منتج واحد — تُعاد دائماً حتى عند الفشل."""
    url:             str   = ""
    title:           str   = ""
    price:           str   = "0"
    stock_status:    str   = "unknown"      # in_stock | out_of_stock | unknown
    success:         bool  = False
    error:           str   = ""
    scrape_method:   str   = ""             # zenrows | curl_cffi | cloudscraper | requests
    domain:          str   = ""
    raw_price_float: float = 0.0
    competitor:      str   = ""
    image_url:       str   = ""

    # ── تحويل لقاموس ──────────────────────────────────────────────────────
    def to_dict(self) -> Dict[str, Any]:
        return {
            "url":             self.url,
            "title":           self.title,
            "price":           self.price,
            "stock_status":    self.stock_status,
            "success":         self.success,
            "error":           self.error,
            "scrape_method":   self.scrape_method,
            "domain":          self.domain,
            "raw_price_float": self.raw_price_float,
            "competitor":      self.competitor,
            "image_url":       self.image_url,
        }

    # ── تحويل لصيغة DB (upsert_competitor_products) ──────────────────────
    def to_db_record(self) -> Dict[str, Any]:
        return {
            "المنتج":        self.title or self.url,
            "السعر":         self.raw_price_float,
            "product_url":   self.url,
            "image_url":     self.image_url,
            "stock_status":  self.stock_status,
            "scrape_method": self.scrape_method,
        }


# ════════════════════════════════════════════════════════════════════════════
#  Gemini Parser
# ════════════════════════════════════════════════════════════════════════════
_GEMINI_PROMPT_TEMPLATE = """\
أنت محرك استخراج بيانات منتجات دقيق. حلّل محتوى الصفحة أدناه واستخرج بيانات المنتج.

قواعد صارمة للإخراج:
1. أرجع JSON فقط — بدون أي نص أو شرح أو markdown قبله أو بعده
2. استخدم هذه المفاتيح الثلاثة بالضبط ولا شيء آخر
3. إذا تعذّر تحديد قيمة استخدم "" أو "0" أو "unknown"

تنسيق JSON المطلوب:
{{
  "title": "<اسم المنتج الكامل كما يظهر في الصفحة>",
  "price": "<السعر كرقم نصي مثل '299.00' أو '0' إن لم يكن متاحاً>",
  "stock_status": "<one of: in_stock | out_of_stock | unknown>"
}}

محتوى الصفحة:
{page_content}
"""

_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)

_SCRAPE_JSON_SYSTEM = (
    "أرجع JSON فقط بالمفاتيح title و price و stock_status "
    "(stock_status واحد من: in_stock | out_of_stock | unknown). بدون markdown أو شرح."
)


def _normalize_product_dict(data: Dict[str, Any]) -> Optional[Dict[str, str]]:
    if not isinstance(data, dict):
        return None
    result = {
        "title":        str(data.get("title", "")).strip(),
        "price":        str(data.get("price", "0")).strip(),
        "stock_status": str(data.get("stock_status", "unknown")).strip().lower(),
    }
    if result["stock_status"] not in ("in_stock", "out_of_stock", "unknown"):
        result["stock_status"] = "unknown"
    return result


def _parse_json_product_response(raw_text: str) -> Optional[Dict[str, str]]:
    """يستخرج {title, price, stock_status} من نص نموذج لغوي."""
    if not raw_text or not str(raw_text).strip():
        return None
    try:
        t = str(raw_text).strip()
        fence_match = _JSON_FENCE_RE.search(t)
        if fence_match:
            t = fence_match.group(1).strip()
        json_match = re.search(r"\{.*\}", t, re.DOTALL)
        if json_match:
            t = json_match.group(0)
        data = json.loads(t)
        out = _normalize_product_dict(data)
        if out:
            logger.info("✅ LLM استخرج: %s | %s", out["title"][:50], out["price"])
        return out
    except json.JSONDecodeError as e:
        logger.warning("❌ استجابة LLM ليست JSON صالحاً: %s", e)
        return None
    except Exception as e:
        logger.warning("❌ فشل تحليل JSON من LLM: %s", e)
        return None


def _call_gemini_sdk_raw(page_content: str) -> Optional[str]:
    """احتياطي: Google Generative AI SDK عند عدم توفر/فشل مسار ai_engine."""
    api_key = _get_next_gemini_key()
    if not api_key:
        return None
    try:
        import google.generativeai as genai

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(_gemini_model_name())
        truncated = page_content[:12_000]
        prompt = _GEMINI_PROMPT_TEMPLATE.format(page_content=truncated)
        response = model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(temperature=0.05),
        )
        return (response.text or "").strip()
    except ImportError:
        logger.debug("حزمة google-generativeai غير مثبتة — تخطي مسار SDK")
        return None
    except Exception as e:
        logger.warning("مسار Gemini SDK: %s", e)
        return None


def _call_gemini(page_content: str, url: str = "") -> Optional[Dict[str, str]]:
    """
    يمرّر المحتوى عبر نفس سلسلة مزودي التطبيق (Gemini REST → OpenRouter → Cohere)
    ثم يحلّل JSON. يعود لـ Gemini SDK إذا لزم.
    """
    truncated = page_content[:12_000]
    prompt = _GEMINI_PROMPT_TEMPLATE.format(page_content=truncated)
    raw_text: Optional[str] = None

    try:
        from engines.ai_engine import _call_cohere, _call_gemini as _ai_gemini_rest, _call_openrouter

        raw_text = _ai_gemini_rest(
            prompt, system=_SCRAPE_JSON_SYSTEM, grounding=False, temperature=0.05, max_tokens=1024
        )
        if not raw_text:
            raw_text = _call_openrouter(prompt, system=_SCRAPE_JSON_SYSTEM)
        if not raw_text:
            raw_text = _call_cohere(prompt, system=_SCRAPE_JSON_SYSTEM)
    except ImportError as e:
        logger.warning("تعذّر استيراد engines.ai_engine — سيتم استخدام Gemini SDK فقط: %s", e)
    except Exception as e:
        logger.warning("سلسلة ai_engine للكشط: %s", e)

    if not raw_text:
        raw_text = _call_gemini_sdk_raw(page_content)

    if not raw_text:
        logger.error("لا يوجد رد من أي مزود AI (راجع مفاتيح config / OPENROUTER / COHERE)")
        return None

    return _parse_json_product_response(raw_text)


def _call_gemini_with_retries(
    page_content: str,
    url: str = "",
    retries: int = 3,
) -> Optional[Dict[str, str]]:
    """
    استدعاء Gemini مع إعادة المحاولة لتقليل الفشل قدر الإمكان.
    """
    for attempt in range(1, max(1, retries) + 1):
        parsed = _call_gemini(page_content=page_content, url=url)
        if parsed:
            return parsed
        # Backoff متدرج لتجاوز الازدحام/الـ transient failures
        if attempt < retries:
            wait_s = min(8.0, 1.5 * attempt + random.uniform(0.1, 0.8))
            logger.warning("Gemini retry %s/%s for %s (sleep %.1fs)", attempt, retries, url[:80], wait_s)
            time.sleep(wait_s)
    return None


# ════════════════════════════════════════════════════════════════════════════
#  دوال الجلب الشبكي
# ════════════════════════════════════════════════════════════════════════════

def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "").strip()
    except Exception:
        return url


def _clean_html_for_llm(html_content: str) -> str:
    """تنظيف HTML لتقليل التوكنز وتحسين جودة استخراج Gemini."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")

        # حذف العناصر غير المفيدة
        for tag in soup(["script", "style", "noscript", "header", "footer",
                          "nav", "svg", "aside", "iframe"]):
            tag.decompose()

        # استخراج Meta tags المهمة
        meta_lines: List[str] = []
        for meta in soup.find_all("meta"):
            prop    = meta.get("property", meta.get("name", ""))
            content = meta.get("content", "")
            if prop and content:
                if any(k in prop.lower() for k in
                       ["og:title", "og:price", "price", "og:image",
                        "og:description", "product:price", "twitter:title"]):
                    meta_lines.append(f"{prop}: {content}")

        # استخراج البيانات المهيكلة (JSON-LD)
        ld_lines: List[str] = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.string or "")
                if isinstance(ld, dict) and ld.get("@type") in (
                    "Product", "Offer", "ItemPage"
                ):
                    ld_lines.append(json.dumps(ld, ensure_ascii=False)[:2000])
            except Exception:
                pass

        # النص النظيف
        text = soup.get_text(separator="\n", strip=True)

        parts = []
        if meta_lines:
            parts.append("--- META ---\n" + "\n".join(meta_lines))
        if ld_lines:
            parts.append("--- JSON-LD ---\n" + "\n".join(ld_lines))
        parts.append("--- TEXT ---\n" + text)

        return "\n\n".join(parts)[:14_000]

    except ImportError:
        # BeautifulSoup غير متوفرة — نعيد النص الخام مقطوعاً
        return html_content[:14_000]
    except Exception as e:
        logger.warning(f"خطأ في تنظيف HTML: {e}")
        return html_content[:14_000]


# ── الطبقة 1: curl_cffi (TLS/JA3 fingerprint) ───────────────────────────
def _fetch_curl_cffi(url: str, timeout: int = 20) -> Optional[str]:
    """
    يجلب الصفحة بمحاكاة بصمة Chrome 120 على مستوى TLS.
    يتجاوز معظم حمايات Cloudflare البسيطة وحماية بصمة المتصفح.
    """
    try:
        from curl_cffi import requests as cffi_req  # type: ignore
        resp = cffi_req.get(
            url,
            impersonate="chrome120",
            timeout=timeout,
            headers={
                "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
                "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest":  "document",
                "Sec-Fetch-Mode":  "navigate",
                "Sec-Fetch-Site":  "none",
            },
            allow_redirects=True,
        )
        if resp.status_code == 200:
            html = resp.text
            logger.info(f"✅ curl_cffi جلب {url[:60]} ({len(html)} حرف)")
            return _clean_html_for_llm(html)
        logger.warning(f"⚠️ curl_cffi: HTTP {resp.status_code} لـ {url[:60]}")
        return None
    except ImportError:
        logger.debug("curl_cffi غير مثبتة")
        return None
    except Exception as e:
        logger.warning(f"⚠️ curl_cffi خطأ: {e!r}")
        return None


# ── ZenRows Web Unlocker (اختياري عبر ZENROWS_API_KEY في config / البيئة) ─
def _zenrows_config() -> Tuple[str, str]:
    try:
        from config import ZENROWS_API_KEY, ZENROWS_MODE

        key = (ZENROWS_API_KEY or "").strip()
        mode = (ZENROWS_MODE or "auto").strip() or "auto"
        return key, mode
    except ImportError:
        import os

        return (
            os.environ.get("ZENROWS_API_KEY", "").strip(),
            (os.environ.get("ZENROWS_MODE", "auto") or "auto").strip(),
        )


def _fetch_zenrows(url: str, api_key: str, timeout: int = 60) -> Optional[str]:
    """يجلب HTML عبر ZenRows (js_render + premium_proxy)."""
    if not api_key or not url.startswith("http"):
        return None
    try:
        import requests

        zenrows_url = "https://api.zenrows.com/v1/"
        params = {
            "apikey": api_key,
            "url": url,
            "js_render": "true",
            "premium_proxy": "true",
        }
        response = requests.get(zenrows_url, params=params, timeout=max(timeout, 15))
        html_content = response.text
        if response.status_code == 200 and (html_content or "").strip():
            logger.info("✅ zenrows جلب %s", url[:60])
            return _clean_html_for_llm(html_content)
        logger.warning("⚠️ zenrows: HTTP %s لـ %s", response.status_code, url[:60])
    except Exception as e:
        logger.warning("⚠️ zenrows خطأ: %r", e)
    return None


# ── الطبقة 2: cloudscraper ───────────────────────────────────────────────
def _fetch_cloudscraper(url: str, timeout: int = 25) -> Optional[str]:
    """يتجاوز حماية Cloudflare عبر حل JavaScript challenges."""
    try:
        import cloudscraper  # type: ignore
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        resp = scraper.get(url, timeout=timeout)
        if resp.status_code == 200:
            html = resp.text
            logger.info(f"✅ cloudscraper جلب {url[:60]}")
            return _clean_html_for_llm(html)
        logger.warning(f"⚠️ cloudscraper: HTTP {resp.status_code}")
        return None
    except ImportError:
        logger.debug("cloudscraper غير مثبتة")
        return None
    except Exception as e:
        logger.warning(f"⚠️ cloudscraper خطأ: {e!r}")
        return None


# ── الطبقة 3: requests (fallback) ─────────────────────────────────────────
def _fetch_requests(url: str, timeout: int = 15) -> Optional[str]:
    """آخر ملاذ — requests عادية مع User-Agent محاكي."""
    try:
        import requests
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ar,en-US;q=0.9",
        }
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            logger.info(f"✅ requests جلب {url[:60]}")
            return _clean_html_for_llm(resp.text)
        logger.warning(f"⚠️ requests: HTTP {resp.status_code}")
        return None
    except Exception as e:
        logger.warning(f"⚠️ requests خطأ: {e!r}")
        return None


# ════════════════════════════════════════════════════════════════════════════
#  محرك الكشط الرئيسي
# ════════════════════════════════════════════════════════════════════════════
class SmartAIScraper:
    """
    محرك الكشط الذكي متعدد الطبقات.

    الاستخدام الأساسي:
        scraper = SmartAIScraper()

        # كشط URL واحد (sync)
        result = scraper.scrape_url_sync("https://store.com/product")

        # كشط دفعة (sync, مع callback للتقدم)
        results = scraper.scrape_batch_sync(
            urls=["url1", "url2", ...],
            concurrency=3,
            delay=1.0,
            progress_cb=lambda done, total, r: print(f"{done}/{total}"),
        )
    """

    def __init__(
        self,
        concurrency: int = 3,
        delay_between: float = 1.0,
        timeout: int = 25,
        max_retries: int = 3,
    ):
        self.concurrency     = max(1, min(concurrency, 10))
        self.delay_between   = max(0.0, delay_between)
        self.timeout         = timeout
        self.max_retries     = max(1, min(max_retries, 8))
        self._semaphore: Optional[asyncio.Semaphore] = None

    # ── دالة الكشط الواحد (async) ────────────────────────────────────────
    async def _scrape_url_async(self, url: str) -> ScrapeResult:
        """
        يكشط URL واحد مع تدرج بين الطبقات:
        ZenRows (إن وُجد مفتاح) → curl_cffi → cloudscraper → requests
        ثم يُمرر النص لـ Gemini.
        """
        url     = url.strip()
        domain  = _extract_domain(url)
        result  = ScrapeResult(url=url, domain=domain, competitor=domain)

        if not url.startswith("http"):
            result.error = "رابط غير صحيح — يجب أن يبدأ بـ http أو https"
            return result

        last_error = "فشل غير معروف"
        for attempt in range(1, self.max_retries + 1):
            attempt_timeout = int(self.timeout + (attempt - 1) * 8)
            page_content: Optional[str] = None
            method_used: str = ""

            # ⓪ ZenRows — أولوية عند توفر ZENROWS_API_KEY (js_render + premium_proxy)
            z_key, _z_mode = _zenrows_config()
            if z_key:
                loop = asyncio.get_event_loop()
                z_timeout = max(60, attempt_timeout)
                page_content = await loop.run_in_executor(
                    None, _fetch_zenrows, url, z_key, z_timeout
                )
                if page_content:
                    method_used = "zenrows"

            # ① curl_cffi ─────────────────────────────────────────────────
            if not page_content:
                loop = asyncio.get_event_loop()
                page_content = await loop.run_in_executor(
                    None, _fetch_curl_cffi, url, attempt_timeout
                )
                if page_content:
                    method_used = "curl_cffi"

            # ② cloudscraper ─────────────────────────────────────────────
            if not page_content:
                loop = asyncio.get_event_loop()
                page_content = await loop.run_in_executor(
                    None, _fetch_cloudscraper, url, attempt_timeout
                )
                if page_content:
                    method_used = "cloudscraper"

            # ③ requests ──────────────────────────────────────────────────
            if not page_content:
                loop = asyncio.get_event_loop()
                page_content = await loop.run_in_executor(
                    None, _fetch_requests, url, attempt_timeout
                )
                if page_content:
                    method_used = "requests"

            if not page_content:
                last_error = "فشل جلب الصفحة بكل الطرق المتاحة"
                if attempt < self.max_retries:
                    await asyncio.sleep(min(10.0, 2.0 * attempt))
                continue

            # ── استدعاء Gemini مع retries ────────────────────────────────
            loop = asyncio.get_event_loop()
            gemini_result = await loop.run_in_executor(
                None, _call_gemini_with_retries, page_content, url, 3
            )

            if not gemini_result:
                last_error = "Gemini فشل في تحليل الصفحة"
                if attempt < self.max_retries:
                    await asyncio.sleep(min(10.0, 2.5 * attempt))
                continue

            # ── تعبئة النتيجة ─────────────────────────────────────────────
            result.title = gemini_result.get("title", "")
            result.price = gemini_result.get("price", "0")
            result.stock_status = gemini_result.get("stock_status", "unknown")
            result.scrape_method = method_used
            result.success = True

            # تحويل السعر لرقم عشري
            price_clean = re.sub(r"[^\d.]", "", result.price)
            try:
                result.raw_price_float = float(price_clean) if price_clean else 0.0
            except ValueError:
                result.raw_price_float = 0.0

            return result

        result.error = last_error
        result.scrape_method = result.scrape_method or "none"
        return result

    # ── دالة الكشط مع semaphore ──────────────────────────────────────────
    async def _scrape_with_sem(
        self,
        url: str,
        sem: asyncio.Semaphore,
        delay: float,
    ) -> ScrapeResult:
        async with sem:
            result = await self._scrape_url_async(url)
            if delay > 0:
                await asyncio.sleep(delay)
            return result

    # ── كشط دفعة (async) ─────────────────────────────────────────────────
    async def _scrape_batch_async(
        self,
        urls: List[str],
        concurrency: int,
        delay: float,
        progress_cb: Optional[Callable[[int, int, ScrapeResult], None]] = None,
    ) -> List[ScrapeResult]:
        """يعالج قائمة URLs بالتوازي مع التحكم في التزامن."""
        sem        = asyncio.Semaphore(concurrency)
        results    = []
        total      = len(urls)
        done_count = 0

        tasks = [
            asyncio.create_task(self._scrape_with_sem(url, sem, delay))
            for url in urls
        ]

        for coro in asyncio.as_completed(tasks):
            r = await coro
            done_count += 1
            results.append(r)
            if progress_cb:
                try:
                    progress_cb(done_count, total, r)
                except Exception:
                    pass

        return results

    # ════════════════════════════════════════════════════════════════════════
    #  API العامة (Sync)
    # ════════════════════════════════════════════════════════════════════════
    def scrape_url_sync(self, url: str) -> ScrapeResult:
        """يكشط URL واحد ويعيد ScrapeResult. واجهة sync لـ Streamlit."""
        return asyncio.run(self._scrape_url_async(url))

    def scrape_batch_sync(
        self,
        urls: List[str],
        concurrency: Optional[int] = None,
        delay: Optional[float] = None,
        progress_cb: Optional[Callable[[int, int, ScrapeResult], None]] = None,
    ) -> List[ScrapeResult]:
        """
        يكشط قائمة URLs ويعيد List[ScrapeResult]. واجهة sync لـ Streamlit.

        المعاملات:
            urls        : قائمة روابط المنتجات
            concurrency : عدد الطلبات المتزامنة (1-10، الافتراضي: self.concurrency)
            delay       : تأخير بين الطلبات بالثانية (الافتراضي: self.delay_between)
            progress_cb : دالة تُستدعى بعد كل نتيجة (done, total, result)
        """
        c = concurrency if concurrency is not None else self.concurrency
        d = delay       if delay is not None       else self.delay_between
        c = max(1, min(c, 10))

        # استخدام event loop جديد لتجنب تعارض مع Streamlit
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self._scrape_batch_async(urls, c, d, progress_cb)
            )
        finally:
            loop.close()


# ════════════════════════════════════════════════════════════════════════════
#  دوال مساعدة للتكامل مع قاعدة البيانات
# ════════════════════════════════════════════════════════════════════════════
def save_results_to_db(results: List[ScrapeResult]) -> Dict[str, int]:
    """
    يحفظ نتائج الكشط في جدول competitor_products عبر upsert_competitor_products.
    يُجمّع النتائج حسب المنافس (الدومين).

    يعيد: {"inserted": N, "updated": M, "failed": K}
    """
    stats = {"inserted": 0, "updated": 0, "failed": 0}
    if not results:
        return stats

    try:
        from utils.db_manager import upsert_competitor_products, init_competitor_store
        init_competitor_store()
    except ImportError as e:
        logger.error(f"تعذّر استيراد db_manager: {e}")
        return stats

    # تجميع حسب الدومين
    by_domain: Dict[str, List[Dict[str, Any]]] = {}
    for r in results:
        if not r.success or not r.title:
            stats["failed"] += 1
            continue
        domain = r.domain or _extract_domain(r.url) or "unknown"
        by_domain.setdefault(domain, []).append(r.to_db_record())

    # حفظ كل مجموعة
    for domain, records in by_domain.items():
        try:
            r = upsert_competitor_products(domain, records)
            stats["inserted"] += r.get("inserted", 0)
            stats["updated"]  += r.get("updated", 0)
        except Exception as e:
            logger.error(f"خطأ في حفظ {domain}: {e}")
            stats["failed"] += len(records)

    return stats


def results_to_dataframe(results: List[ScrapeResult]):
    """يحوّل قائمة ScrapeResult إلى pandas DataFrame."""
    try:
        import pandas as pd
        return pd.DataFrame([r.to_dict() for r in results])
    except ImportError:
        return results
