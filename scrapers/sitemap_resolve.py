import asyncio
import aiohttp
import logging
import urllib.parse
from bs4 import BeautifulSoup
from typing import List, Set
from scrapers.anti_ban import stealth_manager

logger = logging.getLogger(__name__)


class SitemapResolver:
    def __init__(self):
        # الكلمات المفتاحية التي تدل على أن الرابط هو خريطة موقع فرعية
        self.sitemap_indicators = ['sitemap', '.xml']
        # الكلمات المفتاحية التي تستبعد الروابط غير الخاصة بالمنتجات (مثل المقالات، الصفحات الثابتة)
        self.exclude_indicators = ['/blog/', '/pages/', '/categories/', '/tags/']

    def is_product_url(self, url: str) -> bool:
        """تحديد ما إذا كان الرابط يعود لمنتج بناءً على الهيكلية"""
        url_lower = url.lower()

        # استبعاد الصفحات غير المنتجات
        for ex in self.exclude_indicators:
            if ex in url_lower:
                return False

        # مؤشرات Salla و Zid لروابط المنتجات
        if '/p/' in url_lower or 'product' in url_lower:
            return True

        # إذا كان الرابط طويلاً ويحتوي على شرطات (Slug)، غالباً هو منتج
        parsed_url = urllib.parse.urlparse(url)
        path = parsed_url.path
        if path.count('-') >= 2 and len(path) > 15:
            return True

        return False

    async def fetch_and_parse_sitemap(self, session: aiohttp.ClientSession, sitemap_url: str, visited_sitemaps: Set[str]) -> List[str]:
        """دالة متداخلة (Recursive) لجلب كافة الروابط من خرائط الموقع والفهارس"""
        if sitemap_url in visited_sitemaps:
            return []

        visited_sitemaps.add(sitemap_url)
        product_urls = set()
        headers = stealth_manager.get_secure_headers()

        try:
            # تطبيق تأخير شبحي قبل كل طلب لخريطة الموقع
            await stealth_manager.apply_smart_delay(1.5, 4.0)

            async with session.get(sitemap_url, headers=headers, timeout=20) as response:
                html_content = await response.text()
                is_banned, ban_msg = stealth_manager.is_shadow_banned(html_content, response.status)

                if is_banned:
                    logger.error(f"[Sitemap] Ban detected on {sitemap_url}: {ban_msg}")
                    # تطبيق خوارزمية التراجع قبل الاستسلام
                    await stealth_manager.dynamic_backoff(attempt_number=2)
                    return []

                content = await response.read()
                soup = BeautifulSoup(content, 'xml')
                loc_tags = soup.find_all('loc')

                tasks = []
                for loc in loc_tags:
                    url = loc.text.strip()
                    url_lower = url.lower()

                    # التحقق مما إذا كان الرابط هو خريطة موقع فرعية
                    is_sub_sitemap = any(ind in url_lower for ind in self.sitemap_indicators)

                    if is_sub_sitemap and url not in visited_sitemaps:
                        logger.info(f"[Sitemap] Found nested sitemap: {url}")
                        tasks.append(self.fetch_and_parse_sitemap(session, url, visited_sitemaps))
                    elif self.is_product_url(url):
                        product_urls.add(url)

                # تنفيذ جلب الخرائط الفرعية بالتوازي
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for res in results:
                        if isinstance(res, list):
                            product_urls.update(res)
                        elif isinstance(res, Exception):
                            logger.error(f"[Sitemap] Error in nested sitemap task: {str(res)}")

        except Exception as e:
            logger.error(f"[Sitemap] Failed to fetch sitemap {sitemap_url}: {str(e)}")

        return list(product_urls)

    async def resolve(self, base_url: str) -> List[str]:
        """نقطة الدخول الرئيسية لبدء تحليل الموقع"""
        clean_base_url = base_url.rstrip('/')
        candidate_sitemaps = [
            f"{clean_base_url}/sitemap.xml",
            f"{clean_base_url}/sitemap_index.xml",
            f"{clean_base_url}/sitemap-1.xml",
            f"{clean_base_url}/product-sitemap.xml",
            f"{clean_base_url}/sitemap_products.xml",
        ]

        logger.info(f"[Sitemap] Starting deep resolution for: {clean_base_url}")

        # استخدام Connector للحد من عدد الاتصالات المتزامنة لنفس النطاق
        connector = aiohttp.TCPConnector(limit=2)
        async with aiohttp.ClientSession(connector=connector) as session:
            all_product_urls = set()
            visited_sitemaps = set()
            for sitemap_url in candidate_sitemaps:
                if sitemap_url in visited_sitemaps:
                    continue
                urls = await self.fetch_and_parse_sitemap(session, sitemap_url, visited_sitemaps)
                if urls:
                    all_product_urls.update(urls)
                    # بعد نجاح المسار الأول لا داعي لتوسيع الطلبات على نفس النطاق
                    break

            # إزالة التكرارات بشكل نهائي
            unique_urls = list(all_product_urls)
            logger.info(f"[Sitemap] Resolution completed. Total unique products found: {len(unique_urls)}")
            return unique_urls


sitemap_resolver = SitemapResolver()
