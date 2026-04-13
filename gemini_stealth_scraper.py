import json
import logging
from typing import Optional, Dict, Any

from curl_cffi import requests
from bs4 import BeautifulSoup
import google.generativeai as genai
from pydantic import BaseModel, Field

# ─── إعداد السجل ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("GeminiScraper")

# ─── هيكل البيانات المطلوب من Gemini (Strict JSON Schema) ──────────────────
class ProductSchema(BaseModel):
    name: str = Field(description="اسم المنتج الكامل")
    price: float = Field(description="السعر الحالي للمنتج كـ رقم عشري، 0 إذا غير متاح")
    original_price: float = Field(description="السعر قبل الخصم، نفس السعر الحالي إذا لا يوجد خصم")
    image_url: str = Field(description="الرابط المباشر لصورة المنتج الرئيسية")
    brand: str = Field(description="اسم الماركة أو العلامة التجارية، فارغ إذا غير متاح")
    availability: str = Field(description="حالة التوفر: 'متوفر' أو 'غير متوفر'")


class GeminiStealthScraper:
    def __init__(self, gemini_api_key: str):
        """
        تهيئة المحرك مع مفتاح Gemini API
        """
        if not gemini_api_key:
            raise ValueError("مفتاح Gemini API مفقود.")

        genai.configure(api_key=gemini_api_key)
        # نستخدم gemini-2.5-flash لسرعته وتكلفته المنخفضة وقدرته العالية على الـ JSON
        self.model = genai.GenerativeModel('gemini-2.5-flash')

    def _fetch_stealth(self, url: str) -> Optional[str]:
        """
        جلب محتوى الصفحة مع تخطي حمايات WAF (مثل Cloudflare) باستخدام بصمة Chrome 120
        """
        logger.info(f"🌐 جلب الصفحة بالتخفي العميق (Chrome 120): {url}")
        try:
            response = requests.get(
                url,
                impersonate="chrome120",  # محاكاة كاملة لبصمة متصفح كروم
                timeout=20,
                headers={
                    "Accept-Language": "ar,en-US;q=0.9,en;q=0.8"
                }
            )
            if response.status_code == 200:
                return response.text
            else:
                logger.error(f"❌ فشل الجلب: HTTP {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"❌ خطأ أثناء الاتصال الشبكي: {str(e)}")
            return None

    def _clean_html_for_llm(self, html_content: str) -> str:
        """
        تنظيف الـ HTML لتقليل استهلاك التوكنز (Tokens) وتسهيل عمل Gemini
        نستخرج فقط النصوص وبعض الـ Meta tags المهمة.
        """
        logger.info("🧹 تنظيف HTML وتقطير البيانات...")
        soup = BeautifulSoup(html_content, 'html.parser')

        # حذف الأكواد غير المفيدة
        for tag in soup(['script', 'style', 'noscript', 'header', 'footer', 'nav', 'svg']):
            tag.decompose()

        # استخراج خصائص الـ Meta المهمة للصورة والسعر
        meta_info = []
        for meta in soup.find_all('meta'):
            prop = meta.get('property', meta.get('name', ''))
            content = meta.get('content', '')
            if prop and content and any(k in prop.lower() for k in ['og:image', 'price', 'title', 'brand']):
                meta_info.append(f"{prop}: {content}")

        # استخراج النص النظيف
        text_content = soup.get_text(separator='\n', strip=True)

        # دمج الـ Meta مع النص (بحد أقصى 15000 حرف لتجنب إرهاق الموديل بدون داعٍ)
        distilled_data = "\n--- META INFO ---\n" + "\n".join(meta_info) + "\n\n--- PAGE TEXT ---\n" + text_content
        return distilled_data[:15000]

    def extract_product(self, url: str) -> Optional[Dict[str, Any]]:
        """
        العملية الكاملة: جلب -> تنظيف -> إرسال لـ Gemini -> إرجاع JSON
        """
        html = self._fetch_stealth(url)
        if not html:
            return None

        clean_text = self._clean_html_for_llm(html)

        logger.info("🧠 إرسال البيانات إلى محرك Gemini للتحليل...")
        prompt = f"""
        أنت محلل بيانات منتجات خبير. قم باستخراج بيانات المنتج من النص المرفق.
        النص مأخوذ من متجر إلكتروني.
        
        النص:
        {clean_text}
        """

        try:
            # طلب استجابة مهيكلة بصيغة JSON بناءً على الـ Schema
            response = self.model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(
                    response_mime_type="application/json",
                    response_schema=ProductSchema,
                    temperature=0.1  # تقليل الهلوسة للحد الأدنى
                )
            )

            result = json.loads(response.text)
            logger.info("✅ تم الاستخراج بنجاح!")
            return result

        except Exception as e:
            logger.error(f"❌ خطأ أثناء تحليل Gemini: {str(e)}")
            return None


# =====================================================================
# منطقة الاختبار المباشر (Run Test)
# =====================================================================
if __name__ == "__main__":
    import os

    # ⚠️ ضع مفتاح API الخاص بك هنا للاختبار
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "ضع_مفتاح_جيميني_هنا")

    # رابط تجريبي من أحد المواقع التي ذكرتها
    TEST_URL = "https://sara-makeup.com/products/نارس-بودرة-مضغوطة-لايت-ريفليكتينج-اوبسيديان-10-غرام"

    if GEMINI_API_KEY == "ضع_مفتاح_جيميني_هنا":
        print("يرجى وضع مفتاح GEMINI_API_KEY في الكود أو كمتغير بيئة قبل التشغيل.")
    else:
        scraper = GeminiStealthScraper(gemini_api_key=GEMINI_API_KEY)
        product_data = scraper.extract_product(TEST_URL)

        if product_data:
            print("\n" + "=" * 50)
            print("📦 النتيجة النهائية (JSON):")
            print("=" * 50)
            print(json.dumps(product_data, ensure_ascii=False, indent=4))
