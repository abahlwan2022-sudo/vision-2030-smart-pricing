# 🤖 كيفية ربط الذكاء الاصطناعي (Google Gemini) بالمشروع

لقد قمت بإعداد الكود ليدعم مفتاح API الخاص بك بشكل تلقائي. إليك الصيغة الصحيحة والطرق المتاحة لإضافته:

### 1️⃣ الطريقة الأولى: عبر ملف `secrets.toml` (للتشغيل المحلي)
قم بإنشاء مجلد باسم `.streamlit` (إذا لم يكن موجوداً) وبداخله ملف باسم `secrets.toml` وضع فيه الكود التالي:
```toml
GEMINI_API_KEY = "AIzaSyAnq3hKTSS0-lS8MTeYWRMAl-eVVwTw3Jc"
```
*ملاحظة: هذا الملف مستبعد من GitHub تلقائياً لحماية خصوصيتك.*

### 2️⃣ الطريقة الثانية: عبر متغيرات البيئة (Environment Variables)
إذا كنت ترفع المشروع على منصات مثل **Railway** أو **Streamlit Cloud**، قم بإضافة متغير بيئة جديد:
- **Key:** `GEMINI_API_KEY`
- **Value:** `AIzaSyAnq3hKTSS0-lS8MTeYWRMAl-eVVwTw3Jc`

### 3️⃣ الطريقة الثالثة: دعم عدة مفاتيح (لتجنب الحظر)
إذا كان لديك أكثر من مفتاح، يمكنك إضافتها بصيغة مصفوفة في `secrets.toml`:
```toml
GEMINI_API_KEYS = ["key1", "key2", "AIzaSyAnq3hKTSS0-lS8MTeYWRMAl-eVVwTw3Jc"]
```

---
**✅ تم التأكد من أن الكود في `config.py` يقرأ هذه القيم بالترتيب الصحيح.**
