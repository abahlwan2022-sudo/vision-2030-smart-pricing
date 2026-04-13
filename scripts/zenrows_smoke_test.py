"""
اختبار سريع لـ ZenRows باستخدام نفس مصدر المفتاح الذي يستخدمه التطبيق (config).
تشغيل من جذر المشروع:
  set ZENROWS_API_KEY=...   (Windows CMD)
  $env:ZENROWS_API_KEY="..."  (PowerShell)
  python scripts/zenrows_smoke_test.py [url]
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

# جذر المشروع على sys.path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
except ImportError:
    pass


def main() -> int:
    import requests

    from config import ZENROWS_API_KEY, ZENROWS_MODE

    key = (ZENROWS_API_KEY or "").strip()
    if not key:
        print(
            "ZENROWS_API_KEY is empty. Set the env var or add .env at project root.",
            file=sys.stderr,
        )
        print("(Arabic) المفتاح غير معرّف — عيّن ZENROWS_API_KEY ثم أعد التشغيل.", file=sys.stderr)
        return 1

    target = (sys.argv[1] if len(sys.argv) > 1 else "https://alkhabeershop.com").strip()
    mode = (ZENROWS_MODE or "auto").strip() or "auto"

    r = requests.get(
        "https://api.zenrows.com/v1/",
        params={"url": target, "apikey": key, "mode": mode},
        timeout=90,
    )
    text = r.text or ""
    print("HTTP", r.status_code, "| bytes", len(text.encode("utf-8")))
    if r.status_code != 200:
        print(text[:500])
        return 2

    m = re.search(r"<title[^>]*>([^<]{1,200})", text, re.I)
    title = (m.group(1).strip() if m else "(no title tag)")
    print("title:", title[:120])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
