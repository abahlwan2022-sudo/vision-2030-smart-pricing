"""
utils/db_manager.py - v18.0
- تتبع تاريخ الأسعار (يحدث السعر إذا تغير)
- حفظ نقاط استئناف للمعالجة الخلفية
- قرارات لكل منتج (موافق/تأجيل/إزالة)
- سجل كامل بالتاريخ والوقت
"""
import hashlib
import logging
import sqlite3, json, os
from datetime import datetime

from utils.data_paths import get_data_db_path

_logger = logging.getLogger(__name__)

# قاعدة SQLite الرئيسية — مسار الملف عبر get_data_db_path() (DATA_DIR على Railway)
_DB_NAME = "pricing_v18.db"
DB_PATH = get_data_db_path(_DB_NAME)


def _ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _date():
    return datetime.now().strftime("%Y-%m-%d")


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    # WAL: يسمح بالقراءة والكتابة المتزامنة من threads مختلفة بدون تعارض
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")  # 30 ثانية انتظار بدل الخطأ الفوري
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    # أحداث عامة
    c.execute("""CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, page TEXT,
        event_type TEXT, details TEXT,
        product_name TEXT, action_taken TEXT
    )""")

    # قرارات المستخدم (موافق/تأجيل/إزالة)
    c.execute("""CREATE TABLE IF NOT EXISTS decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, product_name TEXT,
        our_price REAL, comp_price REAL,
        diff REAL, competitor TEXT,
        old_status TEXT, new_status TEXT,
        reason TEXT, decided_by TEXT DEFAULT 'user'
    )""")

    # تاريخ الأسعار لكل منتج عند كل منافس
    c.execute("""CREATE TABLE IF NOT EXISTS price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, product_name TEXT,
        competitor TEXT, price REAL,
        our_price REAL, diff REAL,
        match_score REAL, decision TEXT,
        product_id TEXT DEFAULT ''
    )""")

    # نقطة الاستئناف للمعالجة الخلفية
    c.execute("""CREATE TABLE IF NOT EXISTS job_progress (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT UNIQUE,
        started_at TEXT, updated_at TEXT,
        status TEXT DEFAULT 'running',
        total INTEGER DEFAULT 0,
        processed INTEGER DEFAULT 0,
        results_json TEXT DEFAULT '[]',
        missing_json TEXT DEFAULT '[]',
        audit_json TEXT DEFAULT '{}',
        our_file TEXT, comp_files TEXT
    )""")
    # إضافة عمود missing_json إذا لم يكن موجوداً (للتوافق مع قواعد البيانات القديمة)
    try:
        c.execute("ALTER TABLE job_progress ADD COLUMN missing_json TEXT DEFAULT '[]'")
    except sqlite3.OperationalError:
        pass  # العمود موجود بالفعل
    try:
        c.execute("ALTER TABLE job_progress ADD COLUMN audit_json TEXT DEFAULT '{}'")
    except sqlite3.OperationalError:
        pass

    # تاريخ التحليلات
    c.execute("""CREATE TABLE IF NOT EXISTS analysis_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, our_file TEXT,
        comp_file TEXT, total_products INTEGER,
        matched INTEGER, missing INTEGER, summary TEXT
    )""")

    # AI cache
    c.execute("""CREATE TABLE IF NOT EXISTS ai_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, prompt_hash TEXT UNIQUE,
        response TEXT, source TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS hidden_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        product_key TEXT UNIQUE,
        product_name TEXT,
        action TEXT DEFAULT 'hidden'
    )""")

    conn.commit()
    conn.close()


# ─── أحداث ────────────────────────────────
def log_event(page, event_type, details="", product_name="", action=""):
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO events (timestamp,page,event_type,details,product_name,action_taken) VALUES (?,?,?,?,?,?)",
            (_ts(), page, event_type, details, product_name, action)
        )
        conn.commit(); conn.close()
    except: pass


# ─── قرارات ────────────────────────────────
def log_decision(product_name, old_status, new_status, reason="",
                 our_price=0, comp_price=0, diff=0, competitor=""):
    try:
        conn = get_db()
        conn.execute(
            """INSERT INTO decisions
               (timestamp,product_name,our_price,comp_price,diff,competitor,
                old_status,new_status,reason)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (_ts(), product_name, our_price, comp_price, diff,
             competitor, old_status, new_status, reason)
        )
        conn.commit(); conn.close()
    except: pass


def get_decisions(product_name=None, status=None, limit=100):
    try:
        conn = get_db()
        if product_name:
            rows = conn.execute(
                "SELECT * FROM decisions WHERE product_name LIKE ? ORDER BY id DESC LIMIT ?",
                (f"%{product_name}%", limit)
            ).fetchall()
        elif status:
            rows = conn.execute(
                "SELECT * FROM decisions WHERE new_status=? ORDER BY id DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM decisions ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []


# ─── تاريخ الأسعار (الميزة الذكية) ──────────
def upsert_price_history(product_name, competitor, price,
                          our_price=0, diff=0, match_score=0,
                          decision="", product_id=""):
    """
    يحفظ السعر اليوم. إذا وُجد سعر سابق لنفس المنتج/المنافس اليوم → يحدّثه.
    إذا كان أمس → يضيف سجلاً جديداً لتتبع التغيير.
    يرجع True إذا تغير السعر عن آخر تسجيل.
    """
    conn = get_db()
    today = _date()

    # آخر سعر مسجل لهذا المنتج/المنافس
    last = conn.execute(
        """SELECT price, date FROM price_history
           WHERE product_name=? AND competitor=?
           ORDER BY id DESC LIMIT 1""",
        (product_name, competitor)
    ).fetchone()

    price_changed = False
    if last:
        last_price = last["price"]
        last_date  = last["date"]
        price_changed = abs(float(price) - float(last_price)) > 0.01

        if last_date == today:
            # نفس اليوم → حدّث فقط
            conn.execute(
                """UPDATE price_history SET price=?,our_price=?,diff=?,
                   match_score=?,decision=?,product_id=?
                   WHERE product_name=? AND competitor=? AND date=?""",
                (price, our_price, diff, match_score, decision,
                 product_id, product_name, competitor, today)
            )
        else:
            # يوم جديد → أضف سجل
            conn.execute(
                """INSERT INTO price_history
                   (date,product_name,competitor,price,our_price,diff,
                    match_score,decision,product_id)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (today, product_name, competitor, price, our_price,
                 diff, match_score, decision, product_id)
            )
    else:
        # أول مرة
        conn.execute(
            """INSERT INTO price_history
               (date,product_name,competitor,price,our_price,diff,
                match_score,decision,product_id)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (today, product_name, competitor, price, our_price,
             diff, match_score, decision, product_id)
        )

    conn.commit(); conn.close()
    return price_changed


def get_price_history(product_name, competitor="", limit=30):
    try:
        conn = get_db()
        if competitor:
            rows = conn.execute(
                """SELECT * FROM price_history
                   WHERE product_name=? AND competitor=?
                   ORDER BY date DESC LIMIT ?""",
                (product_name, competitor, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT * FROM price_history WHERE product_name=?
                   ORDER BY date DESC LIMIT ?""",
                (product_name, limit)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []


def get_price_changes(days=7):
    """منتجات تغير سعرها خلال X يوم"""
    try:
        conn = get_db()
        rows = conn.execute(
            """SELECT p1.product_name, p1.competitor,
                      p1.price as new_price, p2.price as old_price,
                      p1.date as new_date, p2.date as old_date,
                      (p1.price - p2.price) as price_diff
               FROM price_history p1
               JOIN price_history p2
                 ON p1.product_name=p2.product_name
                AND p1.competitor=p2.competitor
                AND p1.id > p2.id
               WHERE p1.date >= date('now', ?)
                 AND abs(p1.price - p2.price) > 0.01
               ORDER BY abs(p1.price - p2.price) DESC
               LIMIT 100""",
            (f"-{days} days",)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []


# ─── المعالجة الخلفية ──────────────────────
def save_job_progress(job_id, total, processed, results, status="running",
                      our_file="", comp_files="", missing=None, audit_stats=None):
    missing_data = json.dumps(missing if missing else [], ensure_ascii=False, default=str)
    results_data = json.dumps(results, ensure_ascii=False, default=str)
    audit_data = json.dumps(audit_stats if audit_stats is not None else {},
                            ensure_ascii=False, default=str)
    with sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute(
            """INSERT OR REPLACE INTO job_progress
               (job_id,started_at,updated_at,status,total,processed,
                results_json,missing_json,our_file,comp_files,audit_json)
               VALUES (?,
                   COALESCE((SELECT started_at FROM job_progress WHERE job_id=?), ?),
                   ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (job_id, job_id, _ts(), _ts(), status, total, processed,
             results_data, missing_data, our_file, comp_files, audit_data)
        )
        conn.commit()


def get_job_progress(job_id):
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT * FROM job_progress WHERE job_id=?", (job_id,)
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try: d["results"] = json.loads(d.get("results_json", "[]"))
            except: d["results"] = []
            try: d["missing"] = json.loads(d.get("missing_json", "[]"))
            except: d["missing"] = []
            try: d["audit"] = json.loads(d.get("audit_json") or "{}")
            except: d["audit"] = {}
            return d
    except: pass
    return None


def get_last_job():
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT * FROM job_progress ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try: d["results"] = json.loads(d.get("results_json", "[]"))
            except: d["results"] = []
            try: d["missing"] = json.loads(d.get("missing_json", "[]"))
            except: d["missing"] = []
            try: d["audit"] = json.loads(d.get("audit_json") or "{}")
            except: d["audit"] = {}
            return d
    except: pass
    return None


# ─── سجل التحليلات ─────────────────────────
def log_analysis(our_file, comp_file, total, matched, missing, summary=""):
    try:
        conn = get_db()
        conn.execute(
            """INSERT INTO analysis_history
               (timestamp,our_file,comp_file,total_products,matched,missing,summary)
               VALUES (?,?,?,?,?,?,?)""",
            (_ts(), our_file, comp_file, total, matched, missing, summary)
        )
        conn.commit(); conn.close()
    except: pass


def get_analysis_history(limit=20):
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT * FROM analysis_history ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []


def get_events(page=None, limit=50):
    try:
        conn = get_db()
        if page:
            rows = conn.execute(
                "SELECT * FROM events WHERE page=? ORDER BY id DESC LIMIT ?",
                (page, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []


# ── دوال المنتجات المخفية الدائمة ──────────────────────
def save_hidden_product(product_key: str, product_name: str = "", action: str = "hidden"):
    """يحفظ منتجاً مخفياً في قاعدة البيانات بشكل دائم"""
    try:
        conn = get_db()
        conn.execute(
            """INSERT OR REPLACE INTO hidden_products
               (timestamp, product_key, product_name, action)
               VALUES (?, ?, ?, ?)""",
            (_ts(), product_key, product_name, action)
        )
        conn.commit()
        conn.close()
    except Exception as _e:
        _logger.debug("save_hidden_product error: %s", _e)

def get_hidden_product_keys() -> set:
    """يُرجع مجموعة كل مفاتيح المنتجات المخفية من قاعدة البيانات"""
    try:
        conn = get_db()
        rows = conn.execute("SELECT product_key FROM hidden_products").fetchall()
        conn.close()
        return {r["product_key"] for r in rows}
    except Exception as _e:
        _logger.debug("get_hidden_product_keys error: %s", _e)
        return set()


init_db()


# ═══════════════════════════════════════════════════════════════
#  v26 — Upsert Catalog + Processed Products
# ═══════════════════════════════════════════════════════════════

def init_db_v26(conn=None):
    """إضافة جداول v26 للـ upsert ومتابعة المنتجات المعالجة"""
    c_conn = conn or get_db()
    cur = c_conn.cursor()

    # كتالوج مؤقت للمنافسين (يُحدَّث يومياً)
    cur.execute("""CREATE TABLE IF NOT EXISTS comp_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        competitor TEXT NOT NULL,
        product_name TEXT NOT NULL,
        norm_name TEXT,
        price REAL,
        first_seen TEXT,
        last_seen TEXT,
        UNIQUE(competitor, norm_name)
    )""")

    # كتالوج متجرنا (يُحدَّث يومياً)
    cur.execute("""CREATE TABLE IF NOT EXISTS our_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT UNIQUE,
        product_name TEXT NOT NULL,
        norm_name TEXT,
        price REAL,
        first_seen TEXT,
        last_seen TEXT
    )""")

    # المنتجات المعالجة (ترحيل/تسعير/إضافة)
    cur.execute("""CREATE TABLE IF NOT EXISTS processed_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        product_key TEXT UNIQUE,
        product_name TEXT,
        competitor TEXT,
        action TEXT,
        old_price REAL,
        new_price REAL,
        product_id TEXT,
        notes TEXT
    )""")

    c_conn.commit()
    if not conn:
        c_conn.close()


def upsert_our_catalog(our_df, name_col="اسم المنتج", id_col="رقم المنتج", price_col="السعر"):
    """يُحدِّث كتالوج متجرنا عند كل رفع جديد — بدون تكرار"""
    import re
    conn = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    rows_updated = 0
    rows_inserted = 0

    for _, row in our_df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if not name:
            continue
        norm = re.sub(r'\s+', ' ', name.lower().strip())
        pid  = str(row.get(id_col, "")).strip().rstrip(".0")
        try:
            price = float(str(row.get(price_col, 0)).replace(",", ""))
        except Exception:
            price = 0.0

        existing = conn.execute(
            "SELECT id, price FROM our_catalog WHERE product_id=? OR norm_name=?",
            (pid, norm)
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE our_catalog SET price=?, last_seen=?, norm_name=? WHERE id=?",
                (price, today, norm, existing[0])
            )
            rows_updated += 1
        else:
            conn.execute(
                """INSERT INTO our_catalog (product_id, product_name, norm_name, price, first_seen, last_seen)
                   VALUES (?,?,?,?,?,?)""",
                (pid, name, norm, price, today, today)
            )
            rows_inserted += 1

    conn.commit()
    conn.close()
    return {"updated": rows_updated, "inserted": rows_inserted}


def _comp_catalog_product_key(competitor: str, norm_name: str) -> str:
    """مفتاح مستقر لصف المنافس (يتوافق مع عمود comp_product_key إن وُجد)."""
    n = (norm_name or "").strip()
    c = (competitor or "").strip() or "unknown"
    if n:
        return f"{c}::{n}"
    h = hashlib.md5(f"{c}\0{n}".encode("utf-8")).hexdigest()[:16]
    return f"{c}::__{h}"


def _pragma_column_names(conn, table: str):
    """أسماء أعمدة جدول — متوافق مع sqlite3.Row (لا تعتمد على row[1] فقط)."""
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except Exception:
        return []
    out = []
    for r in rows:
        try:
            out.append(str(r["name"]))
        except (KeyError, IndexError, TypeError):
            try:
                out.append(str(r[1]))
            except Exception:
                continue
    return out


def _resolve_comp_name_price_columns(cdf):
    """
    يفضّل أعمدة apply_user_column_map القياسية (المنتج، سعر المنتج) ثم يعود للتخمين.
    """
    cols = list(cdf.columns)
    cs = set(cols)

    if "المنتج" in cs:
        name_col = "المنتج"
    elif "اسم المنتج" in cs:
        name_col = "اسم المنتج"
    else:
        name_col = None
        price_col = None
        for c in cols:
            sample = str(cdf[c].dropna().iloc[0]) if not cdf[c].dropna().empty else ""
            try:
                float(sample.replace(",", ""))
                if price_col is None:
                    price_col = c
            except Exception:
                if name_col is None and len(sample) > 5:
                    name_col = c
        if name_col is None:
            name_col = cols[0]
        if price_col is None:
            price_col = cols[1] if len(cols) > 1 else cols[0]
        return name_col, price_col

    if "سعر المنتج" in cs:
        price_col = "سعر المنتج"
    elif "السعر" in cs:
        price_col = "السعر"
    elif "سعر" in cs:
        price_col = "سعر"
    else:
        price_col = None
        for c in cols:
            if c == name_col:
                continue
            sample = str(cdf[c].dropna().iloc[0]) if not cdf[c].dropna().empty else ""
            try:
                float(str(sample).replace(",", ""))
                price_col = c
                break
            except Exception:
                continue
        if price_col is None:
            price_col = cols[1] if len(cols) > 1 else cols[0]

    return name_col, price_col


def upsert_comp_catalog(comp_dfs: dict):
    """يُحدِّث كتالوج المنافسين عند كل رفع جديد — بدون تكرار"""
    import re
    conn = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    total_new = 0
    rows_updated = 0
    _cc_cols = _pragma_column_names(conn, "comp_catalog")
    _has_cpk = any(c.lower() == "comp_product_key" for c in _cc_cols)

    for cname, cdf in comp_dfs.items():
        name_col, price_col = _resolve_comp_name_price_columns(cdf)

        for _, row in cdf.iterrows():
            name = str(row.get(name_col, "")).strip()
            if not name or len(name) < 4 or name.startswith("styles_"):
                continue
            norm = re.sub(r'\s+', ' ', name.lower().strip())
            try:
                price = float(str(row.get(price_col, 0)).replace(",", ""))
            except Exception:
                price = 0.0

            existing = conn.execute(
                "SELECT id FROM comp_catalog WHERE competitor=? AND norm_name=?",
                (cname, norm)
            ).fetchone()
            _cpk = _comp_catalog_product_key(cname, norm)

            if existing:
                rows_updated += 1
                if _has_cpk:
                    conn.execute(
                        "UPDATE comp_catalog SET price=?, last_seen=?, comp_product_key=? WHERE id=?",
                        (price, today, _cpk, existing[0]),
                    )
                else:
                    try:
                        conn.execute(
                            "UPDATE comp_catalog SET price=?, last_seen=? WHERE id=?",
                            (price, today, existing[0]),
                        )
                    except sqlite3.IntegrityError:
                        conn.execute(
                            "UPDATE comp_catalog SET price=?, last_seen=?, comp_product_key=? WHERE id=?",
                            (price, today, _cpk, existing[0]),
                        )
                        _has_cpk = True
            else:
                try:
                    if _has_cpk:
                        conn.execute(
                            """INSERT INTO comp_catalog (competitor, product_name, norm_name, price,
                                   first_seen, last_seen, comp_product_key)
                               VALUES (?,?,?,?,?,?,?)""",
                            (cname, name, norm, price, today, today, _cpk),
                        )
                    else:
                        conn.execute(
                            """INSERT INTO comp_catalog (competitor, product_name, norm_name, price, first_seen, last_seen)
                               VALUES (?,?,?,?,?,?)""",
                            (cname, name, norm, price, today, today),
                        )
                except sqlite3.IntegrityError as _ie:
                    _em = str(_ie).lower()
                    if "comp_product_key" in _em and not _has_cpk:
                        conn.execute(
                            """INSERT INTO comp_catalog (competitor, product_name, norm_name, price,
                                   first_seen, last_seen, comp_product_key)
                               VALUES (?,?,?,?,?,?,?)""",
                            (cname, name, norm, price, today, today, _cpk),
                        )
                        _has_cpk = True
                    else:
                        raise
                total_new += 1

    conn.commit()
    conn.close()
    return {"new_products": total_new, "updated": rows_updated}


def save_processed(product_key: str, product_name: str, competitor: str,
                   action: str, old_price=0.0, new_price=0.0,
                   product_id="", notes=""):
    """يحفظ منتجاً في قائمة المعالجة — مع منع التكرار، آمن للثريدات"""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute(
                """INSERT OR REPLACE INTO processed_products
                   (timestamp, product_key, product_name, competitor, action,
                    old_price, new_price, product_id, notes)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (_ts(), product_key, product_name, competitor, action,
                 old_price, new_price, product_id, notes)
            )
            conn.commit()
    except Exception:
        pass  # لا يوقف الثريد الخلفي


def get_processed(limit=200) -> list:
    """يُعيد قائمة المنتجات المعالجة"""
    conn = get_db()
    rows = conn.execute(
        """SELECT timestamp, product_key, product_name, competitor,
                  action, old_price, new_price, product_id, notes
           FROM processed_products ORDER BY timestamp DESC LIMIT ?""",
        (limit,)
    ).fetchall()
    conn.close()
    keys = ["timestamp","product_key","product_name","competitor",
            "action","old_price","new_price","product_id","notes"]
    return [dict(zip(keys, r)) for r in rows]


def undo_processed(product_key: str) -> bool:
    """تراجع: إزالة المنتج من قائمة المعالجة"""
    conn = get_db()
    conn.execute("DELETE FROM processed_products WHERE product_key=?", (product_key,))
    conn.execute("DELETE FROM hidden_products WHERE product_key=?", (product_key,))
    conn.commit()
    conn.close()
    return True


def get_processed_keys() -> set:
    """مفاتيح المنتجات المعالجة لاستبعادها من القوائم"""
    conn = get_db()
    rows = conn.execute("SELECT product_key FROM processed_products").fetchall()
    conn.close()
    return {r[0] for r in rows}


# ═══════════════════════════════════════════════════════════════
#  v26.0 — Migration Script + Automation Log
# ═══════════════════════════════════════════════════════════════
def migrate_db_v26():
    """
    سكريبت ترحيل v26.0 — يُنفَّذ مرة واحدة فقط.
    يضمن وجود كل الجداول المطلوبة بدون فقدان أي بيانات.
    آمن للتشغيل المتكرر (idempotent).
    """
    try:
        conn = get_db()
        cur = conn.cursor()

        # ── 1. جدول سجل الأتمتة ──
        cur.execute("""CREATE TABLE IF NOT EXISTS automation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT (datetime('now','localtime')),
            product_name TEXT,
            product_id TEXT,
            rule_name TEXT,
            action TEXT,
            old_price REAL,
            new_price REAL,
            comp_price REAL,
            competitor TEXT,
            match_score REAL,
            reason TEXT,
            pushed_to_make INTEGER DEFAULT 0
        )""")

        # ── 2. جدول إعدادات الأتمتة (للحفظ بين الجلسات) ──
        cur.execute("""CREATE TABLE IF NOT EXISTS automation_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        )""")

        # ── 3. جدول نسخة قاعدة البيانات (لتتبع الترحيلات) ──
        cur.execute("""CREATE TABLE IF NOT EXISTS db_version (
            version TEXT PRIMARY KEY,
            applied_at TEXT DEFAULT (datetime('now','localtime')),
            description TEXT
        )""")

        # ── 4. تسجيل أن الترحيل v26.0 تم تنفيذه ──
        cur.execute("""INSERT OR IGNORE INTO db_version (version, description)
                       VALUES ('v26.0', 'إضافة جداول الأتمتة الذكية وسجل القرارات')""")

        # ── 5. إضافة أعمدة جديدة للجداول الموجودة (بأمان) ──
        # إضافة عمود cost_price لجدول our_catalog إذا لم يكن موجوداً
        try:
            cur.execute("ALTER TABLE our_catalog ADD COLUMN cost_price REAL DEFAULT 0")
        except Exception:
            pass  # العمود موجود مسبقاً

        # إضافة عمود auto_processed لجدول processed_products
        try:
            cur.execute("ALTER TABLE processed_products ADD COLUMN auto_processed INTEGER DEFAULT 0")
        except Exception:
            pass

        # عمود comp_product_key قد يُضاف يدوياً بقيد NOT NULL — نضيفه فارغاً ثم نملأه إن أمكن
        try:
            cur.execute("ALTER TABLE comp_catalog ADD COLUMN comp_product_key TEXT")
        except Exception:
            pass
        try:
            cur.execute(
                """UPDATE comp_catalog SET comp_product_key = competitor || '::' || IFNULL(norm_name, '')
                   WHERE comp_product_key IS NULL OR TRIM(comp_product_key) = ''"""
            )
        except Exception:
            pass

        try:
            cur.execute("ALTER TABLE job_progress ADD COLUMN audit_json TEXT DEFAULT '{}'")
        except Exception:
            pass

        # ── 6. جدول المنافسين الرئيسي (Phase 1) ──
        cur.execute("""CREATE TABLE IF NOT EXISTS competitors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            domain TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            added_at TEXT DEFAULT (datetime('now','localtime')),
            notes TEXT DEFAULT ''
        )""")

        # ── 7. جدول الأسماء البديلة — Aliases (Phase 1) ──
        # يُربط كل اسم بديل/مشوّه بالاسم الرسمي الوحيد للمنافس
        # مثال: alias="نمشي كوم" → canonical_name="Namshi"
        cur.execute("""CREATE TABLE IF NOT EXISTS competitor_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alias TEXT NOT NULL UNIQUE,
            canonical_name TEXT NOT NULL,
            added_at TEXT DEFAULT (datetime('now','localtime'))
        )""")

        # ── 8. تسجيل إصدار Phase 1 ──
        cur.execute("""INSERT OR IGNORE INTO db_version (version, description)
                       VALUES ('v26.1', 'إضافة جداول competitors و competitor_aliases + resolve_competitor')""")

        conn.commit()
        conn.close()
    except Exception as e:
        _logger.error("Migration v26 error: %s", e)
        try: conn.close()
        except: pass


# ═══════════════════════════════════════════════════════════════
#  Phase 1 — resolve_competitor + إدارة Aliases
# ═══════════════════════════════════════════════════════════════

def resolve_competitor(name: str) -> str:
    """
    يحل اسم المنافس عبر جدول competitor_aliases أولاً (Phase 1).

    المنطق:
      - إذا وُجد alias مطابق → يعيد canonical_name (الاسم الرسمي)
      - إذا لم يوجد          → يعيد الاسم بعد trim بدون تغيير

    يضمن عدم تكرار نفس المنافس بأسماء مختلفة في التقارير والمطابقة.
    مثال: resolve_competitor("نمشي") == resolve_competitor("namshi.com") == "Namshi"
    """
    if not name:
        return name
    clean = name.strip()
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT canonical_name FROM competitor_aliases "
            "WHERE alias = ? COLLATE NOCASE",
            (clean,)
        ).fetchone()
        conn.close()
        if row:
            return str(row["canonical_name"]).strip()
    except Exception as exc:
        _logger.debug("resolve_competitor error: %s", exc)
    return clean


def add_competitor_alias(alias: str, canonical_name: str) -> bool:
    """
    يضيف اسماً بديلاً (alias) ويربطه بالاسم الرسمي (canonical_name).

    مثال الاستخدام:
      add_competitor_alias("نمشي", "Namshi")
      add_competitor_alias("namshi.com", "Namshi")
      add_competitor_alias("NAMSHI", "Namshi")

    يعيد True عند النجاح، False عند الخطأ أو المدخلات الفارغة.
    """
    alias = (alias or "").strip()
    canonical_name = (canonical_name or "").strip()
    if not alias or not canonical_name:
        return False
    try:
        conn = get_db()
        conn.execute(
            """INSERT OR REPLACE INTO competitor_aliases
               (alias, canonical_name, added_at)
               VALUES (?, ?, datetime('now','localtime'))""",
            (alias, canonical_name)
        )
        conn.commit()
        conn.close()
        return True
    except Exception as exc:
        _logger.error("add_competitor_alias error: %s", exc)
        return False


def get_all_aliases() -> list:
    """يُرجع كل الأسماء البديلة المسجلة مرتبة حسب الاسم الرسمي."""
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT alias, canonical_name, added_at "
            "FROM competitor_aliases ORDER BY canonical_name"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def register_competitor(name: str, domain: str = "", notes: str = "") -> bool:
    """
    يسجل منافساً رسمياً في جدول competitors.
    آمن للاستدعاء المتكرر (INSERT OR IGNORE).
    """
    name = (name or "").strip()
    if not name:
        return False
    try:
        conn = get_db()
        conn.execute(
            """INSERT OR IGNORE INTO competitors (name, domain, notes)
               VALUES (?, ?, ?)""",
            (name, domain.strip(), notes.strip())
        )
        conn.commit()
        conn.close()
        return True
    except Exception as exc:
        _logger.error("register_competitor error: %s", exc)
        return False


def get_all_competitors() -> list:
    """يُرجع كل المنافسين المسجلين في جدول competitors."""
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT id, name, domain, is_active, added_at, notes "
            "FROM competitors ORDER BY name"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []



# ═══════════════════════════════════════════════════════════════════════════
#  محرك تراكم بيانات المنافسين عبر الجلسات — Persistent Competitor Store
# ═══════════════════════════════════════════════════════════════════════════

def init_competitor_store() -> None:
    """يُنشئ جدول التراكم إذا لم يكن موجوداً — آمن للاستدعاء المتكرر."""
    conn = get_db()
    conn.execute("""CREATE TABLE IF NOT EXISTS competitor_products_store (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        competitor    TEXT NOT NULL,
        product_name  TEXT NOT NULL,
        norm_name     TEXT NOT NULL,
        price         REAL DEFAULT 0,
        image_url     TEXT DEFAULT '',
        product_url   TEXT DEFAULT '',
        brand         TEXT DEFAULT '',
        size          TEXT DEFAULT '',
        gender        TEXT DEFAULT '',
        added_at      TEXT DEFAULT (datetime('now','localtime')),
        updated_at    TEXT DEFAULT (datetime('now','localtime')),
        UNIQUE(competitor, norm_name)
    )""")
    conn.commit()
    conn.close()


def _normalize_for_store(s: str) -> str:
    import unicodedata, re as _re
    t = unicodedata.normalize("NFKC", str(s or ""))
    t = _re.sub(r"[\u064B-\u065F\u0670]", "", t)
    t = _re.sub(r"[أإآا]", "ا", t)
    t = _re.sub(r"[ةه]", "ه", t)
    t = _re.sub(r"[يى]", "ي", t)
    return _re.sub(r"\s+", " ", t).strip().lower()


def upsert_competitor_products(
    competitor: str,
    products: list[dict],
    name_key: str = "المنتج",
    price_key: str = "السعر",
) -> dict:
    """
    يحفظ منتجات المنافس في جدول التراكم — INSERT OR UPDATE.
    يُعيد {'inserted': N, 'updated': M}
    """
    init_competitor_store()
    conn = get_db()
    inserted = updated = 0
    today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for p in products:
        pname = str(p.get(name_key, "") or "").strip()
        if not pname or len(pname) < 2:
            continue
        norm = _normalize_for_store(pname)
        try:
            price = float(str(p.get(price_key, 0) or 0).replace(",", ""))
        except (ValueError, TypeError):
            price = 0.0

        existing = conn.execute(
            "SELECT id FROM competitor_products_store WHERE competitor=? AND norm_name=?",
            (competitor, norm)
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE competitor_products_store
                   SET price=?, updated_at=?,
                       image_url=COALESCE(NULLIF(?,''),(SELECT image_url FROM competitor_products_store WHERE id=?)),
                       product_url=COALESCE(NULLIF(?,''),(SELECT product_url FROM competitor_products_store WHERE id=?))
                   WHERE id=?""",
                (price, today,
                 str(p.get("image_url","") or ""), existing[0],
                 str(p.get("product_url","") or ""), existing[0],
                 existing[0])
            )
            updated += 1
        else:
            conn.execute(
                """INSERT INTO competitor_products_store
                   (competitor, product_name, norm_name, price, image_url,
                    product_url, brand, size, gender, added_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (competitor, pname, norm, price,
                 str(p.get("image_url","") or ""),
                 str(p.get("product_url","") or ""),
                 str(p.get("brand","") or ""),
                 str(p.get("size","") or ""),
                 str(p.get("gender","") or "للجنسين"),
                 today, today)
            )
            inserted += 1

    conn.commit()
    conn.close()
    return {"inserted": inserted, "updated": updated}


def get_all_competitor_products(competitor: str = "") -> list[dict]:
    """يُرجع كل منتجات المنافس (أو الكل إذا competitor فارغ)."""
    init_competitor_store()
    conn = get_db()
    if competitor:
        rows = conn.execute(
            """SELECT competitor, product_name, price, image_url, product_url,
                      brand, size, gender, added_at, updated_at
               FROM competitor_products_store WHERE competitor=?
               ORDER BY updated_at DESC""",
            (competitor,)
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT competitor, product_name, price, image_url, product_url,
                      brand, size, gender, added_at, updated_at
               FROM competitor_products_store
               ORDER BY competitor, updated_at DESC"""
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_competitor_products_df(competitor: str = "") -> "pd.DataFrame":
    """يُرجع DataFrame من جدول التراكم."""
    import pandas as _pd
    rows = get_all_competitor_products(competitor)
    if not rows:
        return _pd.DataFrame()
    return _pd.DataFrame(rows)


def get_competitor_store_stats() -> dict:
    """إحصاءات جدول التراكم."""
    init_competitor_store()
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM competitor_products_store").fetchone()[0]
    comps = conn.execute(
        "SELECT competitor, COUNT(*) as cnt FROM competitor_products_store GROUP BY competitor"
    ).fetchall()
    conn.close()
    return {
        "total_products": total,
        "by_competitor": {r[0]: r[1] for r in comps},
    }


def clear_competitor_store(competitor: str = "") -> int:
    """يحذف منتجات منافس محدد أو الكل. يُعيد عدد الصفوف المحذوفة."""
    init_competitor_store()
    conn = get_db()
    if competitor:
        conn.execute("DELETE FROM competitor_products_store WHERE competitor=?", (competitor,))
    else:
        conn.execute("DELETE FROM competitor_products_store")
    deleted = conn.execute("SELECT changes()").fetchone()[0]
    conn.commit()
    conn.close()
    return deleted

