"""
engines/concurrent_scraper.py — محرك الكشط المتزامن v1.0
✅ كشط متعدد المنافسين بشكل متوازي (Async/Parallel)
✅ آلية التخطي الذكي (Skip & Proceed) مع معالجة الأخطاء
✅ زر التنشيط/الإيقاف (Toggle) لكل منافس
✅ تسجيل الأخطاء والحالات (Logging)
✅ ضمان عدم تداخل البيانات بين المنافسين
"""

import asyncio
import logging
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
import pandas as pd
import traceback

# إعداد السجلات (Logging)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ConcurrentScraper")


class CompetitorStatus(Enum):
    """حالات المنافس أثناء الكشط"""
    ACTIVE = "🟢 نشط"
    DISABLED = "⚫ معطل"
    RUNNING = "⏳ جاري الكشط"
    SUCCESS = "✅ نجح"
    ERROR = "❌ خطأ"
    TIMEOUT = "⏱️ انتهت المهلة الزمنية"
    SKIPPED = "⏭️ تم التخطي"


@dataclass
class CompetitorConfig:
    """إعدادات المنافس الواحد"""
    id: str                    # معرف فريد للمنافس
    name: str                  # اسم المنافس
    url: str                   # رابط الموقع
    enabled: bool = True       # هل يتم كشط هذا المنافس؟
    timeout: int = 30          # مهلة زمنية بالثواني
    retries: int = 2           # عدد محاولات إعادة المحاولة
    priority: int = 0          # الأولوية (أعلى = أولاً)
    custom_headers: Dict = None # رؤوس مخصصة
    
    def __post_init__(self):
        if self.custom_headers is None:
            self.custom_headers = {}


@dataclass
class ScrapingResult:
    """نتيجة الكشط لمنافس واحد"""
    competitor_id: str
    competitor_name: str
    status: CompetitorStatus
    data: Optional[pd.DataFrame] = None
    error_message: Optional[str] = None
    timestamp: str = None
    duration_seconds: float = 0.0
    items_count: int = 0
    retry_count: int = 0
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
    
    def to_dict(self):
        return {
            "competitor_id": self.competitor_id,
            "competitor_name": self.competitor_name,
            "status": self.status.value,
            "items_count": self.items_count,
            "duration_seconds": self.duration_seconds,
            "timestamp": self.timestamp,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
        }


class ConcurrentScraperEngine:
    """
    محرك الكشط المتزامن الرئيسي
    يدير كشط عدة منافسين بشكل متوازي مع معالجة الأخطاء والتخطي الذكي
    """
    
    def __init__(self, max_concurrent_tasks: int = 3, log_file: str = None):
        """
        Args:
            max_concurrent_tasks: عدد المهام المتزامنة القصوى
            log_file: مسار ملف السجل (اختياري)
        """
        self.max_concurrent_tasks = max_concurrent_tasks
        self.competitors: Dict[str, CompetitorConfig] = {}
        self.results: Dict[str, ScrapingResult] = {}
        self.log_file = log_file or "scraping_log.json"
        self.is_running = False
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        
    def register_competitor(self, config: CompetitorConfig) -> None:
        """تسجيل منافس جديد"""
        self.competitors[config.id] = config
        logger.info(f"تم تسجيل المنافس: {config.name} (ID: {config.id})")
    
    def register_competitors(self, configs: List[CompetitorConfig]) -> None:
        """تسجيل عدة منافسين"""
        for config in configs:
            self.register_competitor(config)
    
    def toggle_competitor(self, competitor_id: str, enabled: bool) -> bool:
        """
        تفعيل أو تعطيل كشط منافس معين
        
        Args:
            competitor_id: معرف المنافس
            enabled: هل يتم تفعيله؟
        
        Returns:
            True إذا نجحت العملية
        """
        if competitor_id not in self.competitors:
            logger.warning(f"المنافس غير موجود: {competitor_id}")
            return False
        
        self.competitors[competitor_id].enabled = enabled
        status = "✅ مفعل" if enabled else "⚫ معطل"
        logger.info(f"تم تغيير حالة {self.competitors[competitor_id].name} إلى {status}")
        return True
    
    def get_competitor_status(self, competitor_id: str) -> Optional[Dict]:
        """الحصول على حالة منافس معين"""
        if competitor_id not in self.results:
            return None
        
        result = self.results[competitor_id]
        return result.to_dict()
    
    def get_all_statuses(self) -> List[Dict]:
        """الحصول على حالات جميع المنافسين"""
        return [result.to_dict() for result in self.results.values()]
    
    async def _scrape_single_competitor(
        self, 
        config: CompetitorConfig,
        scraper_func: callable
    ) -> ScrapingResult:
        """
        كشط منافس واحد مع معالجة الأخطاء والتخطي الذكي
        
        Args:
            config: إعدادات المنافس
            scraper_func: دالة الكشط المخصصة
        
        Returns:
            نتيجة الكشط
        """
        async with self.semaphore:
            start_time = time.time()
            retry_count = 0
            
            # إذا كان المنافس معطل، تخطيه
            if not config.enabled:
                logger.info(f"تخطي المنافس المعطل: {config.name}")
                return ScrapingResult(
                    competitor_id=config.id,
                    competitor_name=config.name,
                    status=CompetitorStatus.DISABLED,
                    duration_seconds=0.0
                )
            
            # محاولة الكشط مع إعادة المحاولة
            while retry_count <= config.retries:
                try:
                    logger.info(f"🔄 جاري كشط {config.name} (محاولة {retry_count + 1}/{config.retries + 1})")
                    
                    # تنفيذ دالة الكشط مع مهلة زمنية
                    try:
                        data = await asyncio.wait_for(
                            scraper_func(config),
                            timeout=config.timeout
                        )
                    except asyncio.TimeoutError:
                        logger.warning(f"⏱️ انتهت المهلة الزمنية للمنافس: {config.name}")
                        if retry_count < config.retries:
                            retry_count += 1
                            await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                            continue
                        else:
                            duration = time.time() - start_time
                            return ScrapingResult(
                                competitor_id=config.id,
                                competitor_name=config.name,
                                status=CompetitorStatus.TIMEOUT,
                                error_message=f"انتهت المهلة الزمنية بعد {config.timeout} ثانية",
                                duration_seconds=duration,
                                retry_count=retry_count
                            )
                    
                    # التحقق من صحة البيانات
                    if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                        logger.warning(f"⚠️ لا توجد بيانات من {config.name}")
                        retry_count += 1
                        if retry_count <= config.retries:
                            await asyncio.sleep(2 ** retry_count)
                            continue
                        else:
                            duration = time.time() - start_time
                            return ScrapingResult(
                                competitor_id=config.id,
                                competitor_name=config.name,
                                status=CompetitorStatus.ERROR,
                                error_message="لا توجد بيانات صالحة من الموقع",
                                duration_seconds=duration,
                                retry_count=retry_count
                            )
                    
                    # نجاح الكشط
                    duration = time.time() - start_time
                    items_count = len(data) if isinstance(data, pd.DataFrame) else 0
                    
                    logger.info(f"✅ تم كشط {config.name} بنجاح ({items_count} منتج في {duration:.2f}ث)")
                    
                    return ScrapingResult(
                        competitor_id=config.id,
                        competitor_name=config.name,
                        status=CompetitorStatus.SUCCESS,
                        data=data,
                        duration_seconds=duration,
                        items_count=items_count,
                        retry_count=retry_count
                    )
                
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"❌ خطأ في كشط {config.name}: {error_msg}")
                    logger.debug(traceback.format_exc())
                    
                    retry_count += 1
                    if retry_count <= config.retries:
                        wait_time = 2 ** retry_count
                        logger.info(f"⏳ إعادة محاولة بعد {wait_time} ثانية...")
                        await asyncio.sleep(wait_time)
                    else:
                        duration = time.time() - start_time
                        return ScrapingResult(
                            competitor_id=config.id,
                            competitor_name=config.name,
                            status=CompetitorStatus.ERROR,
                            error_message=error_msg,
                            duration_seconds=duration,
                            retry_count=retry_count
                        )
            
            # إذا انتهت جميع المحاولات بفشل
            duration = time.time() - start_time
            return ScrapingResult(
                competitor_id=config.id,
                competitor_name=config.name,
                status=CompetitorStatus.ERROR,
                error_message="فشلت جميع محاولات الكشط",
                duration_seconds=duration,
                retry_count=retry_count
            )
    
    async def run_all_scrapers(
        self, 
        scraper_func: callable,
        sort_by_priority: bool = True
    ) -> Dict[str, ScrapingResult]:
        """
        تشغيل كشط جميع المنافسين بشكل متزامن
        
        Args:
            scraper_func: دالة الكشط المخصصة
            sort_by_priority: هل يتم الترتيب حسب الأولوية؟
        
        Returns:
            قاموس بنتائج الكشط
        """
        self.is_running = True
        logger.info(f"🚀 بدء كشط {len(self.competitors)} منافس...")
        
        # ترتيب المنافسين حسب الأولوية
        competitors_list = list(self.competitors.values())
        if sort_by_priority:
            competitors_list.sort(key=lambda c: c.priority, reverse=True)
        
        # إنشاء مهام متزامنة
        tasks = [
            self._scrape_single_competitor(config, scraper_func)
            for config in competitors_list
        ]
        
        # تنفيذ المهام
        results = await asyncio.gather(*tasks, return_exceptions=False)
        
        # تخزين النتائج
        self.results = {result.competitor_id: result for result in results}
        
        self.is_running = False
        logger.info("✅ انتهى الكشط المتزامن")
        
        # حفظ السجل
        self._save_log()
        
        return self.results
    
    def run_scrapers_sync(
        self, 
        scraper_func: callable,
        sort_by_priority: bool = True
    ) -> Dict[str, ScrapingResult]:
        """
        تشغيل الكشط المتزامن من سياق متزامن (Sync context)
        
        Args:
            scraper_func: دالة الكشط المخصصة
            sort_by_priority: هل يتم الترتيب حسب الأولوية؟
        
        Returns:
            قاموس بنتائج الكشط
        """
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        return loop.run_until_complete(
            self.run_all_scrapers(scraper_func, sort_by_priority)
        )
    
    def get_successful_data(self) -> pd.DataFrame:
        """
        الحصول على جميع البيانات الناجحة من جميع المنافسين
        مع ضمان عدم تداخل البيانات
        
        Returns:
            DataFrame موحد بجميع البيانات
        """
        all_data = []
        
        for result in self.results.values():
            if result.status == CompetitorStatus.SUCCESS and result.data is not None:
                # إضافة معرف المنافس لضمان التتبع
                df = result.data.copy()
                df['_competitor_source'] = result.competitor_id
                df['_competitor_name'] = result.competitor_name
                df['_scrape_timestamp'] = result.timestamp
                all_data.append(df)
        
        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            logger.info(f"✅ تم دمج {len(all_data)} مصدر بـ {len(combined)} منتج")
            return combined
        else:
            logger.warning("⚠️ لا توجد بيانات ناجحة للدمج")
            return pd.DataFrame()
    
    def get_error_summary(self) -> Dict[str, Any]:
        """الحصول على ملخص الأخطاء والحالات"""
        summary = {
            "total_competitors": len(self.competitors),
            "successful": 0,
            "failed": 0,
            "disabled": 0,
            "timeout": 0,
            "details": []
        }
        
        for result in self.results.values():
            if result.status == CompetitorStatus.SUCCESS:
                summary["successful"] += 1
            elif result.status == CompetitorStatus.DISABLED:
                summary["disabled"] += 1
            elif result.status == CompetitorStatus.TIMEOUT:
                summary["timeout"] += 1
            else:
                summary["failed"] += 1
            
            summary["details"].append(result.to_dict())
        
        return summary
    
    def _save_log(self) -> None:
        """حفظ سجل الكشط إلى ملف JSON"""
        try:
            log_data = {
                "timestamp": datetime.now().isoformat(),
                "summary": self.get_error_summary(),
                "results": [result.to_dict() for result in self.results.values()]
            }
            
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"📝 تم حفظ السجل في {self.log_file}")
        except Exception as e:
            logger.error(f"❌ فشل حفظ السجل: {str(e)}")
    
    def export_results_to_excel(self, output_path: str) -> bool:
        """
        تصدير نتائج الكشط إلى ملف Excel
        
        Args:
            output_path: مسار الملف الناتج
        
        Returns:
            True إذا نجحت العملية
        """
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # ورقة البيانات المدمجة
                combined_data = self.get_successful_data()
                if not combined_data.empty:
                    combined_data.to_excel(writer, sheet_name='البيانات المدمجة', index=False)
                
                # ورقة ملخص الحالات
                summary_df = pd.DataFrame([result.to_dict() for result in self.results.values()])
                summary_df.to_excel(writer, sheet_name='ملخص الحالات', index=False)
            
            logger.info(f"✅ تم تصدير النتائج إلى {output_path}")
            return True
        except Exception as e:
            logger.error(f"❌ فشل التصدير: {str(e)}")
            return False


# دوال مساعدة
def create_default_competitors() -> List[CompetitorConfig]:
    """إنشاء قائمة افتراضية من المنافسين"""
    return [
        CompetitorConfig(
            id="competitor_1",
            name="المنافس الأول",
            url="https://example1.com",
            enabled=True,
            priority=1
        ),
        CompetitorConfig(
            id="competitor_2",
            name="المنافس الثاني",
            url="https://example2.com",
            enabled=True,
            priority=2
        ),
    ]
