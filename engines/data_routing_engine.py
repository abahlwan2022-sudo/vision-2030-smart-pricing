"""
engines/data_routing_engine.py — محرك توزيع البيانات الآمن v1.0
✅ ضمان سلامة فرز وتوزيع البيانات عبر خوارزمية المطابقة متعددة المراحل
✅ عزل تام للبيانات بين المنافسين
✅ معالجة الأخطاء والتحقق من الصحة
✅ تسجيل كامل لعملية التوزيع
"""

import logging
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

logger = logging.getLogger("DataRoutingEngine")


class RoutingDecision(Enum):
    """قرارات التوزيع النهائية"""
    MATCHED = "✅ تم المطابقة"
    MISSING = "🔍 منتج مفقود"
    REVIEW = "⚠️ تحت المراجعة"
    EXCLUDED = "⚪ مستبعد"
    ERROR = "❌ خطأ في المعالجة"


@dataclass
class RoutingLog:
    """سجل عملية التوزيع الواحدة"""
    product_id: str
    product_name: str
    source_competitor: str
    decision: RoutingDecision
    match_score: float = 0.0
    matched_product: Optional[str] = None
    reason: str = ""
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
    
    def to_dict(self):
        return {
            "product_id": self.product_id,
            "product_name": self.product_name,
            "source_competitor": self.source_competitor,
            "decision": self.decision.value,
            "match_score": self.match_score,
            "matched_product": self.matched_product,
            "reason": self.reason,
            "timestamp": self.timestamp,
        }


class DataRoutingEngine:
    """
    محرك توزيع البيانات الآمن
    يضمن عدم تداخل البيانات وتوزيعها بشكل صحيح عبر خوارزمية المطابقة
    """
    
    def __init__(self, match_engine=None):
        """
        Args:
            match_engine: محرك المطابقة (يجب أن يحتوي على دالة calculate_match_score)
        """
        self.match_engine = match_engine
        self.routing_logs: List[RoutingLog] = []
        self.matched_products: Dict[str, Dict] = {}
        self.missing_products: Dict[str, Dict] = {}
        self.review_products: Dict[str, Dict] = {}
        self.excluded_products: Dict[str, Dict] = {}
    
    def validate_data_isolation(
        self, 
        competitor_data: pd.DataFrame,
        competitor_id: str
    ) -> Tuple[bool, List[str]]:
        """
        التحقق من عزل البيانات (عدم وجود بيانات مكررة من مصادر أخرى)
        
        Args:
            competitor_data: بيانات المنافس
            competitor_id: معرف المنافس
        
        Returns:
            (صحيح/خطأ، قائمة الأخطاء)
        """
        issues = []
        
        # التحقق من وجود عمود معرف المصدر
        if '_competitor_source' in competitor_data.columns:
            invalid_sources = competitor_data[
                competitor_data['_competitor_source'] != competitor_id
            ]
            if not invalid_sources.empty:
                issues.append(
                    f"⚠️ وجدت {len(invalid_sources)} صف من مصادر أخرى (تسرب بيانات)"
                )
        
        # التحقق من تكرار المنتجات
        name_col = self._detect_product_name_column(competitor_data)
        if name_col:
            duplicates = competitor_data[name_col].duplicated().sum()
            if duplicates > 0:
                issues.append(f"⚠️ وجدت {duplicates} منتج مكرر من نفس المنافس")
        
        return len(issues) == 0, issues
    
    def route_competitor_data(
        self,
        competitor_data: pd.DataFrame,
        competitor_id: str,
        competitor_name: str,
        our_catalog: pd.DataFrame,
        match_threshold: float = 85.0,
        review_threshold: float = 70.0
    ) -> Dict[str, Any]:
        """
        توزيع بيانات منافس واحد عبر خوارزمية المطابقة متعددة المراحل
        
        Args:
            competitor_data: بيانات المنافس
            competitor_id: معرف المنافس
            competitor_name: اسم المنافس
            our_catalog: الكتالوج الأساسي لدينا
            match_threshold: حد المطابقة القوية
            review_threshold: حد المراجعة
        
        Returns:
            قاموس بنتائج التوزيع
        """
        logger.info(f"🔄 جاري توزيع بيانات {competitor_name}...")
        
        # التحقق من عزل البيانات
        is_isolated, isolation_issues = self.validate_data_isolation(
            competitor_data, 
            competitor_id
        )
        
        if not is_isolated:
            logger.warning(f"⚠️ مشاكل في عزل البيانات: {isolation_issues}")
        
        # تنظيف السجلات السابقة لهذا المنافس
        self.routing_logs = [
            log for log in self.routing_logs 
            if log.source_competitor != competitor_id
        ]
        
        results = {
            "competitor_id": competitor_id,
            "competitor_name": competitor_name,
            "total_products": len(competitor_data),
            "matched": [],
            "missing": [],
            "review": [],
            "excluded": [],
            "errors": [],
            "logs": []
        }
        
        # معالجة كل منتج
        for idx, comp_row in competitor_data.iterrows():
            try:
                product_name = self._extract_product_name(comp_row)
                product_id = f"{competitor_id}_{idx}"
                
                if not product_name:
                    log = RoutingLog(
                        product_id=product_id,
                        product_name="[بدون اسم]",
                        source_competitor=competitor_id,
                        decision=RoutingDecision.ERROR,
                        reason="اسم المنتج فارغ"
                    )
                    self.routing_logs.append(log)
                    results["errors"].append(log.to_dict())
                    continue
                
                # البحث عن أفضل مطابقة في الكتالوج الأساسي
                best_match, best_score = self._find_best_match(
                    product_name,
                    our_catalog
                )
                
                # اتخاذ القرار بناءً على نسبة المطابقة
                if best_score >= match_threshold:
                    # مطابقة قوية
                    decision = RoutingDecision.MATCHED
                    self.matched_products[product_id] = {
                        "source": competitor_id,
                        "product_name": product_name,
                        "matched_with": best_match,
                        "score": best_score,
                        "data": comp_row.to_dict()
                    }
                    results["matched"].append(product_id)
                
                elif best_score >= review_threshold:
                    # تحتاج مراجعة
                    decision = RoutingDecision.REVIEW
                    self.review_products[product_id] = {
                        "source": competitor_id,
                        "product_name": product_name,
                        "potential_match": best_match,
                        "score": best_score,
                        "data": comp_row.to_dict()
                    }
                    results["review"].append(product_id)
                
                else:
                    # لا يوجد تطابق -> منتج مفقود
                    decision = RoutingDecision.MISSING
                    self.missing_products[product_id] = {
                        "source": competitor_id,
                        "product_name": product_name,
                        "data": comp_row.to_dict()
                    }
                    results["missing"].append(product_id)
                
                # تسجيل القرار
                log = RoutingLog(
                    product_id=product_id,
                    product_name=product_name,
                    source_competitor=competitor_id,
                    decision=decision,
                    match_score=best_score,
                    matched_product=best_match,
                    reason=f"نسبة المطابقة: {best_score:.1f}%"
                )
                self.routing_logs.append(log)
                results["logs"].append(log.to_dict())
                
                logger.debug(f"  {decision.value} - {product_name} (النسبة: {best_score:.1f}%)")
            
            except Exception as e:
                logger.error(f"❌ خطأ في معالجة المنتج {idx}: {str(e)}")
                results["errors"].append({
                    "index": idx,
                    "error": str(e)
                })
        
        # ملخص النتائج
        logger.info(
            f"✅ انتهى توزيع {competitor_name}: "
            f"{len(results['matched'])} مطابق، "
            f"{len(results['review'])} للمراجعة، "
            f"{len(results['missing'])} مفقود"
        )
        
        return results
    
    def route_all_competitors(
        self,
        all_competitors_data: pd.DataFrame,
        our_catalog: pd.DataFrame,
        competitor_source_column: str = "_competitor_source",
        competitor_name_column: str = "_competitor_name",
        match_threshold: float = 85.0,
        review_threshold: float = 70.0
    ) -> Dict[str, Any]:
        """
        توزيع بيانات جميع المنافسين
        
        Args:
            all_competitors_data: بيانات جميع المنافسين المدمجة
            our_catalog: الكتالوج الأساسي
            competitor_source_column: اسم عمود معرف المنافس
            competitor_name_column: اسم عمود اسم المنافس
            match_threshold: حد المطابقة
            review_threshold: حد المراجعة
        
        Returns:
            قاموس بنتائج التوزيع الكاملة
        """
        logger.info("🚀 بدء توزيع بيانات جميع المنافسين...")
        
        all_results = {
            "timestamp": datetime.now().isoformat(),
            "total_competitors": 0,
            "total_products": len(all_competitors_data),
            "competitors": {},
            "summary": {
                "total_matched": 0,
                "total_missing": 0,
                "total_review": 0,
                "total_errors": 0
            }
        }
        
        # تجميع البيانات حسب المنافس
        if competitor_source_column not in all_competitors_data.columns:
            logger.error(f"❌ عمود معرف المنافس غير موجود: {competitor_source_column}")
            return all_results
        
        competitors = all_competitors_data[competitor_source_column].unique()
        all_results["total_competitors"] = len(competitors)
        
        # معالجة كل منافس
        for competitor_id in competitors:
            competitor_data = all_competitors_data[
                all_competitors_data[competitor_source_column] == competitor_id
            ].copy()
            
            competitor_name = competitor_data[competitor_name_column].iloc[0] \
                if competitor_name_column in competitor_data.columns else competitor_id
            
            # توزيع بيانات المنافس
            routing_result = self.route_competitor_data(
                competitor_data,
                competitor_id,
                competitor_name,
                our_catalog,
                match_threshold,
                review_threshold
            )
            
            all_results["competitors"][competitor_id] = routing_result
            
            # تحديث الملخص
            all_results["summary"]["total_matched"] += len(routing_result["matched"])
            all_results["summary"]["total_missing"] += len(routing_result["missing"])
            all_results["summary"]["total_review"] += len(routing_result["review"])
            all_results["summary"]["total_errors"] += len(routing_result["errors"])
        
        logger.info(
            f"✅ انتهى التوزيع الكامل: "
            f"{all_results['summary']['total_matched']} مطابق، "
            f"{all_results['summary']['total_missing']} مفقود، "
            f"{all_results['summary']['total_review']} للمراجعة"
        )
        
        return all_results
    
    def get_matched_dataframe(self) -> pd.DataFrame:
        """الحصول على DataFrame للمنتجات المطابقة"""
        if not self.matched_products:
            return pd.DataFrame()
        
        data = []
        for product_id, product_info in self.matched_products.items():
            row = product_info["data"].copy()
            row["_routing_decision"] = RoutingDecision.MATCHED.value
            row["_match_score"] = product_info["score"]
            row["_matched_with"] = product_info["matched_with"]
            data.append(row)
        
        return pd.DataFrame(data) if data else pd.DataFrame()
    
    def get_missing_dataframe(self) -> pd.DataFrame:
        """الحصول على DataFrame للمنتجات المفقودة"""
        if not self.missing_products:
            return pd.DataFrame()
        
        data = []
        for product_id, product_info in self.missing_products.items():
            row = product_info["data"].copy()
            row["_routing_decision"] = RoutingDecision.MISSING.value
            data.append(row)
        
        return pd.DataFrame(data) if data else pd.DataFrame()
    
    def get_review_dataframe(self) -> pd.DataFrame:
        """الحصول على DataFrame للمنتجات تحت المراجعة"""
        if not self.review_products:
            return pd.DataFrame()
        
        data = []
        for product_id, product_info in self.review_products.items():
            row = product_info["data"].copy()
            row["_routing_decision"] = RoutingDecision.REVIEW.value
            row["_match_score"] = product_info["score"]
            row["_potential_match"] = product_info["potential_match"]
            data.append(row)
        
        return pd.DataFrame(data) if data else pd.DataFrame()
    
    def export_routing_report(self, output_path: str) -> bool:
        """تصدير تقرير التوزيع الكامل"""
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # المنتجات المطابقة
                matched_df = self.get_matched_dataframe()
                if not matched_df.empty:
                    matched_df.to_excel(writer, sheet_name='المطابقات', index=False)
                
                # المنتجات المفقودة
                missing_df = self.get_missing_dataframe()
                if not missing_df.empty:
                    missing_df.to_excel(writer, sheet_name='المفقودات', index=False)
                
                # المنتجات تحت المراجعة
                review_df = self.get_review_dataframe()
                if not review_df.empty:
                    review_df.to_excel(writer, sheet_name='المراجعة', index=False)
                
                # السجلات
                logs_df = pd.DataFrame([log.to_dict() for log in self.routing_logs])
                if not logs_df.empty:
                    logs_df.to_excel(writer, sheet_name='السجلات', index=False)
            
            logger.info(f"✅ تم تصدير التقرير إلى {output_path}")
            return True
        except Exception as e:
            logger.error(f"❌ فشل التصدير: {str(e)}")
            return False
    
    # ─── دوال مساعدة خاصة ───
    
    def _detect_product_name_column(self, df: pd.DataFrame) -> Optional[str]:
        """اكتشاف عمود اسم المنتج"""
        possible_columns = [
            "منتج_المنافس", "المنتج", "اسم المنتج", "Product", "Name",
            "product_name", "product", "name"
        ]
        for col in possible_columns:
            if col in df.columns:
                return col
        return None
    
    def _extract_product_name(self, row: pd.Series) -> str:
        """استخراج اسم المنتج من صف"""
        name_col = self._detect_product_name_column(row.to_frame().T)
        if name_col:
            return str(row.get(name_col, "")).strip()
        return ""
    
    def _find_best_match(
        self, 
        product_name: str, 
        catalog: pd.DataFrame
    ) -> Tuple[Optional[str], float]:
        """
        البحث عن أفضل مطابقة في الكتالوج
        
        Returns:
            (اسم المنتج المطابق، نسبة المطابقة)
        """
        if self.match_engine is None or catalog.empty:
            return None, 0.0
        
        best_match = None
        best_score = 0.0
        
        catalog_name_col = self._detect_product_name_column(catalog)
        if not catalog_name_col:
            return None, 0.0
        
        for _, catalog_row in catalog.iterrows():
            catalog_name = str(catalog_row.get(catalog_name_col, "")).strip()
            if not catalog_name:
                continue
            
            # استخدام محرك المطابقة إن وجد
            if hasattr(self.match_engine, 'calculate_match_score'):
                score = self.match_engine.calculate_match_score(
                    product_name, 
                    catalog_name
                )
            else:
                # مطابقة بسيطة كبديل
                from rapidfuzz import fuzz
                score = fuzz.token_set_ratio(product_name, catalog_name)
            
            if score > best_score:
                best_score = score
                best_match = catalog_name
        
        return best_match, best_score
