"""
تشغيل التحليل والمطابقة في خيط خلفي — مشترك بين app.py وصفحات الرفع.
يستدعي run_full_analysis (ومعها كاش match_cache_v21.db عبر محرك المطابقة).
"""
from __future__ import annotations

import os
import traceback

import pandas as pd

from engines.engine import run_full_analysis, smart_missing_barrier
from engines.reconciliation_engine import (
    merge_reconciliation_into_audit,
    reconcile_competitor_upload,
)
from utils.data_helpers import (
    merge_missing_products_dataframes,
    merge_price_analysis_dataframes,
    restore_results_from_json,
    safe_results_for_json,
)
from utils.helpers import safe_float
from utils.db_manager import log_analysis, save_job_progress, upsert_price_history


def run_analysis_background_job(
    job_id: str,
    our_df: pd.DataFrame,
    comp_dfs: dict,
    our_file_name: str,
    comp_names: str,
    merge_previous: bool = False,
    prev_analysis_records: list | None = None,
    prev_missing_records: list | None = None,
) -> None:
    """تعمل في thread منفصل — تحفظ النتائج كل 25 منتجاً مع حماية من الأخطاء."""
    total = len(our_df)
    processed = 0
    _last_save = [0]

    def progress_cb(pct, current_results):
        nonlocal processed
        processed = int(pct * total)
        if processed - _last_save[0] >= 25 or processed >= total:
            _last_save[0] = processed
            try:
                safe_res = safe_results_for_json(current_results)
                save_job_progress(
                    job_id, total, processed,
                    safe_res,
                    "running",
                    our_file_name, comp_names,
                )
            except Exception:
                traceback.print_exc()

    analysis_df = pd.DataFrame()
    missing_df = pd.DataFrame()
    audit_stats: dict = {}

    try:
        analysis_df, audit_stats = run_full_analysis(
            our_df, comp_dfs,
            progress_callback=progress_cb,
        )
    except Exception as e:
        traceback.print_exc()
        save_job_progress(
            job_id, total, processed,
            [], f"error: تحليل المقارنة فشل — {str(e)[:200]}",
            our_file_name, comp_names,
        )
        return

    try:
        for _, row in analysis_df.iterrows():
            if safe_float(row.get("نسبة_التطابق", 0)) > 0:
                upsert_price_history(
                    str(row.get("المنتج", "")),
                    str(row.get("المنافس", "")),
                    safe_float(row.get("سعر_المنافس", 0)),
                    safe_float(row.get("السعر", 0)),
                    safe_float(row.get("الفرق", 0)),
                    safe_float(row.get("نسبة_التطابق", 0)),
                    str(row.get("القرار", "")),
                )
    except Exception:
        pass

    rec = None
    missing_df = pd.DataFrame()
    try:
        rec = reconcile_competitor_upload(our_df, comp_dfs)
        missing_df = smart_missing_barrier(rec.new_products_df, our_df)
        rec.apply_smart_barrier_adjustment(missing_df)
        audit_stats = merge_reconciliation_into_audit(audit_stats, rec)
        if rec.failed_df is not None and not rec.failed_df.empty:
            data_dir = os.environ.get("DATA_DIR", "data")
            os.makedirs(data_dir, exist_ok=True)
            fp = os.path.join(data_dir, f"failed_rows_{job_id}.csv")
            try:
                rec.failed_df.to_csv(fp, index=False, encoding="utf-8-sig")
                audit_stats["reconciliation_failed_csv_path"] = fp
            except Exception:
                traceback.print_exc()
    except Exception:
        traceback.print_exc()
        missing_df = pd.DataFrame()
        try:
            from engines.engine import find_missing_products

            raw_missing_df = find_missing_products(our_df, comp_dfs)
            missing_df = smart_missing_barrier(raw_missing_df, our_df)
        except Exception:
            traceback.print_exc()

    if merge_previous and prev_analysis_records:
        try:
            prev_adf = pd.DataFrame(restore_results_from_json(prev_analysis_records))
            if not prev_adf.empty:
                analysis_df = merge_price_analysis_dataframes(prev_adf, analysis_df)
        except Exception:
            traceback.print_exc()
    if merge_previous and prev_missing_records:
        try:
            prev_m = pd.DataFrame(prev_missing_records)
            if not prev_m.empty:
                missing_df = merge_missing_products_dataframes(prev_m, missing_df)
        except Exception:
            traceback.print_exc()

    try:
        safe_records = safe_results_for_json(analysis_df.to_dict("records"))
        safe_missing = missing_df.to_dict("records") if not missing_df.empty else []

        save_job_progress(
            job_id, total, total,
            safe_records,
            "done",
            our_file_name, comp_names,
            missing=safe_missing,
            audit_stats=audit_stats,
        )
        log_analysis(
            our_file_name, comp_names, total,
            int((analysis_df.get("نسبة_التطابق", pd.Series(dtype=float)) > 0).sum()),
            len(missing_df),
        )
    except Exception as e:
        traceback.print_exc()
        try:
            save_job_progress(
                job_id, total, total,
                safe_results_for_json(analysis_df.to_dict("records")),
                "done",
                our_file_name, comp_names,
                missing=[],
                audit_stats=audit_stats,
            )
        except Exception:
            save_job_progress(
                job_id, total, processed,
                [], f"error: فشل الحفظ النهائي — {str(e)[:200]}",
                our_file_name, comp_names,
            )
