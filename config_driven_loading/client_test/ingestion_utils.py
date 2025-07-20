# ingestion_utils.py (Refactored)
from typing import Dict, Any, List, Optional
from sqlalchemy import Engine
from client.data_ingestion_client import DataIngestionClient
from db_utils import DatabaseManager


class IngestionRunner:
    """Wrapper for common ingestion operations with standardized reporting via response dicts."""

    def __init__(self, engine: Engine, config_path: str):
        self.engine = engine
        self.config_path = config_path
        self.client = None

    def __enter__(self):
        self.client = DataIngestionClient(
            engine=self.engine,
            config_path=self.config_path,
            log_level="INFO"
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.close()

    def run_single_source(self, source_name: str) -> Dict[str, Any]:
        try:
            stats = self.client.execute_data_source(source_name)
            return self._build_execution_summary(stats)
        except Exception as exception:
            return {
                "source": source_name,
                "success": False,
                "error": str(exception),
                "details": None
            }

    def run_multiple_sources(self, source_names: List[str]) -> Dict[str, Any]:
        results = {}

        for source_name in source_names:
            results[source_name] = self.run_single_source(source_name)

        summary = self._build_overall_summary(results)

        return {
            "results": results,
            "summary": summary
        }

    def validate_config(self) -> Dict[str, Any]:
        try:
            validation = self.client.validate_configuration()
            return {
                "success": validation.get("valid", False),
                "details": validation
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "details": None
            }

    def get_available_sources(self) -> Dict[str, Any]:
        try:
            sources = self.client.get_available_sources()
            return {
                "success": True,
                "sources": sources
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "sources": []
            }

    def _build_execution_summary(self, stats) -> Dict[str, Any]:
        """Prepare structured ingestion result."""
        success = stats.error_records == 0
        partial = 0 < stats.error_records < stats.total_records

        response = {
            "total_records": stats.total_records,
            "successful_records": stats.successful_records,
            "failed_records": stats.error_records,
            "write_duration_ms": stats.write_time_ms,
            "throughput_records_per_sec": round(stats.records_per_second, 2),
            "average_time_per_record_ms": round(stats.write_time_ms / max(stats.total_records, 1), 1),
            "batches": stats.batch_count,
            "errors": stats.get_all_errors()[:10],
            "success": success,
            "partial_success": partial,
            "status": "SUCCESS" if success else "PARTIAL_SUCCESS" if partial else "FAILED"
        }

        if len(stats.get_all_errors()) > 10:
            response["error_summary"] = f"... {len(stats.get_all_errors()) - 10} more errors not shown"

        return response

    def _build_overall_summary(self, results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        successful_sources = sum(1 for r in results.values() if r.get("success"))
        total_sources = len(results)
        total_records = sum(r.get("total_records", 0) for r in results.values())
        successful_records = sum(r.get("successful_records", 0) for r in results.values())

        return {
            "sources_processed": total_sources,
            "sources_successful": successful_sources,
            "total_records": total_records,
            "successful_records": successful_records,
            "success_rate_percent": round((successful_records / total_records * 100), 1) if total_records > 0 else 0.0,
        }