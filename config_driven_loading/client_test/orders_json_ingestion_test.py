import json
from decimal import Decimal

from db_utils import DatabaseManager
from ingestion_utils import IngestionRunner


def order_details_ingestion() -> dict:
    """Test order details ingestion with detailed structured response."""
    engine = DatabaseManager.create_engine()

    try:
        with IngestionRunner(engine, config_path="data-sources.yaml") as runner:
            available_sources_response = runner.get_available_sources()

            if not available_sources_response["success"]:
                return {
                    "success": False,
                    "error": "Failed to fetch available sources",
                    "details": available_sources_response.get("error"),
                    "step": "get_available_sources"
                }

            available_sources = available_sources_response["sources"]
            source_name = "order_details_json"

            if source_name not in available_sources:
                return {
                    "success": False,
                    "error": f"Source '{source_name}' not found in configuration",
                    "available_sources": available_sources,
                    "step": "source_validation"
                }

            # Run ingestion
            loading_result = runner.run_single_source(source_name)

            ingestion_summary = {
                "total_records": loading_result.get("total_records"),
                "successful_records": loading_result.get("successful_records"),
                "failed_records": loading_result.get("failed_records"),
                "duration_ms": loading_result.get("write_duration_ms"),
                "throughput": loading_result.get("throughput_records_per_sec"),
                "errors": loading_result.get("errors"),
                "error_summary": loading_result.get("error_summary"),
                "status": loading_result.get("status"),
            }

            return {
                "success": loading_result.get("success"),
                "partial_success": loading_result.get("partial_success"),
                "ingestion_summary": ingestion_summary,
            }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "step": "exception"
        }
    finally:
        engine.dispose()


def json_default_encoder(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# Usage in your main block:
if __name__ == "__main__":
    result = order_details_ingestion()
    print(json.dumps(result, indent=2, default=json_default_encoder))
    exit(0 if result.get("success") else 1)