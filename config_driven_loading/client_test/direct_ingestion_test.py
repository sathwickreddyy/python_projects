import json
from decimal import Decimal

from client_test.db_utils import DatabaseManager
from client_test.ingestion_utils import IngestionRunner


def direct_ingestion_test():
    engine = DatabaseManager.create_engine()

    try:
        with IngestionRunner(engine, config_path="data-sources.yaml") as runner:
            source_config_name = "user_profile_json_direct"

            if source_config_name not in runner.get_available_sources_configs_in_yaml()["sources"]:
                return {
                    "success": False,
                    "message": "Source configuration not found",
                    "source_config_name": source_config_name
                }

            loading_result = runner.run_single_source(source_config_name)

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
    result = direct_ingestion_test()
    print(json.dumps(result, indent=2, default=json_default_encoder))
    exit(0 if result.get("success") else 1)
