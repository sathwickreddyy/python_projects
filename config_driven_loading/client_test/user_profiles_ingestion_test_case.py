import json

from client_test.db_utils import DatabaseManager
from client_test.ingestion_utils import IngestionRunner


def user_profiles_ingestion() -> dict:
    """Test user profiles ingestion with detailed structured response."""
    engine = DatabaseManager.create_engine()

    try:
        with IngestionRunner(engine, config_path="data-sources.yaml") as runner:
            available_sources_response = runner.get_available_sources_configs_in_yaml()

            if not available_sources_response["success"]:
                return {
                    "success": False,
                    "error": "Failed to fetch available sources",
                    "details": available_sources_response.get("error"),
                    "step": "get_available_sources"
                }

            available_sources = available_sources_response["sources"]
            source_name = "user_profile_json"

            if source_name not in available_sources:
                return {
                    "success": False,
                    "error": f"Source '{source_name}' not found in configuration",
                    "available_sources": available_sources,
                    "step": "source_validation"
                }

            # Run ingestion
            load_result = runner.run_single_source(source_name)

            # Build response
            ingestion_summary = {
                "total_records": load_result.get("total_records"),
                "successful_records": load_result.get("successful_records"),
                "failed_records": load_result.get("failed_records"),
                "duration_ms": load_result.get("write_duration_ms"),
                "throughput": load_result.get("throughput_records_per_sec"),
                "errors": load_result.get("errors"),
                "error_summary": load_result.get("error_summary"),
                "status": load_result.get("status"),
            }

            return {
                "success": load_result.get("success"),
                "partial_success": load_result.get("partial_success"),
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


if __name__ == "__main__":
    result = user_profiles_ingestion()
    print(json.dumps(result, indent=2))
    exit(0 if result.get("success") else 1)