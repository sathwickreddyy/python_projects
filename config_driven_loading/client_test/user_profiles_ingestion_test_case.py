from client_test.db_utils import DatabaseManager
from client_test.ingestion_utils import IngestionRunner


def user_profiles_ingestion() -> dict:
    """Test user profiles ingestion with detailed structured response."""
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

            # Validate inserted data
            _validate_user_profiles_data(engine)

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


def _validate_user_profiles_data(engine) -> dict:
    """Validate that data was inserted correctly and return details."""
    from sqlalchemy import text

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM user_profiles"))
            count = result.fetchone()[0]

            sample_query = text(
                "SELECT user_id, first_name, last_name, email FROM user_profiles LIMIT 3"
            )
            sample_result = conn.execute(sample_query)
            rows = [
                {
                    "user_id": row.user_id,
                    "first_name": row.first_name,
                    "last_name": row.last_name,
                    "email": row.email,
                }
                for row in sample_result.fetchall()
            ]

            return {
                "record_count": count,
                "sample_records": rows,
                "validation_success": True
            }

    except Exception as e:
        return {
            "validation_success": False,
            "error": str(e)
        }


if __name__ == "__main__":
    result = user_profiles_ingestion()
    # Example: return result as JSON in an API, or log it for debugging.
    import json
    print(json.dumps(result, indent=2))
    exit(0 if result.get("success") else 1)