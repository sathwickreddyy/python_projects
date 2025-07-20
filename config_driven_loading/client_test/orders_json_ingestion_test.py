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

            # Validate inserted data
            _validate_order_data(engine)

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


def _validate_order_data(engine) -> dict:
    """Validate order data in database."""
    from sqlalchemy import text

    try:
        with engine.connect() as conn:
            # Count orders
            result = conn.execute(text("SELECT COUNT(*) FROM order_details"))
            count = result.fetchone()[0]

            # Sample records
            sample_query = text("""
                SELECT order_id, customer_name, total_amount, order_status, created_date
                FROM order_details 
                ORDER BY created_date 
                LIMIT 3
            """)
            sample_result = conn.execute(sample_query)
            rows = [
                {
                    "order_id": row.order_id,
                    "customer_name": row.customer_name,
                    "total_amount": row.total_amount,
                    "order_status": row.order_status,
                    "created_date": row.created_date.isoformat() if row.created_date else None,
                }
                for row in sample_result.fetchall()
            ]

            # Check for null values in important fields
            nulls_query = text("""
                SELECT 
                    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
                    SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) as null_names
                FROM order_details
            """)
            null_counts = conn.execute(nulls_query).fetchone()

            return {
                "record_count": count,
                "sample_records": rows,
                "null_checks": {
                    "null_order_ids": null_counts.null_order_ids,
                    "null_customer_names": null_counts.null_names
                },
                "validation_success": (null_counts.null_order_ids == 0 and null_counts.null_names == 0)
            }

    except Exception as e:
        return {
            "validation_success": False,
            "error": str(e)
        }



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