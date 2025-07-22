# test_direct_ingestion_db2.py
"""
DB2 direct ingestion test without SQLAlchemy dependencies.
Demonstrates corporate-network-friendly data ingestion approach.

@author sathwick
"""
import json
from decimal import Decimal

from ingestion_utils import DB2IngestionRunner


def db2_direct_ingestion_test(server_name: str, source_config_name: str = "user_profile_json_direct") -> Dict[str, Any]:
    """
    Execute DB2 direct ingestion test for a specific data source.

    Args:
        server_name: DB2 server name
        source_config_name: Configuration name from YAML file

    Returns:
        Dictionary with comprehensive test results
    """
    test_context = {
        "server_name": server_name,
        "source_config_name": source_config_name,
        "connection_type": "DB2 Direct",
        "timestamp": None
    }

    try:
        with DB2IngestionRunner(server_name, config_path="data-sources.yaml") as runner:

            # Step 1: Validate connectivity
            print("🔍 Step 1: Testing DB2 connectivity...")
            connectivity_test = runner.test_db2_connectivity()
            if not connectivity_test["success"]:
                return {
                    "success": False,
                    "step": "connectivity_test",
                    "error": "DB2 connectivity test failed",
                    "details": connectivity_test,
                    "context": test_context
                }

            print("✅ DB2 connectivity validated")

            # Step 2: Validate configuration
            print("🔍 Step 2: Validating configuration...")
            config_validation = runner.validate_config()
            if not config_validation["success"]:
                return {
                    "success": False,
                    "step": "config_validation",
                    "error": "Configuration validation failed",
                    "details": config_validation,
                    "context": test_context
                }

            print("✅ Configuration validated")

            # Step 3: Check source availability
            print("🔍 Step 3: Checking source availability...")
            available_sources = runner.get_available_sources()

            if source_config_name not in available_sources["sources"]:
                return {
                    "success": False,
                    "step": "source_availability",
                    "error": "Source configuration not found",
                    "source_config_name": source_config_name,
                    "available_sources": available_sources["sources"],
                    "context": test_context
                }

            print(f"✅ Source '{source_config_name}' found in configuration")

            # Step 4: Execute ingestion
            print(f"🚀 Step 4: Executing ingestion for '{source_config_name}'...")
            loading_result = runner.run_single_source(source_config_name, print_stats=True)

            # Step 5: Analyze results
            ingestion_summary = {
                "total_records": loading_result.get("total_records"),
                "successful_records": loading_result.get("successful_records"),
                "failed_records": loading_result.get("failed_records"),
                "duration_ms": loading_result.get("write_duration_ms"),
                "throughput": loading_result.get("throughput_records_per_sec"),
                "batches": loading_result.get("batches"),
                "errors": loading_result.get("errors"),
                "error_summary": loading_result.get("error_summary"),
                "status": loading_result.get("status"),
                "db2_server": server_name,
                "connection_type": "DB2 Direct"
            }

            # Determine overall success
            overall_success = loading_result.get("success", False)
            partial_success = loading_result.get("partial_success", False)

            return {
                "success": overall_success,
                "partial_success": partial_success,
                "ingestion_summary": ingestion_summary,
                "connectivity_test": connectivity_test,
                "config_validation": config_validation,
                "context": test_context,
                "step": "completed"
            }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "step": "exception_during_execution",
            "context": test_context
        }


def db2_table_validation_test(server_name: str, schema_name: str, table_name: str) -> Dict[str, Any]:
    """
    Test DB2 table access and validation.

    Args:
        server_name: DB2 server name
        schema_name: Database schema name
        table_name: Table name to validate

    Returns:
        Dictionary with table validation results
    """
    try:
        with DB2IngestionRunner(server_name, config_path="data-sources.yaml") as runner:

            # Validate table access
            table_validation = runner.validate_table_access(schema_name, table_name)

            return {
                "success": True,
                "table_validation": table_validation,
                "server_name": server_name,
                "connection_type": "DB2 Direct"
            }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "server_name": server_name,
            "schema_name": schema_name,
            "table_name": table_name
        }


def json_default_encoder(obj):
    """Custom JSON encoder for complex types."""
    if isinstance(obj, Decimal):
        return float(obj)
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# Main execution examples
def main():
    """
    Main function demonstrating different DB2 ingestion test scenarios.

    Replace 'YOUR_DB2_SERVER' with your actual DB2 server name.
    """
    SERVER_NAME = "YOUR_DB2_SERVER"  # Replace with actual server name

    print("🚀 DB2 Direct Ingestion Tests")
    print("=" * 50)

    # Test 1: Single source ingestion
    print("\n📋 Test 1: Single Source Ingestion")
    print("-" * 30)
    single_source_result = db2_direct_ingestion_test(
        server_name=SERVER_NAME,
        source_config_name="user_profile_json_direct"
    )
    print(f"Result: {'SUCCESS' if single_source_result.get('success') else 'FAILED'}")


    # Test 3: Table validation
    print("\n📋 Test 3: Table Validation")
    print("-" * 25)
    table_validation_result = db2_table_validation_test(
        server_name=SERVER_NAME,
        schema_name="PUBLIC",
        table_name="USER_PROFILES"
    )
    print(f"Result: {'SUCCESS' if table_validation_result.get('success') else 'FAILED'}")

    # Output results
    print("\n📊 Detailed Results:")
    print(json.dumps({
        "single_source_test": single_source_result,
        "table_validation_test": table_validation_result
    }, indent=2, default=json_default_encoder))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Test execution failed: {str(e)}")
        exit(1)
