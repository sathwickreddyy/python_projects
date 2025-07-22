# ingestion_utils.py
"""
DB2-focused ingestion utilities for common data ingestion operations.
Provides reusable wrappers using direct DB2 connections.

@author sathwick
"""
from typing import Dict, Any, List
from client.data_ingestion_client import DataIngestionClient
from db_utils import DB2ConnectionManager


class DB2IngestionRunner:
    """
    DB2-specific wrapper for common ingestion operations with standardized reporting.

    This runner is designed for internal environments using direct DB2 connections,
    providing data ingestion capabilities without SQLAlchemy dependencies.

    Key Features:
    - Direct DB2 connection management
    - Standardized response dictionaries
    - Internal network compatibility
    - Comprehensive error handling
    - Performance metrics collection
    """

    def __init__(self, server_name: str, config_path: str):
        """
        Initialize DB2 ingestion runner with server connection details.

        Args:
            server_name: DB2 server name as configured in corporate environment
            config_path: Path to YAML configuration file
        """
        self.server_name = server_name
        self.config_path = config_path
        self.connection = None
        self.client = None

    def __enter__(self):
        """
        Context manager entry - establish DB2 connection and initialize client.
        """
        try:
            # Create DB2 connection
            self.connection = DB2ConnectionManager.create_connection(self.server_name)

            # Initialize data ingestion client with DB2 connection
            self.client = DataIngestionClient(
                db_connection=self.connection,
                connection_type="db2",
                config_path=self.config_path,
                log_level="INFO"
            )

            return self

        except Exception as e:
            # Clean up on initialization failure
            if self.connection:
                DB2ConnectionManager.close_connection(self.connection)
            raise Exception(f"Failed to initialize DB2 ingestion runner: {str(e)}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit - clean up resources.
        """
        try:
            # Close client first
            if self.client:
                self.client.close()

            # Close DB2 connection
            if self.connection:
                DB2ConnectionManager.close_connection(self.connection)

        except Exception as e:
            print(f"⚠️ Warning: Error during cleanup: {str(e)}")

    def run_single_source(self, source_name: str, print_stats: bool = True) -> Dict[str, Any]:
        """
        Execute ingestion for a single data source with comprehensive result reporting.

        Args:
            source_name: Name of data source to process
            print_stats: Whether to print detailed statistics

        Returns:
            Standardized dictionary with execution results
        """
        try:
            # Execute data source ingestion
            stats = self.client.execute_data_source(source_name)

            # Build standardized response
            result = self._build_execution_summary(source_name, stats)

            if print_stats:
                self._print_execution_details(result)

            return result

        except Exception as exception:
            error_result = {
                "source": source_name,
                "success": False,
                "partial_success": False,
                "error": str(exception),
                "details": None,
                "db2_info": {
                    "server": self.server_name,
                    "connection_type": "DB2 Direct"
                }
            }

            if print_stats:
                print(f"❌ Ingestion failed for {source_name}: {str(exception)}")

            return error_result

    def run_multiple_sources(self, source_names: List[str]) -> Dict[str, Any]:
        """
        Execute ingestion for multiple data sources with overall summary.

        Args:
            source_names: List of source names to process

        Returns:
            Dictionary with individual results and overall summary
        """
        print(f"🚀 Running DB2 ingestion for {len(source_names)} sources")
        print("=" * 60)

        results = {}

        for source_name in source_names:
            print(f"\n📋 Processing: {source_name}")
            print("-" * 40)
            results[source_name] = self.run_single_source(source_name, print_stats=True)

        # Build overall summary
        summary = self._build_overall_summary(results)

        # Print summary
        self._print_overall_summary(summary)

        return {
            "results": results,
            "summary": summary,
            "db2_info": {
                "server": self.server_name,
                "connection_type": "DB2 Direct",
                "sources_processed": len(source_names)
            }
        }

    def validate_config(self) -> Dict[str, Any]:
        """
        Validate configuration and DB2 connectivity.

        Returns:
            Dictionary with validation results
        """
        try:
            # Validate configuration
            validation = self.client.validate_configuration()

            # Add DB2-specific validation
            connectivity_info = self.client.get_connectivity_info()

            return {
                "success": validation.get("valid", False),
                "details": validation,
                "connectivity": connectivity_info,
                "db2_info": {
                    "server": self.server_name,
                    "connection_type": "DB2 Direct"
                }
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "details": None,
                "db2_info": {
                    "server": self.server_name,
                    "error": "Validation failed"
                }
            }

    def get_available_sources(self) -> Dict[str, Any]:
        """
        Get available data sources from configuration.

        Returns:
            Dictionary with source information
        """
        try:
            sources = self.client.get_available_sources()
            return {
                "success": True,
                "sources": sources,
                "count": len(sources),
                "db2_info": {
                    "server": self.server_name,
                    "connection_type": "DB2 Direct"
                }
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "sources": [],
                "count": 0
            }

    def test_db2_connectivity(self) -> Dict[str, Any]:
        """
        Test DB2 connectivity and return detailed information.

        Returns:
            Dictionary with connectivity test results
        """
        try:
            # Test basic connection
            conn_test = DB2ConnectionManager.test_connection(self.connection)

            # Test client connectivity
            client_test = self.client.test_connectivity()

            return {
                "success": conn_test["success"] and client_test["test_passed"],
                "db2_connection": conn_test,
                "client_connectivity": client_test,
                "server": self.server_name
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "server": self.server_name
            }

    def validate_table_access(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Validate access to specific DB2 table.

        Args:
            schema_name: Database schema name
            table_name: Table name

        Returns:
            Dictionary with table access validation results
        """
        try:
            table_info = DB2ConnectionManager.validate_table_access(
                self.connection, schema_name, table_name
            )

            write_test = DB2ConnectionManager.test_write_access(
                self.connection, schema_name, table_name
            )

            return {
                "table_validation": table_info,
                "write_access": write_test,
                "server": self.server_name
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "schema": schema_name,
                "table": table_name,
                "server": self.server_name
            }

    def _build_execution_summary(self, source_name: str, stats) -> Dict[str, Any]:
        """Build standardized execution summary."""
        success = stats.error_records == 0
        partial = 0 < stats.error_records < stats.total_records

        response = {
            "source": source_name,
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
            "status": "SUCCESS" if success else "PARTIAL_SUCCESS" if partial else "FAILED",
            "db2_info": {
                "server": self.server_name,
                "connection_type": "DB2 Direct"
            }
        }

        if len(stats.get_all_errors()) > 10:
            response["error_summary"] = f"... {len(stats.get_all_errors()) - 10} more errors not shown"

        return response

    def _build_overall_summary(self, results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Build overall summary from multiple source results."""
        successful_sources = sum(1 for r in results.values() if r.get("success"))
        partial_sources = sum(1 for r in results.values() if r.get("partial_success"))
        failed_sources = len(results) - successful_sources - partial_sources

        total_sources = len(results)
        total_records = sum(r.get("total_records", 0) for r in results.values())
        successful_records = sum(r.get("successful_records", 0) for r in results.values())

        return {
            "sources_processed": total_sources,
            "sources_successful": successful_sources,
            "sources_partial": partial_sources,
            "sources_failed": failed_sources,
            "total_records": total_records,
            "successful_records": successful_records,
            "failed_records": total_records - successful_records,
            "success_rate_percent": round((successful_records / total_records * 100), 1) if total_records > 0 else 0.0,
            "db2_info": {
                "server": self.server_name,
                "connection_type": "DB2 Direct"
            }
        }

    def _print_execution_details(self, result: Dict[str, Any]):
        """Print detailed execution information."""
        source = result.get("source", "Unknown")
        status = result.get("status", "UNKNOWN")

        if result.get("success"):
            print(f"✅ {source}: {status}")
        elif result.get("partial_success"):
            print(f"⚠️ {source}: {status}")
        else:
            print(f"❌ {source}: {status}")

        print(f"   Records: {result.get('successful_records', 0)}/{result.get('total_records', 0)}")
        print(f"   Duration: {result.get('write_duration_ms', 0)}ms")
        print(f"   Throughput: {result.get('throughput_records_per_sec', 0)} records/sec")

        if result.get('failed_records', 0) > 0:
            print(f"   Failures: {result.get('failed_records', 0)}")

        errors = result.get('errors', [])
        if errors:
            print(f"   Errors (showing first 3):")
            for error in errors[:3]:
                print(f"     - {error}")

    def _print_overall_summary(self, summary: Dict[str, Any]):
        """Print overall execution summary."""
        print(f"\n🎯 OVERALL SUMMARY (DB2: {self.server_name})")
        print("=" * 50)
        print(f"Sources processed: {summary['sources_successful']}/{summary['sources_processed']}")
        print(f"Total records: {summary['total_records']:,}")
        print(f"Successful records: {summary['successful_records']:,}")
        print(f"Success rate: {summary['success_rate_percent']}%")

        if summary['sources_partial'] > 0:
            print(f"Partial successes: {summary['sources_partial']}")
        if summary['sources_failed'] > 0:
            print(f"Failed sources: {summary['sources_failed']}")


if __name__ == "__main__":
    # Quick test of DB2 ingestion utilities
    print("🔧 Testing DB2 Ingestion Utilities")
    print("-" * 40)

    try:
        SERVER_NAME = "YOUR_DB2_SERVER"  # Replace with actual server

        with DB2IngestionRunner(SERVER_NAME, "data-sources.yaml") as runner:
            # Test connectivity
            connectivity = runner.test_db2_connectivity()
            print(f"✅ Connectivity test: {'Passed' if connectivity['success'] else 'Failed'}")

            # Test config validation
            validation = runner.validate_config()
            print(f"✅ Config validation: {'Passed' if validation['success'] else 'Failed'}")

            # Get available sources
            sources = runner.get_available_sources()
            print(f"✅ Available sources: {sources['count']}")

    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
