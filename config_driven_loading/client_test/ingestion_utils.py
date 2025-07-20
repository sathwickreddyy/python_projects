# ingestion_utils.py
"""
Ingestion utilities for common data ingestion operations.
Provides reusable wrappers and helper functions.

@author sathwick
"""
from typing import Dict, Any, List

from sqlalchemy import Engine

from client.data_ingestion_client import DataIngestionClient
from db_utils import DatabaseManager


class IngestionRunner:
    """Wrapper for common ingestion operations with standardized reporting."""

    def __init__(self, engine: Engine, config_path: str):
        """
        Initialize ingestion runner.

        Args:
            engine: SQLAlchemy engine
            config_path: Path to configuration file
        """
        self.engine = engine
        self.config_path = config_path
        self.client = None

    def __enter__(self):
        """Context manager entry."""
        self.client = DataIngestionClient(
            engine=self.engine,
            config_path=self.config_path,
            log_level="INFO"
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.client:
            self.client.close()

    def run_single_source(self, source_name: str, print_stats: bool = True) -> Dict[str, Any]:
        """
        Run ingestion for a single data source.

        Args:
            source_name: Name of data source to process
            print_stats: Whether to print detailed statistics

        Returns:
            Dictionary with execution results
        """
        try:
            stats = self.client.execute_data_source(source_name)

            result = {
                "success": True,
                "source_name": source_name,
                "total_records": stats.total_records,
                "successful_records": stats.successful_records,
                "error_records": stats.error_records,
                "duration_ms": stats.write_time_ms,
                "throughput": stats.records_per_second,
                "batch_count": stats.batch_count,
                "errors": stats.get_all_errors()
            }

            if print_stats:
                self._print_execution_summary(result)

            return result

        except Exception as e:
            result = {
                "success": False,
                "source_name": source_name,
                "error": str(e)
            }

            if print_stats:
                print(f"❌ Failed to execute {source_name}: {str(e)}")

            return result

    def run_multiple_sources(self, source_names: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Run ingestion for multiple data sources.

        Args:
            source_names: List of source names to process

        Returns:
            Dictionary mapping source names to execution results
        """
        results = {}

        print(f"🚀 Running ingestion for {len(source_names)} sources")
        print("=" * 60)

        for source_name in source_names:
            print(f"\n📋 Processing: {source_name}")
            print("-" * 40)
            results[source_name] = self.run_single_source(source_name, print_stats=True)

        # Print overall summary
        self._print_overall_summary(results)

        return results

    def validate_config(self) -> Dict[str, Any]:
        """Validate configuration and return results."""
        return self.client.validate_configuration()

    def get_available_sources(self) -> List[str]:
        """Get list of available data sources."""
        return self.client.get_available_sources()

    def _print_execution_summary(self, result: Dict[str, Any]):
        """Print formatted execution summary."""
        if result["success"]:
            print(f"✅ Success: {result['successful_records']}/{result['total_records']} records")
            print(f"   Duration: {result['duration_ms']}ms")
            print(f"   Throughput: {result['throughput']:.2f} records/sec")
            if result['batch_count'] > 0:
                print(f"   Batches: {result['batch_count']}")
            if result["error_records"] > 0:
                print(f"⚠️  Errors: {result['error_records']} records failed")
                if result["errors"]:
                    print("   Error details:")
                    for error in result["errors"][:3]:  # Show first 3 errors
                        print(f"     - {error}")
                    if len(result["errors"]) > 3:
                        print(f"     ... and {len(result['errors']) - 3} more")
        else:
            print(f"❌ Failed: {result['error']}")

    def _print_overall_summary(self, results: Dict[str, Dict[str, Any]]):
        """Print overall summary for multiple sources."""
        print(f"\n🎯 OVERALL SUMMARY")
        print("=" * 40)

        successful_sources = sum(1 for r in results.values() if r["success"])
        total_records = sum(r.get("total_records", 0) for r in results.values())
        successful_records = sum(r.get("successful_records", 0) for r in results.values())

        print(f"Sources processed: {successful_sources}/{len(results)}")
        print(f"Total records: {total_records:,}")
        print(f"Successful records: {successful_records:,}")
        if total_records > 0:
            print(f"Success rate: {(successful_records / total_records * 100):.1f}%")


if __name__ == "__main__":
    # Quick client_test of ingestion utilities
    print("🔧 Testing Ingestion Utilities")
    print("-" * 40)

    engine = DatabaseManager.create_engine()

    try:
        with IngestionRunner(engine, config_path="data-sources.yaml") as runner:
            # Test config validation
            validation = runner.validate_config()
            print(f"✅ Config validation: {'Valid' if validation['valid'] else 'Invalid'}")

            # Test getting available sources
            sources = runner.get_available_sources()
            print(f"✅ Available sources: {len(sources)}")
            for source in sources:
                print(f"   - {source}")

    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
    finally:
        engine.dispose()
