# test.py
"""
Comprehensive test script for config-driven data ingestion library using external engine.

@author sathwick
"""
from sqlalchemy import create_engine, URL, Engine
from sqlalchemy.pool import QueuePool
from client.data_ingestion_client import DataIngestionClient
import os
from pathlib import Path


def create_database_engine() -> Engine:
    """
    Create a customized SQLAlchemy engine with specific configuration.

    Returns:
        Configured SQLAlchemy Engine instance
    """
    # Create URL object for better parameter handling
    database_url = URL.create(
        drivername="postgresql",
        username="docker_user",
        password="Sathwick@18",
        host="localhost",
        port=5433,
        database="config_driven_approach"
    )

    # Create engine with custom configuration
    engine = create_engine(
        database_url,
        # Connection Pool Configuration
        pool_size=15,  # Number of connections to maintain in pool
        max_overflow=25,  # Additional connections beyond pool_size
        pool_pre_ping=True,  # Validate connections before use
        pool_recycle=3600,  # Recycle connections every hour
        poolclass=QueuePool,  # Use QueuePool for connection pooling

        # Connection Configuration
        connect_args={
            "application_name": "ConfigDrivenDataIngestion",  # Identify app in DB logs
            "connect_timeout": 10,  # Connection timeout
            "server_settings": {
                "timezone": "UTC"  # Set timezone
            }
        },

        # Logging and Debugging
        echo=False,  # Set to True to see all SQL statements
        echo_pool=False,  # Set to True to see pool events

        # Execution Options
        future=True,  # Use SQLAlchemy 2.0 style
        isolation_level="READ_COMMITTED"  # Set transaction isolation level
    )

    print(f"✅ Created custom SQLAlchemy engine")
    print(f"   Driver: {engine.dialect.name}")
    print(f"   Pool size: {engine.pool.size()}")
    print(f"   Max overflow: {engine.pool._max_overflow}")

    return engine


def test_engine_connection(engine: Engine) -> bool:
    """
    Test the database engine connection.

    Args:
        engine: SQLAlchemy engine to test

    Returns:
        True if connection successful, False otherwise
    """
    try:
        with engine.connect() as conn:
            result = conn.execute("SELECT version() as db_version")
            db_version = result.fetchone()
            print(f"✅ Database connection successful")
            print(f"   PostgreSQL version: {db_version[0][:50]}...")

            # Test a simple query
            result = conn.execute("SELECT current_database(), current_user, now()")
            db_info = result.fetchone()
            print(f"   Database: {db_info[0]}, User: {db_info[1]}, Time: {db_info[2]}")

            return True
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        return False


def test_data_ingestion():
    """Test data ingestion with different scenarios using custom engine."""

    print("🚀 Starting Config-Driven Data Ingestion Tests")
    print("=" * 60)

    # Create custom database engine
    engine = create_database_engine()

    try:
        # Test engine connection first
        if not test_engine_connection(engine):
            print("💥 Cannot proceed with tests - database connection failed")
            return

        # Ensure sample data directory exists
        sample_data_dir = Path("sample-data")
        sample_data_dir.mkdir(exist_ok=True)

        # Use the custom engine with DataIngestionClient
        with DataIngestionClient(
                engine=engine,  # Pass our custom engine
                config_path="data-sources.yaml",
                log_level="INFO"
        ) as client:

            # Display engine information
            print(f"\n🔍 Engine Information:")
            engine_info = client.get_engine_info()
            for key, value in engine_info.items():
                print(f"   {key}: {value}")

            # Validate configuration
            print(f"\n🔍 Configuration Validation:")
            validation_result = client.validate_configuration()
            if validation_result["valid"]:
                print("✅ Configuration is valid")
            else:
                print("⚠️ Configuration issues found:")
                for error in validation_result["errors"]:
                    print(f"   - {error}")

            # Test 1: MAPPED strategy with database write
            print("\n📋 Test 1: MAPPED Strategy - User Profiles (Database Write)")
            print("-" * 50)
            try:
                stats = client.execute_data_source("user_profile_json")
                print(f"✅ Success: Loaded {stats.successful_records}/{stats.total_records} records")
                print(f"   Duration: {stats.write_time_ms}ms")
                print(f"   Throughput: {stats.records_per_second:.2f} records/sec")
                print(f"   Batch count: {stats.batch_count}")
                if stats.error_records > 0:
                    print(f"⚠️  Errors: {stats.error_records} records failed")
            except Exception as e:
                print(f"❌ Error: {str(e)}")

            # Test 2: MAPPED strategy without database write (testing mode)
            print("\n📋 Test 2: MAPPED Strategy - Orders (Print Only)")
            print("-" * 50)
            try:
                stats = client.execute_data_source("order_details_json_test")
                print(f"✅ Success: Processed {stats.total_records} records (print mode)")
                print(f"   Valid records: {stats.successful_records}")
                print(f"   Duration: {stats.write_time_ms}ms")
            except Exception as e:
                print(f"❌ Error: {str(e)}")

            # Test 3: MAPPED strategy with full order details
            print("\n📋 Test 3: MAPPED Strategy - Order Details (Database Write)")
            print("-" * 50)
            try:
                stats = client.execute_data_source("order_details_json")
                print(f"✅ Success: Loaded {stats.successful_records}/{stats.total_records} records")
                print(f"   Duration: {stats.write_time_ms}ms")
                print(f"   Throughput: {stats.records_per_second:.2f} records/sec")
                print(f"   Batch count: {stats.batch_count}")
                if stats.error_records > 0:
                    print(f"⚠️  Errors: {stats.error_records} records failed")
            except Exception as e:
                print(f"❌ Error: {str(e)}")

            # Test 4: DIRECT strategy (if available)
            print("\n📋 Test 4: DIRECT Strategy - User Profiles (if available)")
            print("-" * 50)
            try:
                available_sources = client.get_available_sources()
                if "user_profile_json_direct" in available_sources:
                    stats = client.execute_data_source("user_profile_json_direct")
                    print(f"✅ Success: Loaded {stats.successful_records}/{stats.total_records} records")
                    print(f"   Duration: {stats.write_time_ms}ms")
                    print(f"   Throughput: {stats.records_per_second:.2f} records/sec")
                else:
                    print("ℹ️  DIRECT strategy test skipped (user_profile_json_direct not configured)")
            except Exception as e:
                print(f"❌ Error: {str(e)}")

            # Test 5: Get available sources
            print("\n📋 Test 5: Available Data Sources")
            print("-" * 50)
            try:
                sources = client.get_available_sources()
                print(f"✅ Available sources ({len(sources)}):")
                for i, source in enumerate(sources, 1):
                    print(f"   {i}. {source}")
            except Exception as e:
                print(f"❌ Error: {str(e)}")

            # Test 6: Execute all sources
            print("\n📋 Test 6: Execute All Sources")
            print("-" * 50)
            try:
                results = client.execute_all_sources()
                print(f"✅ Executed {len(results)} data sources:")
                total_records = 0
                total_successful = 0
                for source_name, stats in results.items():
                    print(f"   - {source_name}: {stats.successful_records}/{stats.total_records} records")
                    total_records += stats.total_records
                    total_successful += stats.successful_records

                print(f"\n🎯 Overall Summary:")
                print(f"   Total records processed: {total_records}")
                print(f"   Total successful: {total_successful}")
                print(
                    f"   Success rate: {(total_successful / total_records * 100):.1f}%" if total_records > 0 else "N/A")

            except Exception as e:
                print(f"❌ Error: {str(e)}")

            # Test 7: Engine pool statistics
            print("\n📋 Test 7: Connection Pool Statistics")
            print("-" * 50)
            try:
                pool = engine.pool
                print(f"✅ Pool Statistics:")
                print(f"   Pool size: {pool.size()}")
                print(f"   Checked out: {pool.checkedout()}")
                print(f"   Checked in: {pool.checkedin()}")
                print(f"   Invalid: {pool.invalidated()}")

            except Exception as e:
                print(f"❌ Error getting pool stats: {str(e)}")

    except Exception as e:
        print(f"💥 Fatal Error: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        # Clean up the engine
        try:
            engine.dispose()
            print(f"\n✅ Database engine disposed successfully")
        except Exception as e:
            print(f"⚠️  Warning: Error disposing engine: {str(e)}")

    print("\n" + "=" * 60)
    print("🏁 Data Ingestion Tests Completed")


def test_engine_performance():
    """Test engine performance with multiple concurrent operations."""

    print("\n🚀 Engine Performance Test")
    print("=" * 40)

    engine = create_database_engine()

    try:
        import time
        import threading
        from concurrent.futures import ThreadPoolExecutor

        def run_concurrent_test(thread_id: int):
            """Run a test in a separate thread."""
            try:
                with engine.connect() as conn:
                    start_time = time.time()
                    result = conn.execute("SELECT pg_sleep(0.1), :thread_id as thread", {"thread_id": thread_id})
                    result.fetchone()
                    duration = time.time() - start_time
                    print(f"   Thread {thread_id}: {duration:.3f}s")
                    return duration
            except Exception as e:
                print(f"   Thread {thread_id}: Error - {str(e)}")
                return None

        # Run concurrent tests
        with ThreadPoolExecutor(max_workers=5) as executor:
            start_time = time.time()
            futures = [executor.submit(run_concurrent_test, i) for i in range(10)]
            results = [f.result() for f in futures]
            total_time = time.time() - start_time

        successful_results = [r for r in results if r is not None]
        if successful_results:
            avg_time = sum(successful_results) / len(successful_results)
            print(f"✅ Concurrent test completed:")
            print(f"   Total time: {total_time:.3f}s")
            print(f"   Average per operation: {avg_time:.3f}s")
            print(f"   Successful operations: {len(successful_results)}/10")

    except Exception as e:
        print(f"❌ Performance test failed: {str(e)}")

    finally:
        engine.dispose()


if __name__ == "__main__":
    # Run main tests
    test_data_ingestion()

    # Optionally run performance tests
    if os.getenv("RUN_PERFORMANCE_TESTS", "false").lower() == "true":
        test_engine_performance()
