# user_profiles_ingestion_test_case.py
"""
User profiles ingestion client_test using MAPPED strategy.

@author sathwick
"""
from client_test.db_utils import DatabaseManager
from client_test.ingestion_utils import IngestionRunner


def user_profiles_ingestion():
    """Test user profiles ingestion with detailed validation."""
    print("👤 User Profiles Ingestion Test (MAPPED Strategy)")
    print("=" * 55)

    engine = DatabaseManager.create_engine()

    try:
        with IngestionRunner(engine, config_path= "data-sources.yaml") as runner:
            # Validate that source exists
            available_sources = runner.get_available_sources()
            source_name = "user_profile_json"

            if source_name not in available_sources:
                print(f"❌ Source '{source_name}' not found in configuration")
                print(f"Available sources: {available_sources}")
                return False

            print(f"✅ Source '{source_name}' found in configuration")

            # Get source configuration details
            source_config = runner.client.get_source_config(source_name)
            if source_config:
                print(f"📋 Configuration details:")
                print(f"   Type: {source_config.get('type')}")
                print(f"   File: {source_config.get('source_config', {}).get('file_path')}")
                print(f"   Target table: {source_config.get('target_config', {}).get('table')}")
                print(f"   Batch size: {source_config.get('target_config', {}).get('batch_size')}")
                print(f"   Enabled: {source_config.get('target_config', {}).get('enabled')}")

            # Run the ingestion
            print(f"\n🚀 Executing ingestion...")
            result = runner.run_single_source(source_name, print_stats=False)

            if result["success"]:
                print(f"✅ Ingestion completed successfully")
                print(f"   Records processed: {result['total_records']}")
                print(f"   Successful: {result['successful_records']}")
                print(f"   Errors: {result['error_records']}")
                print(f"   Duration: {result['duration_ms']}ms")
                print(f"   Throughput: {result['throughput']:.2f} records/sec")

                if result['error_records'] > 0:
                    print(f"\n⚠️  Error details:")
                    for error in result['errors'][:5]:  # Show first 5 errors
                        print(f"   - {error}")

                # Validate data in database
                print(f"\n🔍 Validating data in database...")
                _validate_user_profiles_data(engine)

                return True
            else:
                print(f"❌ Ingestion failed: {result['error']}")
                return False

    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        return False
    finally:
        engine.dispose()


def _validate_user_profiles_data(engine):
    """Validate that data was inserted correctly."""
    try:
        from sqlalchemy import text

        with engine.connect() as conn:
            # Check record count
            result = conn.execute(text("SELECT COUNT(*) FROM user_profiles"))
            count = result.fetchone()[0]
            print(f"   Total records in table: {count}")

            # Check sample data
            result = conn.execute(text(
                "SELECT user_id, first_name, last_name, email FROM user_profiles LIMIT 3"
            ))
            rows = result.fetchall()

            print(f"   Sample records:")
            for row in rows:
                print(f"     - {row.user_id}: {row.first_name} {row.last_name} ({row.email})")

    except Exception as e:
        print(f"   ⚠️ Validation failed: {str(e)}")


if __name__ == "__main__":
    success = user_profiles_ingestion()
    exit(0 if success else 1)
