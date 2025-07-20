# test_order_details_ingestion.py
"""
Order details ingestion test using MAPPED strategy.

@author sathwick
"""
from db_utils import DatabaseManager
from ingestion_utils import IngestionRunner


def order_details_ingestion():
    """Test order details ingestion with comprehensive validation."""
    print("📦 Order Details Ingestion Test (MAPPED Strategy)")
    print("=" * 52)

    engine = DatabaseManager.create_engine()

    try:
        with IngestionRunner(engine, config_path="data-sources.yaml") as runner:
            source_name = "order_details_json"

            # Check if source exists
            available_sources = runner.get_available_sources()
            if source_name not in available_sources:
                print(f"❌ Source '{source_name}' not available")
                return False

            print(f"✅ Source '{source_name}' found")

            # Execute ingestion
            print(f"\n🚀 Starting order details ingestion...")
            result = runner.run_single_source(source_name, print_stats=False)

            if result:
                print("✅ Ingestion was successful")
                print(f"\n🔍 Validating inserted data...")
                _validate_order_data(engine)
                return True
            else:
                print(f"❌ Ingestion failed: {result['error']}")
                return False

    except Exception as e:
        print(f"❌ Test execution failed: {str(e)}")
        return False
    finally:
        engine.dispose()


def _validate_order_data(engine):
    """Validate order data in database."""
    try:
        from sqlalchemy import text

        with engine.connect() as conn:
            # Count orders
            result = conn.execute(text("SELECT COUNT(*) FROM order_details"))
            count = result.fetchone()[0]
            print(f"   Total orders in database: {count}")

            # Check data types and sample records
            result = conn.execute(text("""
                SELECT order_id, customer_name, total_amount, order_status, created_date
                FROM order_details 
                ORDER BY created_date 
                LIMIT 3
            """))

            print(f"   Sample orders:")
            for row in result.fetchall():
                print(f"     - {row.order_id}: {row.customer_name}, ${row.total_amount}, {row.order_status}")

            # Check for any null values in required fields
            result = conn.execute(text("""
                SELECT 
                    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
                    SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) as null_names
                FROM order_details
            """))

            null_counts = result.fetchone()
            if null_counts.null_order_ids > 0 or null_counts.null_names > 0:
                print(f"   ⚠️ Found null values in required fields")
                print(f"     Null order_ids: {null_counts.null_order_ids}")
                print(f"     Null names: {null_counts.null_names}")
            else:
                print(f"   ✅ No null values in required fields")

    except Exception as e:
        print(f"   ❌ Data validation failed: {str(e)}")


if __name__ == "__main__":
    success = order_details_ingestion()
    exit(0 if success else 1)
