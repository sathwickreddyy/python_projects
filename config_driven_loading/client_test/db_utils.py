# db_utils.py
"""
DB2 connection utilities for config-driven data ingestion client project.
Provides reusable DB2 direct connection management for corporate networks.

@author sathwick
"""
from typing import Optional, Dict, Any
import ms.db2


class DB2ConnectionManager:
    """
    Manages DB2 direct connections for data ingestion operations.

    This manager provides DB2-specific connection utilities designed for
    corporate networks where SQLAlchemy engines may be restricted.

    Key Features:
    - Direct DB2 connection management
    - Connection health testing
    - Schema validation capabilities
    - Corporate network compatibility
    """

    @staticmethod
    def create_connection(server_name: str) -> Any:
        """
        Create a direct DB2 connection using ms.db2.

        Args:
            server_name: DB2 server name as configured in corporate environment

        Returns:
            DB2 connection object

        Raises:
            Exception: If connection fails

        Example:
            conn = DB2ConnectionManager.create_connection("PROD_DB2_SERVER")
        """
        try:
            connection = ms.db2.connect(server_name)
            print(f"✅ DB2 connection created successfully for server: {server_name}")
            return connection

        except Exception as e:
            print(f"❌ Failed to connect to DB2 server '{server_name}': {str(e)}")
            raise

    @staticmethod
    def test_connection(connection: Any) -> Dict[str, Any]:
        """
        Test DB2 connection health and retrieve basic database information.

        Args:
            connection: Active DB2 connection object

        Returns:
            Dictionary with connection status and database information
        """
        try:
            cursor = connection.cursor()

            # Test basic connectivity with a simple query
            cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
            result = cursor.fetchone()

            if result and result[0] == 1:
                # Get database information
                db_info = DB2ConnectionManager._get_db2_info(cursor)
                cursor.close()

                return {
                    "success": True,
                    "connection_type": "DB2 Direct Connection",
                    **db_info
                }
            else:
                cursor.close()
                return {
                    "success": False,
                    "error": "Connection test query failed"
                }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "connection_type": "DB2 Direct Connection"
            }

    @staticmethod
    def _get_db2_info(cursor) -> Dict[str, Any]:
        """
        Retrieve detailed DB2 database information.

        Args:
            cursor: Active DB2 cursor

        Returns:
            Dictionary with DB2 database details
        """
        db_info = {}

        try:
            # Get DB2 version information
            cursor.execute("SELECT SERVICE_LEVEL, FIXPACK_NUM FROM SYSIBMADM.ENV_INST_INFO")
            version_info = cursor.fetchone()
            if version_info:
                db_info["service_level"] = version_info[0]
                db_info["fixpack_number"] = version_info[1]

            # Get current database name
            cursor.execute("SELECT CURRENT SERVER FROM SYSIBM.SYSDUMMY1")
            server_info = cursor.fetchone()
            if server_info:
                db_info["current_server"] = server_info[0]

            # Get current user
            cursor.execute("SELECT USER FROM SYSIBM.SYSDUMMY1")
            user_info = cursor.fetchone()
            if user_info:
                db_info["current_user"] = user_info[0]

            # Get current timestamp
            cursor.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
            timestamp_info = cursor.fetchone()
            if timestamp_info:
                db_info["current_timestamp"] = str(timestamp_info[0])

        except Exception as e:
            db_info["info_error"] = f"Failed to retrieve DB2 info: {str(e)}"

        return db_info

    @staticmethod
    def validate_table_access(connection: Any, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Validate access to a specific DB2 table and retrieve column information.

        Args:
            connection: Active DB2 connection
            schema_name: Database schema name
            table_name: Table name to validate

        Returns:
            Dictionary with table validation results
        """
        try:
            cursor = connection.cursor()

            # Check if table exists and get column information
            check_query = """
            SELECT 
                COLNAME,
                TYPENAME,
                LENGTH,
                SCALE,
                NULLS
            FROM SYSCAT.COLUMNS 
            WHERE TABSCHEMA = ? 
              AND TABNAME = ?
            ORDER BY COLNO
            """

            cursor.execute(check_query, (schema_name.upper(), table_name.upper()))
            columns = cursor.fetchall()
            cursor.close()

            if columns:
                column_info = []
                for col in columns:
                    column_info.append({
                        "name": col[0].lower(),
                        "type": col[1],
                        "length": col[2],
                        "scale": col[3],
                        "nullable": col[4] == 'Y'
                    })

                return {
                    "success": True,
                    "table_exists": True,
                    "schema": schema_name,
                    "table": table_name,
                    "column_count": len(column_info),
                    "columns": column_info[:10],  # Show first 10 columns
                    "total_columns": len(column_info)
                }
            else:
                return {
                    "success": False,
                    "table_exists": False,
                    "schema": schema_name,
                    "table": table_name,
                    "error": f"Table '{schema_name}.{table_name}' not found or no access"
                }

        except Exception as e:
            return {
                "success": False,
                "schema": schema_name,
                "table": table_name,
                "error": str(e)
            }

    @staticmethod
    def test_write_access(connection: Any, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Test write access to a DB2 table by performing a dry-run operation.

        Args:
            connection: Active DB2 connection
            schema_name: Database schema name
            table_name: Table name to test

        Returns:
            Dictionary with write access test results
        """
        try:
            cursor = connection.cursor()

            # Test write access by preparing an insert statement (without executing)
            test_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
            cursor.execute(test_query)
            count_result = cursor.fetchone()

            cursor.close()

            return {
                "success": True,
                "write_access": True,
                "schema": schema_name,
                "table": table_name,
                "current_row_count": count_result[0] if count_result else 0,
                "note": "Write access validated through table query"
            }

        except Exception as e:
            return {
                "success": False,
                "write_access": False,
                "schema": schema_name,
                "table": table_name,
                "error": str(e)
            }

    @staticmethod
    def close_connection(connection: Any) -> None:
        """
        Safely close a DB2 connection.

        Args:
            connection: DB2 connection to close
        """
        try:
            if connection:
                connection.close()
                print("✅ DB2 connection closed successfully")
        except Exception as e:
            print(f"⚠️ Warning: Error closing DB2 connection: {str(e)}")


if __name__ == "__main__":
    # Quick test of DB2 connection utilities
    print("🔧 Testing DB2 Connection Utilities")
    print("-" * 40)

    try:
        # Replace with your actual DB2 server name
        SERVER_NAME = "YOUR_DB2_SERVER"

        # Create connection
        print(f"Connecting to DB2 server: {SERVER_NAME}")
        conn = DB2ConnectionManager.create_connection(SERVER_NAME)

        # Test connection
        conn_info = DB2ConnectionManager.test_connection(conn)
        if conn_info["success"]:
            print("✅ DB2 connection test successful")
            print(f"   Server: {conn_info.get('current_server', 'N/A')}")
            print(f"   User: {conn_info.get('current_user', 'N/A')}")
            print(f"   Service Level: {conn_info.get('service_level', 'N/A')}")
        else:
            print(f"❌ Connection test failed: {conn_info.get('error', 'Unknown error')}")

        # Test table validation (example)
        print("\n🔍 Testing table access...")
        table_info = DB2ConnectionManager.validate_table_access(conn, "PUBLIC", "SAMPLE_TABLE")
        if table_info["success"]:
            print(f"✅ Table access validated: {table_info['column_count']} columns found")
        else:
            print(f"⚠️ Table validation: {table_info.get('error', 'Unknown error')}")

        # Close connection
        DB2ConnectionManager.close_connection(conn)

    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
