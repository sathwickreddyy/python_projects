# db_utils.py
"""
Database utilities for config-driven data ingestion client project.
Provides reusable database engine creation and connection management.

@author sathwick
"""
from sqlalchemy import create_engine, URL, Engine, text
from sqlalchemy.pool import QueuePool
from typing import Optional, Dict, Any
import os


class DatabaseManager:
    """Manages database connections and engine configuration for data ingestion."""

    @staticmethod
    def create_engine(
        host: str = "localhost",
        port: int = 5433,
        username: str = "docker_user",
        password: str = "Sathwick@18",
        database: str = "config_driven_approach",
        pool_size: int = 15,
        max_overflow: int = 25,
        echo: bool = False
    ) -> Engine:
        """
        Create a production-ready SQLAlchemy engine with optimized settings.

        Args:
            host: Database host
            port: Database port
            username: Database username
            password: Database password
            database: Database name
            pool_size: Connection pool size
            max_overflow: Maximum pool overflow
            echo: Enable SQL logging

        Returns:
            Configured SQLAlchemy Engine
        """
        database_url = URL.create(
            drivername="postgresql",
            username=username,
            password=password,
            host=host,
            port=port,
            database=database
        )

        engine = create_engine(
            database_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            pool_recycle=3600,
            poolclass=QueuePool,
            connect_args={
                "application_name": "ConfigDrivenDataIngestion",
                "connect_timeout": 10,
                "options": "-c timezone=UTC"
            },
            echo=echo,
            echo_pool=False,
            future=True,
            isolation_level="READ_COMMITTED"
        )

        return engine

    @staticmethod
    def test_connection(engine: Engine) -> Dict[str, Any]:
        """
        Test database connection and return connection info.

        Args:
            engine: SQLAlchemy engine to client_test

        Returns:
            Dictionary with connection status and database info
        """
        try:
            with engine.connect() as conn:
                # Get database version
                result = conn.execute(text("SELECT version() as db_version"))
                db_version = result.fetchone()[0]

                # Get current database info
                result = conn.execute(text("SELECT current_database(), current_user, now()"))
                db_info = result.fetchone()

                return {
                    "success": True,
                    "database": db_info[0],
                    "user": db_info[1],
                    "timestamp": db_info[2],
                    "version": db_version[:100],
                    "driver": engine.dialect.name,
                    "pool_size": engine.pool.size(),
                    "max_overflow": engine.pool._max_overflow
                }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }

    @staticmethod
    def get_pool_stats(engine: Engine) -> Dict[str, Any]:
        """Get connection pool statistics."""
        try:
            pool = engine.pool
            return {
                "pool_size": pool.size(),
                "checked_out": pool.checkedout(),
                "checked_in": pool.checkedin(),
                "invalid": pool.invalidated()
            }
        except Exception as e:
            return {"error": str(e)}


if __name__ == "__main__":
    # Quick client_test of database utilities
    print("🔧 Testing Database Utilities")
    print("-" * 40)

    # Create engine
    engine = DatabaseManager.create_engine(echo=False)
    print("✅ Engine created successfully")

    # Test connection
    conn_info = DatabaseManager.test_connection(engine)
    if conn_info["success"]:
        print("✅ Database connection successful")
        print(f"   Database: {conn_info['database']}")
        print(f"   User: {conn_info['user']}")
        print(f"   Driver: {conn_info['driver']}")
    else:
        print(f"❌ Connection failed: {conn_info['error']}")

    # Get pool stats
    pool_stats = DatabaseManager.get_pool_stats(engine)
    print(f"📊 Pool Stats: {pool_stats}")

    # Cleanup
    engine.dispose()
    print("✅ Engine disposed")
