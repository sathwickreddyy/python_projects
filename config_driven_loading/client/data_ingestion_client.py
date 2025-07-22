# client/data_ingestion_client.py
"""
Enhanced data ingestion client supporting flexible database connectivity options.

This client provides a high-level interface that automatically handles different
database connectivity patterns based on corporate network restrictions and requirements.

@author sathwick
"""
from typing import Dict, Any, List, Optional, Union
from sqlalchemy import Engine, create_engine
from client.orchestrator_factory import DataIngestionFactory
from models.core.base_types import LoadingStats
from models.core.exceptions import DataIngestionException
from models.core.logging_config import setup_logging, DataIngestionLogger


class DataIngestionClient:
    """
    Flexible data ingestion client supporting multiple database connectivity patterns.

    This client automatically handles:
    1. Direct database connections (highest preference for corporate networks)
    2. SQLAlchemy engines (standard approach)
    3. Print-only mode (no database connectivity required)

    Connection Priority:
    -------------------
    1. Direct database connection (db_connection parameter)
    2. Pre-configured SQLAlchemy engine (engine parameter)
    3. SQLAlchemy engine from database_url (database_url parameter)
    4. Print-only mode (no database parameters provided)

    Usage Examples:
    --------------
    # Corporate network with DB2 connection
    import ms.db2
    conn = ms.db2.connect(SERVER_NAME)
    client = DataIngestionClient(db_connection=conn)

    # Standard SQLAlchemy engine
    client = DataIngestionClient(engine=my_engine)

    # Database URL (creates engine internally)
    client = DataIngestionClient(database_url="postgresql://...")

    # Print-only mode (no database)
    client = DataIngestionClient(config_path="config.yaml")
    """

    def __init__(self,
                 # Database connectivity options (in priority order)
                 db_connection: Optional[Any] = None,
                 connection_type: str = "db2",
                 engine: Optional[Engine] = None,
                 database_url: Optional[str] = None,

                 # Configuration options
                 config_path: Optional[str] = None,
                 config_dict: Optional[Dict[str, Any]] = None,

                 # SQLAlchemy engine options (only used with database_url)
                 pool_size: int = 10,
                 max_overflow: int = 20,
                 echo: bool = False,

                 # Logging options
                 log_level: str = "INFO",
                 json_logs: bool = False):
        """
        Initialize the enhanced data ingestion client.

        Args:
            db_connection: Direct database connection object (highest preference)
            connection_type: Type of direct connection ("db2", "postgres", etc.)
            engine: Pre-configured SQLAlchemy engine
            database_url: Database connection URL (creates engine internally)
            config_path: Path to YAML configuration file
            config_dict: Configuration dictionary
            pool_size: Database connection pool size (only used with database_url)
            max_overflow: Maximum pool overflow (only used with database_url)
            echo: Whether to echo SQL statements (only used with database_url)
            log_level: Logging level
            json_logs: Whether to use JSON logging format

        Note:
            If no database connectivity options are provided, client operates in print-only mode.
            This allows for testing and validation without requiring database access.
        """
        # Setup logging
        setup_logging(log_level, json_logs)
        self.logger = DataIngestionLogger(__name__)

        # Store connectivity information
        self.db_connection = db_connection
        self.connection_type = connection_type
        self.engine = engine
        self.database_url = database_url

        # Determine database connectivity mode
        self.connectivity_mode = self._determine_connectivity_mode()
        self._engine_owned = False

        # Handle engine creation or validation based on priority
        self.active_engine = self._setup_database_connectivity(
            pool_size, max_overflow, echo
        )

        self.logger.info(
            "Data ingestion client initializing",
            connectivity_mode=self.connectivity_mode,
            connection_type=self.connection_type if self.db_connection else "N/A"
        )

        # Initialize factory
        self.factory = DataIngestionFactory()

        # Create orchestrator with appropriate connectivity
        self.orchestrator = self.factory.create_orchestrator(
            engine=self.active_engine,
            db_connection=self.db_connection,
            connection_type=self.connection_type
        )

        # Load configuration with auto-disable for print-only mode
        auto_disable_targets = (self.connectivity_mode == "print_only")
        if auto_disable_targets:
            self.logger.info("Print-only mode detected, auto-disabling database targets")

        self.config = self.factory.load_configuration(
            config_path, config_dict, auto_disable_targets
        )

        self.logger.info(
            "Data ingestion client initialized successfully",
            connectivity_mode=self.connectivity_mode,
            auto_disabled_targets=auto_disable_targets,
            available_sources=len(self.config.data_sources)
        )

    def _determine_connectivity_mode(self) -> str:
        """Determine database connectivity mode based on provided parameters."""
        if self.db_connection is not None:
            return "direct_connection"
        elif self.engine is not None:
            return "provided_engine"
        elif self.database_url is not None:
            return "database_url"
        else:
            return "print_only"

    def _setup_database_connectivity(self, pool_size: int, max_overflow: int, echo: bool) -> Optional[Engine]:
        """Setup database connectivity based on available options."""
        if self.connectivity_mode == "direct_connection":
            # Direct connection takes precedence - no engine needed
            self.logger.info(f"Using direct {self.connection_type} connection")
            return None

        elif self.connectivity_mode == "provided_engine":
            # Use provided engine
            self.logger.info("Using provided SQLAlchemy engine")
            return self.engine

        elif self.connectivity_mode == "database_url":
            # Create engine from URL
            self.logger.info("Creating SQLAlchemy engine from database URL")
            engine = self._create_engine(self.database_url, pool_size, max_overflow, echo)
            self._engine_owned = True
            return engine

        else:
            # Print-only mode
            self.logger.info("No database connectivity - operating in print-only mode")
            return None

    def _create_engine(self, database_url: str, pool_size: int, max_overflow: int, echo: bool) -> Engine:
        """Create SQLAlchemy engine with specified parameters."""
        return create_engine(
            database_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            echo=echo
        )

    def execute_data_source(self, source_name: str) -> LoadingStats:
        """
        Execute data loading for a specific data source.

        This method works regardless of the database connectivity mode and
        provides appropriate behavior for each mode:
        - Direct connection: Uses cursor-based operations
        - Engine-based: Uses SQLAlchemy operations
        - Print-only: Shows sample records

        Args:
            source_name: Name of the data source to execute

        Returns:
            LoadingStats with execution metrics

        Raises:
            DataIngestionException: If data loading fails
        """
        try:
            self.logger.info(
                f"Executing data source: {source_name}",
                connectivity_mode=self.connectivity_mode
            )

            stats = self.orchestrator.execute_data_loading(self.config, source_name)

            self.logger.info(
                f"Data source execution completed: {source_name}",
                connectivity_mode=self.connectivity_mode,
                total_records=stats.total_records,
                successful_records=stats.successful_records,
                error_records=stats.error_records,
                duration_ms=stats.write_time_ms
            )

            return stats

        except Exception as e:
            self.logger.error(f"Failed to execute data source {source_name}: {str(e)}")
            raise DataIngestionException(f"Data source execution failed: {str(e)}", e)

    def execute_all_sources(self) -> Dict[str, LoadingStats]:
        """
        Execute data loading for all configured data sources.

        Returns:
            Dictionary mapping source names to their loading statistics
        """
        try:
            self.logger.info(
                "Executing all data sources",
                connectivity_mode=self.connectivity_mode,
                total_sources=len(self.config.data_sources)
            )

            results = self.orchestrator.execute_all_data_sources(self.config)

            total_records = sum(stats.total_records for stats in results.values())
            successful_records = sum(stats.successful_records for stats in results.values())

            self.logger.info(
                f"All data sources executed successfully",
                connectivity_mode=self.connectivity_mode,
                total_sources=len(results),
                total_records=total_records,
                successful_records=successful_records
            )

            return results

        except Exception as e:
            self.logger.error(f"Failed to execute all data sources: {str(e)}")
            raise DataIngestionException(f"All sources execution failed: {str(e)}", e)

    def get_available_sources(self) -> List[str]:
        """
        Get list of available data sources from configuration.

        Returns:
            List of data source names
        """
        return list(self.config.data_sources.keys())

    def get_source_config(self, source_name: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific data source.

        Args:
            source_name: Name of the data source

        Returns:
            Configuration dictionary or None if not found
        """
        if source_name in self.config.data_sources:
            return self.config.data_sources[source_name].dict()
        return None

    def validate_configuration(self) -> Dict[str, Any]:
        """
        Validate the current configuration and database connectivity.

        Returns:
            Comprehensive validation results dictionary
        """
        results = {
            "valid": True,
            "connectivity": {
                "mode": self.connectivity_mode,
                "connection_type": self.connection_type if self.db_connection else None,
                "has_database_access": self.connectivity_mode != "print_only"
            },
            "sources": {},
            "errors": []
        }

        # Validate database connectivity
        try:
            db_info = self.orchestrator.get_database_info()
            results["connectivity"]["database_info"] = db_info
        except Exception as e:
            results["connectivity"]["validation_error"] = str(e)
            results["errors"].append(f"Database connectivity validation failed: {str(e)}")

        # Validate each data source configuration
        for source_name, source_config in self.config.data_sources.items():
            try:
                source_validation = {
                    "type": source_config.type.value if source_config.type else None,
                    "target_enabled": source_config.target_config.enabled if source_config.target_config else False,
                    "valid": True,
                    "errors": []
                }

                # Basic validation
                if not source_config.type:
                    source_validation["errors"].append("Missing source type")
                    source_validation["valid"] = False

                # Check if file paths exist for file-based sources
                if source_config.type and source_config.type.value in ["CSV", "JSON"]:
                    file_path = source_config.source_config.file_path
                    if file_path:
                        from pathlib import Path
                        if not Path(file_path).exists():
                            source_validation["errors"].append(f"Source file not found: {file_path}")
                            source_validation["valid"] = False

                results["sources"][source_name] = source_validation

                if not source_validation["valid"]:
                    results["valid"] = False

            except Exception as e:
                results["sources"][source_name] = {
                    "valid": False,
                    "errors": [str(e)]
                }
                results["valid"] = False
                results["errors"].append(f"Source {source_name}: {str(e)}")

        return results

    def get_connectivity_info(self) -> Dict[str, Any]:
        """
        Get detailed information about database connectivity.

        Returns:
            Dictionary with comprehensive connectivity information
        """
        info = {
            "connectivity_mode": self.connectivity_mode,
            "connection_type": self.connection_type if self.db_connection else None,
            "has_direct_connection": self.db_connection is not None,
            "has_engine": self.active_engine is not None,
            "engine_owned": self._engine_owned
        }

        # Add engine-specific information if available
        if self.active_engine:
            try:
                info.update({
                    "engine_url": str(self.active_engine.url).split('@')[0] + '@***',
                    "engine_driver": self.active_engine.dialect.name,
                    "pool_size": getattr(self.active_engine.pool, 'size', None),
                    "pool_checked_out": getattr(self.active_engine.pool, 'checkedout', None)
                })
            except Exception as e:
                info["engine_error"] = str(e)

        # Add orchestrator database information
        try:
            orchestrator_info = self.orchestrator.get_database_info()
            info["orchestrator_info"] = orchestrator_info
        except Exception as e:
            info["orchestrator_error"] = str(e)

        return info

    def test_connectivity(self) -> Dict[str, Any]:
        """
        Test database connectivity and return results.

        Returns:
            Dictionary with connectivity test results
        """
        test_result = {
            "connectivity_mode": self.connectivity_mode,
            "test_passed": False,
            "details": {}
        }

        try:
            if self.connectivity_mode == "direct_connection":
                # Test direct connection (basic check)
                if self.db_connection:
                    test_result["test_passed"] = True
                    test_result["details"]["connection_status"] = "Direct connection available"
                else:
                    test_result["details"]["error"] = "Direct connection is None"

            elif self.connectivity_mode in ["provided_engine", "database_url"]:
                # Test engine connectivity
                if self.active_engine:
                    with self.active_engine.connect() as conn:
                        result = conn.execute("SELECT 1")
                        result.fetchone()
                    test_result["test_passed"] = True
                    test_result["details"]["connection_status"] = "Engine connection successful"
                else:
                    test_result["details"]["error"] = "Engine is None"

            else:
                # Print-only mode
                test_result["test_passed"] = True
                test_result["details"]["connection_status"] = "Print-only mode (no database connectivity required)"

        except Exception as e:
            test_result["details"]["error"] = str(e)

        return test_result

    def close(self):
        """Close all connections and clean up resources."""
        try:
            # Close factory and orchestrators
            self.factory.close_all()

            # Close direct database connection if we have one
            if self.db_connection and hasattr(self.db_connection, 'close'):
                self.db_connection.close()
                self.logger.info("Direct database connection closed")

            # Only dispose the engine if we created it
            if self._engine_owned and self.active_engine:
                self.active_engine.dispose()
                self.logger.info("Database engine disposed")

            self.logger.info("Data ingestion client closed successfully")

        except Exception as e:
            self.logger.warning(f"Error during client cleanup: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
