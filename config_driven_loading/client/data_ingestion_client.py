"""
Simplified client interface for data ingestion operations.

@author sathwick
"""
from typing import Dict, Any, List, Optional

from sqlalchemy import Engine, create_engine
from client.orchestrator_factory import DataIngestionFactory
from models.core.base_types import LoadingStats
from models.core.exceptions import DataIngestionException
from models.core.logging_config import setup_logging, DataIngestionLogger


class DataIngestionClient:
    """
    Simplified client interface for data ingestion operations.

    This class provides a high-level, easy-to-use interface for client applications
    to perform data ingestion operations without dealing with internal complexity.

    Updated to support both external engines and database URLs for flexibility.
    """

    def __init__(self,
                 engine: Optional[Engine] = None,
                 database_url: Optional[str] = None,
                 config_path: Optional[str] = None,
                 config_dict: Optional[Dict[str, Any]] = None,
                 pool_size: int = 10,
                 max_overflow: int = 20,
                 echo: bool = False,
                 log_level: str = "INFO",
                 json_logs: bool = False):
        """
        Initialize the data ingestion client.

        Args:
            engine: Pre-configured SQLAlchemy engine (takes precedence over database_url)
            database_url: Database connection URL (used only if engine is not provided)
            config_path: Path to YAML configuration file
            config_dict: Configuration dictionary
            pool_size: Database connection pool size (only used with database_url)
            max_overflow: Maximum pool overflow (only used with database_url)
            echo: Whether to echo SQL statements (only used with database_url)
            log_level: Logging level
            json_logs: Whether to use JSON logging format

        Raises:
            ValueError: If neither engine nor database_url is provided
        """
        # Setup logging
        setup_logging(log_level, json_logs)
        self.logger = DataIngestionLogger(__name__)

        # Handle engine creation or validation
        if engine is not None:
            self.engine = engine
            self._engine_owned = False  # We don't own this engine
            self.logger.info("Using provided SQLAlchemy engine")
        elif database_url is not None:
            self.engine = self._create_engine(database_url, pool_size, max_overflow, echo)
            self._engine_owned = True  # We created this engine
            self.logger.info("Created SQLAlchemy engine from database URL")
        else:
            raise ValueError("Either 'engine' or 'database_url' must be provided")

        # Initialize factory
        self.factory = DataIngestionFactory()

        # Create orchestrator with the engine
        self.orchestrator = self.factory.create_orchestrator(engine=self.engine)

        # Load configuration
        self.config = self.factory.load_configuration(config_path, config_dict)

        self.logger.info("Data ingestion client initialized successfully")

    def _create_engine(self, database_url: str, pool_size: int, max_overflow: int, echo: bool) -> Engine:
        """
        Create SQLAlchemy engine with specified parameters.

        Args:
            database_url: Database connection URL
            pool_size: Connection pool size
            max_overflow: Maximum pool overflow
            echo: Whether to echo SQL statements

        Returns:
            Configured SQLAlchemy engine
        """
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

        Args:
            source_name: Name of the data source to execute

        Returns:
            LoadingStats with execution metrics

        Raises:
            DataIngestionException: If data loading fails
        """
        try:
            self.logger.info(f"Executing data source: {source_name}")
            stats = self.orchestrator.execute_data_loading(self.config, source_name)

            self.logger.info(
                f"Data source execution completed: {source_name}",
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
            self.logger.info("Executing all data sources")
            results = self.orchestrator.execute_all_data_sources(self.config)

            total_records = sum(stats.total_records for stats in results.values())
            successful_records = sum(stats.successful_records for stats in results.values())

            self.logger.info(
                f"All data sources executed successfully",
                total_sources=len(results),
                total_records=total_records,
                successful_records=successful_records
            )

            return results

        except Exception as e:
            self.logger.error(f"Failed to execute all data sources: {str(e)}")
            raise DataIngestionException(f"All sources execution failed: {str(e)}", e)

    def get_available_sources_configs_in_yaml(self) -> List[str]:
        """
        Get list of available data sources.

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
        Validate the current configuration.

        Returns:
            Validation results dictionary
        """
        results = {
            "valid": True,
            "sources": {},
            "errors": []
        }

        for source_name, source_config in self.config.data_sources.items():
            try:
                # Basic validation
                if source_config.type:
                    results["sources"][source_name] = {
                        "type": source_config.type,
                        "valid": True,
                        "errors": []
                    }
                else:
                    results["sources"][source_name] = {
                        "valid": False,
                        "errors": ["Missing source type"]
                    }
                    results["valid"] = False

            except Exception as e:
                results["sources"][source_name] = {
                    "valid": False,
                    "errors": [str(e)]
                }
                results["valid"] = False
                results["errors"].append(f"Source {source_name}: {str(e)}")

        return results

    def get_engine_info(self) -> Dict[str, Any]:
        """
        Get information about the current database engine.

        Returns:
            Dictionary with engine information
        """
        try:
            return {
                "url": str(self.engine.url).split('@')[0] + '@***',  # Hide credentials
                "driver": self.engine.dialect.name,
                "pool_size": getattr(self.engine.pool, 'size', None),
                "pool_checked_out": getattr(self.engine.pool, 'checkedout', None),
                "engine_owned": self._engine_owned
            }
        except Exception as e:
            return {"error": str(e)}

    def close(self):
        """Close all connections and clean up resources."""
        self.factory.close_all()

        # Only dispose the engine if we created it
        if self._engine_owned and hasattr(self, 'engine'):
            self.engine.dispose()
            self.logger.info("Database engine disposed")

        self.logger.info("Data ingestion client closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
