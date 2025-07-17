"""
Simplified client interface for data ingestion operations.

@author sathwick
"""
from typing import Dict, Any, List, Optional, Union
from pathlib import Path

from client.orchestrator_factory import DataIngestionFactory
from core.base_types import LoadingStats
from core.exceptions import DataIngestionException
from core.logging_config import setup_logging, DataIngestionLogger


class DataIngestionClient:
    """
    Simplified client interface for data ingestion operations.

    This class provides a high-level, easy-to-use interface for client applications
    to perform data ingestion operations without dealing with internal complexity.
    """

    def __init__(
            self,
            database_url: str,
            config_path: Optional[str] = None,
            config_dict: Optional[Dict[str, Any]] = None,
            pool_size: int = 10,
            max_overflow: int = 20,
            echo: bool = False,
            log_level: str = "INFO",
            json_logs: bool = False
    ):
        """
        Initialize the data ingestion client.

        Args:
            database_url: Database connection URL
            config_path: Path to YAML configuration file
            config_dict: Configuration dictionary
            pool_size: Database connection pool size
            max_overflow: Maximum pool overflow
            echo: Whether to echo SQL statements
            log_level: Logging level
            json_logs: Whether to use JSON logging format
        """
        # Setup logging
        setup_logging(log_level, json_logs)
        self.logger = DataIngestionLogger(__name__)

        # Initialize factory
        self.factory = DataIngestionFactory()

        # Create orchestrator
        self.orchestrator = self.factory.create_orchestrator(
            database_url=database_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            echo=echo,
        )

        # Load configuration
        self.config = self.factory.load_configuration(config_path, config_dict)

        self.logger.info("Data ingestion client initialized successfully")

    def execute_data_source_loading_to_db(self, source_name: str) -> LoadingStats:
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

    def execute_all_sources_to_db(self) -> Dict[str, LoadingStats]:
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

    # def load_models(
    #         self,
    #         models: List[Union[Dict[str, Any], Any]],
    #         table_name: str,
    #         schema_name: str = "public"
    # ) -> LoadingStats:
    #     """
    #     Load model objects directly to database.
    #
    #     Args:
    #         models: List of model objects (dicts or Pydantic models)
    #         table_name: Target table name
    #         schema_name: Target schema name
    #
    #     Returns:
    #         LoadingStats with execution metrics
    #     """
    #     try:
    #         self.logger.info(f"Loading {len(models)} models to {schema_name}.{table_name}")
    #
    #         # Convert Pydantic models to dicts if necessary
    #         processed_models = []
    #         for model in models:
    #             if hasattr(model, 'dict'):
    #                 processed_models.append(model.dict())
    #             elif hasattr(model, '__dict__'):
    #                 processed_models.append(model.__dict__)
    #             else:
    #                 processed_models.append(model)
    #
    #         stats = self.orchestrator.load_models_to_database(
    #             processed_models, table_name, schema_name
    #         )
    #
    #         self.logger.info(
    #             f"Models loaded successfully",
    #             table_name=table_name,
    #             total_records=stats.total_records,
    #             successful_records=stats.successful_records
    #         )
    #
    #         return stats
    #
    #     except Exception as e:
    #         self.logger.error(f"Failed to load models: {str(e)}")
    #         raise DataIngestionException(f"Model loading failed: {str(e)}", e)

    def get_available_sources(self) -> List[str]:
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

    def close(self):
        """Close all connections and clean up resources."""
        self.factory.close_all()
        self.logger.info("Data ingestion client closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
