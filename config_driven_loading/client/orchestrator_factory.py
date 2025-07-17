"""
Factory for creating data orchestrator instances with different configurations.

@author sathwick
"""
from pathlib import Path
from typing import Dict, Any, Optional

import yaml
from sqlalchemy import Engine, create_engine

from config.data_loader_config import DataLoaderConfiguration
from core.logging_config import DataIngestionLogger
from orchestrators.data_orchestrator import DataOrchestrator


class DataIngestionFactory:
    """
    Factory class for creating data ingestion components with different configurations.

    This factory provides a simple interface for client applications to create
    and configure data orchestrators without dealing with internal complexity.
    """

    def __init__(self):
        self.logger = DataIngestionLogger(__name__)
        self._orchestrators: Dict[str, DataOrchestrator] = {}
        self._engines: Dict[str, Engine] = {}

    def create_orchestrator(self,
                            database_url: str,
                            pool_size: int =10,
                            max_overflow: int = 20,
                            echo: bool = False,
                            orchestrator_id: str = "default") -> DataOrchestrator:
        """
        Create a data orchestrator instance with the specified configuration.

        :param database_url: Database connection URL
        :param pool_size: Database connection pool size
        :param max_overflow: Maximum pool overflow
        :param echo: Whether to echo SQL statements
        :param orchestrator_id: Unique identifier for this orchestrator
        :return: Configured DataOrchestrator instance

        Raises:
            ValueError: If neither config_path nor config_dict is provided
            FileNotFoundError: If config_path doesn't exist
        """

        # Check if orchestrator already exists
        if orchestrator_id in self._orchestrators:
            self.logger.info(f"Returning existing orchestrator: {orchestrator_id}")
            return self._orchestrators[orchestrator_id]
        # Create database engine
        engine = self._create_engine(
            database_url, pool_size, max_overflow, echo, orchestrator_id
        )

        # Create orchestrator
        orchestrator = DataOrchestrator(engine)
        # Cache the orchestrator
        self._orchestrators[orchestrator_id] = orchestrator
        self.logger.info(f"Created data orchestrator: {orchestrator_id}")
        return orchestrator

    def get_orchestrator(self, orchestrator_id: str = "default") -> Optional[DataOrchestrator]:
        """
        Get an existing orchestrator by ID.

        Args:
            orchestrator_id: Orchestrator identifier

        Returns:
            DataOrchestrator instance or None if not found
        """
        return self._orchestrators.get(orchestrator_id)

    def load_configuration(
            self,
            config_path: Optional[str] = None,
            config_dict: Optional[Dict[str, Any]] = None
    ) -> DataLoaderConfiguration:
        """
        Load configuration from file or dictionary.

        Args:
            config_path: Path to YAML configuration file
            config_dict: Configuration dictionary

        Returns:
            DataLoaderConfiguration instance

        Raises:
            ValueError: If neither config_path nor config_dict is provided
        """
        if config_path:
            config_file = Path(config_path)
            if not config_file.exists():
                raise FileNotFoundError(f"Configuration file not found: {config_path}")
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
        elif config_dict:
            config_data = config_dict
        else:
            raise ValueError("Either config_path or config_dict must be provided")

        return DataLoaderConfiguration(**config_data)

    def _create_engine(
            self,
            database_url: str,
            pool_size: int,
            max_overflow: int,
            echo: bool,
            engine_id: str
    ) -> Engine:
        """Create and cache database engine."""
        if engine_id in self._engines:
            return self._engines[engine_id]

        engine = create_engine(
            database_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            echo=echo
        )

        self._engines[engine_id] = engine
        return engine

    def close_all(self):
        """Close all database connections and clean up resources."""
        for engine in self._engines.values():
            engine.dispose()

        self._engines.clear()
        self._orchestrators.clear()

        self.logger.info("All resources cleaned up")