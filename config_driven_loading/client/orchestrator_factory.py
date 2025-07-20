# client/orchestrator_factory.py
"""
Factory for creating data orchestrator instances with different configurations.

@author sathwick
"""
from pathlib import Path
from typing import Dict, Any, Optional

import yaml
from sqlalchemy import Engine

from config.data_loader_config import DataLoaderConfiguration
from models.core.logging_config import DataIngestionLogger
from orchestrators.data_orchestrator import DataOrchestrator


class DataIngestionFactory:
    """
    Factory class for creating data ingestion components with different configurations.

    This factory provides a simple interface for client applications to create
    and configure data orchestrators without dealing with internal complexity.
    
    Updated to accept external SQLAlchemy engines for better flexibility and control.
    """

    def __init__(self):
        self.logger = DataIngestionLogger(__name__)
        self._orchestrators: Dict[str, DataOrchestrator] = {}

    def create_orchestrator(self,
                           engine: Engine,
                           orchestrator_id: str = "default") -> DataOrchestrator:
        """
        Create a data orchestrator instance with the provided engine.

        Args:
            engine: Pre-configured SQLAlchemy engine
            orchestrator_id: Unique identifier for this orchestrator

        Returns:
            Configured DataOrchestrator instance

        Raises:
            ValueError: If engine is None
        """
        if engine is None:
            raise ValueError("SQLAlchemy engine cannot be None")

        # Check if orchestrator already exists
        if orchestrator_id in self._orchestrators:
            self.logger.info(f"Returning existing orchestrator: {orchestrator_id}")
            return self._orchestrators[orchestrator_id]

        # Create orchestrator with provided engine
        orchestrator = DataOrchestrator(engine)
        
        # Cache the orchestrator
        self._orchestrators[orchestrator_id] = orchestrator
        
        self.logger.info(
            f"Created data orchestrator: {orchestrator_id}",
            engine_url=str(engine.url).split('@')[0] + '@***'  # Hide credentials in logs
        )
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

    def load_configuration(self,
                          config_path: Optional[str] = None,
                          config_dict: Optional[Dict[str, Any]] = None) -> DataLoaderConfiguration:
        """
        Load configuration from file or dictionary.

        Args:
            config_path: Path to YAML configuration file
            config_dict: Configuration dictionary

        Returns:
            DataLoaderConfiguration instance

        Raises:
            ValueError: If neither config_path nor config_dict is provided
            FileNotFoundError: If config_path doesn't exist
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

    def close_all(self):
        """Close all orchestrators and clean up resources."""
        # Note: We don't dispose engines here since they're externally managed
        # The client is responsible for engine lifecycle management
        
        self._orchestrators.clear()
        self.logger.info("All orchestrators cleaned up")

    def get_active_orchestrators(self) -> Dict[str, DataOrchestrator]:
        """
        Get all active orchestrators.
        
        Returns:
            Dictionary of active orchestrators
        """
        return self._orchestrators.copy()

    def remove_orchestrator(self, orchestrator_id: str) -> bool:
        """
        Remove a specific orchestrator from cache.
        
        Args:
            orchestrator_id: ID of orchestrator to remove
            
        Returns:
            True if orchestrator was removed, False if not found
        """
        if orchestrator_id in self._orchestrators:
            del self._orchestrators[orchestrator_id]
            self.logger.info(f"Removed orchestrator: {orchestrator_id}")
            return True
        return False
