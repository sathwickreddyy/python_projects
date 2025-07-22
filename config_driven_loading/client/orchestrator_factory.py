# client/orchestrator_factory.py
"""
Enhanced factory for creating data orchestrator instances with flexible database connectivity.

This factory supports multiple database connectivity patterns:
1. Direct database connections (highest preference)
2. SQLAlchemy engines (standard approach)
3. No database connectivity (print-only mode)

@author sathwick
"""
from pathlib import Path
from typing import Dict, Any, Optional, Union
import yaml
from sqlalchemy import Engine
from config.data_loader_config import DataLoaderConfiguration
from models.core.logging_config import DataIngestionLogger
from orchestrators.data_orchestrator import DataOrchestrator


class DataIngestionFactory:
    """
    Enhanced factory class supporting flexible database connectivity options.

    This factory provides multiple ways to create data orchestrators:
    1. With direct database connections (preferred for corporate networks)
    2. With SQLAlchemy engines (standard usage)
    3. Without database connectivity (print-only mode for testing/validation)

    Connection Priority:
    -------------------
    1. Direct database connection (highest preference)
    2. SQLAlchemy engine (fallback)
    3. No database connectivity (automatic print-only mode)

    Usage Examples:
    --------------
    # With direct DB2 connection (corporate network friendly)
    import ms.db2
    conn = ms.db2.connect(SERVER_NAME)
    orchestrator = factory.create_orchestrator(db_connection=conn)

    # With SQLAlchemy engine (standard)
    engine = create_engine(database_url)
    orchestrator = factory.create_orchestrator(engine=engine)

    # Print-only mode (no database required)
    orchestrator = factory.create_orchestrator()
    """

    def __init__(self):
        self.logger = DataIngestionLogger(__name__)
        self._orchestrators: Dict[str, DataOrchestrator] = {}

    def create_orchestrator(self,
                            engine: Optional[Engine] = None,
                            db_connection: Optional[Any] = None,
                            connection_type: str = "db2",
                            orchestrator_id: str = "default") -> DataOrchestrator:
        """
        Create a data orchestrator with flexible database connectivity options.

        Args:
            engine: Pre-configured SQLAlchemy engine (fallback option)
            db_connection: Direct database connection object (highest preference)
            connection_type: Type of direct connection ("db2", "postgres", etc.)
            orchestrator_id: Unique identifier for this orchestrator

        Returns:
            Configured DataOrchestrator instance

        Note:
            - If both engine and db_connection are provided, db_connection takes precedence
            - If neither is provided, orchestrator works in print-only mode
            - This allows flexible usage in different network environments
        """
        # Check if orchestrator already exists
        if orchestrator_id in self._orchestrators:
            self.logger.info(f"Returning existing orchestrator: {orchestrator_id}")
            return self._orchestrators[orchestrator_id]

        # Determine connectivity mode for logging
        connectivity_mode = self._determine_connectivity_mode(engine, db_connection)

        self.logger.info(
            f"Creating data orchestrator: {orchestrator_id}",
            connectivity_mode=connectivity_mode,
            connection_type=connection_type if db_connection else "N/A",
            has_engine=engine is not None
        )

        # Create orchestrator with flexible connectivity
        orchestrator = DataOrchestrator(
            engine=engine,
            db_connection=db_connection,
            connection_type=connection_type
        )

        # Cache the orchestrator
        self._orchestrators[orchestrator_id] = orchestrator

        self.logger.info(
            f"Data orchestrator created successfully: {orchestrator_id}",
            connectivity_mode=connectivity_mode
        )

        return orchestrator

    def create_db2_orchestrator(self,
                                db_connection: Any,
                                orchestrator_id: str = "db2_default") -> DataOrchestrator:
        """
        Convenience method to create DB2-specific orchestrator with direct connection.

        Args:
            db_connection: Active DB2 connection from ms.db2.connect()
            orchestrator_id: Unique identifier for this orchestrator

        Returns:
            DataOrchestrator configured for DB2 connection-based operations

        Example:
            import ms.db2
            conn = ms.db2.connect(SERVER_NAME)
            orchestrator = factory.create_db2_orchestrator(conn)
        """
        return self.create_orchestrator(
            db_connection=db_connection,
            connection_type="db2",
            orchestrator_id=orchestrator_id
        )

    def create_engine_orchestrator(self,
                                   engine: Engine,
                                   orchestrator_id: str = "engine_default") -> DataOrchestrator:
        """
        Convenience method to create orchestrator with SQLAlchemy engine.

        Args:
            engine: Pre-configured SQLAlchemy engine
            orchestrator_id: Unique identifier for this orchestrator

        Returns:
            DataOrchestrator configured for SQLAlchemy engine operations

        Raises:
            ValueError: If engine is None
        """
        if engine is None:
            raise ValueError("SQLAlchemy engine cannot be None")

        return self.create_orchestrator(
            engine=engine,
            orchestrator_id=orchestrator_id
        )

    def create_print_only_orchestrator(self,
                                       orchestrator_id: str = "print_only_default") -> DataOrchestrator:
        """
        Convenience method to create print-only orchestrator (no database connectivity).

        This is useful for:
        - Testing and validation without database
        - Data preview and debugging
        - Environments where database access is restricted

        Args:
            orchestrator_id: Unique identifier for this orchestrator

        Returns:
            DataOrchestrator configured for print-only operations
        """
        return self.create_orchestrator(orchestrator_id=orchestrator_id)

    def _determine_connectivity_mode(self, engine: Optional[Engine], db_connection: Optional[Any]) -> str:
        """Determine connectivity mode for logging purposes."""
        if db_connection is not None:
            return "direct_connection"
        elif engine is not None:
            return "sqlalchemy_engine"
        else:
            return "print_only"

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
                           config_dict: Optional[Dict[str, Any]] = None,
                           auto_disable_targets: bool = False) -> DataLoaderConfiguration:
        """
        Load configuration from file or dictionary with optional target auto-disable.

        Args:
            config_path: Path to YAML configuration file
            config_dict: Configuration dictionary
            auto_disable_targets: If True, automatically set all targets to disabled
                                (useful for print-only mode testing)

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

        # Auto-disable targets if requested (useful for print-only mode)
        if auto_disable_targets:
            self.logger.info("Auto-disabling all target configurations")
            for source_name, source_config in config_data.get("data_sources", {}).items():
                if "target_config" in source_config:
                    source_config["target_config"]["enabled"] = False
                    self.logger.debug(f"Disabled target for data source: {source_name}")

        return DataLoaderConfiguration(**config_data)

    def validate_connectivity(self, orchestrator_id: str = "default") -> Dict[str, Any]:
        """
        Validate database connectivity for a specific orchestrator.

        Args:
            orchestrator_id: Orchestrator identifier

        Returns:
            Dictionary with connectivity validation results
        """
        orchestrator = self.get_orchestrator(orchestrator_id)
        if not orchestrator:
            return {
                "valid": False,
                "error": f"Orchestrator '{orchestrator_id}' not found"
            }

        try:
            db_info = orchestrator.get_database_info()

            validation_result = {
                "valid": True,
                "orchestrator_id": orchestrator_id,
                **db_info
            }

            # Perform basic connectivity test if possible
            if orchestrator.database_mode == "connection":
                validation_result["connection_test"] = "Direct connection available"
            elif orchestrator.database_mode == "engine":
                validation_result["connection_test"] = "SQLAlchemy engine available"
            else:
                validation_result["connection_test"] = "Print-only mode (no database connectivity)"

            return validation_result

        except Exception as e:
            return {
                "valid": False,
                "orchestrator_id": orchestrator_id,
                "error": str(e)
            }

    def close_all(self):
        """Close all orchestrators and clean up resources."""
        self.logger.info(f"Closing {len(self._orchestrators)} orchestrators")

        for orchestrator_id, orchestrator in self._orchestrators.items():
            try:
                orchestrator.close()
                self.logger.debug(f"Closed orchestrator: {orchestrator_id}")
            except Exception as e:
                self.logger.warning(f"Error closing orchestrator {orchestrator_id}: {e}")

        self._orchestrators.clear()
        self.logger.info("All orchestrators cleaned up")

    def get_active_orchestrators(self) -> Dict[str, DataOrchestrator]:
        """
        Get all active orchestrators with their connectivity information.

        Returns:
            Dictionary of active orchestrators with metadata
        """
        active_orchestrators = {}

        for orchestrator_id, orchestrator in self._orchestrators.items():
            try:
                db_info = orchestrator.get_database_info()
                active_orchestrators[orchestrator_id] = {
                    "orchestrator": orchestrator,
                    "database_info": db_info
                }
            except Exception as e:
                active_orchestrators[orchestrator_id] = {
                    "orchestrator": orchestrator,
                    "database_info": {"error": str(e)}
                }

        return active_orchestrators

    def remove_orchestrator(self, orchestrator_id: str) -> bool:
        """
        Remove a specific orchestrator from cache with proper cleanup.

        Args:
            orchestrator_id: ID of orchestrator to remove

        Returns:
            True if orchestrator was removed, False if not found
        """
        if orchestrator_id in self._orchestrators:
            try:
                # Close the orchestrator before removing
                orchestrator = self._orchestrators[orchestrator_id]
                orchestrator.close()

                del self._orchestrators[orchestrator_id]
                self.logger.info(f"Removed orchestrator: {orchestrator_id}")
                return True

            except Exception as e:
                self.logger.error(f"Error removing orchestrator {orchestrator_id}: {e}")
                # Remove anyway to avoid memory leaks
                del self._orchestrators[orchestrator_id]
                return True

        return False

    def create_orchestrator_from_config(self,
                                        config_data: Dict[str, Any],
                                        **orchestrator_kwargs) -> DataOrchestrator:
        """
        Create orchestrator with automatic target disabling if no database connectivity.

        This method analyzes the provided orchestrator arguments and automatically
        disables targets if running in print-only mode.

        Args:
            config_data: Configuration dictionary
            **orchestrator_kwargs: Arguments for orchestrator creation

        Returns:
            DataOrchestrator instance with appropriate configuration
        """
        # Determine if we have database connectivity
        has_db_connectivity = (
                orchestrator_kwargs.get('engine') is not None or
                orchestrator_kwargs.get('db_connection') is not None
        )

        # Auto-disable targets if no database connectivity
        auto_disable = not has_db_connectivity

        if auto_disable:
            self.logger.info("No database connectivity detected, enabling print-only mode")

        # Load configuration
        config = DataLoaderConfiguration(**config_data)
        if auto_disable:
            # Modify configuration to disable all targets
            for source_name, source_def in config.data_sources.items():
                source_def.target_config.enabled = False
                self.logger.debug(f"Auto-disabled target for: {source_name}")

        # Create orchestrator
        orchestrator = self.create_orchestrator(**orchestrator_kwargs)

        return orchestrator
