"""
Base data loader interface and common functionality.

@author sathwick
"""
from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any
from models.data_record import DataRecord
from config.data_loader_config import DataSourceDefinition
from core.logging_config import DataIngestionLogger

class BaseDataLoader(ABC):
    """
    Abstract base class for all data sources.

    This class defines the interface that all data loaders must implement and provides
    common functionality for data loading operations.
    """

    def __init__(self):
        self.logger = DataIngestionLogger(self.__class__.__name__)

    @abstractmethod
    def get_type(self) -> str:
        """Return the type identifier for this loader."""
        pass

    @abstractmethod
    def load_data(self, config: DataSourceDefinition) -> Iterator[DataRecord]:
        """
        Load data from the configured data source.
        Args:
            config: Data source configuration

        Yields:
            DataRecord objects representing loaded data

        Raises:
            DataLoadingException: If data loading fails
        :param config:  Data source configuration
        :return:  Data records
        """
        pass

    def validate_config(self, config: DataSourceDefinition) -> None:
        """
        Validate configuration for this loader type.

        Args:
            config: Data source configuration to validate

        Raises:
            ConfigurationException: If configuration is invalid
        """
        if config.type.value != self.get_type():
            raise ValueError(f"Invalid configuration type. Expected {self.get_type()}, got {config.type.value}")

    def _create_data_record(self, data: Dict[str, Any], row_number: int) -> DataRecord:
        """Create a valid data record."""
        return DataRecord.create_valid(data, row_number)

    def _create_error_record(self, data: Dict[str, Any], row_number: int, error_message: str) -> DataRecord:
        """Create an invalid data record with error information."""
        return DataRecord.create_invalid(data, row_number, error_message)