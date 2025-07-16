"""
Data orchestrator for managing end-to-end data loading operations.

@author sathwick
"""
from typing import Dict, Iterator
from sqlalchemy.engine import Engine
from config.data_loader_config import DataLoaderConfiguration, DataSourceDefinition
from converters.data_type_converter import DataTypeConverter
from core.base_types import LoadingStats, DataSourceType
from core.exceptions import DataIngestionException
from core.logging_config import DataIngestionLogger
from data_loaders.csv_loader import CSVDataLoader
from data_loaders.json_loader import JSONDataLoader
from models.data_record import DataRecord
from processors.data_processor import DataProcessor
from writers.database_writer import DatabaseWriter


class DataOrchestrator:
    """
    Main orchestrator for data loading operations.

    This class coordinates the entire data loading pipeline:
    1. Data loading from various sources
    2. Data processing and validation
    3. Database writing with transaction management
    4. Error handling and monitoring
    """

    def __init__(self, engine: Engine):
        """
        Initialize data orchestrator with database engine.

        Args:
            engine: SQLAlchemy engine for database operations
        """
        self.engine = engine
        self.logger = DataIngestionLogger(__name__)

        # Initialize components
        self.data_type_converter = DataTypeConverter()
        self.data_processor = DataProcessor(self.data_type_converter)
        self.database_writer = DatabaseWriter(engine)

        # Initialize loaders
        self.loaders = {
            DataSourceType.CSV: CSVDataLoader(),
            DataSourceType.JSON: JSONDataLoader(),
            # Add more loaders as needed
        }

    def execute_data_loading(self, config: DataLoaderConfiguration, data_source_name: str) -> LoadingStats:
        """
        Execute data loading for a specific data source.

        Args:
            config: Complete data loading configuration
            data_source_name: Name of the data source to process

        Returns:
            LoadingStats with execution metrics

        Raises:
            DataIngestionException: If data loading fails
        """
        if data_source_name not in config.data_sources:
            raise DataIngestionException(f"Data source '{data_source_name}' not found in configuration")

        data_source_config = config.data_sources[data_source_name]

        self.logger.info(
            "Starting data loading execution",
            data_source=data_source_name,
            source_type=data_source_config.type,
            target_table=data_source_config.target.table
        )

        try:
            # Step 1: Load data from source
            data_stream = self._load_data_from_source(data_source_config)

            # Step 2: Process data (transformation, validation)
            processed_stream = self._process_data_stream(data_stream, data_source_config)

            # Step 3: Write to database
            stats = self.database_writer.write_data(processed_stream, data_source_config)

            self.logger.info(
                "Data loading execution completed",
                data_source=data_source_name,
                **stats.model_dump()
            )

            return stats

        except Exception as e:
            self.logger.error(
                "Data loading execution failed",
                data_source=data_source_name,
                error_message=str(e)
            )
            raise DataIngestionException(f"Data loading failed for '{data_source_name}': {str(e)}", e)

    def execute_all_data_sources(self, config: DataLoaderConfiguration) -> Dict[str, LoadingStats]:
        """
        Execute data loading for all configured data sources.

        Args:
            config: Complete data loading configuration

        Returns:
            Dictionary mapping data source names to their loading statistics
        """
        results = {}

        for data_source_name in config.data_sources:
            try:
                stats = self.execute_data_loading(config, data_source_name)
                results[data_source_name] = stats
            except Exception as e:
                self.logger.error(
                    "Failed to execute data source",
                    data_source=data_source_name,
                    error_message=str(e)
                )
                # Continue with other data sources

        return results

    # def load_models_to_database(self, models: List[Any], table_name: str, schema_name: str = "public") -> LoadingStats:
    #     """
    #     Load a list of model objects directly to database.
    #
    #     Args:
    #         models: List of model objects to load
    #         table_name: Target table name
    #         schema_name: Target schema name
    #
    #     Returns:
    #         LoadingStats with execution metrics
    #     """
    #     self.logger.info(
    #         "Loading models to database",
    #         model_count=len(models),
    #         table_name=table_name,
    #         schema_name=schema_name
    #     )
    #
    #     try:
    #         # Convert models to data records
    #         data_records = self._convert_models_to_records(models)
    #
    #         # Create temporary configuration
    #         from config.data_loader_config import TargetConfig
    #         target_config = TargetConfig(
    #             schema_name=schema_name,
    #             table=table_name,
    #             batch_size=1000
    #         )
    #
    #         # Create temporary data source definition
    #         temp_config = type('TempConfig', (), {
    #             'target': target_config,
    #             'type': 'MODEL',
    #             'identifier': f'model_loading_{table_name}'
    #         })()
    #
    #         # Write to database
    #         stats = self.database_writer.write_data(iter(data_records), temp_config)
    #
    #         self.logger.info(
    #             "Models loaded to database successfully",
    #             **stats.dict()
    #         )
    #
    #         return stats
    #
    #     except Exception as e:
    #         self.logger.error(
    #             "Failed to load models to database",
    #             error_message=str(e)
    #         )
    #         raise DataIngestionException(f"Model loading failed: {str(e)}", e)
    #
    def _load_data_from_source(self, config: DataSourceDefinition) -> Iterator[DataRecord]:
        """Load data from configured source."""
        loader = self.loaders.get(config.type.value)
        if not loader:
            raise DataIngestionException(f"No loader available for type: {config.type.value}")

        return loader.load_data(config)

    def _process_data_stream(self, data_stream: Iterator[DataRecord],
                             config: DataSourceDefinition) -> Iterator[DataRecord]:
        """Process data stream with transformations and validation."""
        return self.data_processor.process_data(data_stream, config)

    # def _convert_models_to_records(self, models: List[Any]) -> List[DataRecord]:
    #     """Convert model objects to DataRecord objects."""
    #     records = []
    #
    #     for i, model in enumerate(models, 1):
    #         try:
    #             # Convert model to dictionary
    #             if hasattr(model, 'dict'):
    #                 # Pydantic model
    #                 data = model.dict()
    #             elif hasattr(model, '__dict__'):
    #                 # Regular class
    #                 data = model.__dict__
    #             else:
    #                 # Fallback - try to convert to dict
    #                 data = dict(model)
    #
    #             records.append(DataRecord.create_valid(data, i))
    #
    #         except Exception as e:
    #             self.logger.error(
    #                 "Failed to convert model to record",
    #                 model_index=i,
    #                 error_message=str(e)
    #             )
    #             records.append(DataRecord.create_invalid(
    #                 {}, i, f"Model conversion error: {str(e)}"
    #             ))
    #
    #     return records
