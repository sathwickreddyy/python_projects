# orchestrators/data_orchestrator.py
"""
Enhanced data orchestrator supporting both SQLAlchemy engines and direct database connections.

This orchestrator provides flexible database connectivity options:
1. Direct database connections (highest preference) - for corporate networks with restrictions
2. SQLAlchemy engines (fallback) - for standard usage

@author sathwick
"""
from datetime import datetime
from typing import Dict, Iterator, Optional, Union, Any
from sqlalchemy.engine import Engine
from config.data_loader_config import DataLoaderConfiguration, DataSourceDefinition
from converters.data_type_converter import DataTypeConverter
from models.core.base_types import LoadingStats, DataSourceType
from models.core.exceptions import DataIngestionException
from models.core.logging_config import DataIngestionLogger
from data_loaders.csv_loader import CSVDataLoader
from data_loaders.json_loader import JSONDataLoader
from models.data_record import DataRecord
from processors.data_processor import DataProcessor
from writers.database_writer import DatabaseWriter
from writers.database_writer_db2 import DB2DatabaseWriter


class DataOrchestrator:
    """
    Flexible data orchestrator supporting multiple database connectivity patterns.

    This orchestrator coordinates the entire data loading pipeline:
    1. Data loading from various sources (CSV, JSON, etc.)
    2. Data processing and validation
    3. Database writing with flexible connectivity (connection vs engine)
    4. Error handling and monitoring
    5. Statistics collection and reporting

    Connection Priority:
    -------------------
    1. Direct database connection (highest preference)
    2. SQLAlchemy engine (fallback)
    3. No database connectivity (print-only mode)

    Usage Examples:
    --------------
    # With direct DB2 connection (preferred for corporate networks)
    import ms.db2
    conn = ms.db2.connect(SERVER_NAME)
    orchestrator = DataOrchestrator(db_connection=conn)

    # With SQLAlchemy engine (standard usage)
    from sqlalchemy import create_engine
    engine = create_engine(database_url)
    orchestrator = DataOrchestrator(engine=engine)

    # Without database (print-only mode)
    orchestrator = DataOrchestrator()
    """

    def __init__(self, 
                 engine: Optional[Engine] = None, 
                 db_connection: Optional[Any] = None,
                 connection_type: str = "db2"):
        """
        Initialize data orchestrator with flexible database connectivity.

        Args:
            engine: SQLAlchemy engine for database operations (fallback option)
            db_connection: Direct database connection object (highest preference)
            connection_type: Type of direct connection ("db2", "postgres", etc.)

        Note:
            - If both engine and db_connection are provided, db_connection takes precedence
            - If neither is provided, orchestrator works in print-only mode
            - connection_type is used to select appropriate writer implementation
        """
        self.engine = engine
        self.db_connection = db_connection
        self.connection_type = connection_type.lower()
        self.logger = DataIngestionLogger(__name__)

        # Determine database connectivity mode
        self.database_mode = self._determine_database_mode()
        
        self.logger.info(
            "Data orchestrator initialized",
            database_mode=self.database_mode,
            connection_type=self.connection_type if self.db_connection else "N/A",
            has_engine=self.engine is not None
        )

        # Initialize core components
        self.data_type_converter = DataTypeConverter()
        self.data_processor = DataProcessor(self.data_type_converter)
        
        # Initialize database writer based on connectivity mode
        self.database_writer = self._initialize_database_writer()

        # Initialize data loaders
        self.loaders = {
            DataSourceType.CSV: CSVDataLoader(),
            DataSourceType.JSON: JSONDataLoader(),
            # Add more loaders as needed
        }

    def _determine_database_mode(self) -> str:
        """
        Determine database connectivity mode based on available connections.
        
        Returns:
            str: Database mode ("connection", "engine", "print_only")
        """
        if self.db_connection is not None:
            return "connection"
        elif self.engine is not None:
            return "engine"
        else:
            return "print_only"

    def _initialize_database_writer(self):
        """
        Initialize appropriate database writer based on connectivity mode.
        
        Returns:
            Database writer instance or None for print-only mode
        """
        if self.database_mode == "connection":
            # Use connection-based writer (highest preference)
            if self.connection_type == "db2":
                self.logger.info("Initializing DB2 connection-based database writer")
                return DB2DatabaseWriter(self.db_connection)
            else:
                # Add support for other connection types here
                raise DataIngestionException(f"Unsupported connection type: {self.connection_type}")
        
        elif self.database_mode == "engine":
            # Use SQLAlchemy-based writer (fallback)
            self.logger.info("Initializing SQLAlchemy engine-based database writer")
            return DatabaseWriter(self.engine)
        
        else:
            # Print-only mode - no database writer needed
            self.logger.info("Initializing in print-only mode (no database connectivity)")
            return None

    def execute_data_loading(self, config: DataLoaderConfiguration, data_source_name: str) -> LoadingStats:
        """
        Execute data loading with support for multiple database connectivity modes.
        
        This method handles the complete data loading pipeline regardless of the
        underlying database connectivity approach.
        """
        if data_source_name not in config.data_sources:
            raise DataIngestionException(f"Data source '{data_source_name}' not found in configuration")

        data_source_config = config.data_sources[data_source_name]
        start_time = datetime.now()

        self.logger.info(
            "Starting data loading execution",
            data_source=data_source_name,
            source_type=data_source_config.type,
            target_table=data_source_config.target_config.table,
            target_enabled=data_source_config.target_config.enabled,
            database_mode=self.database_mode
        )

        try:
            # Step 1: Load data from source
            read_start = datetime.now()
            data_stream = self._load_data_from_source(data_source_config)
            read_end = datetime.now()
            read_time_ms = int((read_end - read_start).total_seconds() * 1000)

            # Step 2: Process data and collect all records
            process_start = datetime.now()
            processed_records = list(self._process_data_stream(data_stream, data_source_config))
            process_end = datetime.now()
            process_time_ms = int((process_end - process_start).total_seconds() * 1000)

            # Get processing stats from processor
            processing_stats = self.data_processor.get_processing_stats() if hasattr(
                self.data_processor, 'get_processing_stats') else None

            # Step 3: Write to database or print based on connectivity and configuration
            write_stats = self._execute_database_write(processed_records, data_source_config)
            write_end = datetime.now()

            # Merge statistics from processing and writing
            final_stats = self._merge_statistics(
                processing_stats, write_stats, read_time_ms, process_time_ms, write_end
            )

            self.logger.info(
                "Data loading execution completed",
                data_source=data_source_name,
                database_mode=self.database_mode,
                total_records=final_stats.total_records,
                successful_records=final_stats.successful_records,
                error_records=final_stats.error_records,
                total_errors=len(final_stats.get_all_errors()),
                success_rate=final_stats.success_rate
            )

            return final_stats

        except Exception as e:
            self.logger.error(
                "Data loading execution failed",
                data_source=data_source_name,
                database_mode=self.database_mode,
                error_message=str(e)
            )
            raise DataIngestionException(f"Data loading failed for '{data_source_name}': {str(e)}", e)

    def _execute_database_write(self, processed_records: list, data_source_config: DataSourceDefinition) -> LoadingStats:
        """
        Execute database write operation based on available connectivity.
        
        This method handles different database connectivity modes:
        1. Connection-based writing (DB2, etc.)
        2. Engine-based writing (SQLAlchemy)
        3. Print-only mode (no database connectivity)
        """
        target = data_source_config.target_config
        
        # Check if target is disabled or no database connectivity
        if not target.enabled or self.database_mode == "print_only":
            self.logger.info(
                "Executing in print mode",
                target_enabled=target.enabled,
                database_mode=self.database_mode
            )
            return self._print_sample_records(iter(processed_records), data_source_config)
        
        # Execute database write using appropriate writer
        if self.database_writer:
            return self.database_writer.write_data(iter(processed_records), data_source_config)
        else:
            # Fallback to print mode if no database writer available
            self.logger.warning("No database writer available, falling back to print mode")
            return self._print_sample_records(iter(processed_records), data_source_config)

    def _merge_statistics(self, processing_stats: Optional[LoadingStats], 
                         write_stats: LoadingStats, 
                         read_time_ms: int, 
                         process_time_ms: int, 
                         write_end: datetime) -> LoadingStats:
        """
        Merge statistics from processing and writing phases.
        
        This method creates a comprehensive LoadingStats object that includes
        metrics from all phases of the data loading pipeline.
        """
        if processing_stats:
            # Update processing stats with write results
            processing_stats.read_time_ms = read_time_ms
            processing_stats.process_time_ms = process_time_ms
            processing_stats.write_time_ms = write_stats.write_time_ms
            processing_stats.batch_count = write_stats.batch_count
            processing_stats.records_per_second = write_stats.records_per_second
            processing_stats.execution_time = write_end

            # Handle error reconciliation between processing and writing
            if processing_stats.error_records == 0 and write_stats.error_records > 0:
                self.logger.error("No processing errors found, overriding with database write errors")
                processing_stats.error_records = write_stats.error_records
                processing_stats.successful_records = write_stats.successful_records
                
                # Add write errors to processing stats
                if hasattr(write_stats, 'get_all_errors'):
                    for error in write_stats.get_all_errors():
                        processing_stats.processing_errors.append(f"Database write error: {error}")

            # Update derived fields
            processing_stats.has_errors = len(processing_stats.get_all_errors()) > 0
            processing_stats.success_rate = (
                processing_stats.successful_records / processing_stats.total_records * 100
            ) if processing_stats.total_records > 0 else 0.0

            return processing_stats
        else:
            # Return write stats if no processing stats available
            return write_stats

    def execute_all_data_sources(self, config: DataLoaderConfiguration) -> Dict[str, LoadingStats]:
        """
        Execute data loading for all configured data sources.

        This method works with any database connectivity mode and provides
        comprehensive statistics for each data source processed.
        """
        results = {}

        self.logger.info(
            "Starting execution of all data sources",
            total_sources=len(config.data_sources),
            database_mode=self.database_mode
        )

        for data_source_name in config.data_sources:
            try:
                self.logger.info(f"Processing data source: {data_source_name}")
                stats = self.execute_data_loading(config, data_source_name)
                results[data_source_name] = stats
                
                self.logger.info(
                    f"Data source completed: {data_source_name}",
                    successful_records=stats.successful_records,
                    error_records=stats.error_records
                )
                
            except Exception as e:
                self.logger.error(
                    "Failed to execute data source",
                    data_source=data_source_name,
                    error_message=str(e)
                )
                # Continue with other data sources rather than failing completely

        # Log overall summary
        total_records = sum(stats.total_records for stats in results.values())
        successful_records = sum(stats.successful_records for stats in results.values())
        
        self.logger.info(
            "All data sources execution completed",
            processed_sources=len(results),
            total_sources=len(config.data_sources),
            total_records=total_records,
            successful_records=successful_records,
            overall_success_rate=f"{(successful_records/total_records*100):.1f}%" if total_records > 0 else "0%"
        )

        return results

    def _load_data_from_source(self, config: DataSourceDefinition) -> Iterator[DataRecord]:
        """Load data from configured source using appropriate loader."""
        loader = self.loaders.get(config.type)
        if not loader:
            raise DataIngestionException(f"No loader available for type: {config.type.value}")

        return loader.load_data(config)

    def _process_data_stream(self, data_stream: Iterator[DataRecord],
                           config: DataSourceDefinition) -> Iterator[DataRecord]:
        """Process data stream with transformations and validation."""
        return self.data_processor.process_data(data_stream, config)

    def _print_sample_records(self, data_stream: Iterator[DataRecord],
                            config: DataSourceDefinition) -> LoadingStats:
        """
        Print sample records when database writing is not available or disabled.
        
        This method provides detailed output for debugging and validation purposes.
        """
        start_time = datetime.now()
        
        print(f"\n{'=' * 80}")
        print(f"📋 SAMPLE RECORDS FOR {config.type.value} SOURCE")
        print(f"   Target: {config.target_config.schema_name}.{config.target_config.table}")
        print(f"   Mode: {self.database_mode.upper()}")
        print(f"   Enabled: {config.target_config.enabled}")
        print(f"{'=' * 80}")

        records_processed = 0
        valid_records = 0
        error_records = 0
        sample_count = 0
        max_samples = 15

        for record in data_stream:
            records_processed += 1

            if not record.is_valid():
                error_records += 1
                print(f"❌ Record {record.row_number}: {record.error_message}")
                continue

            valid_records += 1

            if sample_count < max_samples:
                sample_count += 1
                print(f"\n📄 Record {record.row_number}:")
                for key, value in record.get_data().items():
                    print(f"   {key}: {value}")
                print("-" * 60)

        if records_processed > max_samples:
            remaining = records_processed - sample_count
            print(f"\n... and {remaining} more records")

        print(f"\n📊 SUMMARY:")
        print(f"   Total records: {records_processed}")
        print(f"   Valid records: {valid_records}")
        print(f"   Invalid records: {error_records}")
        print(f"   Samples shown: {min(sample_count, max_samples)}")
        print(f"   Database mode: {self.database_mode}")
        print(f"={'=' * 80}")

        end_time = datetime.now()
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        return LoadingStats(
            total_records=records_processed,
            successful_records=0,  # No actual database writes in print mode
            error_records=error_records,
            write_time_ms=duration_ms,
            execution_time=end_time
        )

    def get_database_info(self) -> Dict[str, Any]:
        """
        Get information about the current database connectivity.
        
        Returns:
            Dictionary with database connectivity information
        """
        info = {
            "database_mode": self.database_mode,
            "connection_type": self.connection_type if self.db_connection else None,
            "has_engine": self.engine is not None,
            "has_connection": self.db_connection is not None,
            "writer_type": type(self.database_writer).__name__ if self.database_writer else "None"
        }
        
        # Add engine-specific information if available
        if self.engine:
            try:
                info.update({
                    "engine_url": str(self.engine.url).split('@')[0] + '@***',
                    "engine_driver": self.engine.dialect.name
                })
            except Exception:
                pass
        
        return info

    def close(self):
        """
        Clean up resources and close connections.
        
        This method handles cleanup for both connection-based and engine-based modes.
        """
        try:
            # Close database writer resources
            if self.database_writer and hasattr(self.database_writer, 'close'):
                self.database_writer.close()
            
            # Close direct database connection if we're managing it
            if self.db_connection and hasattr(self.db_connection, 'close'):
                self.db_connection.close()
                self.logger.info("Database connection closed")
            
            # Note: Engine disposal is handled by the client/factory
            
            self.logger.info("Data orchestrator resources cleaned up")
            
        except Exception as e:
            self.logger.warning(f"Error during orchestrator cleanup: {e}")
