# writers/database_writer.py
"""
Database writer with comprehensive batch processing, schema validation, and transaction management.

This class handles writing streaming data into a database efficiently using batch inserts, validates schema integrity,
and provides audit logging for traceability.

Summary of the flow:
    DataStream (Iterator<DataRecord>)
    └── write_data()
         ├── Reflect DB schema → validate columns
         ├── For each record:
         │    ├── Validate record
         │    ├── Buffer valid records into batch
         │    ├── When batch is full → _execute_batch()
         │         ├── Prepare INSERT SQL
         │         ├── Filter columns from schema
         │         └── Execute batch insert via SQLAlchemy
         ├── After loop → process any remaining batch
         ├── Commit transaction
         ├── Compute performance stats (duration, throughput, counts)
         └── Record audit trail into audit table

@author sathwick
"""

from typing import Iterator, List, Dict, Any, Set
from datetime import datetime
from sqlalchemy import text, MetaData, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError
from models.data_record import DataRecord
from config.data_loader_config import DataSourceDefinition
from models.core.base_types import LoadingStats
from models.core.exceptions import DatabaseWriteException
from models.core.logging_config import DataIngestionLogger


class DatabaseWriter:
    """
    High-performance, schema-validated database writer with:
    - Batch processing with configurable batch sizes
    - Schema column validation via reflection
    - Transaction management with automatic rollback
    - Comprehensive logging and audit trail
    - Column filtering to prevent SQL injection
    - Performance monitoring and metrics collection
    """

    def __init__(self, engine: Engine):
        """
        Initialize with SQLAlchemy engine.

        Args:
            engine (Engine): SQLAlchemy Engine for database connectivity.
        """
        self.engine = engine
        self.logger = DataIngestionLogger(__name__)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        # Cache for validated columns to avoid repeated reflection
        self._column_cache: Dict[str, List[str]] = {}

    def write_data(self, data_stream: Iterator[DataRecord], config: DataSourceDefinition) -> LoadingStats:
        """
        Write data stream to target database table with batch processing and transaction handling.

        Args:
            data_stream (Iterator[DataRecord]): Stream of data records to write.
            config (DataSourceDefinition): Configuration holding target schema and table details.

        Returns:
            LoadingStats: Contains detailed metrics about the write operation.

        Raises:
            DatabaseWriteException: Raised if database operations fail.
        """
        start_time = datetime.now()
        target = config.target_config
        batch_size = target.batch_size or 1000

        self.logger.info(
            "Starting database write",
            schema=target.schema_name,
            table=target.table,
            batch_size=batch_size,
            enabled=target.enabled
        )

        # Reflect schema to validate columns before starting
        valid_columns = self._get_validated_columns(target)
        if not valid_columns:
            raise DatabaseWriteException(f"No valid columns found for table '{target.schema_name}.{target.table}'")

        total_records = 0
        successful_records = 0
        error_records = 0
        batch_count = 0

        try:
            with self.SessionLocal() as session:
                batch = []
                
                for record in data_stream:
                    total_records += 1

                    if not record.is_valid():
                        error_records += 1
                        self.logger.warning(
                            "Skipping invalid record",
                            row_number=record.row_number,
                            error_message=record.error_message
                        )
                        continue

                    batch.append(record)

                    if len(batch) >= batch_size:
                        success_count = self._execute_batch(session, batch, target, valid_columns)
                        successful_records += success_count
                        batch_count += 1
                        batch.clear()

                # Process remaining records
                if batch:
                    success_count = self._execute_batch(session, batch, target, valid_columns)
                    successful_records += success_count
                    batch_count += 1

                session.commit()

            end_time = datetime.now()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            records_per_second = successful_records / (duration_ms / 1000) if duration_ms > 0 else 0

            stats = LoadingStats(
                write_time_ms=duration_ms,
                batch_count=batch_count,
                records_per_second=records_per_second,
                total_records=total_records,
                successful_records=successful_records,
                error_records=error_records,
                execution_time=end_time
            )

            self.logger.info(
                "Database write completed successfully",
                **stats.dict()
            )

            # Record audit trail
            self._record_audit_trail(config, stats)

            return stats

        except Exception as e:
            self.logger.error(
                "Database write failed",
                error_message=str(e),
                table=f"{target.schema_name}.{target.table}"
            )
            raise DatabaseWriteException(f"Database write failed: {str(e)}", e)

    def _get_validated_columns(self, target) -> List[str]:
        """
        Get validated columns with caching to improve performance.
        
        Args:
            target: Target configuration containing schema and table info
            
        Returns:
            List[str]: Validated column names
        """
        cache_key = f"{target.schema_name}.{target.table}"
        
        if cache_key in self._column_cache:
            self.logger.debug(f"Using cached columns for {cache_key}")
            return self._column_cache[cache_key]
        
        columns = self._validate_columns(target)
        self._column_cache[cache_key] = columns
        return columns

    def _validate_columns(self, target) -> List[str]:
        """
        Validate target table schema and return valid columns via reflection.

        Args:
            target: Configuration object containing schema and table.

        Returns:
            List[str]: Validated list of column names for safe writes.

        Raises:
            DatabaseWriteException: If table does not exist or reflection fails.
        """
        try:
            # Use inspector for more reliable schema reflection
            inspector = inspect(self.engine)
            
            # Check if table exists
            if not inspector.has_table(target.table, schema=target.schema_name):
                raise DatabaseWriteException(
                    f"Table '{target.schema_name}.{target.table}' does not exist"
                )
            
            # Get column information
            columns_info = inspector.get_columns(target.table, schema=target.schema_name)
            columns = [col['name'] for col in columns_info]

            if not columns:
                raise DatabaseWriteException(
                    f"No columns found for table '{target.schema_name}.{target.table}'"
                )

            self.logger.info(
                "Validated columns from database schema",
                schema=target.schema_name,
                table=target.table,
                column_count=len(columns),
                columns=columns[:10]  # Log first 10 columns to avoid log spam
            )
            
            return columns

        except NoSuchTableError:
            raise DatabaseWriteException(
                f"Table '{target.schema_name}.{target.table}' does not exist"
            )
        except SQLAlchemyError as e:
            raise DatabaseWriteException(
                f"Database error while validating columns for '{target.schema_name}.{target.table}': {str(e)}"
            )
        except Exception as e:
            raise DatabaseWriteException(
                f"Failed to validate columns for '{target.schema_name}.{target.table}': {str(e)}"
            )

    def _execute_batch(self, session, batch: List[DataRecord], target, valid_columns: List[str]) -> int:
        """
        Execute a batch insert safely into the target table.

        Args:
            session: SQLAlchemy session.
            batch (List[DataRecord]): Batch of records to insert.
            target: Target schema/table configuration.
            valid_columns (List[str]): Schema-reflected valid columns.

        Returns:
            int: Count of successful records inserted.
        """
        if not batch:
            return 0

        try:
            table_name = f"{target.schema_name}.{target.table}"
            
            # Get the intersection of record columns and valid columns
            first_record_columns = set(batch[0].get_data().keys())
            insert_columns = [col for col in valid_columns if col in first_record_columns]
            
            if not insert_columns:
                self.logger.warning(
                    "No matching columns between data and schema",
                    data_columns=list(first_record_columns),
                    schema_columns=valid_columns[:10]  # Limit for logging
                )
                return 0
            
            # Build INSERT SQL with only valid columns
            column_placeholders = ', '.join([f':{col}' for col in insert_columns])
            column_names = ', '.join(insert_columns)
            
            insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({column_placeholders})"

            # Prepare batch data with only valid columns
            batch_data = []
            successful_count = 0
            
            for record in batch:
                try:
                    record_data = record.get_data()
                    # Filter to only include columns that exist in both data and schema
                    filtered_data = {
                        col: record_data.get(col) 
                        for col in insert_columns 
                        if col in record_data
                    }
                    
                    # Skip records that don't have any valid columns
                    if filtered_data:
                        batch_data.append(filtered_data)
                        successful_count += 1
                    else:
                        self.logger.warning(
                            "Skipping record with no valid columns",
                            row_number=record.row_number
                        )
                        
                except Exception as e:
                    self.logger.warning(
                        "Failed to prepare record for batch",
                        row_number=record.row_number,
                        error_message=str(e)
                    )

            if batch_data:
                # Execute the batch insert
                session.execute(text(insert_sql), batch_data)
                
                self.logger.debug(
                    "Batch executed successfully",
                    batch_size=len(batch_data),
                    table=table_name,
                    columns_inserted=len(insert_columns)
                )
                
                return successful_count
            else:
                self.logger.warning("No valid records in batch, skipping insert")
                return 0

        except SQLAlchemyError as e:
            self.logger.error(
                "SQL error during batch execution",
                batch_size=len(batch),
                table=f"{target.schema_name}.{target.table}",
                error_message=str(e)
            )
            return 0
        except Exception as e:
            self.logger.error(
                "Unexpected error during batch execution",
                batch_size=len(batch),
                table=f"{target.schema_name}.{target.table}",
                error_message=str(e)
            )
            return 0

    def _record_audit_trail(self, config: DataSourceDefinition, stats: LoadingStats):
        """
        Record audit trail entry after write completion.

        Args:
            config (DataSourceDefinition): Configuration with target info.
            stats (LoadingStats): Metrics from the write operation.
        """
        try:
            with self.SessionLocal() as session:
                audit_sql = """
                INSERT INTO data_loading_audit 
                (table_name, source_type, record_count, duration_ms, execution_time, successful_records, error_records)
                VALUES (:table_name, :source_type, :record_count, :duration_ms, :execution_time, :successful_records, :error_records)
                """

                session.execute(text(audit_sql), {
                    'table_name': config.target_config.table,
                    'source_type': config.type.value,
                    'record_count': stats.total_records,
                    'duration_ms': stats.write_time_ms,
                    'execution_time': stats.execution_time,
                    'successful_records': stats.successful_records,
                    'error_records': stats.error_records
                })

                session.commit()

                self.logger.info(
                    "Audit trail recorded successfully",
                    table=config.target_config.table,
                    record_count=stats.total_records
                )

        except Exception as e:
            self.logger.warning(
                "Failed to record audit trail",
                table=config.target_config.table,
                error_message=str(e)
            )
            # Don't raise exception for audit failures

    def clear_column_cache(self):
        """Clear the column validation cache."""
        self._column_cache.clear()
        self.logger.info("Column validation cache cleared")

    def get_table_info(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get detailed table information for debugging purposes.
        
        Args:
            schema_name: Database schema name
            table_name: Table name
            
        Returns:
            Dict containing table metadata
        """
        try:
            inspector = inspect(self.engine)
            
            if not inspector.has_table(table_name, schema=schema_name):
                return {"exists": False}
            
            columns = inspector.get_columns(table_name, schema=schema_name)
            primary_keys = inspector.get_pk_constraint(table_name, schema=schema_name)
            
            return {
                "exists": True,
                "columns": [{"name": col["name"], "type": str(col["type"])} for col in columns],
                "primary_keys": primary_keys.get("constrained_columns", []),
                "column_count": len(columns)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get table info: {str(e)}")
            return {"exists": False, "error": str(e)}
