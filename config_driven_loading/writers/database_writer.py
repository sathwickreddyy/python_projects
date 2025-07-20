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

from typing import Iterator, List, Dict, Any
from datetime import datetime
from sqlalchemy import text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from models.data_record import DataRecord
from config.data_loader_config import DataSourceDefinition
from models.core.base_types import LoadingStats
from models.core.exceptions import DatabaseWriteException
from models.core.logging_config import DataIngestionLogger


class DatabaseWriter:
    """Enhanced database writer with fail-fast behavior for data quality."""

    def __init__(self, engine: Engine):
        """Initialize with SQLAlchemy engine."""
        self.engine = engine
        self.logger = DataIngestionLogger(__name__)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        self._column_cache: Dict[str, List[str]] = {}

    def write_data(self, data_stream: Iterator[DataRecord], config: DataSourceDefinition) -> LoadingStats:
        """Write data stream with fail-fast behavior for invalid records."""
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

        # Check if target is disabled - print records instead of writing
        if not target.enabled:
            return self._print_sample_records(data_stream, target, start_time)

        # Validate schema
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
                            "Skipping invalid record - fail fast enabled",
                            row_number=record.row_number,
                            error_message=record.error_message
                        )
                        continue

                    batch.append(record)

                    if len(batch) >= batch_size:
                        success_count, batch_errors = self._execute_batch(session, batch, target, valid_columns)
                        successful_records += success_count
                        error_records += batch_errors
                        batch_count += 1
                        batch.clear()

                # Process remaining records
                if batch:
                    success_count, batch_errors = self._execute_batch(session, batch, target, valid_columns)
                    successful_records += success_count
                    error_records += batch_errors
                    batch_count += 1

                # Only commit if no errors occurred
                if error_records > 0:
                    self.logger.error(
                        "Rolling back transaction due to processing errors",
                        total_errors=error_records,
                        successful_records=successful_records
                    )
                    session.rollback()
                    successful_records = 0  # Reset since we rolled back
                    raise DatabaseWriteException(f"Transaction rolled back due to {error_records} record errors")
                else:
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
                "Database write completed",
                **stats.dict()
            )

            self._record_audit_trail(config, stats)
            return stats

        except Exception as e:
            self.logger.error(
                "Database write failed",
                error_message=str(e.__cause__),
                table=f"{target.schema_name}.{target.table}"
            )

            # Create failed stats
            end_time = datetime.now()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)

            failed_stats = LoadingStats(
                write_time_ms=duration_ms,
                batch_count=batch_count,
                records_per_second=0.0,
                total_records=total_records,
                successful_records=0,  # No records written due to rollback
                error_records=total_records,  # All records considered failed
                execution_time=end_time
            )

            failed_stats.add_processing_error(
                row_number=-1,
                error_message=str(e.__cause__),
            )

            return failed_stats

    def _execute_batch(self, session, batch: List[DataRecord], target, valid_columns: List[str]) -> tuple[int, int]:
        """Execute a batch and return (successful_count, error_count)."""
        if not batch:
            return 0, 0

        try:
            table_name = f"{target.schema_name}.{target.table}"

            # Get intersection of record columns and valid columns
            first_record_columns = set(batch[0].get_data().keys())
            insert_columns = [col for col in valid_columns if col in first_record_columns]

            if not insert_columns:
                self.logger.error(
                    "No matching columns between data and schema",
                    data_columns=list(first_record_columns),
                    schema_columns=valid_columns[:10]
                )
                return 0, len(batch)

            # Prepare batch data
            batch_data = []
            successful_count = 0

            for record in batch:
                try:
                    record_data = record.get_data()
                    filtered_data = {
                        col: record_data.get(col)
                        for col in insert_columns
                        if col in record_data
                    }

                    if filtered_data:
                        batch_data.append(filtered_data)
                        successful_count += 1
                    else:
                        self.logger.warning(
                            "Record has no valid columns",
                            row_number=record.row_number
                        )

                except Exception as e:
                    self.logger.error(
                        "Failed to prepare record for batch",
                        row_number=record.row_number,
                        error_message=str(e)
                    )

            if batch_data:
                # Build and execute INSERT
                column_placeholders = ', '.join([f':{col}' for col in insert_columns])
                column_names = ', '.join(insert_columns)
                insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({column_placeholders})"

                session.execute(text(insert_sql), batch_data)

                self.logger.debug(
                    "Batch executed successfully",
                    batch_size=len(batch_data),
                    table=table_name
                )

                return successful_count, 0
            else:
                self.logger.warning("No valid records in batch")
                return 0, len(batch)

        except SQLAlchemyError as e:
            self.logger.error(
                "SQL error during batch execution",
                batch_size=len(batch),
                table=f"{target.schema_name}.{target.table}",
                error_message=str(e.__cause__)
            )
            # Return all records as failed for this batch
            raise e
        except Exception as e:
            self.logger.error(
                "Unexpected error during batch execution",
                batch_size=len(batch),
                error_message=str(e)
            )
            raise e

    def _print_sample_records(self, data_stream: Iterator[DataRecord], target, start_time: datetime) -> LoadingStats:
        """Print sample records when target is disabled."""
        print(f"\n{'=' * 70}")
        print(f"📋 SAMPLE RECORDS TO BE INSERTED INTO {target.schema_name}.{target.table}")
        print(f"{'=' * 70}")

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
                print("-" * 50)

        if records_processed > max_samples:
            remaining = records_processed - sample_count
            print(f"... and {remaining} more records")

        print(f"\n📊 SUMMARY:")
        print(f"   Total records: {records_processed}")
        print(f"   Valid records: {valid_records}")
        print(f"   Invalid records: {error_records}")
        print(f"   Sample shown: {min(sample_count, max_samples)}")
        print(f"{'=' * 70}")

        end_time = datetime.now()
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        return LoadingStats(
            write_time_ms=duration_ms,
            batch_count=0,
            records_per_second=0.0,
            total_records=records_processed,
            successful_records=0,  # No actual database writes
            error_records=error_records,
            execution_time=end_time
        )

    def _get_validated_columns(self, target) -> List[str]:
        """Get validated columns with caching."""
        cache_key = f"{target.schema_name}.{target.table}"

        if cache_key in self._column_cache:
            return self._column_cache[cache_key]

        columns = self._validate_columns(target)
        self._column_cache[cache_key] = columns
        return columns

    def _validate_columns(self, target) -> List[str]:
        """Validate target table schema."""
        try:
            inspector = inspect(self.engine)

            if not inspector.has_table(target.table, schema=target.schema_name):
                raise DatabaseWriteException(
                    f"Table '{target.schema_name}.{target.table}' does not exist"
                )

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
                columns=columns[:10]
            )

            return columns

        except Exception as e:
            raise DatabaseWriteException(
                f"Failed to validate columns for '{target.schema_name}.{target.table}': {str(e)}"
            )

    def _record_audit_trail(self, config: DataSourceDefinition, stats: LoadingStats):
        """Record audit trail."""
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

    def clear_column_cache(self):
        """Clear the column validation cache."""
        self._column_cache.clear()
        self.logger.info("Column validation cache cleared")
