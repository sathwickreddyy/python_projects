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
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError
from models.data_record import DataRecord
from config.data_loader_config import DataSourceDefinition
from core.base_types import LoadingStats
from core.exceptions import DatabaseWriteException
from core.logging_config import DataIngestionLogger


class DatabaseWriter:
    """
    High-performance, schema-validated database writer with:
    - Batch processing
    - Schema column validation (via reflection)
    - Transaction management with rollback
    - Comprehensive logging and audit trail
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
        target = config.target
        batch_size = target.batch_size or 1000

        self.logger.info(
            "Starting database write",
            schema=target.schema_name,
            table=target.table,
            batch_size=batch_size
        )

        # Reflect schema to validate columns before starting.
        valid_columns = self.validate_columns(target)

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

                # Flush remaining records.
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
                "Database write completed",
                **stats.dict()
            )

            # Record audit log
            self._record_audit_trail(config, stats)

            return stats

        except Exception as e:
            self.logger.error(
                "Database write failed",
                error_message=str(e),
                table=target.table
            )
            raise DatabaseWriteException(f"Database write failed: {str(e)}", e)

    def validate_columns(self, target) -> List[str]:
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
            metadata = MetaData(schema=target.schema_name)
            metadata.reflect(bind=self.engine, only=[target.table])

            # Handle schema-qualified names for Postgres
            full_table_name = f"{target.schema_name}.{target.table}"
            if full_table_name in metadata.tables:
                table = metadata.tables[full_table_name]
            else:
                table = metadata.tables[target.table]

            columns = [col.name for col in table.columns]

            self.logger.info(
                "Validated columns from DB schema",
                schema=target.schema_name,
                table=target.table,
                columns=columns
            )
            return columns

        except NoSuchTableError:
            raise DatabaseWriteException(f"Table '{target.schema_name}.{target.table}' does not exist.")
        except Exception as e:
            raise DatabaseWriteException(f"Failed to validate columns for '{target.schema_name}.{target.table}': {str(e)}")

    def _execute_batch(self, session, batch: List[DataRecord], target, valid_columns: List[str]) -> int:
        """
        Execute a batch insert safely into the target table.

        Args:
            session: SQLAlchemy session.
            batch (List[DataRecord]): Batch of records.
            target: Target schema/table config.
            valid_columns (List[str]): Schema-reflected valid columns.

        Returns:
            int: Count of successful records inserted.
        """
        if not batch:
            return 0

        try:
            table_name = f"{target.schema_name}.{target.table}"
            column_placeholders = ', '.join([f':{col}' for col in valid_columns])
            column_names = ', '.join(valid_columns)

            insert_sql = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES ({column_placeholders})
            """

            # Prepare filtered data for valid columns
            batch_data = []
            for record in batch:
                filtered_data = {k: v for k, v in record.get_data().items() if k in valid_columns}
                batch_data.append(filtered_data)

            session.execute(text(insert_sql), batch_data)

            self.logger.debug(
                "Batch executed successfully",
                batch_size=len(batch),
                table=table_name
            )

            return len(batch)

        except Exception as e:
            self.logger.error(
                "Batch execution failed",
                batch_size=len(batch),
                table=f"{target.schema_name}.{target.table}",
                error_message=str(e)
            )
            return 0  # Skip raising; continue other batches

    def _record_audit_trail(self, config: DataSourceDefinition, stats: LoadingStats):
        """
        Record audit trail entry post write completion.

        Args:
            config (DataSourceDefinition): Target schema/table config.
            stats (LoadingStats): Metrics for the write operation.
        """
        try:
            with self.SessionLocal() as session:
                audit_sql = """
                INSERT INTO data_loading_audit 
                (table_name, source_type, record_count, duration_ms, execution_time, successful_records, error_records)
                VALUES (:table_name, :source_type, :record_count, :duration_ms, :execution_time, :successful_records, :error_records)
                """

                session.execute(text(audit_sql), {
                    'table_name': config.target.table,
                    'source_type': config.type.value,
                    'record_count': stats.total_records,
                    'duration_ms': stats.write_time_ms,
                    'execution_time': stats.execution_time,
                    'successful_records': stats.successful_records,
                    'error_records': stats.error_records
                })

                session.commit()

                self.logger.info(
                    "Audit trail recorded",
                    table=config.target.table,
                    record_count=stats.total_records
                )

        except Exception as e:
            self.logger.warning(
                "Failed to record audit trail",
                error_message=str(e)
            )