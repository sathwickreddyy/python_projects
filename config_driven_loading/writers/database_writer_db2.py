# writers/database_writer_with_connections.py
"""
DB2 cursor-based database writer with comprehensive batch processing and schema validation.

This writer uses raw DB2 connection and cursor objects for database operations,
providing direct database access without SQLAlchemy dependencies.

Key Features:
- Direct DB2 connection and cursor usage
- Schema reflection using DB2 system catalogs
- Batch processing with transaction management
- Fail-fast behavior with rollback support
- Case-insensitive column matching (lowercase normalization)
- DIRECT mapping strategy support with type conversion
- No audit trail (as requested)

Usage:
    import ms.db2
    conn = ms.db2.connect(SERVER_NAME)
    writer = DB2DatabaseWriter(conn)
    stats = writer.write_data(data_stream, config)
    conn.close()

@author sathwick
"""

from typing import Iterator, List, Dict, Any, Tuple, Optional
from datetime import datetime, date
import decimal
from models.data_record import DataRecord
from config.data_loader_config import DataSourceDefinition
from models.core.base_types import LoadingStats, MappingStrategy
from models.core.exceptions import DatabaseWriteException
from models.core.logging_config import DataIngestionLogger
from converters.data_type_converter import DataTypeConverter
from config.data_loader_config import ColumnMapping
from models.core.base_types import DataType


class DB2ColumnInfo:
    """
    Container for DB2 column metadata retrieved from system catalogs.

    This class standardizes DB2 column information for use in data processing
    and type conversion operations.
    """

    def __init__(self, column_name: str, data_type: str, length: Optional[int] = None,
                 scale: Optional[int] = None, nullable: str = 'Y'):
        """
        Initialize DB2 column information.

        Args:
            column_name: Column name (will be normalized to lowercase)
            data_type: DB2 data type (VARCHAR, INTEGER, DECIMAL, etc.)
            length: Column length for character/numeric types
            scale: Decimal scale for numeric types
            nullable: 'Y' if nullable, 'N' if not nullable
        """
        self.column_name = column_name.lower()  # Normalize to lowercase
        self.data_type = data_type.upper()
        self.length = length
        self.scale = scale
        self.nullable = nullable.upper() == 'Y'
        self.framework_type = self._map_db2_to_framework_type()

    def _map_db2_to_framework_type(self) -> DataType:
        """
        Map DB2 data types to framework DataType enum.

        DB2 Type Mappings:
        - CHARACTER, VARCHAR, CHAR, CLOB, GRAPHIC, VARGRAPHIC → STRING
        - INTEGER, SMALLINT, BIGINT → INTEGER
        - DECIMAL, NUMERIC → DECIMAL
        - REAL, FLOAT, DOUBLE → FLOAT
        - DATE → DATE
        - TIMESTAMP, TIME → DATETIME
        - Boolean types (rare in DB2) → BOOLEAN

        Returns:
            DataType: Corresponding framework data type
        """
        db2_type = self.data_type

        # String/Character types
        if db2_type in ('CHARACTER', 'VARCHAR', 'CHAR', 'CLOB', 'GRAPHIC', 'VARGRAPHIC', 'LONG VARCHAR'):
            return DataType.STRING

        # Integer types
        elif db2_type in ('INTEGER', 'SMALLINT', 'BIGINT', 'INT'):
            return DataType.INTEGER

        # Decimal/Numeric types
        elif db2_type in ('DECIMAL', 'NUMERIC', 'DEC'):
            return DataType.DECIMAL

        # Floating point types
        elif db2_type in ('REAL', 'FLOAT', 'DOUBLE', 'DECFLOAT'):
            return DataType.FLOAT

        # Date type
        elif db2_type == 'DATE':
            return DataType.DATE

        # Timestamp/Time types
        elif db2_type in ('TIMESTAMP', 'TIME'):
            return DataType.DATETIME

        # Boolean (uncommon in DB2, but handle if present)
        elif db2_type == 'BOOLEAN':
            return DataType.BOOLEAN

        # Default to STRING for unknown types
        else:
            return DataType.STRING

    def __str__(self) -> str:
        """String representation for debugging."""
        return f"DB2Column({self.column_name}, {self.data_type}, nullable={self.nullable})"


class DB2DatabaseWriter:
    """
    DB2-specific database writer using direct connection and cursor operations.

    This writer bypasses SQLAlchemy and uses raw DB2 API calls for maximum
    compatibility with corporate network restrictions.

    Workflow:
    ---------
    1. Accept pre-established DB2 connection object
    2. Reflect schema using DB2 system catalog queries
    3. Process data records with type validation
    4. Execute batch inserts using parameterized queries
    5. Handle transaction commit/rollback
    6. Return comprehensive statistics
    """

    def __init__(self, db2_connection):
        """
        Initialize DB2 database writer with established connection.

        Args:
            db2_connection: Active DB2 connection object from ms.db2.connect()

        Example:
            import ms.db2
            conn = ms.db2.connect(SERVER_NAME)
            writer = DB2DatabaseWriter(conn)
        """
        self.connection = db2_connection
        self.logger = DataIngestionLogger(__name__)

        # Cache for schema information to avoid repeated catalog queries
        self._column_cache: Dict[str, List[str]] = {}
        self._schema_cache: Dict[str, Dict[str, DB2ColumnInfo]] = {}

        # Initialize data type converter for DIRECT mapping
        self.data_type_converter = DataTypeConverter()

        self.logger.info("DB2DatabaseWriter initialized with direct connection")

    def write_data(self, data_stream: Iterator[DataRecord], config: DataSourceDefinition) -> LoadingStats:
        """
        Write data stream to DB2 database using cursor-based operations.

        This method provides the same interface as the SQLAlchemy version but
        uses direct DB2 cursor operations for all database interactions.

        Args:
            data_stream: Iterator of DataRecord objects to write
            config: Data source configuration

        Returns:
            LoadingStats with execution metrics (no audit trail)
        """
        start_time = datetime.now()
        target = config.target_config
        batch_size = target.batch_size or 1000

        self.logger.info(
            "Starting DB2 cursor-based database write",
            schema=target.schema_name,
            table=target.table,
            batch_size=batch_size,
            enabled=target.enabled,
            mapping_strategy=config.input_output_mapping.mapping_strategy.value
        )

        # Handle disabled target - print sample records
        if not target.enabled:
            return self._print_sample_records(data_stream, target, start_time)

        # Get DB2 schema information using system catalogs
        schema_info = self._get_db2_schema_info(target)
        if not schema_info:
            raise DatabaseWriteException(f"No schema information found for table '{target.schema_name}.{target.table}'")

        self.logger.info(
            "Retrieved DB2 schema information",
            schema=target.schema_name,
            table=target.table,
            column_count=len(schema_info),
            sample_columns=list(schema_info.keys())[:5]
        )

        # Initialize counters
        total_records = 0
        successful_records = 0
        error_records = 0
        batch_count = 0

        cursor = None

        try:
            cursor = self.connection.cursor()
            batch = []

            # Process each record in the stream
            for record in data_stream:
                total_records += 1

                # Skip invalid records
                if not record.is_valid():
                    error_records += 1
                    self.logger.warning(
                        "Skipping invalid record - fail fast enabled",
                        row_number=record.row_number,
                        error_message=record.error_message
                    )
                    continue

                # Apply schema-aware processing for DIRECT mapping
                if config.input_output_mapping.mapping_strategy == MappingStrategy.DIRECT:
                    processed_record = self._apply_db2_schema_processing(record, schema_info)
                    if not processed_record.is_valid():
                        error_records += 1
                        self.logger.error(
                            "DB2 schema validation failed for DIRECT mapping",
                            row_number=record.row_number,
                            error_message=processed_record.error_message
                        )
                        continue
                    record = processed_record

                batch.append(record)

                # Execute batch when size threshold is reached
                if len(batch) >= batch_size:
                    success_count, batch_errors = self._execute_db2_batch(
                        cursor, batch, target, schema_info
                    )
                    successful_records += success_count
                    error_records += batch_errors
                    batch_count += 1
                    batch.clear()

            # Process remaining records in final batch
            if batch:
                success_count, batch_errors = self._execute_db2_batch(
                    cursor, batch, target, schema_info
                )
                successful_records += success_count
                error_records += batch_errors
                batch_count += 1

            # Handle transaction commit/rollback based on errors
            if error_records > 0:
                self.logger.error(
                    "Rolling back DB2 transaction due to processing errors",
                    total_errors=error_records,
                    successful_records=successful_records
                )
                self.connection.rollback()
                successful_records = 0  # Reset since we rolled back
                raise DatabaseWriteException(f"DB2 transaction rolled back due to {error_records} record errors")
            else:
                self.connection.commit()
                self.logger.info(
                    "DB2 transaction committed successfully",
                    successful_records=successful_records,
                    batch_count=batch_count
                )

            # Calculate final statistics
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
                "DB2 cursor-based write completed successfully",
                **stats.model_dump()
            )

            return stats

        except Exception as e:
            self.logger.error(
                "DB2 cursor-based write failed",
                error_message=str(e),
                table=f"{target.schema_name}.{target.table}"
            )

            # Attempt rollback on error
            try:
                self.connection.rollback()
                self.logger.info("DB2 transaction rolled back after error")
            except Exception as rollback_error:
                self.logger.error(f"Failed to rollback DB2 transaction: {rollback_error}")

            # Create failed statistics
            end_time = datetime.now()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)

            failed_stats = LoadingStats(
                write_time_ms=duration_ms,
                batch_count=batch_count,
                records_per_second=0.0,
                total_records=total_records,
                successful_records=0,
                error_records=total_records,
                execution_time=end_time
            )

            failed_stats.add_processing_error(
                row_number=-1,
                error_message=str(e)
            )

            return failed_stats

        finally:
            # Clean up cursor
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def _get_db2_schema_info(self, target) -> Dict[str, DB2ColumnInfo]:
        """
        Retrieve comprehensive schema information from DB2 system catalogs.

        This method queries the DB2 system catalog tables to get column metadata:
        - SYSCAT.COLUMNS: Column names, data types, lengths, nullable constraints

        Args:
            target: Target configuration with schema and table information

        Returns:
            Dict mapping lowercase column names to DB2ColumnInfo objects
        """
        cache_key = f"{target.schema_name}.{target.table}".upper()

        if cache_key in self._schema_cache:
            self.logger.debug(f"Using cached DB2 schema info for {cache_key}")
            return self._schema_cache[cache_key]

        cursor = None

        try:
            cursor = self.connection.cursor()

            # Query DB2 system catalog for column information
            schema_query = """
            SELECT 
                COLNAME,
                TYPENAME,
                LENGTH,
                SCALE,
                NULLS
            FROM SYSCAT.COLUMNS 
            WHERE TABSCHEMA = ? 
              AND TABNAME = ?
            ORDER BY COLNO
            """

            cursor.execute(schema_query, (target.schema_name.upper(), target.table.upper()))
            rows = cursor.fetchall()

            if not rows:
                raise DatabaseWriteException(
                    f"No columns found for DB2 table '{target.schema_name}.{target.table}'"
                )

            # Build schema information dictionary
            schema_info = {}

            for row in rows:
                col_name, type_name, length, scale, nulls = row

                # Create DB2ColumnInfo object
                column_info = DB2ColumnInfo(
                    column_name=col_name,
                    data_type=type_name,
                    length=length,
                    scale=scale,
                    nullable=nulls
                )

                # Store with lowercase key for case-insensitive matching
                schema_info[col_name.lower()] = column_info

            # Cache the schema information
            self._schema_cache[cache_key] = schema_info

            self.logger.info(
                "DB2 schema information retrieved and cached",
                schema=target.schema_name,
                table=target.table,
                column_count=len(schema_info)
            )

            # Log detailed column information for debugging
            self.logger.debug(
                "DB2 column details",
                columns={
                    name: {
                        'type': info.data_type,
                        'framework_type': info.framework_type.value,
                        'nullable': info.nullable
                    }
                    for name, info in list(schema_info.items())[:5]  # First 5 columns
                }
            )

            return schema_info

        except Exception as e:
            self.logger.error(
                "Failed to retrieve DB2 schema information",
                schema=target.schema_name,
                table=target.table,
                error_message=str(e)
            )
            raise DatabaseWriteException(
                f"Failed to get DB2 schema info for '{target.schema_name}.{target.table}': {str(e)}"
            )

        finally:
            if cursor:
                cursor.close()

    def _apply_db2_schema_processing(self, record: DataRecord,
                                     schema_info: Dict[str, DB2ColumnInfo]) -> DataRecord:
        """
        Apply DB2 schema-aware data type conversion for DIRECT mapping strategy.

        This method performs intelligent type conversion based on DB2 column definitions:
        1. Matches input fields to DB2 columns (case-insensitive)
        2. Converts data types to match DB2 expectations
        3. Validates nullable constraints
        4. Provides detailed error messages for conversion failures

        Args:
            record: Input data record to process
            schema_info: DB2 schema information

        Returns:
            DataRecord with DB2-compatible data or error information
        """
        original_data = record.get_data()
        converted_data = {}
        conversion_errors = []

        self.logger.debug(
            "Applying DB2 schema-aware processing for DIRECT mapping",
            row_number=record.row_number,
            input_fields=list(original_data.keys()),
            db2_columns=list(schema_info.keys())
        )

        # Process each input field against DB2 schema
        for input_field, input_value in original_data.items():
            input_field_lower = input_field.lower()

            # Find matching DB2 column
            if input_field_lower in schema_info:
                column_info = schema_info[input_field_lower]

                try:
                    # Create temporary ColumnMapping for type conversion
                    temp_mapping = ColumnMapping(
                        source=input_field,
                        target=column_info.column_name,
                        data_type=column_info.framework_type,
                        required=not column_info.nullable
                    )

                    # Apply type conversion
                    if input_value is not None:
                        converted_value = self.data_type_converter.convert_for_database(
                            input_value, temp_mapping
                        )

                        # Additional DB2-specific type handling
                        converted_value = self._apply_db2_type_formatting(
                            converted_value, column_info
                        )

                        converted_data[column_info.column_name] = converted_value

                        self.logger.debug(
                            "DB2 DIRECT mapping conversion successful",
                            row_number=record.row_number,
                            input_field=input_field,
                            db2_column=column_info.column_name,
                            db2_type=column_info.data_type,
                            input_value=input_value,
                            converted_value=converted_value
                        )

                    elif not column_info.nullable:
                        # Non-nullable column with null value
                        error_msg = f"DB2 column '{column_info.column_name}' ({column_info.data_type}) cannot be NULL but input field '{input_field}' is null"
                        conversion_errors.append(error_msg)

                        self.logger.warning(
                            "DB2 DIRECT mapping validation failed - NULL constraint",
                            row_number=record.row_number,
                            input_field=input_field,
                            db2_column=column_info.column_name,
                            nullable=column_info.nullable
                        )

                    else:
                        # Nullable column with null value
                        converted_data[column_info.column_name] = None

                except Exception as e:
                    error_msg = f"DB2 type conversion failed for field '{input_field}' -> column '{column_info.column_name}' ({column_info.data_type}): {str(e)}"
                    conversion_errors.append(error_msg)

                    self.logger.error(
                        "DB2 DIRECT mapping type conversion failed",
                        row_number=record.row_number,
                        input_field=input_field,
                        db2_column=column_info.column_name,
                        db2_type=column_info.data_type,
                        input_value=input_value,
                        error_message=str(e)
                    )

            else:
                # Input field doesn't match any DB2 column
                self.logger.warning(
                    "Input field not found in DB2 schema - skipping",
                    row_number=record.row_number,
                    input_field=input_field,
                    available_columns=list(schema_info.keys())[:10]
                )

        # Handle conversion errors
        if conversion_errors:
            combined_error = f"DB2 schema-aware conversion failed: {'; '.join(conversion_errors)}"
            return DataRecord.create_invalid(original_data, record.row_number, combined_error)

        # Validate we have data to insert
        if not converted_data:
            return DataRecord.create_invalid(
                original_data,
                record.row_number,
                "No input fields match DB2 schema columns"
            )

        self.logger.debug(
            "DB2 schema-aware processing completed successfully",
            row_number=record.row_number,
            converted_fields=list(converted_data.keys()),
            field_count=len(converted_data)
        )

        return DataRecord.create_valid(converted_data, record.row_number)

    def _apply_db2_type_formatting(self, value, column_info: DB2ColumnInfo):
        """
        Apply DB2-specific type formatting and constraints.

        This method handles DB2-specific data formatting requirements:
        - Decimal precision and scale validation
        - String length truncation
        - Date/timestamp formatting

        Args:
            value: Converted value
            column_info: DB2 column information

        Returns:
            DB2-formatted value
        """
        if value is None:
            return None

        db2_type = column_info.data_type

        # Handle string types with length constraints
        if db2_type in ('CHARACTER', 'VARCHAR', 'CHAR') and column_info.length:
            if isinstance(value, str) and len(value) > column_info.length:
                self.logger.warning(
                    f"Truncating string value for DB2 column {column_info.column_name}",
                    original_length=len(value),
                    max_length=column_info.length
                )
                return value[:column_info.length]

        # Handle decimal precision/scale
        elif db2_type in ('DECIMAL', 'NUMERIC') and isinstance(value, decimal.Decimal):
            if column_info.scale is not None:
                # Round to the specified scale
                return value.quantize(decimal.Decimal('0.' + '0' * column_info.scale))

        return value

    def _execute_db2_batch(self, cursor, batch: List[DataRecord], target,
                           schema_info: Dict[str, DB2ColumnInfo]) -> Tuple[int, int]:
        """
        Execute batch insert using DB2 cursor with parameterized queries.

        This method constructs and executes DB2 INSERT statements using
        parameter markers for safe and efficient batch processing.

        Args:
            cursor: DB2 cursor object
            batch: List of data records to insert
            target: Target configuration
            schema_info: DB2 schema information

        Returns:
            Tuple of (successful_count, error_count)
        """
        if not batch:
            return 0, 0

        try:
            table_name = f"{target.schema_name}.{target.table}"

            # Get columns from first record and validate against schema
            first_record_columns = set(col.lower() for col in batch[0].get_data().keys())
            available_columns = set(schema_info.keys())

            # Find intersection of record columns and DB2 columns
            insert_columns = list(first_record_columns.intersection(available_columns))

            if not insert_columns:
                self.logger.error(
                    "No matching columns between record data and DB2 schema",
                    record_columns=list(first_record_columns),
                    db2_columns=list(available_columns)[:10]
                )
                return 0, len(batch)

            self.logger.debug(
                "Executing DB2 batch with schema validation",
                table=table_name,
                batch_size=len(batch),
                insert_columns=insert_columns
            )

            # Prepare batch data
            batch_data = []
            successful_count = 0

            for record in batch:
                try:
                    record_data = record.get_data()
                    # Create tuple of values in column order
                    row_values = []

                    for col in insert_columns:
                        value = record_data.get(col)
                        row_values.append(value)

                    batch_data.append(tuple(row_values))
                    successful_count += 1

                except Exception as e:
                    self.logger.error(
                        "Failed to prepare record for DB2 batch",
                        row_number=record.row_number,
                        error_message=str(e)
                    )

            # Execute batch insert if we have valid data
            if batch_data:
                # Construct INSERT statement with parameter markers
                column_names = ', '.join(insert_columns)
                param_markers = ', '.join(['?' for _ in insert_columns])
                insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({param_markers})"

                # Execute batch insert using DB2 cursor
                cursor.executemany(insert_sql, batch_data)

                self.logger.debug(
                    "DB2 batch executed successfully",
                    batch_size=len(batch_data),
                    table=table_name,
                    columns_used=len(insert_columns),
                    sql=insert_sql
                )

                return successful_count, 0
            else:
                self.logger.warning("No valid records in DB2 batch")
                return 0, len(batch)

        except Exception as e:
            self.logger.error(
                "Error during DB2 batch execution",
                batch_size=len(batch),
                table=f"{target.schema_name}.{target.table}",
                error_message=str(e)
            )
            raise e

    def _print_sample_records(self, data_stream: Iterator[DataRecord], target,
                              start_time: datetime) -> LoadingStats:
        """
        Print sample records when target is disabled.

        This method provides the same interface as the SQLAlchemy version
        but works with the cursor-based approach.
        """
        print(f"\n{'=' * 70}")
        print(f"📋 SAMPLE RECORDS TO BE INSERTED INTO {target.schema_name}.{target.table} (DB2)")
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
        print(f"   Database: DB2 (cursor-based)")
        print(f"{'=' * 70}")

        end_time = datetime.now()
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        return LoadingStats(
            write_time_ms=duration_ms,
            batch_count=0,
            records_per_second=0.0,
            total_records=records_processed,
            successful_records=0,
            error_records=error_records,
            execution_time=end_time
        )

    def clear_schema_cache(self):
        """Clear schema information caches."""
        self._column_cache.clear()
        self._schema_cache.clear()
        self.logger.info("DB2 schema caches cleared")

    def close(self):
        """
        Close the database connection.

        Note: This closes the connection passed to the constructor.
        Use with caution if the connection is shared.
        """
        try:
            if self.connection:
                self.connection.close()
                self.logger.info("DB2 connection closed")
        except Exception as e:
            self.logger.warning(f"Error closing DB2 connection: {e}")
