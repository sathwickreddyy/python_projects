"""
Data processor for transforming and validating data records.

@author sathwick
"""
from datetime import datetime
from typing import Iterator, List

import re

from models.core.base_types import MappingStrategy, LoadingStats
from models.data_record import DataRecord
from config.data_loader_config import DataSourceDefinition, ColumnMapping
from converters.data_type_converter import DataTypeConverter
from models.core.exceptions import DataConversionException
from models.core.logging_config import DataIngestionLogger


class DataProcessor:
    """
    DataProcessor is responsible for processing a stream of DataRecord objects by applying:
    - Column mapping (explicit via mappings or implicit via direct strategy)
    - Data type conversion
    - Data validation (required fields, quality checks)
    - Per-record error handling

    This ensures input data is standardized, cleaned, and ready for downstream persistence.

    -------------------------------------------------------------------------------
    |                               DataProcessor                                 |
    -------------------------------------------------------------------------------
    | Input: Iterator[DataRecord]                                                 |
    |                                                                             |
    | ┌──────────────────────────────────────────────────────────────────────┐    |
    | │                              For Each Record                         │    |
    | └──────────────────────────────────────────────────────────────────────┘    |
    |         │                                                                   │
    |         ├─── Is record invalid? ──────> Yes ──> Yield as is (invalid)       │
    |         │                                         with error reason         │
    |         │                                                                   │
    |         ▼                                                                   │
    |    Mapping Strategy?                                                        │
    |     ┌────────────────────┐                                                  │
    |     │  MAPPED            │ ──> Apply mappings,                              │
    |     │                    │     type conversion                              │
    |     │  (source → target) │     default values                               │
    |     └────────────────────┘                                                  │
    |             │                                                               │
    |             └─ Missing required field? → Mark invalid with reason           │
    |                                                                             │
    |     ┌────────────────────┐                                                  │
    |     │  DIRECT            │ ──> Convert field names                          │
    |     │ (camelCase → SNAKE)│     Keep values as-is                            │
    |     └────────────────────┘                                                  │
    |                                                                             │
    | Validation:                                                                 │
    | - Required columns present?                                                 │
    | - Data quality checks?                                                      │
    |                                                                             │
    | ┌───────────────────────────────────────────────────────────────────────┐   |
    | │     Yield Valid Record    │      OR      │     Yield Invalid Record   │   |
    | │ (mapped + validated data) │              │ (with error message)       │   |
    | └───────────────────────────────────────────────────────────────────────┘   |
    -------------------------------------------------------------------------------

    What is DataRecord?
    -------------------
    DataRecord is a standardized container for a single input row.

    Attributes:
    -----------
    - `data`: Dict[str, Any]  → The actual fields and values after mapping
    - `row_number`: int       → Source row number for traceability
    - `valid`: bool           → Flag indicating validity (True/False)
    - `error_message`: str    → Reason for invalidity, if any

    Benefits of DataRecord:
    -----------------------
    - Consistent structure for valid and invalid data
    - Tracks processing failures per record without halting pipeline
    - Enables auditability and debugging via row numbers and error details
    - Compatible with streaming large datasets
    """

    def __init__(self, data_type_converter: DataTypeConverter):
        """Initialize data processor."""
        self.data_type_converter = data_type_converter
        self.logger = DataIngestionLogger(__name__)

        # Initialize processing statistics
        self.stats = None
        self.start_time = None

    def process_data(self, data_stream: Iterator[DataRecord],
                     config: DataSourceDefinition) -> Iterator[DataRecord]:
        """
        Process data stream with enhanced error tracking.
        """
        self.start_time = datetime.now()

        # Initialize stats
        self.stats = LoadingStats(
            execution_time=self.start_time,
            source_name=config.type.value,
            target_table=config.target_config.table if config.target_config else None
        )

        self.logger.info(
            "Starting data processing with error tracking",
            data_source=config.type.value,
            mapping_strategy=config.input_output_mapping.mapping_strategy.value
        )

        processed_count = 0
        error_count = 0

        for record in data_stream:
            self.stats.total_records += 1

            if not record.is_valid():
                error_count += 1
                self.stats.error_records += 1

                # Add processing error for already invalid records
                self.stats.add_processing_error(
                    row_number=record.row_number,
                    error_message=record.error_message or "Record marked as invalid"
                )

                yield record
                continue

            try:
                # Apply validation on raw data before processing with error tracking
                if config.validation:
                    record = self._validate_record_with_tracking(
                        record, config.validation
                    )

                # Apply mapping strategy
                if config.input_output_mapping.mapping_strategy == MappingStrategy.MAPPED:
                    processed_record = self._apply_mapped_strategy_with_tracking(
                        record, config.input_output_mapping.column_mappings
                    )
                else:
                    processed_record = self._apply_direct_strategy(record)

                if processed_record.is_valid():
                    self.stats.successful_records += 1
                    processed_count += 1
                else:
                    self.stats.error_records += 1
                    error_count += 1

                yield processed_record

                if processed_count % 1000 == 0:
                    self.logger.debug(f"Processed {processed_count} records")

            except Exception as e:
                error_count += 1
                self.stats.error_records += 1

                # Add processing error
                self.stats.add_processing_error(
                    row_number=record.row_number,
                    error_message=f"Unexpected processing error: {str(e)}"
                )

                self.logger.error(
                    "Record processing failed",
                    row_number=record.row_number,
                    error_message=str(e)
                )

                yield DataRecord.create_invalid(
                    record.get_data(),
                    record.row_number,
                    f"Processing error: {str(e)}"
                )

        # Calculate processing time
        end_time = datetime.now()
        self.stats.process_time_ms = int((end_time - self.start_time).total_seconds() * 1000)
        self.stats.execution_time = end_time

        self.logger.info(
            "Data processing completed",
            processed_records=processed_count,
            error_records=error_count,
            total_errors=len(self.stats.get_all_errors())
        )

    def _apply_mapped_strategy_with_tracking(self, record: DataRecord,
                                             mappings: List[ColumnMapping]) -> DataRecord:
        """Apply mapped strategy with enhanced error tracking."""
        if not mappings:
            self.logger.warning("MAPPED strategy selected but no column mappings provided")
            return record

        original_data = record.get_data()
        mapped_data = {}

        for mapping in mappings:
            source_key = mapping.source
            target_key = mapping.target

            try:
                # Get source value
                source_value = original_data.get(source_key)

                # Apply type conversion with error tracking
                if source_value is not None:
                    try:
                        converted_value = self.data_type_converter.convert_for_database(
                            source_value, mapping
                        )
                        mapped_data[target_key] = converted_value
                    except DataConversionException as e:
                        # Add conversion error
                        self.stats.add_conversion_error(
                            row_number=record.row_number,
                            field_name=source_key,
                            error_message=str(e),
                            field_value=source_value
                        )
                        # Continue processing other fields
                        return DataRecord.create_invalid(original_data,
                                                         record.row_number,
                                                         error_message=str(e))

                elif mapping.default_value is not None:
                    try:
                        converted_value = self.data_type_converter.convert_for_database(
                            mapping.default_value, mapping
                        )
                        mapped_data[target_key] = converted_value
                    except DataConversionException as e:
                        err_msg = f"Default value conversion failed: {str(e)}"
                        self.stats.add_conversion_error(
                            row_number=record.row_number,
                            field_name=source_key,
                            error_message=err_msg,
                            field_value=mapping.default_value
                        )
                        return DataRecord.create_invalid(original_data, record.row_number, error_message=err_msg)

                elif mapping.required:
                    error_message = f"Source field provided in column mappings of config:'{source_key}' is missing/null in feed input"
                    # Add validation error for missing required field
                    self.stats.add_validation_error(
                        row_number=record.row_number,
                        field_name=source_key,
                        error_message=error_message,
                    )
                    self.logger.error(
                        "Processing Error",
                        row_number=record.row_number,
                        error_message=record.error_message
                    )
                    return DataRecord.create_invalid(
                        original_data,
                        record.row_number,
                        error_message=error_message,
                    )

            except Exception as e:
                self.stats.add_processing_error(
                    row_number=record.row_number,
                    error_message=f"Column mapping error for '{source_key}': {str(e)}",
                    field_name=source_key
                )
                return DataRecord.create_invalid(
                    original_data,
                    record.row_number,
                    f"Column mapping error for '{source_key}': {str(e)}"
                )

        return DataRecord.create_valid(mapped_data, record.row_number)

    def _apply_direct_strategy(self, record: DataRecord) -> DataRecord:
        """Apply direct strategy - convert camelCase to snake_case and uppercase."""
        original_data = record.get_data()
        direct_data = {}

        for key, value in original_data.items():
            try:
                # Convert camelCase to snake_case and uppercase
                snake_case_key = self._camel_to_snake(key).upper()
                direct_data[snake_case_key] = value
            except Exception as e:
                self.stats.add_processing_error(
                    row_number=record.row_number,
                    error_message=f"Field name conversion failed for '{key}': {str(e)}",
                    field_name=key
                )

        return DataRecord.create_valid(direct_data, record.row_number)

    def _validate_record_with_tracking(self, record: DataRecord, validation_config) -> DataRecord:
        """Apply validation rules with error tracking."""
        if not record.is_valid():
            return record

        if not validation_config.data_quality_checks:
            return record

        data = record.get_data()
        validation_errors = []

        # Check required columns
        if validation_config.required_columns:
            for required_col in validation_config.required_columns:
                if required_col not in data or data[required_col] is None:
                    error_msg = f"Required column provided in validation section of config: '{required_col}' is missing or null from feed input"
                    validation_errors.append(error_msg)

                    self.stats.add_validation_error(
                        row_number=record.row_number,
                        field_name=required_col,
                        error_message=error_msg
                    )

        if validation_errors:
            return DataRecord.create_invalid(
                data,
                record.row_number,
                f"Validation failed: {'; '.join(validation_errors)}"
            )

        return record

    def _camel_to_snake(self, camel_str: str) -> str:
        """Convert camelCase to snake_case."""
        import re
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_str)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)

    def get_processing_stats(self) -> LoadingStats:
        """Get processing statistics."""
        return self.stats