# app/processors/data_processor.py
"""
Data processor for transforming and validating data records.

@author sathwick
"""
from typing import Iterator, List

import re
from models.core.base_types import MappingStrategy
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
        """
        Initialize data processor.

        Args:
            data_type_converter: Data type converter instance
        """
        self.data_type_converter = data_type_converter
        self.logger = DataIngestionLogger(__name__)

    def process_data(self, data_stream: Iterator[DataRecord],
                     config: DataSourceDefinition) -> Iterator[DataRecord]:
        """
        Process data stream with transformations and validation.

        Args:
            data_stream: Iterator of DataRecord objects
            config: Data source configuration

        Yields:
            Processed DataRecord objects
        """
        self.logger.info(
            "Starting data processing",
            data_source=config.type.value,
            mapping_strategy=config.input_output_mapping.mapping_strategy.value
        )

        processed_count = 0
        error_count = 0

        for record in data_stream:
            if not record.is_valid():
                error_count += 1
                yield record
                continue

            try:
                # Apply column mapping and type conversion
                # Apply column mapping and type conversion based on strategy
                if config.input_output_mapping.mapping_strategy == MappingStrategy.MAPPED:
                    processed_record = self._apply_mapped_strategy(record, config.input_output_mapping.column_mappings)
                else:
                    processed_record = self._apply_direct_strategy(record)
                # Apply validation
                if config.validation:
                    processed_record = self._validate_record(processed_record, config.validation)

                yield processed_record
                processed_count += 1

                if processed_count % 1000 == 0:
                    self.logger.debug(f"Processed {processed_count} records")

            except Exception as e:
                error_count += 1
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

        self.logger.info(
            "Data processing completed",
            processed_records=processed_count,
            error_records=error_count
        )

    def _apply_mapped_strategy(self, record: DataRecord,
                               mappings: List[ColumnMapping]) -> DataRecord:
        """Apply mapped strategy with explicit column mappings."""
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

                # Apply type conversion
                if source_value is not None:
                    converted_value = self.data_type_converter.convert_for_database(
                        source_value, mapping
                    )
                    mapped_data[target_key] = converted_value
                elif mapping.default_value is not None:
                    converted_value = self.data_type_converter.convert_for_database(
                        mapping.default_value, mapping
                    )
                    mapped_data[target_key] = converted_value
                elif mapping.required:
                    raise DataConversionException(f"Required field '{source_key}' is missing")

            except Exception as e:
                self.logger.error(
                    "Column mapping failed",
                    source_key=source_key,
                    target_key=target_key,
                    row_number=record.row_number,
                    error_message=str(e)
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
            # Convert camelCase to snake_case and uppercase
            snake_case_key = self._camel_to_snake(key).upper()
            direct_data[snake_case_key] = value

        return DataRecord.create_valid(direct_data, record.row_number)

    def _camel_to_snake(self, camel_str: str) -> str:
        """Convert camelCase to snake_case."""
        # Insert underscore before uppercase letters
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_str)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)

    def _validate_record(self, record: DataRecord, validation_config) -> DataRecord:
        """Apply validation rules to the record."""
        if not validation_config.data_quality_checks:
            return record

        data = record.get_data()

        # Check required columns
        if validation_config.required_columns:
            for required_col in validation_config.required_columns:
                if required_col not in data or data[required_col] is None:
                    return DataRecord.create_invalid(
                        data,
                        record.row_number,
                        f"Required column '{required_col}' is missing or null"
                    )

        return record
