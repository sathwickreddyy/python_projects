# app/processors/data_processor.py
"""
Data processor for transforming and validating data records.

@author sathwick
"""
from typing import Iterator, Dict, Any, List
from models.data_record import DataRecord
from config.data_loader_config import DataSourceDefinition, ColumnMapping
from converters.data_type_converter import DataTypeConverter
from core.exceptions import DataConversionException
from core.logging_config import DataIngestionLogger


class DataProcessor:
    """
    Data processor for applying transformations and validations to data records.

    This processor handles:
    - Column mapping and renaming
    - Data type conversion
    - Data validation
    - Error handling and recovery
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
            data_source=config.identifier,
            column_mappings=len(config.column_mapping)
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
                processed_record = self._apply_column_mapping(record, config.column_mapping)

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

    def _apply_column_mapping(self, record: DataRecord,
                              mappings: List[ColumnMapping]) -> DataRecord:
        """Apply column mapping and type conversion."""
        if not mappings:
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
