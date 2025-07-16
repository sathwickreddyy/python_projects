"""
Comprehensive data type conversion utilities.

@author sathwick
"""
from typing import Any, Optional, Dict, List
from decimal import Decimal
from datetime import datetime, date
from core.base_types import DataType
from core.exceptions import DataConversionException
from core.logging_config import DataIngestionLogger
from config.data_loader_config import ColumnMapping
import re


class DataTypeConverter:
    """
    Comprehensive data type converters with support for various data types and formats.

    This converters handles conversion of string values to typed Python objects
    with proper error handling and validation.
    """

    def __init__(self):
        self.logger = DataIngestionLogger(__name__)
        self.common_date_patterns = [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y%m%d",
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S"
        ]

    def convert_for_database(self, value: Any, mapping: ColumnMapping) -> Any:
        """
        Convert value to appropriate type for database insertion.

        Args:
            value: Raw input value to convert
            mapping: Column mapping configuration

        Returns:
            Converted value ready for database insertion

        Raises:
            DataConversionException: If conversion fails
        """
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return self._handle_null_value(mapping)

        try:
            value_str = str(value).strip()

            conversion_map = {
                DataType.STRING: lambda v: v,
                DataType.INTEGER: lambda v: int(v),
                DataType.LONG: lambda v: int(v),
                DataType.FLOAT: lambda v: float(v),
                DataType.DECIMAL: lambda v: Decimal(v),
                DataType.BOOLEAN: self._parse_boolean,
                DataType.DATE: lambda v: self._parse_date(v, mapping.source_date_format),
                DataType.DATETIME: lambda v: self._parse_datetime(v, mapping.source_date_format),
                DataType.TIMESTAMP: lambda v: self._parse_datetime(v, mapping.source_date_format)
            }

            converter = conversion_map.get(mapping.data_type)
            if converter:
                result = converter(value_str)
                self.logger.debug(
                    "Converted value successfully",
                    original_value=value,
                    converted_value=result,
                    data_type=mapping.data_type.value
                )
                return result
            else:
                return value_str

        except Exception as e:
            error_msg = f"Failed to convert value '{value}' to type '{mapping.data_type.value}': {str(e)}"
            self.logger.error(
                "Data conversion error",
                original_value=value,
                target_type=mapping.data_type.value,
                error_message=str(e)
            )
            raise DataConversionException(error_msg, e)

    def _handle_null_value(self, mapping: ColumnMapping) -> Any:
        """Handle null values with default value support."""
        if mapping.default_value is not None:
            return self.convert_for_database(mapping.default_value, mapping)
        return None

    def _parse_boolean(self, value: str) -> bool:
        """Parse boolean values with flexible input support."""
        value_lower = value.lower()
        true_values = {'true', '1', 'yes', 'y', 'on'}
        false_values = {'false', '0', 'no', 'n', 'off'}

        if value_lower in true_values:
            return True
        elif value_lower in false_values:
            return False
        else:
            raise ValueError(f"Invalid boolean value: '{value}'")

    def _parse_date(self, value: str, format_pattern: Optional[str] = None) -> date:
        """Parse date values with multiple format support."""
        if format_pattern:
            try:
                return datetime.strptime(value, format_pattern).date()
            except ValueError as e:
                raise ValueError(f"Date parsing failed with format '{format_pattern}': {str(e)}")

        # Try common patterns
        for pattern in self.common_date_patterns:
            try:
                return datetime.strptime(value, pattern).date()
            except ValueError:
                continue

        raise ValueError(f"Unable to parse date '{value}' with any known format")

    def _parse_datetime(self, value: str, format_pattern: Optional[str] = None) -> datetime:
        """Parse datetime values with multiple format support."""
        if format_pattern:
            try:
                return datetime.strptime(value, format_pattern)
            except ValueError as e:
                raise ValueError(f"DateTime parsing failed with format '{format_pattern}': {str(e)}")

        # Try common patterns
        for pattern in self.common_date_patterns:
            try:
                return datetime.strptime(value, pattern)
            except ValueError:
                continue

        raise ValueError(f"Unable to parse datetime '{value}' with any known format")

    def get_sql_type_hint(self, data_type: DataType) -> str:
        """Get SQL type hint for database schema generation."""
        type_mapping = {
            DataType.STRING: "VARCHAR",
            DataType.INTEGER: "INTEGER",
            DataType.LONG: "BIGINT",
            DataType.FLOAT: "FLOAT",
            DataType.DECIMAL: "DECIMAL",
            DataType.BOOLEAN: "BOOLEAN",
            DataType.DATE: "DATE",
            DataType.DATETIME: "TIMESTAMP",
            DataType.TIMESTAMP: "TIMESTAMP"
        }
        return type_mapping.get(data_type, "VARCHAR")
