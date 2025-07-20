"""
Base types and enums for the data ingestion library.

@author sathwick
"""
from datetime import datetime
from enum import Enum
from typing import Optional, List, Any, Dict

from pydantic import BaseModel, Field


class DataSourceType(str, Enum):
    """Enumeration of supported data source types."""
    CSV = "CSV"
    JSON = "JSON"

class TargetType(str, Enum):
    """
    Enumeration of supported target loading types.
    """
    TABLE = "table"

class DataType(str, Enum):
    """Enumeration of supported data types for conversion."""
    STRING = "STRING"
    INTEGER = "INTEGER"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"

class MappingStrategy(str, Enum):
    """Enumeration of field mapping strategies."""
    DIRECT = "DIRECT"
    MAPPED = "MAPPED"


class ErrorDetail(BaseModel):
    """Detailed error information for a specific record."""
    row_number: int = Field(..., description="Row number where error occurred")
    error_type: str = Field(..., description="Type of error (validation, conversion, etc.)")
    error_message: str = Field(..., description="Detailed error message")
    field_name: Optional[str] = Field(None, description="Field name that caused the error")
    field_value: Optional[str] = Field(None, description="Field value that caused the error")

    def __str__(self) -> str:
        """String representation for logging and display."""
        if self.field_name:
            return f"Row {self.row_number}: {self.field_name} - {self.error_message}"
        else:
            return f"Row {self.row_number}: {self.error_message}"


class LoadingStats(BaseModel):
    """
    Comprehensive statistics for data loading operations with detailed error tracking.

    This model provides complete visibility into the data loading process including
    timing information, success/failure counts, and detailed error messages for
    debugging and monitoring purposes.
    """

    # Timing information
    read_time_ms: int = Field(0, description="Time spent reading from source in milliseconds")
    process_time_ms: int = Field(0, description="Time spent processing/transforming data in milliseconds")
    write_time_ms: int = Field(0, description="Time spent writing to target in milliseconds")
    total_time_ms: int = Field(0, description="Total execution time in milliseconds")

    # Performance metrics
    batch_count: int = Field(0, description="Number of batches processed")
    records_per_second: float = Field(0.0, description="Processing throughput in records per second")

    # Record counts
    total_records: int = Field(0, description="Total number of records processed")
    successful_records: int = Field(0, description="Number of successfully processed records")
    error_records: int = Field(0, description="Number of records that failed processing")
    skipped_records: int = Field(0, description="Number of records skipped due to filters")

    # Execution metadata
    execution_time: datetime = Field(..., description="Timestamp when execution completed")
    source_name: Optional[str] = Field(None, description="Name of the data source")
    target_table: Optional[str] = Field(None, description="Target table name")

    # Detailed error information
    validation_errors: List[str] = Field(default_factory=list, description="List of validation error messages")
    conversion_errors: List[str] = Field(default_factory=list, description="List of data conversion error messages")
    processing_errors: List[str] = Field(default_factory=list, description="List of processing error messages")
    error_details: List[ErrorDetail] = Field(default_factory=list, description="Detailed error information")

    # Summary information
    has_errors: bool = Field(False, description="True if any errors occurred during processing")
    success_rate: float = Field(0.0, description="Percentage of successfully processed records")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    def __post_init__(self):
        """Calculate derived fields after initialization."""
        self.total_time_ms = self.read_time_ms + self.process_time_ms + self.write_time_ms
        self.has_errors = self.error_records > 0
        self.success_rate = (self.successful_records / self.total_records * 100) if self.total_records > 0 else 0.0

    def add_validation_error(self, row_number: int, field_name: str, error_message: str, field_value: Any = None):
        """
        Add a validation error with detailed information.

        Args:
            row_number: Row number where error occurred
            field_name: Name of the field that failed validation
            error_message: Detailed error message
            field_value: Value that caused the error
        """
        error_detail = ErrorDetail(
            row_number=row_number,
            error_type="validation",
            error_message=error_message,
            field_name=field_name,
            field_value=str(field_value) if field_value is not None else None
        )

        self.error_details.append(error_detail)
        self.validation_errors.append(str(error_detail))
        self.has_errors = True

    def add_conversion_error(self, row_number: int, field_name: str, error_message: str, field_value: Any = None):
        """
        Add a data conversion error.

        Args:
            row_number: Row number where error occurred
            field_name: Name of the field that failed conversion
            error_message: Detailed error message
            field_value: Value that caused the error
        """
        error_detail = ErrorDetail(
            row_number=row_number,
            error_type="conversion",
            error_message=error_message,
            field_name=field_name,
            field_value=str(field_value) if field_value is not None else None
        )

        self.error_details.append(error_detail)
        self.conversion_errors.append(str(error_detail))
        self.has_errors = True

    def add_processing_error(self, row_number: int, error_message: str, field_name: str = None):
        """
        Add a general processing error.

        Args:
            row_number: Row number where error occurred
            error_message: Detailed error message
            field_name: Optional field name if error is field-specific
        """
        error_detail = ErrorDetail(
            row_number=row_number,
            error_type="processing",
            error_message=error_message,
            field_name=field_name
        )

        self.error_details.append(error_detail)
        self.processing_errors.append(str(error_detail))
        self.has_errors = True

    def get_all_errors(self) -> List[str]:
        """Get all error messages combined."""
        return self.validation_errors + self.conversion_errors + self.processing_errors

    def get_errors_by_type(self, error_type: str) -> List[ErrorDetail]:
        """Get errors filtered by type."""
        return [error for error in self.error_details if error.error_type == error_type]

    def get_error_summary(self) -> Dict[str, int]:
        """Get summary of errors by type."""
        from collections import Counter
        error_types = [error.error_type for error in self.error_details]
        return dict(Counter(error_types))

    def print_summary(self):
        """Print a comprehensive summary of the loading statistics."""
        print("\n" + "=" * 60)
        print(f"📊 DATA LOADING SUMMARY")
        print("=" * 60)
        print(f"Source: {self.source_name or 'Unknown'}")
        print(f"Target: {self.target_table or 'Unknown'}")
        print(f"Execution Time: {self.execution_time.isoformat()}")
        print()

        # Record counts
        print("📈 RECORD STATISTICS:")
        print(f"  Total Records:      {self.total_records:,}")
        print(f"  Successful:         {self.successful_records:,}")
        print(f"  Errors:            {self.error_records:,}")
        print(f"  Success Rate:       {self.success_rate:.1f}%")
        print()

        # Timing information
        print("⏱️  PERFORMANCE METRICS:")
        print(f"  Read Time:          {self.read_time_ms:,}ms")
        print(f"  Process Time:       {self.process_time_ms:,}ms")
        print(f"  Write Time:         {self.write_time_ms:,}ms")
        print(f"  Total Time:         {self.total_time_ms:,}ms")
        print(f"  Throughput:         {self.records_per_second:.2f} records/sec")
        print(f"  Batch Count:        {self.batch_count:,}")
        print()

        # Error details
        if self.has_errors:
            print("❌ ERROR DETAILS:")
            error_summary = self.get_error_summary()
            for error_type, count in error_summary.items():
                print(f"  {error_type.title()} Errors: {count:,}")
            print()

            if self.validation_errors:
                print("🔍 VALIDATION ERRORS:")
                for error in self.validation_errors[:10]:  # Show first 10
                    print(f"  - {error}")
                if len(self.validation_errors) > 10:
                    print(f"  ... and {len(self.validation_errors) - 10} more validation errors")
                print()

            if self.conversion_errors:
                print("🔄 CONVERSION ERRORS:")
                for error in self.conversion_errors[:10]:  # Show first 10
                    print(f"  - {error}")
                if len(self.conversion_errors) > 10:
                    print(f"  ... and {len(self.conversion_errors) - 10} more conversion errors")
                print()

            if self.processing_errors:
                print("⚙️  PROCESSING ERRORS:")
                for error in self.processing_errors[:10]:  # Show first 10
                    print(f"  - {error}")
                if len(self.processing_errors) > 10:
                    print(f"  ... and {len(self.processing_errors) - 10} more processing errors")
        else:
            print("✅ NO ERRORS - All records processed successfully!")

        print("=" * 60)