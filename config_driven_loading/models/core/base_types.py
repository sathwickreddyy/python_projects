"""
Base types and enums for the data ingestion library.

@author sathwick
"""
from datetime import datetime
from enum import Enum

from pydantic import BaseModel


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


class LoadingStats(BaseModel):
    """Statistics for data loading operations."""
    read_time_ms: int = 0
    process_time_ms: int = 0
    write_time_ms: int = 0
    batch_count: int = 0
    records_per_second: float = 0.0
    total_records: int = 0
    successful_records: int = 0
    error_records: int = 0
    execution_time: datetime

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }