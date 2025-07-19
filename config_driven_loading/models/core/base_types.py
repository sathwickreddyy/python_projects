"""
Base types and enums for the data ingestion library.

@author sathwick
"""
from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class DataSourceType(str, Enum):
    """
    Enumeration of supported data source types.
    """
    CSV = "csv"
    JSON = "json"
    API = "api"
    PROTO = "proto"

class TargetType(str, Enum):
    """
    Enumeration of supported target loading types.
    """
    TABLE = "table"
    MODEL = "model"

class DataType(str, Enum):
    """
    Enumeration of supported data types for conversions.
    """
    STRING = "string"
    INTEGER = "integer"
    LONG = "long"
    FLOAT = "float"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"

class MappingStrategy(str, Enum):
    """
    Enumeration of supported mapping strategies for model based loadings.
    """
    DIRECT = "direct"
    MAPPED = "mapped"

class LoadingStats(BaseModel):
    """
    Statistics for data loading operations.
    """
    read_time_ms: int = 0
    process_time_ms: int = 0
    write_time_ms: int = 0
    batch_count: int = 0
    records_per_second: int = 0
    total_records: int = 0
    successful_records: int = 0
    error_records: int = 0
    execution_time_ms: int = 0

    class Config:
        """
        Pydantic Config class used to customize how Pydantic models behave when converting to JSON.
        """
        json_encoders = {
            datetime: lambda dt: dt.isoformat(),
        }