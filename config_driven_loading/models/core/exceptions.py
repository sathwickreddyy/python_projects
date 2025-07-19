"""
Custom Exceptions for the data ingestion library

@author sathwick
"""

class DataIngestionException(Exception):
    """
    Base Exception class for data ingestion operations.
    """
    def __init__(self, message:str, cause:Exception = None):
        super().__init__(message)
        self.message = message
        self.cause = cause

class DataLoadingException(DataIngestionException):
    """Exception raised during data loading operations."""
    pass


class DataConversionException(DataIngestionException):
    """Exception raised during data type conversion."""
    pass


class ModelConversionException(DataIngestionException):
    """Exception raised during model conversion."""
    pass


class DatabaseWriteException(DataIngestionException):
    """Exception raised during database write operations."""
    pass


class ConfigurationException(DataIngestionException):
    """Exception raised for configuration errors."""
    pass


class ValidationException(DataIngestionException):
    """Exception raised during data validation."""
    pass