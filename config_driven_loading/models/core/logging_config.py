"""
Comprehensive logging configuration for the library

@author sathwick
"""

import logging
import structlog
from typing import Dict, Any, List
import sys
from datetime import datetime

def setup_logging(log_level: str = "INFO", json_format: bool = False) -> None:
    """
    Configure Structured logging for the application.
    :param log_level: The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :param json_format: Whether to use JSON format for logs
    :return: None
    """
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if json_format else structlog.dev.ConsoleRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper()),
    )


class DataIngestionLogger:
    """Custom logger for data ingestion operations with structured logging."""

    def __init__(self, name: str):
        self.logger = structlog.get_logger(name)

    def info(self, message: str, **kwargs):
        """Log an info message with context."""
        self.logger.info(message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Log a debug message with context."""
        self.logger.debug(message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log a warning message with context."""
        self.logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log an error message with context."""
        self.logger.error(message, **kwargs)

    def critical(self, message: str, **kwargs):
        """Log a critical message with context."""
        self.logger.critical(message, **kwargs)

    def log_data_loading_start(self, source_type: str, source_path: str, target_table: str):
        """Log the start of data loading operation."""
        self.info(
            "Starting data loading operation",
            source_type=source_type,
            source_path=source_path,
            target_table=target_table,
            timestamp=datetime.now().isoformat()
        )

    def log_data_loading_complete(self, stats: Dict[str, Any]):
        """Log the completion of data loading operation."""
        self.info(
            "Data loading operation completed",
            **stats,
            timestamp=datetime.now().isoformat()
        )

    def log_conversion_error(self, row_number: int, error_message: str, **kwargs):
        """Log data conversion errors."""
        self.error(
            "Data conversion error",
            row_number=row_number,
            error_message=error_message,
            **kwargs
        )

    def log_validation_error(self, row_number: int, validation_errors: List[str], **kwargs):
        """Log validation errors."""
        self.error(
            "Validation error",
            row_number=row_number,
            validation_errors=validation_errors,
            **kwargs
        )