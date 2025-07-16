"""
Data record model for representing processed data.

@author sathwick
"""
from typing import Dict, Any, Optional
from pydantic import BaseModel

class DataRecord(BaseModel):
    """
    Represents a single data record with validation status
    """
    data: Dict[str, Any]
    row_number: int
    valid: bool
    error_message: Optional[str] = None

    @classmethod
    def create_valid(cls, data: Dict[str, Any], row_number: int) -> "DataRecord":
        """Creates a valid data record"""
        return cls(data=data, row_number=row_number, valid=True)

    @classmethod
    def create_invalid(cls, data: Dict[str, Any], row_number: int, error_message: str) -> 'DataRecord':
        """Create an invalid data record with error message."""
        return cls(data=data, row_number=row_number, valid=False, error_message=error_message)

    def is_valid(self) -> bool:
        """Check if record is valid."""
        return self.valid

    def get_data(self) -> Dict[str, Any]:
        """Get record data."""
        return self.data

    def get_row_number(self) -> int:
        """Get row number."""
        return self.row_number

    def get_error_message(self) -> Optional[str]:
        """Get error message if record is invalid."""
        return self.error_message