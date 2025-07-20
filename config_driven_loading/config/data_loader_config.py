"""
Skeleton for configuration files i.e, YAML Config.

@version 1.0.0 supports CSV loading, JSON loading to database
@author sathwick
"""

from typing import Dict, List, Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from models.core.base_types import DataSourceType, TargetType, DataType, MappingStrategy

class SourceConfig(BaseModel):
    """
    Configuration for the data source connection and reading params.
    """
    file_path: Optional[str] = Field(None, description="Path to the data file")

    # CSV Related Skeleton
    delimiter: Optional[str] = Field(",", description="Delimiter to use for CSV Files")
    header: Optional[bool] = Field(True, description="Whether first row contains header")
    encoding: Optional[str] = Field("utf-8", description="Character encoding to use for CSV Files")

    # API Specific Skeleton
    url: Optional[str] = Field(None, description="API Endpoint URL")
    method: Optional[str] = Field("GET", description="HTTP Method to read the response")
    headers: Optional[Dict[str, str]] = Field(None, description="HTTP headers")
    timeout: Optional[int] = Field(30, description="Request timeout in seconds")
    retry_attempts: Optional[int] = Field(3, description="Number of retry attempts")

    # JSON-specific parameters
    json_path: Optional[str] = Field(None, description="JSONPath expression for data extraction")

class TargetConfig(BaseModel):
    """
    Configuration for the target Destination.
    """

    # database related configuration
    schema_name: str = Field(None, description="Database Schema name")
    table: str = Field(None, description="Table name")
    type: TargetType = Field(TargetType.TABLE, description="Target Type")
    batch_size: Optional[int] = Field(1000, description="Batch size for database operations")
    enabled: bool = Field(..., description="Whether this target is enabled for processing") # false should ensure first 10 records to be printed

    @classmethod
    @field_validator("batch_size")
    def validate_batch_size(cls, v):
        if v is not None and (v < 1 or v > 10000):
            raise ValueError('Batch size must be between 1 and 10000')
        return v

    @model_validator(mode="after")
    def validate_table_fields(self):
        if self.type == TargetType.TABLE:
            if not self.schema_name or not self.table:
                raise ValueError("When type is 'table', 'schema_name' and 'table' must be provided.")
        return self


class ColumnMapping(BaseModel):
    """Configuration for column mapping with type conversion."""
    source: str = Field(..., description="Source column name or JSON path")
    target: str = Field(..., description="Target column name")
    data_type: DataType = Field(DataType.STRING, description="Data type for conversion")
    source_date_format: Optional[str] = Field(None, description="Source date format pattern")
    target_date_format: Optional[str] = Field(None, description="Target date format pattern")
    timezone: Optional[str] = Field(None, description="Timezone for datetime conversion")
    decimal_format: Optional[str] = Field(None, description="Decimal format pattern")
    required: Optional[bool] = Field(False, description="Whether field is required")
    default_value: Optional[str] = Field(None, description="Default value for missing fields")

class ModelConfig(BaseModel):
    """
    Configuration for the model-based processing.
    """
    class_name: str = Field(..., description="Model class name")
    mapping_strategy: MappingStrategy = Field(MappingStrategy.MAPPED, description="Mapping strategy")
    strict_mapping: Optional[bool] = Field(True, description="Whether to enforce strict mapping")
    date_format: Optional[str] = Field(None, description="Default date format")
    timezone: Optional[str] = Field(None, description="Default timezone")


class ValidationConfig(BaseModel):
    """Configuration for data validation."""
    required_columns: Optional[List[str]] = Field(None, description="List of required columns")
    data_quality_checks: bool = Field(False, description="Enable data quality checks")

class InputOutputMapping(BaseModel):
    mapping_strategy: MappingStrategy = Field(..., description="Mapping strategy")
    column_mappings: Optional[List[ColumnMapping]] = Field(None, description="Column mappings from input field to output field")

    @model_validator(mode="after")
    def validate_mapping_strategy(self):
        if self.mapping_strategy == MappingStrategy.MAPPED:
            if not self.column_mappings or len(self.column_mappings) == 0:
                raise ValueError(
                    "Column mappings must be explicitly provided when 'MAPPED' strategy is selected."
                )
        elif self.mapping_strategy == MappingStrategy.DIRECT:
            if self.column_mappings:
                raise ValueError(
                    "Column mappings should not be provided when 'DIRECT' strategy is selected. "
                    "In 'DIRECT' mode, it is expected that camelCase input fields will automatically map "
                    "to equivalent snake_case target fields (typically in uppercase)."
                )
        return self

class DataSourceDefinition(BaseModel):
    """
    Complete definition of a data source.
    """
    type: DataSourceType = Field(..., description="Type of data source")
    source_config: SourceConfig = Field(..., description="Source configuration")
    target_config: TargetConfig = Field(..., description="Target configuration")
    input_output_mapping: InputOutputMapping = Field(..., description="Input and Output mapping definitions")
    validation: Optional[ValidationConfig] = Field(None, description="Validation configuration")

    @classmethod
    @field_validator('model')
    def validate_model_config(cls, v, values):
        if 'target' in values and values['target'].type == TargetType.MODEL and v is None:
            raise ValueError('Model configuration is required when target type is MODEL')
        return v

class DataLoaderConfiguration(BaseModel):
    """
    Root configuration for data loading operations.
    """

    data_sources: Dict[str, DataSourceDefinition] = Field(
        ..., description="Dictionary/Map of data source definitions."
    )

    @classmethod
    @field_validator('data_sources')
    def validate_data_sources(cls, v, values):
        if not v:
            raise ValueError('At least one data source must be configured')
        return v
