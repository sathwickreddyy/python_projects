"""
JSON data loader with nested path support and JSONPath integration.

This loader reads JSON files and converts the content into DataRecord objects.
It supports flexible extraction strategies such as JSONPath expressions, dot notation,
and direct array access.

Typical Usage Example:

    loader = JSONDataLoader()

    try:
        records = loader.load_data(config)
        for record in records:
            print(record)
    except DataLoadingException as e:
        logger.error(f"Failed to load JSON: {e}")


@author Sathwick
"""
import json
from typing import Iterator, Dict, Any, List, Union
from pathlib import Path
from jsonpath_ng import parse as jsonpath_parse

from data_loaders.base_loader import BaseDataLoader
from models.data_record import DataRecord
from config.data_loader_config import DataSourceDefinition, ColumnMapping
from core.exceptions import DataLoadingException
from core.base_types import DataSourceType


class JSONDataLoader(BaseDataLoader):
    """
    JSON data loader with comprehensive nested path support.

    Features:
    - JSONPath expression support
    - Dot notation for nested field access
    - Array element access (items[0])
    - Resilient error handling for malformed or unexpected JSON formats
    - Configurable through DataSourceDefinition
    """

    def get_type(self) -> str:
        """
        Identifies the type of loader.

        Returns:
            str: Identifier for this loader, 'JSON'
        """
        return DataSourceType.JSON.value

    def load_data(self, config: DataSourceDefinition) -> Iterator[DataRecord]:
        """
        Load data from JSON file with nested path support.

        Args:
            config (DataSourceDefinition): Configuration for the data source.

        Yields:
            Iterator[DataRecord]: DataRecord instances representing extracted rows.

        Raises:
            DataLoadingException: If file is not found or JSON is malformed.
        """
        self.validate_config(config)

        source = config.source
        file_path = Path(source.file_path)

        if not file_path.exists():
            raise DataLoadingException(f"JSON file not found: {file_path}")

        # Start processing
        self.logger.info(
            "Loading JSON file",
            file_path=str(file_path),
            json_path=source.json_path
        )

        try:
            # Load JSON content from file
            with open(file_path, 'r', encoding=source.encoding or 'utf-8') as jsonfile:
                json_data = json.load(jsonfile)

            # Extract target nodes (usually arrays) using JSONPath or fallback
            data_nodes = self._extract_data_nodes(json_data, source.json_path)

            self.logger.debug(f"Extracted {len(data_nodes)} data nodes from JSON")

            # Process each node into DataRecord
            for row_number, node in enumerate(data_nodes, 1):
                try:
                    if config.column_mapping:
                        # If column mapping is provided, extract values per mapping
                        data = self._process_column_mappings(node, config.column_mapping)
                    else:
                        # Otherwise, take entire node content as dictionary
                        data = self._extract_all_fields(node)

                    yield self._create_data_record(data, row_number)

                except Exception as e:
                    # Error in single record: capture as error DataRecord
                    self.logger.error(
                        "Failed to process JSON node",
                        row_number=row_number,
                        error_message=str(e)
                    )
                    yield self._create_error_record(
                        {}, row_number, f"JSON processing error: {str(e)}"
                    )

            self.logger.info(
                "JSON loading completed",
                total_nodes=len(data_nodes),
                file_path=str(file_path)
            )

        except Exception as e:
            # Log and raise failure to load JSON file
            self.logger.error(
                "JSON loading failed",
                file_path=str(file_path),
                error_message=str(e)
            )
            raise DataLoadingException(f"Failed to load JSON file: {str(e)}", e)

    def _extract_data_nodes(self, json_data: Any, json_path: str = None) -> List[Any]:
        """
        Extract relevant nodes from JSON using JSONPath or known array field names.

        Args:
            json_data (Any): Parsed JSON data
            json_path (str): Optional JSONPath expression

        Returns:
            List[Any]: List of extracted data nodes
        """
        if json_path:
            try:
                # JSONPath-based extraction
                jsonpath_expr = jsonpath_parse(json_path)
                matches = jsonpath_expr.find(json_data)
                return [match.value for match in matches]
            except Exception as e:
                self.logger.warning(
                    "JSONPath extraction failed, falling back to direct access",
                    json_path=json_path,
                    error_message=str(e)
                )

        # Fallback to common conventions (e.g., "data", "items", etc.)
        if isinstance(json_data, list):
            return json_data
        elif isinstance(json_data, dict):
            for field in ['data', 'items', 'results', 'records', 'content']:
                if field in json_data and isinstance(json_data[field], list):
                    return json_data[field]
            return [json_data]
        else:
            return [json_data]

    def _process_column_mappings(self, node: Any, mappings: List[ColumnMapping]) -> Dict[str, Any]:
        """
        Apply column mappings to extract fields from JSON node.

        Args:
            node (Any): JSON node (dict or object)
            mappings (List): List of mapping configurations

        Returns:
            Dict[str, Any]: Extracted data keyed by target column names.
        """
        data = {}

        for mapping in mappings:
            source_path = mapping.source
            target_field = mapping.target

            try:
                # Extract value from nested path
                value = self._extract_value_from_path(node, source_path)

                if value is not None:
                    data[target_field] = value
                elif mapping.default_value is not None:
                    data[target_field] = mapping.default_value

            except Exception as e:
                self.logger.warning(
                    "Failed to extract value from path",
                    source_path=source_path,
                    target_field=target_field,
                    error_message=str(e)
                )
                if mapping.default_value is not None:
                    data[target_field] = mapping.default_value

        return data

    def _extract_value_from_path(self, node: Any, path: str) -> Any:
        """
        Extract value from JSON node via JSONPath or dot notation.

        Args:
            node (Any): Current JSON node (dict)
            path (str): Path expression to extract value

        Returns:
            Any: Extracted value or None
        """
        if not path:
            return None

        if path.startswith('$') or '[?' in path:
            try:
                jsonpath_expr = jsonpath_parse(path)
                matches = jsonpath_expr.find(node)
                return matches[0].value if matches else None
            except Exception:
                return None

        # Dot notation fallback
        return self._extract_with_dot_notation(node, path)

    def _extract_with_dot_notation(self, node: Any, path: str) -> Any:
        """
        Extract nested fields via dot notation with optional array access.

        Example path: user.address.city or items[0].id

        Args:
            node (Any): JSON object (dict)
            path (str): Dot notation path

        Returns:
            Any: Value at path or None
        """
        current = node
        parts = path.split('.')

        for part in parts:
            if current is None:
                return None

            if '[' in part and ']' in part:
                # Handle array like 'items[0]'
                field_name = part[:part.index('[')]
                index_str = part[part.index('[') + 1:part.index(']')]

                if isinstance(current, dict) and field_name in current:
                    current = current[field_name]
                    if isinstance(current, list):
                        try:
                            current = current[0] if index_str == '*' else current[int(index_str)]
                        except (ValueError, IndexError):
                            return None
                    else:
                        return None
                else:
                    return None
            else:
                # Simple dictionary field access
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return None

        return current

    def _extract_all_fields(self, node: Any) -> Dict[str, Any]:
        """
        Extract all fields from a JSON node if no column mapping is provided.

        Args:
            node (Any): JSON object

        Returns:
            Dict[str, Any]: Flattened fields dictionary
        """
        if isinstance(node, dict):
            return node
        else:
            # Non-dictionary content fallback
            return {'value': node}