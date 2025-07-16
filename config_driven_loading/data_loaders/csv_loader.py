"""
CSV data loader with comprehensive error handling and configuration support.

This loader is responsible for reading CSV files and converting each row into a DataRecord object
for further processing within the data ingestion pipeline.

Features:
---------
- Configurable delimiter and encoding
- Supports CSV files with or without headers
- Skips initial rows if configured
- Graceful error handling for file access and row-level parsing issues
- Memory-efficient row-wise processing using generator (yield)
- Logging at various stages for observability and debugging

Usage:
------
Example usage within the data ingestion framework:

    csv_loader = CSVDataLoader()

    # Assuming 'config' is an instance of DataSourceDefinition
    try:
        data_records = csv_loader.load_data(config)
        for record in data_records:
            process_record(record)  # your processing function
    except DataLoadingException as e:
        logger.error(f"Failed to load CSV data: {str(e)}")

Author:
-------
@sathwick
"""
import csv
from typing import Iterator, Dict, Any
from pathlib import Path
from core.base_types import DataSourceType
from data_loaders.base_loader import BaseDataLoader
from models.data_record import DataRecord
from config.data_loader_config import DataSourceDefinition
from core.exceptions import DataLoadingException


class CSVDataLoader(BaseDataLoader):
    """
    CSV data loader that reads CSV files and converts rows to DataRecord objects.

    Methods:
    --------
    - get_type(): Returns the loader type (csv).
    - load_data(config): Reads the CSV file and yields DataRecord objects.

    Raises:
    -------
    DataLoadingException:
        If file does not exist, is not a file, or any fatal error occurs while reading.

    Example:
    --------
    csv_loader = CSVDataLoader()
    data_records = csv_loader.load_data(config)

    for record in data_records:
        print(record)
    """

    def get_type(self) -> str:
        """
        Return the CSV loader type identifier.

        Returns:
            str: The string 'csv' as defined in DataSourceType
        """
        return DataSourceType.CSV.value

    def load_data(self, config: DataSourceDefinition) -> Iterator[DataRecord]:
        """
        Load data from a CSV file based on the provided configuration.

        This method uses a generator to yield one DataRecord at a time, making it efficient for large files.

        Args:
            config (DataSourceDefinition): Configuration specifying file path, delimiter,
                                           encoding, header presence, and skip row options.

        Yields:
            Iterator[DataRecord]: Yields DataRecord objects representing each CSV row.

        Raises:
            DataLoadingException: If file not found, invalid file, or any other loading issue.
        """
        self.validate_config(config)

        source = config.source
        file_path = Path(source.file_path)

        if not file_path.exists():
            raise DataLoadingException(f"CSV file not found: {file_path}")

        if not file_path.is_file():
            raise DataLoadingException(f"Path is not a file: {file_path}")

        self.logger.info(
            "Loading CSV file",
            file_path=str(file_path),
            delimiter=source.delimiter,
            encoding=source.encoding,
            has_header=source.header
        )

        try:
            with open(file_path, 'r', encoding=source.encoding, newline='') as csvfile:
                reader = csv.DictReader(
                    csvfile,
                    delimiter=source.delimiter,
                    skipinitialspace=True
                ) if source.header else csv.reader(csvfile, delimiter=source.delimiter)

                if source.skip_rows and source.skip_rows > 0:
                    for _ in range(source.skip_rows):
                        next(reader, None)

                row_number = 1
                processed_rows = 0

                for row in reader:
                    try:
                        if isinstance(row, dict):
                            data = {key: value for key, value in row.items() if key is not None}
                        else:
                            data = {f"column_{i}": value for i, value in enumerate(row)}

                        yield self._create_data_record(data, row_number)
                        processed_rows += 1

                        if processed_rows % 1000 == 0:
                            self.logger.debug(f"Processed {processed_rows} CSV rows")

                    except Exception as e:
                        self.logger.error(
                            "Failed to process CSV row",
                            row_number=row_number,
                            error_message=str(e)
                        )
                        yield self._create_error_record(
                            {}, row_number, f"CSV parsing error: {str(e)}"
                        )

                    row_number += 1

                self.logger.info(
                    "CSV loading completed",
                    total_rows=processed_rows,
                    file_path=str(file_path)
                )

        except Exception as e:
            self.logger.error(
                "CSV loading failed",
                file_path=str(file_path),
                error_message=str(e)
            )
            raise DataLoadingException(f"Failed to load CSV file: {str(e)}", e)