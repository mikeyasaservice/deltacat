"""Table format abstractions for DeltaCAT.

This module provides a unified interface for working with different table formats
including Delta Lake, Apache Iceberg, and Parquet files.
"""

from deltacat.storage.formats.base import TableFormat, TableMetadata
from deltacat.storage.formats.delta import DeltaFormat
from deltacat.storage.formats.iceberg import IcebergFormat
from deltacat.storage.formats.parquet import ParquetFormat
from deltacat.storage.formats.unified import (
    get_table_format,
    detect_format,
    convert_format,
    UnifiedTableInterface
)

__all__ = [
    "TableFormat",
    "TableMetadata",
    "DeltaFormat",
    "IcebergFormat",
    "ParquetFormat",
    "get_table_format",
    "detect_format",
    "convert_format",
    "UnifiedTableInterface"
]