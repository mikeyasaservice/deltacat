"""DeltaCAT SQL Interface - Query DeltaCAT tables using SQL via DuckDB.

This module provides a SQL interface to DeltaCAT tables using DuckDB's
zero-copy Arrow integration. Tables are exposed as Arrow Datasets which
DuckDB can query directly without data movement.
"""

from deltacat.sql.gateway import DeltaCATSQLGateway
from deltacat.sql.catalog_adapter import CatalogAdapter
from deltacat.sql.iceberg_adapter import IcebergSQLAdapter

__all__ = [
    "DeltaCATSQLGateway",
    "CatalogAdapter",
    "IcebergSQLAdapter",
]