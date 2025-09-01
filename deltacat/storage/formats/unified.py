"""Unified interface for table format detection and operations."""

from typing import Optional, Union, Dict, Any, Type
import os
from pathlib import Path
import pyarrow as pa
import pandas as pd

from deltacat.storage.formats.base import TableFormat
from deltacat.storage.formats.delta import DeltaFormat
from deltacat.storage.formats.iceberg import IcebergFormat
from deltacat.storage.formats.parquet import ParquetFormat


def detect_format(path: str) -> Optional[str]:
    """Detect table format from path.
    
    Args:
        path: Path to table location
        
    Returns:
        Format string ('delta', 'iceberg', 'parquet') or None if unknown
    """
    if not os.path.exists(path):
        # Check if it's a catalog identifier (namespace.table)
        if '.' in path and not path.endswith('.parquet'):
            # Likely an Iceberg catalog identifier
            return 'iceberg'
        return None
    
    # Check for Delta Lake
    delta_log = Path(path) / '_delta_log'
    if delta_log.exists() and delta_log.is_dir():
        return 'delta'
    
    # Check for Iceberg
    metadata_dir = Path(path) / 'metadata'
    if metadata_dir.exists() and metadata_dir.is_dir():
        # Look for Iceberg metadata files
        metadata_files = list(metadata_dir.glob('*.metadata.json'))
        if metadata_files:
            return 'iceberg'
    
    # Check for Parquet
    if path.endswith('.parquet'):
        return 'parquet'
    
    # Check if directory contains Parquet files
    if os.path.isdir(path):
        parquet_files = list(Path(path).rglob('*.parquet'))
        if parquet_files and not delta_log.exists():
            return 'parquet'
    
    return None


def get_table_format(
    path: str,
    format: Optional[str] = None,
    **kwargs
) -> TableFormat:
    """Get appropriate table format handler.
    
    Args:
        path: Path to table location
        format: Explicit format ('delta', 'iceberg', 'parquet') or None to auto-detect
        **kwargs: Format-specific configuration
        
    Returns:
        TableFormat implementation instance
        
    Raises:
        ValueError: If format cannot be determined or is unsupported
    """
    # Auto-detect format if not specified
    if format is None:
        format = detect_format(path)
        if format is None:
            raise ValueError(
                f"Could not detect table format at {path}. "
                "Please specify format explicitly."
            )
    
    # Normalize format string
    format = format.lower()
    
    # Create appropriate handler
    if format == 'delta':
        return DeltaFormat(path, **kwargs)
    elif format == 'iceberg':
        return IcebergFormat(path, **kwargs)
    elif format == 'parquet':
        return ParquetFormat(path, **kwargs)
    else:
        raise ValueError(f"Unsupported table format: {format}")


def convert_format(
    source_path: str,
    target_path: str,
    source_format: Optional[str] = None,
    target_format: str = 'delta',
    **kwargs
) -> TableFormat:
    """Convert table from one format to another.
    
    Args:
        source_path: Path to source table
        target_path: Path for target table
        source_format: Source format (None to auto-detect)
        target_format: Target format ('delta', 'iceberg', 'parquet')
        **kwargs: Conversion options
        
    Returns:
        TableFormat handler for the new table
    """
    # Get source handler
    source = get_table_format(source_path, source_format)
    
    # Read source data
    data = source.read()
    
    # Create target handler
    target = get_table_format(target_path, target_format)
    
    # Write to target format
    partition_by = kwargs.pop('partition_by', None)
    target.write(data, mode='overwrite', partition_by=partition_by, **kwargs)
    
    return target


class UnifiedTableInterface:
    """High-level unified interface for table operations.
    
    This class provides a consistent API regardless of the underlying
    table format, automatically handling format detection and conversion.
    """
    
    def __init__(self, path: str, format: Optional[str] = None, **kwargs):
        """Initialize unified table interface.
        
        Args:
            path: Path to table
            format: Table format or None to auto-detect
            **kwargs: Format-specific configuration
        """
        self.path = path
        self.handler = get_table_format(path, format, **kwargs)
        self.format = self.handler.__class__.__name__.replace('Format', '').lower()
    
    def read(self, **kwargs) -> pa.Table:
        """Read table data."""
        return self.handler.read(**kwargs)
    
    def write(self, data: Union[pa.Table, pd.DataFrame], **kwargs) -> None:
        """Write data to table."""
        self.handler.write(data, **kwargs)
    
    def append(self, data: Union[pa.Table, pd.DataFrame]) -> None:
        """Append data to table."""
        self.handler.append(data)
    
    def schema(self) -> pa.Schema:
        """Get table schema."""
        return self.handler.get_schema()
    
    def history(self, limit: Optional[int] = None) -> list:
        """Get table history."""
        return self.handler.get_history(limit)
    
    def optimize(self, **kwargs) -> Dict[str, Any]:
        """Optimize table storage."""
        return self.handler.optimize(**kwargs)
    
    def vacuum(self, **kwargs) -> Dict[str, Any]:
        """Clean up old files."""
        return self.handler.vacuum(**kwargs)
    
    def convert_to(self, 
                   target_path: str,
                   target_format: str,
                   **kwargs) -> 'UnifiedTableInterface':
        """Convert table to different format.
        
        Args:
            target_path: Path for converted table
            target_format: Target format
            **kwargs: Conversion options
            
        Returns:
            New UnifiedTableInterface for converted table
        """
        new_handler = convert_format(
            self.path,
            target_path,
            self.format,
            target_format,
            **kwargs
        )
        
        return UnifiedTableInterface(target_path, target_format)
    
    def __repr__(self) -> str:
        """String representation."""
        return f"UnifiedTableInterface(format='{self.format}', path='{self.path}')"
    
    @classmethod
    def create(cls,
               path: str,
               data: Union[pa.Table, pd.DataFrame],
               format: str = 'delta',
               **kwargs) -> 'UnifiedTableInterface':
        """Create a new table.
        
        Args:
            path: Path for new table
            data: Initial data
            format: Table format
            **kwargs: Format-specific options
            
        Returns:
            UnifiedTableInterface for new table
        """
        handler = get_table_format(path, format)
        handler.write(data, mode='overwrite', **kwargs)
        
        return cls(path, format)


def convert_schema_to_deltacat(schema: pa.Schema) -> 'DeltaCATSchema':
    """Convert PyArrow schema to DeltaCAT schema.
    
    Args:
        schema: PyArrow schema
        
    Returns:
        DeltaCAT Schema object
    """
    from deltacat.storage.model.schema import Schema as DeltaCATSchema
    
    return DeltaCATSchema.of(schema=schema)


def convert_schema_from_deltacat(schema: 'DeltaCATSchema') -> pa.Schema:
    """Convert DeltaCAT schema to PyArrow schema.
    
    Args:
        schema: DeltaCAT Schema object
        
    Returns:
        PyArrow schema
    """
    return schema.arrow


def arrow_to_deltacat_schema(schema: pa.Schema) -> 'DeltaCATSchema':
    """Alias for convert_schema_to_deltacat for backward compatibility."""
    return convert_schema_to_deltacat(schema)


def deltacat_to_arrow_schema(schema: 'DeltaCATSchema') -> pa.Schema:
    """Alias for convert_schema_from_deltacat for backward compatibility."""
    return convert_schema_from_deltacat(schema)


def get_table_format_from_catalog(
    catalog: 'DeltaCATCatalog',
    namespace: str,
    table_name: str
) -> TableFormat:
    """Get table format handler from catalog table.
    
    Args:
        catalog: DeltaCAT catalog instance
        namespace: Catalog namespace
        table_name: Table name
        
    Returns:
        TableFormat implementation for the catalog table
    """
    # Get table definition from catalog
    table_def = catalog.get_table(
        name=table_name,
        namespace=namespace
    )
    
    if not table_def:
        raise ValueError(f"Table {namespace}.{table_name} not found in catalog")
    
    # Get path and format from table properties
    properties = table_def.properties or {}
    path = properties.get('path')
    format_type = properties.get('format')
    
    if not path:
        raise ValueError(f"Table {namespace}.{table_name} has no path property")
    
    return get_table_format(path, format_type)


def integrate_with_catalog(
    catalog: 'DeltaCATCatalog',
    namespace: str,
    table_name: str,
    path: str,
    format: Optional[str] = None
) -> Dict[str, Any]:
    """Integrate table format with DeltaCAT catalog.
    
    Args:
        catalog: DeltaCAT catalog instance
        namespace: Catalog namespace
        table_name: Table name in catalog
        path: Path to table
        format: Table format or None to auto-detect
        
    Returns:
        Dictionary with integration details
    """
    # Get table handler
    handler = get_table_format(path, format)
    
    # Get schema
    schema = handler.get_schema()
    
    # Convert to DeltaCAT schema
    deltacat_schema = convert_schema_to_deltacat(schema)
    
    # Register with catalog
    from deltacat.catalog.model.table_definition import TableDefinition
    
    table_def = catalog.create_table(
        name=table_name,
        namespace=namespace,
        schema=deltacat_schema,
        properties={
            'format': handler.__class__.__name__.replace('Format', '').lower(),
            'path': path
        }
    )
    
    return {
        'catalog': catalog.name,
        'namespace': namespace,
        'table': table_name,
        'format': format or detect_format(path),
        'path': path,
        'registered': True
    }


def read_table(
    path: str,
    format: Optional[str] = None,
    **kwargs
) -> pa.Table:
    """Read table using appropriate format handler.
    
    Args:
        path: Path to table
        format: Table format or None to auto-detect
        **kwargs: Read options
        
    Returns:
        PyArrow Table with the data
    """
    handler = get_table_format(path, format)
    return handler.read(**kwargs)


def write_table(
    path: str,
    data: Union[pa.Table, pd.DataFrame],
    format: str = 'delta',
    **kwargs
) -> None:
    """Write table using appropriate format handler.
    
    Args:
        path: Path to table
        data: Data to write
        format: Table format
        **kwargs: Write options
    """
    handler = get_table_format(path, format)
    handler.write(data, **kwargs)


def create_table(
    path: str,
    schema: pa.Schema,
    format: str = 'delta',
    **kwargs
) -> TableFormat:
    """Create a new table with schema.
    
    Args:
        path: Path for new table
        schema: Table schema
        format: Table format
        **kwargs: Format-specific options
        
    Returns:
        TableFormat handler for new table
    """
    # Create empty table with schema
    empty_table = pa.Table.from_pydict({}, schema=schema)
    handler = get_table_format(path, format)
    handler.write(empty_table, mode='overwrite', **kwargs)
    return handler