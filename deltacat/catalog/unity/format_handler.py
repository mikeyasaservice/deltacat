"""Format handling for Delta, Iceberg, and Hudi tables in Unity Catalog."""

import logging
from typing import Dict, Any, List, Union, Optional

from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def detect_format(table_info: Any) -> str:
    """Detect the format of a Unity Catalog table.
    
    Args:
        table_info: TableInfo object from Unity Catalog
        
    Returns:
        Format string: "DELTA", "ICEBERG", or "HUDI"
    """
    if hasattr(table_info, 'data_source_format'):
        return table_info.data_source_format.upper()
    
    # Default to Delta if format is not specified
    return "DELTA"


def get_uniform_properties(
    formats: Union[str, List[str]]
) -> Dict[str, Any]:
    """Generate Delta UniForm properties for cross-format compatibility.
    
    Delta UniForm allows Delta tables to be read as Iceberg or Hudi tables
    without data duplication, by automatically generating the necessary
    metadata for these formats.
    
    Args:
        formats: Target format(s) - "ICEBERG", "HUDI", or list of both
        
    Returns:
        Dictionary of table properties for UniForm
    """
    if isinstance(formats, str):
        formats = [formats]
    
    # Normalize format names
    normalized = []
    for fmt in formats:
        fmt_upper = fmt.upper()
        if fmt_upper == "ICEBERG":
            normalized.append("iceberg")
        elif fmt_upper == "HUDI":
            normalized.append("hudi")
    
    if not normalized:
        return {}
    
    # Generate UniForm properties
    properties = {
        "delta.universalFormat.enabledFormats": ",".join(normalized)
    }
    
    # Add version-specific properties if needed
    if "iceberg" in normalized:
        # Iceberg-specific UniForm settings
        properties.update({
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
        })
    
    logger.debug(f"Generated UniForm properties for {normalized}: {properties}")
    return properties


def should_use_uniform(
    source_format: str, target_format: str
) -> bool:
    """Determine if UniForm should be used for format compatibility.
    
    Args:
        source_format: Current table format
        target_format: Desired read/write format
        
    Returns:
        True if UniForm should be enabled
    """
    source = source_format.upper()
    target = target_format.upper()
    
    # UniForm is only needed when converting between formats
    if source == target:
        return False
    
    # UniForm is primarily for Delta tables to be read as Iceberg/Hudi
    if source == "DELTA" and target in ["ICEBERG", "HUDI"]:
        return True
    
    return False


def get_format_reader(format_type: str):
    """Get the appropriate reader for a table format.
    
    Args:
        format_type: Table format (DELTA, ICEBERG, HUDI)
        
    Returns:
        Reader module or None if not available
    """
    format_upper = format_type.upper()
    
    if format_upper == "DELTA":
        try:
            from deltalake import DeltaTable
            return DeltaTable
        except ImportError:
            logger.warning("Delta Lake reader not available. Install with: pip install deltalake")
            return None
    
    elif format_upper == "ICEBERG":
        try:
            from pyiceberg.table import load_table
            return load_table
        except ImportError:
            logger.warning("Iceberg reader not available. Install with: pip install pyiceberg")
            return None
    
    elif format_upper == "HUDI":
        # Hudi support is more complex and typically requires Spark
        logger.warning("Hudi format support is experimental")
        return None
    
    else:
        logger.warning(f"Unknown format: {format_type}")
        return None


def get_format_writer(format_type: str):
    """Get the appropriate writer for a table format.
    
    Args:
        format_type: Table format (DELTA, ICEBERG, HUDI)
        
    Returns:
        Writer function or None if not available
    """
    format_upper = format_type.upper()
    
    if format_upper == "DELTA":
        try:
            from deltalake import write_deltalake
            return write_deltalake
        except ImportError:
            logger.warning("Delta Lake writer not available. Install with: pip install deltalake")
            return None
    
    elif format_upper == "ICEBERG":
        # For Iceberg, we typically write through PyIceberg Table API
        # or use Delta with UniForm for compatibility
        logger.info("Iceberg writing will use Delta UniForm for compatibility")
        try:
            from deltalake import write_deltalake
            return write_deltalake  # Write as Delta with UniForm
        except ImportError:
            return None
    
    elif format_upper == "HUDI":
        logger.warning("Hudi write support is experimental")
        return None
    
    else:
        logger.warning(f"Unknown format: {format_type}")
        return None


def convert_arrow_schema_to_unity_columns(
    arrow_schema: Any
) -> List[Any]:
    """Convert PyArrow schema to Unity Catalog column definitions.
    
    Args:
        arrow_schema: PyArrow schema
        
    Returns:
        List of ColumnInfo objects for Unity Catalog
    """
    try:
        from databricks.sdk.service.catalog import ColumnInfo, ColumnTypeName
    except ImportError:
        logger.warning("Databricks SDK not available")
        return []
    
    columns = []
    for field in arrow_schema:
        # Map PyArrow types to Unity Catalog types
        type_mapping = {
            "int8": ColumnTypeName.BYTE,
            "int16": ColumnTypeName.SHORT,
            "int32": ColumnTypeName.INT,
            "int64": ColumnTypeName.LONG,
            "uint8": ColumnTypeName.BYTE,
            "uint16": ColumnTypeName.SHORT,
            "uint32": ColumnTypeName.INT,
            "uint64": ColumnTypeName.LONG,
            "float32": ColumnTypeName.FLOAT,
            "float64": ColumnTypeName.DOUBLE,
            "bool": ColumnTypeName.BOOLEAN,
            "string": ColumnTypeName.STRING,
            "large_string": ColumnTypeName.STRING,
            "binary": ColumnTypeName.BINARY,
            "large_binary": ColumnTypeName.BINARY,
            "date32": ColumnTypeName.DATE,
            "date64": ColumnTypeName.DATE,
            "timestamp": ColumnTypeName.TIMESTAMP,
            "time32": ColumnTypeName.TIMESTAMP,
            "time64": ColumnTypeName.TIMESTAMP,
            "decimal128": ColumnTypeName.DECIMAL,
        }
        
        # Get the type name string
        type_str = str(field.type)
        for arrow_type, unity_type in type_mapping.items():
            if arrow_type in type_str:
                column = ColumnInfo(
                    name=field.name,
                    type_name=unity_type,
                    nullable=field.nullable,
                    comment=None,
                )
                columns.append(column)
                break
        else:
            # Default to STRING for unknown types
            column = ColumnInfo(
                name=field.name,
                type_name=ColumnTypeName.STRING,
                nullable=field.nullable,
                comment=f"Original type: {field.type}",
            )
            columns.append(column)
    
    return columns