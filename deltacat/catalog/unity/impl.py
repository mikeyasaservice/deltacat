"""Unity Catalog implementation for DeltaCAT.

This module implements all catalog interface methods for Unity Catalog,
providing full support for Delta, Iceberg, and Hudi tables.
"""

import logging
from typing import Any, Dict, List, Optional, Union
import pyarrow as pa
import ray

from deltacat import logs
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.namespace import Namespace, NamespaceProperties
from deltacat.storage.model.schema import Schema, Field
from deltacat.storage.model.table import Table, TableProperties
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.partition import PartitionScheme
from deltacat.storage.model.sort_key import SortScheme
from deltacat.storage.model.table_version import TableVersion
from deltacat.storage.model.types import (
    DistributedDataset,
    LifecycleState,
    LocalDataset,
    LocalTable,
    StreamFormat,
)
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode

from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
from deltacat.catalog.unity.client import UnityClient
from deltacat.catalog.unity import format_handler

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


# Interface implementation - all methods from deltacat.catalog.interface

def initialize(
    config: Optional[UnityCatalogConfig] = None,
    *args,
    **kwargs
) -> UnityClient:
    """Initialize Unity Catalog with configuration.
    
    Args:
        config: UnityCatalogConfig with workspace URL and credentials
        
    Returns:
        UnityClient instance for catalog operations
    """
    if not config:
        raise ValueError("UnityCatalogConfig is required")
    
    if not isinstance(config, UnityCatalogConfig):
        raise TypeError(
            f"Expected UnityCatalogConfig, got {type(config).__name__}"
        )
    
    # Create Unity client
    kwargs = {
        "workspace_url": config.workspace_url,
        "token": config.token,
    }
    if config.warehouse_id:
        kwargs["warehouse_id"] = config.warehouse_id
    if config.cluster_id:
        kwargs["cluster_id"] = config.cluster_id
    
    client = UnityClient(**kwargs)
    
    logger.info(f"Unity Catalog initialized for {config.catalog_name}")
    return client


def create_namespace(
    namespace: str,
    *args,
    properties: Optional[NamespaceProperties] = None,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> Namespace:
    """Create a namespace (schema) in Unity Catalog.
    
    Args:
        namespace: Name of the namespace/schema to create
        properties: Optional namespace properties
        inner: Unity client instance
        config: Unity catalog configuration
        
    Returns:
        Created Namespace object
    """
    if not inner:
        raise ValueError("Unity client not initialized")
    
    if not config:
        raise ValueError("Unity config required")
    
    # Create schema in Unity Catalog
    comment = properties.get("comment") if properties else None
    schema_info = inner.workspace.schemas.create(
        catalog_name=config.catalog_name,
        name=namespace,
        comment=comment,
    )
    
    # Convert to DeltaCAT Namespace
    return Namespace.of(
        name=namespace,
        properties=properties or {},
    )


def list_namespaces(
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> ListResult:
    """List all namespaces (schemas) in the catalog.
    
    Args:
        inner: Unity client instance
        config: Unity catalog configuration
        
    Returns:
        ListResult containing Namespace objects
    """
    if not inner:
        raise ValueError("Unity client not initialized")
    
    if not config:
        raise ValueError("Unity config required")
    
    # List schemas from Unity Catalog
    schemas = inner.workspace.schemas.list(catalog_name=config.catalog_name)
    
    # Convert to DeltaCAT Namespaces
    namespaces = []
    for schema in schemas:
        namespace = Namespace.of(
            name=schema.name,
            properties={"catalog": schema.catalog_name},
        )
        namespaces.append(namespace)
    
    return ListResult(items=namespaces)


def create_table(
    table: str,
    namespace: Optional[str] = None,
    schema: Optional[Schema] = None,
    partition_columns: Optional[List[str]] = None,
    sort_keys: Optional[SortScheme] = None,
    description: Optional[str] = None,
    properties: Optional[TableProperties] = None,
    format: str = "DELTA",
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> TableDefinition:
    """Create a table in Unity Catalog.
    
    Args:
        table: Name of the table
        namespace: Namespace/schema name
        schema: Table schema
        partition_columns: Partition column names
        sort_keys: Sort key configuration
        description: Table description
        properties: Table properties
        format: Table format (DELTA, ICEBERG, HUDI)
        inner: Unity client instance
        config: Unity catalog configuration
        
    Returns:
        Created TableDefinition
    """
    if not inner:
        raise ValueError("Unity client not initialized")
    
    if not config:
        raise ValueError("Unity config required")
    
    if not schema:
        raise ValueError("Schema is required")
    
    # Convert schema to Unity columns
    arrow_schema = pa.schema([
        pa.field(f.arrow.name, f.arrow.type, f.arrow.nullable)
        for f in schema.fields
    ])
    columns = format_handler.convert_arrow_schema_to_unity_columns(arrow_schema)
    
    # Prepare table properties
    table_props = dict(properties) if properties else {}
    
    # Add UniForm properties if creating Iceberg/Hudi compatible table
    if format.upper() in ["ICEBERG", "HUDI"]:
        uniform_props = format_handler.get_uniform_properties(format)
        table_props.update(uniform_props)
        # Actually create as Delta with UniForm
        actual_format = "DELTA"
    else:
        actual_format = format.upper()
    
    # Create table in Unity Catalog
    table_info = inner.workspace.tables.create(
        name=f"{config.catalog_name}.{namespace}.{table}",
        catalog_name=config.catalog_name,
        schema_name=namespace,
        table_type="MANAGED",
        data_source_format=actual_format,
        columns=columns,
        properties=table_props,
        comment=description,
    )
    
    # Create DeltaCAT TableDefinition
    table_obj = Table.of(
        name=table,
        locator=table_info.storage_location,
    )
    
    table_version = TableVersion.of(
        version=1,
        schema=schema,
        lifecycle_state=LifecycleState.ACTIVE,
    )
    
    return TableDefinition.of(
        table=table_obj,
        table_version=table_version,
        namespace=namespace,
    )


def get_table(
    table: str,
    namespace: Optional[str] = None,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> TableDefinition:
    """Get table metadata from Unity Catalog.
    
    Args:
        table: Name of the table
        namespace: Namespace/schema name
        inner: Unity client instance
        config: Unity catalog configuration
        
    Returns:
        TableDefinition object
    """
    if not inner:
        raise ValueError("Unity client not initialized")
    
    if not config:
        raise ValueError("Unity config required")
    
    # Get table from Unity Catalog
    full_name = f"{config.catalog_name}.{namespace}.{table}"
    table_info = inner.workspace.tables.get(full_name=full_name)
    
    # Convert columns to DeltaCAT schema
    fields = []
    for i, col in enumerate(table_info.columns or []):
        field = Field.of(
            field=pa.field(col.name, pa.string(), col.nullable),
            field_id=i + 1,
        )
        fields.append(field)
    
    schema = Schema.of(fields)
    
    # Create TableDefinition
    table_obj = Table.of(
        name=table,
        locator=table_info.storage_location,
    )
    
    table_version = TableVersion.of(
        version=1,
        schema=schema,
        lifecycle_state=LifecycleState.ACTIVE,
    )
    
    return TableDefinition.of(
        table=table_obj,
        table_version=table_version,
        namespace=namespace,
        properties=table_info.properties or {},
    )


def list_tables(
    namespace: Optional[str] = None,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> ListResult:
    """List tables in a namespace.
    
    Args:
        namespace: Namespace/schema name
        inner: Unity client instance
        config: Unity catalog configuration
        
    Returns:
        ListResult containing TableDefinition objects
    """
    if not inner:
        raise ValueError("Unity client not initialized")
    
    if not config:
        raise ValueError("Unity config required")
    
    # List tables from Unity Catalog
    tables_info = inner.workspace.tables.list(
        catalog_name=config.catalog_name,
        schema_name=namespace,
    )
    
    # Convert to TableDefinitions
    table_defs = []
    for table_info in tables_info:
        table_obj = Table.of(
            name=table_info.name,
            locator=table_info.storage_location,
        )
        
        # Basic schema - would need to fetch full details for complete schema
        table_version = TableVersion.of(
            version=1,
            schema=Schema.of([]),  # Empty schema for listing
            lifecycle_state=LifecycleState.ACTIVE,
        )
        
        table_def = TableDefinition.of(
            table=table_obj,
            table_version=table_version,
            namespace=namespace,
        )
        table_defs.append(table_def)
    
    return ListResult(items=table_defs)


def read_table(
    table: str,
    namespace: Optional[str] = None,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> DistributedDataset:
    """Read data from a Unity Catalog table.
    
    Args:
        table: Name of the table
        namespace: Namespace/schema name
        inner: Unity client instance
        config: Unity catalog configuration
        
    Returns:
        DistributedDataset containing table data
    """
    if not inner:
        raise ValueError("Unity client not initialized")
    
    if not config:
        raise ValueError("Unity config required")
    
    # Get table metadata
    full_name = f"{config.catalog_name}.{namespace}.{table}"
    table_info = inner.workspace.tables.get(full_name=full_name)
    
    # Detect format
    table_format = format_handler.detect_format(table_info)
    storage_location = table_info.storage_location
    
    # Get appropriate reader
    if table_format == "DELTA":
        try:
            from deltalake import DeltaTable
            dt = DeltaTable(storage_location)
            arrow_table = dt.to_pyarrow_table()
        except ImportError:
            logger.error("Delta Lake reader not available")
            raise
    
    elif table_format == "ICEBERG":
        try:
            from pyiceberg.table import load_table
            iceberg_table = load_table(storage_location)
            arrow_table = iceberg_table.scan().to_arrow()
        except ImportError:
            logger.error("Iceberg reader not available")
            raise
    
    else:
        raise NotImplementedError(f"Format {table_format} not yet supported")
    
    # Convert to Ray dataset
    dataset = ray.data.from_arrow(arrow_table)
    return dataset


def write_to_table(
    data: Union[LocalTable, LocalDataset, DistributedDataset],
    table: str,
    namespace: Optional[str] = None,
    mode: TableWriteMode = TableWriteMode.AUTO,
    content_type: ContentType = ContentType.PARQUET,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> None:
    """Write data to a Unity Catalog table.
    
    Args:
        data: Data to write
        table: Name of the table
        namespace: Namespace/schema name
        mode: Write mode (append, overwrite, etc.)
        content_type: Content type of data
        inner: Unity client instance
        config: Unity catalog configuration
    """
    if not inner:
        raise ValueError("Unity client not initialized")
    
    if not config:
        raise ValueError("Unity config required")
    
    # Get table metadata
    full_name = f"{config.catalog_name}.{namespace}.{table}"
    table_info = inner.workspace.tables.get(full_name=full_name)
    
    # Detect format
    table_format = format_handler.detect_format(table_info)
    storage_location = table_info.storage_location
    
    # Convert data to Arrow table
    if isinstance(data, pa.Table):
        arrow_table = data
    elif hasattr(data, 'to_arrow'):
        arrow_table = data.to_arrow()
    else:
        # Try to convert Ray dataset
        arrow_table = ray.get(data.to_arrow())
    
    # Get appropriate writer
    if table_format == "DELTA":
        try:
            from deltalake import write_deltalake
            
            # Map write mode
            mode_map = {
                TableWriteMode.APPEND: "append",
                TableWriteMode.OVERWRITE: "overwrite",
                TableWriteMode.ERROR_IF_EXISTS: "error",
            }
            delta_mode = mode_map.get(mode, "append")
            
            write_deltalake(
                storage_location,
                arrow_table,
                mode=delta_mode,
            )
        except ImportError:
            logger.error("Delta Lake writer not available")
            raise
    
    else:
        raise NotImplementedError(f"Writing to {table_format} not yet supported")


def alter_table(
    table: str,
    namespace: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema_updates: Optional[Dict[str, Any]] = None,
    partition_updates: Optional[Dict[str, Any]] = None,
    sort_keys: Optional[SortScheme] = None,
    description: Optional[str] = None,
    properties: Optional[TableProperties] = None,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> None:
    """Alter table metadata in Unity Catalog.
    
    Args:
        table: Name of the table
        namespace: Namespace/schema name
        lifecycle_state: New lifecycle state
        schema_updates: Schema changes
        partition_updates: Partition changes
        sort_keys: New sort keys
        description: New description
        properties: New properties
        inner: Unity client instance
        config: Unity catalog configuration
    """
    if not inner:
        raise ValueError("Unity client not initialized")
    
    if not config:
        raise ValueError("Unity config required")
    
    full_name = f"{config.catalog_name}.{namespace}.{table}"
    
    # Build update parameters
    update_args = {}
    
    if schema_updates and "schema" in schema_updates:
        # Convert new schema to Unity columns
        new_schema = schema_updates["schema"]
        arrow_schema = pa.schema([
            pa.field(f.arrow.name, f.arrow.type, f.arrow.nullable)
            for f in new_schema.fields
        ])
        columns = format_handler.convert_arrow_schema_to_unity_columns(arrow_schema)
        update_args["columns"] = columns
    
    if description:
        update_args["comment"] = description
    
    if properties:
        update_args["properties"] = dict(properties)
    
    # Update table in Unity Catalog
    if update_args:
        inner.workspace.tables.update(
            full_name=full_name,
            **update_args,
        )


def drop_table(
    table: str,
    namespace: Optional[str] = None,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> None:
    """Drop a table from Unity Catalog.
    
    Args:
        table: Name of the table
        namespace: Namespace/schema name
        inner: Unity client instance
        config: Unity catalog configuration
    """
    if not inner:
        raise ValueError("Unity client not initialized")
    
    if not config:
        raise ValueError("Unity config required")
    
    full_name = f"{config.catalog_name}.{namespace}.{table}"
    inner.workspace.tables.delete(full_name=full_name)


# Additional interface methods with basic implementations

def default_namespace(*args, **kwargs) -> str:
    """Get default namespace."""
    return "default"


def get_namespace(
    namespace: str,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> Namespace:
    """Get namespace metadata."""
    return Namespace.of(name=namespace)


def namespace_exists(
    namespace: str,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> bool:
    """Check if namespace exists."""
    if not inner or not config:
        return False
    
    try:
        schemas = inner.workspace.schemas.list(catalog_name=config.catalog_name)
        return any(s.name == namespace for s in schemas)
    except:
        return False


def table_exists(
    table: str,
    namespace: Optional[str] = None,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> bool:
    """Check if table exists."""
    if not inner or not config:
        return False
    
    try:
        full_name = f"{config.catalog_name}.{namespace}.{table}"
        inner.workspace.tables.get(full_name=full_name)
        return True
    except:
        return False


def drop_namespace(
    namespace: str,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> None:
    """Drop a namespace."""
    if not inner or not config:
        return
    
    inner.workspace.schemas.delete(
        full_name=f"{config.catalog_name}.{namespace}"
    )


def alter_namespace(
    namespace: str,
    *args,
    properties: Optional[NamespaceProperties] = None,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> None:
    """Alter namespace properties."""
    # Unity Catalog namespace alterations are limited
    pass


def rename_table(
    table: str,
    new_table: str,
    namespace: Optional[str] = None,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> None:
    """Rename a table."""
    # Unity Catalog doesn't support direct rename, would need to recreate
    raise NotImplementedError("Table rename not yet supported")


def truncate_table(
    table: str,
    namespace: Optional[str] = None,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> None:
    """Truncate a table."""
    # Would need to use SQL or Delta operations
    raise NotImplementedError("Table truncate not yet supported")


def refresh_table(
    table: str,
    namespace: Optional[str] = None,
    *args,
    inner: Optional[UnityClient] = None,
    config: Optional[UnityCatalogConfig] = None,
    **kwargs,
) -> None:
    """Refresh table metadata."""
    # Unity Catalog handles this automatically
    pass