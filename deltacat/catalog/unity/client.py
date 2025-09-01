"""Unity Catalog client wrapper for DeltaCAT."""

import logging
from typing import List, Optional, Any
from dataclasses import dataclass

from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

# Try to import Databricks SDK
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import (
        CatalogInfo,
        SchemaInfo,
        TableInfo,
        ColumnInfo,
    )
    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False
    logger.warning(
        "Databricks SDK not available. Install with: pip install databricks-sdk"
    )
    # Create mock classes for when SDK is not available
    WorkspaceClient = None
    CatalogInfo = Any
    SchemaInfo = Any
    TableInfo = Any
    ColumnInfo = Any


class UnityClient:
    """Client wrapper for Unity Catalog operations.
    
    This class wraps the Databricks SDK WorkspaceClient to provide
    Unity Catalog operations for DeltaCAT.
    """
    
    def __init__(
        self,
        workspace_url: str,
        token: str,
        warehouse_id: Optional[str] = None,
        cluster_id: Optional[str] = None,
    ):
        """Initialize Unity Catalog client.
        
        Args:
            workspace_url: Databricks workspace URL
            token: Authentication token
            warehouse_id: Optional SQL warehouse ID
            cluster_id: Optional compute cluster ID
        """
        if not DATABRICKS_SDK_AVAILABLE:
            raise ImportError(
                "Databricks SDK is required for Unity Catalog support. "
                "Install with: pip install databricks-sdk"
            )
        
        self.workspace_url = workspace_url
        self.token = token
        self.warehouse_id = warehouse_id
        self.cluster_id = cluster_id
        
        # Initialize Databricks workspace client
        self.workspace = WorkspaceClient(
            host=workspace_url,
            token=token,
        )
        
        logger.info(f"Unity Catalog client initialized for {workspace_url}")
    
    def list_catalogs(self) -> List[CatalogInfo]:
        """List all available catalogs.
        
        Returns:
            List of CatalogInfo objects
        """
        try:
            catalogs = list(self.workspace.catalogs.list())
            logger.debug(f"Found {len(catalogs)} catalogs")
            return catalogs
        except Exception as e:
            logger.error(f"Failed to list catalogs: {e}")
            raise
    
    def list_schemas(self, catalog_name: str) -> List[SchemaInfo]:
        """List all schemas in a catalog.
        
        Args:
            catalog_name: Name of the catalog
            
        Returns:
            List of SchemaInfo objects
        """
        try:
            schemas = list(self.workspace.schemas.list(catalog_name=catalog_name))
            logger.debug(f"Found {len(schemas)} schemas in catalog {catalog_name}")
            return schemas
        except Exception as e:
            logger.error(f"Failed to list schemas in {catalog_name}: {e}")
            raise
    
    def list_tables(
        self, catalog_name: str, schema_name: str
    ) -> List[TableInfo]:
        """List all tables in a schema.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            
        Returns:
            List of TableInfo objects
        """
        try:
            tables = list(
                self.workspace.tables.list(
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                )
            )
            logger.debug(
                f"Found {len(tables)} tables in {catalog_name}.{schema_name}"
            )
            return tables
        except Exception as e:
            logger.error(
                f"Failed to list tables in {catalog_name}.{schema_name}: {e}"
            )
            raise
    
    def get_table(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> TableInfo:
        """Get table metadata.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            TableInfo object
        """
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        try:
            table = self.workspace.tables.get(full_name=full_name)
            logger.debug(f"Retrieved table metadata for {full_name}")
            return table
        except Exception as e:
            logger.error(f"Failed to get table {full_name}: {e}")
            raise
    
    def create_schema(
        self,
        catalog_name: str,
        schema_name: str,
        comment: Optional[str] = None,
    ) -> SchemaInfo:
        """Create a new schema.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            comment: Optional description
            
        Returns:
            SchemaInfo object
        """
        try:
            schema = self.workspace.schemas.create(
                catalog_name=catalog_name,
                name=schema_name,
                comment=comment,
            )
            logger.info(f"Created schema {catalog_name}.{schema_name}")
            return schema
        except Exception as e:
            logger.error(f"Failed to create schema {schema_name}: {e}")
            raise
    
    def create_table(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        columns: List[ColumnInfo],
        table_type: str = "MANAGED",
        data_source_format: str = "DELTA",
        storage_location: Optional[str] = None,
        properties: Optional[dict] = None,
        comment: Optional[str] = None,
    ) -> TableInfo:
        """Create a new table.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            columns: List of column definitions
            table_type: MANAGED or EXTERNAL
            data_source_format: DELTA, ICEBERG, or HUDI
            storage_location: Location for external tables
            properties: Table properties (e.g., UniForm settings)
            comment: Optional description
            
        Returns:
            TableInfo object
        """
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        try:
            table = self.workspace.tables.create(
                name=full_name,
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_type=table_type,
                data_source_format=data_source_format,
                columns=columns,
                storage_location=storage_location,
                properties=properties,
                comment=comment,
            )
            logger.info(f"Created table {full_name} with format {data_source_format}")
            return table
        except Exception as e:
            logger.error(f"Failed to create table {full_name}: {e}")
            raise
    
    def delete_table(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> None:
        """Delete a table.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
        """
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        try:
            self.workspace.tables.delete(full_name=full_name)
            logger.info(f"Deleted table {full_name}")
        except Exception as e:
            logger.error(f"Failed to delete table {full_name}: {e}")
            raise
    
    def update_table(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        columns: Optional[List[ColumnInfo]] = None,
        properties: Optional[dict] = None,
        comment: Optional[str] = None,
    ) -> TableInfo:
        """Update table metadata.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            columns: Updated column definitions
            properties: Updated table properties
            comment: Updated description
            
        Returns:
            Updated TableInfo object
        """
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        try:
            table = self.workspace.tables.update(
                full_name=full_name,
                columns=columns,
                properties=properties,
                comment=comment,
            )
            logger.info(f"Updated table {full_name}")
            return table
        except Exception as e:
            logger.error(f"Failed to update table {full_name}: {e}")
            raise