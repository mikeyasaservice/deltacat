"""DeltaCAT SQL Gateway - Query DeltaCAT tables using SQL via DuckDB."""

import logging
import re
from typing import Any, Dict, List, Optional, Union
import duckdb
import pyarrow as pa
import pyarrow.dataset as ds

from deltacat import logs
from deltacat.catalog import (
    create_table as catalog_create_table,
    drop_table as catalog_drop_table,
    alter_table as catalog_alter_table,
    table_exists,
    is_initialized,
    raise_if_not_initialized,
)
from deltacat.sql.catalog_adapter import CatalogAdapter
from deltacat.sql.iceberg_adapter import IcebergSQLAdapter
from deltacat.storage import Schema
from deltacat.types.media import ContentType

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class DeltaCATSQLGateway:
    """SQL interface for DeltaCAT using DuckDB's zero-copy Arrow integration."""
    
    def __init__(
        self,
        catalog_name: Optional[str] = None,
        connection_config: Optional[Dict[str, Any]] = None,
        auto_register_tables: bool = True,
        enable_iceberg_optimization: bool = True,
    ):
        """Initialize the SQL gateway.
        
        Args:
            catalog_name: Name of the DeltaCAT catalog to use
            connection_config: Optional DuckDB connection configuration
            auto_register_tables: Whether to automatically register all catalog tables
            enable_iceberg_optimization: Whether to use native Iceberg support for Iceberg tables
        """
        raise_if_not_initialized()
        
        self.catalog_name = catalog_name
        self.catalog_adapter = CatalogAdapter(catalog_name)
        
        # Initialize DuckDB connection
        config = connection_config or {}
        self.connection = duckdb.connect(":memory:", config=config)
        
        # Initialize Iceberg adapter if optimization is enabled
        self.iceberg_adapter = None
        if enable_iceberg_optimization:
            try:
                self.iceberg_adapter = IcebergSQLAdapter(catalog_name, self.connection)
                logger.info("Iceberg optimization enabled")
            except Exception as e:
                logger.warning(f"Could not enable Iceberg optimization: {e}")
        
        # Register all tables if requested
        if auto_register_tables:
            self._register_all_tables()
            
        logger.info(f"DeltaCAT SQL Gateway initialized with catalog: {catalog_name or 'default'}")
    
    def _register_all_tables(self):
        """Register all DeltaCAT tables with DuckDB."""
        registered_count = 0
        
        # First try to register Iceberg tables with native support
        if self.iceberg_adapter:
            iceberg_count = self.iceberg_adapter.register_all_iceberg_tables()
            registered_count += iceberg_count
            logger.info(f"Registered {iceberg_count} Iceberg tables with native support")
        
        # Then register remaining tables via Arrow Datasets
        tables = self.catalog_adapter.list_all_tables()
        
        for namespace, table_name in tables:
            full_name = f"{namespace}.{table_name}" if namespace else table_name
            # Skip if already registered as Iceberg (need to check actual table)
            # TODO: Implement proper deduplication check
            if self.iceberg_adapter:
                # For now, try to register all non-Iceberg tables
                pass
            if self.register_table(table_name, namespace, alias=full_name):
                registered_count += 1
                
        logger.info(f"Registered {registered_count} total tables with DuckDB")
    
    def register_table(
        self,
        table_name: str,
        namespace: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> bool:
        """Register a DeltaCAT table with DuckDB.
        
        Args:
            table_name: Name of the table to register
            namespace: Namespace containing the table
            alias: Optional alias for the table in SQL queries
            
        Returns:
            True if registration succeeded, False otherwise.
        """
        dataset = self.catalog_adapter.get_table_as_arrow_dataset(
            table_name, namespace
        )
        
        if dataset is None:
            logger.warning(f"Could not find table {namespace}.{table_name}")
            return False
        
        # Use alias or construct from namespace and table name
        sql_name = alias or (f"{namespace}_{table_name}" if namespace else table_name)
        
        try:
            # Register the Arrow Dataset with DuckDB
            self.connection.register(sql_name, dataset)
            logger.debug(f"Registered table {sql_name} with DuckDB")
            return True
        except Exception as e:
            logger.error(f"Failed to register table {sql_name}: {e}")
            return False
    
    def sql(
        self,
        query: str,
        parameters: Optional[Union[List, Dict]] = None
    ) -> pa.Table:
        """Execute a SQL query and return results as an Arrow Table.
        
        Args:
            query: SQL query string
            parameters: Optional query parameters for prepared statements
            
        Returns:
            Query results as an Arrow Table.
        """
        try:
            # Parse query to find table references
            self._ensure_tables_registered(query)
            
            # Execute query
            if parameters:
                result = self.connection.execute(query, parameters)
            else:
                result = self.connection.execute(query)
            
            # Return as Arrow Table (zero-copy)
            return result.arrow()
            
        except Exception as e:
            logger.error(f"SQL query failed: {e}")
            raise
    
    def execute(
        self,
        query: str,
        parameters: Optional[Union[List, Dict]] = None
    ) -> duckdb.DuckDBPyRelation:
        """Execute a SQL query and return DuckDB relation for further processing.
        
        Args:
            query: SQL query string
            parameters: Optional query parameters
            
        Returns:
            DuckDB relation object.
        """
        try:
            self._ensure_tables_registered(query)
            
            if parameters:
                return self.connection.execute(query, parameters)
            else:
                return self.connection.execute(query)
                
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            raise
    
    def _ensure_tables_registered(self, query: str):
        """Parse SQL query and ensure referenced tables are registered.
        
        Args:
            query: SQL query string
        """
        # Simple regex to find table names in FROM and JOIN clauses
        # This is a simplified implementation - a real parser would be more robust
        table_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        matches = re.findall(table_pattern, query, re.IGNORECASE)
        
        for match in matches:
            if '.' in match:
                namespace, table = match.split('.', 1)
                self.register_table(table, namespace, alias=match)
            else:
                # Try to register without namespace
                self.register_table(match)
    
    def create_table(
        self,
        table_name: str,
        schema: Union[pa.Schema, Schema],
        namespace: Optional[str] = None,
        partition_columns: Optional[List[str]] = None,
        primary_keys: Optional[List[str]] = None,
        **kwargs
    ) -> bool:
        """Create a new table via SQL DDL mapped to DeltaCAT catalog.
        
        Args:
            table_name: Name of the table to create
            schema: Arrow or DeltaCAT schema
            namespace: Namespace for the table
            partition_columns: Optional partition column names
            primary_keys: Optional primary key columns
            **kwargs: Additional arguments for catalog_create_table
            
        Returns:
            True if table was created successfully.
        """
        try:
            # Convert Arrow schema to DeltaCAT schema if needed
            if isinstance(schema, pa.Schema):
                # TODO: Implement Arrow to DeltaCAT schema conversion
                deltacat_schema = self._arrow_to_deltacat_schema(schema)
            else:
                deltacat_schema = schema
            
            # Create table in catalog
            table_def = catalog_create_table(
                name=table_name,
                namespace=namespace,
                catalog=self.catalog_name,
                schema=deltacat_schema,
                content_types=[ContentType.PARQUET],
                **kwargs
            )
            
            logger.info(f"Created table {namespace}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table {namespace}.{table_name}: {e}")
            return False
    
    def drop_table(
        self,
        table_name: str,
        namespace: Optional[str] = None,
        if_exists: bool = True
    ) -> bool:
        """Drop a table via SQL DDL mapped to DeltaCAT catalog.
        
        Args:
            table_name: Name of the table to drop
            namespace: Namespace containing the table
            if_exists: If True, don't error if table doesn't exist
            
        Returns:
            True if table was dropped successfully.
        """
        try:
            # Check if table exists
            if not table_exists(table_name, namespace=namespace, catalog=self.catalog_name):
                if if_exists:
                    return True
                else:
                    raise ValueError(f"Table {namespace}.{table_name} does not exist")
            
            # Drop from catalog
            catalog_drop_table(
                name=table_name,
                namespace=namespace,
                catalog=self.catalog_name
            )
            
            # Remove from DuckDB if registered
            sql_name = f"{namespace}_{table_name}" if namespace else table_name
            try:
                self.connection.unregister(sql_name)
            except:
                pass  # Table might not be registered
            
            # Clear from cache
            self.catalog_adapter.clear_cache()
            
            logger.info(f"Dropped table {namespace}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop table {namespace}.{table_name}: {e}")
            return False
    
    def _arrow_to_deltacat_schema(self, arrow_schema: pa.Schema) -> Schema:
        """Convert Arrow schema to DeltaCAT schema.
        
        Args:
            arrow_schema: PyArrow schema
            
        Returns:
            DeltaCAT Schema object.
        """
        from deltacat.storage import Field
        
        fields = []
        for i, field in enumerate(arrow_schema):
            # Convert Arrow field to DeltaCAT field using the actual Field.of API
            deltacat_field = Field.of(
                field=field,  # Pass the PyArrow field directly
                field_id=i,    # Use field_id parameter
            )
            fields.append(deltacat_field)
        
        return Schema.of(fields)
    
    def refresh_tables(self):
        """Refresh the table cache and re-register all tables."""
        self.catalog_adapter.clear_cache()
        
        # Clear all registered tables in DuckDB
        # Note: DuckDB doesn't have a direct API for this, so we reconnect
        config = self.connection.execute("SELECT * FROM duckdb_settings()").arrow().to_pydict()
        self.connection.close()
        self.connection = duckdb.connect(":memory:")
        
        # Re-register all tables
        self._register_all_tables()
    
    def explain(self, query: str) -> str:
        """Get the query execution plan.
        
        Args:
            query: SQL query string
            
        Returns:
            Query execution plan as a string.
        """
        self._ensure_tables_registered(query)
        result = self.connection.execute(f"EXPLAIN {query}")
        return result.fetchall()[0][1] if result else ""
    
    def close(self):
        """Close the DuckDB connection and clear caches."""
        self.connection.close()
        self.catalog_adapter.clear_cache()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def __repr__(self) -> str:
        """String representation."""
        return f"DeltaCATSQLGateway(catalog={self.catalog_name or 'default'})"