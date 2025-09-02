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
        use_connection_pool: bool = True,
    ):
        """Initialize the SQL gateway.
        
        Args:
            catalog_name: Name of the DeltaCAT catalog to use
            connection_config: Optional DuckDB connection configuration
            auto_register_tables: Whether to automatically register all catalog tables
            enable_iceberg_optimization: Whether to use native Iceberg support for Iceberg tables
            use_connection_pool: Whether to use connection pooling
        """
        raise_if_not_initialized()
        
        self.catalog_name = catalog_name
        self.catalog_adapter = CatalogAdapter(catalog_name)
        
        # Initialize DuckDB connection with pooling if enabled
        from deltacat.config.performance import get_performance_config
        from deltacat.catalog.connection_pool import get_pool_manager
        
        perf_config = get_performance_config()
        config = connection_config or {}
        
        self._use_pool = use_connection_pool and perf_config.connection_pool.enabled
        
        # When using connection pooling, we need a shared database
        # Otherwise each connection would have its own in-memory database
        if self._use_pool:
            # Use connection pool with a shared temporary database
            import tempfile
            import atexit
            import os
            
            # Create a temporary file with automatic cleanup
            temp_fd, db_path = tempfile.mkstemp(suffix='.duckdb', prefix=f'deltacat_{os.getpid()}_')
            os.close(temp_fd)  # Close the file descriptor, we just need the path
            
            # Remove file if it exists (from previous failed run)
            if os.path.exists(db_path):
                os.unlink(db_path)
            
            # Initialize the database with the first connection
            init_conn = duckdb.connect(db_path, config=config)
            init_conn.close()
            
            self._db_file = db_path  # Store path for cleanup
            
            # Register cleanup handler for this specific instance
            def cleanup_temp_db():
                self._cleanup_temp_database()
            atexit.register(cleanup_temp_db)
            self._pool_manager = get_pool_manager()
            
            # Pass the database path in the config so all connections share the same database
            pool_config = {**config, 'database': db_path}
            self._duckdb_pool = self._pool_manager.get_duckdb_pool(**pool_config)
            
            # Also create a persistent connection for registrations
            self.connection = duckdb.connect(db_path, config=config)
        else:
            # Direct connection with in-memory database
            self.connection = duckdb.connect(":memory:", config=config)
            self._db_file = None
        
        # Initialize Iceberg adapter if optimization is enabled
        self.iceberg_adapter = None
        if enable_iceberg_optimization:
            try:
                self.iceberg_adapter = IcebergSQLAdapter(catalog_name, self.connection)
                logger.info("Iceberg optimization enabled")
            except ImportError as e:
                logger.warning(f"Iceberg dependencies not available: {e}")
            except (ValueError, RuntimeError) as e:
                logger.warning(f"Could not initialize Iceberg optimization: {e}")
            except Exception as e:
                logger.warning(f"Unexpected error enabling Iceberg optimization: {type(e).__name__}: {e}")
        
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
            if self._use_pool:
                # When using pooling with a shared database, we need to create a persistent table
                # so it's visible to all connections in the pool
                
                # First register the dataset temporarily
                temp_name = f"_temp_{sql_name}"
                self.connection.register(temp_name, dataset)
                
                # Create a persistent table from the dataset
                # Use CREATE OR REPLACE to handle re-registrations
                self.connection.execute(f"CREATE OR REPLACE TABLE {sql_name} AS SELECT * FROM {temp_name}")
                
                # Unregister the temporary table
                self.connection.unregister(temp_name)
                
                logger.debug(f"Created persistent table {sql_name} in shared database")
            else:
                # For non-pooled connections, just register the dataset directly
                self.connection.register(sql_name, dataset)
                logger.debug(f"Registered table {sql_name} with DuckDB")
            
            # Store registration info for re-registration if needed
            if not hasattr(self, '_registered_tables'):
                self._registered_tables = {}
            self._registered_tables[sql_name] = (table_name, namespace, dataset)
            
            return True
        except duckdb.CatalogException as e:
            logger.error(f"DuckDB catalog error registering table {sql_name}: {e}")
            return False
        except ValueError as e:
            logger.error(f"Invalid table configuration for {sql_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error registering table {sql_name}: {type(e).__name__}: {e}")
            return False
    
    def _get_connection(self):
        """Get a DuckDB connection from pool or direct connection."""
        if self._use_pool:
            return self._duckdb_pool.get_connection()
        else:
            return self.connection
    
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
            
            # Execute query with pooled or direct connection
            if self._use_pool:
                # Use a connection from the pool - tables are visible because we use a shared database file
                with self._duckdb_pool.get_connection() as conn:
                    if parameters:
                        result = conn.execute(query, parameters)
                    else:
                        result = conn.execute(query)
                    # Return as Arrow Table (zero-copy)
                    return result.arrow()
            else:
                # Use direct connection
                if parameters:
                    result = self.connection.execute(query, parameters)
                else:
                    result = self.connection.execute(query)
                return result.arrow()
            
        except duckdb.ParserException as e:
            logger.error(f"SQL syntax error: {e}")
            raise
        except duckdb.CatalogException as e:
            logger.error(f"Table or column not found: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected SQL query error: {type(e).__name__}: {e}")
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
            
            if self._use_pool:
                # Use pooled connection - tables are visible via shared database
                with self._duckdb_pool.get_connection() as conn:
                    if parameters:
                        return conn.execute(query, parameters)
                    else:
                        return conn.execute(query)
            else:
                # Use direct connection
                if parameters:
                    return self.connection.execute(query, parameters)
                else:
                    return self.connection.execute(query)
                
        except duckdb.ParserException as e:
            logger.error(f"SQL syntax error in execution: {e}")
            raise
        except duckdb.CatalogException as e:
            logger.error(f"Catalog error during execution: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected SQL execution error: {type(e).__name__}: {e}")
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
            
        except duckdb.CatalogException as e:
            logger.error(f"DuckDB catalog error creating table {namespace}.{table_name}: {e}")
            return False
        except pa.ArrowException as e:
            logger.error(f"Arrow error creating table {namespace}.{table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error creating table {namespace}.{table_name}: {type(e).__name__}: {e}")
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
            self._cleanup_table(sql_name)
            
            # Clear from cache
            self.catalog_adapter.clear_cache()
            
            logger.info(f"Dropped table {namespace}.{table_name}")
            return True
            
        except duckdb.CatalogException as e:
            logger.error(f"Table {namespace}.{table_name} not found: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error dropping table {namespace}.{table_name}: {type(e).__name__}: {e}")
            return False
    
    def _cleanup_table(self, sql_name: str) -> None:
        """Clean up a table from DuckDB registry.
        
        Args:
            sql_name: The SQL table name to unregister.
        """
        try:
            import duckdb
            self.connection.unregister(sql_name)
        except duckdb.CatalogException:
            pass  # Table might not be registered
    
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
    
    def _cleanup_temp_database(self):
        """Clean up temporary database files."""
        if self._db_file:
            import os
            try:
                db_path = self._db_file
                if os.path.exists(db_path):
                    os.unlink(db_path)
                    # Also remove WAL and SHM files if they exist
                    for suffix in ['.wal', '.shm']:
                        wal_path = db_path + suffix
                        if os.path.exists(wal_path):
                            os.unlink(wal_path)
                    logger.debug(f"Cleaned up temporary database file: {db_path}")
            except (OSError, IOError) as e:
                logger.warning(f"Failed to clean up temporary database file: {e}")
            except Exception as e:
                logger.warning(f"Unexpected error during cleanup: {type(e).__name__}: {e}")
            finally:
                self._db_file = None
    
    def close(self):
        """Close the DuckDB connection and clear caches."""
        if self.connection:
            self.connection.close()
        
        if self._use_pool:
            self._cleanup_temp_database()
        
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