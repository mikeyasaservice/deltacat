"""Iceberg-specific adapter for DuckDB SQL access to DeltaCAT Iceberg tables."""

import logging
from typing import Optional, Dict, Any
import duckdb
import pyarrow as pa

from deltacat import logs
from deltacat.catalog import get_catalog

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

# Optional import for Iceberg support
try:
    from deltacat.experimental.catalog.iceberg import IcebergCatalog
    ICEBERG_AVAILABLE = True
except ImportError:
    ICEBERG_AVAILABLE = False
    logger.debug("Iceberg support not available - install with: pip install deltacat[iceberg]")


class IcebergSQLAdapter:
    """Optimized SQL adapter for DeltaCAT Iceberg tables using DuckDB's native Iceberg support."""
    
    def __init__(
        self, 
        catalog_name: Optional[str] = None,
        connection: Optional[duckdb.DuckDBPyConnection] = None
    ):
        """Initialize the Iceberg SQL adapter.
        
        Args:
            catalog_name: Name of the DeltaCAT catalog
            connection: Existing DuckDB connection to use
        """
        self.catalog_name = catalog_name
        # Try to get catalog, but don't fail if not available
        try:
            self.catalog = get_catalog(catalog_name)
        except (ValueError, RuntimeError):
            self.catalog = None
            logger.debug("No catalog available - adapter will work in limited mode")
        
        # Use provided connection or create new one
        self.connection = connection or duckdb.connect(":memory:")
        
        # Install and load Iceberg extension
        self._setup_iceberg_extension()
        
    def _setup_iceberg_extension(self):
        """Install and load DuckDB's Iceberg extension."""
        try:
            self.connection.execute("INSTALL iceberg")
            self.connection.execute("LOAD iceberg")
            logger.info("DuckDB Iceberg extension loaded successfully")
        except Exception as e:
            logger.warning(f"Failed to load Iceberg extension: {e}")
            logger.warning("Falling back to Arrow Dataset approach")
    
    def is_iceberg_table(self, table_def) -> bool:
        """Check if a table is an Iceberg table.
        
        Args:
            table_def: Table definition from DeltaCAT
            
        Returns:
            True if this is an Iceberg table
        """
        # Check if catalog is Iceberg type
        if self.catalog and ICEBERG_AVAILABLE and hasattr(self.catalog, 'impl') and self.catalog.impl.__name__ == 'IcebergCatalog':
            return True
            
        # Check for Iceberg-specific properties
        if hasattr(table_def, 'properties') and table_def.properties:
            if 'table_type' in table_def.properties:
                return table_def.properties['table_type'].lower() == 'iceberg'
                
        # Check for Iceberg metadata location
        if hasattr(table_def, 'metadata_location'):
            return table_def.metadata_location is not None
            
        return False
    
    def get_iceberg_location(self, table_def) -> Optional[str]:
        """Get the Iceberg table location (metadata.json path).
        
        Args:
            table_def: Table definition from DeltaCAT
            
        Returns:
            Path to Iceberg metadata.json file, or None
        """
        # Try metadata_location first (standard Iceberg)
        if hasattr(table_def, 'metadata_location'):
            return table_def.metadata_location
            
        # Try table location + metadata.json
        if hasattr(table_def.table, 'location'):
            base_location = table_def.table.location
            # Iceberg metadata is typically at location/metadata/[version]-metadata.json
            # For simplicity, we'll construct the path
            return f"{base_location}/metadata.json"
            
        # For Glue catalog, construct from database and table name
        if hasattr(table_def.table, 'database') and hasattr(table_def.table, 'name'):
            # This would need the actual S3 path from Glue
            # Placeholder for now
            return None
            
        return None
    
    def register_iceberg_table(
        self,
        table_name: str,
        namespace: Optional[str] = None,
        alias: Optional[str] = None
    ) -> bool:
        """Register an Iceberg table with DuckDB using native iceberg_scan.
        
        Args:
            table_name: Name of the table
            namespace: Namespace containing the table
            alias: Optional SQL alias for the table
            
        Returns:
            True if registration succeeded
        """
        try:
            # Get table definition from catalog
            from deltacat.catalog import get_table
            table_def = get_table(
                name=table_name,
                namespace=namespace,
                catalog=self.catalog_name
            )
            
            if not table_def:
                logger.warning(f"Table {namespace}.{table_name} not found")
                return False
            
            # Check if it's an Iceberg table
            if not self.is_iceberg_table(table_def):
                logger.debug(f"Table {namespace}.{table_name} is not an Iceberg table")
                return False
            
            # Get Iceberg location
            iceberg_location = self.get_iceberg_location(table_def)
            if not iceberg_location:
                logger.warning(f"Could not determine Iceberg location for {namespace}.{table_name}")
                return False
            
            # Create SQL view name
            sql_name = alias or (f"{namespace}_{table_name}" if namespace else table_name)
            
            # Register as a view using iceberg_scan
            self.connection.execute(f"""
                CREATE OR REPLACE VIEW {sql_name} AS 
                SELECT * FROM iceberg_scan('{iceberg_location}')
            """)
            
            logger.info(f"Registered Iceberg table {sql_name} via native iceberg_scan")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register Iceberg table {namespace}.{table_name}: {e}")
            return False
    
    def register_all_iceberg_tables(self) -> int:
        """Register all Iceberg tables from the catalog.
        
        Returns:
            Number of tables successfully registered
        """
        registered_count = 0
        
        try:
            # List all namespaces
            from deltacat.catalog import list_namespaces, list_tables
            namespaces = list_namespaces(catalog=self.catalog_name)
            
            for namespace in namespaces.all_items():
                # List tables in namespace
                tables = list_tables(
                    namespace=namespace.name,
                    catalog=self.catalog_name
                )
                
                for table_def in tables.all_items():
                    if self.register_iceberg_table(
                        table_def.table.name,
                        namespace.name
                    ):
                        registered_count += 1
                        
        except Exception as e:
            logger.error(f"Error registering Iceberg tables: {e}")
            
        logger.info(f"Registered {registered_count} Iceberg tables with native support")
        return registered_count
    
    def sql(self, query: str) -> pa.Table:
        """Execute SQL query on Iceberg tables.
        
        Args:
            query: SQL query string
            
        Returns:
            Query results as Arrow Table
        """
        return self.connection.execute(query).arrow()
    
    def time_travel_query(
        self,
        table_name: str,
        snapshot_id: Optional[int] = None,
        timestamp: Optional[str] = None
    ) -> pa.Table:
        """Query an Iceberg table at a specific snapshot or timestamp.
        
        Args:
            table_name: Name of the Iceberg table
            snapshot_id: Specific snapshot ID to query
            timestamp: Timestamp to query (e.g., '2024-01-01 00:00:00')
            
        Returns:
            Query results as Arrow Table
        """
        if snapshot_id:
            query = f"""
                SELECT * FROM iceberg_scan(
                    '{table_name}',
                    snapshot_id => {snapshot_id}
                )
            """
        elif timestamp:
            query = f"""
                SELECT * FROM iceberg_scan(
                    '{table_name}',
                    timestamp => '{timestamp}'::TIMESTAMP
                )
            """
        else:
            query = f"SELECT * FROM {table_name}"
            
        return self.connection.execute(query).arrow()
    
    def get_table_snapshots(self, table_name: str) -> pa.Table:
        """Get all snapshots for an Iceberg table.
        
        Args:
            table_name: Name of the Iceberg table
            
        Returns:
            Table of snapshot metadata
        """
        query = f"""
            SELECT * FROM iceberg_snapshots('{table_name}')
        """
        return self.connection.execute(query).arrow()
    
    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """Get metadata for an Iceberg table.
        
        Args:
            table_name: Name of the Iceberg table
            
        Returns:
            Dictionary of table metadata
        """
        query = f"""
            SELECT * FROM iceberg_metadata('{table_name}')
        """
        result = self.connection.execute(query).fetchall()
        return dict(result) if result else {}