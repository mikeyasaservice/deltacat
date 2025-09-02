"""Adapter to expose DeltaCAT catalog tables to DuckDB via Arrow Datasets."""

import logging
from typing import Dict, List, Optional, Tuple
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pathlib import Path

from deltacat import logs
from deltacat.catalog import (
    get_catalog,
    list_namespaces,
    list_tables,
    get_table,
)
from deltacat.storage import Manifest, ManifestEntry
from deltacat.utils.filesystem import resolve_path_and_filesystem

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class CatalogAdapter:
    """Adapts DeltaCAT catalog tables to Arrow Datasets for DuckDB consumption."""
    
    def __init__(self, catalog_name: Optional[str] = None):
        """Initialize the catalog adapter.
        
        Args:
            catalog_name: Name of the DeltaCAT catalog to use. If None, uses default.
        """
        self.catalog_name = catalog_name
        self._table_cache: Dict[str, ds.Dataset] = {}
        
        # Initialize metadata cache and lazy loading
        from deltacat.catalog.cache.metadata_cache import get_metadata_cache
        from deltacat.config.performance import get_performance_config
        
        self._metadata_cache = get_metadata_cache()
        self._perf_config = get_performance_config()
        
    def list_all_tables(self) -> List[Tuple[str, str]]:
        """List all tables in the catalog.
        
        Returns:
            List of (namespace, table_name) tuples.
        """
        # Check cache first
        cache_key = f"catalog:{self.catalog_name}:tables"
        cached_tables = self._metadata_cache.get(cache_key, "table_list")
        if cached_tables is not None:
            return cached_tables
        
        tables = []
        
        # List all namespaces
        namespaces_result = list_namespaces(catalog=self.catalog_name)
        
        for namespace in namespaces_result.all_items():
            # List tables in each namespace
            tables_result = list_tables(
                namespace=namespace.name,
                catalog=self.catalog_name
            )
            
            for table_def in tables_result.all_items():
                tables.append((namespace.name, table_def.table.name))
        
        # Cache the result
        self._metadata_cache.set(cache_key, tables, "table_list")
        
        return tables
    
    def get_table_as_arrow_dataset(
        self,
        table_name: str,
        namespace: Optional[str] = None,
        version: Optional[str] = None,
    ) -> Optional[ds.Dataset]:
        """Get a DeltaCAT table as an Arrow Dataset.
        
        Args:
            table_name: Name of the table
            namespace: Namespace containing the table
            version: Specific table version to read
            
        Returns:
            Arrow Dataset pointing to the table's Parquet files, or None if not found.
        """
        cache_key = f"{namespace or 'default'}.{table_name}"
        
        # Check local cache first
        if cache_key in self._table_cache and version is None:
            return self._table_cache[cache_key]
        
        # Check metadata cache
        metadata_cache_key = f"dataset:{cache_key}:{version or 'latest'}"
        cached_dataset = self._metadata_cache.get(metadata_cache_key, "dataset")
        if cached_dataset is not None:
            self._table_cache[cache_key] = cached_dataset
            return cached_dataset
        
        try:
            # Use lazy loading if enabled
            from deltacat.catalog.lazy_schema import create_lazy_table_definition
            
            # Get table definition from catalog (with lazy loading)
            def load_table_def():
                return get_table(
                    name=table_name,
                    namespace=namespace,
                    catalog=self.catalog_name,
                    table_version=version,
                )
            
            if self._perf_config.lazy_loading.enabled:
                table_def = create_lazy_table_definition(
                    load_table_def,
                    cache_key=f"table_def:{cache_key}",
                    config=self._perf_config.lazy_loading,
                )
            else:
                table_def = load_table_def()
            
            if not table_def:
                logger.warning(f"Table {cache_key} not found in catalog")
                return None
                
            # Extract file paths from manifest entries
            file_paths = self._extract_parquet_files(table_def)
            
            if not file_paths:
                logger.warning(f"No Parquet files found for table {cache_key}")
                return None
            
            # Create Arrow Dataset from Parquet files with optimizations
            from deltacat.sql.arrow_adapter import coalesce_small_fragments
            
            arrow_dataset = self._create_arrow_dataset(
                file_paths,
                table_def.table.partition_keys if hasattr(table_def.table, 'partition_keys') else None
            )
            
            # Apply fragment coalescing optimization
            if self._perf_config.arrow_optimization.enabled:
                arrow_dataset = coalesce_small_fragments(
                    arrow_dataset,
                    self._perf_config.arrow_optimization
                )
            
            # Cache the dataset
            if version is None:
                self._table_cache[cache_key] = arrow_dataset
                self._metadata_cache.set(metadata_cache_key, arrow_dataset, "dataset")
                
            return arrow_dataset
            
        except Exception as e:
            logger.error(f"Failed to create Arrow Dataset for {cache_key}: {e}")
            return None
    
    def _extract_parquet_files(self, table_def) -> List[str]:
        """Extract Parquet file paths from a table definition.
        
        Args:
            table_def: DeltaCAT table definition
            
        Returns:
            List of Parquet file paths.
        """
        file_paths = []
        
        # Get the latest manifest for the table
        # This is a simplified implementation - real one would need to handle
        # multiple manifests, delta files, etc.
        if hasattr(table_def, 'manifest') and table_def.manifest:
            manifest = table_def.manifest
            if hasattr(manifest, 'entries'):
                for entry in manifest.entries:
                    if entry.url and entry.url.endswith('.parquet'):
                        file_paths.append(entry.url)
        
        # Alternative: Look for table location/path attribute
        if not file_paths and hasattr(table_def.table, 'location'):
            location = table_def.table.location
            # List all Parquet files in the location
            path, filesystem = resolve_path_and_filesystem(location)
            try:
                from pyarrow.fs import FileSelector
                file_info_list = filesystem.get_file_info(FileSelector(path, recursive=True))
                for file_info in file_info_list:
                    if file_info.path.endswith('.parquet'):
                        file_paths.append(f"{location}/{file_info.path}")
            except Exception as e:
                logger.warning(f"Could not list files in {location}: {e}")
                
        return file_paths
    
    def _create_arrow_dataset(
        self,
        file_paths: List[str],
        partition_keys: Optional[List[str]] = None
    ) -> ds.Dataset:
        """Create an Arrow Dataset from Parquet file paths.
        
        Args:
            file_paths: List of Parquet file paths
            partition_keys: Optional partition column names
            
        Returns:
            Arrow Dataset.
        """
        if len(file_paths) == 1:
            # Single file - simple case
            return ds.dataset(file_paths[0], format="parquet")
        else:
            # Multiple files - may be partitioned
            # Extract common base path
            base_path = self._find_common_base_path(file_paths)
            
            if partition_keys:
                # Create partitioned dataset
                partitioning = ds.partitioning(
                    pa.schema([(key, pa.string()) for key in partition_keys])
                )
                return ds.dataset(
                    base_path,
                    format="parquet",
                    partitioning=partitioning
                )
            else:
                # Create dataset from file list
                return ds.dataset(file_paths, format="parquet")
    
    def _find_common_base_path(self, file_paths: List[str]) -> str:
        """Find the common base path for a list of files.
        
        Args:
            file_paths: List of file paths
            
        Returns:
            Common base path.
        """
        if not file_paths:
            return ""
            
        # Use Path for robust path handling
        paths = [Path(p) for p in file_paths]
        
        # Find common parent
        common_parent = paths[0].parent
        for path in paths[1:]:
            while not path.is_relative_to(common_parent):
                common_parent = common_parent.parent
                if common_parent == Path("/"):
                    break
                    
        return str(common_parent)
    
    def clear_cache(self):
        """Clear the cached Arrow Datasets."""
        self._table_cache.clear()
        # Also clear relevant metadata cache entries
        if self._metadata_cache:
            # Clear dataset and table list caches for this catalog
            self._metadata_cache.delete(f"catalog:{self.catalog_name}:tables")
    
    def get_table_schema(
        self,
        table_name: str,
        namespace: Optional[str] = None
    ) -> Optional[pa.Schema]:
        """Get the Arrow schema for a table.
        
        Args:
            table_name: Name of the table
            namespace: Namespace containing the table
            
        Returns:
            Arrow Schema or None if table not found.
        """
        dataset = self.get_table_as_arrow_dataset(table_name, namespace)
        if dataset:
            return dataset.schema
        return None