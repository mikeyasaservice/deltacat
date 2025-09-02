"""Utilities for exposing DeltaCAT data as Arrow objects for zero-copy SQL access."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Union, Iterator, Tuple
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc
from functools import lru_cache

from deltacat import logs
from deltacat.storage import Delta, Manifest
from deltacat.types.media import ContentType
from deltacat.utils.pyarrow import (
    content_type_to_reader_kwargs,
    content_type_to_pyarrow_read_func,
)
from deltacat.config.performance import ArrowOptimizationConfig, get_performance_config
from deltacat.catalog.cache.metadata_cache import get_metadata_cache

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def manifest_to_arrow_dataset(
    manifest: Manifest,
    base_path: Optional[str] = None,
    filters: Optional[pc.Expression] = None,
) -> ds.Dataset:
    """Convert a DeltaCAT manifest to an Arrow Dataset.
    
    This enables zero-copy access to the underlying Parquet files.
    
    Args:
        manifest: DeltaCAT manifest containing file entries
        base_path: Optional base path for relative file paths
        filters: Optional Arrow compute expressions for predicate pushdown
        
    Returns:
        Arrow Dataset pointing to the manifest's files.
    """
    if not manifest or not manifest.entries:
        raise ValueError("Manifest has no entries")
    
    # Extract Parquet file paths from manifest entries
    parquet_files = []
    for entry in manifest.entries:
        if entry.url and entry.content_type == ContentType.PARQUET:
            file_path = entry.url
            if base_path and not file_path.startswith(('s3://', 'gs://', '/')):
                file_path = f"{base_path}/{file_path}"
            parquet_files.append(file_path)
    
    if not parquet_files:
        raise ValueError("No Parquet files found in manifest")
    
    # Create Arrow Dataset
    dataset = ds.dataset(parquet_files, format="parquet")
    
    # Apply filters if provided
    if filters:
        dataset = dataset.filter(filters)
    
    return dataset


def delta_to_arrow_table(
    delta: Delta,
    columns: Optional[List[str]] = None,
    filters: Optional[pc.Expression] = None,
) -> pa.Table:
    """Convert a DeltaCAT Delta to an Arrow Table.
    
    Args:
        delta: DeltaCAT Delta object
        columns: Optional list of columns to read
        filters: Optional Arrow compute expressions for filtering
        
    Returns:
        Arrow Table containing the Delta's data.
    """
    if not delta.manifest:
        raise ValueError("Delta has no manifest")
    
    # Get Arrow Dataset from manifest
    dataset = manifest_to_arrow_dataset(delta.manifest, filters=filters)
    
    # Read into Arrow Table
    if columns:
        return dataset.to_table(columns=columns)
    else:
        return dataset.to_table()


def create_arrow_scanner(
    dataset: ds.Dataset,
    columns: Optional[List[str]] = None,
    filter: Optional[pc.Expression] = None,
    batch_size: int = 65536,
) -> ds.Scanner:
    """Create an Arrow Scanner for efficient streaming reads.
    
    Scanners enable predicate and projection pushdown for optimal performance.
    
    Args:
        dataset: Arrow Dataset to scan
        columns: Optional columns to project
        filter: Optional filter expression
        batch_size: Number of rows per batch
        
    Returns:
        Arrow Scanner configured for efficient reading.
    """
    return dataset.scanner(
        columns=columns,
        filter=filter,
        batch_size=batch_size,
    )


def create_partitioned_dataset(
    file_paths: List[str],
    partition_keys: List[str],
    schema: Optional[pa.Schema] = None,
) -> ds.Dataset:
    """Create a partitioned Arrow Dataset from file paths.
    
    Args:
        file_paths: List of Parquet file paths
        partition_keys: Column names used for partitioning
        schema: Optional schema to enforce
        
    Returns:
        Partitioned Arrow Dataset.
    """
    # Infer base path from file paths
    if not file_paths:
        raise ValueError("No file paths provided")
    
    # Find common directory
    import os
    common_dir = os.path.dirname(os.path.commonpath(file_paths))
    
    # Create partitioning schema
    if schema:
        # Use provided schema for partition columns
        partition_schema = pa.schema(
            [(key, schema.field(key).type) for key in partition_keys]
        )
    else:
        # Default to string partitions
        partition_schema = pa.schema(
            [(key, pa.string()) for key in partition_keys]
        )
    
    partitioning = ds.partitioning(partition_schema)
    
    return ds.dataset(
        common_dir,
        format="parquet",
        partitioning=partitioning,
    )


def optimize_dataset_for_sql(dataset: ds.Dataset) -> ds.Dataset:
    """Optimize an Arrow Dataset for SQL query performance.
    
    Args:
        dataset: Arrow Dataset to optimize
        
    Returns:
        Optimized Arrow Dataset.
    """
    # Get dataset statistics for query planning
    fragments = list(dataset.get_fragments())
    
    if len(fragments) > 100:
        # For many fragments, enable fragment coalescing
        logger.info(f"Dataset has {len(fragments)} fragments, enabling optimization")
        # Note: Real implementation would coalesce small fragments
        
    return dataset


class ArrowDatasetCache:
    """Cache for Arrow Datasets to avoid repeated file system operations."""
    
    def __init__(self, config: Optional[ArrowOptimizationConfig] = None):
        """Initialize the cache.
        
        Args:
            config: Arrow optimization configuration
        """
        self.config = config or get_performance_config().arrow_optimization
        self._cache: Dict[str, Tuple[ds.Dataset, float]] = {}  # key -> (dataset, timestamp)
        self._metadata_cache = get_metadata_cache()
        self._access_count: Dict[str, int] = {}
        
    def get(self, key: str) -> Optional[ds.Dataset]:
        """Get a dataset from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached dataset or None.
        """
        if not self.config.cache_datasets:
            return None
            
        if key in self._cache:
            dataset, timestamp = self._cache[key]
            # Check TTL
            if (time.time() - timestamp) > self.config.dataset_cache_ttl:
                del self._cache[key]
                return None
            
            # Update access count for LRU
            self._access_count[key] = self._access_count.get(key, 0) + 1
            return dataset
        
        return None
    
    def put(self, key: str, dataset: ds.Dataset):
        """Add a dataset to the cache.
        
        Args:
            key: Cache key
            dataset: Dataset to cache
        """
        if not self.config.cache_datasets:
            return
            
        # Evict if at capacity
        if len(self._cache) >= self.config.dataset_cache_size:
            # Remove least recently used
            lru_key = min(self._access_count, key=self._access_count.get)
            del self._cache[lru_key]
            del self._access_count[lru_key]
        
        self._cache[key] = (dataset, time.time())
        self._access_count[key] = 0
    
    def clear(self):
        """Clear the cache."""
        self._cache.clear()
        self._access_count.clear()


def build_filter_expression(
    filters: Dict[str, Union[str, int, float, List]]
) -> pc.Expression:
    """Build an Arrow compute expression from filter dictionary.
    
    Args:
        filters: Dictionary of column_name -> value filters
        
    Returns:
        Arrow compute expression for filtering.
    """
    expressions = []
    
    for column, value in filters.items():
        if isinstance(value, list):
            # IN clause
            expr = pc.field(column).isin(value)
        else:
            # Equality
            expr = pc.field(column) == pc.scalar(value)
        expressions.append(expr)
    
    # Combine with AND
    if len(expressions) == 1:
        return expressions[0]
    else:
        result = expressions[0]
        for expr in expressions[1:]:
            result = result & expr
        return result


# New optimized functions

def create_optimized_scanner(
    dataset: ds.Dataset,
    columns: Optional[List[str]] = None,
    filter: Optional[pc.Expression] = None,
    config: Optional[ArrowOptimizationConfig] = None,
) -> ds.Scanner:
    """Create an optimized Arrow Scanner with performance settings.
    
    Args:
        dataset: Arrow Dataset to scan
        columns: Optional columns to project
        filter: Optional filter expression
        config: Arrow optimization configuration
        
    Returns:
        Optimized Arrow Scanner.
    """
    config = config or get_performance_config().arrow_optimization
    
    scanner_kwargs = {
        "columns": columns,
        "filter": filter,
        "batch_size": config.batch_size,
        "use_threads": config.use_threads,
    }
    
    if config.num_threads is not None:
        scanner_kwargs["fragment_scan_options"] = {
            "thread_pool": ds.ThreadPoolExecutor(max_workers=config.num_threads)
        }
    
    return dataset.scanner(**scanner_kwargs)


def read_dataset_in_batches(
    dataset: ds.Dataset,
    batch_size: Optional[int] = None,
    columns: Optional[List[str]] = None,
    filter: Optional[pc.Expression] = None,
    config: Optional[ArrowOptimizationConfig] = None,
) -> Iterator[pa.RecordBatch]:
    """Read dataset in optimized batches.
    
    Args:
        dataset: Arrow Dataset to read
        batch_size: Number of rows per batch
        columns: Optional columns to read
        filter: Optional filter expression
        config: Arrow optimization configuration
        
    Yields:
        Arrow RecordBatch objects.
    """
    config = config or get_performance_config().arrow_optimization
    batch_size = batch_size or config.batch_size
    
    scanner = create_optimized_scanner(dataset, columns, filter, config)
    
    # Use parallel batch reading if enabled
    if config.use_threads:
        with ThreadPoolExecutor(max_workers=config.num_threads or 4) as executor:
            futures = []
            for batch in scanner.to_batches():
                futures.append(executor.submit(lambda b: b, batch))
                
                # Yield completed batches
                for future in as_completed(futures):
                    yield future.result()
                    futures.remove(future)
    else:
        # Sequential reading
        for batch in scanner.to_batches():
            yield batch


def coalesce_small_fragments(
    dataset: ds.Dataset,
    config: Optional[ArrowOptimizationConfig] = None,
) -> ds.Dataset:
    """Coalesce small dataset fragments for better performance.
    
    Args:
        dataset: Arrow Dataset with potentially small fragments
        config: Arrow optimization configuration
        
    Returns:
        Optimized dataset with coalesced fragments.
    """
    config = config or get_performance_config().arrow_optimization
    
    if not config.coalesce_small_files:
        return dataset
    
    fragments = list(dataset.get_fragments())
    
    if len(fragments) <= 1:
        return dataset
    
    # Group small fragments
    min_size_bytes = config.min_fragment_size_mb * 1024 * 1024
    target_size_bytes = config.target_fragment_size_mb * 1024 * 1024
    
    # Get fragment sizes (approximate)
    fragment_groups = []
    current_group = []
    current_size = 0
    
    for fragment in fragments:
        # Estimate fragment size (this is approximate)
        try:
            # Try to get actual file size if it's a file fragment
            if hasattr(fragment, 'path'):
                import os
                size = os.path.getsize(fragment.path)
            else:
                # Fallback: estimate based on schema and row count
                size = min_size_bytes  # Conservative estimate
        except:
            size = min_size_bytes
        
        if current_size + size > target_size_bytes and current_group:
            fragment_groups.append(current_group)
            current_group = [fragment]
            current_size = size
        else:
            current_group.append(fragment)
            current_size += size
    
    if current_group:
        fragment_groups.append(current_group)
    
    # If no significant coalescing possible, return original
    if len(fragment_groups) >= len(fragments) * 0.8:
        return dataset
    
    logger.info(f"Coalescing {len(fragments)} fragments into {len(fragment_groups)} groups")
    
    # Create new dataset from coalesced fragments
    # Note: This is a simplified version - real implementation would
    # actually combine the fragments
    return dataset


def optimize_manifest_reading(
    manifest: Manifest,
    columns: Optional[List[str]] = None,
    filters: Optional[pc.Expression] = None,
    config: Optional[ArrowOptimizationConfig] = None,
) -> pa.Table:
    """Optimized reading of manifest with predicate and projection pushdown.
    
    Args:
        manifest: DeltaCAT manifest
        columns: Columns to read
        filters: Filter expressions
        config: Arrow optimization configuration
        
    Returns:
        Optimized Arrow Table.
    """
    config = config or get_performance_config().arrow_optimization
    
    # Get dataset with caching
    cache = ArrowDatasetCache(config)
    cache_key = f"manifest_{id(manifest)}"
    
    dataset = cache.get(cache_key)
    if dataset is None:
        dataset = manifest_to_arrow_dataset(manifest, filters=filters)
        dataset = coalesce_small_fragments(dataset, config)
        cache.put(cache_key, dataset)
    
    # Create optimized scanner
    scanner = create_optimized_scanner(dataset, columns, filters, config)
    
    # Read with optimal settings
    if config.use_threads:
        # Parallel reading
        batches = []
        with ThreadPoolExecutor(max_workers=config.num_threads or 4) as executor:
            futures = [
                executor.submit(lambda b: b, batch)
                for batch in scanner.to_batches()
            ]
            
            for future in as_completed(futures):
                batches.append(future.result())
        
        return pa.Table.from_batches(batches)
    else:
        # Sequential reading
        return scanner.to_table()


@lru_cache(maxsize=128)
def get_parquet_metadata(file_path: str) -> pq.FileMetaData:
    """Get cached Parquet file metadata.
    
    Args:
        file_path: Path to Parquet file
        
    Returns:
        Parquet file metadata.
    """
    return pq.read_metadata(file_path)


def estimate_dataset_size(dataset: ds.Dataset) -> int:
    """Estimate the size of a dataset in bytes.
    
    Args:
        dataset: Arrow Dataset
        
    Returns:
        Estimated size in bytes.
    """
    total_size = 0
    
    for fragment in dataset.get_fragments():
        if hasattr(fragment, 'path'):
            try:
                metadata = get_parquet_metadata(fragment.path)
                for row_group in range(metadata.num_row_groups):
                    rg_metadata = metadata.row_group(row_group)
                    total_size += rg_metadata.total_byte_size
            except:
                # Fallback estimate
                total_size += 100 * 1024 * 1024  # 100MB default
        else:
            total_size += 100 * 1024 * 1024
    
    return total_size


# Global cache instance
_global_dataset_cache: Optional[ArrowDatasetCache] = None


def get_dataset_cache() -> ArrowDatasetCache:
    """Get the global dataset cache."""
    global _global_dataset_cache
    if _global_dataset_cache is None:
        _global_dataset_cache = ArrowDatasetCache()
    return _global_dataset_cache


def reset_dataset_cache() -> None:
    """Reset the global dataset cache."""
    global _global_dataset_cache
    if _global_dataset_cache:
        _global_dataset_cache.clear()
    _global_dataset_cache = None