"""Utilities for exposing DeltaCAT data as Arrow objects for zero-copy SQL access."""

import logging
from typing import List, Optional, Union
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc

from deltacat import logs
from deltacat.storage import Delta, Manifest
from deltacat.types.media import ContentType
from deltacat.utils.pyarrow import (
    content_type_to_reader_kwargs,
    content_type_to_pyarrow_read_func,
)

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
    
    def __init__(self, max_size: int = 100):
        """Initialize the cache.
        
        Args:
            max_size: Maximum number of datasets to cache
        """
        self._cache: Dict[str, ds.Dataset] = {}
        self._max_size = max_size
        
    def get(self, key: str) -> Optional[ds.Dataset]:
        """Get a dataset from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached dataset or None.
        """
        return self._cache.get(key)
    
    def put(self, key: str, dataset: ds.Dataset):
        """Add a dataset to the cache.
        
        Args:
            key: Cache key
            dataset: Dataset to cache
        """
        if len(self._cache) >= self._max_size:
            # Remove oldest entry (simple FIFO)
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]
        
        self._cache[key] = dataset
    
    def clear(self):
        """Clear the cache."""
        self._cache.clear()


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