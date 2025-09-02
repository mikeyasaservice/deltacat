"""Parquet file format implementation."""

from typing import Optional, List, Dict, Any, Union
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os
from pathlib import Path
from deltacat.storage.formats.base import TableFormat, TableMetadata


class ParquetFormat(TableFormat):
    """Parquet file format implementation.
    
    This class provides basic Parquet file operations without versioning
    or transaction support. It's suitable for simple data storage needs.
    """
    
    def __init__(self, path: str, **kwargs):
        """Initialize Parquet file handler.
        
        Args:
            path: Path to Parquet file or directory
            **kwargs: Parquet-specific configuration
        """
        super().__init__(path, **kwargs)
        self._metadata = None
    
    def read(self,
             columns: Optional[List[str]] = None,
             filter: Optional[str] = None,
             version: Optional[int] = None,
             timestamp: Optional[datetime] = None) -> pa.Table:
        """Read Parquet file(s).
        
        Args:
            columns: Columns to read (None for all)
            filter: Filter expression (limited support)
            version: Not supported for Parquet
            timestamp: Not supported for Parquet
            
        Returns:
            PyArrow Table with the data
        """
        if version is not None or timestamp is not None:
            raise NotImplementedError(
                "Parquet format does not support versioning or time travel"
            )
        
        if not os.path.exists(self.path):
            raise ValueError(f"Parquet file not found: {self.path}")
        
        # Read Parquet file(s)
        if os.path.isdir(self.path):
            # Read partitioned dataset
            dataset = pq.ParquetDataset(
                self.path,
                filters=filter,
                use_legacy_dataset=False
            )
            table = dataset.read(columns=columns)
        else:
            # Read single file
            table = pq.read_table(
                self.path,
                columns=columns
            )
            
            # Apply filter if provided (basic support)
            if filter:
                import pyarrow.compute as pc
                # This is a simplified filter implementation
                # In production, would need proper expression parsing
                table = table.filter(filter)
        
        return table
    
    def write(self,
              data: Union[pa.Table, pd.DataFrame],
              mode: str = 'overwrite',
              partition_by: Optional[List[str]] = None,
              **kwargs) -> None:
        """Write data to Parquet file(s).
        
        Args:
            data: Data to write
            mode: Write mode (only 'overwrite' fully supported)
            partition_by: Columns to partition by
            **kwargs: Additional Parquet write options
        """
        # Convert pandas to PyArrow if needed
        if isinstance(data, pd.DataFrame):
            data = pa.Table.from_pandas(data)
        
        if mode == 'append':
            if os.path.exists(self.path):
                # Read existing data
                existing = self.read()
                # Concatenate with new data
                data = pa.concat_tables([existing, data])
        elif mode == 'error' and os.path.exists(self.path):
            raise FileExistsError(f"Parquet file already exists: {self.path}")
        
        # Write Parquet file(s)
        if partition_by:
            # Write partitioned dataset
            pq.write_to_dataset(
                data,
                root_path=self.path,
                partition_cols=partition_by,
                **kwargs
            )
        else:
            # Ensure parent directory exists
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            
            # Write single file
            pq.write_table(
                data,
                self.path,
                **kwargs
            )
        
        # Clear cached metadata
        self._metadata = None
    
    def append(self, data: Union[pa.Table, pd.DataFrame]) -> None:
        """Append data to Parquet file.
        
        Note: This reads the existing file and rewrites it with appended data.
        """
        self.write(data, mode='append')
    
    def overwrite(self, data: Union[pa.Table, pd.DataFrame]) -> None:
        """Overwrite Parquet file with new data."""
        self.write(data, mode='overwrite')
    
    def get_schema(self, version: Optional[int] = None) -> pa.Schema:
        """Get Parquet file schema.
        
        Args:
            version: Not supported for Parquet
            
        Returns:
            PyArrow Schema
        """
        if version is not None:
            raise NotImplementedError("Parquet format does not support versioning")
        
        if not os.path.exists(self.path):
            raise ValueError(f"Parquet file not found: {self.path}")
        
        if os.path.isdir(self.path):
            # Get schema from partitioned dataset
            dataset = pq.ParquetDataset(self.path)
            return dataset.schema
        else:
            # Get schema from single file
            return pq.read_schema(self.path)
    
    def get_metadata(self) -> TableMetadata:
        """Get Parquet file metadata.
        
        Returns:
            TableMetadata with Parquet-specific information
        """
        if not os.path.exists(self.path):
            return TableMetadata(
                format="parquet",
                location=self.path,
                version=None
            )
        
        # Get file stats
        if os.path.isfile(self.path):
            stat = os.stat(self.path)
            created_time = datetime.fromtimestamp(stat.st_ctime)
            modified_time = datetime.fromtimestamp(stat.st_mtime)
            
            # Get Parquet metadata
            parquet_file = pq.ParquetFile(self.path)
            metadata = parquet_file.metadata
            
            properties = {
                "num_rows": metadata.num_rows,
                "num_columns": len(parquet_file.schema),
                "num_row_groups": metadata.num_row_groups,
                "format_version": metadata.format_version,
                "created_by": metadata.created_by
            }
        else:
            # Directory of Parquet files
            created_time = None
            modified_time = None
            properties = {
                "is_partitioned": True,
                "num_files": len(list(Path(self.path).rglob("*.parquet")))
            }
        
        return TableMetadata(
            format="parquet",
            version=None,  # No versioning in Parquet
            location=self.path,
            created_time=created_time,
            last_modified=modified_time,
            partition_columns=None,  # Would need to detect from directory structure
            properties=properties
        )
    
    def get_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get file history.
        
        Parquet doesn't support versioning.
        
        Raises:
            NotImplementedError: Parquet doesn't support history
        """
        raise NotImplementedError(
            "Parquet format does not support version history"
        )
    
    def time_travel(self,
                    version: Optional[int] = None,
                    timestamp: Optional[datetime] = None) -> pa.Table:
        """Time travel is not supported for Parquet.
        
        Raises:
            NotImplementedError: Parquet doesn't support time travel
        """
        raise NotImplementedError(
            "Parquet format does not support time travel or versioning"
        )
    
    def optimize(self, **kwargs) -> Dict[str, Any]:
        """Optimize Parquet storage.
        
        Limited optimization for Parquet - mainly rewriting with better compression.
        
        Args:
            **kwargs: Optimization options (compression, row_group_size)
            
        Returns:
            Dictionary with optimization results
        """
        if not os.path.exists(self.path):
            raise ValueError(f"Parquet file not found: {self.path}")
        
        # Read existing data
        table = self.read()
        
        # Get original size
        if os.path.isfile(self.path):
            original_size = os.path.getsize(self.path)
        else:
            original_size = sum(
                f.stat().st_size 
                for f in Path(self.path).rglob("*.parquet")
            )
        
        # Rewrite with optimization
        compression = kwargs.get('compression', 'snappy')
        row_group_size = kwargs.get('row_group_size', 64 * 1024 * 1024)  # 64MB
        
        self.write(
            table,
            compression=compression,
            row_group_size=row_group_size
        )
        
        # Get new size
        if os.path.isfile(self.path):
            new_size = os.path.getsize(self.path)
        else:
            new_size = sum(
                f.stat().st_size 
                for f in Path(self.path).rglob("*.parquet")
            )
        
        return {
            "original_size": original_size,
            "new_size": new_size,
            "compression_ratio": original_size / new_size if new_size > 0 else 0,
            "compression": compression
        }
    
    def vacuum(self, retention_hours: int = 168, **kwargs) -> Dict[str, Any]:
        """Vacuum is not applicable for Parquet.
        
        Returns:
            Empty results as Parquet doesn't have versions to clean up
        """
        return {
            "files_deleted": 0,
            "message": "Parquet format does not support vacuum (no versioning)"
        }
    
    def exists(self) -> bool:
        """Check if Parquet file exists.
        
        Returns:
            True if file/directory exists, False otherwise
        """
        return os.path.exists(self.path)
    
    def delete(self) -> None:
        """Delete the Parquet file(s).
        
        This removes the file or directory.
        """
        import shutil
        
        if os.path.exists(self.path):
            if os.path.isfile(self.path):
                os.remove(self.path)
            else:
                shutil.rmtree(self.path)
    
    def get_format_type(self) -> str:
        """Get the format type identifier.
        
        Returns:
            'parquet'
        """
        return 'parquet'