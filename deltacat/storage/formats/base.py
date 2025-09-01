"""Base abstract class for table format implementations."""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
import pyarrow as pa
from dataclasses import dataclass


@dataclass
class TableMetadata:
    """Metadata about a table."""
    format: str
    version: Optional[int] = None
    location: Optional[str] = None
    created_time: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    partition_columns: Optional[List[str]] = None
    properties: Optional[Dict[str, Any]] = None


class TableFormat(ABC):
    """Abstract base class for table format implementations.
    
    This class defines the interface that all table format implementations
    must follow, ensuring consistent behavior across Delta Lake, Iceberg,
    and Parquet formats.
    """
    
    def __init__(self, path: str, **kwargs):
        """Initialize table format handler.
        
        Args:
            path: Path to the table location
            **kwargs: Format-specific configuration options
        """
        self.path = path
        self.config = kwargs
    
    @abstractmethod
    def read(self, 
             columns: Optional[List[str]] = None,
             filter: Optional[str] = None,
             version: Optional[int] = None,
             timestamp: Optional[datetime] = None) -> pa.Table:
        """Read table data.
        
        Args:
            columns: List of columns to read (None for all)
            filter: Filter expression to apply
            version: Specific version to read (format-specific)
            timestamp: Read table as of specific timestamp
            
        Returns:
            PyArrow Table containing the data
        """
        pass
    
    @abstractmethod
    def write(self, 
              data: Union[pa.Table, 'pd.DataFrame'],
              mode: str = 'append',
              partition_by: Optional[List[str]] = None,
              **kwargs) -> None:
        """Write data to table.
        
        Args:
            data: Data to write (PyArrow Table or Pandas DataFrame)
            mode: Write mode ('append', 'overwrite', 'error')
            partition_by: Columns to partition by
            **kwargs: Format-specific write options
        """
        pass
    
    @abstractmethod
    def append(self, data: Union[pa.Table, 'pd.DataFrame']) -> None:
        """Append data to existing table.
        
        Args:
            data: Data to append
        """
        pass
    
    @abstractmethod
    def overwrite(self, data: Union[pa.Table, 'pd.DataFrame']) -> None:
        """Overwrite entire table with new data.
        
        Args:
            data: Data to write
        """
        pass
    
    @abstractmethod
    def get_schema(self, version: Optional[int] = None) -> pa.Schema:
        """Get table schema.
        
        Args:
            version: Specific version to get schema for
            
        Returns:
            PyArrow Schema
        """
        pass
    
    @abstractmethod
    def get_metadata(self) -> TableMetadata:
        """Get table metadata.
        
        Returns:
            TableMetadata object with format-specific information
        """
        pass
    
    @abstractmethod
    def get_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get table history/versions.
        
        Args:
            limit: Maximum number of history entries to return
            
        Returns:
            List of history entries with format-specific information
        """
        pass
    
    @abstractmethod
    def time_travel(self, 
                    version: Optional[int] = None,
                    timestamp: Optional[datetime] = None) -> pa.Table:
        """Read table at specific version or timestamp.
        
        Args:
            version: Version number to read
            timestamp: Timestamp to read table as of
            
        Returns:
            PyArrow Table at the specified version/time
        """
        pass
    
    @abstractmethod
    def optimize(self, **kwargs) -> Dict[str, Any]:
        """Optimize table storage.
        
        Args:
            **kwargs: Format-specific optimization options
            
        Returns:
            Dictionary with optimization results
        """
        pass
    
    @abstractmethod
    def vacuum(self, retention_hours: int = 168, **kwargs) -> Dict[str, Any]:
        """Clean up old files and versions.
        
        Args:
            retention_hours: Hours of history to retain (default 7 days)
            **kwargs: Format-specific vacuum options
            
        Returns:
            Dictionary with vacuum results
        """
        pass
    
    @abstractmethod
    def exists(self) -> bool:
        """Check if table exists at the specified path.
        
        Returns:
            True if table exists, False otherwise
        """
        pass
    
    @abstractmethod
    def delete(self) -> None:
        """Delete the entire table.
        
        This is a destructive operation that removes all data and metadata.
        """
        pass
    
    @abstractmethod
    def get_format_type(self) -> str:
        """Get the format type identifier.
        
        Returns:
            Format type string ('delta', 'iceberg', 'parquet')
        """
        pass
    
    def __repr__(self) -> str:
        """String representation of table format handler."""
        return f"{self.__class__.__name__}(path='{self.path}')"