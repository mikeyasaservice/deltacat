"""Lazy loading implementation for schema operations."""

import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union
import pyarrow as pa

from deltacat import logs
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.schema import Schema, Field
from deltacat.storage.model.table import Table
from deltacat.storage.model.manifest import Manifest
from deltacat.config.performance import LazyLoadingConfig, get_performance_config
from deltacat.catalog.cache.metadata_cache import get_metadata_cache
from deltacat.exceptions import DeltaCATException

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class LazyLoadError(DeltaCATException):
    """Exception raised for lazy loading errors."""
    pass


class LazyProperty:
    """A property that loads its value lazily."""
    
    def __init__(self, loader: Callable[[], Any], cache_key: Optional[str] = None):
        """Initialize the lazy property.
        
        Args:
            loader: Function to load the value
            cache_key: Optional cache key for caching the value
        """
        self.loader = loader
        self.cache_key = cache_key
        self._value = None
        self._loaded = False
        self._lock = threading.RLock()
        self._metadata_cache = get_metadata_cache() if cache_key else None
    
    def get(self) -> Any:
        """Get the value, loading it if necessary."""
        if self._loaded:
            return self._value
        
        with self._lock:
            # Double-check after acquiring lock
            if self._loaded:
                return self._value
            
            # Try cache first
            if self._metadata_cache and self.cache_key:
                cached_value = self._metadata_cache.get(self.cache_key, "lazy")
                if cached_value is not None:
                    self._value = cached_value
                    self._loaded = True
                    return self._value
            
            # Load the value
            try:
                self._value = self.loader()
                self._loaded = True
                
                # Cache the value
                if self._metadata_cache and self.cache_key:
                    self._metadata_cache.set(self.cache_key, self._value, "lazy")
                
                return self._value
            except Exception as e:
                logger.error(f"Failed to load lazy property: {e}")
                raise LazyLoadError(f"Failed to load property: {e}")
    
    def is_loaded(self) -> bool:
        """Check if the value has been loaded."""
        return self._loaded
    
    def reset(self) -> None:
        """Reset the lazy property to unloaded state."""
        with self._lock:
            self._value = None
            self._loaded = False


@dataclass
class LazySchema:
    """A schema that loads its fields lazily."""
    
    schema_loader: Callable[[], Schema]
    config: LazyLoadingConfig = field(default_factory=lambda: get_performance_config().lazy_loading)
    _schema: Optional[Schema] = field(default=None, init=False)
    _fields: Optional[List[Field]] = field(default=None, init=False)
    _arrow_schema: Optional[pa.Schema] = field(default=None, init=False)
    _loaded: bool = field(default=False, init=False)
    _partial_loaded: bool = field(default=False, init=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)
    
    def _ensure_loaded(self, full: bool = True) -> None:
        """Ensure the schema is loaded.
        
        Args:
            full: Whether to load the full schema or just metadata
        """
        if self._loaded:
            return
        
        with self._lock:
            if self._loaded:
                return
            
            try:
                self._schema = self.schema_loader()
                self._loaded = True
            except Exception as e:
                logger.error(f"Failed to load schema: {e}")
                raise LazyLoadError(f"Failed to load schema: {e}")
    
    def _load_fields_progressively(self, start: int = 0, count: Optional[int] = None) -> List[Field]:
        """Load fields progressively.
        
        Args:
            start: Starting field index
            count: Number of fields to load (None = all remaining)
            
        Returns:
            List of loaded fields
        """
        self._ensure_loaded()
        
        if not self.config.progressive_schema_loading:
            return self._schema.fields
        
        if self._fields is None:
            self._fields = []
        
        # Determine how many fields to load
        total_fields = len(self._schema.fields)
        if count is None:
            count = self.config.schema_chunk_size
        
        end = min(start + count, total_fields)
        
        # Load the requested fields
        for i in range(start, end):
            if i < len(self._fields):
                continue  # Already loaded
            self._fields.append(self._schema.fields[i])
        
        return self._fields
    
    @property
    def fields(self) -> List[Field]:
        """Get all schema fields."""
        if not self.config.enabled or not self.config.lazy_schema:
            self._ensure_loaded()
            return self._schema.fields
        
        if self._fields and len(self._fields) == len(self._schema.fields):
            return self._fields
        
        # Load all fields
        self._load_fields_progressively(0, None)
        return self._fields
    
    @property
    def field_count(self) -> int:
        """Get the number of fields without loading them all."""
        self._ensure_loaded()
        return len(self._schema.fields) if self._schema else 0
    
    def get_field(self, index: int) -> Field:
        """Get a specific field by index."""
        if not self.config.enabled or not self.config.lazy_schema:
            self._ensure_loaded()
            return self._schema.fields[index]
        
        # Load just the fields up to the requested index
        self._load_fields_progressively(0, index + 1)
        return self._fields[index]
    
    def get_field_by_name(self, name: str) -> Optional[Field]:
        """Get a field by name."""
        # This requires loading all fields to search
        for field in self.fields:
            if field.name == name:
                return field
        return None
    
    @property
    def arrow_schema(self) -> pa.Schema:
        """Get the Arrow schema representation."""
        if self._arrow_schema is None:
            self._ensure_loaded()
            # Convert to Arrow schema
            arrow_fields = []
            for field in self.fields:
                # Convert DeltaCAT field to Arrow field
                # This is a simplified conversion
                arrow_type = pa.string()  # Default type
                if hasattr(field, 'arrow_type'):
                    arrow_type = field.arrow_type
                arrow_fields.append(pa.field(field.name, arrow_type))
            self._arrow_schema = pa.schema(arrow_fields)
        
        return self._arrow_schema
    
    def is_loaded(self) -> bool:
        """Check if the schema has been loaded."""
        return self._loaded
    
    def reset(self) -> None:
        """Reset the lazy schema to unloaded state."""
        with self._lock:
            self._schema = None
            self._fields = None
            self._arrow_schema = None
            self._loaded = False
            self._partial_loaded = False


@dataclass
class LazyTableDefinition:
    """A table definition that loads its components lazily."""
    
    table_loader: Callable[[], TableDefinition]
    config: LazyLoadingConfig = field(default_factory=lambda: get_performance_config().lazy_loading)
    cache_key: Optional[str] = field(default=None)
    
    _table_def: Optional[TableDefinition] = field(default=None, init=False)
    _table: Optional[Table] = field(default=None, init=False)
    _schema: Optional[LazySchema] = field(default=None, init=False)
    _manifest: Optional[Manifest] = field(default=None, init=False)
    _loaded: bool = field(default=False, init=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)
    
    def _ensure_loaded(self) -> None:
        """Ensure the table definition is loaded."""
        if self._loaded:
            return
        
        with self._lock:
            if self._loaded:
                return
            
            # Try cache first
            if self.cache_key:
                cache = get_metadata_cache()
                cached_def = cache.get(self.cache_key, "table_definition")
                if cached_def:
                    self._table_def = cached_def
                    self._loaded = True
                    return
            
            try:
                self._table_def = self.table_loader()
                self._loaded = True
                
                # Cache the definition
                if self.cache_key:
                    cache = get_metadata_cache()
                    cache.set(self.cache_key, self._table_def, "table_definition")
                    
            except Exception as e:
                logger.error(f"Failed to load table definition: {e}")
                raise LazyLoadError(f"Failed to load table definition: {e}")
    
    @property
    def table(self) -> Table:
        """Get the table metadata."""
        if not self.config.enabled or not self.config.lazy_table_definition:
            self._ensure_loaded()
            return self._table_def.table
        
        if self._table is None:
            self._ensure_loaded()
            self._table = self._table_def.table
        
        return self._table
    
    @property
    def schema(self) -> LazySchema:
        """Get the table schema lazily."""
        if not self.config.enabled or not self.config.lazy_schema:
            self._ensure_loaded()
            return self._table_def.schema
        
        if self._schema is None:
            # Create a lazy schema wrapper
            def load_schema():
                self._ensure_loaded()
                return self._table_def.schema
            
            self._schema = LazySchema(
                schema_loader=load_schema,
                config=self.config,
            )
        
        return self._schema
    
    @property
    def manifest(self) -> Optional[Manifest]:
        """Get the table manifest lazily."""
        if not self.config.enabled or not self.config.load_manifest_on_access:
            self._ensure_loaded()
            return self._table_def.manifest if hasattr(self._table_def, 'manifest') else None
        
        if self._manifest is None:
            self._ensure_loaded()
            if hasattr(self._table_def, 'manifest'):
                self._manifest = self._table_def.manifest
        
        return self._manifest
    
    @property
    def name(self) -> str:
        """Get the table name without loading everything."""
        # Try to get name from table if already loaded
        if self._table:
            return self._table.name
        
        # Otherwise load the definition
        self._ensure_loaded()
        return self._table_def.table.name
    
    @property
    def namespace(self) -> Optional[str]:
        """Get the table namespace without loading everything."""
        if self._table:
            return self._table.namespace
        
        self._ensure_loaded()
        return self._table_def.table.namespace
    
    @property
    def location(self) -> Optional[str]:
        """Get the table location."""
        if self._table:
            return self._table.location if hasattr(self._table, 'location') else None
        
        self._ensure_loaded()
        return self._table_def.table.location if hasattr(self._table_def.table, 'location') else None
    
    def is_loaded(self) -> bool:
        """Check if the table definition has been loaded."""
        return self._loaded
    
    def preload(self) -> None:
        """Preload the table definition and all its components."""
        self._ensure_loaded()
        
        # Force load all components
        _ = self.schema
        _ = self.manifest
        
        logger.debug(f"Preloaded table definition for {self.name}")
    
    def reset(self) -> None:
        """Reset the lazy table definition to unloaded state."""
        with self._lock:
            self._table_def = None
            self._table = None
            if self._schema:
                self._schema.reset()
            self._schema = None
            self._manifest = None
            self._loaded = False


class LazyTableList:
    """A list of tables that loads table definitions lazily."""
    
    def __init__(
        self,
        table_loaders: List[Tuple[str, str, Callable[[], TableDefinition]]],
        config: Optional[LazyLoadingConfig] = None,
    ):
        """Initialize the lazy table list.
        
        Args:
            table_loaders: List of (namespace, name, loader) tuples
            config: Lazy loading configuration
        """
        self.config = config or get_performance_config().lazy_loading
        self._tables: List[LazyTableDefinition] = []
        
        for namespace, name, loader in table_loaders:
            cache_key = f"table:{namespace}:{name}"
            lazy_def = LazyTableDefinition(
                table_loader=loader,
                config=self.config,
                cache_key=cache_key,
            )
            self._tables.append(lazy_def)
        
        # Preload common tables if configured
        if self.config.preload_common_tables:
            for table in self._tables:
                if table.name in self.config.common_tables:
                    table.preload()
    
    def __len__(self) -> int:
        """Get the number of tables."""
        return len(self._tables)
    
    def __getitem__(self, index: int) -> LazyTableDefinition:
        """Get a table by index."""
        return self._tables[index]
    
    def __iter__(self):
        """Iterate over the tables."""
        return iter(self._tables)
    
    def get_table(self, name: str, namespace: Optional[str] = None) -> Optional[LazyTableDefinition]:
        """Get a table by name and namespace."""
        for table in self._tables:
            if table.name == name:
                if namespace is None or table.namespace == namespace:
                    return table
        return None
    
    def list_table_names(self) -> List[Tuple[Optional[str], str]]:
        """List all table names without loading full definitions."""
        return [(table.namespace, table.name) for table in self._tables]
    
    def preload_all(self) -> None:
        """Preload all table definitions."""
        for table in self._tables:
            table.preload()


def create_lazy_schema(
    schema_or_loader: Union[Schema, Callable[[], Schema]],
    config: Optional[LazyLoadingConfig] = None,
) -> Union[Schema, LazySchema]:
    """Create a lazy schema wrapper if lazy loading is enabled.
    
    Args:
        schema_or_loader: Schema or function to load schema
        config: Lazy loading configuration
        
    Returns:
        Original schema or lazy wrapper
    """
    config = config or get_performance_config().lazy_loading
    
    if not config.enabled or not config.lazy_schema:
        # Return original or load immediately
        if callable(schema_or_loader):
            return schema_or_loader()
        return schema_or_loader
    
    # Create lazy wrapper
    if callable(schema_or_loader):
        return LazySchema(schema_loader=schema_or_loader, config=config)
    else:
        # Already have the schema, wrap it
        return LazySchema(
            schema_loader=lambda: schema_or_loader,
            config=config,
        )


def create_lazy_table_definition(
    table_def_or_loader: Union[TableDefinition, Callable[[], TableDefinition]],
    cache_key: Optional[str] = None,
    config: Optional[LazyLoadingConfig] = None,
) -> Union[TableDefinition, LazyTableDefinition]:
    """Create a lazy table definition wrapper if lazy loading is enabled.
    
    Args:
        table_def_or_loader: Table definition or function to load it
        cache_key: Optional cache key
        config: Lazy loading configuration
        
    Returns:
        Original table definition or lazy wrapper
    """
    config = config or get_performance_config().lazy_loading
    
    if not config.enabled or not config.lazy_table_definition:
        # Return original or load immediately
        if callable(table_def_or_loader):
            return table_def_or_loader()
        return table_def_or_loader
    
    # Create lazy wrapper
    if callable(table_def_or_loader):
        return LazyTableDefinition(
            table_loader=table_def_or_loader,
            config=config,
            cache_key=cache_key,
        )
    else:
        # Already have the definition, wrap it
        return LazyTableDefinition(
            table_loader=lambda: table_def_or_loader,
            config=config,
            cache_key=cache_key,
        )