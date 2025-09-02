"""Performance optimization configuration for DeltaCAT."""

import os
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from enum import Enum


class CacheBackend(Enum):
    """Supported cache backends."""
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHE = "memcache"


@dataclass
class ConnectionPoolConfig:
    """Configuration for connection pooling."""
    
    enabled: bool = True
    min_size: int = 1
    max_size: int = 10
    max_idle_time: int = 300  # seconds
    connection_timeout: int = 30  # seconds
    validation_interval: int = 60  # seconds
    
    # Per-catalog type configurations
    duckdb_pool_size: int = 5
    unity_pool_size: int = 10
    rest_pool_size: int = 20
    
    # Thread safety
    thread_safe: bool = True
    
    @classmethod
    def from_env(cls) -> "ConnectionPoolConfig":
        """Create config from environment variables."""
        return cls(
            enabled=os.getenv("DELTACAT_POOL_ENABLED", "true").lower() == "true",
            min_size=int(os.getenv("DELTACAT_POOL_MIN_SIZE", "1")),
            max_size=int(os.getenv("DELTACAT_POOL_MAX_SIZE", "10")),
            max_idle_time=int(os.getenv("DELTACAT_POOL_IDLE_TIME", "300")),
            connection_timeout=int(os.getenv("DELTACAT_POOL_TIMEOUT", "30")),
        )


@dataclass
class MetadataCacheConfig:
    """Configuration for metadata caching."""
    
    enabled: bool = True
    backend: CacheBackend = CacheBackend.MEMORY
    
    # TTL settings (seconds)
    default_ttl: int = 300  # 5 minutes
    schema_ttl: int = 600  # 10 minutes
    manifest_ttl: int = 300  # 5 minutes
    table_list_ttl: int = 60  # 1 minute
    namespace_ttl: int = 600  # 10 minutes
    
    # Cache size limits
    max_memory_items: int = 1000
    max_memory_size_mb: int = 100
    
    # Redis configuration
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    redis_ssl: bool = False
    redis_connection_pool_kwargs: Dict[str, Any] = field(default_factory=dict)
    
    # Memcache configuration
    memcache_servers: list = field(default_factory=lambda: ["localhost:11211"])
    memcache_timeout: int = 1
    memcache_connect_timeout: int = 1
    
    # Cache key prefix
    key_prefix: str = "deltacat:"
    
    @classmethod
    def from_env(cls) -> "MetadataCacheConfig":
        """Create config from environment variables."""
        backend_str = os.getenv("DELTACAT_CACHE_BACKEND", "memory").lower()
        backend = CacheBackend.MEMORY
        if backend_str == "redis":
            backend = CacheBackend.REDIS
        elif backend_str == "memcache":
            backend = CacheBackend.MEMCACHE
            
        return cls(
            enabled=os.getenv("DELTACAT_CACHE_ENABLED", "true").lower() == "true",
            backend=backend,
            default_ttl=int(os.getenv("DELTACAT_CACHE_TTL", "300")),
            schema_ttl=int(os.getenv("DELTACAT_CACHE_SCHEMA_TTL", "600")),
            manifest_ttl=int(os.getenv("DELTACAT_CACHE_MANIFEST_TTL", "300")),
            redis_host=os.getenv("DELTACAT_REDIS_HOST", "localhost"),
            redis_port=int(os.getenv("DELTACAT_REDIS_PORT", "6379")),
            redis_password=os.getenv("DELTACAT_REDIS_PASSWORD"),
        )


@dataclass
class ArrowOptimizationConfig:
    """Configuration for Arrow/Parquet optimizations."""
    
    enabled: bool = True
    
    # Batch reading
    batch_size: int = 65536  # rows per batch
    use_threads: bool = True
    num_threads: Optional[int] = None  # None = use all available
    
    # Predicate pushdown
    enable_predicate_pushdown: bool = True
    enable_projection_pushdown: bool = True
    
    # Fragment coalescing
    coalesce_small_files: bool = True
    min_fragment_size_mb: int = 10
    target_fragment_size_mb: int = 128
    
    # Memory management
    memory_pool: Optional[str] = None  # "system", "jemalloc", or None
    max_memory_mb: int = 1024
    
    # Columnar optimization
    enable_dictionary_encoding: bool = True
    compression: str = "snappy"  # "snappy", "gzip", "brotli", "lz4", "zstd"
    compression_level: Optional[int] = None
    
    # Dataset caching
    cache_datasets: bool = True
    dataset_cache_size: int = 100
    dataset_cache_ttl: int = 300  # seconds
    
    @classmethod
    def from_env(cls) -> "ArrowOptimizationConfig":
        """Create config from environment variables."""
        return cls(
            enabled=os.getenv("DELTACAT_ARROW_OPT_ENABLED", "true").lower() == "true",
            batch_size=int(os.getenv("DELTACAT_ARROW_BATCH_SIZE", "65536")),
            use_threads=os.getenv("DELTACAT_ARROW_USE_THREADS", "true").lower() == "true",
            enable_predicate_pushdown=os.getenv("DELTACAT_ARROW_PREDICATE_PUSHDOWN", "true").lower() == "true",
            coalesce_small_files=os.getenv("DELTACAT_ARROW_COALESCE", "true").lower() == "true",
        )


@dataclass
class LazyLoadingConfig:
    """Configuration for lazy loading."""
    
    enabled: bool = True
    
    # Schema loading
    lazy_schema: bool = True
    schema_fetch_timeout: int = 30  # seconds
    
    # Table definition loading
    lazy_table_definition: bool = True
    load_manifest_on_access: bool = True
    
    # Progressive loading
    progressive_schema_loading: bool = True
    schema_chunk_size: int = 100  # fields per chunk
    
    # Preload settings
    preload_common_tables: bool = False
    common_tables: list = field(default_factory=list)
    
    @classmethod
    def from_env(cls) -> "LazyLoadingConfig":
        """Create config from environment variables."""
        return cls(
            enabled=os.getenv("DELTACAT_LAZY_ENABLED", "true").lower() == "true",
            lazy_schema=os.getenv("DELTACAT_LAZY_SCHEMA", "true").lower() == "true",
            lazy_table_definition=os.getenv("DELTACAT_LAZY_TABLE_DEF", "true").lower() == "true",
            progressive_schema_loading=os.getenv("DELTACAT_PROGRESSIVE_SCHEMA", "true").lower() == "true",
        )


@dataclass
class PerformanceConfig:
    """Main performance optimization configuration."""
    
    # Feature flags
    enable_all_optimizations: bool = True
    
    # Individual configurations
    connection_pool: ConnectionPoolConfig = field(default_factory=ConnectionPoolConfig)
    metadata_cache: MetadataCacheConfig = field(default_factory=MetadataCacheConfig)
    arrow_optimization: ArrowOptimizationConfig = field(default_factory=ArrowOptimizationConfig)
    lazy_loading: LazyLoadingConfig = field(default_factory=LazyLoadingConfig)
    
    # Global settings
    profile_performance: bool = False
    log_performance_metrics: bool = False
    metrics_interval: int = 60  # seconds
    
    @classmethod
    def from_env(cls) -> "PerformanceConfig":
        """Create config from environment variables."""
        enable_all = os.getenv("DELTACAT_PERF_ENABLED", "true").lower() == "true"
        
        config = cls(
            enable_all_optimizations=enable_all,
            connection_pool=ConnectionPoolConfig.from_env(),
            metadata_cache=MetadataCacheConfig.from_env(),
            arrow_optimization=ArrowOptimizationConfig.from_env(),
            lazy_loading=LazyLoadingConfig.from_env(),
            profile_performance=os.getenv("DELTACAT_PROFILE", "false").lower() == "true",
            log_performance_metrics=os.getenv("DELTACAT_LOG_METRICS", "false").lower() == "true",
        )
        
        # Override individual settings if global is disabled
        if not enable_all:
            config.connection_pool.enabled = False
            config.metadata_cache.enabled = False
            config.arrow_optimization.enabled = False
            config.lazy_loading.enabled = False
            
        return config
    
    def is_enabled(self, feature: str) -> bool:
        """Check if a specific feature is enabled."""
        if not self.enable_all_optimizations:
            return False
            
        feature_map = {
            "connection_pool": self.connection_pool.enabled,
            "metadata_cache": self.metadata_cache.enabled,
            "arrow_optimization": self.arrow_optimization.enabled,
            "lazy_loading": self.lazy_loading.enabled,
        }
        
        return feature_map.get(feature, False)


# Global configuration instance
_global_config: Optional[PerformanceConfig] = None


def get_performance_config() -> PerformanceConfig:
    """Get the global performance configuration."""
    global _global_config
    if _global_config is None:
        _global_config = PerformanceConfig.from_env()
    return _global_config


def set_performance_config(config: PerformanceConfig) -> None:
    """Set the global performance configuration."""
    global _global_config
    _global_config = config


def reset_performance_config() -> None:
    """Reset the global performance configuration."""
    global _global_config
    _global_config = None