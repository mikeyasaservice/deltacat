"""Metadata caching layer with TTL support."""

import hashlib
import json
import logging
import pickle
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union
import threading

from deltacat import logs
from deltacat.config.performance import (
    CacheBackend,
    MetadataCacheConfig,
    get_performance_config,
)
from deltacat.exceptions import DeltaCATException

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class CacheError(DeltaCATException):
    """Exception raised for cache errors."""
    pass


@dataclass
class CacheEntry:
    """A cache entry with value and metadata."""
    
    key: str
    value: Any
    ttl: int
    created_at: float
    accessed_at: float
    access_count: int = 0
    size_bytes: int = 0
    
    def is_expired(self) -> bool:
        """Check if the entry has expired."""
        if self.ttl <= 0:
            return False
        return (time.time() - self.created_at) > self.ttl
    
    def access(self) -> None:
        """Mark the entry as accessed."""
        self.accessed_at = time.time()
        self.access_count += 1


class CacheBackendInterface(ABC):
    """Abstract interface for cache backends."""
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache."""
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set a value in the cache."""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete a value from the cache."""
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """Clear all values from the cache."""
        pass
    
    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        pass
    
    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        pass


class MemoryCacheBackend(CacheBackendInterface):
    """In-memory cache backend with LRU eviction."""
    
    def __init__(self, config: MetadataCacheConfig):
        """Initialize the memory cache backend."""
        self.config = config
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._total_size_bytes = 0
        self._hits = 0
        self._misses = 0
        
    def _evict_if_needed(self) -> None:
        """Evict entries if cache is too large."""
        # Evict expired entries first
        expired_keys = [
            key for key, entry in self._cache.items()
            if entry.is_expired()
        ]
        for key in expired_keys:
            self._remove_entry(key)
        
        # Evict by LRU if still over limits
        max_size_bytes = self.config.max_memory_size_mb * 1024 * 1024
        while (
            len(self._cache) > self.config.max_memory_items or
            self._total_size_bytes > max_size_bytes
        ):
            if not self._cache:
                break
            # Remove least recently used
            key = next(iter(self._cache))
            self._remove_entry(key)
    
    def _remove_entry(self, key: str) -> None:
        """Remove an entry from the cache."""
        if key in self._cache:
            entry = self._cache[key]
            self._total_size_bytes -= entry.size_bytes
            del self._cache[key]
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache."""
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
            
            entry = self._cache[key]
            if entry.is_expired():
                self._remove_entry(key)
                self._misses += 1
                return None
            
            # Move to end (most recently used)
            self._cache.move_to_end(key)
            entry.access()
            self._hits += 1
            
            return entry.value
    
    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set a value in the cache."""
        with self._lock:
            # Calculate size
            try:
                size_bytes = len(pickle.dumps(value))
            except Exception:
                size_bytes = 0
            
            # Remove old entry if exists
            if key in self._cache:
                self._remove_entry(key)
            
            # Create new entry
            entry = CacheEntry(
                key=key,
                value=value,
                ttl=ttl,
                created_at=time.time(),
                accessed_at=time.time(),
                size_bytes=size_bytes,
            )
            
            # Add to cache
            self._cache[key] = entry
            self._total_size_bytes += size_bytes
            
            # Evict if needed
            self._evict_if_needed()
    
    def delete(self, key: str) -> None:
        """Delete a value from the cache."""
        with self._lock:
            self._remove_entry(key)
    
    def clear(self) -> None:
        """Clear all values from the cache."""
        with self._lock:
            self._cache.clear()
            self._total_size_bytes = 0
            self._hits = 0
            self._misses = 0
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        with self._lock:
            if key not in self._cache:
                return False
            entry = self._cache[key]
            return not entry.is_expired()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total_requests = self._hits + self._misses
            hit_rate = self._hits / total_requests if total_requests > 0 else 0
            
            return {
                "backend": "memory",
                "entries": len(self._cache),
                "size_bytes": self._total_size_bytes,
                "size_mb": self._total_size_bytes / (1024 * 1024),
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
            }


class RedisCacheBackend(CacheBackendInterface):
    """Redis cache backend."""
    
    def __init__(self, config: MetadataCacheConfig):
        """Initialize the Redis cache backend."""
        self.config = config
        self._redis_client = None
        self._connect()
        
    def _connect(self) -> None:
        """Connect to Redis."""
        try:
            import redis
            
            pool_kwargs = self.config.redis_connection_pool_kwargs.copy()
            pool_kwargs.update({
                "host": self.config.redis_host,
                "port": self.config.redis_port,
                "db": self.config.redis_db,
                "password": self.config.redis_password,
                "ssl": self.config.redis_ssl,
                "decode_responses": False,  # We'll handle encoding/decoding
            })
            
            pool = redis.ConnectionPool(**pool_kwargs)
            self._redis_client = redis.Redis(connection_pool=pool)
            
            # Test connection
            self._redis_client.ping()
            logger.info(f"Connected to Redis at {self.config.redis_host}:{self.config.redis_port}")
            
        except ImportError:
            raise CacheError("Redis client not installed. Install with: pip install redis")
        except Exception as e:
            raise CacheError(f"Failed to connect to Redis: {e}")
    
    def _make_key(self, key: str) -> str:
        """Create a prefixed key for Redis."""
        return f"{self.config.key_prefix}{key}"
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache."""
        try:
            redis_key = self._make_key(key)
            data = self._redis_client.get(redis_key)
            
            if data is None:
                return None
            
            return pickle.loads(data)
            
        except Exception as e:
            logger.warning(f"Redis get error for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set a value in the cache."""
        try:
            redis_key = self._make_key(key)
            data = pickle.dumps(value)
            
            if ttl > 0:
                self._redis_client.setex(redis_key, ttl, data)
            else:
                self._redis_client.set(redis_key, data)
                
        except Exception as e:
            logger.warning(f"Redis set error for key {key}: {e}")
    
    def delete(self, key: str) -> None:
        """Delete a value from the cache."""
        try:
            redis_key = self._make_key(key)
            self._redis_client.delete(redis_key)
        except Exception as e:
            logger.warning(f"Redis delete error for key {key}: {e}")
    
    def clear(self) -> None:
        """Clear all values from the cache."""
        try:
            pattern = f"{self.config.key_prefix}*"
            cursor = 0
            while True:
                cursor, keys = self._redis_client.scan(cursor, match=pattern, count=100)
                if keys:
                    self._redis_client.delete(*keys)
                if cursor == 0:
                    break
        except Exception as e:
            logger.warning(f"Redis clear error: {e}")
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        try:
            redis_key = self._make_key(key)
            return bool(self._redis_client.exists(redis_key))
        except Exception as e:
            logger.warning(f"Redis exists error for key {key}: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        try:
            info = self._redis_client.info("stats")
            return {
                "backend": "redis",
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "connected_clients": info.get("connected_clients", 0),
            }
        except Exception as e:
            logger.warning(f"Redis stats error: {e}")
            return {"backend": "redis", "error": str(e)}


class MemcacheCacheBackend(CacheBackendInterface):
    """Memcache cache backend."""
    
    def __init__(self, config: MetadataCacheConfig):
        """Initialize the Memcache cache backend."""
        self.config = config
        self._memcache_client = None
        self._connect()
        
    def _connect(self) -> None:
        """Connect to Memcache."""
        try:
            from pymemcache.client.hash import HashClient
            from pymemcache.client.base import Client
            
            if len(self.config.memcache_servers) == 1:
                # Single server
                self._memcache_client = Client(
                    self.config.memcache_servers[0],
                    timeout=self.config.memcache_timeout,
                    connect_timeout=self.config.memcache_connect_timeout,
                )
            else:
                # Multiple servers with consistent hashing
                self._memcache_client = HashClient(
                    self.config.memcache_servers,
                    timeout=self.config.memcache_timeout,
                    connect_timeout=self.config.memcache_connect_timeout,
                )
            
            logger.info(f"Connected to Memcache servers: {self.config.memcache_servers}")
            
        except ImportError:
            raise CacheError("Memcache client not installed. Install with: pip install pymemcache")
        except Exception as e:
            raise CacheError(f"Failed to connect to Memcache: {e}")
    
    def _make_key(self, key: str) -> str:
        """Create a prefixed key for Memcache."""
        full_key = f"{self.config.key_prefix}{key}"
        # Memcache has a 250 char key limit
        if len(full_key) > 250:
            # Hash long keys
            hash_val = hashlib.sha256(full_key.encode()).hexdigest()
            return f"{self.config.key_prefix}{hash_val}"
        return full_key
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache."""
        try:
            memcache_key = self._make_key(key)
            data = self._memcache_client.get(memcache_key)
            
            if data is None:
                return None
            
            return pickle.loads(data)
            
        except Exception as e:
            logger.warning(f"Memcache get error for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set a value in the cache."""
        try:
            memcache_key = self._make_key(key)
            data = pickle.dumps(value)
            
            # Memcache TTL is in seconds, 0 means no expiration
            expire_time = ttl if ttl > 0 else 0
            self._memcache_client.set(memcache_key, data, expire=expire_time)
                
        except Exception as e:
            logger.warning(f"Memcache set error for key {key}: {e}")
    
    def delete(self, key: str) -> None:
        """Delete a value from the cache."""
        try:
            memcache_key = self._make_key(key)
            self._memcache_client.delete(memcache_key)
        except Exception as e:
            logger.warning(f"Memcache delete error for key {key}: {e}")
    
    def clear(self) -> None:
        """Clear all values from the cache."""
        try:
            self._memcache_client.flush_all()
        except Exception as e:
            logger.warning(f"Memcache clear error: {e}")
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        return self.get(key) is not None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        try:
            stats = self._memcache_client.stats()
            return {
                "backend": "memcache",
                "stats": stats,
            }
        except Exception as e:
            logger.warning(f"Memcache stats error: {e}")
            return {"backend": "memcache", "error": str(e)}


class TieredMetadataCache:
    """Multi-tier metadata cache with TTL support."""
    
    def __init__(self, config: Optional[MetadataCacheConfig] = None):
        """Initialize the tiered metadata cache.
        
        Args:
            config: Cache configuration
        """
        self.config = config or get_performance_config().metadata_cache
        
        if not self.config.enabled:
            self.backend = None
            logger.info("Metadata cache is disabled")
            return
        
        # Initialize the appropriate backend
        if self.config.backend == CacheBackend.REDIS:
            self.backend = RedisCacheBackend(self.config)
        elif self.config.backend == CacheBackend.MEMCACHE:
            self.backend = MemcacheCacheBackend(self.config)
        else:
            self.backend = MemoryCacheBackend(self.config)
        
        logger.info(f"Initialized metadata cache with backend: {self.config.backend.value}")
    
    def _get_ttl_for_type(self, cache_type: str) -> int:
        """Get TTL for a specific cache type."""
        ttl_map = {
            "schema": self.config.schema_ttl,
            "manifest": self.config.manifest_ttl,
            "table_list": self.config.table_list_ttl,
            "namespace": self.config.namespace_ttl,
        }
        return ttl_map.get(cache_type, self.config.default_ttl)
    
    def get(self, key: str, cache_type: str = "default") -> Optional[Any]:
        """Get a value from the cache.
        
        Args:
            key: Cache key
            cache_type: Type of cached data for TTL determination
            
        Returns:
            Cached value or None
        """
        if not self.config.enabled or not self.backend:
            return None
        
        return self.backend.get(key)
    
    def set(self, key: str, value: Any, cache_type: str = "default", ttl: Optional[int] = None) -> None:
        """Set a value in the cache.
        
        Args:
            key: Cache key
            value: Value to cache
            cache_type: Type of cached data for TTL determination
            ttl: Optional TTL override in seconds
        """
        if not self.config.enabled or not self.backend:
            return
        
        if ttl is None:
            ttl = self._get_ttl_for_type(cache_type)
        
        self.backend.set(key, value, ttl)
    
    def delete(self, key: str) -> None:
        """Delete a value from the cache.
        
        Args:
            key: Cache key
        """
        if not self.config.enabled or not self.backend:
            return
        
        self.backend.delete(key)
    
    def clear(self) -> None:
        """Clear all values from the cache."""
        if not self.config.enabled or not self.backend:
            return
        
        self.backend.clear()
        logger.info("Cleared metadata cache")
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if key exists and is not expired
        """
        if not self.config.enabled or not self.backend:
            return False
        
        return self.backend.exists(key)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        if not self.config.enabled or not self.backend:
            return {"enabled": False}
        
        stats = self.backend.get_stats()
        stats["enabled"] = True
        return stats
    
    def make_cache_key(self, *parts: str) -> str:
        """Create a cache key from parts.
        
        Args:
            *parts: Key components
            
        Returns:
            Cache key string
        """
        return ":".join(str(p) for p in parts if p)


# Global cache instance
_global_cache: Optional[TieredMetadataCache] = None


def get_metadata_cache() -> TieredMetadataCache:
    """Get the global metadata cache."""
    global _global_cache
    if _global_cache is None:
        _global_cache = TieredMetadataCache()
    return _global_cache


def set_metadata_cache(cache: TieredMetadataCache) -> None:
    """Set the global metadata cache."""
    global _global_cache
    _global_cache = cache


def reset_metadata_cache() -> None:
    """Reset the global metadata cache."""
    global _global_cache
    if _global_cache:
        _global_cache.clear()
    _global_cache = None