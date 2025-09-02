"""Tests for metadata caching functionality."""

import pytest
import time
import pickle
from unittest.mock import Mock, patch, MagicMock
from collections import OrderedDict

from deltacat.catalog.cache.metadata_cache import (
    CacheEntry,
    MemoryCacheBackend,
    RedisCacheBackend,
    MemcacheCacheBackend,
    TieredMetadataCache,
    get_metadata_cache,
    set_metadata_cache,
    reset_metadata_cache,
)
from deltacat.config.performance import MetadataCacheConfig, CacheBackend


class TestCacheEntry:
    """Test CacheEntry class."""
    
    def test_cache_entry_initialization(self):
        """Test CacheEntry initialization."""
        entry = CacheEntry(
            key="test_key",
            value="test_value",
            ttl=300,
            created_at=time.time(),
            accessed_at=time.time(),
        )
        
        assert entry.key == "test_key"
        assert entry.value == "test_value"
        assert entry.ttl == 300
        assert entry.access_count == 0
        assert entry.size_bytes == 0
    
    def test_cache_entry_expiration(self):
        """Test cache entry expiration check."""
        current_time = time.time()
        
        # Non-expiring entry (ttl <= 0)
        entry1 = CacheEntry(
            key="key1",
            value="value1",
            ttl=0,
            created_at=current_time - 1000,
            accessed_at=current_time,
        )
        assert not entry1.is_expired()
        
        # Not expired entry
        entry2 = CacheEntry(
            key="key2",
            value="value2",
            ttl=300,
            created_at=current_time - 100,
            accessed_at=current_time,
        )
        assert not entry2.is_expired()
        
        # Expired entry
        entry3 = CacheEntry(
            key="key3",
            value="value3",
            ttl=100,
            created_at=current_time - 200,
            accessed_at=current_time,
        )
        assert entry3.is_expired()
    
    def test_cache_entry_access(self):
        """Test marking cache entry as accessed."""
        entry = CacheEntry(
            key="test_key",
            value="test_value",
            ttl=300,
            created_at=time.time(),
            accessed_at=time.time(),
        )
        
        initial_count = entry.access_count
        initial_time = entry.accessed_at
        
        time.sleep(0.01)  # Small delay
        entry.access()
        
        assert entry.access_count == initial_count + 1
        assert entry.accessed_at > initial_time


class TestMemoryCacheBackend:
    """Test MemoryCacheBackend class."""
    
    def test_memory_cache_initialization(self):
        """Test MemoryCacheBackend initialization."""
        config = MetadataCacheConfig(
            max_memory_items=100,
            max_memory_size_mb=10,
        )
        
        cache = MemoryCacheBackend(config)
        
        assert cache.config == config
        assert len(cache._cache) == 0
        assert cache._total_size_bytes == 0
        assert cache._hits == 0
        assert cache._misses == 0
    
    def test_memory_cache_set_and_get(self):
        """Test setting and getting values from memory cache."""
        config = MetadataCacheConfig()
        cache = MemoryCacheBackend(config)
        
        # Set a value
        cache.set("key1", "value1", 300)
        
        # Get the value
        result = cache.get("key1")
        assert result == "value1"
        assert cache._hits == 1
        
        # Get non-existent key
        result = cache.get("key2")
        assert result is None
        assert cache._misses == 1
    
    def test_memory_cache_lru_eviction(self):
        """Test LRU eviction in memory cache."""
        config = MetadataCacheConfig(
            max_memory_items=3,
            max_memory_size_mb=10,
        )
        cache = MemoryCacheBackend(config)
        
        # Add items up to limit
        cache.set("key1", "value1", 300)
        cache.set("key2", "value2", 300)
        cache.set("key3", "value3", 300)
        
        # Access key1 and key2 to make them more recently used
        cache.get("key1")
        cache.get("key2")
        
        # Add one more item - should evict key3 (least recently used)
        cache.set("key4", "value4", 300)
        
        assert cache.get("key1") == "value1"
        assert cache.get("key2") == "value2"
        assert cache.get("key3") is None  # Should be evicted
        assert cache.get("key4") == "value4"
    
    def test_memory_cache_ttl_expiration(self):
        """Test TTL expiration in memory cache."""
        config = MetadataCacheConfig()
        cache = MemoryCacheBackend(config)
        
        # Set a value with very short TTL
        current_time = time.time()
        entry = CacheEntry(
            key="key1",
            value="value1",
            ttl=1,  # 1 second TTL
            created_at=current_time - 2,  # Created 2 seconds ago
            accessed_at=current_time,
            size_bytes=0,
        )
        cache._cache["key1"] = entry
        
        # Should return None due to expiration
        result = cache.get("key1")
        assert result is None
        assert "key1" not in cache._cache
    
    def test_memory_cache_delete(self):
        """Test deleting values from memory cache."""
        config = MetadataCacheConfig()
        cache = MemoryCacheBackend(config)
        
        cache.set("key1", "value1", 300)
        cache.set("key2", "value2", 300)
        
        # Delete key1
        cache.delete("key1")
        
        assert cache.get("key1") is None
        assert cache.get("key2") == "value2"
    
    def test_memory_cache_clear(self):
        """Test clearing memory cache."""
        config = MetadataCacheConfig()
        cache = MemoryCacheBackend(config)
        
        cache.set("key1", "value1", 300)
        cache.set("key2", "value2", 300)
        
        cache.clear()
        
        assert len(cache._cache) == 0
        assert cache._total_size_bytes == 0
        assert cache._hits == 0
        assert cache._misses == 0
    
    def test_memory_cache_exists(self):
        """Test checking if key exists in memory cache."""
        config = MetadataCacheConfig()
        cache = MemoryCacheBackend(config)
        
        cache.set("key1", "value1", 300)
        
        assert cache.exists("key1") is True
        assert cache.exists("key2") is False
    
    def test_memory_cache_stats(self):
        """Test getting memory cache statistics."""
        config = MetadataCacheConfig()
        cache = MemoryCacheBackend(config)
        
        cache.set("key1", "value1", 300)
        cache.get("key1")  # Hit
        cache.get("key2")  # Miss
        
        stats = cache.get_stats()
        
        assert stats["backend"] == "memory"
        assert stats["entries"] == 1
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["hit_rate"] == 0.5


class TestRedisCacheBackend:
    """Test RedisCacheBackend class."""
    
    @patch('redis.ConnectionPool')
    @patch('redis.Redis')
    def test_redis_cache_initialization(self, mock_redis, mock_pool):
        """Test RedisCacheBackend initialization."""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        
        config = MetadataCacheConfig(
            backend=CacheBackend.REDIS,
            redis_host="localhost",
            redis_port=6379,
        )
        
        cache = RedisCacheBackend(config)
        
        assert cache.config == config
        mock_redis_instance.ping.assert_called_once()
    
    @patch('redis.ConnectionPool')
    @patch('redis.Redis')
    def test_redis_cache_set_and_get(self, mock_redis, mock_pool):
        """Test setting and getting values from Redis cache."""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        
        config = MetadataCacheConfig(backend=CacheBackend.REDIS)
        cache = RedisCacheBackend(config)
        
        # Set a value
        cache.set("key1", {"data": "value1"}, 300)
        mock_redis_instance.setex.assert_called_once()
        
        # Get a value
        mock_redis_instance.get.return_value = pickle.dumps({"data": "value1"})
        result = cache.get("key1")
        assert result == {"data": "value1"}
        
        # Get non-existent key
        mock_redis_instance.get.return_value = None
        result = cache.get("key2")
        assert result is None
    
    @patch('redis.ConnectionPool')
    @patch('redis.Redis')
    def test_redis_cache_delete(self, mock_redis, mock_pool):
        """Test deleting values from Redis cache."""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        
        config = MetadataCacheConfig(backend=CacheBackend.REDIS)
        cache = RedisCacheBackend(config)
        
        cache.delete("key1")
        mock_redis_instance.delete.assert_called_once_with("deltacat:key1")
    
    @patch('redis.ConnectionPool')
    @patch('redis.Redis')
    def test_redis_cache_exists(self, mock_redis, mock_pool):
        """Test checking if key exists in Redis cache."""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        
        config = MetadataCacheConfig(backend=CacheBackend.REDIS)
        cache = RedisCacheBackend(config)
        
        mock_redis_instance.exists.return_value = 1
        assert cache.exists("key1") is True
        
        mock_redis_instance.exists.return_value = 0
        assert cache.exists("key2") is False


class TestMemcacheCacheBackend:
    """Test MemcacheCacheBackend class."""
    
    @patch('pymemcache.client.base.Client')
    def test_memcache_cache_initialization(self, mock_client):
        """Test MemcacheCacheBackend initialization."""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        config = MetadataCacheConfig(
            backend=CacheBackend.MEMCACHE,
            memcache_servers=["localhost:11211"],
        )
        
        cache = MemcacheCacheBackend(config)
        
        assert cache.config == config
        mock_client.assert_called_once()
    
    @patch('pymemcache.client.base.Client')
    def test_memcache_cache_set_and_get(self, mock_client):
        """Test setting and getting values from Memcache cache."""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        config = MetadataCacheConfig(backend=CacheBackend.MEMCACHE)
        cache = MemcacheCacheBackend(config)
        
        # Set a value
        cache.set("key1", {"data": "value1"}, 300)
        mock_client_instance.set.assert_called_once()
        
        # Get a value
        mock_client_instance.get.return_value = pickle.dumps({"data": "value1"})
        result = cache.get("key1")
        assert result == {"data": "value1"}
        
        # Get non-existent key
        mock_client_instance.get.return_value = None
        result = cache.get("key2")
        assert result is None


class TestTieredMetadataCache:
    """Test TieredMetadataCache class."""
    
    def test_tiered_cache_initialization_memory(self):
        """Test TieredMetadataCache initialization with memory backend."""
        config = MetadataCacheConfig(
            enabled=True,
            backend=CacheBackend.MEMORY,
        )
        
        cache = TieredMetadataCache(config)
        
        assert cache.config == config
        assert isinstance(cache.backend, MemoryCacheBackend)
    
    @patch('redis.ConnectionPool')
    @patch('redis.Redis')
    def test_tiered_cache_initialization_redis(self, mock_redis, mock_pool):
        """Test TieredMetadataCache initialization with Redis backend."""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        
        config = MetadataCacheConfig(
            enabled=True,
            backend=CacheBackend.REDIS,
        )
        
        cache = TieredMetadataCache(config)
        
        assert cache.config == config
        assert isinstance(cache.backend, RedisCacheBackend)
    
    def test_tiered_cache_disabled(self):
        """Test TieredMetadataCache when disabled."""
        config = MetadataCacheConfig(enabled=False)
        
        cache = TieredMetadataCache(config)
        
        assert cache.backend is None
        
        # Operations should be no-ops
        cache.set("key1", "value1")
        assert cache.get("key1") is None
        assert cache.exists("key1") is False
    
    def test_tiered_cache_get_ttl_for_type(self):
        """Test getting TTL for different cache types."""
        config = MetadataCacheConfig(
            schema_ttl=600,
            manifest_ttl=300,
            table_list_ttl=60,
            namespace_ttl=900,
            default_ttl=120,
        )
        
        cache = TieredMetadataCache(config)
        
        assert cache._get_ttl_for_type("schema") == 600
        assert cache._get_ttl_for_type("manifest") == 300
        assert cache._get_ttl_for_type("table_list") == 60
        assert cache._get_ttl_for_type("namespace") == 900
        assert cache._get_ttl_for_type("unknown") == 120
    
    def test_tiered_cache_operations(self):
        """Test TieredMetadataCache operations."""
        config = MetadataCacheConfig(
            enabled=True,
            backend=CacheBackend.MEMORY,
        )
        
        cache = TieredMetadataCache(config)
        
        # Set and get
        cache.set("key1", "value1", "schema")
        assert cache.get("key1", "schema") == "value1"
        
        # Exists
        assert cache.exists("key1") is True
        assert cache.exists("key2") is False
        
        # Delete
        cache.delete("key1")
        assert cache.get("key1") is None
        
        # Clear
        cache.set("key2", "value2")
        cache.clear()
        assert cache.get("key2") is None
    
    def test_make_cache_key(self):
        """Test cache key creation."""
        cache = TieredMetadataCache()
        
        key = cache.make_cache_key("catalog", "namespace", "table")
        assert key == "catalog:namespace:table"
        
        key = cache.make_cache_key("part1", None, "part2")
        assert key == "part1:part2"


class TestGlobalCacheFunctions:
    """Test global cache functions."""
    
    def test_get_metadata_cache(self):
        """Test getting the global metadata cache."""
        reset_metadata_cache()  # Ensure clean state
        
        cache1 = get_metadata_cache()
        cache2 = get_metadata_cache()
        
        assert cache1 is cache2  # Should be the same instance
        assert isinstance(cache1, TieredMetadataCache)
    
    def test_set_metadata_cache(self):
        """Test setting the global metadata cache."""
        reset_metadata_cache()
        
        custom_cache = TieredMetadataCache(
            MetadataCacheConfig(default_ttl=999)
        )
        
        set_metadata_cache(custom_cache)
        
        retrieved_cache = get_metadata_cache()
        assert retrieved_cache is custom_cache
        assert retrieved_cache.config.default_ttl == 999
    
    def test_reset_metadata_cache(self):
        """Test resetting the global metadata cache."""
        cache1 = get_metadata_cache()
        cache1.set("key1", "value1")
        
        reset_metadata_cache()
        
        cache2 = get_metadata_cache()
        assert cache1 is not cache2  # Should be different instances
        assert cache2.get("key1") is None  # New cache should be empty