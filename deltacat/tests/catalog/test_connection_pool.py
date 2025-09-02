"""Tests for connection pooling functionality."""

import pytest
import threading
import time
from unittest.mock import Mock, patch, MagicMock
import duckdb

from deltacat.catalog.connection_pool import (
    ConnectionPool,
    ConnectionPoolError,
    ConnectionPoolManager,
    DuckDBConnectionPool,
    PooledConnection,
    get_pool_manager,
    reset_pool_manager,
)
from deltacat.config.performance import ConnectionPoolConfig


class TestPooledConnection:
    """Test PooledConnection class."""
    
    def test_pooled_connection_initialization(self):
        """Test PooledConnection initialization."""
        conn = Mock()
        pooled = PooledConnection(
            connection=conn,
            created_at=time.time(),
            last_used_at=time.time(),
            pool_id="test_pool",
        )
        
        assert pooled.connection == conn
        assert pooled.usage_count == 0
        assert pooled.is_valid is True
        assert pooled.pool_id == "test_pool"
    
    def test_pooled_connection_use(self):
        """Test marking connection as used."""
        conn = Mock()
        pooled = PooledConnection(
            connection=conn,
            created_at=time.time(),
            last_used_at=time.time(),
        )
        
        initial_count = pooled.usage_count
        initial_time = pooled.last_used_at
        
        time.sleep(0.01)  # Small delay to ensure time difference
        pooled.use()
        
        assert pooled.usage_count == initial_count + 1
        assert pooled.last_used_at > initial_time
    
    def test_pooled_connection_expiration(self):
        """Test connection expiration check."""
        conn = Mock()
        created_time = time.time() - 100  # 100 seconds ago
        pooled = PooledConnection(
            connection=conn,
            created_at=created_time,
            last_used_at=created_time,
        )
        
        # Should not be expired with large timeout
        assert not pooled.is_expired(1000)
        
        # Should be expired with small timeout
        assert pooled.is_expired(10)


class TestConnectionPool:
    """Test ConnectionPool class."""
    
    def test_connection_pool_initialization(self):
        """Test ConnectionPool initialization."""
        create_conn = Mock(return_value="connection")
        config = ConnectionPoolConfig(min_size=2, max_size=5)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        assert pool.name == "test_pool"
        assert create_conn.call_count == 2  # min_size connections created
        assert len(pool._pool) == 2
    
    def test_get_connection_basic(self):
        """Test getting a connection from the pool."""
        connections = ["conn1", "conn2", "conn3"]
        create_conn = Mock(side_effect=connections)
        config = ConnectionPoolConfig(min_size=2, max_size=5)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        with pool.get_connection() as conn:
            assert conn in connections[:2]  # Should get one of the pre-created connections
            assert len(pool._in_use) == 1
        
        # After context manager, connection should be returned to pool
        assert len(pool._in_use) == 0
        assert len(pool._pool) == 2
    
    def test_get_connection_creates_new_when_needed(self):
        """Test that pool creates new connections when all are in use."""
        connections = ["conn1", "conn2", "conn3", "conn4"]
        create_conn = Mock(side_effect=connections)
        config = ConnectionPoolConfig(min_size=2, max_size=5)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        # Get all pre-created connections and hold them
        held_connections = []
        ctx_managers = []
        for _ in range(2):
            ctx = pool.get_connection()
            conn = ctx.__enter__()
            held_connections.append(conn)
            ctx_managers.append(ctx)
        
        assert create_conn.call_count == 2  # Only min_size created so far
        assert len(pool._in_use) == 2  # Both connections in use
        
        # Get one more connection - should create new one
        with pool.get_connection() as conn:
            assert conn == "conn3"
            assert create_conn.call_count == 3  # New connection created
            assert len(pool._in_use) == 3  # Three connections in use
        
        # Clean up
        for ctx in ctx_managers:
            ctx.__exit__(None, None, None)
    
    def test_connection_pool_max_size(self):
        """Test that pool respects maximum size."""
        create_conn = Mock(side_effect=lambda: f"conn_{time.time()}")
        config = ConnectionPoolConfig(min_size=1, max_size=2, connection_timeout=1)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        # Get max connections and hold them
        held_connections = []
        ctx_managers = []
        for _ in range(2):
            ctx = pool.get_connection()
            conn = ctx.__enter__()
            held_connections.append(conn)
            ctx_managers.append(ctx)
        
        # Verify both connections are in use
        assert len(pool._in_use) == 2
        assert len(pool._pool) == 0
        
        # Try to get one more - should timeout
        with pytest.raises(ConnectionPoolError) as exc_info:
            with pool.get_connection(timeout=1):
                pass
        
        assert "Timeout waiting for connection" in str(exc_info.value)
        
        # Clean up
        for ctx in ctx_managers:
            ctx.__exit__(None, None, None)
    
    def test_connection_validation(self):
        """Test connection validation and cleanup."""
        connections = ["conn1", "conn2"]
        create_conn = Mock(side_effect=connections)
        validate_conn = Mock(side_effect=lambda c: c != "conn1")  # conn1 is invalid
        config = ConnectionPoolConfig(min_size=2, max_size=5)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            validate_connection=validate_conn,
            config=config,
        )
        
        # Trigger validation
        pool._validate_and_cleanup()
        
        # conn1 should be removed, only conn2 should remain
        assert len(pool._pool) == 1
        assert pool._pool[0].connection == "conn2"
    
    def test_connection_pool_close(self):
        """Test closing the connection pool."""
        connections = ["conn1", "conn2"]
        create_conn = Mock(side_effect=connections)
        close_conn = Mock()
        config = ConnectionPoolConfig(min_size=2, max_size=5)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            close_connection=close_conn,
            config=config,
        )
        
        pool.close()
        
        assert pool._closed is True
        assert close_conn.call_count == 2  # Both connections closed
        assert len(pool._pool) == 0
    
    def test_get_stats(self):
        """Test getting pool statistics."""
        create_conn = Mock(return_value="connection")
        config = ConnectionPoolConfig(min_size=2, max_size=5)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        stats = pool.get_stats()
        
        assert stats["name"] == "test_pool"
        assert stats["total_created"] == 2
        assert stats["available"] == 2
        assert stats["in_use"] == 0
        assert stats["closed"] is False


class TestDuckDBConnectionPool:
    """Test DuckDBConnectionPool class."""
    
    @patch('duckdb.connect')
    def test_duckdb_pool_initialization(self, mock_connect):
        """Test DuckDB connection pool initialization."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        config = ConnectionPoolConfig(min_size=2, max_size=5)
        pool = DuckDBConnectionPool(config=config)
        
        assert pool.name == "DuckDB"
        assert mock_connect.call_count == 2  # min_size connections created
    
    @patch('duckdb.connect')
    def test_duckdb_connection_validation(self, mock_connect):
        """Test DuckDB connection validation."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (1,)
        mock_connect.return_value = mock_conn
        
        config = ConnectionPoolConfig(min_size=1, max_size=5)
        pool = DuckDBConnectionPool(config=config)
        
        # Test valid connection
        assert pool.validate_connection(mock_conn) is True
        mock_conn.execute.assert_called_with("SELECT 1")
        
        # Test invalid connection
        mock_conn.execute.side_effect = Exception("Connection error")
        assert pool.validate_connection(mock_conn) is False


class TestConnectionPoolManager:
    """Test ConnectionPoolManager class."""
    
    def test_pool_manager_initialization(self):
        """Test ConnectionPoolManager initialization."""
        config = ConnectionPoolConfig()
        manager = ConnectionPoolManager(config=config)
        
        assert manager.config == config
        assert len(manager._pools) == 0
    
    def test_get_or_create_pool(self):
        """Test getting or creating a pool."""
        manager = ConnectionPoolManager()
        
        create_conn = Mock(return_value="connection")
        
        # First call should create the pool
        pool1 = manager.get_or_create_pool(
            "test_pool",
            create_connection=create_conn,
        )
        
        assert pool1.name == "test_pool"
        assert "test_pool" in manager._pools
        
        # Second call should return the same pool
        pool2 = manager.get_or_create_pool(
            "test_pool",
            create_connection=create_conn,
        )
        
        assert pool1 is pool2
    
    @patch('duckdb.connect')
    def test_get_duckdb_pool(self, mock_connect):
        """Test getting a DuckDB pool."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        manager = ConnectionPoolManager()
        pool = manager.get_duckdb_pool()
        
        assert isinstance(pool, DuckDBConnectionPool)
        assert "duckdb_default" in manager._pools
    
    def test_close_pool(self):
        """Test closing a specific pool."""
        manager = ConnectionPoolManager()
        
        create_conn = Mock(return_value="connection")
        pool = manager.get_or_create_pool(
            "test_pool",
            create_connection=create_conn,
        )
        
        manager.close_pool("test_pool")
        
        assert "test_pool" not in manager._pools
        assert pool._closed is True
    
    def test_close_all(self):
        """Test closing all pools."""
        manager = ConnectionPoolManager()
        
        create_conn = Mock(return_value="connection")
        
        # Create multiple pools
        pool1 = manager.get_or_create_pool("pool1", create_connection=create_conn)
        pool2 = manager.get_or_create_pool("pool2", create_connection=create_conn)
        
        manager.close_all()
        
        assert len(manager._pools) == 0
        assert pool1._closed is True
        assert pool2._closed is True
    
    def test_get_stats(self):
        """Test getting statistics for all pools."""
        manager = ConnectionPoolManager()
        
        create_conn = Mock(return_value="connection")
        
        # Create multiple pools
        manager.get_or_create_pool("pool1", create_connection=create_conn)
        manager.get_or_create_pool("pool2", create_connection=create_conn)
        
        stats = manager.get_stats()
        
        assert "pool1" in stats
        assert "pool2" in stats
        assert stats["pool1"]["name"] == "pool1"
        assert stats["pool2"]["name"] == "pool2"


class TestGlobalFunctions:
    """Test global pool manager functions."""
    
    def test_get_pool_manager(self):
        """Test getting the global pool manager."""
        reset_pool_manager()  # Ensure clean state
        
        manager1 = get_pool_manager()
        manager2 = get_pool_manager()
        
        assert manager1 is manager2  # Should be the same instance
        assert isinstance(manager1, ConnectionPoolManager)
    
    def test_reset_pool_manager(self):
        """Test resetting the global pool manager."""
        manager1 = get_pool_manager()
        
        # Create a pool
        create_conn = Mock(return_value="connection")
        manager1.get_or_create_pool("test_pool", create_connection=create_conn)
        
        reset_pool_manager()
        
        manager2 = get_pool_manager()
        assert manager1 is not manager2  # Should be different instances
        assert len(manager2._pools) == 0  # New manager should have no pools


class TestThreadSafety:
    """Test thread safety of connection pools."""
    
    def test_concurrent_get_connection(self):
        """Test concurrent access to connection pool."""
        connections = [f"conn_{i}" for i in range(10)]
        create_conn = Mock(side_effect=connections)
        config = ConnectionPoolConfig(min_size=2, max_size=10, thread_safe=True)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        results = []
        
        def get_and_use_connection():
            with pool.get_connection() as conn:
                results.append(conn)
                time.sleep(0.01)  # Simulate some work
        
        # Create threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=get_and_use_connection)
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All threads should have gotten a connection
        assert len(results) == 5
        assert len(set(results)) <= 5  # May reuse connections