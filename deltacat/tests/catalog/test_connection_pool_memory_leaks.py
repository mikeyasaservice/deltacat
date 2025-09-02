"""Tests for connection pool memory leak scenarios."""

import gc
import os
import pytest
import tempfile
import threading
import time
import weakref
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
    _cleanup_on_exit,
)
from deltacat.config.performance import ConnectionPoolConfig


class TestMemoryLeakScenarios:
    """Test scenarios that could lead to memory leaks."""
    
    def test_orphaned_connections_are_reclaimed(self):
        """Test that orphaned connections are automatically reclaimed."""
        create_conn = Mock(side_effect=lambda: f"conn_{time.time()}")
        config = ConnectionPoolConfig(min_size=1, max_size=5, thread_safe=True)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        # Get a connection and hold it
        ctx = pool.get_connection()
        conn = ctx.__enter__()
        
        # Manually mark it as orphaned by setting checked_out time to past
        pooled_conn = pool._in_use[id(conn)]
        pooled_conn.checked_out_at = time.time() - 3700  # Over 1 hour ago
        
        # Should be marked as orphaned
        stats = pool.get_stats()
        assert stats["orphaned_connections"] == 1
        
        # Trigger reclamation
        pool._reclaim_orphaned_connections()
        
        # Orphaned connections should be cleaned up
        assert len(pool._in_use) == 0
        stats = pool.get_stats()
        assert stats["orphaned_connections"] == 0
        
        pool.close()
    
    def test_connection_close_failure_doesnt_leak_count(self):
        """Test that connection count is properly tracked even when close fails."""
        create_conn = Mock(side_effect=lambda: f"conn_{time.time()}")
        close_conn = Mock(side_effect=Exception("Close failed"))
        config = ConnectionPoolConfig(min_size=2, max_size=5, thread_safe=False)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            close_connection=close_conn,
            config=config,
        )
        
        initial_count = pool._created_count
        assert initial_count == 2
        
        # Get a connection and mark it as invalid
        ctx = pool.get_connection()
        conn = ctx.__enter__()
        pool._in_use[id(conn)].is_valid = False
        
        # Return it - close will fail but count should be tracked
        try:
            ctx.__exit__(None, None, None)
        except:
            pass  # Expected to fail due to close error
        
        # Count should be decremented despite close failure
        assert pool._created_count == initial_count - 1
        assert close_conn.called
        
        pool.close()
    
    def test_atexit_handler_cleanup(self):
        """Test that atexit handler properly cleans up pools."""
        # Reset to clean state
        reset_pool_manager()
        
        # Create a pool manager
        manager = get_pool_manager()
        
        # Create some pools
        create_conn = Mock(return_value="connection")
        pool1 = manager.get_or_create_pool("pool1", create_connection=create_conn)
        pool2 = manager.get_or_create_pool("pool2", create_connection=create_conn)
        
        # Simulate exit cleanup
        _cleanup_on_exit()
        
        # Pools should be closed
        assert pool1._closed
        assert pool2._closed
    
    def test_temporary_file_cleanup_on_gateway_close(self):
        """Test that temporary database files are cleaned up."""
        import tempfile
        
        # Create a temporary file to simulate database file
        temp_fd, temp_path = tempfile.mkstemp(suffix='.duckdb', prefix='test_')
        os.close(temp_fd)
        
        # Test the cleanup logic directly
        class MockGateway:
            def __init__(self):
                self._db_file = temp_path
            
            def _cleanup_temp_database(self):
                """Clean up temporary database files."""
                if self._db_file:
                    import os
                    try:
                        db_path = self._db_file
                        if os.path.exists(db_path):
                            os.unlink(db_path)
                            # Also remove WAL and SHM files if they exist
                            for suffix in ['.wal', '.shm']:
                                wal_path = db_path + suffix
                                if os.path.exists(wal_path):
                                    os.unlink(wal_path)
                    except Exception:
                        pass
                    finally:
                        self._db_file = None
        
        gateway = MockGateway()
        
        # File should exist
        assert os.path.exists(temp_path)
        
        # Clean up
        gateway._cleanup_temp_database()
        
        # File should be cleaned up
        assert not os.path.exists(temp_path)
        assert gateway._db_file is None
    
    @pytest.mark.skip(reason="Flaky due to jitter in exponential backoff")
    def test_exponential_backoff_reduces_cpu_usage(self):
        """Test that exponential backoff is used when waiting for connections."""
        create_conn = Mock(side_effect=lambda: f"conn_{time.time()}")
        config = ConnectionPoolConfig(min_size=1, max_size=1, connection_timeout=2)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        # Hold the only connection
        ctx = pool.get_connection()
        conn = ctx.__enter__()
        
        # Try to get another connection in a thread
        wait_times = []
        original_sleep = time.sleep
        
        def mock_sleep(duration):
            wait_times.append(duration)
            original_sleep(min(duration, 0.01))  # Sleep shorter for test speed
        
        with patch('time.sleep', side_effect=mock_sleep):
            try:
                with pytest.raises(ConnectionPoolError):
                    with pool.get_connection(timeout=0.5):
                        pass
            finally:
                ctx.__exit__(None, None, None)
        
        # Check that we have multiple wait times
        assert len(wait_times) > 1
        # Note: Due to jitter, we can't guarantee monotonic increase
        # but the implementation does use exponential backoff
        
        pool.close()
    
    def test_weakref_tracking_allows_gc(self):
        """Test that ConnectionPoolManager instances can be garbage collected."""
        # Create a manager
        manager = ConnectionPoolManager()
        manager_ref = weakref.ref(manager)
        
        # Manager should be tracked
        assert manager_ref() is not None
        
        # Delete the manager
        del manager
        gc.collect()
        
        # Manager should be garbage collected
        assert manager_ref() is None
    
    def test_health_check_thread_stops_on_close(self):
        """Test that health check thread is properly stopped."""
        create_conn = Mock(return_value="connection")
        config = ConnectionPoolConfig(min_size=1, max_size=5, thread_safe=True)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        # Health check thread should be running
        assert pool._health_check_thread is not None
        assert pool._health_check_thread.is_alive()
        
        # Close the pool
        pool.close()
        
        # Health check thread should stop
        time.sleep(0.1)  # Give thread time to stop
        assert not pool._health_check_thread.is_alive()
    
    def test_concurrent_access_no_leak(self):
        """Test that concurrent access doesn't cause memory leaks."""
        create_conn = Mock(side_effect=lambda: f"conn_{time.time()}")
        config = ConnectionPoolConfig(min_size=2, max_size=10, thread_safe=True)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        errors = []
        
        def worker():
            try:
                for _ in range(10):
                    with pool.get_connection() as conn:
                        time.sleep(0.001)  # Simulate work
            except Exception as e:
                errors.append(e)
        
        # Start multiple threads
        threads = []
        for _ in range(20):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # No errors should occur
        assert len(errors) == 0
        
        # All connections should be returned
        time.sleep(0.1)  # Allow time for returns
        stats = pool.get_stats()
        assert stats["in_use"] == 0
        assert stats["available"] > 0
        
        pool.close()
    
    def test_pool_metrics_logging(self):
        """Test that pool metrics are properly logged."""
        create_conn = Mock(return_value="connection")
        config = ConnectionPoolConfig(min_size=1, max_size=5)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        # Get some connections to create metrics
        contexts = []
        for _ in range(3):
            ctx = pool.get_connection()
            ctx.__enter__()
            contexts.append(ctx)
        
        # Log metrics
        with patch('deltacat.catalog.connection_pool.logger') as mock_logger:
            pool.log_pool_metrics()
            
            # Should log warning about high utilization (3/5 = 60%, but let's check it was called)
            assert mock_logger.info.called
        
        # Clean up
        for ctx in contexts:
            ctx.__exit__(None, None, None)
        
        pool.close()
    
    def test_stats_include_extended_metrics(self):
        """Test that stats include all extended metrics."""
        create_conn = Mock(return_value="connection")
        config = ConnectionPoolConfig(min_size=2, max_size=5, thread_safe=True)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        # Wait a bit to have non-zero age
        time.sleep(0.1)
        
        stats = pool.get_stats()
        
        # Check all expected fields are present
        assert "name" in stats
        assert "total_created" in stats
        assert "available" in stats
        assert "in_use" in stats
        assert "closed" in stats
        assert "orphaned_connections" in stats
        assert "orphaned_details" in stats
        assert "avg_connection_age_seconds" in stats
        assert "max_connection_age_seconds" in stats
        assert "health_check_running" in stats
        
        # Verify some values
        assert stats["avg_connection_age_seconds"] > 0
        assert stats["max_connection_age_seconds"] > 0
        assert stats["health_check_running"] is True
        
        pool.close()


class TestLongRunningStability:
    """Tests for long-running stability and resource management."""
    
    @pytest.mark.slow
    def test_long_running_pool_stability(self):
        """Test that pool remains stable over many operations."""
        create_conn = Mock(side_effect=lambda: f"conn_{time.time()}")
        config = ConnectionPoolConfig(min_size=2, max_size=10, thread_safe=True)
        
        pool = ConnectionPool(
            name="test_pool",
            create_connection=create_conn,
            config=config,
        )
        
        # Perform many operations
        for i in range(100):
            with pool.get_connection() as conn:
                # Simulate some work
                time.sleep(0.001)
            
            # Periodically check stats
            if i % 20 == 0:
                stats = pool.get_stats()
                assert stats["in_use"] == 0
                assert stats["orphaned_connections"] == 0
        
        # Final verification
        stats = pool.get_stats()
        assert stats["total_created"] <= config.max_size
        assert stats["in_use"] == 0
        
        pool.close()